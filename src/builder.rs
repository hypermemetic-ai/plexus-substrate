//! Plexus RPC builder - constructs a fully configured `DynamicHub` instance
//!
//! This module is used by both the main binary and examples.

use std::sync::{Arc, Weak};

use crate::activations::arbor::{Arbor, ArborConfig, HandleResolvers};
use crate::activations::bash::Bash;
#[cfg(feature = "chaos")]
use crate::activations::chaos::Chaos;
use crate::activations::claudecode::{ClaudeCode, ClaudeCodeStorage, ClaudeCodeStorageConfig};
use crate::activations::claudecode_loopback::{ClaudeCodeLoopback, LoopbackStorageConfig};
use crate::activations::cone::{Cone, ConeStorageConfig};
use crate::activations::echo::Echo;
use crate::activations::health::Health;
use crate::activations::interactive::Interactive;
use crate::activations::lattice::{Lattice, LatticeStorageConfig};
use crate::activations::changelog::{Changelog, ChangelogStorageConfig};
use crate::activations::mustache::{Mustache, MustacheStorageConfig};
use crate::activations::orcha::pm::{Pm, PmStorage, PmStorageConfig};
use crate::activations::orcha::{GraphRuntime, Orcha, OrchaStorage, OrchaStorageConfig};
use crate::activations::solar::Solar;
use crate::plexus::DynamicHub;
// use plexus_jsexec::{JsExec, JsExecConfig};  // temporarily disabled - needs API updates
use registry::Registry;

/// Build the Plexus RPC hub with registered activations
///
/// The hub implements the Plexus RPC protocol and provides introspection methods:
/// - substrate.call: Route calls to registered activations
/// - substrate.hash: Get configuration hash for cache invalidation
/// - `substrate.list_activations`: Enumerate registered activations
/// - substrate.schema: Get full Plexus RPC schema
///
/// Hub activations (with nested children) are registered with `register_hub`
/// to enable direct nested routing like `substrate.solar.mercury.info`.
///
/// This function uses `Arc::new_cyclic` to inject a weak reference to the hub
/// into Cone and `ClaudeCode`, enabling them to resolve foreign handles through
/// the hub without creating reference cycles.
///
/// This function is async because Arbor, Cone, and `ClaudeCode` require
/// async database initialization.
pub async fn build_plexus_rpc() -> Arc<DynamicHub> {
    // Initialize Arbor first (other activations depend on its storage)
    let arbor = Arbor::new(ArborConfig::default())
        .await
        .expect("Failed to initialize Arbor");
    let arbor_storage = arbor.storage();

    // Initialize Cone with shared Arbor storage
    let cone = Cone::new(ConeStorageConfig::default(), arbor_storage.clone())
        .await
        .expect("Failed to initialize Cone");

    // Initialize ClaudeCode with shared Arbor storage
    // Use explicit type annotation for Weak<DynamicHub> parent context
    let claudecode_storage = ClaudeCodeStorage::new(
        ClaudeCodeStorageConfig::default(),
        arbor_storage,
    )
    .await
    .expect("Failed to initialize ClaudeCode storage");
    let claudecode: ClaudeCode<Weak<DynamicHub>> = ClaudeCode::with_context_type(Arc::new(claudecode_storage));

    // Initialize Mustache for template rendering
    let mustache = Mustache::new(MustacheStorageConfig::default())
        .await
        .expect("Failed to initialize Mustache");

    // Initialize ClaudeCode Loopback for tool permission routing
    let loopback = Arc::new(
        ClaudeCodeLoopback::new(LoopbackStorageConfig::default())
            .await
            .expect("Failed to initialize ClaudeCodeLoopback")
    );

    // Initialize Orcha storage for multi-agent orchestration
    let orcha_storage = Arc::new(
        OrchaStorage::new(OrchaStorageConfig::default())
            .await
            .expect("Failed to initialize Orcha storage")
    );

    // Initialize PM storage for ticket→node mapping
    let pm_storage = Arc::new(
        PmStorage::new(PmStorageConfig::default())
            .await
            .expect("Failed to initialize PM storage")
    );

    // Initialize Changelog for tracking plexus hash transitions
    let changelog = Changelog::new(ChangelogStorageConfig::default())
        .await
        .expect("Failed to initialize Changelog");

    // Clone arbor_storage for Orcha (needs separate reference)
    let arbor_storage_for_orcha = arbor.storage();

    // Initialize JsExec for JavaScript execution in V8 isolates
    // let jsexec = JsExec::new(JsExecConfig::default());  // temporarily disabled

    // Initialize Lattice DAG execution engine
    let lattice = Lattice::new(LatticeStorageConfig::default())
        .await
        .expect("Failed to initialize Lattice storage");

    // Initialize Registry for backend discovery
    let registry = Registry::with_defaults()
        .await
        .expect("Failed to initialize Registry");

    // Use Arc::new_cyclic to get a Weak<DynamicHub> during construction
    // This allows us to inject the parent context into Cone and ClaudeCode
    // before the hub is fully constructed, avoiding reference cycles
    //
    // We keep a clone of `orcha` outside the closure so we can call
    // `recover_running_graphs` after the hub is fully assembled.
    let orcha_for_recovery: std::cell::OnceCell<Orcha<Weak<DynamicHub>>> = std::cell::OnceCell::new();

    // PLX-117 / PLX-111: Arbor's parent handle was a `plugin_id -> resolver`
    // lookup and nothing else, so it is wired here as data, after the two
    // providers exist. `Arc::new_cyclic` / `Weak<DynamicHub>` are no longer part
    // of Arbor's construction.
    let arbor = arbor.with_resolvers(
        HandleResolvers::new()
            .with(Arc::new(cone.clone()))
            .with(Arc::new(claudecode.clone())),
    );

    let hub = Arc::new_cyclic(|weak_hub: &Weak<DynamicHub>| {
        // Inject parent context into activations that need it
        claudecode.inject_parent(weak_hub.clone());

        // Initialize Orcha with dependencies (needs to be inside closure to access claudecode)
        let graph_runtime = Arc::new(GraphRuntime::new(lattice.storage()));
        let pm = Arc::new(Pm::new(pm_storage.clone(), lattice.storage()));
        let orcha: Orcha<Weak<DynamicHub>> = Orcha::new(
            orcha_storage.clone(),
            Arc::new(claudecode.clone()),
            loopback.clone(),
            arbor_storage_for_orcha,
            graph_runtime,
            pm,
        );

        // Store a clone for the post-construction recovery pass.
        let _ = orcha_for_recovery.set(orcha.clone());

        // Build and return the DynamicHub with "substrate" namespace
        let hub = DynamicHub::new("substrate")
            .register(Health::new())
            .register(Echo::new())
            .register(Bash::new());

        // Chaos activation is feature-gated — off by default because it pulls
        // in libc + narrow unsafe signal primitives.
        #[cfg(feature = "chaos")]
        let hub = hub.register(Chaos::new(lattice.storage()));

        hub.register(arbor)
            .register(cone)
            .register(claudecode)
            .register(mustache)
            .register(changelog.clone())
            .register((*loopback).clone())
            .register(orcha)
            // .register(jsexec)  // temporarily disabled
            .register(registry)
            .register(lattice)
            .register(Interactive::new())  // Bidirectional demo activation
            .register(Solar::new())
    });

    // Run changelog startup check
    let plexus_hash = hub.compute_hash();
    match changelog.startup_check(&plexus_hash).await {
        Ok((hash_changed, is_documented, message)) => {
            if hash_changed && !is_documented {
                tracing::error!("{}", message);
            } else if hash_changed {
                tracing::info!("{}", message);
            } else {
                tracing::debug!("{}", message);
            }
        }
        Err(e) => {
            tracing::error!("Changelog startup check failed: {}", e);
        }
    }

    // Run startup recovery for any Orcha graphs that were mid-execution when the
    // substrate last shut down.  This is best-effort: failures are logged, never fatal.
    if let Some(orcha) = orcha_for_recovery.into_inner() {
        orcha.recover_running_graphs().await;
    }

    hub
}
