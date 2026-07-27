//! Plexus RPC builder - constructs a fully configured `DynamicHub` instance
//!
//! This module is used by both the main binary and examples.

use std::sync::Arc;

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
/// This function uses `Arc::new_cyclic` to obtain a weak reference to the hub for
/// Orcha, which names it in its own type. PLX-111/116/117 removed the
/// parent-injection ritual: Cone and `ClaudeCode` are handle *providers* and never
/// needed a parent, and Arbor's handle was a `plugin_id -> resolver` lookup, wired
/// below as data.
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
    let claudecode_storage = ClaudeCodeStorage::new(
        ClaudeCodeStorageConfig::default(),
        arbor_storage,
    )
    .await
    .expect("Failed to initialize ClaudeCode storage");
    let claudecode: ClaudeCode = ClaudeCode::with_context_type(Arc::new(claudecode_storage));

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

    // PLX-111/116/117: the parent-injection ritual is gone — Arbor's handle was a
    // `plugin_id -> resolver` lookup (now wired as data below), and Cone's and
    // ClaudeCode's parent accessors had zero callers. Only Orcha still needs the
    // weak handle, to name its own `Weak<DynamicHub>` parameter.
    //
    // We keep a clone of `orcha` outside the closure so we can call
    // `recover_running_graphs` after the hub is fully assembled.
    let orcha_for_recovery: std::cell::OnceCell<Orcha> = std::cell::OnceCell::new();

    // PLX-117 / PLX-111: Arbor's parent handle was a `plugin_id -> resolver`
    // lookup and nothing else, so it is wired here as data, after the two
    // providers exist. `Arc::new_cyclic` / `Weak<DynamicHub>` are no longer part
    // of Arbor's construction.
    let arbor = arbor.with_resolvers(
        HandleResolvers::new()
            .with(Arc::new(cone.clone()))
            .with(Arc::new(claudecode.clone())),
    );

    // PLX-111/115/116/117: `Arc::new_cyclic` is gone. It existed solely to hand a
    // `Weak<DynamicHub>` to the parent-injection ritual; with Arbor's resolvers wired
    // as data and Cone's/ClaudeCode's/Orcha's parent handles deleted, no activation
    // takes a back-reference to the hub, so the hub is now built plainly.
    let hub = Arc::new({
        // Initialize Orcha with dependencies
        let graph_runtime = Arc::new(GraphRuntime::new(lattice.storage()));
        let pm = Arc::new(Pm::new(pm_storage.clone(), lattice.storage()));
        let orcha: Orcha = Orcha::new(
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

        let hub = hub.register(arbor)
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
            .register(Solar::new());

        // PLX-142: hand the hub each activation's macro-built ActivationIr, so
        // `substrate.connectome` serves the real tree rather than a document
        // lifted from the legacy schema. Must run after every `register`.
        crate::activations::connectome::declare_connectomes(hub)
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
