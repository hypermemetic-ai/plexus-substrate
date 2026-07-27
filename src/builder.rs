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
use plexus_core::plexus::{AdmittedTenant, TenantMount, TenantMountGate, TenantSubtreeFactory};
// use plexus_jsexec::{JsExec, JsExecConfig};  // temporarily disabled - needs API updates
use registry::Registry;

// ============================================================================
// PLX-127 (M4·C) — the exclusion list, as data
// ============================================================================

/// The activations that are **absent** from a tenant mount, and why.
///
/// This is PLX-130's recommendation 1 made mechanical: *"exclude A1, A3, A4,
/// A5, B1, B2 from tenant mounts by mount composition — a separate tenant
/// `DynamicHub` that never registers them. Absence, not denial."*
///
/// Mount composition is the **first** layer and it has to be, because the
/// second layer cannot do this job: `list_methods` and `plugin_schema` take no
/// auth context, so a scope gate denies without hiding. PLX-127's criterion
/// demands absence. Scope gating (`#[method(scope = …)]` plus
/// `DynamicHub::with_default_deny`, both already built, the latter shipping
/// OFF) remains the right second layer.
///
/// | ns | PLX-130 rows | why |
/// |---|---|---|
/// | `bash` | A1 | `Command::new` with no cwd, no env scrub, no uid change — a general-purpose read primitive. |
/// | `orcha` | A3, A4, B1 | `sh -c` from a caller argument (A3) *and* from a command regex-extracted out of model output (A4), which no argument-validation layer can see; plus `run_tickets_files`, documented as taking absolute paths and echoing contents on parse failure (B1). |
/// | `chaos` | A5 | `kill_process` / `crash`. Already `#[cfg(feature = "chaos")]` and off by default; excluded here as well so the two cannot both be on. |
///
/// Every name here must be a namespace that `compose_host_hub` registers, or
/// the list has drifted from the thing it excludes — pinned by
/// `the_exclusion_list_names_only_real_activations`.
pub const TENANT_EXCLUDED_ACTIVATIONS: &[&str] = &["bash", "chaos", "orcha"];

/// `claudecode`, the one surface that **cannot** be excluded, because it is
/// the product.
///
/// PLX-130 measured that no change confined to substrate makes it tenant-safe:
/// `allowed_tools` is caller-supplied, `disallowed_tools` was never populated,
/// and enforcement ultimately lives in a third-party CLI's permission model.
/// PLX-144 escalated it and the operator chose option 1 — *tenancy ships WITH
/// claudecode, behind a per-tenant execution sandbox* — and built
/// `plexus-sandbox` for it. **PLX-151 wires the launcher to that sandbox and
/// has not landed.**
///
/// So this is a switch, not a silent omission. It is `false` here because
/// registering `claudecode` today would ship tenancy with a known cross-tenant
/// read, which M4 decision gate 2 forbids; and it is *named* rather than
/// absent because the product decision is that it comes back. PLX-151 flips
/// this, and `claudecode_is_absent_until_the_sandbox_is_wired` /
/// `a_sandboxed_deployment_gets_claudecode_back` pin both sides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantSurface {
    /// Whether `claudecode` launches inside a `plexus_sandbox::Sandbox`
    /// (PLX-151 c1). Until it does, the tenant hub omits the activation.
    pub claudecode_is_sandboxed: bool,
}

impl Default for TenantSurface {
    fn default() -> Self {
        Self {
            claudecode_is_sandboxed: false,
        }
    }
}

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
/// Build every activation the substrate offers, once.
///
/// Split out of [`build_plexus_rpc`] by PLX-127 so that the host hub and a
/// tenant hub are two *compositions* of the same objects. It performs the
/// storage initialisation and nothing else — no changelog check, no orcha
/// recovery — which is also what makes it usable from a test.
///
/// Returns the activation set and the `Orcha` clone the caller needs for the
/// post-assembly recovery pass.
pub async fn build_activations() -> (SubstrateActivations, Orcha) {

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
    (
        SubstrateActivations {
            arbor,
            cone,
            claudecode,
            mustache,
            changelog: changelog.clone(),
            loopback: (*loopback).clone(),
            orcha: orcha.clone(),
            registry,
            lattice,
        },
        orcha,
    )
}

/// Build the Plexus RPC hub with registered activations
///
/// The hub implements the Plexus RPC protocol and provides introspection methods:
/// - substrate.call: Route calls to registered activations
/// - substrate.hash: Get configuration hash for cache invalidation
/// - `substrate.list_activations`: Enumerate registered activations
/// - substrate.schema: Get full Plexus RPC schema
///
/// PLX-127 added the `tenants/<id>` mount on top of the host composition; see
/// [`compose_host_hub`], [`compose_tenant_hub`] and
/// [`TENANT_EXCLUDED_ACTIVATIONS`].
pub async fn build_plexus_rpc() -> Arc<DynamicHub> {
    let (activations, orcha_for_recovery) = build_activations().await;
    let changelog = activations.changelog.clone();
    let activations = Arc::new(activations);

    // PLX-127: the host hub is what it always was. The tenant hub is a
    // DIFFERENT composition of the SAME activations, and the difference is the
    // exclusion list — see `TENANT_EXCLUDED_ACTIVATIONS`.
    let host_activations = Arc::clone(&activations);
    let tenant_surface = TenantSurface::default();
    let factory: TenantSubtreeFactory = Arc::new(move |_admitted: &AdmittedTenant| {
        // Reached ONLY with an `AdmittedTenant` in hand, which only
        // `TenantMountGate::admit` can mint. See plexus-core's
        // `plexus::tenant_mount` for why that is the whole of "verify before
        // instantiate".
        //
        // RESIDUAL, stated rather than papered over: every tenant currently
        // gets the same storage handles, because `build_plexus_rpc` builds
        // each storage from `*Config::default()` — one process-global path
        // with no tenant component (PLX-130 row B4). This ticket delivers
        // SURFACE isolation (which activations exist under a tenant) and the
        // gate in front of it. DATA isolation is PLX-128 (M4·D, per-tenant
        // instances) and PLX-129 (M4·E, per-tenant storage), and PLX-130 is
        // explicit that M4·E's value is conditional on these exclusions being
        // in place — which is the order this ticket lands in.
        Some(Arc::new(compose_tenant_hub(
            &Arc::clone(&host_activations),
            tenant_surface,
        )))
    });

    let mount = TenantMount::new(
        Arc::new(TenantMountGate::new(Arc::new(
            plexus_auth_core::ClaimTenantResolver::new(),
        ))),
        factory,
        // The shape shared by every tenant, built from the same composition
        // its factory uses and bound to no tenant.
        compose_tenant_hub(&activations, tenant_surface).connectome(),
    );

    let hub = Arc::new(compose_host_hub(&activations).register(mount));

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
    orcha_for_recovery.recover_running_graphs().await;

    hub
}

// ============================================================================
// PLX-127 — the two compositions
// ============================================================================

/// Every activation the substrate builds, held once so that the host hub and a
/// tenant hub can be two *compositions* of the same objects rather than two
/// construction paths that could drift apart.
pub struct SubstrateActivations {
    arbor: Arbor,
    cone: Cone,
    claudecode: ClaudeCode,
    mustache: Mustache,
    changelog: Changelog,
    loopback: ClaudeCodeLoopback,
    orcha: Orcha,
    registry: Registry,
    lattice: Lattice,
}

/// The host surface — everything, exactly as before PLX-127.
///
/// The `tenants` mount is registered by `build_plexus_rpc` on top of this, not
/// here, so that `compose_tenant_hub` can never accidentally nest a mount
/// inside a mount.
pub fn compose_host_hub(a: &SubstrateActivations) -> DynamicHub {
    let hub = DynamicHub::new("substrate")
        .register(Health::new())
        .register(Echo::new())
        .register(Bash::new());

    // Chaos activation is feature-gated — off by default because it pulls
    // in libc + narrow unsafe signal primitives.
    #[cfg(feature = "chaos")]
    let hub = hub.register(Chaos::new(a.lattice.storage()));

    let hub = hub
        .register(a.arbor.clone())
        .register(a.cone.clone())
        .register(a.claudecode.clone())
        .register(a.mustache.clone())
        .register(a.changelog.clone())
        .register(a.loopback.clone())
        .register(a.orcha.clone())
        // .register(jsexec)  // temporarily disabled
        .register(a.registry.clone())
        .register(a.lattice.clone())
        .register(Interactive::new()) // Bidirectional demo activation
        .register(Solar::new());

    // PLX-142: hand the hub each activation's macro-built ActivationIr, so
    // `substrate.connectome` serves the real tree rather than a document
    // lifted from the legacy schema. Must run after every `register`.
    crate::activations::connectome::declare_connectomes(hub)
}

/// The tenant surface — the host surface minus `TENANT_EXCLUDED_ACTIVATIONS`,
/// and minus `claudecode` until PLX-151 wires the sandbox.
///
/// **This function is the enforcement.** Not a filter applied to a built hub,
/// not a deny list consulted at dispatch: the excluded activations are never
/// registered, so they are absent from `list_methods`, from `plugin_schema`,
/// and from `connectome` — the three caller-independent surfaces — as well as
/// unreachable at dispatch. `the_excluded_surface_is_absent_not_denied` asserts
/// all four.
pub fn compose_tenant_hub(a: &SubstrateActivations, surface: TenantSurface) -> DynamicHub {
    // EXCLUDED: `bash` (PLX-130 A1) — no `.register(Bash::new())` here.
    // EXCLUDED: `chaos` (A5) — no `#[cfg(feature = "chaos")]` arm here either,
    //           so the cargo feature cannot re-admit it into a tenant.
    // EXCLUDED: `orcha` (A3/A4/B1) — no `.register(a.orcha)`.
    let hub = DynamicHub::new("substrate")
        .register(Health::new())
        .register(Echo::new())
        .register(a.arbor.clone())
        .register(a.cone.clone())
        .register(a.mustache.clone())
        .register(a.changelog.clone())
        .register(a.loopback.clone())
        .register(a.registry.clone())
        .register(a.lattice.clone())
        .register(Interactive::new())
        .register(Solar::new());

    // The one that is not excludable, because it is the product (PLX-144).
    let hub = if surface.claudecode_is_sandboxed {
        hub.register(a.claudecode.clone())
    } else {
        hub
    };

    crate::activations::connectome::declare_connectomes(hub)
}
