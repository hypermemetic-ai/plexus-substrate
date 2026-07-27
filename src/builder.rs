//! Plexus RPC builder - constructs a fully configured `DynamicHub` instance
//!
//! This module is used by both the main binary and examples.

use std::collections::BTreeMap;
use std::sync::Arc;

use plexus_auth_core::tenant::TenantId;
use plexus_sandbox::docker::{DockerConfig, DockerSandbox};
use plexus_sandbox::Sandbox;

use crate::activations::claudecode::Confinement;
use crate::tenancy::TenantAdmission;

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
    build_plexus_rpc_with_tenancy(None).await
}

/// PLX-151 — everything a deployment needs to run `claudecode` for a tenant.
///
/// Built once, at startup, by [`TenantExecution::detect`]. Holding one is what
/// lets `build_plexus_rpc_with_tenancy` flip
/// [`TenantSurface::claudecode_is_sandboxed`] to `true`: the flag and the
/// capability are the same fact, so a deployment cannot claim to sandbox
/// `claudecode` without owning a sandbox.
#[derive(Clone)]
pub struct TenantExecution {
    sandbox: Arc<dyn Sandbox>,
    admission: Arc<TenantAdmission>,
    env: BTreeMap<String, String>,
    cli: String,
}

impl std::fmt::Debug for TenantExecution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantExecution")
            .field("runtime", &self.sandbox.runtime())
            .field("admission", &self.admission)
            .field("cli", &self.cli)
            .field("env_keys", &self.env.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl TenantExecution {
    /// Detect the container runtime **at startup** and refuse to start without
    /// one (PLX-151 c4, inheriting PLX-144 c4).
    ///
    /// `DockerSandbox::detect` is the only constructor Docker has and it is
    /// fallible: it runs `docker version --format {{.Server.Version}}`, which
    /// distinguishes a missing client from an unreachable daemon. Whatever it
    /// says is what an operator sees, prefixed with what substrate was trying
    /// to do — so "a substrate that cannot sandbox says so at startup, not at
    /// the first chat turn."
    ///
    /// # Errors
    ///
    /// A message ready to print: the runtime, what was tried, the hint, and
    /// the consequence of the failure.
    pub async fn detect(
        image: impl Into<String>,
        admission: Arc<TenantAdmission>,
    ) -> Result<Self, String> {
        let image = image.into();
        let sandbox = DockerSandbox::detect(DockerConfig::new(image.clone()))
            .await
            .map_err(|e| {
                format!(
                    "cannot start a tenanted substrate: {e}\n\
                     tenant `claudecode` runs inside a per-tenant container (PLX-144/PLX-151), \
                     so a substrate that cannot sandbox must not serve tenants.\n\
                     image that would have been used: {image}\n\
                     to run single-tenant instead, start without tenancy — the untenanted \
                     path does not require a container runtime."
                )
            })?;
        Ok(Self::with_sandbox(Arc::new(sandbox), admission))
    }

    /// Use an already-constructed backend. The runtime-agnostic seam: PLX-144
    /// built `Sandbox` as a trait so podman and bubblewrap can be dropped in
    /// here without substrate changing.
    #[must_use]
    pub fn with_sandbox(sandbox: Arc<dyn Sandbox>, admission: Arc<TenantAdmission>) -> Self {
        Self {
            sandbox,
            admission,
            env: BTreeMap::new(),
            cli: "claude".to_owned(),
        }
    }

    /// Set one environment variable for every tenant confinement.
    ///
    /// Nothing is inherited from the substrate process — PLX-130 finding C is
    /// that substrate's launchers never scrubbed the environment, so
    /// `ANTHROPIC_API_KEY` and `PLEXUS_MCP_URL` were readable by `env` inside a
    /// tenant's shell. A credential a tenant's CLI needs must be named here,
    /// deliberately.
    #[must_use]
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    /// Name the CLI inside the image. Defaults to `claude` on its `PATH`.
    #[must_use]
    pub fn with_cli(mut self, cli: impl Into<String>) -> Self {
        self.cli = cli.into();
        self
    }

    /// The confinement for one tenant.
    #[must_use]
    pub fn confinement_for(&self, tenant: &TenantId) -> Confinement {
        let mut c = Confinement::new(
            Arc::clone(&self.sandbox),
            Arc::clone(&self.admission),
            tenant.clone(),
        )
        .with_cli(self.cli.clone());
        for (key, value) in &self.env {
            c = c.with_env(key, value);
        }
        c
    }
}

/// Build the hub, optionally with per-tenant confined execution.
///
/// `None` is the untenanted deployment: `claudecode` is absent from the tenant
/// mount exactly as PLX-127 shipped it, and the host surface is unchanged. See
/// `ClaudeCodeExecutor::spawn_on_host_unconfined` for what the host path gets.
pub async fn build_plexus_rpc_with_tenancy(
    tenancy: Option<TenantExecution>,
) -> Arc<DynamicHub> {
    let (activations, orcha_for_recovery) = build_activations().await;
    let changelog = activations.changelog.clone();
    let activations = Arc::new(activations);

    // PLX-127: the host hub is what it always was. The tenant hub is a
    // DIFFERENT composition of the SAME activations, and the difference is the
    // exclusion list — see `TENANT_EXCLUDED_ACTIVATIONS`.
    //
    // PLX-151: the flag and the capability are one fact. `claudecode` is in a
    // tenant's surface exactly when this deployment has a sandbox to put it in.
    let host_activations = Arc::clone(&activations);
    let tenant_surface = TenantSurface {
        claudecode_is_sandboxed: tenancy.is_some(),
    };
    let tenancy_for_factory = tenancy.clone();
    let factory: TenantSubtreeFactory = Arc::new(move |admitted: &AdmittedTenant| {
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
        //
        // PLX-151: the confinement is per tenant, and it is built from the
        // ADMITTED id — `admitted.id()`, which only `TenantMountGate::admit`
        // can have produced — never from a segment the caller spelled.
        let confinement = match tenancy_for_factory.as_ref() {
            Some(t) => Some(t.confinement_for(admitted.id())),
            // A tenanted surface with no sandbox is refused outright rather
            // than served with an unconfined `claudecode` in it. `None` here is
            // the `Option` PLX-127 put on this factory "precisely so a composer
            // can refuse".
            None if tenant_surface.claudecode_is_sandboxed => return None,
            None => None,
        };

        Some(Arc::new(compose_tenant_hub_confined(
            &Arc::clone(&host_activations),
            tenant_surface,
            confinement,
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
        // PLX-157 — RFC 002 §3.3 / §7.6 root facts. `backend_name` is the name
        // this backend registers under and answers `_info` with, so a client
        // that has the document no longer has to probe for it; `respond_method`
        // is the reply channel the runtime actually registers (`{ns}.respond`),
        // which is what §7.6 asks a consumer to be able to see before invoking.
        .with_backend_name("substrate")
        .with_respond_method("substrate.respond")
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

/// The tenant surface as a **shape**, bound to no tenant.
///
/// This is what `TenantMount` renders as its connectome template and what
/// PLX-127's composition tests assert on. It is *not* what a tenant dispatches
/// into: a real tenant's hub comes from [`compose_tenant_hub_confined`], which
/// takes the per-tenant [`Confinement`] this one has no way to know.
///
/// **This function is the enforcement.** Not a filter applied to a built hub,
/// not a deny list consulted at dispatch: the excluded activations are never
/// registered, so they are absent from `list_methods`, from `plugin_schema`,
/// and from `connectome` — the three caller-independent surfaces — as well as
/// unreachable at dispatch. `the_excluded_surface_is_absent_not_denied` asserts
/// all four.
pub fn compose_tenant_hub(a: &SubstrateActivations, surface: TenantSurface) -> DynamicHub {
    compose_tenant_hub_confined(a, surface, None)
}

/// The tenant surface for **one** tenant, with `claudecode` confined to it.
///
/// PLX-151. The `claudecode` registered here is `a.claudecode.confined_to(c)` —
/// the same activation and the same storage, launching inside that tenant's
/// `plexus_sandbox::Sandbox` instead of on the host.
///
/// # The combination that must never reach a tenant
///
/// `claudecode_is_sandboxed == true` with `confinement == None` composes an
/// **unconfined** `claudecode`. That composition exists for exactly one
/// purpose — the connectome *template*, which is a shape and is never
/// dispatched into — and it is unreachable as a tenant's hub, because
/// `build_plexus_rpc_with_tenancy`'s factory has a `Confinement` for every
/// tenant it admits and returns `None` (refusing the mount) when it does not.
/// `a_tenant_hub_is_never_composed_with_an_unconfined_claudecode` pins that.
pub fn compose_tenant_hub_confined(
    a: &SubstrateActivations,
    surface: TenantSurface,
    confinement: Option<Confinement>,
) -> DynamicHub {
    // EXCLUDED: `bash` (PLX-130 A1) — no `.register(Bash::new())` here.
    // EXCLUDED: `chaos` (A5) — no `#[cfg(feature = "chaos")]` arm here either,
    //           so the cargo feature cannot re-admit it into a tenant.
    // EXCLUDED: `orcha` (A3/A4/B1) — no `.register(a.orcha)`.
    let hub = DynamicHub::new("substrate")
        // PLX-157 — same root facts as the host surface. A tenant's document
        // must answer §3.3/§7.6 too; the reply channel is namespaced on the
        // hub root, which is `substrate` here as well.
        .with_backend_name("substrate")
        .with_respond_method("substrate.respond")
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

    // The one that is not excludable, because it is the product (PLX-144),
    // and — since PLX-151 — the one that is confined rather than omitted.
    let hub = match (surface.claudecode_is_sandboxed, confinement) {
        // A real tenant: the activation, launching inside that tenant's
        // sandbox. This is the composition a tenant actually dispatches into.
        (true, Some(c)) => hub.register(a.claudecode.confined_to(c)),
        // The shape, bound to no tenant: the namespace is present so the
        // connectome template and PLX-127's `a_sandboxed_deployment_gets_
        // claudecode_back` see what a tenant will see. Nothing dispatches here.
        (true, None) => hub.register(a.claudecode.clone()),
        // Not a sandboxing deployment: absent, exactly as PLX-127 shipped it.
        (false, _) => hub,
    };

    crate::activations::connectome::declare_connectomes(hub)
}
