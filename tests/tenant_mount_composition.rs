//! PLX-127 (M4·C) — the exclusion list, and that it is enforced by **absence**
//! rather than denial.
//!
//! PLX-130's finding is the whole reason this file asserts what it asserts:
//! `list_methods` and `plugin_schema` take **no auth context**, so a scope gate
//! denies without hiding. Mount composition is therefore the first layer, and
//! the test of a first layer is that the excluded surface is *not there* — in
//! every caller-independent rendering, not merely refused at dispatch.
//!
//! These tests build the real substrate activation set, so they redirect
//! `HOME` to a temp dir first (`activation_db_path` reads `HOME`). `HOME` is
//! process-global, so this file is deliberately ONE test binary with the env
//! set once, up front, before any storage is opened.

use std::sync::{Arc, Once};

use plexus_substrate::{
    build_activations, compose_host_hub, compose_tenant_hub, SubstrateActivations, TenantSurface,
    TENANT_EXCLUDED_ACTIVATIONS,
};

static REDIRECT_HOME: Once = Once::new();

/// Point every `activation_db_path` at a scratch directory. Leaked on purpose:
/// the directory must outlive every storage handle in the process.
fn redirect_home() {
    REDIRECT_HOME.call_once(|| {
        let dir = Box::leak(Box::new(
            tempfile::tempdir().expect("temp dir for substrate storages"),
        ));
        std::env::set_var("HOME", dir.path());
    });
}

/// Substrate's storage initialisation is NOT safe to run concurrently against
/// one `HOME`: `OrchaStorage::new` performs `ALTER TABLE … ADD COLUMN` without
/// an existence check, so two initialisations race and one dies with
/// `duplicate column name: agent_mode`. That is a pre-existing substrate
/// property, not something this ticket introduced — `cargo test` simply never
/// had two callers of the builder in one binary before.
///
/// So the activation set is built exactly once and shared, and anything else
/// that opens the same files takes `STORAGE_INIT` for the duration.
static ACTIVATIONS: tokio::sync::OnceCell<Arc<SubstrateActivations>> =
    tokio::sync::OnceCell::const_new();
static STORAGE_INIT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn activations() -> Arc<SubstrateActivations> {
    ACTIVATIONS
        .get_or_init(|| async {
            redirect_home();
            let _guard = STORAGE_INIT.lock().await;
            Arc::new(build_activations().await.0)
        })
        .await
        .clone()
}

/// The namespaces a hub advertises, from the caller-independent surface.
fn advertised(hub: &plexus_core::plexus::DynamicHub) -> Vec<String> {
    hub.list_activations_info()
        .into_iter()
        .map(|i| i.namespace)
        .collect()
}

// ============================================================================
// The list is real, and it names real things
// ============================================================================

/// A deny list that names something nobody registers is decoration. This is the
/// drift guard: every excluded namespace must actually exist on the host.
#[tokio::test]
async fn the_exclusion_list_names_only_activations_the_host_really_has() {
    let a = activations().await;
    let host = advertised(&compose_host_hub(&a));

    for excluded in TENANT_EXCLUDED_ACTIVATIONS {
        // `chaos` is `#[cfg(feature = "chaos")]` and off by default, which is
        // itself the A5 disposition ("already excluded; make it permanent").
        if *excluded == "chaos" && !cfg!(feature = "chaos") {
            continue;
        }
        assert!(
            host.contains(&(*excluded).to_string()),
            "the exclusion list names {excluded:?}, which the host hub does not register — \
             the list has drifted from the surface it excludes. Host: {host:?}"
        );
    }
}

// ============================================================================
// Absence, on all four surfaces
// ============================================================================

/// The criterion, stated four ways. Three of these are the caller-independent
/// renderings PLX-130 said a scope gate cannot fix; the fourth is dispatch.
#[tokio::test]
async fn the_excluded_surface_is_absent_not_denied() {
    let a = activations().await;
    let tenant = compose_tenant_hub(&a, TenantSurface::default());

    let namespaces = advertised(&tenant);
    let methods = tenant.list_methods();
    let schemas = serde_json::to_string(&tenant.list_plugin_schemas()).expect("schemas");
    let connectome = serde_json::to_string(&tenant.connectome()).expect("connectome");

    for excluded in TENANT_EXCLUDED_ACTIVATIONS {
        // 1. list_activations_info
        assert!(
            !namespaces.contains(&(*excluded).to_string()),
            "{excluded} is advertised on a tenant hub: {namespaces:?}"
        );
        // 2. list_methods
        assert!(
            !methods.iter().any(|m| m.starts_with(&format!("{excluded}."))),
            "a {excluded}.* method is advertised on a tenant hub"
        );
        // 3. plugin_schema
        assert!(
            !schemas.contains(&format!("\"namespace\":\"{excluded}\"")),
            "{excluded} appears in a tenant hub's plugin schemas"
        );
        // 4. connectome
        assert!(
            !connectome.contains(&format!("\"namespace\":\"{excluded}\"")),
            "{excluded} appears in a tenant hub's Connectome"
        );
    }

    // NON-VACUITY: the same four surfaces DO carry the activations that are
    // not excluded. Without this, all four assertions above would pass on an
    // empty hub.
    for kept in ["echo", "arbor", "cone", "lattice"] {
        assert!(
            namespaces.contains(&kept.to_string()),
            "{kept} should be on a tenant hub: {namespaces:?}"
        );
        assert!(methods.iter().any(|m| m.starts_with(&format!("{kept}."))));
        assert!(connectome.contains(&format!("\"namespace\":\"{kept}\"")));
    }
}

/// And the excluded surface really is on the host — otherwise the tenant hub's
/// emptiness would be a build accident rather than a composition decision.
#[tokio::test]
async fn the_host_surface_is_unchanged() {
    let a = activations().await;
    let host = compose_host_hub(&a);
    let methods = host.list_methods();

    assert!(
        methods.iter().any(|m| m.starts_with("bash.")),
        "the host lost bash — this ticket must not change the host surface"
    );
    assert!(methods.iter().any(|m| m.starts_with("orcha.")));
    assert!(methods.iter().any(|m| m.starts_with("claudecode.")));

    // Every namespace the tenant hub has, the host has too: the tenant surface
    // is a strict subset, never a divergent composition.
    let tenant = advertised(&compose_tenant_hub(&a, TenantSurface::default()));
    let host_ns = advertised(&host);
    for ns in &tenant {
        assert!(
            host_ns.contains(ns),
            "the tenant hub advertises {ns}, which the host does not — the two \
             compositions have drifted"
        );
    }
}

// ============================================================================
// claudecode — the one that is not excludable
// ============================================================================

/// PLX-144's operator decision is that tenancy ships WITH claudecode, behind a
/// per-tenant execution sandbox, and PLX-151 wires it. Until it does, shipping
/// the activation into a tenant mount would be shipping a known cross-tenant
/// read, which M4 decision gate 2 forbids.
///
/// So its absence today is a *switch in a named position*, and both positions
/// are pinned so PLX-151 has one line to flip and a test that proves the flip
/// worked.
#[tokio::test]
async fn claudecode_is_absent_until_the_sandbox_is_wired() {
    let a = activations().await;
    assert!(!TenantSurface::default().claudecode_is_sandboxed);

    let tenant = compose_tenant_hub(&a, TenantSurface::default());
    assert!(
        !advertised(&tenant).contains(&"claudecode".to_string()),
        "claudecode is mounted under a tenant without a sandbox (PLX-151 has not landed)"
    );
}

#[tokio::test]
async fn a_sandboxed_deployment_gets_claudecode_back() {
    let a = activations().await;
    let tenant = compose_tenant_hub(
        &a,
        TenantSurface {
            claudecode_is_sandboxed: true,
        },
    );
    assert!(
        advertised(&tenant).contains(&"claudecode".to_string()),
        "claudecode must return once it launches inside a Sandbox — it is not excludable"
    );

    // Even then the rest of the list stays excluded: the sandbox is the answer
    // to A2 only, not to A1/A3/A4/A5.
    let namespaces = advertised(&tenant);
    for excluded in TENANT_EXCLUDED_ACTIVATIONS {
        assert!(
            !namespaces.contains(&(*excluded).to_string()),
            "{excluded} came back with claudecode"
        );
    }
}

/// The `chaos` cargo feature must not be able to re-admit chaos into a tenant.
/// `compose_tenant_hub` has no `#[cfg(feature = "chaos")]` arm at all, so this
/// holds in both feature configurations.
#[tokio::test]
async fn the_chaos_feature_cannot_re_admit_chaos_to_a_tenant() {
    let a = activations().await;
    let tenant = advertised(&compose_tenant_hub(&a, TenantSurface::default()));
    assert!(!tenant.contains(&"chaos".to_string()));
}

// ============================================================================
// End to end, through the real composition root
// ============================================================================

fn caller_of(tenant: &str) -> plexus_core::plexus::AuthContext {
    plexus_core::plexus::AuthContext::new(
        format!("user-of-{tenant}"),
        "sess".to_string(),
        vec!["user".to_string()],
        serde_json::json!({ "org_id": tenant }),
    )
}

/// The whole thing, wired: `build_plexus_rpc()`'s hub, a real caller, real
/// dispatch.
///
/// The sharpest assertion here is the middle one. Tenant B is **correctly
/// admitted** — the gate said yes — and `bash` is *still* unreachable, because
/// it was never registered on the composition B descends into. That is the
/// difference between a deny list and mount composition, and it is why PLX-130
/// put composition first.
#[tokio::test]
async fn end_to_end_a_tenant_reaches_its_own_mount_and_nothing_else() {
    redirect_home();
    let hub = {
        let _guard = STORAGE_INIT.lock().await;
        plexus_substrate::build_plexus_rpc().await
    };
    let b = caller_of("tenant-b");

    // 1. B descends into its own mount and reaches a kept activation.
    assert!(
        hub.route("tenants.tenant-b.echo.once", serde_json::json!({"message": "hi"}), Some(&b))
            .await
            .is_ok(),
        "a tenant must reach its own mount"
    );

    // 2. B is admitted, and bash is STILL not there. Absence, not denial.
    assert!(
        hub.route("tenants.tenant-b.bash.execute", serde_json::json!({"command": "id"}), Some(&b))
            .await
            .is_err(),
        "bash is reachable from inside a tenant mount"
    );
    for excluded in TENANT_EXCLUDED_ACTIVATIONS {
        assert!(
            hub.route(
                &format!("tenants.tenant-b.{excluded}.anything"),
                serde_json::json!({}),
                Some(&b)
            )
            .await
            .is_err(),
            "{excluded} is reachable from inside a tenant mount"
        );
    }

    // 3. B cannot reach tenant A at all.
    assert!(
        hub.route("tenants.tenant-a.echo.once", serde_json::json!({"message": "hi"}), Some(&b))
            .await
            .is_err(),
        "tenant B reached tenant A's mount"
    );

    // 4. And the host's own surface still has bash — this ticket did not
    //    remove it, it composed a second hub without it.
    assert!(hub.list_methods().iter().any(|m| m.starts_with("bash.")));

    // 5. No tenant identity is in the caller-independent rendering.
    let connectome = serde_json::to_string(&hub.connectome()).expect("connectome");
    assert!(!connectome.contains("tenant-a") && !connectome.contains("tenant-b"));
    assert!(connectome.contains("tenants/{id}"), "the mount is not rendered");
}
