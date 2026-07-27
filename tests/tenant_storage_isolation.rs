//! PLX-128 (M4·D) and PLX-129 (M4·E) — per-tenant instances, per-tenant files.
//!
//! # The bar this file is written to
//!
//! PLX-129 c1 asks for isolation proved **by file separation, demonstrated** —
//! not by asserting that a predicate is present. So the central test does not
//! stop at "tenant B's read returned not-found". It goes to the filesystem,
//! finds the two `templates.db` files, and asserts that tenant A's secret
//! **bytes are in A's file and are not in B's**. A query that cannot cross
//! beats one that must not, and the way you show a query cannot cross is to
//! show it is looking at a different file.
//!
//! # Non-vacuity, permanently
//!
//! A separation test passes trivially if the write never happened, if the read
//! never works, or if the two tenants were never really two. Three controls
//! run on every suite run:
//!
//! 1. **Liveness** — A reads its own template back and gets the secret. If the
//!    write silently failed, this fires.
//! 2. **The leak probe** — the identical read, against two activation sets
//!    built over *one* scope, and it MUST leak. This is the mutation PLX-144
//!    and PLX-151 both used, made permanent rather than one-off: if a future
//!    refactor makes the two scopes resolve to the same directory, the
//!    separation test goes red and this one stays green, which is the pair
//!    that tells you which of the two broke.
//! 3. **Both tenants live before the assertion** — PLX-127's shape. A
//!    memoising implementation that only ever built one hub would have two to
//!    confuse.
//!
//! # `HOME` is process-global
//!
//! `activation_db_path` reads it, so — exactly as PLX-127's composition tests
//! do — this is ONE test binary that redirects `HOME` once, up front, before
//! any storage is opened.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};

use plexus_auth_core::tenant::TenantId;
use plexus_idp::store::IdentityStore;
use plexus_substrate::activations::storage::StorageScope;
use plexus_substrate::tenancy::{AdmissionRefused, TenantAdmission};
use plexus_substrate::{build_activations_in, compose_tenant_hub_confined, TenantSurface};
use serde_json::json;

static REDIRECT_HOME: Once = Once::new();

/// Point the *host* storage root at a scratch directory. Leaked on purpose:
/// it must outlive every storage handle in the process.
fn redirect_home() -> PathBuf {
    static HOME: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    REDIRECT_HOME.call_once(|| {
        let dir = Box::leak(Box::new(
            tempfile::tempdir().expect("temp dir for host storage"),
        ));
        std::env::set_var("HOME", dir.path());
        let _ = HOME.set(dir.path().to_path_buf());
    });
    HOME.get().expect("HOME set").clone()
}

/// Substrate's storage initialisation is not safe to run concurrently against
/// one directory — `OrchaStorage::new` does an unguarded
/// `ALTER TABLE … ADD COLUMN`. Inherited from PLX-127's file, same reason.
static STORAGE_INIT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn tid(s: &str) -> TenantId {
    TenantId::try_new(s).expect("tenant id")
}

/// Two minted, active tenants and the admission that proves them.
struct Scene {
    _dir: tempfile::TempDir,
    roots: PathBuf,
    store: Arc<IdentityStore>,
    admission: Arc<TenantAdmission>,
}

impl Scene {
    async fn new() -> Self {
        redirect_home();
        let dir = tempfile::tempdir().expect("scene dir");
        let roots = dir.path().join("roots");
        let store = Arc::new(
            IdentityStore::open(
                dir.path()
                    .join("identity.db")
                    .to_str()
                    .expect("utf-8 db path"),
            )
            .await
            .expect("identity store"),
        );
        store.mint_tenant("tenant-a", "Tenant A").await.expect("mint a");
        store.mint_tenant("tenant-b", "Tenant B").await.expect("mint b");
        Self {
            admission: Arc::new(TenantAdmission::new(Arc::clone(&store), roots.clone())),
            store,
            roots,
            _dir: dir,
        }
    }

    async fn scope(&self, tenant: &str) -> StorageScope {
        let root = self
            .admission
            .tenant_storage(&tid(tenant))
            .await
            .expect("an active, minted tenant resolves a storage root");
        StorageScope::for_tenant(&root)
    }
}

/// The `plugin_id` every template in this file is filed under. A constant, so
/// A and B are genuinely asking the same question.
const PLUGIN: &str = "6f1d4a1e-0000-4000-8000-00000000ffff";
const SECRET: &str = "TENANT_A_TEMPLATE_SECRET_c9b1f0";

/// Build a tenant hub over `scope`, exactly as the factory does.
async fn tenant_hub(scope: &StorageScope) -> plexus_core::plexus::DynamicHub {
    let _guard = STORAGE_INIT.lock().await;
    let (activations, _orcha) = build_activations_in(scope).await;
    compose_tenant_hub_confined(&activations, TenantSurface::default(), None)
}

/// Every `.db` file under `dir`, recursively.
fn databases_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|e| e.to_str()) == Some("db") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

fn file_contains(path: &Path, needle: &str) -> bool {
    let Ok(bytes) = std::fs::read(path) else {
        return false;
    };
    bytes
        .windows(needle.len())
        .any(|w| w == needle.as_bytes())
}

// ═══════════════════════════════════════════════════════════════════════════
// PLX-129 c1 — the query cannot cross, and the reason is on the filesystem
// ═══════════════════════════════════════════════════════════════════════════

/// **THE test.** Tenant A writes a template; tenant B asks the same question
/// and does not get it — and the reason is that B's connection is open on a
/// different file, which is asserted on the bytes rather than inferred.
#[tokio::test]
async fn c1_a_writes_and_b_cannot_read_it_and_the_files_are_separate() {
    let scene = Scene::new().await;

    // Both tenants live BEFORE any assertion (PLX-127's shape): an
    // implementation with only one real instance would have nothing to confuse.
    let a_scope = scene.scope("tenant-a").await;
    let b_scope = scene.scope("tenant-b").await;
    let a = tenant_hub(&a_scope).await;
    let b = tenant_hub(&b_scope).await;

    // ── A writes. ──────────────────────────────────────────────────────────
    //
    // NOTE, learned the hard way and worth keeping: `route` returns a LAZY
    // stream. Dropping it without draining runs nothing, and a separation test
    // whose write never happened passes for the wrong reason. `collect` drives
    // it, and the liveness control below is what caught this.
    let wrote = collect(
        a.route(
            "mustache.register_template",
            json!({
                "plugin_id": PLUGIN,
                "method": "greet",
                "name": "default",
                "template": SECRET,
            }),
            None,
        )
        .await
        .expect("tenant A registers its own template"),
    )
    .await;
    assert!(!wrote.contains("\"kind\": String(\"failed\")"), "the write failed: {wrote}");

    // ── Control 1: liveness. A reads its own back. ─────────────────────────
    let a_read = a
        .route(
            "mustache.get_template",
            json!({ "plugin_id": PLUGIN, "method": "greet", "name": "default" }),
            None,
        )
        .await
        .expect("tenant A reads its own template");
    let a_body = collect(a_read).await;
    assert!(
        a_body.contains(SECRET),
        "the write did not land, so the separation below would prove nothing.\n{a_body}"
    );

    // ── B asks the identical question. ─────────────────────────────────────
    let b_read = b
        .route(
            "mustache.get_template",
            json!({ "plugin_id": PLUGIN, "method": "greet", "name": "default" }),
            None,
        )
        .await;
    let b_body = match b_read {
        Ok(stream) => collect(stream).await,
        Err(e) => e.to_string(),
    };
    assert!(
        !b_body.contains(SECRET),
        "tenant B read tenant A's template.\n{b_body}"
    );

    // …and enumeration does not disclose it either.
    let b_list = b
        .route("mustache.list_templates", json!({ "plugin_id": PLUGIN }), None)
        .await
        .expect("list is a legal call for B");
    let b_list = collect(b_list).await;
    assert!(
        !b_list.contains(SECRET) && !b_list.contains("greet"),
        "tenant B enumerated tenant A's templates.\n{b_list}"
    );

    // ── The demonstration: it is a different FILE. ─────────────────────────
    let a_db = a_scope.db_path("mustache", "templates.db");
    let b_db = b_scope.db_path("mustache", "templates.db");
    assert_ne!(a_db, b_db, "both tenants resolved to one path");
    assert!(a_db.exists() && b_db.exists(), "both files must exist");
    assert!(
        file_contains(&a_db, SECRET),
        "tenant A's secret is not in tenant A's own file at {}; the test is \
         looking at the wrong place and its negative half means nothing",
        a_db.display()
    );
    assert!(
        !file_contains(&b_db, SECRET),
        "tenant A's secret bytes are present in tenant B's database file at {}",
        b_db.display()
    );

    // Every one of B's files is under B's root, and none of A's is.
    let a_root = scene.roots.join("tenant-a").canonicalize().expect("a root");
    let b_root = scene.roots.join("tenant-b").canonicalize().expect("b root");
    for db in databases_under(&b_root) {
        assert!(
            !file_contains(&db, SECRET),
            "tenant A's secret leaked into {}",
            db.display()
        );
    }
    assert!(
        databases_under(&a_root).iter().any(|p| p == &a_db),
        "tenant A's mustache database is not inside tenant A's root"
    );
}

/// **Control 2: the permanent leak probe.**
///
/// The identical read, with the two activation sets built over **one** scope.
/// It MUST leak. If it ever stops leaking, the negative test above has become
/// vacuous — the read stopped working — and this is what says so.
///
/// This is PLX-144's mutation discipline made permanent: a test that cannot
/// observe the thing it asserts is worse than no test.
#[tokio::test]
async fn c1_probe_the_identical_read_leaks_when_the_scope_is_shared() {
    let scene = Scene::new().await;
    let shared = scene.scope("tenant-a").await;

    let writer = tenant_hub(&shared).await;
    let reader = tenant_hub(&shared).await;

    let _wrote = collect(
        writer
            .route(
                "mustache.register_template",
                json!({
                    "plugin_id": PLUGIN,
                    "method": "probe",
                    "name": "default",
                    "template": SECRET,
                }),
                None,
            )
            .await
            .expect("write"),
    )
    .await;

    let leaked = reader
        .route(
            "mustache.get_template",
            json!({ "plugin_id": PLUGIN, "method": "probe", "name": "default" }),
            None,
        )
        .await
        .expect("a second hub over the SAME scope must read the first one's write");
    let leaked = collect(leaked).await;
    assert!(
        leaked.contains(SECRET),
        "the probe did not leak, which means the negative test above cannot \
         detect a leak either — the read path is broken, not the isolation.\n{leaked}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// PLX-128 c3 — the instances are genuinely separate
// ═══════════════════════════════════════════════════════════════════════════

/// Two tenants, two storage roots, two of every database, and neither root
/// contains the other.
#[tokio::test]
async fn c3_two_tenants_get_distinct_roots_and_distinct_databases() {
    let scene = Scene::new().await;
    let a = scene.scope("tenant-a").await;
    let b = scene.scope("tenant-b").await;

    assert_eq!(a.tenant().map(TenantId::as_str), Some("tenant-a"));
    assert_eq!(b.tenant().map(TenantId::as_str), Some("tenant-b"));
    assert_ne!(a.root(), b.root());
    assert!(
        !a.root().starts_with(b.root()) && !b.root().starts_with(a.root()),
        "one tenant's storage root contains the other's: {} / {}",
        a.root().display(),
        b.root().display()
    );

    // Build both sets, then assert every single database differs. This is the
    // enumeration PLX-129 c4 asks for, executed rather than written down: a
    // tenth activation that gains storage and is not scoped shows up here as
    // an equal pair of paths.
    let _a_set = tenant_hub(&a).await;
    let _b_set = tenant_hub(&b).await;

    let a_dbs = databases_under(a.root());
    let b_dbs = databases_under(b.root());
    assert!(!a_dbs.is_empty(), "tenant A opened no databases at all");
    assert_eq!(
        a_dbs.len(),
        b_dbs.len(),
        "the two tenants opened different numbers of databases:\nA: {a_dbs:#?}\nB: {b_dbs:#?}"
    );
    for db in &a_dbs {
        assert!(!b_dbs.contains(db), "{} is shared by both tenants", db.display());
    }

    // The nine activations that own storage, each named. `registry` is here
    // because its default path is outside `~/.plexus` entirely and an audit
    // that greps for `activation_db_path` misses it.
    for (activation, file) in [
        ("arbor", "arbor.db"),
        ("cone", "cones.db"),
        ("claudecode", "claudecode.db"),
        ("claudecode_loopback", "loopback.db"),
        ("mustache", "templates.db"),
        ("changelog", "changelog.db"),
        ("lattice", "lattice.db"),
        ("orcha", "orcha.db"),
        ("pm", "pm.db"),
        ("registry", "registry.db"),
    ] {
        let pa = a.db_path(activation, file);
        let pb = b.db_path(activation, file);
        assert_ne!(pa, pb, "{activation}/{file} resolves to one path for both tenants");
        assert!(
            pa.starts_with(a.root()) && pb.starts_with(b.root()),
            "{activation}/{file} escaped its tenant root"
        );
    }

    // And the session transcript directory, which is not a database and is the
    // surface an unchecked join reached.
    assert_ne!(a.claude_sessions_root(), b.claude_sessions_root());
    assert!(a.claude_sessions_root().starts_with(a.root()));
}

/// The host scope is untouched by any of this: same directory it always used,
/// and no tenant identity in it.
#[tokio::test]
async fn the_host_scope_is_where_it_always_was() {
    let home = redirect_home();
    let host = StorageScope::host();
    assert!(host.tenant().is_none());
    assert_eq!(
        host.db_path("arbor", "arbor.db"),
        home.join(".plexus/substrate/activations/arbor/arbor.db")
    );
    // `registry` and `claudecode`'s sessions keep their own, non-`~/.plexus`
    // homes on the host — moving them would relocate a live deployment's data.
    assert!(!host
        .registry_config()
        .db_path
        .starts_with(home.join(".plexus")));
    assert_eq!(
        host.claude_sessions_root(),
        dirs::home_dir().expect("home").join(".claude/projects")
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// The join sites, and the two traps PLX-130 measured
// ═══════════════════════════════════════════════════════════════════════════

/// `TenantId::try_new("../tenant-a")` is **valid**. The new join site checks
/// for itself, in the hard form: the tenant is minted first, so the record
/// really exists and really is active, and the join site's own check is the
/// only thing standing between a traversing id and a joined path.
#[tokio::test]
async fn a_traversing_tenant_id_is_refused_at_the_storage_join_site() {
    let scene = Scene::new().await;
    // The HARD form, PLX-151's: mint each hostile id FIRST, so the record
    // really exists and really is active and the join site's own check is the
    // only thing standing between a traversing id and a joined path. The mint
    // accepts them, because it delegates to the same `TenantId::try_new`.
    for hostile in ["../tenant-a", "a/b", "..", "/etc", "a.b", "tenant a"] {
        scene
            .store
            .mint_tenant(hostile, "hostile")
            .await
            .expect("the mint accepts these; that is the whole premise");
    }
    for hostile in ["../tenant-a", "a/b", "..", "/etc", "a.b", "tenant a"] {
        let id = TenantId::try_new(hostile)
            .unwrap_or_else(|_| panic!("TenantId::try_new must ACCEPT {hostile:?} — if it \
                 stops doing so this test is no longer testing the join site"));
        let refusal = scene
            .admission
            .tenant_storage(&id)
            .await
            .expect_err("a traversing id must not produce a storage root");
        assert!(
            matches!(refusal, AdmissionRefused::UnsafeSegment(_)),
            "{hostile:?} was refused for the wrong reason: {refusal:?}"
        );
    }
}

/// **Trap 2, and it is the one that no-ops silently.** A tenant owns the
/// contents of its own root, so it can plant a symlink where its storage
/// directory goes. Canonicalize-then-compare catches it; comparing the
/// spelling would not.
#[tokio::test]
async fn a_symlink_planted_at_the_storage_directory_is_refused() {
    let scene = Scene::new().await;

    // Materialise A's root, then destroy and replace `storage` with a link to
    // somewhere outside it.
    let a = scene.scope("tenant-a").await;
    let storage_dir = scene.roots.join("tenant-a").join("storage");
    assert!(storage_dir.exists());
    assert!(a.root().starts_with(storage_dir.canonicalize().expect("canon")));

    let elsewhere = scene._dir.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).expect("elsewhere");
    std::fs::remove_dir_all(&storage_dir).expect("remove");
    std::os::unix::fs::symlink(&elsewhere, &storage_dir).expect("plant symlink");

    let refusal = scene
        .admission
        .tenant_storage(&tid("tenant-a"))
        .await
        .expect_err("a storage directory that resolves outside the tenant root must be refused");
    assert!(
        matches!(refusal, AdmissionRefused::Root { .. }),
        "expected a containment refusal, got {refusal:?}"
    );
}

/// A symlink planted *inside* the root, pointing back inside it, is fine — the
/// rule is containment, not "no symlinks".
#[tokio::test]
async fn containment_is_the_rule_not_a_symlink_ban() {
    let scene = Scene::new().await;
    let a = scene.scope("tenant-a").await;
    let root = scene.roots.join("tenant-a");
    let real = root.join("real-storage");
    std::fs::create_dir_all(real.join("activations")).expect("real");
    std::fs::remove_dir_all(root.join("storage")).expect("remove");
    std::os::unix::fs::symlink(&real, root.join("storage")).expect("link");

    let again = scene
        .admission
        .tenant_storage(&tid("tenant-a"))
        .await
        .expect("a symlink that stays inside the root is not an escape");
    assert!(again.path().starts_with(root.canonicalize().expect("canon")));
    let _ = a;
}

// ═══════════════════════════════════════════════════════════════════════════
// PLX-129 requirement 2 — storage is not only the database
// ═══════════════════════════════════════════════════════════════════════════

/// `claudecode.sessions_*` joined a caller-supplied `project_path` onto a
/// process-global base with **no validation of any kind**. Per-tenant sqlite
/// does nothing about that: it is not a database.
#[test]
fn the_session_join_refuses_what_it_used_to_accept() {
    use plexus_substrate::SessionRoot;

    let dir = tempfile::tempdir().expect("dir");
    let root = SessionRoot::new(dir.path());

    for hostile in [
        "..",
        "../..",
        "../other-tenant",
        "a/b",
        "/etc",
        "/Users/someone/.ssh",
        "",
    ] {
        assert!(
            root.project_dir(hostile, false).is_err(),
            "project_path {hostile:?} was accepted"
        );
        assert!(
            root.session_path("ok", hostile, false).is_err()
                || root.session_path(hostile, "ok", false).is_err(),
            "{hostile:?} was accepted as a component"
        );
    }

    // A real Claude Code project slug — dots inside a name are legal, because
    // that is what the directories actually look like.
    let ok = root
        .project_dir("-Users-x-.config-thing", true)
        .expect("a normal slug must still work");
    assert!(ok.starts_with(dir.path().canonicalize().expect("canon")));

    // Trap 2 again, one layer up: a symlinked project directory aimed out.
    let outside = tempfile::tempdir().expect("outside");
    std::os::unix::fs::symlink(outside.path(), dir.path().join("escape")).expect("link");
    assert!(
        root.project_dir("escape", false).is_err(),
        "a symlinked project directory resolved outside the session root and was accepted"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// PLX-128 c1/c2 and PLX-129 c2 — the greps, as tests
// ═══════════════════════════════════════════════════════════════════════════

fn src_files() -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).expect("read src").flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

/// **PLX-129 c2** — no per-tenant storage path is assembled from a string at a
/// call site. `StorageScope::db_path` is called from exactly one function.
#[test]
fn every_storage_path_is_decided_in_one_place() {
    let mut callers = Vec::new();
    for f in src_files() {
        let text = std::fs::read_to_string(&f).expect("read");
        if f.ends_with("activations/storage.rs") {
            continue; // the definition, and `registry_config`'s two literals
        }
        for (n, line) in text.lines().enumerate() {
            if line.contains(".db_path(") && !line.trim_start().starts_with("//") {
                callers.push(format!("{}:{}", f.display(), n + 1));
            }
        }
    }
    assert!(
        callers.iter().all(|c| c.contains("builder.rs")),
        "a storage path is decided somewhere other than `build_activations_in`: {callers:#?}"
    );
    assert!(!callers.is_empty(), "the grep found nothing — it has drifted");
}

/// **PLX-129 c2, the other half** — the host-only free function has no tenant
/// parameter and no new callers. Every `activation_db_path` use is a
/// `Default` impl, which `build_activations_in` overwrites.
#[test]
fn activation_db_path_is_host_only_and_stayed_that_way() {
    for f in src_files() {
        let text = std::fs::read_to_string(&f).expect("read");
        if f.ends_with("activations/storage.rs") {
            continue;
        }
        for (n, line) in text.lines().enumerate() {
            let is_call = line.contains("activation_db_path(")
                || line.contains("activation_db_path_from_module!");
            if is_call && !line.trim_start().starts_with("//") && !line.contains("use crate") {
                assert!(
                    text[..text
                        .lines()
                        .take(n)
                        .map(|l| l.len() + 1)
                        .sum::<usize>()]
                        .contains("impl Default for"),
                    "{}:{} calls the HOST path helper outside a Default impl",
                    f.display(),
                    n + 1
                );
            }
        }
    }
}

/// **PLX-128 c2** — exactly one place mints a scoped `TenantId` from a
/// caller-supplied segment, and it is the mount layer.
///
/// Substrate never calls `TenantId::try_new` in product code at all: the only
/// identifier it ever scopes by is `record.id()`, out of the sealed
/// `TenantRecord`, or `admitted.id()`, out of the gate.
#[test]
fn substrate_never_mints_a_tenant_id() {
    let mut mints = Vec::new();
    for f in src_files() {
        let text = std::fs::read_to_string(&f).expect("read");
        for (n, line) in text.lines().enumerate() {
            // `TenantId::try_new(` — the CALL. The identical text appears in
            // three doc comments and one `#[error]` string explaining exactly
            // why it is not enough; those are the reason this test exists, not
            // violations of it.
            if line.contains("TenantId::try_new(") && !line.trim_start().starts_with("//") {
                mints.push(format!("{}:{}", f.display(), n + 1));
            }
        }
    }
    assert!(
        mints.is_empty(),
        "substrate mints a TenantId in product code; the mount layer is the only \
         place that may, and everything below it must take `record.id()` or \
         `admitted.id()`: {mints:#?}"
    );
}

/// **PLX-128 c1** — no activation reads its tenant, or its storage location,
/// from ambient process state at request time.
///
/// The two ambient reads that remain are named rather than hidden:
/// `activation_db_path` (host `Default` impls, construction only) and
/// `get_sessions_base_dir` (the host `SessionRoot` default). Both are
/// construction-time and both are overwritten for a tenant scope. Anything
/// else reaching for `HOME` or `home_dir` inside an activation is what this
/// test is for.
#[test]
fn no_activation_resolves_storage_from_ambient_state() {
    const ALLOWED: &[&str] = &[
        // the host default, construction time, overwritten per tenant
        "activations/storage.rs",
        // ditto, for Claude Code's own transcript directory
        "activations/claudecode/sessions.rs",
        // finds the `claude` BINARY, not tenant data; under a confinement the
        // binary comes from the image and this path is not consulted
        "activations/claudecode/executor.rs",
    ];
    let mut offenders = Vec::new();
    for f in src_files() {
        let rel = f
            .strip_prefix(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src"))
            .expect("rel")
            .to_string_lossy()
            .replace('\\', "/");
        if !rel.starts_with("activations/") || ALLOWED.iter().any(|a| rel == *a) {
            continue;
        }
        let text = std::fs::read_to_string(&f).expect("read");
        for (n, line) in text.lines().enumerate() {
            let l = line.trim_start();
            if l.starts_with("//") || l.starts_with("///") {
                continue;
            }
            if line.contains("dirs::home_dir")
                || line.contains(r#"env::var("HOME")"#)
                || line.contains(r#"env::var("USERPROFILE")"#)
            {
                offenders.push(format!("{rel}:{}", n + 1));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "an activation resolves storage from ambient process state: {offenders:#?}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// PLX-131's obligation, discharged
// ═══════════════════════════════════════════════════════════════════════════

/// Deleting a tenant removes its files, and `per_tenant_storage_removed`
/// stops being a hard-coded `false`.
#[tokio::test]
async fn deleting_a_tenant_removes_its_storage() {
    use plexus_idp::TenantStorageReaper;
    use plexus_substrate::SubstrateTenantStorage;

    let scene = Scene::new().await;
    let scope = scene.scope("tenant-a").await;
    let _hub = tenant_hub(&scope).await;

    let root = scene.roots.join("tenant-a");
    assert!(root.exists() && !databases_under(&root).is_empty());

    let reaper = SubstrateTenantStorage::new(Arc::clone(&scene.admission));
    assert!(
        reaper
            .remove_tenant_storage(&tid("tenant-a"))
            .await
            .expect("reap"),
        "the reaper reported nothing to remove, but the databases were there"
    );
    assert!(!root.exists(), "tenant-a's root survived deletion");

    // Idempotent, and a tenant that never wrote is not an error.
    assert!(
        !reaper
            .remove_tenant_storage(&tid("tenant-a"))
            .await
            .expect("second reap"),
        "a second reap must report `false`, not fail"
    );

    // And the trap. `remove_dir_all` on a traversing id is the worst version
    // of this hazard, so the check runs BEFORE the join here too — and the
    // neighbour whose name the id spells has to actually be on disk, or
    // "nothing was deleted" would be true for the wrong reason.
    let neighbour = scene.roots.join("tenant-b");
    let _ = scene.scope("tenant-b").await;
    assert!(neighbour.exists(), "the fixture did not materialise tenant-b");

    let hostile = TenantId::try_new("../tenant-b").expect("try_new accepts it");
    assert!(
        reaper.remove_tenant_storage(&hostile).await.is_err(),
        "a traversing id reached the reaper"
    );
    assert!(
        neighbour.exists(),
        "the reaper followed a traversing id and destroyed the neighbour"
    );
}

// ═══════════════════════════════════════════════════════════════════════════

/// Flatten a `PlexusStream` into one string.
async fn collect(stream: plexus_core::plexus::PlexusStream) -> String {
    use futures::StreamExt;
    let mut out = String::new();
    let mut stream = stream;
    while let Some(item) = stream.next().await {
        out.push_str(&format!("{item:?}"));
    }
    out
}

// ═══════════════════════════════════════════════════════════════════════════
// The same property, through the REAL mount
// ═══════════════════════════════════════════════════════════════════════════

/// The tests above compose a tenant hub the way the factory does. This one
/// makes the factory do it — one process-wide hub, two callers, dispatch by
/// `tenants.<id>.<activation>.<method>` — because "the composer would scope it
/// correctly if it were called" and "the composer is what runs" are different
/// claims and only the second one ships.
#[tokio::test]
async fn end_to_end_one_hub_two_tenants_and_the_write_does_not_cross() {
    let scene = Scene::new().await;
    let hub = {
        let _guard = STORAGE_INIT.lock().await;
        plexus_substrate::build_plexus_rpc_with_admission(
            Some(Arc::clone(&scene.admission)),
            None,
        )
        .await
    };

    let caller = |tenant: &str| {
        plexus_core::plexus::AuthContext::new(
            format!("user-of-{tenant}"),
            "sess".to_owned(),
            vec!["user".to_owned()],
            json!({ "org_id": tenant }),
        )
    };
    let a = caller("tenant-a");
    let b = caller("tenant-b");

    const NAME: &str = "end-to-end";
    let wrote = collect(
        hub.route(
            "tenants.tenant-a.mustache.register_template",
            json!({
                "plugin_id": PLUGIN,
                "method": NAME,
                "name": "default",
                "template": SECRET,
            }),
            Some(&a),
        )
        .await
        .expect("A registers through its own mount"),
    )
    .await;
    assert!(
        !wrote.contains("\"kind\": String(\"failed\")"),
        "the write failed, so nothing below means anything: {wrote}"
    );

    // Liveness: A reads it back through the mount.
    let a_read = collect(
        hub.route(
            "tenants.tenant-a.mustache.get_template",
            json!({ "plugin_id": PLUGIN, "method": NAME, "name": "default" }),
            Some(&a),
        )
        .await
        .expect("A reads through its own mount"),
    )
    .await;
    assert!(a_read.contains(SECRET), "A cannot read its own write: {a_read}");

    // B, correctly admitted to its OWN mount, asks the identical question.
    let b_read = match hub
        .route(
            "tenants.tenant-b.mustache.get_template",
            json!({ "plugin_id": PLUGIN, "method": NAME, "name": "default" }),
            Some(&b),
        )
        .await
    {
        Ok(stream) => collect(stream).await,
        Err(e) => e.to_string(),
    };
    assert!(
        !b_read.contains(SECRET),
        "tenant B read tenant A's template through the mount: {b_read}"
    );

    // And it is on the filesystem: A's file has the bytes, B's does not.
    let a_db = scene
        .roots
        .join("tenant-a/storage/activations/mustache/templates.db");
    let b_db = scene
        .roots
        .join("tenant-b/storage/activations/mustache/templates.db");
    assert!(
        a_db.exists(),
        "the mount did not put tenant A's mustache database where PLX-129 says: {}",
        a_db.display()
    );
    assert!(b_db.exists(), "tenant B never got a database of its own");
    assert!(file_contains(&a_db, SECRET));
    assert!(
        !file_contains(&b_db, SECRET),
        "the secret bytes are in tenant B's file"
    );

    // The host's own storage is untouched by either tenant.
    let host = StorageScope::host().db_path("mustache", "templates.db");
    assert!(
        !file_contains(&host, SECRET),
        "a tenant's write landed in the HOST database at {}",
        host.display()
    );
}
