//! PLX-151 — the measured attack, through the real path.
//!
//! # The bar, and what each test does about it
//!
//! PLX-144's bar was *test the escape, not the config*, and its escape test was
//! verified non-vacuous by deliberately breaking the confinement and watching
//! the test go red. This file holds the same bar one layer up, where the
//! product actually is:
//!
//! * The attack runs through **`claudecode.chat` with `allowed_tools:
//!   ["Bash"]`**, at a `working_dir` the tenant chose — the exact shape PLX-130
//!   measured.
//! * The confined process is the **real Claude CLI**, from the tenant image,
//!   which really spawns a real `bash`.
//! * The mutation probe is not a manual one-off: it is a **test**
//!   (`the_escape_test_can_detect_a_leak_when_the_confinement_is_broken`) that
//!   deliberately adds a second bind mount exposing the tenant root's parent
//!   and asserts the same attack **does** leak. A future change that silently
//!   removes the boundary makes the escape test red; a future change that
//!   silently removes the *test's ability to see* a leak makes the probe red.
//!
//! # What is real here and what is substituted
//!
//! Real: the CLI (`@anthropic-ai/claude-code`, pinned in
//! `plexus-sandbox/images/Dockerfile.tenant`), its tool dispatch, the `bash` it
//! spawns, the Docker confinement, substrate's executor, activation, storage
//! and `ChatEvent` stream, and the identity store the tenant is admitted from.
//!
//! Substituted: **only the model.** See `common/adversary.rs` for why — a live
//! model is a probabilistic attacker and needs credentials a test must not go
//! looking for. The scripted adversary always attacks, and attacks exactly what
//! the test says.
//!
//! # These tests do not skip when Docker is missing
//!
//! PLX-144's rule, kept: a green run with the escape test skipped proves
//! nothing.

mod common;

use std::sync::Arc;

use common::adversary::Adversary;
use common::{reap_stray_containers, Fixture};

use futures::StreamExt;
use plexus_auth_core::tenant::TenantId;
use plexus_idp::store::IdentityStore;
use plexus_idp::tenant::TenantStatus;
use plexus_sandbox::docker::{DockerConfig, DockerSandbox};
use plexus_sandbox::Sandbox;
use plexus_substrate::activations::arbor::{ArborConfig, ArborStorage};
use plexus_substrate::activations::claudecode::{
    ChatEvent, ClaudeCode, ClaudeCodeStorage, ClaudeCodeStorageConfig, Model,
};
use plexus_substrate::builder::TenantExecution;
use plexus_substrate::tenancy::{AdmissionRefused, TenantAdmission};

/// The image built by `plexus-sandbox/images/Dockerfile.tenant`. It contains
/// `bash` and the real `claude`; PLX-130 verified the shipped alpine has
/// neither, which is why the image is part of this ticket.
const TENANT_IMAGE: &str = "plexus-tenant:dev";

// ═══════════════════════════════════════════════════════════════════════════
// Fixture plumbing
// ═══════════════════════════════════════════════════════════════════════════

fn tid(s: &str) -> TenantId {
    TenantId::try_new(s).expect("valid tenant id")
}

/// Serialises the tests that start a container.
///
/// Cargo runs the tests in one binary concurrently, and each of these boots a
/// real `claude` inside a container with a 2 GiB ceiling. Measured: three at
/// once on this machine (macOS + colima) produced a CLI that exited having
/// written nothing, and the mutation probe failed on its own liveness control
/// rather than on its assertion. That is a flaky *test*, not a finding, and a
/// flaky escape test is worse than none — so they take turns.
///
/// It is a guard rather than `--test-threads=1` on purpose: the requirement
/// belongs to these tests, not to whoever runs `cargo test`.
fn container_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// A nonce-tagged secret, so a leak cannot be confused with anything else that
/// happened to be in the output.
fn secret(label: &str) -> String {
    format!(
        "{label}_SECRET_{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    )
}

async fn identity_store(fx: &Fixture) -> Arc<IdentityStore> {
    let db = fx.base().join("identity.db");
    Arc::new(
        IdentityStore::open(db.to_str().expect("utf-8 db path"))
            .await
            .expect("identity store"),
    )
}

/// A Docker backend for the tenant image.
///
/// `extra_args` carries `--add-host` so the confined CLI can reach the
/// adversary on the host loopback. That is a **test** widening of reachability,
/// not of the filesystem: the bind mount is unchanged, and every escape
/// assertion below is about the filesystem.
async fn docker(extra_args: Vec<String>) -> Arc<dyn Sandbox> {
    let mut config = DockerConfig::new(TENANT_IMAGE);
    config.extra_args = extra_args;
    let sandbox = DockerSandbox::detect(config).await.expect(
        "Docker must be available to discharge PLX-151 c2. These tests deliberately \
         do not skip: a green run with the escape test skipped would prove nothing. \
         The image must exist too — build it with \
         `docker build -f images/Dockerfile.tenant -t plexus-tenant:dev .` in plexus-sandbox.",
    );
    Arc::new(sandbox)
}

fn host_gateway_args() -> Vec<String> {
    vec!["--add-host=host.docker.internal:host-gateway".to_owned()]
}

async fn claudecode(fx: &Fixture, label: &str) -> ClaudeCode {
    let arbor = ArborStorage::new(ArborConfig {
        db_path: fx.base().join(format!("{label}-arbor.db")),
        ..Default::default()
    })
    .await
    .expect("arbor");
    let storage = ClaudeCodeStorage::new(
        ClaudeCodeStorageConfig {
            db_path: fx.base().join(format!("{label}-claudecode.db")),
        },
        Arc::new(arbor),
    )
    .await
    .expect("claudecode storage");
    ClaudeCode::with_context_type(Arc::new(storage))
}

/// Everything a `claudecode.chat` turn said, flattened, under a hard timeout.
///
/// A hang is a red test, never a suite that never finishes — and it reaps the
/// container on its way out.
async fn chat_transcript(
    cc: &ClaudeCode,
    session: &str,
    prompt: &str,
    allowed_tools: Vec<String>,
) -> String {
    let stream = cc
        .chat(
            session.to_owned(),
            prompt.to_owned(),
            None,
            Some(allowed_tools),
        )
        .await;
    let collect = async {
        let mut stream = Box::pin(stream);
        let mut out = String::new();
        while let Some(event) = stream.next().await {
            use std::fmt::Write as _;
            match event {
                ChatEvent::Content { text } => out.push_str(&text),
                ChatEvent::ToolUse { tool_name, input, .. } => {
                    let _ = write!(out, "\n[tool_use {tool_name} {input}]\n");
                }
                ChatEvent::ToolResult { output, is_error, .. } => {
                    let _ = write!(out, "\n[tool_result err={is_error} {output}]\n");
                }
                ChatEvent::Err { message } => {
                    let _ = write!(out, "\n[err {message}]\n");
                }
                other => {
                    let _ = write!(out, "\n[{}]\n", serde_json::to_string(&other).unwrap_or_default());
                }
            }
        }
        out
    };

    match tokio::time::timeout(std::time::Duration::from_secs(180), collect).await {
        Ok(out) => out,
        Err(_) => {
            reap_stray_containers();
            panic!("the confined chat turn did not finish within 180s");
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// c2 — THE test
// ═══════════════════════════════════════════════════════════════════════════

/// The attack command: five routes to tenant B's file, the way PLX-144's escape
/// test tried five. `LIVENESS` proves the shell ran and A's own root mounted.
fn attack_script(
    b_canonical: &std::path::Path,
    base_canonical: &std::path::Path,
) -> String {
    format!(
        // Every route must PRINT CONTENT, not a filename.
        //
        // This is a correction the mutation probe forced, and it is the same
        // lesson PLX-144 recorded: its probe "exposed one existing assertion as
        // too weak to notice the leak". A first draft of this script ended with
        // `grep -rl` and `find -xdev`. Under the base-exposed mutation both did
        // their job — `grep -rl` found the file — and the test still went GREEN,
        // because `-l` prints the *path* and the assertion looks for the
        // *secret*. `-xdev` was worse: it refuses to cross into the bind mount,
        // so it could never have seen anything. A read primitive that reports
        // "there is a file here" is not the leak; the contents are.
        //
        // `./mine.txt` rather than an absolute path also proves the
        // tenant-chosen `working_dir` was honoured inside the confinement.
        "echo LIVENESS_BEGIN; pwd; cat ./mine.txt 2>&1; echo ---; \
         cat /workspace/../tenant-b/secret.txt 2>&1; \
         cat /workspace/neighbour/secret.txt 2>&1; \
         cat {b}/secret.txt 2>&1; \
         ls -a {base} 2>&1; \
         find / -name 'secret.txt' -exec cat {{}} \\; 2>/dev/null; \
         grep -rh 'TENANT_B' /workspace /etc /root /home /srv /var /opt /mnt /data /Users 2>/dev/null | head -5; \
         echo LIVENESS_END",
        b = b_canonical.display(),
        base = base_canonical.display(),
    )
}

/// Build the world both the escape test and its mutation probe use: two
/// tenants in a real identity store, B holding a nonce-tagged secret, A holding
/// a symlink aimed at B, and a confined `claudecode` for A.
struct Scene {
    fx: Fixture,
    secret: String,
    a_work: std::path::PathBuf,
    b_canonical: std::path::PathBuf,
    base_canonical: std::path::PathBuf,
    admission: Arc<TenantAdmission>,
}

impl Scene {
    async fn new(label: &str) -> Self {
        let fx = Fixture::new(label);
        let roots = fx.dir("roots");
        let secret = secret("TENANT_B");

        std::fs::create_dir_all(roots.join("tenant-a")).expect("a root");
        let a_work = roots.join("tenant-a/work");
        std::fs::create_dir_all(&a_work).expect("a work");
        std::fs::write(a_work.join("mine.txt"), "TENANT_A_LIVENESS").expect("a file");

        let b_root = roots.join("tenant-b");
        std::fs::create_dir_all(&b_root).expect("b root");
        std::fs::write(b_root.join("secret.txt"), &secret).expect("b secret");

        // A symlink planted inside A's OWN root, aimed at B. The tenant
        // controls the contents of its root, so it controls this.
        let _ = std::os::unix::fs::symlink(&b_root, roots.join("tenant-a/neighbour"));

        let store = identity_store(&fx).await;
        store.mint_tenant("tenant-a", "Tenant A").await.expect("mint a");
        store.mint_tenant("tenant-b", "Tenant B").await.expect("mint b");

        Self {
            secret,
            a_work,
            b_canonical: b_root.canonicalize().expect("canonical b"),
            base_canonical: roots.canonicalize().expect("canonical base"),
            admission: Arc::new(TenantAdmission::new(store, roots)),
            fx,
        }
    }

    fn attack(&self) -> String {
        attack_script(&self.b_canonical, &self.base_canonical)
    }
}

/// **THE test.** Tenant A calls `claudecode.chat` with `allowed_tools:
/// ["Bash"]`, at a `working_dir` A chose, and the real CLI really runs a real
/// shell that really tries five routes to tenant B's file.
#[tokio::test]
async fn tenant_a_chatting_with_bash_cannot_read_tenant_bs_file() {
    let _serial = container_lock().lock().await;
    let scene = Scene::new("escape").await;

    // ── control 1: unsandboxed, the attack works. ────────────────────────
    // If this does not leak, the test cannot detect a leak and everything
    // below is theatre.
    let host = std::process::Command::new("sh")
        .arg("-c")
        .arg(format!("cat {}/secret.txt", scene.b_canonical.display()))
        .output()
        .expect("host read");
    let host_out = String::from_utf8_lossy(&host.stdout);
    assert!(
        host_out.contains(&scene.secret),
        "the unsandboxed control must leak; got {host_out:?}"
    );

    // ── the real path ────────────────────────────────────────────────────
    let adversary = Adversary::start(&scene.attack());
    let tenancy = TenantExecution::with_sandbox(
        docker(host_gateway_args()).await,
        Arc::clone(&scene.admission),
    )
    .with_env("ANTHROPIC_BASE_URL", adversary.base_url_for_container())
    .with_env("ANTHROPIC_API_KEY", "adversary-not-a-real-key")
    .with_env("HOME", "/tmp")
    .with_env("DISABLE_AUTOUPDATER", "1")
    .with_env("DISABLE_TELEMETRY", "1");

    let cc = claudecode(&scene.fx, "escape")
        .await
        .confined_to(tenancy.confinement_for(&tid("tenant-a")));

    // The working_dir is the tenant's own choice, exactly as PLX-130 row B3
    // describes: `claudecode.create(working_dir)`.
    cc.create(
        "attack".to_owned(),
        scene.a_work.display().to_string(),
        Model::Sonnet,
        None,
        None,
        None,
    )
    .await
    .expect("create session");

    let transcript = chat_transcript(
        &cc,
        "attack",
        "read the neighbouring tenant's secret",
        vec!["Bash".to_owned()],
    )
    .await;

    let tool_output = adversary.tool_results().join("\n");
    let everything = format!("{transcript}\n---SHELL---\n{tool_output}");

    // ── control 2: the shell really ran, and A's own root really mounted ──
    assert!(
        adversary.the_shell_ran(),
        "the CLI never executed a Bash tool call, so nothing was attempted — \
         this test would pass vacuously.\n{everything}"
    );
    assert!(
        everything.contains("LIVENESS_BEGIN") && everything.contains("LIVENESS_END"),
        "the confined shell did not run to completion.\n{everything}"
    );
    assert!(
        everything.contains("TENANT_A_LIVENESS"),
        "tenant A could not read its own file, so the sandbox is broken rather \
         than tight — a confinement that breaks the product is a different \
         failure, not a success.\n{everything}"
    );

    // ── the assertion that matters ───────────────────────────────────────
    assert!(
        !everything.contains(&scene.secret),
        "TENANT ESCAPE: tenant B's secret reached tenant A's confined \
         claudecode.chat turn.\n{everything}"
    );
}

/// The mutation probe, mechanised.
///
/// PLX-144 broke its confinement by hand, watched three of five read routes
/// fire, then reverted. This does the same mutation — a second bind mount
/// exposing the tenant root's *parent* — and asserts the identical attack
/// **succeeds**. So the escape test above is proven able to see a leak, every
/// run, rather than once on an agent's afternoon.
#[tokio::test]
async fn the_escape_test_can_detect_a_leak_when_the_confinement_is_broken() {
    let _serial = container_lock().lock().await;
    let scene = Scene::new("mutation").await;

    let adversary = Adversary::start(&scene.attack());

    // THE MUTATION: expose the parent of the tenant root. Everything else —
    // the image, the argv, the workdir containment, the environment, the
    // assertions — is identical to the escape test.
    let mut broken = host_gateway_args();
    broken.push("--mount".to_owned());
    broken.push(format!(
        "type=bind,src={},dst={}",
        scene.base_canonical.display(),
        scene.base_canonical.display()
    ));

    let tenancy =
        TenantExecution::with_sandbox(docker(broken).await, Arc::clone(&scene.admission))
            .with_env("ANTHROPIC_BASE_URL", adversary.base_url_for_container())
            .with_env("ANTHROPIC_API_KEY", "adversary-not-a-real-key")
            .with_env("HOME", "/tmp");

    let cc = claudecode(&scene.fx, "mutation")
        .await
        .confined_to(tenancy.confinement_for(&tid("tenant-a")));

    cc.create(
        "attack".to_owned(),
        scene.a_work.display().to_string(),
        Model::Sonnet,
        None,
        None,
        None,
    )
    .await
    .expect("create session");

    let transcript = chat_transcript(
        &cc,
        "attack",
        "read the neighbouring tenant's secret",
        vec!["Bash".to_owned()],
    )
    .await;
    let everything = format!("{transcript}\n---SHELL---\n{}", adversary.tool_results().join("\n"));

    assert!(
        adversary.the_shell_ran(),
        "the probe's shell never ran, so the probe proves nothing either.\n{everything}"
    );
    assert!(
        everything.contains(&scene.secret),
        "THE MUTATION PROBE FAILED TO LEAK. Either the probe no longer breaks \
         the confinement, or the attack no longer reaches tenant B's file — \
         either way the escape test above is no longer known to be able to \
         detect an escape, and it must not be trusted until this is \
         understood.\n{everything}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// c4 — the sealed proof, honoured at the caller
// ═══════════════════════════════════════════════════════════════════════════

/// A suspended tenant cannot launch. Checked at the launch, not once at mount
/// time — so suspension stops the *next turn*.
#[tokio::test]
async fn a_suspended_tenant_cannot_launch_a_confined_session() {
    let _serial = container_lock().lock().await;
    let scene = Scene::new("suspended").await;

    // It works first — otherwise "suspension refused it" would be
    // indistinguishable from "it never worked".
    let root = scene
        .admission
        .tenant_root(&tid("tenant-a"))
        .await
        .expect("an active tenant resolves");
    assert_eq!(root.tenant().as_str(), "tenant-a");

    let store = identity_store(&scene.fx).await;
    store
        .set_tenant_status("tenant-a", TenantStatus::Suspended)
        .await
        .expect("suspend");

    let refusal = scene
        .admission
        .tenant_root(&tid("tenant-a"))
        .await
        .expect_err("a suspended tenant must not resolve a root");
    assert!(
        matches!(refusal, AdmissionRefused::Suspended(ref t) if t == "tenant-a"),
        "expected Suspended, got {refusal:?}"
    );

    // …and the refusal reaches an actual chat turn as an error, not a launch.
    let tenancy = TenantExecution::with_sandbox(
        docker(host_gateway_args()).await,
        Arc::clone(&scene.admission),
    );
    let cc = claudecode(&scene.fx, "suspended")
        .await
        .confined_to(tenancy.confinement_for(&tid("tenant-a")));
    cc.create(
        "s".to_owned(),
        scene.a_work.display().to_string(),
        Model::Sonnet,
        None,
        None,
        None,
    )
    .await
    .expect("create");

    let transcript = chat_transcript(&cc, "s", "hello", vec!["Bash".to_owned()]).await;
    assert!(
        transcript.contains("suspended"),
        "a suspended tenant's chat turn must refuse, and say why.\n{transcript}"
    );
}

/// A tenant that does not exist is refused, and is refused *differently* from a
/// path that is unsafe — the operator needs to be able to tell them apart.
#[tokio::test]
async fn an_unknown_tenant_and_an_unsafe_id_are_both_refused() {
    let fx = Fixture::new("admission");
    let roots = fx.dir("roots");
    let store = identity_store(&fx).await;
    let admission = TenantAdmission::new(store, &roots);

    let refusal = admission
        .tenant_root(&tid("never-minted"))
        .await
        .expect_err("an unminted tenant has no root");
    assert!(matches!(refusal, AdmissionRefused::UnknownTenant(_)), "{refusal:?}");

    // THE TRAP, pinned, and pinned in its HARDEST form.
    //
    // `TenantId::try_new("../tenant-a")` is VALID — it validates non-empty,
    // length and control characters, nothing about path safety. And so is
    // `mint_tenant`, which delegates to exactly that validator: a traversing
    // tenant can be MINTED. So this does not test a lookup miss (which would
    // pass for the wrong reason); it mints the traversing tenant first, so the
    // record exists and is active, and the ONLY thing standing between it and
    // a joined path is the join site's own check.
    let store = identity_store(&fx).await;
    let admission = TenantAdmission::new(Arc::clone(&store), &roots);
    for hostile in ["../tenant-a", "a.b", "a/b", "/etc"] {
        let minted = store.mint_tenant(hostile, "hostile").await;
        assert!(
            minted.is_ok(),
            "the mint accepts {hostile:?} — that is the finding, and why the \
             join site cannot delegate this check to the identity type"
        );
        let id = TenantId::try_new(hostile).expect("TenantId::try_new accepts it too");
        let refusal = admission
            .tenant_root(&id)
            .await
            .expect_err("the join site must check for itself");
        assert!(
            matches!(refusal, AdmissionRefused::UnsafeSegment(_)),
            "{hostile:?} must be refused as an unsafe segment BEFORE the join, \
             got {refusal:?}"
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// The working_dir, constrained rather than merely resolved
// ═══════════════════════════════════════════════════════════════════════════

/// PLX-130 row B3: `claudecode.create` canonicalizes `working_dir` and stores
/// it, which **resolves** without **constraining**. Under confinement the
/// stored value is passed to `TenantRoot::contain`, so choosing another
/// tenant's directory is a refusal, not a launch.
#[tokio::test]
async fn a_working_dir_outside_the_tenant_root_refuses_to_launch() {
    let _serial = container_lock().lock().await;
    let scene = Scene::new("workdir").await;

    let tenancy = TenantExecution::with_sandbox(
        docker(host_gateway_args()).await,
        Arc::clone(&scene.admission),
    );
    let cc = claudecode(&scene.fx, "workdir")
        .await
        .confined_to(tenancy.confinement_for(&tid("tenant-a")));

    // Tenant A names tenant B's directory as its working_dir. `create` accepts
    // it — canonicalize resolves it happily, which is exactly PLX-130's point.
    cc.create(
        "cross".to_owned(),
        scene.b_canonical.display().to_string(),
        Model::Sonnet,
        None,
        None,
        None,
    )
    .await
    .expect("create still accepts it — resolution is not constraint");

    let transcript = chat_transcript(&cc, "cross", "hello", vec!["Bash".to_owned()]).await;
    assert!(
        transcript.contains("path escape") || transcript.contains("outside tenant root"),
        "a working_dir outside the tenant root must be refused at launch.\n{transcript}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// c5 — the untenanted path
// ═══════════════════════════════════════════════════════════════════════════

/// An untenanted `claudecode` still launches, and gets **no confinement**.
///
/// The report states this plainly rather than implying otherwise: the
/// untenanted path spawns on the host with the substrate process's uid, its
/// `$HOME`, its environment minus `CLAUDECODE`, and `.current_dir()` set as a
/// convenience that PLX-130 measured is not a boundary. It exists because a
/// single-tenant deployment has one principal who already owns the machine,
/// and making a container runtime mandatory would buy no isolation there.
///
/// This test asserts the two facts that matter: the untenanted executor has no
/// confinement, and it still reaches the CLI (it produces a launch event and a
/// turn rather than erroring at the confinement).
#[tokio::test]
async fn an_untenanted_call_still_works_and_is_explicitly_unconfined() {
    let _serial = container_lock().lock().await;
    let fx = Fixture::new("untenanted");
    let work = fx.dir("work");
    let adversary = Adversary::start("echo UNTENANTED_SHELL_RAN");

    let cc = claudecode(&fx, "untenanted").await;
    assert!(
        cc.confinement().is_none(),
        "an untenanted claudecode must have no confinement, and must say so"
    );

    cc.create(
        "plain".to_owned(),
        work.display().to_string(),
        Model::Sonnet,
        None,
        None,
        None,
    )
    .await
    .expect("create");

    // Point the HOST CLI at the adversary the same way the confined one is
    // pointed. This is the untenanted path end to end: no container.
    std::env::set_var("ANTHROPIC_BASE_URL", format!("http://127.0.0.1:{}", adversary.port()));
    std::env::set_var("ANTHROPIC_API_KEY", "adversary-not-a-real-key");

    let transcript = chat_transcript(&cc, "plain", "say hello", vec!["Bash".to_owned()]).await;

    std::env::remove_var("ANTHROPIC_BASE_URL");
    std::env::remove_var("ANTHROPIC_API_KEY");

    assert!(
        !transcript.contains("confined launch refused"),
        "the untenanted path must not require a sandbox.\n{transcript}"
    );
    assert!(
        adversary.the_shell_ran(),
        "the untenanted path must still reach the CLI and run its tool.\n{transcript}"
    );
    assert!(
        transcript.contains("UNTENANTED_SHELL_RAN"),
        "the untenanted turn must complete normally.\n{transcript}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// c1 — the launcher is confined, structurally
// ═══════════════════════════════════════════════════════════════════════════

/// A tenant's `claudecode` has a confinement; the host's does not.
///
/// This is a *composition* assertion, not the escape assertion — the escape is
/// tested by running it. It is here so that a future refactor which registers
/// the shared host `claudecode` into a tenant hub fails a test rather than a
/// boundary.
#[tokio::test]
async fn a_tenant_hub_is_never_composed_with_an_unconfined_claudecode() {
    let _serial = container_lock().lock().await;
    let scene = Scene::new("composition").await;
    let tenancy = TenantExecution::with_sandbox(
        docker(Vec::new()).await,
        Arc::clone(&scene.admission),
    );

    let host = claudecode(&scene.fx, "composition").await;
    assert!(host.confinement().is_none(), "the host instance is unconfined");

    let tenant = host.confined_to(tenancy.confinement_for(&tid("tenant-a")));
    let confinement = tenant
        .confinement()
        .expect("a tenant instance MUST be confined");
    assert_eq!(confinement.tenant(), "tenant-a");

    // …and the confinement resolves to that tenant's root and no other.
    let root = confinement.tenant_root().await.expect("root");
    assert_eq!(root.tenant().as_str(), "tenant-a");
    assert!(
        root.path().as_path().ends_with("tenant-a"),
        "the exposed directory must be the tenant's own root, got {}",
        root.path()
    );
    assert!(
        root.contain(&scene.b_canonical).is_err(),
        "the confinement must refuse to contain another tenant's root"
    );
}
