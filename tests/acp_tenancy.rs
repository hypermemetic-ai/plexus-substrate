//! PLX-140 c4 — tenancy holds **through ACP**, and the standard is absence.
//!
//! # The criterion, and why it is worded the way it is
//!
//! > the PLX-127 negative test holds for an ACP session: tenant B cannot reach
//! > tenant A's session and A is **absent** from B's visible surface.
//!
//! PLX-127 c2 drew the distinction this file is built around: *enumeration is
//! disclosure*. A mount that answers "you may not have that session" has told
//! the caller the session exists. So every assertion below is about **absence**
//! — B's list does not contain A, B's Connectome does not contain A, and B's
//! error for A's live id is **byte-identical** to its error for an id nobody
//! ever minted.
//!
//! # Test the escape, not the config
//!
//! PLX-144's bar, kept by PLX-151 and kept here. Nothing in this file asserts
//! that a flag is set or that two objects are different. Every test **mounts
//! the attack**: it takes A's real, live session id and drives it at B's agent
//! through `session/prompt`, `session/cancel` and `session/list`, which are the
//! only three ACP methods that take one.
//!
//! And the attack is proven capable of seeing a leak. The last test in this
//! file is a **permanent probe**: it composes the two agents over **one shared
//! mount** — the single mutation that would break isolation — and asserts the
//! *identical* attack succeeds. If a future change makes the escape tests pass
//! for the wrong reason, the probe goes red.
//!
//! # Why there is no tenant id in the agent
//!
//! `ClaudeCodeAcpAgent` holds no tenant, checks no tenant, and has no code path
//! that could compare one. That is the design, not an omission: `builder.rs`'s
//! `TenantSubtreeFactory` — reachable only with an `AdmittedTenant`, which only
//! `TenantMountGate::admit` can mint — builds **one agent per tenant**, over
//! that tenant's own `ClaudeCode` and its own `SessionMount`. Isolation is
//! therefore *not having* the other tenant's sessions rather than *declining to
//! serve* them, and a check that does not exist cannot be forgotten, reordered
//! or bypassed. These tests compose the same shape the factory does.
//!
//! Note what this file does and does not cover. **Execution** confinement (the
//! sandbox) is PLX-151's `tests/tenant_confinement.rs` and **storage**
//! isolation is PLX-128/129's `tests/tenant_storage_isolation.rs`; both already
//! hold and this build did not touch them. What is new here, and what PLX-140
//! c4 actually asks for, is that the **ACP session surface** does not cross.

use std::sync::Arc;
use std::time::Duration;

use plexus_acp::v1::schema::{
    CancelNotification, ContentBlock, NewSessionRequest, PromptRequest, SessionId,
};
use plexus_acp::v1::transport::Peer;
use plexus_acp::v1::Agent;
use plexus_substrate::acp::ClaudeCodeAcpAgent;
use plexus_substrate::activations::arbor::{ArborConfig, ArborStorage};
use plexus_substrate::activations::claudecode::{
    ClaudeCode, ClaudeCodeStorage, ClaudeCodeStorageConfig,
};

/// PLX-146's rule. Nothing here should take a second; a hang is the failure
/// mode of anything that touches a turn, and a test that waits forever reports
/// nothing.
const LIMIT: Duration = Duration::from_secs(30);

async fn bounded<F: std::future::Future>(what: &str, fut: F) -> F::Output {
    tokio::time::timeout(LIMIT, fut)
        .await
        .unwrap_or_else(|_| panic!("HUNG: {what} did not resolve within {LIMIT:?}"))
}

// ═══════════════════════════════════════════════════════════════════════════
// The two tenants
// ═══════════════════════════════════════════════════════════════════════════

struct Fixture {
    base: std::path::PathBuf,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let base = std::env::temp_dir().join(format!(
            "plx140t-{label}-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&base).expect("fixture dir");
        Self { base }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

/// One tenant's `claudecode`, over that tenant's own storage — the shape
/// `build_activations_in` produces behind the `TenantSubtreeFactory`.
async fn claudecode_for(fx: &Fixture, tenant: &str) -> Arc<ClaudeCode> {
    let arbor = ArborStorage::new(ArborConfig {
        db_path: fx.base.join(format!("{tenant}-arbor.db")),
        ..Default::default()
    })
    .await
    .expect("arbor");
    let storage = ClaudeCodeStorage::new(
        ClaudeCodeStorageConfig {
            db_path: fx.base.join(format!("{tenant}-claudecode.db")),
        },
        Arc::new(arbor),
    )
    .await
    .expect("claudecode storage");
    Arc::new(ClaudeCode::with_context_type(Arc::new(storage)))
}

/// Two tenants, composed the way the factory composes them: one agent each,
/// over one `ClaudeCode` each, with one `SessionMount` each.
async fn two_tenants(fx: &Fixture) -> (ClaudeCodeAcpAgent, ClaudeCodeAcpAgent) {
    let a = ClaudeCodeAcpAgent::new(claudecode_for(fx, "a").await, Peer::new());
    let b = ClaudeCodeAcpAgent::new(claudecode_for(fx, "b").await, Peer::new());
    (a, b)
}

async fn open_session(agent: &ClaudeCodeAcpAgent, fx: &Fixture) -> SessionId {
    let response = bounded(
        "session/new",
        agent.new_session(NewSessionRequest::new(fx.base.clone())),
    )
    .await
    .expect("session/new");
    response.session_id
}

fn prompt_for(session: &SessionId) -> PromptRequest {
    PromptRequest::new(session.clone(), vec![ContentBlock::from("reach across")])
}

/// An id nobody ever minted. The control every absence assertion is compared
/// against.
fn never_minted() -> SessionId {
    SessionId::new("acp-never-minted-0")
}

// ═══════════════════════════════════════════════════════════════════════════
// c4 — the attack
// ═══════════════════════════════════════════════════════════════════════════

/// **c4, the escape.** B drives A's live session id at its own agent, and gets
/// exactly what it gets for an id that never existed.
///
/// The byte-identity is the assertion that matters. A distinct error — a
/// `TenantBoundary` reason, a "forbidden", anything — would **confirm the
/// session exists**, which is the disclosure PLX-127 c2 refused to make. The
/// agent cannot make it, because it has no way to look.
#[tokio::test]
async fn tenant_b_cannot_reach_tenant_as_acp_session() {
    let fx = Fixture::new("escape");
    let (a, b) = two_tenants(&fx).await;

    // BOTH tenants live first, so a leak has something to leak. A mount that
    // memoised, or a registry that was global, would have two entries here.
    let a_session = open_session(&a, &fx).await;
    let _b_session = open_session(&b, &fx).await;

    // NON-VACUITY FIRST: the id is real, live, and reachable by its OWNER.
    // Without this, "B is refused" could just mean the id was malformed.
    assert!(
        a.mount().resolve(a_session.0.as_ref()).is_some(),
        "A's own mount resolves A's session — otherwise the refusal below \
         proves nothing"
    );

    // ── THE ATTACK ────────────────────────────────────────────────────────
    let across = bounded("B prompting A's session", b.prompt(prompt_for(&a_session)))
        .await
        .expect_err("B must not be able to prompt A's session");
    let control = bounded("B prompting a never-minted id", b.prompt(prompt_for(&never_minted())))
        .await
        .expect_err("a never-minted id is also refused");

    assert_eq!(
        serde_json::to_value(&across).unwrap(),
        serde_json::to_value(&control).unwrap(),
        "ABSENCE, NOT DENIAL: reaching for another tenant's session must be \
         byte-identical to reaching for one that never existed — anything else \
         confirms it exists"
    );

    // And nothing was constructed on B's side by the attempt.
    assert_eq!(b.mount().len(), 1, "B still has exactly its own session");
    assert_eq!(a.mount().len(), 1, "A is untouched");

    println!("\n=== c4: B's error for A's live session ===\n  {across:?}\n=== end ===\n");
}

/// **c4, absence on the listing surface.** A is not in B's `session/list`.
#[tokio::test]
async fn tenant_a_is_absent_from_tenant_bs_session_list() {
    let fx = Fixture::new("list");
    let (a, b) = two_tenants(&fx).await;

    let a_session = open_session(&a, &fx).await;
    let b_session = open_session(&b, &fx).await;

    let listed = bounded(
        "B's session/list",
        b.list_sessions(plexus_acp::v1::schema::ListSessionsRequest::new()),
    )
    .await
    .expect("session/list");

    let ids: Vec<String> = listed
        .sessions
        .iter()
        .map(|s| s.session_id.0.to_string())
        .collect();

    // Non-vacuity: B's list is not simply empty.
    assert_eq!(
        ids,
        vec![b_session.0.to_string()],
        "B sees exactly its own session"
    );
    assert!(
        !ids.contains(&a_session.0.to_string()),
        "A is ABSENT from B's listing, not merely refused: {ids:?}"
    );
}

/// **c4, absence on the Connectome.** A's session id is not in the bytes of
/// B's rendered edge — and the template still is, so this cannot pass because
/// the edge rendered nothing.
///
/// This is PLX-127 c2's construction at the ACP layer, and it matters for
/// PLX-127's exact reason: `connectome` takes **no `AuthContext`**, so anything
/// named there is named to everyone who can read it.
#[tokio::test]
async fn tenant_as_session_is_absent_from_tenant_bs_connectome() {
    let fx = Fixture::new("connectome");
    let (a, b) = two_tenants(&fx).await;

    let a_session = open_session(&a, &fx).await;
    let b_session = open_session(&b, &fx).await;

    let rendered = serde_json::to_string(&b.acp_connectome_edge()).expect("serialize the edge");

    // Non-vacuity: the edge really did render the Indexed family.
    assert!(
        rendered.contains("sessionId"),
        "the edge carries its id_field, so an absence below is a real absence: {rendered}"
    );

    assert!(
        !rendered.contains(a_session.0.as_ref()),
        "A's session id must not appear in B's Connectome: {rendered}"
    );
    // And B's own id is absent too — the Indexed family is ONE TEMPLATE, never
    // an enumeration. If instance ids appeared here at all, the assertion above
    // would be about which ids leak rather than about there being none.
    assert!(
        !rendered.contains(b_session.0.as_ref()),
        "an Indexed edge renders a template and NO instance ids: {rendered}"
    );

    println!("\n=== c4: B's rendered ACP edge ===\n  {rendered}\n=== end ===\n");
}

/// **c4, the notification path.** `session/cancel` is a notification and never
/// errors, so "B was refused" is not observable there — which makes it exactly
/// the method where a leak would be silent. The assertion is on the effect.
#[tokio::test]
async fn tenant_bs_cancel_cannot_touch_tenant_as_session() {
    let fx = Fixture::new("cancel");
    let (a, b) = two_tenants(&fx).await;

    let a_session = open_session(&a, &fx).await;
    let _b_session = open_session(&b, &fx).await;

    let a_runtime = a
        .mount()
        .resolve(a_session.0.as_ref())
        .expect("A's own session");

    // B cancels A's session id. ACP says this cannot error, so the only
    // observable is whether it reached anything.
    bounded(
        "B cancelling A's session",
        b.cancel(CancelNotification::new(a_session.clone())),
    )
    .await
    .expect("session/cancel never errors");

    // A's session is still there, and it is still the SAME OBJECT — nothing
    // was replaced, removed, or reconstructed on A's side by B's call.
    let after = a
        .mount()
        .resolve(a_session.0.as_ref())
        .expect("A's session survives B's cancel");
    assert!(
        Arc::ptr_eq(&a_runtime, &after),
        "B's cancel must not have reached anything of A's"
    );
    assert_eq!(a.mount().len(), 1);
}

/// The regression test for a bug the escape test above found.
///
/// `ClaudeCodeAcpAgent::mint` originally used a **per-agent** counter, so two
/// tenants in one process both minted `acp-{pid}-1`. Nothing crossed — B's
/// mount resolved B's own session — but the id had stopped identifying a
/// session process-wide, and `tenant_b_cannot_reach_tenant_as_acp_session`
/// failed because "A's id" was also B's id.
///
/// A per-agent counter is the natural thing to write, so this pins the property
/// rather than the implementation: **no two agents in one process mint the same
/// id**, whatever the mechanism.
#[tokio::test]
async fn two_tenants_in_one_process_never_mint_the_same_session_id() {
    let fx = Fixture::new("mint");
    let (a, b) = two_tenants(&fx).await;

    let mut ids = std::collections::BTreeSet::new();
    for _ in 0..4 {
        // Interleaved on purpose: the original bug was invisible if one agent
        // minted all of its ids first and the other's were compared as a set.
        assert!(ids.insert(open_session(&a, &fx).await.0.to_string()));
        assert!(
            ids.insert(open_session(&b, &fx).await.0.to_string()),
            "two agents in one process minted the same session id — the ids no \
             longer identify a session, which is how the per-agent counter bug \
             presented"
        );
    }
    assert_eq!(ids.len(), 8);
}

/// **The two lines PLX-138 promised, asserted through the trait it named.**
///
/// PLX-138 shipped `SessionMount` with `connectome_edge()` and `resolve()` and
/// no consumer — its own report said "ACP·F wires them in two lines". This
/// asserts the wiring exists where it was meant to: on
/// `plexus_core::plexus::Activation`, which is what a hub actually calls.
#[tokio::test]
async fn the_acp_mount_renders_as_an_indexed_edge_through_the_activation_trait() {
    use plexus_core::plexus::Activation;

    let fx = Fixture::new("edge");
    let (a, _b) = two_tenants(&fx).await;
    let _session = open_session(&a, &fx).await;

    let edge = Activation::connectome_edge(&a).expect("the ACP mount renders an edge");
    let rendered = serde_json::to_string(&edge).expect("serialize");

    assert!(
        rendered.contains("sessionId"),
        "RFC 002 §5.1's id_field, from the mount rather than from this test: {rendered}"
    );
    assert_eq!(
        serde_json::to_value(&edge).unwrap(),
        serde_json::to_value(a.acp_connectome_edge()).unwrap(),
        "the trait's answer IS the mount's answer — one source, not two"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// The probe — proving the tests above can see a leak
// ═══════════════════════════════════════════════════════════════════════════

/// **The permanent non-vacuity probe.** Break isolation on purpose, and assert
/// the *identical* attack succeeds.
///
/// PLX-151's finding is the reason this exists: its first attack script went
/// **green under the mutation on a real leak**, because `grep -rl` prints paths
/// rather than contents. A test that cannot observe the thing it asserts is
/// worse than no test.
///
/// # Choosing the mutation, and a structural fact found on the way
///
/// The obvious mutation — give the two agents **one** `SessionMount` — turns
/// out to be *impossible to write*. `SessionMount` is not `Clone`, its
/// `SessionStore` is private to it, and `ClaudeCodeAcpAgent` builds its own in
/// `new`. So two agents cannot share a mount by any composition a caller can
/// express. That is a stronger result than a probe, and it is the reason the
/// mutation below is the one it is.
///
/// What remains reachable is the **single fact a process-global registry would
/// make true**: A's session present in B's store. `SessionMount::insert` is the
/// one public door to that, so the mutation walks through it. Everything
/// downstream — the listing, the resolution, the byte-identity comparison — is
/// then exercised exactly as the escape tests exercise it.
///
/// It runs on every suite run rather than being a one-off, so a change that
/// silently removes the escape tests' *ability to see* a leak turns this red.
#[tokio::test]
async fn the_escape_tests_detect_a_leak_when_a_session_crosses_the_mount() {
    let fx = Fixture::new("probe");
    let (a, b) = two_tenants(&fx).await;

    let a_session = open_session(&a, &fx).await;
    let _b_session = open_session(&b, &fx).await;

    // ── THE MUTATION ──────────────────────────────────────────────────────
    // Exactly what a process-global session registry would produce, and
    // nothing else: A's live session, reachable from B's mount.
    let a_runtime = a
        .mount()
        .resolve(a_session.0.as_ref())
        .expect("A's own session");
    b.mount().insert(&a_session, a_runtime);

    // ── THE IDENTICAL ATTACK ──────────────────────────────────────────────
    // 1. The listing surface.
    let listed = bounded(
        "B's session/list under the mutation",
        b.list_sessions(plexus_acp::v1::schema::ListSessionsRequest::new()),
    )
    .await
    .expect("session/list");
    assert!(
        listed.sessions.iter().any(|s| s.session_id.0 == a_session.0),
        "THE PROBE FAILED TO OBSERVE A LEAK: A must be visible in B's listing \
         once its session is in B's mount. If this fires, \
         `tenant_a_is_absent_from_tenant_bs_session_list` is passing for a \
         reason other than isolation."
    );

    // 2. The reach surface. Under isolation this is `Err(unknown sessionId)`,
    //    byte-identical to the never-minted control. Under the mutation B
    //    RESOLVES it, so it stops being that error — which is precisely what
    //    the escape test's byte-identity assertion is comparing.
    let across = bounded(
        "B prompting A's session under the mutation",
        b.prompt(prompt_for(&a_session)),
    )
    .await;
    assert!(
        across.is_ok(),
        "THE PROBE FAILED TO OBSERVE A LEAK: B must resolve A's session once it \
         is in B's mount, so `tenant_b_cannot_reach_tenant_as_acp_session`'s \
         byte-identity assertion is measuring resolution rather than passing \
         vacuously. Got: {across:?}"
    );
    let control = bounded(
        "B prompting a never-minted id under the mutation",
        b.prompt(prompt_for(&never_minted())),
    )
    .await;
    assert!(
        control.is_err(),
        "a never-minted id is still refused even under the mutation, so the \
         difference above is about THIS session and not about B being broken"
    );
}
