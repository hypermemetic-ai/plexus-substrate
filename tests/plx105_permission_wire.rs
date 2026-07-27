//! PLX-105 — what the spawned Claude CLI actually reads back from
//! `loopback.permit`, measured end to end.
//!
//! # Why this file exists
//!
//! `claudecode_loopback::permit` is the tool a Claude CLI is pointed at by
//! `--permission-prompt-tool`. The CLI parses **`content[0].text` of the MCP
//! tool result** as its permission decision — that is the CLI's protocol, not
//! ours. PLX-116 measured that **no test anywhere asserted `content[0].text`**
//! and deliberately left `permit` unconverted rather than change its shape
//! blind.
//!
//! These tests close that hole from the substrate side: they drive the real
//! activation through `Activation::call_arc` — the dispatch path both gateways
//! use — and run the resulting `PlexusStream` through the **same** rule
//! `ServerHandler::call_tool` uses
//! (`plexus_transport::mcp::bridge::render_tool_text`, reached via the
//! `tool_text` testing hook). Nothing is re-described; the assertion is on the
//! bytes.
//!
//! # What they found
//!
//! The permission payload no longer arrived as the bare JSON object the CLI
//! contract requires. Since the M2 turn switchover, a `#[method]` that returns
//! `impl Stream` is projected onto the legacy stream as **the yielded item as a
//! `Data` update plus the turn's terminal as a second `Data`** — and the MCP
//! gateway buffered both, because it never read `content_type`. Two buffered
//! items of mixed type render as a pretty-printed JSON **array**.
//!
//! So `content[0].text` was a JSON array whose element 0 was the permission
//! payload, not the payload itself.
//!
//! # PLX-145 — restored, and this file now says so
//!
//! The invitation PLX-105 left ("if this ever becomes an object again, the CLI
//! contract has been restored and this test should say so") is taken up here.
//! `plexus_transport::mcp::bridge::tool_payload` now reads `content_type` and
//! strips the turn projection's `{"stop":…,"value":…}` envelope for a
//! `complete` terminal with no value — so `permit`'s single yielded string is
//! the only buffered item again and `content[0].text` is the bare object. The
//! decision, and what it changes for every other MCP tool, is documented on
//! `tool_payload` and pinned in `plexus-transport/tests/mcp_tool_text.rs`.
//!
//! **Nothing about `permit` itself changed.** The poll loop, the correlation,
//! and the shape of what it yields are exactly as PLX-105 left them; this was a
//! rendering contract, not a re-plumb. The re-plumb remains blocked on PLX-146.

use plexus_core::plexus::Activation;
use plexus_substrate::activations::claudecode_loopback::{
    ApprovalStatus, ClaudeCodeLoopback, LoopbackStorageConfig,
};
use plexus_transport::mcp::bridge::testing::{tool_text, tool_text_and_terminal};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

async fn loopback() -> Arc<ClaudeCodeLoopback> {
    let dir = std::env::temp_dir().join(format!("plx105_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    Arc::new(
        ClaudeCodeLoopback::new(LoopbackStorageConfig {
            db_path: dir.join("loopback.db"),
        })
        .await
        .expect("loopback"),
    )
}

/// Play the parent: wait for `permit` to have created its approval, then answer.
///
/// This is the half `respond` serves in production. It exists here because
/// `permit` mints its own approval id, so a test cannot pre-resolve one.
fn answer_the_next_request(
    lb: Arc<ClaudeCodeLoopback>,
    approve: bool,
    message: Option<String>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let storage = lb.storage();
        for _ in 0..200 {
            if let Ok(pending) = storage.list_pending(None).await {
                if let Some(a) = pending.first() {
                    storage
                        .resolve_approval(&a.id, approve, message.clone())
                        .await
                        .expect("resolve");
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("no approval request was ever created");
    })
}

/// The rendered text **and** the raw turn terminal the gateway stripped.
///
/// PLX-145: once the envelope stops reaching `content[0].text`, a test can no
/// longer read `stop.kind` out of the text — but PLX-105's finding that a
/// denial is a *successful* turn is a fact about the stream, and it must keep
/// being asserted at the layer where it is true.
async fn permit_text_and_terminal(lb: &Arc<ClaudeCodeLoopback>) -> (String, Value) {
    let stream = Activation::call_arc(
        lb.clone(),
        "permit",
        json!({
            "tool_name": "Bash",
            "tool_use_id": "toolu_plx105",
            "input": {"command": "echo hi"}
        }),
        None,
        None,
    )
    .await
    .expect("permit dispatched");

    let (text, terminal) = tool_text_and_terminal(stream).await;
    (
        text.expect("permit is never an MCP error"),
        terminal.expect("a turn-native method always emits exactly one terminal"),
    )
}

// ---------------------------------------------------------------------------
// The CLI-facing payload
// ---------------------------------------------------------------------------

/// **The contract, restored and pinned.** `content[0].text` for the permission
/// path is exactly the bare object the spawned Claude CLI requires — no array,
/// no turn envelope, byte for byte what `permit` yielded.
///
/// This is PLX-145's c2, driven through the real loopback (`Activation::call_arc`,
/// the dispatch path both gateways use) and the real gateway rule
/// (`tool_payload` + `render_tool_text`, the same two functions `call_tool`
/// calls). Nothing here is re-described; the assertion is on the bytes.
#[tokio::test]
async fn an_approval_reaches_the_cli_as_the_bare_object_its_contract_requires() {
    let lb = loopback().await;
    let answer = answer_the_next_request(lb.clone(), true, None);
    let (text, terminal) = permit_text_and_terminal(&lb).await;
    answer.await.unwrap();

    // The whole contract, stated as one equality: the text IS the payload.
    assert_eq!(
        text,
        json!({"behavior": "allow", "updatedInput": {"command": "echo hi"}}).to_string(),
        "content[0].text must be the permission object alone"
    );

    let parsed: Value = serde_json::from_str(&text).unwrap_or_else(|e| {
        panic!("content[0].text was not JSON at all: {e}\n{text}");
    });
    assert!(
        parsed.is_object(),
        "MEASURED, not assumed: the CLI's `--permission-prompt-tool` contract is \
         an object with a `behavior` key. PLX-105 measured an ARRAY here — the \
         turn projection's terminal buffered alongside the payload — and PLX-145 \
         taught the gateway to strip that envelope. If this ever becomes an array \
         again the contract has re-broken.\ntext = {text}"
    );
    assert_eq!(parsed["behavior"], "allow");
    assert_eq!(parsed["updatedInput"]["command"], "echo hi");

    // The terminal is still emitted — it is *stripped*, not suppressed. The turn
    // model is intact upstream of the gateway; only the rendering changed.
    assert_eq!(terminal["stop"]["kind"], "complete");
    assert_eq!(terminal["value"], Value::Null);
}

/// A denial travels the same road and, crucially, **as a success**.
///
/// This is PLX-116's finding (c) asserted rather than asserted-about: nothing in
/// this subsystem maps a refusal onto `Failed`. `respond(approve = false)`
/// succeeds and the "no" arrives as an `Ok`-valued payload. There is no
/// conflation to un-do — a refusal simply has no representation on this wire.
#[tokio::test]
async fn a_denial_is_a_successful_turn_and_not_a_refusal() {
    let lb = loopback().await;
    let answer = answer_the_next_request(lb.clone(), false, Some("nope".into()));
    let (text, terminal) = permit_text_and_terminal(&lb).await;
    answer.await.unwrap();

    // PLX-145: the denial reaches the CLI as a bare object too — same fix, same
    // contract. It reads out of the text directly now instead of out of element
    // 0 of an array.
    let payload: Value = serde_json::from_str(&text).expect("payload");
    assert_eq!(payload["behavior"], "deny");
    assert_eq!(payload["message"], "nope");
    assert!(payload.is_object(), "not an array wrapper: {text}");

    // PLX-145: the stop-kind assertions below moved from the rendered text to
    // the raw terminal, because the gateway now strips a `complete` terminal
    // before rendering. The finding is unchanged and is asserted at the layer
    // where it is actually true — the stream, not the text.
    //
    // The heart of it. RFC 002 §6.6 makes `Refused` distinct from `Failed`
    // precisely so a considered "no" is legible; PLX-112 made `Refused`
    // *expressible* by a generated handler. It is still not *reached* here,
    // because `permit` resolves the denial into a success-valued payload
    // rather than terminating the turn. PLX-145 did not change that, and could
    // not have: the refusal is lost inside `permit`'s body, upstream of every
    // line this build touched.
    assert_eq!(
        terminal["stop"]["kind"], "complete",
        "a denied permission still terminates the turn as `complete`"
    );
    assert_ne!(terminal["stop"]["kind"], "refused");
    assert_ne!(
        terminal["stop"]["kind"], "failed",
        "and it is NOT conflated with a failure either — there is nothing to un-conflate"
    );
    assert!(
        terminal["stop"].get("error").is_none(),
        "RFC 002 §6.7.1: a non-Failed terminal carries no structured error"
    );

    // And the corollary PLX-145 must state: because the turn completes, the
    // gateway strips its envelope, so the CLI sees the denial payload alone.
    // Were `permit` ever fixed to terminate `Refused`, `tool_payload` would pass
    // the envelope through whole instead — pinned in
    // `plexus-transport/tests/mcp_tool_text.rs`.
    assert!(
        !text.contains("\"stop\""),
        "a completed turn's envelope never reaches the tool caller: {text}"
    );
}

/// `respond(approve = false)` is itself a success, at the method boundary.
///
/// The other half of the same fact: the *decision* method does not fail either,
/// so a denial is never an `Err` anywhere along the path.
#[tokio::test]
async fn respond_with_approve_false_succeeds() {
    let lb = loopback().await;
    let storage = lb.storage();
    let approval = storage
        .create_approval("sess", "Bash", "toolu_x", &json!({"command": "rm -rf /"}))
        .await
        .expect("create");

    let stream = Activation::call_arc(
        lb.clone(),
        "respond",
        json!({"approval_id": approval.id, "approve": false, "message": "absolutely not"}),
        None,
        None,
    )
    .await
    .expect("respond dispatched");

    let text = tool_text(stream)
        .await
        .expect("a denial is not an MCP error");
    let parsed: Value = serde_json::from_str(&text).expect("JSON");

    // PLX-145, change #1 asserted on a live tool rather than only in the unit
    // tests: `respond` is a unary `Result<RespondOk, _>`, so it emits exactly one
    // `Data` item — its terminal. Its text WAS the envelope
    // `{"stop":{"kind":"complete"},"value":{"approval_id":…}}`; it is now the
    // `RespondOk` the method declared. Every unary MCP tool moved this way.
    assert!(parsed.is_object(), "one buffered item, so no array wrapper: {text}");
    assert!(
        parsed.get("stop").is_none(),
        "the turn envelope no longer reaches a unary tool's caller: {text}"
    );
    assert_eq!(
        parsed["approval_id"], approval.id.to_string(),
        "the text is the method's own return value: {text}"
    );

    assert_eq!(
        storage.get_approval(&approval.id).await.unwrap().status,
        ApprovalStatus::Denied
    );
}
