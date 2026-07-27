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
//! The permission payload no longer arrives as the bare JSON object the CLI
//! contract requires. Since the M2 turn switchover, a `#[method]` that returns
//! `impl Stream` is projected onto the legacy stream as **the yielded item as a
//! `Data` update plus the turn's terminal as a second `Data`** — and the MCP
//! gateway buffers both, because it never reads `content_type`. Two buffered
//! items of mixed type render as a pretty-printed JSON **array**.
//!
//! So `content[0].text` is today a JSON array whose element 0 is the permission
//! payload, not the payload itself. These tests pin that, so the fix — whichever
//! side it lands on — has to change an assertion deliberately.

use plexus_core::plexus::Activation;
use plexus_substrate::activations::claudecode_loopback::{
    ApprovalStatus, ClaudeCodeLoopback, LoopbackStorageConfig,
};
use plexus_transport::mcp::bridge::testing::tool_text;
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

async fn permit_text(lb: &Arc<ClaudeCodeLoopback>) -> String {
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

    tool_text(stream).await.expect("permit is never an MCP error")
}

// ---------------------------------------------------------------------------
// The CLI-facing payload
// ---------------------------------------------------------------------------

/// An approval reaches the CLI — but wrapped.
///
/// The permission payload the loopback yields is intact and is element 0. What
/// changed is the **top-level JSON type** of `content[0].text`: the CLI's
/// contract is an object with a `behavior` key, and it gets an array.
#[tokio::test]
async fn an_approval_reaches_the_cli_wrapped_in_the_turn_terminal() {
    let lb = loopback().await;
    let answer = answer_the_next_request(lb.clone(), true, None);
    let text = permit_text(&lb).await;
    answer.await.unwrap();

    let parsed: Value = serde_json::from_str(&text).unwrap_or_else(|e| {
        panic!("content[0].text was not JSON at all: {e}\n{text}");
    });

    assert!(
        parsed.is_array(),
        "MEASURED, not assumed: the turn projection emits the yielded payload as \
         a `.update` Data item and the terminal as a `.terminal` Data item, the \
         MCP gateway buffers both because it does not read `content_type`, and \
         two mixed-type items render as a JSON array. If this ever becomes an \
         object again, the CLI contract has been restored and this test should \
         say so.\ntext = {text}"
    );

    let items = parsed.as_array().unwrap();
    assert_eq!(items.len(), 2, "one update + one terminal");

    // Element 0 is the payload the CLI's protocol actually wants, unharmed.
    let payload: Value =
        serde_json::from_str(items[0].as_str().expect("the yielded item is a JSON string"))
            .expect("the payload is still well-formed permission JSON");
    assert_eq!(payload["behavior"], "allow");
    assert_eq!(payload["updatedInput"]["command"], "echo hi");

    // Element 1 is the turn terminal, which the CLI has no contract for.
    assert_eq!(items[1]["stop"]["kind"], "complete");
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
    let text = permit_text(&lb).await;
    answer.await.unwrap();

    let parsed: Value = serde_json::from_str(&text).expect("JSON");
    let items = parsed.as_array().expect("array, as above");

    let payload: Value = serde_json::from_str(items[0].as_str().unwrap()).expect("payload");
    assert_eq!(payload["behavior"], "deny");
    assert_eq!(payload["message"], "nope");

    // The heart of it. RFC 002 §6.6 makes `Refused` distinct from `Failed`
    // precisely so a considered "no" is legible; PLX-112 made `Refused`
    // *expressible* by a generated handler. It is still not *reached* here,
    // because `permit` resolves the denial into a success-valued payload
    // rather than terminating the turn.
    assert_eq!(
        items[1]["stop"]["kind"], "complete",
        "a denied permission still terminates the turn as `complete`"
    );
    assert_ne!(items[1]["stop"]["kind"], "refused");
    assert_ne!(
        items[1]["stop"]["kind"], "failed",
        "and it is NOT conflated with a failure either — there is nothing to un-conflate"
    );
    assert!(
        items[1]["stop"].get("error").is_none(),
        "RFC 002 §6.7.1: a non-Failed terminal carries no structured error"
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

    // The count-dependency, visible side by side with `permit`: a unary
    // `Result` method emits exactly ONE `Data` item (its terminal), so the text
    // is that object pretty-printed — an object, where `permit`'s is an array.
    // Same gateway, same rule, different top-level JSON type, decided purely by
    // how many items the method emitted.
    assert!(parsed.is_object(), "one buffered item, so no array wrapper: {text}");
    assert_eq!(parsed["stop"]["kind"], "complete");

    assert_eq!(
        storage.get_approval(&approval.id).await.unwrap().status,
        ApprovalStatus::Denied
    );
}
