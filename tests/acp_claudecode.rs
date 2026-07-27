//! PLX-140 (ACP·F) — `claudecode` as the reference ACP agent, over real pipes.
//!
//! # This file plays the editor, out of process, in hand-rolled JSON
//!
//! Every test here spawns **`acp-stdio`** — plexus-substrate's own binary, not
//! plexus-acp's probe — as a real subprocess and talks NDJSON to it over real
//! pipes. Nothing is shared but two file descriptors.
//!
//! The client side is hand-rolled JSON rather than the ACP SDK's client, for
//! PLX-139's reason: if both ends used the same library a framing mistake would
//! cancel itself out.
//!
//! # What is real, and what is substituted
//!
//! **Real:** the ACP transport, the session runtime and its `Indexed` edge, the
//! typed client handle, a real `plexus_core::runtime::entry` turn, substrate's
//! `ClaudeCodeExecutor` (it really `spawn`s a child process and really parses
//! its stream-json), the real `ClaudeCodeStorage` over real sqlite, the real
//! `ChatEvent` stream, and `ChatEventProjection`.
//!
//! **Substituted: only the model.** `PLEXUS_ACP_CLAUDE_BIN` points the executor
//! at a scripted `claude` that emits a fixed stream-json transcript. That is
//! the same line `tests/tenant_confinement.rs` draws, for the same reasons: a
//! live model is probabilistic and needs credentials a test must not go looking
//! for. Here it is *stronger* than a live model, because it emits a
//! `tool_use`/`tool_result` pair on every run and a live one might not.
//!
//! # Every await carries a timeout
//!
//! PLX-146's rule. A hang is the failure mode of a transport *and* of a
//! callback, and a test that waits forever reports nothing. Every read from the
//! child is inside [`bounded`], and a timeout is a loud, named failure.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};

const LIMIT: Duration = Duration::from_secs(60);

/// The option ids `ClaudeCodeAcpAgent` offers. Spelled here as literals on
/// purpose: if the agent renames one, this file must be updated deliberately
/// rather than following along.
const ALLOW: &str = "allow-launch";
const DENY: &str = "reject-launch";

async fn bounded<F: std::future::Future>(what: &str, fut: F) -> F::Output {
    tokio::time::timeout(LIMIT, fut)
        .await
        .unwrap_or_else(|_| panic!("HUNG: {what} did not resolve within {LIMIT:?}"))
}

// ═══════════════════════════════════════════════════════════════════════════
// The scripted CLI — the only substituted part
// ═══════════════════════════════════════════════════════════════════════════

/// Write a `claude` stand-in that emits a fixed stream-json transcript.
///
/// It emits one of **each** `ChatEvent` shape the projection has a row for —
/// text, thinking, a tool call and its result — so a single prompt exercises
/// the whole table rather than only the easy row.
///
/// # A pre-existing claudecode gap this script pins rather than papers over
///
/// The last `tool_result` below is emitted inside a `"type":"user"` event, the
/// way the real CLI reports one — and **claudecode drops it**:
/// `claudecode/activation.rs:483` is `RawClaudeEvent::User { .. } => {}`, so no
/// `ChatEvent` is ever produced for it. That is not an ACP defect and this
/// build did not change it; converting it would be a change to the legacy
/// `claudecode.chat` stream every existing consumer reads. It is scripted here
/// so the gap is **asserted** (the `tu-ignored` call never reaches the editor)
/// rather than merely known, and so it turns into a red test if someone fixes
/// it without updating the projection's expectations.
///
/// It also **records its own argv** to `argv.txt`, which is what
/// `the_acp_path_carries_no_loopback_correlation` reads. That makes the c3
/// assertion a measurement of the process that was actually spawned rather
/// than a reading of the source.
fn write_scripted_cli(dir: &Path) -> PathBuf {
    let path = dir.join("claude");
    let argv_log = dir.join("argv.txt");
    let script = format!(
        r#"#!/bin/sh
# The scripted adversary-free CLI. See tests/acp_claudecode.rs.
printf '%s\n' "$*" >> '{argv}'
env >> '{argv}.env'
cat <<'EOF'
{{"type":"system","subtype":"init","session_id":"claude-sess-1","model":"sonnet"}}
{{"type":"assistant","message":{{"id":"m1","role":"assistant","content":[{{"type":"thinking","thinking":"considering the request"}}]}}}}
{{"type":"assistant","message":{{"id":"m2","role":"assistant","content":[{{"type":"text","text":"hello from the scripted cli"}}]}}}}
{{"type":"assistant","message":{{"id":"m3","role":"assistant","content":[{{"type":"tool_use","id":"tu-1","name":"Read","input":{{"path":"/etc/hostname"}}}}]}}}}
{{"type":"assistant","message":{{"id":"m4","role":"assistant","content":[{{"type":"tool_result","tool_use_id":"tu-1","content":"a-host","is_error":false}}]}}}}
{{"type":"user","message":{{"id":"m5","role":"user","content":[{{"type":"tool_result","tool_use_id":"tu-ignored","content":"never-seen","is_error":false}}]}}}}
{{"type":"result","subtype":"success","session_id":"claude-sess-1","is_error":false,"num_turns":1,"result":"done"}}
EOF
"#,
        argv = argv_log.display()
    );
    let mut file = std::fs::File::create(&path).expect("create scripted cli");
    file.write_all(script.as_bytes()).expect("write scripted cli");
    drop(file);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("chmod scripted cli");
    }
    path
}

/// A temp directory that cleans up after itself.
struct Fixture {
    base: PathBuf,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let base = std::env::temp_dir().join(format!(
            "plx140-{label}-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&base).expect("fixture dir");
        Self { base }
    }
    fn base(&self) -> &Path {
        &self.base
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// The editor
// ═══════════════════════════════════════════════════════════════════════════

struct Editor {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Lines<BufReader<ChildStdout>>,
    stderr: Option<ChildStderr>,
    next_id: i64,
    /// Every line the service sent, verbatim. The evidence for c1 and c2.
    transcript: Vec<String>,
}

impl Editor {
    fn spawn(fx: &Fixture) -> Self {
        let cli = write_scripted_cli(fx.base());
        let mut child = Command::new(env!("CARGO_BIN_EXE_acp-stdio"))
            .env("PLEXUS_ACP_STATE_DIR", fx.base())
            .env("PLEXUS_ACP_CLAUDE_BIN", &cli)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn acp-stdio");

        let stdin = child.stdin.take().expect("child stdin");
        let stdout = child.stdout.take().expect("child stdout");
        let stderr = child.stderr.take();

        Self {
            child,
            stdin: Some(stdin),
            stdout: BufReader::new(stdout).lines(),
            stderr,
            next_id: 1,
            transcript: Vec::new(),
        }
    }

    async fn request(&mut self, method: &str, params: Value) -> i64 {
        let id = self.next_id;
        self.next_id += 1;
        self.write(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
            .await;
        id
    }

    async fn notify(&mut self, method: &str, params: Value) {
        self.write(&json!({"jsonrpc":"2.0","method":method,"params":params}))
            .await;
    }

    async fn answer(&mut self, id: &Value, result: Value) {
        self.write(&json!({"jsonrpc":"2.0","id":id,"result":result}))
            .await;
    }

    async fn write(&mut self, message: &Value) {
        let mut line = serde_json::to_string(message).unwrap();
        assert!(!line.contains('\n'), "a frame must be exactly one line: {line}");
        line.push('\n');
        let stdin = self.stdin.as_mut().expect("stdin is still open");
        bounded("writing to the child", stdin.write_all(line.as_bytes()))
            .await
            .expect("write");
        bounded("flushing to the child", stdin.flush()).await.expect("flush");
    }

    /// Read the next NDJSON frame, asserting the framing as we go.
    ///
    /// Every line the service writes must parse, so anything that leaked onto
    /// the protocol channel fails the very next read rather than being
    /// tolerated. `plexus-substrate` drags a much larger dependency graph than
    /// PLX-139's probe did, which makes this a stronger check here than there.
    async fn next_frame(&mut self, why: &str) -> Value {
        let line = bounded(why, self.stdout.next_line())
            .await
            .expect("reading the child's stdout")
            .unwrap_or_else(|| panic!("the service closed stdout while waiting for {why}"));
        self.transcript.push(line.clone());
        let frame: Value = serde_json::from_str(&line).unwrap_or_else(|error| {
            panic!(
                "NDJSON FRAMING VIOLATION while waiting for {why}: not JSON ({error}) — \
                 something wrote to the protocol channel:\n  {line}"
            )
        });
        assert_eq!(frame["jsonrpc"], "2.0", "every frame is JSON-RPC 2.0; got {line}");
        frame
    }

    async fn response_to(&mut self, id: i64, why: &str) -> (Value, Vec<Value>) {
        let mut before = Vec::new();
        loop {
            let frame = self.next_frame(why).await;
            if frame.get("id") == Some(&json!(id)) && frame.get("method").is_none() {
                return (frame, before);
            }
            before.push(frame);
        }
    }

    async fn incoming_request(&mut self, method: &str) -> (Value, Vec<Value>) {
        let mut before = Vec::new();
        loop {
            let frame = self.next_frame(method).await;
            if frame.get("method") == Some(&json!(method)) && frame.get("id").is_some() {
                return (frame, before);
            }
            before.push(frame);
        }
    }

    async fn initialize(&mut self) -> Value {
        let id = self
            .request(
                "initialize",
                json!({"protocolVersion": 1, "clientCapabilities": {}}),
            )
            .await;
        let (response, _) = self.response_to(id, "the initialize response").await;
        response
    }

    async fn open(&mut self, cwd: &Path) -> String {
        self.initialize().await;
        let id = self
            .request(
                "session/new",
                json!({"cwd": cwd.to_string_lossy(), "mcpServers": []}),
            )
            .await;
        let (response, _) = self.response_to(id, "the session/new response").await;
        response["result"]["sessionId"]
            .as_str()
            .expect("the AGENT mints the session id")
            .to_owned()
    }

    fn print_transcript(&self, title: &str) {
        println!("\n=== {title} — every byte the service sent ===");
        for line in &self.transcript {
            println!("  {line}");
        }
        println!("=== end ===\n");
    }

    async fn shutdown(mut self) -> String {
        drop(self.stdin.take());
        let mut diagnostics = String::new();
        if let Some(mut stderr) = self.stderr.take() {
            let _ = bounded(
                "draining stderr",
                tokio::io::AsyncReadExt::read_to_string(&mut stderr, &mut diagnostics),
            )
            .await;
        }
        let _ = bounded("the service to exit", self.child.wait()).await;
        diagnostics
    }
}

fn selected(option: &str) -> Value {
    json!({"outcome": {"outcome": "selected", "optionId": option}})
}

/// The `session/update` notifications inside a batch of frames, in order.
fn updates(frames: &[Value]) -> Vec<&Value> {
    frames
        .iter()
        .filter(|f| f.get("method") == Some(&json!("session/update")))
        .collect()
}

// ═══════════════════════════════════════════════════════════════════════════
// c1 — the reference agent works
// ═══════════════════════════════════════════════════════════════════════════

/// **c1.** An editor-shaped client spawns the service, opens a session,
/// prompts, and receives streamed `session/update` notifications terminated by
/// a `stopReason`.
///
/// The assertion is on the **order and content** of the notifications, not
/// merely that some arrived: the projection table's four live rows each have to
/// show up, in the order claudecode emitted them, and the response has to come
/// after all of them.
#[tokio::test]
async fn an_editor_spawns_claudecode_and_gets_a_streamed_turn() {
    let fx = Fixture::new("c1");
    let mut editor = Editor::spawn(&fx);
    let session = editor.open(fx.base()).await;

    let id = editor
        .request(
            "session/prompt",
            json!({"sessionId": session, "prompt": [{"type": "text", "text": "say hello"}]}),
        )
        .await;

    // The agent asks the EDITOR for permission before it launches. This is the
    // right party, and it is the whole difference from the loopback.
    let (ask, before_ask) = editor.incoming_request("session/request_permission").await;
    assert_eq!(
        ask["params"]["sessionId"], session,
        "the ask is scoped to the session that is prompting"
    );
    assert!(
        updates(&before_ask).is_empty(),
        "nothing has streamed yet — the launch has not been approved"
    );
    editor.answer(&ask["id"], selected(ALLOW)).await;

    let (response, before) = editor.response_to(id, "the session/prompt response").await;
    editor.print_transcript("c1: a full claudecode turn over ACP");

    let notes = updates(&before);
    assert!(
        notes.len() >= 4,
        "every live row of the projection table should have streamed; got {}",
        notes.len()
    );

    // The order is claudecode's, preserved: thinking, then text, then the tool
    // call, then its result.
    let kinds: Vec<&str> = notes
        .iter()
        .filter_map(|n| n["params"]["update"]["sessionUpdate"].as_str())
        .collect();
    assert_eq!(
        kinds,
        vec![
            "agent_thought_chunk",
            "agent_message_chunk",
            "tool_call",
            "tool_call_update"
        ],
        "the projection table's rows, in claudecode's own order"
    );

    // `tool_use_id` IS `toolCallId` — no translation layer, which is what makes
    // the update land on the call rather than beside it.
    assert_eq!(notes[2]["params"]["update"]["toolCallId"], "tu-1");
    assert_eq!(notes[3]["params"]["update"]["toolCallId"], "tu-1");
    assert_eq!(notes[3]["params"]["update"]["status"], "completed");
    // The `user`-shaped tool_result the script also emits is absent, because
    // claudecode drops `RawClaudeEvent::User`. See `write_scripted_cli`.
    assert!(
        !editor.transcript.iter().any(|l| l.contains("tu-ignored")),
        "claudecode drops user-shaped tool results; if this fires, that gap was \
         closed and this test's expectations need updating"
    );
    assert_eq!(
        notes[1]["params"]["update"]["content"]["text"],
        "hello from the scripted cli",
        "the model's text reached the editor verbatim"
    );

    // Terminated by a stopReason, and it is a SUCCESSFUL response.
    assert_eq!(response["result"]["stopReason"], "end_turn");
    assert!(
        response.get("error").is_none(),
        "a turn that finished is not an error: {response}"
    );

    editor.shutdown().await;
}

/// `session/list` answers, because the edge declares a `list_method` — and the
/// session it lists is the one the agent minted.
#[tokio::test]
async fn the_agent_advertises_and_serves_session_list() {
    let fx = Fixture::new("list");
    let mut editor = Editor::spawn(&fx);

    let init = editor.initialize().await;
    // ACP spells "supported" as `{}` here, not `true` — omitted or `null` mean
    // unsupported, and `{}` means supported with no sub-options. So the
    // assertion is on presence, and the `is_object` half is what distinguishes
    // it from `null`.
    let list = &init["result"]["agentCapabilities"]["sessionCapabilities"]["list"];
    assert!(
        list.is_object(),
        "session/list is advertised as {{}}, derived from the edge's list_method \
         (PLX-138 c4); got {list}"
    );

    let id = editor
        .request(
            "session/new",
            json!({"cwd": fx.base().to_string_lossy(), "mcpServers": []}),
        )
        .await;
    let (response, _) = editor.response_to(id, "session/new").await;
    let session = response["result"]["sessionId"].as_str().unwrap().to_owned();

    let id = editor.request("session/list", json!({})).await;
    let (response, _) = editor.response_to(id, "session/list").await;
    let listed: Vec<String> = response["result"]["sessions"]
        .as_array()
        .expect("sessions array")
        .iter()
        .map(|s| s["sessionId"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(listed, vec![session], "one session, and it is the one we opened");

    editor.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// c2 — a refusal reads as a refusal
// ═══════════════════════════════════════════════════════════════════════════

/// **c2.** A permission request round-trips through the typed handle, and a
/// DENIAL arrives with a `stopReason` distinguishable from an error —
/// **asserted on the wire**.
///
/// This is the first `StopKind::Refused` produced anywhere in
/// `plexus-substrate`. Before this build the crate's only two mentions of it
/// were doc comments recording its absence.
#[tokio::test]
async fn a_denial_reaches_the_editor_as_a_refusal_and_not_as_an_error() {
    let fx = Fixture::new("c2");
    let mut editor = Editor::spawn(&fx);
    let session = editor.open(fx.base()).await;

    let id = editor
        .request(
            "session/prompt",
            json!({"sessionId": session, "prompt": [{"type": "text", "text": "do something"}]}),
        )
        .await;

    let (ask, _) = editor.incoming_request("session/request_permission").await;

    // The options the AGENT offered. `PermissionDecision::classify` reads
    // allow-versus-deny off these, never off the id's spelling — so this
    // assertion is what makes selecting DENY below actually mean "no".
    let options = ask["params"]["options"].as_array().expect("options offered");
    let kinds: Vec<&str> = options.iter().filter_map(|o| o["kind"].as_str()).collect();
    assert_eq!(kinds, vec!["allow_once", "reject_once"]);
    assert_eq!(options[1]["optionId"], DENY);

    editor.answer(&ask["id"], selected(DENY)).await;

    let (response, before) = editor.response_to(id, "the refused prompt response").await;
    editor.print_transcript("c2: a denial on the wire");

    // RFC 002 §6.7.1, tested on the bytes: a refusal is a SUCCESSFUL response
    // carrying `refusal`, and it has no `error` key. A client can tell "the
    // user said no" from "the agent broke" without reading prose.
    assert_eq!(
        response["result"]["stopReason"], "refusal",
        "a considered no is a refusal: {response}"
    );
    assert!(
        response.get("error").is_none(),
        "RFC 002 §6.7.1: a refused terminal carries NO error: {response}"
    );
    assert!(
        response.get("result").is_some(),
        "a refusal travels on the Ok half: {response}"
    );

    // And the turn genuinely stopped: the scripted CLI's transcript never
    // streamed, because the launch never happened.
    assert!(
        updates(&before).is_empty(),
        "a denied launch streams nothing: {before:?}"
    );

    editor.shutdown().await;
}

/// The non-vacuity twin: the identical exchange with ALLOW selected is
/// `end_turn`, not `refusal`.
///
/// Without this, `a_denial_reaches_the_editor_as_a_refusal_and_not_as_an_error`
/// would still pass if the agent refused *everything*.
#[tokio::test]
async fn the_same_exchange_with_allow_selected_is_not_a_refusal() {
    let fx = Fixture::new("c2-twin");
    let mut editor = Editor::spawn(&fx);
    let session = editor.open(fx.base()).await;

    let id = editor
        .request(
            "session/prompt",
            json!({"sessionId": session, "prompt": [{"type": "text", "text": "do something"}]}),
        )
        .await;
    let (ask, _) = editor.incoming_request("session/request_permission").await;
    editor.answer(&ask["id"], selected(ALLOW)).await;
    let (response, before) = editor.response_to(id, "the allowed prompt response").await;

    assert_eq!(response["result"]["stopReason"], "end_turn");
    assert!(
        !updates(&before).is_empty(),
        "an allowed launch does stream — so the denial test is measuring the DECISION"
    );

    editor.shutdown().await;
}

/// `session/cancel` reaches a prompt that is already running.
///
/// Built so a flag could not pass it: the prompt is parked on a permission ask
/// **nobody will answer**, so a session that was merely *marked* cancelled
/// would stay parked and [`bounded`] would report HUNG. PLX-138 c3's
/// construction, now over the wire against a real claudecode agent.
#[tokio::test]
async fn session_cancel_reaches_a_prompt_parked_on_a_permission_ask() {
    let fx = Fixture::new("cancel");
    let mut editor = Editor::spawn(&fx);
    let session = editor.open(fx.base()).await;

    let id = editor
        .request(
            "session/prompt",
            json!({"sessionId": session, "prompt": [{"type": "text", "text": "go"}]}),
        )
        .await;

    // Wait until the ask is genuinely in flight, then never answer it.
    let (_ask, _) = editor.incoming_request("session/request_permission").await;
    editor
        .notify("session/cancel", json!({"sessionId": session}))
        .await;

    let (response, _) = editor.response_to(id, "the cancelled prompt response").await;
    editor.print_transcript("cancel: a parked prompt reached by session/cancel");

    assert_eq!(
        response["result"]["stopReason"], "cancelled",
        "the TURN terminated, not merely the session flagged: {response}"
    );
    assert!(
        response.get("error").is_none(),
        "a cancelled turn is not an error either: {response}"
    );

    editor.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════
// c3 — no hand-rolled correlation on this path
// ═══════════════════════════════════════════════════════════════════════════

/// **c3, on the path this ticket creates.** The ACP path spawns the CLI with
/// **none** of the loopback's correlation machinery.
///
/// This is measured on the **process that was actually spawned** — the scripted
/// CLI records its own argv and environment — rather than read off the source,
/// because a source grep proves only that a string is absent from a file.
///
/// See `src/acp/mod.rs` for what this criterion does NOT close: the legacy
/// `claudecode.chat`-over-MCP path keeps its loopback, and PLX-105 still owns
/// deleting it.
#[tokio::test]
async fn the_acp_path_carries_no_loopback_correlation() {
    let fx = Fixture::new("c3");
    let mut editor = Editor::spawn(&fx);
    let session = editor.open(fx.base()).await;

    let id = editor
        .request(
            "session/prompt",
            json!({"sessionId": session, "prompt": [{"type": "text", "text": "go"}]}),
        )
        .await;
    let (ask, _) = editor.incoming_request("session/request_permission").await;
    editor.answer(&ask["id"], selected(ALLOW)).await;
    let (response, _) = editor.response_to(id, "the prompt response").await;
    assert_eq!(response["result"]["stopReason"], "end_turn");
    editor.shutdown().await;

    let argv = std::fs::read_to_string(fx.base().join("argv.txt"))
        .expect("the scripted CLI recorded its argv — so it really was spawned");
    let env = std::fs::read_to_string(fx.base().join("argv.txt.env")).unwrap_or_default();

    // NON-VACUITY FIRST: prove the recording works at all, and that this really
    // was a claudecode launch, before asserting anything is absent.
    assert!(
        argv.contains("--model"),
        "the recording must contain a real claudecode argv or the absences below \
         prove nothing; got: {argv}"
    );

    assert!(
        !argv.contains("--permission-prompt-tool"),
        "the ACP path does not route permission through an MCP tool; got: {argv}"
    );
    assert!(
        !argv.contains("loopback_permit"),
        "no loopback tool on this path; got: {argv}"
    );
    assert!(
        !argv.contains("session_id="),
        "no correlation smuggled through an MCP URL query param; got: {argv}"
    );
    assert!(
        !env.contains("PLEXUS_SESSION_ID"),
        "no correlation smuggled through an env var; got: {env}"
    );
    assert!(
        !env.contains("LOOPBACK_SESSION_ID"),
        "no correlation smuggled through the loopback's env var either; got: {env}"
    );

    println!("\n=== c3: the argv of the process the ACP path actually spawned ===");
    println!("{argv}");
    println!("=== end ===\n");
}

/// The author-facing permission path in `src/acp/agent.rs` has no correlation
/// code in it — asserted on the source, as c3's `grep` clause words it.
///
/// This is a **second, weaker** check that complements the process-level one
/// above: that one proves nothing correlational reached the child, this one
/// proves nobody re-introduced a poll loop or an id map in the agent itself.
#[test]
fn the_acp_agent_source_contains_no_correlation_machinery() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/acp/agent.rs"))
        .expect("read the agent source");

    // Prove the file is the one we think it is before asserting absences.
    assert!(
        source.contains("request_permission"),
        "this must be the agent that asks for permission"
    );

    for (needle, why) in [
        ("PLEXUS_SESSION_ID", "an env var used for correlation"),
        ("session_id=", "a query param used for correlation"),
        ("permission-prompt-tool", "the CLI flag that routes to the loopback"),
        ("AwaitingPermission", "a parallel status flag"),
        ("wait_for_approval", "the parent-side polling surface"),
        ("poll_interval", "a poll loop"),
    ] {
        for line in source.lines() {
            let code = line.trim_start();
            if code.starts_with("//") || code.starts_with("///") || code.starts_with("//!") {
                continue; // the module docs discuss all of these by name
            }
            assert!(
                !code.contains(needle),
                "PLX-140 c3: {why} ({needle}) is back in the ACP agent: {line}"
            );
        }
    }
}
