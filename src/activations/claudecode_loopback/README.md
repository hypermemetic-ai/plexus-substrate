# claudecode_loopback

Route tool permissions to parent for approval.

## Overview

ClaudeCodeLoopback implements the server side of the loopback approval flow.
When a ClaudeCode session is launched with `loopback_enabled=true`, Claude
Code CLI is configured with `--permission-prompt-tool` pointing at an MCP
endpoint on this substrate. Every tool call then invokes
`loopback.permit(tool_name, tool_use_id, input)`, which **blocks** inside
the stream — polling storage — until an external approver calls
`loopback.respond(approval_id, approve, message)`.

Approved calls return `{"behavior":"allow","updatedInput":…}` (a JSON
**string**, not an object — required by the MCP permission-prompt contract).
Denials, timeouts (default 5 minutes), and creation failures return
`{"behavior":"deny","message":…}`.

`wait_for_approval(session_id, timeout_secs)` is a complementary method for
approvers: it blocks until a new approval arrives for that session (using a
per-session `tokio::sync::Notify`) so the approver does not have to poll.
`configure(session_id)` generates the MCP config block to hand to
`claudecode.create(loopback_session_id=…)`.

## Namespace

`loopback` — invoked via `synapse <backend> loopback.<method>`.

## Methods

| Method | Params | Returns | Description |
|---|---|---|---|
| `permit` | `tool_name: String, tool_use_id: String, input: Value, _connection: Option<Value>` | `Stream<Item=String>` | Permission-prompt handler — blocks polling storage until the approval resolves. Returns a stringified JSON response per the MCP contract. |
| `respond` | `approval_id: ApprovalId, approve: bool, message: Option<String>` | `Result<RespondOk, LoopbackError>` | Approve or deny a pending approval. |
| `pending` | `session_id: Option<String>` | `Result<PendingOk, LoopbackError>` | Snapshot of pending approvals, optionally filtered by session. |
| `wait_for_approval` | `session_id: String, timeout_secs: Option<u64>` | `Result<WaitOutcome, LoopbackError>` | Block until a new approval arrives for the session, or timeout (default 300s). A timeout is a `WaitOutcome::Timeout` **value**, not an error. |
| `configure` | `session_id: String` | `Result<ConfigureOk, LoopbackError>` | Generate an MCP config block for a loopback session. |

PLX-116 converted the four non-`permit` methods from `impl Stream` to unary
`Result`s: the failure is now the turn's `TurnError` terminal (`StopKind::Failed`)
rather than an `Err` variant of the streamed item, and every failure is shaped by
the single `impl From<LoopbackError> for TurnError` in `types.rs`.

`permit` was deliberately left as `impl Stream` — see the doc comment on the
method. It is the CLI-facing half of the permission path and PLX-105 owns it.

**A denial is still not an error.** `respond(approve = false)` succeeds, and the
"no" reaches the CLI as an `Ok`-valued `{"behavior":"deny"}` payload out of
`permit`. RFC 002 §6.6's `StopKind::Refused` is now *expressible* by a generated
handler (PLX-112 landed `TurnStop` / `IntoTurnStop`), but it is still not
*reached* here — a denial terminates the turn as `complete`, asserted by
`tests/plx105_permission_wire.rs::a_denial_is_a_successful_turn_and_not_a_refusal`.

## PLX-105 — what is blocked, and where

PLX-105 set out to replace the poll loop with
`client.request_permission_async(..).await`. It did not, and the reasons are
structural rather than a matter of effort. All four were measured, two by
compile probe:

1. **A generated activation method cannot take a `Client<C>`.** The vNext IR
   parser understands the handle, but the legacy `#[activation]` parser
   (`plexus-macros/src/parse.rs`) has no branch for it and treats it as a wire
   parameter. A compile probe returns `E0277: Client<(Permission,)>: Serialize
   is not satisfied`. `Turn<C>::client()` is reachable only from a hand-written
   `DeclaredHandler`, which `plexus-core`'s own `declared.rs` says is out of
   scope to wire into the registration surface.
2. **A generated activation's callbacks cannot be answered.** The emitted
   `call_arc` passes `live: None` to `turn_stream_to_plexus_stream`, so the
   `TurnControl` is dropped and nothing can call `respond`. A callback would be
   emitted as a `PlexusStreamItem::Request` and never resolve.
3. **The MCP gateway has no responder.** `mcp/bridge.rs` forwards a `Request`
   item as a logging notification whose comment says the client "should respond
   via `_plexus_respond`" — a tool that does not exist anywhere in the tree.
4. **The peer is the wrong party.** `permit`'s turn is opened *by the spawned
   CLI*. Its peer is the CLI, so `request_permission_async` would ask the asker.
   The correlation this ticket wanted deleted is between two different turns
   (the CLI's `permit` and the parent's `chat`), and `plexus-core`'s per-turn
   router refuses cross-turn delivery by design (`RespondError::WrongTurn`).

(1) and (2) live in `plexus-macros`, which PLX-105 is forbidden to touch.

## Storage

- Backend: SQLite
- Config: `LoopbackStorageConfig` with `db_path`.
- Schema: pending approvals keyed by `approval_id`, with `session_id`,
  `tool_use_id` → session-id mapping, and `status` (`Pending` / `Approved`
  / `Denied` / `TimedOut`). Per-session notifiers live in memory.

## Composition

- `PLEXUS_MCP_URL` env var (default `http://127.0.0.1:4445/mcp`) — baked
  into the config emitted by `configure`.
- Orcha consumes `LoopbackStorage` directly (via `loopback.storage()`) for
  its approval-management methods (`list_pending_approvals`,
  `approve_request`, `deny_request`) so the orchestrator can broker
  approvals on behalf of parent callers.

## Example

```bash
# Generate MCP config for a new loopback session
synapse --port 44104 lforge substrate loopback.configure '{"session_id":"demo-1"}'

# Approver side: wait for the next approval on a session
synapse --port 44104 lforge substrate loopback.wait_for_approval \
  '{"session_id":"demo-1","timeout_secs":60}'

# Respond
synapse --port 44104 lforge substrate loopback.respond \
  '{"approval_id":"<uuid>","approve":true}'
```

## Source

- `activation.rs` — RPC method surface + blocking-poll permit loop
- `storage.rs` — SQLite + in-memory notifier map + `LoopbackStorageConfig`
- `types.rs` — `ApprovalStatus`, `ApprovalId`, unary terminal types, and the one `From<LoopbackError> for TurnError`
- `mod.rs` — module exports
