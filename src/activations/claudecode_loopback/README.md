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
`permit`. RFC 002 §6.6's `StopKind::Refused` — the spelling a denial should get
once the loopback resolves through the turn callback — does not exist yet
(PLX-112); `Err` is currently *defined* as `Failed`.

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
