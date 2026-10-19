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
`claudecode.create(None, loopback_session_id=…)`.

## Namespace

`loopback` — invoked via `synapse <backend> loopback.<method>`.

## Methods

| Method | Params | Returns | Description |
|---|---|---|---|
| `permit` | `tool_name: String, tool_use_id: String, input: Value, _connection: Option<Value>` | `Stream<Item=String>` | Permission-prompt handler — blocks polling storage until the approval resolves. Returns a stringified JSON response per the MCP contract. |
| `respond` | `approval_id: ApprovalId, approve: bool, message: Option<String>` | `Stream<Item=RespondResult>` | Approve or deny a pending approval. |
| `pending` | `session_id: Option<String>` | `Stream<Item=PendingResult>` | Snapshot of pending approvals, optionally filtered by session. |
| `wait_for_approval` | `session_id: String, timeout_secs: Option<u64>` | `Stream<Item=WaitForApprovalResult>` | Block until a new approval arrives for the session, or timeout (default 300s). |
| `configure` | `session_id: String` | `Stream<Item=ConfigureResult>` | Generate an MCP config block for a loopback session. |

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
- `types.rs` — `ApprovalStatus`, `ApprovalId`, result enums
- `mod.rs` — module exports
