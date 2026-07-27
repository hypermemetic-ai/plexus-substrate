# claudecode

Manage Claude Code sessions with Arbor-backed conversation history.

## Overview

ClaudeCode wraps the Claude Code CLI as a persistent session resource.
A session has a name, a working directory, a model (`opus`/`sonnet`/`haiku`),
an optional system prompt, and a `Position` (tree + node) into an Arbor
conversation tree that records every event Claude produces (user prompts,
assistant messages, tool calls, tool results, raw passthrough events). Each
non-trivial event becomes an external node in Arbor with a `Handle` back
to a row in the ClaudeCode database.

Two chat modes exist:
- `chat` — fully streamed: the caller keeps the connection open for the
  duration of the turn and receives `ChatEvent`s live.
- `chat_async` + `poll` — returns a `stream_id` immediately; the turn runs
  in a background task that buffers events for later polling. This is the
  path used by loopback approval flows where the parent needs to interleave
  tool-approval traffic with chat observation.

Loopback mode wires Claude Code to the `loopback` activation's MCP endpoint
so every tool call is mediated through parent-approved permits. Creation
with `loopback_enabled=true` fails fast if the MCP server is unreachable
(via `executor::check_mcp_reachable`).

ClaudeCode also exposes a filesystem-backed "session files" subsurface —
the JSONL files Claude Code writes under a project path — with methods to
list / get / import-to-arbor / export-from-arbor / delete those files.

## Namespace

`claudecode` — invoked via `synapse <backend> claudecode.<method>`, or per-
session via `synapse <backend> claudecode.session <id>.<method>`.

## Methods (ClaudeCode)

### Session lifecycle

| Method | Params | Returns | Description |
|---|---|---|---|
| `create` | `name: String, working_dir: String, model: Model, system_prompt: Option<String>, loopback_enabled: Option<bool>, loopback_session_id: Option<String>` | `Stream<Item=CreateResult>` | **Not yet unary** (PLX-116 residual: eight in-process `orcha` call sites match on `CreateResult`). Create a new session. Canonicalizes `working_dir`; verifies MCP reachability when loopback is enabled. |
| `get` | `name: String` | `Result<GetOk, ClaudeCodeError>` | Get session configuration by name. |
| `list` | — | `Result<ListOk, ClaudeCodeError>` | List all sessions. |
| `delete` | `name: String` | `Result<DeleteOk, ClaudeCodeError>` | Delete a session. |
| `fork` | `name: String, new_name: String` | `Result<ForkOk, ClaudeCodeError>` | Fork a session at its current head into a new session. |

### Chat

| Method | Params | Returns | Description |
|---|---|---|---|
| `chat` | `name: String, prompt: String, ephemeral: Option<bool>, allowed_tools: Option<Vec<String>>` | `Stream<Item=ChatEvent>` (streaming) | Stream a chat turn. If `ephemeral=true`, nodes are created but the head is not advanced. |
| `chat_async` | `name: String, prompt: String, ephemeral: Option<bool>` | `Result<ChatStartOk, ClaudeCodeError>` | Kick off a chat turn in the background and return a `stream_id` for polling. |
| `poll` | `stream_id: StreamId, from_seq: Option<u64>, limit: Option<u64>` | `Result<PollOk, ClaudeCodeError>` | Poll a background chat for new events since `from_seq`. |
| `streams` | `session_id: Option<ClaudeCodeId>` | `Result<StreamListOk, ClaudeCodeError>` | List active background streams, optionally filtered by session. |

### Tree / context

| Method | Params | Returns | Description |
|---|---|---|---|
| `get_tree` | `name: String` | `Result<GetTreeOk, ClaudeCodeError>` | Return the Arbor tree id and current head node id for a session. |
| `render_context` | `name: String, start: Option<NodeId>, end: Option<NodeId>` | `Result<RenderOk, ClaudeCodeError>` | Render the tree path between two nodes as a `Vec<Message>` suitable for the Claude API. |

### Session files (disk)

| Method | Params | Returns | Description |
|---|---|---|---|
| `sessions_list` | `project_path: String` | `Result<SessionsListOk, ClaudeCodeError>` | List session files for a project path. |
| `sessions_get` | `project_path: String, session_id: String` | `Result<SessionsGetOk, ClaudeCodeError>` | Read raw events from a session file. |
| `sessions_import` | `project_path: String, session_id: String, owner_id: Option<String>` | `Result<SessionsImportOk, ClaudeCodeError>` | Import a session file into Arbor as a new tree. |
| `sessions_export` | `tree_id: TreeId, project_path: String, session_id: String` | `Result<SessionsExportOk, ClaudeCodeError>` | Export an Arbor tree to a session file. |
| `sessions_delete` | `project_path: String, session_id: String` | `Result<SessionsDeleteOk, ClaudeCodeError>` | Delete a session file. |

## Children

| Child | Kind | list method | search method | Description |
|---|---|---|---|---|
| `session` | dynamic | `session_ids` | `session` | Look up a session by UUID and return a `SessionActivation` — a typed per-session namespace (IR-18). Fails with `None` on malformed UUIDs or unknown ids. |

## Methods (SessionActivation — per-session)

Exposed via `claudecode.session <id>.<method>`:

| Method | Params | Returns | Description |
|---|---|---|---|
| `chat` | `prompt: String, ephemeral: Option<bool>, allowed_tools: Option<Vec<String>>` | `Stream<Item=ChatEvent>` (streaming) | Mirror of the flat `chat` with the session pinned by id. |
| `get` | — | `Result<GetOk, ClaudeCodeError>` | Fetch this session's config. |
| `delete` | — | `Result<DeleteOk, ClaudeCodeError>` | Delete this session. |

## Handle system

ClaudeCode derives `ClaudeCodeHandle` with `#[derive(HandleEnum)]`:

- `Message { message_id, role, name }` — resolves to the message row;
  encoded as `ClaudeCode@1.0.0::chat:msg-{uuid}:{role}:{name}`.
- `Passthrough { event_id, event_type }` — inline-only; no resolution.

`resolve_handle` is wired via `#[plexus_macros::activation(... resolve_handle)]`
and `resolve_handle_impl` returns a `ResolveResult::Message` from the
backing database.

## Storage

- Backend: SQLite
- Config: `ClaudeCodeStorageConfig { db_path }`; construction also takes
  `Arc<ArborStorage>`.
- Schema: sessions keyed by UUID (+ `claude_session_id` for Claude's own
  session UUID); messages keyed by UUID with role/content/model; stream
  buffers for `chat_async`. See `src/activations/claudecode/storage.rs`.

## Composition

- `Arc<ArborStorage>` — injected at construction; every chat event is
  persisted as an external Arbor node.
- `ClaudeCodeExecutor` — spawns the Claude Code CLI and parses its event
  stream (see `executor.rs`).
- No parent handle. PLX-116 deleted the `P: HubContext` / `inject_parent`
  ritual: PLX-109 §4 measured `ClaudeCode::parent()` at zero callers. The
  `resolve_handle` / `resolve_handle_impl` pair is unaffected — `ClaudeCode`
  is a handle *provider*, which never needed a parent.
- `claudecode_loopback` + env `PLEXUS_MCP_URL` — loopback mode routes
  tool permissions through the parent.
- `sessions.rs` — filesystem I/O over Claude Code's session files.

## Example

```bash
synapse --port 44104 lforge substrate claudecode.create \
  '{"name":"demo","working_dir":"/workspace","model":"sonnet"}'
synapse --port 44104 lforge substrate claudecode.chat \
  '{"name":"demo","prompt":"hi"}'

# Async + poll
synapse --port 44104 lforge substrate claudecode.chat_async \
  '{"name":"demo","prompt":"hi"}'
synapse --port 44104 lforge substrate claudecode.poll \
  '{"stream_id":"<uuid>"}'

# Per-session child gate (IR-18)
synapse --port 44104 lforge substrate claudecode.session \
  <session-uuid>.chat '{"prompt":"hi"}'
```

## Source

- `activation.rs` — `ClaudeCode<P>` + `SessionActivation` RPC surfaces,
  handle resolution, dynamic `session` child gate
- `executor.rs` — Claude Code CLI spawn + event parsing, MCP reachability
- `sessions.rs` — session-file reader / writer
- `storage.rs` — SQLite persistence + `ClaudeCodeStorageConfig` + stream buffers
- `render.rs` — tree-to-message rendering
- `types.rs` — `ClaudeCodeHandle` (`HandleEnum`), `ClaudeCodeConfig`,
  `Model`, `ChatEvent`, `StreamId`, result enums, `ClaudeCodeError`
- `mod.rs` — module exports
