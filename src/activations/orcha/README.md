# orcha

Full task orchestration with approval loops and validation.

## Overview

Orcha is substrate's orchestration activation. It composes ClaudeCode,
ClaudeCodeLoopback, Lattice, PM, and Arbor into a pipeline that turns
natural-language tasks, inline graph definitions, or a ticket-DSL document
into a running Lattice graph of Claude-driven nodes — with approval
brokering, validation artifact extraction, and crash-recovery.

Three principal entry points:

- **`run_task` / `run_task_async`** — classic single-agent orchestration
  with approval loops and a validation artifact protocol. Produces an
  `OrchaEvent` stream (Progress / NodeStarted / Complete / Failed / …).
- **`run_tickets` (+ `_async` / `_files` / `_async_files`)** — compile a
  ticket document (`--- <id> [type] [> dep1, dep2]` / `task:` / `validate:`)
  into a Lattice graph and execute it end-to-end. The `_async` family
  returns `GraphStarted { graph_id }` immediately so callers can
  disconnect; `subscribe_graph(graph_id)` re-attaches.
- **`run_graph_definition` / `build_tickets` / `add_*_node` / `add_edge` /
  `run_graph`** — primitive graph-building APIs for callers that construct
  their own node+edge sets.

Orcha delegates graph topology + event persistence to Lattice, and uses PM
(the `pm` static child) to keep a ticket-id ↔ node-id mapping, per-node
execution logs, and the raw ticket source for diagnosis. On substrate
restart, `Orcha::recover_running_graphs()` scans PM-tracked graphs, resets
interrupted `Running` nodes to `Ready`, re-emits `NodeReady` events, and
re-attaches a dispatcher to each surviving graph so execution continues
from where the crash happened.

## Namespace

`orcha` — invoked via `synapse <backend> orcha.<method>`.

## Children

| Child | Kind | list method | search method | Description |
|---|---|---|---|---|
| `pm` | static | — | — | Project-management subsystem — ticket-vocabulary views of graph state. See `orcha/pm/README.md`. |

## Methods

### Session-style orchestration

| Method | Params | Returns | Description |
|---|---|---|---|
| `run_task` | `request: RunTaskRequest` | `Stream<Item=OrchaEvent>` | Run a complete orchestration task with approval loops and validation. |
| `run_task_async` | `request: RunTaskRequest` | `Result<SessionRef, OrchaError>` | Kick off `run_task` in the background; returns a session id. |
| `create_session` | `request: CreateSessionRequest` | `Result<SessionCreated, OrchaError>` | Create a session record. |
| `update_session_state` | `session_id: SessionId, state: SessionState` | `Result<(), OrchaError>` | Update a session's state. |
| `get_session` | `request: GetSessionRequest` | `Result<SessionInfo, OrchaError>` | Get a session by id. |
| `list_sessions` | — | `Result<Vec<SessionInfo>, OrchaError>` | List all sessions. |
| `delete_session` | `session_id: SessionId` | `Result<(), OrchaError>` | Delete a session. |
| `increment_retry` | `session_id: SessionId` | `Result<RetryStatus, OrchaError>` | Increment a session's retry counter. |
| `check_status` | `request: CheckStatusRequest` | `Result<StatusSummary, OrchaError>` | Summarize each agent (via Haiku) and produce a meta-summary for the session. |
| `list_monitor_trees` | — | `Result<Vec<MonitorTreeInfo>, OrchaError>` | List Arbor trees used for status monitoring. |

### Validation protocol

| Method | Params | Returns | Description |
|---|---|---|---|
| `extract_validation` | `text: String` | `Result<Option<ValidationArtifact>, OrchaError>` | Extract a `{"orcha_validate": {...}}` artifact from accumulated text. |
| `run_validation` | `artifact: ValidationArtifact` | `Result<ValidationResult, OrchaError>` | Execute a validation `test_command` in its `cwd` and report pass/fail. |

### Agents

| Method | Params | Returns | Description |
|---|---|---|---|
| `spawn_agent` | `request: SpawnAgentRequest` | `Result<SpawnedAgent, OrchaError>` | Spawn a new Claude-agent record for a session. |
| `list_agents` | `request: ListAgentsRequest` | `Result<Vec<AgentInfo>, OrchaError>` | List agents for a session. |
| `get_agent` | `request: GetAgentRequest` | `Result<AgentInfo, OrchaError>` | Get a specific agent. |

### Approval brokering

| Method | Params | Returns | Description |
|---|---|---|---|
| `list_pending_approvals` | `request: ListApprovalsRequest` | `Result<Vec<ApprovalInfo>, OrchaError>` | List pending loopback approvals for a session or graph. |
| `approve_request` | `request: ApproveRequest` | `Result<ApprovalOutcome, OrchaError>` | Approve a pending loopback request. |
| `deny_request` | `request: DenyRequest` | `Result<ApprovalOutcome, OrchaError>` | Deny a pending loopback request. |
| `subscribe_approvals` | `graph_id: String, timeout_secs: Option<u64>` | `Stream<Item=OrchaEvent>` | Watch a graph for approval requests (default 300s). |

### Graph execution

| Method | Params | Returns | Description |
|---|---|---|---|
| `run_graph` | `graph_id: String, model: Option<String>, working_directory: Option<String>` | `Stream<Item=OrchaEvent>` | Execute an existing Lattice graph through Orcha's Claude dispatcher. |
| `run_plan` | `task: String, model: Option<String>, working_directory: Option<String>` | `Stream<Item=OrchaEvent>` | Ask Claude to plan a task as tickets, compile, execute. |
| `cancel_graph` | `graph_id: String` | `Result<GraphRef, OrchaError>` | Cancel a running graph via a tracked `watch::Sender<bool>`. |
| `subscribe_graph` | `graph_id: String, after_seq: Option<u64>` | `Stream<Item=OrchaEvent>` | Re-attach to a running graph's event stream, replaying from `after_seq`. |
| `watch_graph_tree` | `graph_id: String, after_seq: Option<u64>` | `Stream<Item=OrchaEvent>` | Multiplex root + all child-graph events into one stream. |

### Graph construction (primitives)

| Method | Params | Returns | Description |
|---|---|---|---|
| `create_graph` | `metadata: Value` | `Result<GraphRef, OrchaError>` | Create an empty graph. |
| `add_task_node` | `graph_id: String, task: String` | `Result<NodeRef, OrchaError>` | Add a Claude task node. |
| `add_synthesize_node` | `graph_id: String, task: String` | `Result<NodeRef, OrchaError>` | Add a synthesize node. |
| `add_validate_node` | `graph_id: String, command: String, cwd: Option<String>` | `Result<NodeRef, OrchaError>` | Add a shell-validation node. |
| `add_gather_node` | `graph_id: String, strategy: GatherStrategy` | `Result<NodeRef, OrchaError>` | Add a Gather node (`all` or `first N`). |
| `add_subgraph_node` | `graph_id: String, child_graph_id: String` | `Result<NodeRef, OrchaError>` | Add a SubGraph node pointing at another graph. |
| `add_dependency` | `graph_id: String, dependent_node_id: String, dependency_node_id: String` | `Result<(), OrchaError>` | Declare a dependency edge. |

### Ticket DSL

| Method | Params | Returns | Description |
|---|---|---|---|
| `build_tickets` | `tickets: String, metadata: Value` | `Result<GraphRef, OrchaError>` | Compile a ticket document and build the graph without running it. |
| `run_tickets` | `tickets: String, metadata: Value, model: Option<String>, working_directory: Option<String>` | `Stream<Item=OrchaEvent>` | Compile + execute; detaches into a background task after `GraphStarted`. |
| `run_tickets_async` | `tickets: String, metadata: Value, model: Option<String>, working_directory: Option<String>` | `Result<GraphRef, OrchaError>` | Fire-and-forget variant: returns the `graph_id` and detaches. |
| `run_tickets_files` | `paths: Vec<String>, metadata: Value, model: Option<String>, working_directory: Option<String>` | `Stream<Item=OrchaEvent>` | Read N ticket files from disk, join, then `run_tickets`. |
| `run_tickets_async_files` | `paths: Vec<String>, metadata: Value, model: Option<String>, working_directory: Option<String>` | `Stream<Item=OrchaEvent>` | Fire-and-forget variant of `run_tickets_files`. |
| `run_graph_definition` | `metadata: Value, model: Option<String>, working_directory: Option<String>, nodes: Vec<OrchaNodeDef>, edges: Vec<OrchaEdgeDef>` | `Stream<Item=OrchaEvent>` | Build and run a graph from an inline node+edge definition. |

## Storage

- Backend: SQLite (owned by `OrchaStorage`)
- Config: `OrchaStorageConfig { db_path }` — sessions, agents, retry counts.
- PM storage (`PmStorageConfig`) lives alongside for ticket maps and node
  execution logs.

## Composition

Orcha is a coordinator — it holds many `Arc`s:

- `Arc<OrchaStorage>` — session + agent records.
- `Arc<ClaudeCode<P>>` — spawns and chats to Claude agents.
- `Arc<ClaudeCodeLoopback>` — brokers tool approvals.
- `Arc<ArborStorage>` — monitor trees and conversation lookup for
  `check_status` summaries.
- `Arc<GraphRuntime>` (wrapping `Arc<LatticeStorage>`) — graph construction
  and dispatch via the typed runtime API (`graph_runtime.rs`,
  `graph_runner.rs`).
- `Arc<pm::Pm>` — ticket↔node maps, ticket source, per-node execution logs.
- `cancel_registry: Arc<Mutex<HashMap<String, watch::Sender<bool>>>>` — a
  per-graph cancellation channel observed by the graph runner so
  `cancel_graph(graph_id)` can halt in-flight execution.
- Parent `HubContext` — the `P` parameter exists **only** to name the
  `ClaudeCode<P>` this activation holds. PLX-115 deleted the `PhantomData<P>`
  marker; the generic itself cannot go until `ClaudeCode` drops its own
  (PLX-116).

The actual per-node dispatch (Claude spawn, event pump, retry logic,
approval wiring) lives in `graph_runner.rs`; the typed graph construction
API is in `graph_runtime.rs`; the ticket parser is in `ticket_compiler.rs`.

## Example

```bash
# Compile + run a ticket document
synapse --port 44104 lforge substrate orcha.run_tickets_async \
  '{"tickets":"--- t1 [agent]\ntask: say hi","metadata":{}}'

# Watch progress
synapse --port 44104 lforge substrate orcha.subscribe_graph \
  '{"graph_id":"<from GraphStarted>","after_seq":0}'

# Natural-language planning
synapse --port 44104 lforge substrate orcha.run_plan \
  '{"task":"write a hello-world rust crate"}'
```

## Source

- `activation.rs` — RPC method surface + recovery + child gate for `pm`
- `context.rs` — `OrchaContext` carried through dispatch
- `graph_runtime.rs` — typed graph API (`GraphRuntime`, `OrchaGraph`)
- `graph_runner.rs` — per-graph execution loop
- `orchestrator.rs` — classic `run_task` orchestration
- `ticket_compiler.rs` — ticket-DSL parser
- `storage.rs` — SQLite persistence + `OrchaStorageConfig`
- `types.rs` — request/result enums, `OrchaEvent`, `OrchaNodeSpec`,
  `OrchaNodeKind`, `OrchaNodeDef`, `OrchaEdgeDef`, `ValidationArtifact`,
  `ValidationResult`, …
- `pm/` — project-management sub-activation (see its own README)
- `tests.rs` — in-process integration tests
- `mod.rs` — module exports
