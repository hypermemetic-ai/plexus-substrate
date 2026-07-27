pub use crate::activations::lattice::GatherStrategy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ═══════════════════════════════════════════════════════════════════════════
// Error Types
// ═══════════════════════════════════════════════════════════════════════════

/// Structured error type for Orcha operations
///
/// PLX-115: this is the single error type every unary Orcha (and Pm) method
/// returns.  It is `Serialize` so that the whole typed value survives into
/// [`TurnError::details`] instead of being flattened to a message — the
/// property RFC 002 §6.7 requires and the reason the `From` impl below is the
/// **only** place in this activation that constructs a `TurnError`.  PLX-114 is
/// still deciding whether the envelope's `code` is a string or an integer; the
/// fix, whichever way it lands, is this one function.
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OrchaError {
    #[error("session not found: {session_id}")]
    SessionNotFound { session_id: String },

    #[error("orchestration error: {detail}")]
    OrchestrationError { detail: String },

    #[error("storage error during {operation}: {detail}")]
    StorageError { operation: String, detail: String },

    #[error("validation error: {detail}")]
    ValidationError { detail: String },
}

impl OrchaError {
    /// The stable machine token a client branches on.
    const fn code(&self) -> &'static str {
        match self {
            Self::SessionNotFound { .. } => "orcha.session_not_found",
            Self::OrchestrationError { .. } => "orcha.orchestration_error",
            Self::StorageError { .. } => "orcha.storage_error",
            Self::ValidationError { .. } => "orcha.validation_error",
        }
    }

    /// A storage failure, tagged with the operation that produced it.
    pub fn storage(operation: &str, detail: impl std::fmt::Display) -> Self {
        Self::StorageError {
            operation: operation.to_string(),
            detail: detail.to_string(),
        }
    }
}

impl From<String> for OrchaError {
    fn from(detail: String) -> Self {
        OrchaError::OrchestrationError { detail }
    }
}

/// The one and only bridge from Orcha's domain error to the turn envelope.
///
/// PLX-110's decision: `E: Into<TurnError>`, and `Err` terminates the turn with
/// `StopKind::Failed`.  Nothing else in this activation builds a `TurnError`.
impl From<OrchaError> for plexus_core::runtime::TurnError {
    fn from(err: OrchaError) -> Self {
        let code = err.code();
        let message = err.to_string();
        Self::structured(code, message, &err)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Orcha Node Kind — typed dispatch
// ═══════════════════════════════════════════════════════════════════════════

/// Typed Orcha node payload — serialized into `NodeSpec::Task` { data }.
/// `graph_runner` deserializes this to dispatch to the correct executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "orcha_type", rename_all = "snake_case")]
pub enum OrchaNodeKind {
    Task { task: String, #[serde(default)] max_retries: Option<u8> },
    Synthesize { task: String, #[serde(default)] max_retries: Option<u8> },
    Validate { command: String, cwd: Option<String>, #[serde(default)] max_retries: Option<u8> },
    Review { prompt: String },
    Plan { task: String },
}

// ═══════════════════════════════════════════════════════════════════════════
// Session Management Types
// ═══════════════════════════════════════════════════════════════════════════

/// Unique identifier for an orcha session
pub type SessionId = String;

/// Current state of an orcha session
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SessionState {
    /// Session is idle, ready to accept tasks
    Idle,
    /// Session is executing a task
    Running {
        /// Current Claude Code stream ID
        stream_id: String,
        /// Current sequence number for polling
        sequence: u64,
        /// Number of active agents (multi-agent mode)
        #[serde(default)]
        active_agents: u32,
        /// Number of completed agents (multi-agent mode)
        #[serde(default)]
        completed_agents: u32,
        /// Number of failed agents (multi-agent mode)
        #[serde(default)]
        failed_agents: u32,
    },
    /// Session is waiting for approval response
    WaitingApproval {
        /// Approval request ID
        approval_id: String,
    },
    /// Session is waiting for validation
    Validating {
        /// Test command being executed
        test_command: String,
    },
    /// Session has completed successfully
    Complete,
    /// Session has failed
    Failed {
        /// Error message
        error: String,
    },
}

/// Session metadata
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SessionInfo {
    pub session_id: SessionId,
    pub model: String,
    pub created_at: i64, // Unix timestamp
    pub last_activity: i64,
    pub state: SessionState,
    pub retry_count: u32,
    pub max_retries: u32,
    /// Agent mode (single or multi)
    #[serde(default)]
    pub agent_mode: AgentMode,
    /// Primary agent ID (if in multi mode)
    pub primary_agent_id: Option<AgentId>,
    /// Arbor tree ID for tracking orchestration events
    pub tree_id: Option<String>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Request/Response Types
// ═══════════════════════════════════════════════════════════════════════════

/// Request to create a new orcha session
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CreateSessionRequest {
    /// Model to use (sonnet, opus, haiku)
    pub model: String,
    /// Working directory for the session
    #[serde(default = "default_cwd")]
    pub working_directory: String,
    /// Approval rules for the session
    pub rules: Option<String>,
    /// Maximum retry attempts for validation failures
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Enable multi-agent mode (default: false for backward compatibility)
    #[serde(default)]
    pub multi_agent: bool,
}

/// Request to run a task with full orchestration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunTaskRequest {
    /// Model to use (sonnet, opus, haiku)
    pub model: String,
    /// Task description/prompt
    pub task: String,
    /// Working directory for the session
    #[serde(default = "default_cwd")]
    pub working_directory: String,
    /// Approval rules (for Claude-as-judge)
    pub rules: Option<String>,
    /// Maximum retry attempts for validation failures
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Show Claude's output and tool use (default: false)
    #[serde(default)]
    pub verbose: bool,
    /// Enable automatic approval via Haiku decision agent (default: true)
    ///
    /// When true, spawns ephemeral Haiku session to judge each approval.
    /// When false, approvals must be handled manually via `orcha.approve_request`.
    #[serde(default = "default_auto_approve")]
    pub auto_approve: bool,
}

const fn default_auto_approve() -> bool {
    false
}

fn default_cwd() -> String {
    "/workspace".to_string()
}

const fn default_max_retries() -> u32 {
    3
}

/// A newly created session — the terminal value of `create_session`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SessionCreated {
    pub session_id: SessionId,
    pub created_at: i64,
}

/// Request to submit a task to a session
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SubmitTaskRequest {
    pub session_id: SessionId,
    pub task: String,
}

/// Request to get session status
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GetSessionRequest {
    pub session_id: SessionId,
}

/// Request to respond to an approval
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RespondApprovalRequest {
    pub approval_id: String,
    pub approve: bool,
    pub message: Option<String>,
}

/// Request to list pending approvals for a session
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ListApprovalsRequest {
    pub session_id: SessionId,
}

/// Information about a pending approval request
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ApprovalInfo {
    pub approval_id: String,
    pub session_id: String,
    pub tool_name: String,
    pub tool_use_id: String,
    pub tool_input: serde_json::Value,
    pub created_at: String, // ISO 8601 timestamp
}

/// Request to approve a pending request
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ApproveRequest {
    pub approval_id: String,
    /// Optional message explaining approval decision
    pub message: Option<String>,
}

/// Request to deny a pending request
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DenyRequest {
    pub approval_id: String,
    /// Reason for denial (shown to agent)
    pub reason: Option<String>,
}

/// The outcome of approving or denying a request — the terminal value of
/// `approve_request` and `deny_request`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ApprovalOutcome {
    pub approval_id: String,
    pub message: Option<String>,
}

/// A session's retry counter after `increment_retry`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RetryStatus {
    pub retry_count: u32,
    pub max_retries: u32,
    pub exceeded: bool,
}

/// Request to check status of a running session
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CheckStatusRequest {
    pub session_id: SessionId,
}

/// A session status summary — the terminal value of `check_status`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StatusSummary {
    pub summary: String,
    /// Per-agent summaries (empty for single-agent mode)
    #[serde(default)]
    pub agent_summaries: Vec<AgentSummary>,
}

/// A reference to a session — the terminal value of `run_task_async`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SessionRef {
    pub session_id: SessionId,
}

/// Information about a monitor tree
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MonitorTreeInfo {
    pub tree_id: String,
    pub session_id: String,
    pub tree_path: String,
}

// ═══════════════════════════════════════════════════════════════════════════
// Event Streaming Types
// ═══════════════════════════════════════════════════════════════════════════

/// Events streamed from an orcha session
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OrchaEvent {
    /// Session state changed
    StateChange {
        session_id: SessionId,
        state: SessionState,
    },

    /// Output from the agent (Claude content)
    Output {
        text: String,
    },

    /// Tool use detected
    ToolUse {
        tool_id: String,
        tool_name: String,
        input: serde_json::Value,
    },

    /// Tool result
    ToolResult {
        tool_id: String,
        content: String,
        is_error: bool,
    },

    /// Approval request detected
    ApprovalRequest {
        approval_id: String,
        tool_name: String,
        input: serde_json::Value,
        timeout_seconds: u64,
    },

    /// Approval response received
    ApprovalResponse {
        approval_id: String,
        approved: bool,
        message: Option<String>,
    },

    /// Validation artifact detected
    ValidationArtifact {
        test_command: String,
        cwd: String,
    },

    /// Validation started
    ValidationStarted {
        test_command: String,
    },

    /// Validation result
    ValidationResult {
        success: bool,
        output: String,
    },

    /// Retry attempt
    RetryAttempt {
        attempt: u32,
        max_retries: u32,
        reason: String,
    },

    /// Task completed successfully
    Complete {
        session_id: SessionId,
    },

    /// Task failed
    Failed {
        session_id: SessionId,
        error: String,
    },

    /// Progress update
    Progress {
        message: String,
        percentage: Option<f32>,
    },

    /// Agent spawned (multi-agent mode)
    AgentSpawned {
        session_id: SessionId,
        agent_id: AgentId,
        subtask: String,
        parent_agent_id: Option<AgentId>,
    },

    /// Agent state changed (multi-agent mode)
    AgentStateChange {
        agent_id: AgentId,
        state: AgentState,
    },

    /// Agent completed (multi-agent mode)
    AgentComplete {
        agent_id: AgentId,
        subtask: String,
    },

    /// Agent failed (multi-agent mode)
    AgentFailed {
        agent_id: AgentId,
        subtask: String,
        error: String,
    },

    /// Graph execution has started
    GraphStarted {
        graph_id: String,
    },

    /// A node is ready and has been dispatched for execution
    NodeStarted {
        node_id: String,
        label: Option<String>,
        /// Ticket ID (e.g. "CALC-1") if this node was built from a ticket definition
        ticket_id: Option<String>,
        /// Completion percentage before this node started (`complete_nodes` / `total_nodes` * 100)
        percentage: Option<u32>,
    },

    /// A node completed successfully
    NodeComplete {
        node_id: String,
        label: Option<String>,
        /// Ticket ID (e.g. "CALC-1") if this node was built from a ticket definition
        ticket_id: Option<String>,
        output_summary: Option<String>,
        /// Completion percentage after this node finished (`complete_nodes` / `total_nodes` * 100)
        percentage: Option<u32>,
    },

    /// A node failed
    NodeFailed {
        node_id: String,
        label: Option<String>,
        /// Ticket ID (e.g. "CALC-1") if this node was built from a ticket definition
        ticket_id: Option<String>,
        error: String,
        /// Completion percentage after this node failed (`complete_nodes` / `total_nodes` * 100)
        percentage: Option<u32>,
    },

    /// A validate node is retrying after a failed validation attempt
    Retrying {
        node_id: String,
        ticket_id: Option<String>,
        attempt: usize,
        max_attempts: usize,
        error: String,
    },

    /// Live output chunk from a node during execution
    NodeOutput {
        node_id: String,
        ticket_id: Option<String>,
        chunk: String,
    },

    /// Graph was cancelled via `cancel_graph`
    Cancelled {
        graph_id: String,
    },

    /// A pending approval request is waiting for a human decision
    ApprovalPending {
        approval_id: String,
        graph_id: String,
        tool_name: String,
        tool_input: serde_json::Value,
        created_at: String,
    },
}

// ═══════════════════════════════════════════════════════════════════════════
// Validation Types
// ═══════════════════════════════════════════════════════════════════════════

/// Validation artifact extracted from agent output
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ValidationArtifact {
    pub test_command: String,
    pub cwd: String,
}

/// Result of running a validation test
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ValidationResult {
    pub success: bool,
    pub output: String,
    pub exit_code: Option<i32>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Multi-Agent Orchestration Types
// ═══════════════════════════════════════════════════════════════════════════

/// Unique identifier for an agent
pub type AgentId = String;

/// Agent mode for sessions
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum AgentMode {
    /// Single-agent mode (backward compatible)
    #[default]
    Single,
    /// Multi-agent orchestration mode
    Multi,
}


/// Agent-specific state (mirrors `SessionState` but for individual agents)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum AgentState {
    /// Agent is idle, ready to start
    Idle,
    /// Agent is executing a task
    Running {
        /// Current sequence number for polling
        sequence: u64,
    },
    /// Agent is waiting for approval response
    WaitingApproval {
        /// Approval request ID
        approval_id: String,
    },
    /// Agent is running validation
    Validating {
        /// Test command being executed
        test_command: String,
    },
    /// Agent has completed successfully
    Complete,
    /// Agent has failed
    Failed {
        /// Error message
        error: String,
    },
}

/// Agent metadata and state
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AgentInfo {
    pub agent_id: AgentId,
    pub session_id: SessionId,
    pub claudecode_session_id: String,
    pub subtask: String,
    pub state: AgentState,
    pub is_primary: bool,
    pub parent_agent_id: Option<AgentId>,
    pub created_at: i64,
    pub last_activity: i64,
    pub completed_at: Option<i64>,
    pub error_message: Option<String>,
}

/// Request to spawn a new agent
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SpawnAgentRequest {
    pub session_id: SessionId,
    pub subtask: String,
    /// Optional parent agent (if spawned by another agent)
    pub parent_agent_id: Option<AgentId>,
}

/// A newly spawned agent — the terminal value of `spawn_agent`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SpawnedAgent {
    pub agent_id: AgentId,
    pub claudecode_session_id: String,
}

/// Request to list agents in a session
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ListAgentsRequest {
    pub session_id: SessionId,
}

/// Request to get specific agent info
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GetAgentRequest {
    pub agent_id: AgentId,
}

/// Summary of an individual agent's work
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AgentSummary {
    pub agent_id: AgentId,
    pub subtask: String,
    pub state: AgentState,
    pub summary: String,  // AI-generated summary of this agent's work
}

// ═══════════════════════════════════════════════════════════════════════════
// Graph Builder Result Types
// ═══════════════════════════════════════════════════════════════════════════

/// A reference to a graph — the terminal value of `create_graph`,
/// `build_tickets`, `run_tickets_async` and `cancel_graph`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GraphRef {
    pub graph_id: String,
}

/// A reference to a node — the terminal value of the `add_*_node` methods.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NodeRef {
    pub node_id: String,
}

// ═══════════════════════════════════════════════════════════════════════════
// Inline Graph Definition Types (for run_graph_definition)
// ═══════════════════════════════════════════════════════════════════════════

/// Typed node spec in Orcha vocabulary — no raw `NodeSpec` JSON needed.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OrchaNodeSpec {
    Task { task: String, #[serde(default)] max_retries: Option<u8> },
    Synthesize { task: String, #[serde(default)] max_retries: Option<u8> },
    Validate { command: String, cwd: Option<String>, #[serde(default)] max_retries: Option<u8> },
    Gather { strategy: GatherStrategy },
    Review { prompt: String },
    Plan { task: String },
}

/// One node in an inline graph definition.
/// `id` is a caller-supplied stable label used in `OrchaEdgeDef`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OrchaNodeDef {
    pub id: String,
    pub spec: OrchaNodeSpec,
}

/// One edge in an inline graph definition.
/// `from`/`to` reference OrchaNodeDef.id values.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OrchaEdgeDef {
    pub from: String,
    pub to: String,
}
