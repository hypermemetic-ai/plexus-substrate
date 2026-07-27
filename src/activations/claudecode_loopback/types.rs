use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

/// Unique identifier for an approval request
pub type ApprovalId = Uuid;

/// Typed errors for the claudecode loopback subsystem
#[derive(Debug, Error)]
pub enum LoopbackError {
    #[error("{operation}: {detail}")]
    Storage { operation: &'static str, detail: String },

    #[error("Approval not found: {id}")]
    ApprovalNotFound { id: String },

    #[error("Serialization failed: {detail}")]
    Serialization { detail: String },

    #[error("Invalid data: {detail}")]
    InvalidData { detail: String },
}

impl From<LoopbackError> for String {
    fn from(e: LoopbackError) -> Self {
        e.to_string()
    }
}

/// The **one** place the loopback shapes a wire error (PLX-116).
///
/// PLX-110 fixed the bound at `E: Into<TurnError>`. Keeping the mapping in a
/// single impl — rather than inlining `TurnError` construction per method — is
/// what makes **PLX-114** (whether the envelope's `code` stays a `String` or
/// becomes the JSON integer RFC 002 §3.6 item 22 describes) a one-line edit
/// here.
///
/// **Note for PLX-105 / PLX-112**: every code below is a genuine *failure*.
/// A permission **denial** is not among them and must not become one — the
/// denial path in `permit` is still an `Ok`-valued `{"behavior":"deny"}`
/// payload, never an `Err`. When PLX-105 moves that decision onto the turn
/// callback it needs `StopKind::Refused`, which PLX-112 records as having no
/// spelling yet; `Err` here is *defined* as `Failed`.
impl From<LoopbackError> for plexus_core::runtime::TurnError {
    fn from(e: LoopbackError) -> Self {
        let code = match &e {
            LoopbackError::Storage { .. } => "loopback.storage",
            LoopbackError::ApprovalNotFound { .. } => "loopback.approval_not_found",
            LoopbackError::Serialization { .. } => "loopback.serialization",
            LoopbackError::InvalidData { .. } => "loopback.invalid_data",
        };
        Self::new(code, e.to_string())
    }
}

/// Status of an approval request
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Denied,
    TimedOut,
}

/// A pending approval request
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ApprovalRequest {
    pub id: ApprovalId,
    pub session_id: String,
    pub tool_name: String,
    pub tool_use_id: String,
    pub input: Value,
    pub status: ApprovalStatus,
    pub response_message: Option<String>,
    pub created_at: i64,
    pub resolved_at: Option<i64>,
}

/// Request to the permit MCP tool
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PermitRequest {
    pub tool_name: String,
    pub tool_use_id: String,
    pub input: Value,
}

/// Response from the permit MCP tool (Claude Code expected format)
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "behavior", rename_all = "lowercase")]
pub enum PermitResponse {
    Allow {
        #[serde(rename = "updatedInput", skip_serializing_if = "Option::is_none")]
        updated_input: Option<Value>,
    },
    Deny {
        message: String,
    },
}

/// Configuration for loopback mode
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LoopbackConfig {
    /// Session ID for correlation
    pub session_id: String,
    /// Plexus MCP URL
    pub mcp_url: String,
    /// Tools to auto-approve (no human needed)
    #[serde(default)]
    pub auto_approve_tools: Vec<String>,
    /// Tools to always deny
    #[serde(default)]
    pub deny_tools: Vec<String>,
    /// Timeout in seconds for approval (default: 300)
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

const fn default_timeout() -> u64 { 300 }

// ───────────────────────────────────────────────────────────────────────────
// PLX-116 / T1 — unary terminals. Field names preserved from the old `Ok`
// variants; the `Err` variants are gone because the failure is now the turn's
// terminal (`TurnError`) rather than a variant of the streamed item.
// ───────────────────────────────────────────────────────────────────────────

/// Terminal of `respond` — the approval that was resolved.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RespondOk {
    pub approval_id: ApprovalId,
}

/// Terminal of `pending` — the approvals currently awaiting a decision.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PendingOk {
    pub approvals: Vec<ApprovalRequest>,
}

/// Terminal of `configure` — the MCP config handed to the spawned CLI.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConfigureOk {
    pub mcp_config: Value,
}

/// Terminal of `wait_for_approval`.
///
/// This is **not** an Ok/Err result enum and is deliberately not collapsed into
/// one: a timeout here is a considered outcome of a successful wait, not a
/// failure of it, and it was already spelled as a non-error variant before
/// PLX-116. Deciding that a timeout is instead an error (or a
/// `StopKind::Refused`, which per **PLX-112** has no spelling at all today)
/// would be a change to the permission path, which PLX-116 reserves for
/// **PLX-105**. Only the former `Err` variant was removed.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WaitOutcome {
    #[serde(rename = "ok")]
    Ok { approvals: Vec<ApprovalRequest> },
    #[serde(rename = "timeout")]
    Timeout { message: String },
}
