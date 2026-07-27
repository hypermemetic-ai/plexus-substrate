use plexus_core::runtime::TurnError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The single error type for the chaos activation.
///
/// PLX-118: the three chaos methods that yield exactly once are now unary
/// `Result`s (PLX-110), and their `Err { message }` variants collapsed into
/// this type. All wire shaping happens in the one `From` impl below, so
/// PLX-114's pending decision about `TurnError.code` is a one-line change here.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum ChaosError {
    /// A lattice storage operation failed.
    #[error("{0}")]
    Storage(String),

    /// `libc::kill` returned a non-zero status that is not ESRCH.
    #[error("kill failed: {0}")]
    KillFailed(String),
}

impl From<String> for ChaosError {
    fn from(message: String) -> Self {
        Self::Storage(message)
    }
}

impl From<ChaosError> for TurnError {
    fn from(e: ChaosError) -> Self {
        let code = match e {
            ChaosError::Storage(_) => "chaos.storage_error",
            ChaosError::KillFailed(_) => "chaos.kill_failed",
        };
        TurnError::structured(code, e.to_string(), &e)
    }
}

/// A running node found across all active graphs
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunningNode {
    pub graph_id: String,
    pub node_id: String,
    pub spec_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ListRunningResult {
    #[serde(rename = "node")]
    Node(RunningNode),
    #[serde(rename = "done")]
    Done { count: usize },
    #[serde(rename = "error")]
    Err { message: String },
}

/// The outcome of an injection.
///
/// PLX-118: no longer a result type — the failure variant became `ChaosError`.
/// `Skipped` stays because "the node was not Running" is a legitimate
/// non-failure outcome, not an error.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InjectResult {
    #[serde(rename = "ok")]
    Ok { graph_id: String, node_id: String, action: String },
    #[serde(rename = "skipped")]
    Skipped { reason: String },
}

/// A process found on the system
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProcessInfo {
    pub pid: u32,
    pub cmdline: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ListProcessesResult {
    #[serde(rename = "process")]
    Process(ProcessInfo),
    #[serde(rename = "done")]
    Done { count: usize },
    #[serde(rename = "error")]
    Err { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KillProcessResult {
    #[serde(rename = "killed")]
    Killed { pid: u32 },
    #[serde(rename = "not_found")]
    NotFound,
}

/// Per-node status snapshot
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct NodeSnapshot {
    pub node_id: String,
    pub status: String,
    pub spec_type: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GraphSnapshotResult {
    #[serde(rename = "node")]
    Node(NodeSnapshot),
    #[serde(rename = "summary")]
    Summary {
        graph_id: String,
        graph_status: String,
        total: usize,
        pending: usize,
        ready: usize,
        running: usize,
        complete: usize,
        failed: usize,
    },
    #[serde(rename = "error")]
    Err { message: String },
}
