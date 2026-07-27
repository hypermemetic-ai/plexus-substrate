use plexus_core::runtime::TurnError;
use plexus_core::types::Handle;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type GraphId = String;
pub type NodeId = String;

// ─── Token Model ──────────────────────────────────────────────────────────────

/// Token color — the routing discriminant (Petri net "color").
/// Ok/Error are lattice primitives; Named is application vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
#[derive(Default)]
pub enum TokenColor {
    #[default]
    Ok,
    Error,
    Named { name: String },
}


/// Token payload — data OR a handle, never both.
/// A token can also carry no payload (color-only signal for pure routing).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TokenPayload {
    Data { value: Value },
    Handle(Handle),
}

/// Atomic unit flowing on edges.
/// payload is optional — a token may be a color-only routing signal.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Token {
    #[serde(default)]
    pub color: TokenColor,
    pub payload: Option<TokenPayload>,
}

impl Token {
    pub const fn ok() -> Self {
        Self { color: TokenColor::Ok, payload: None }
    }

    pub const fn ok_data(data: Value) -> Self {
        Self {
            color: TokenColor::Ok,
            payload: Some(TokenPayload::Data { value: data }),
        }
    }

    pub const fn ok_handle(handle: Handle) -> Self {
        Self {
            color: TokenColor::Ok,
            payload: Some(TokenPayload::Handle(handle)),
        }
    }

    pub fn error(message: String) -> Self {
        Self {
            color: TokenColor::Error,
            payload: Some(TokenPayload::Data {
                value: serde_json::json!({ "message": message }),
            }),
        }
    }
}

/// Output produced when completing a node.
/// Many triggers fan-out — one downstream execution per token (used by Scatter).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeOutput {
    Single(Token),
    Many { tokens: Vec<Token> },
}

impl NodeOutput {
    pub fn tokens(&self) -> Vec<&Token> {
        match self {
            NodeOutput::Single(t) => vec![t],
            NodeOutput::Many { tokens } => tokens.iter().collect(),
        }
    }
}

/// A token with its payload fully resolved to an inline Value.
/// Handles have been fetched from their backing store server-side.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ResolvedToken {
    pub color: TokenColor,
    pub data: Option<Value>,
}

// ─── Graph Structure ──────────────────────────────────────────────────────────

/// How a node becomes enabled by its predecessors.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    #[default]
    All, // AND-join: every inbound edge must deliver a token
    Any, // OR-join: any inbound edge token is enough
}

/// What to produce from a Gather node (auto-executed by the engine).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GatherStrategy {
    All,
    First { n: usize },
}

/// Node execution semantics (the "place type" in colored Petri net terms).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeSpec {
    /// Caller-executed: engine emits `NodeReady`, caller drives and reports output.
    Task { data: Value, handle: Option<Handle> },

    /// Like Task but expected to produce `NodeOutput::Many` for fan-out.
    Scatter { data: Value, handle: Option<Handle> },

    /// Engine-executed: collects inbound tokens per strategy, produces Many output.
    Gather { strategy: GatherStrategy },

    /// Engine-executed: launch nested graph. Not yet implemented (reserved).
    SubGraph { graph_id: String },
}

/// Edge condition — filter tokens by color.
/// None = pass any token; Some(color) = only route if token.color == color.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EdgeCondition(pub Option<TokenColor>);

impl EdgeCondition {
    pub fn matches(&self, color: &TokenColor) -> bool {
        match &self.0 {
            None => true,
            Some(c) => c == color,
        }
    }
}

// ─── Node / Graph Status ──────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Pending,
    Ready,
    Running,
    Complete,
    Failed,
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeStatus::Pending => write!(f, "pending"),
            NodeStatus::Ready => write!(f, "ready"),
            NodeStatus::Running => write!(f, "running"),
            NodeStatus::Complete => write!(f, "complete"),
            NodeStatus::Failed => write!(f, "failed"),
        }
    }
}

impl std::str::FromStr for NodeStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(NodeStatus::Pending),
            "ready" => Ok(NodeStatus::Ready),
            "running" => Ok(NodeStatus::Running),
            "complete" => Ok(NodeStatus::Complete),
            "failed" => Ok(NodeStatus::Failed),
            other => Err(format!("Unknown node status: {other}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GraphStatus {
    Pending,
    Running,
    Complete,
    Failed,
    Cancelled,
}

impl std::fmt::Display for GraphStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphStatus::Pending => write!(f, "pending"),
            GraphStatus::Running => write!(f, "running"),
            GraphStatus::Complete => write!(f, "complete"),
            GraphStatus::Failed => write!(f, "failed"),
            GraphStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl std::str::FromStr for GraphStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(GraphStatus::Pending),
            "running" => Ok(GraphStatus::Running),
            "complete" => Ok(GraphStatus::Complete),
            "failed" => Ok(GraphStatus::Failed),
            "cancelled" => Ok(GraphStatus::Cancelled),
            other => Err(format!("Unknown graph status: {other}")),
        }
    }
}

// ─── Graph / Node Data Models ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LatticeNode {
    pub id: NodeId,
    pub graph_id: GraphId,
    pub spec: NodeSpec,
    pub status: NodeStatus,
    pub join_type: JoinType,
    pub output: Option<NodeOutput>,
    pub error: Option<String>,
    pub created_at: i64,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LatticeGraph {
    pub id: GraphId,
    pub metadata: Value,
    pub status: GraphStatus,
    pub created_at: i64,
    pub node_count: usize,
    pub edge_count: usize,
    pub parent_graph_id: Option<String>,
}

// ─── Events ───────────────────────────────────────────────────────────────────

/// Events emitted by the `execute()` stream
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LatticeEvent {
    NodeReady { node_id: NodeId, spec: NodeSpec },
    NodeStarted { node_id: NodeId },
    NodeDone { node_id: NodeId, output: Option<NodeOutput> },
    NodeFailed { node_id: NodeId, error: String },
    GraphDone { graph_id: GraphId },
    GraphFailed { graph_id: GraphId, node_id: NodeId, error: String },
}

/// An event paired with its durable sequence number.
///
/// Callers should persist the last `seq` they successfully processed.
/// On reconnect, pass it as `after_seq` to `execute()` to replay everything
/// that happened while disconnected — no gaps, correct ordering.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LatticeEventEnvelope {
    /// Monotonically increasing sequence number assigned at persistence time.
    pub seq: u64,
    pub event: LatticeEvent,
}

// ─── Result Types ─────────────────────────────────────────────────────────────

/// The success payload of `lattice.get`.
///
/// PLX-118: every lattice method except `execute` yields exactly once, so each
/// is now a unary `Result<T, LatticeError>` (PLX-110). The ten per-method
/// `Ok`/`Err` enums that existed *because the stream was the only channel an
/// error could travel down* are gone: the `Ok` payload became the terminal type
/// and the `Err { message }` variant collapsed into the single `LatticeError`
/// below, shaped for the wire in exactly one `impl From<LatticeError> for
/// TurnError`.
///
/// `GetGraphResult` survives as a **struct** because its success payload has
/// two fields and needs a name; it is no longer a result type.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GetGraphResult {
    pub graph: LatticeGraph,
    pub nodes: Vec<LatticeNode>,
}

/// The single error type for the lattice activation.
///
/// `LatticeStorage` reports failures as `String`; `From<String>` lets every
/// method use `?` directly on it. The one `From<LatticeError> for TurnError`
/// below is the *only* place lattice shapes an error for the wire — PLX-114 can
/// move `TurnError.code` from `String` to an integer by editing this one impl.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum LatticeError {
    /// A storage or topology operation failed.
    #[error("{0}")]
    Storage(String),

    /// The named node is not part of the named graph.
    #[error("Node {node_id} not found in graph {graph_id}")]
    NodeNotInGraph { graph_id: GraphId, node_id: NodeId },
}

impl From<String> for LatticeError {
    fn from(message: String) -> Self {
        Self::Storage(message)
    }
}

impl From<LatticeError> for TurnError {
    fn from(e: LatticeError) -> Self {
        let code = match e {
            LatticeError::Storage(_) => "lattice.storage_error",
            LatticeError::NodeNotInGraph { .. } => "lattice.node_not_in_graph",
        };
        TurnError::structured(code, e.to_string(), &e)
    }
}
