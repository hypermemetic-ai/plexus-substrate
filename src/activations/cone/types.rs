use crate::activations::arbor::{NodeId, TreeId};
use plexus_macros::HandleEnum;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::activation::Cone;

/// Unique identifier for an cone configuration
pub type ConeId = Uuid;

// ============================================================================
// Handle types for Cone activation
// ============================================================================

/// Type-safe handles for Cone activation data
///
/// Handles reference data stored in the Cone database and can be embedded
/// in Arbor tree nodes for external resolution.
#[derive(Debug, Clone, HandleEnum)]
#[handle(
    plugin_id = "Cone::PLUGIN_ID",
    // PLX-117: `Cone` is no longer generic, so the IR-21 pin that named
    // `Cone<NoParent>` is now just the type itself.
    plugin_id_type = "Cone",
    version = "1.0.0"
)]
pub enum ConeHandle {
    /// Handle to a message in the cone database
    /// Format: `{plugin_id}@1.0.0::chat:msg-{uuid}:{role}:{name}`
    #[handle(
        method = "chat",
        table = "messages",
        key = "id",
        key_field = "message_id",
        strip_prefix = "msg-"
    )]
    Message {
        /// Message ID with "msg-" prefix (e.g., "msg-550e8400-...")
        message_id: String,
        /// Role: "user", "assistant", or "system"
        role: String,
        /// Display name (cone name or "user")
        name: String,
    },
}

/// Unique identifier for a message
pub type MessageId = Uuid;

/// Role of a message sender
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
    System,
}

impl MessageRole {
    pub const fn as_str(&self) -> &'static str {
        match self {
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::System => "system",
        }
    }

    // Returns `Option<Self>` (not `Result`), so intentionally does not
    // implement `std::str::FromStr`. Callers pass DB column strings where
    // `None` is the expected signal for unknown values.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "user" => Some(MessageRole::User),
            "assistant" => Some(MessageRole::Assistant),
            "system" => Some(MessageRole::System),
            _ => None,
        }
    }
}

/// A message stored in the cone database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: MessageId,
    pub cone_id: ConeId,
    pub role: MessageRole,
    pub content: String,
    pub created_at: i64,
    /// Model used (for assistant messages)
    pub model_id: Option<String>,
    /// Token usage (for assistant messages)
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
}

/// A position in the context tree - couples `tree_id` and `node_id` together.
/// This ensures we always have a valid reference into a specific tree.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct Position {
    /// The tree containing this position
    pub tree_id: TreeId,
    /// The specific node within the tree
    pub node_id: NodeId,
}

impl Position {
    /// Create a new position
    pub const fn new(tree_id: TreeId, node_id: NodeId) -> Self {
        Self { tree_id, node_id }
    }

    /// Advance to a new node in the same tree
    pub const fn advance(&self, new_node_id: NodeId) -> Self {
        Self {
            tree_id: self.tree_id,
            node_id: new_node_id,
        }
    }
}

/// Cone configuration - defines an cone's identity and behavior
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ConeConfig {
    /// Unique identifier for this cone
    pub id: ConeId,
    /// Human-readable name
    pub name: String,
    /// Model ID to use (e.g., "gpt-4o-mini", "claude-3-haiku-20240307")
    pub model_id: String,
    /// System prompt / instructions for the cone
    pub system_prompt: Option<String>,
    /// The canonical head - current position in conversation tree
    /// This couples `tree_id` and `node_id` together
    pub head: Position,
    /// Additional configuration metadata
    pub metadata: Option<Value>,
    /// Created timestamp
    pub created_at: i64,
    /// Last updated timestamp
    pub updated_at: i64,
}

impl ConeConfig {
    /// Get the tree ID (convenience accessor)
    pub const fn tree_id(&self) -> TreeId {
        self.head.tree_id
    }

    /// Get the current node ID (convenience accessor)
    pub const fn node_id(&self) -> NodeId {
        self.head.node_id
    }
}

/// Lightweight cone info (for listing)
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ConeInfo {
    pub id: ConeId,
    pub name: String,
    pub model_id: String,
    pub head: Position,
    pub created_at: i64,
}

impl From<&ConeConfig> for ConeInfo {
    fn from(config: &ConeConfig) -> Self {
        Self {
            id: config.id,
            name: config.name.clone(),
            model_id: config.model_id.clone(),
            head: config.head,
            created_at: config.created_at,
        }
    }
}

// ============================================================================
// Method-specific return types
// Each method returns only its valid variants, making the API clearer
// ============================================================================

/// Result of cone.create
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type")]
pub enum CreateResult {
    #[serde(rename = "cone_created")]
    Created {
        cone_id: ConeId,
        /// Initial position (tree + root node)
        head: Position,
    },
}

/// Result of cone.get
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type")]
pub enum GetResult {
    #[serde(rename = "cone_data")]
    Data { cone: ConeConfig },
}

/// Result of cone.list
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type")]
pub enum ListResult {
    #[serde(rename = "cone_list")]
    List { cones: Vec<ConeInfo> },
}

/// Result of cone.delete
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type")]
pub enum DeleteResult {
    #[serde(rename = "cone_deleted")]
    Deleted { cone_id: ConeId },
}

/// Events emitted during cone.chat (streaming)
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type")]
pub enum ChatEvent {
    /// Chat response started
    #[serde(rename = "chat_start")]
    Start {
        cone_id: ConeId,
        /// Position of the user message node
        user_position: Position,
    },
    /// Chat content chunk (streaming)
    #[serde(rename = "chat_content")]
    Content {
        cone_id: ConeId,
        content: String,
    },
    /// Chat response complete
    #[serde(rename = "chat_complete")]
    Complete {
        cone_id: ConeId,
        /// The new head position (tree + response node)
        new_head: Position,
        /// Total tokens used (if available)
        usage: Option<ChatUsage>,
    },
    #[serde(rename = "error")]
    Error { message: String },
}

/// Result of `cone.set_head`
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type")]
pub enum SetHeadResult {
    #[serde(rename = "head_updated")]
    Updated {
        cone_id: ConeId,
        old_head: Position,
        new_head: Position,
    },
}

/// Result of cone.registry
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type")]
pub enum RegistryResult {
    #[serde(rename = "registry")]
    Registry(cllient::RegistryExport),
}

/// Resolved message from handle resolution
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type")]
pub enum ResolveResult {
    #[serde(rename = "resolved_message")]
    Message {
        id: String,
        role: String,
        content: String,
        model: Option<String>,
        name: String,
    },
    #[serde(rename = "error")]
    Error { message: String },
}

/// Token usage information
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ChatUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}

/// Error type for cone operations
///
/// `Serialize` is derived (PLX-117) so `TurnError::structured` can carry the
/// typed value in `details` rather than flattening it to a string, which
/// RFC 002 §6.7 forbids at this boundary.
#[derive(Debug, Clone, thiserror::Error, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConeError {
    #[error("Cone not found: {name}")]
    SessionNotFound { name: String },
    #[error("Storage error ({operation}): {detail}")]
    StorageError { operation: String, detail: String },
    #[error("Arbor error: {detail}")]
    ArborError { detail: String },
    #[error("{message}")]
    InvalidState { message: String },
}

impl From<String> for ConeError {
    fn from(s: String) -> Self {
        Self::InvalidState { message: s }
    }
}

impl From<&str> for ConeError {
    fn from(s: &str) -> Self {
        Self::InvalidState { message: s.to_string() }
    }
}

/// The ONE place Cone's domain error becomes a wire error (PLX-110 / PLX-117).
///
/// Every unary `cone` method returns `Result<_, ConeError>`; this impl is the
/// whole of the error shaping, which is what keeps PLX-114's open question about
/// the envelope's `code` field a single-function change. Never construct a
/// `TurnError` at a call site in this activation.
impl From<ConeError> for plexus_core::runtime::TurnError {
    fn from(e: ConeError) -> Self {
        let code = match &e {
            ConeError::SessionNotFound { .. } => "cone.not_found",
            ConeError::StorageError { .. } => "cone.storage_error",
            ConeError::ArborError { .. } => "cone.arbor_error",
            ConeError::InvalidState { .. } => "cone.invalid_state",
        };
        let message = e.to_string();
        Self::structured(code, message, &e)
    }
}
