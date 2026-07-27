//! Mustache activation types

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Information about a registered template
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TemplateInfo {
    /// Unique template ID
    pub id: String,
    /// Plugin that owns this template
    pub plugin_id: Uuid,
    /// Method this template is for
    pub method: String,
    /// Template name (e.g., "default", "compact", "verbose")
    pub name: String,
    /// When the template was created (Unix timestamp)
    pub created_at: i64,
    /// When the template was last updated (Unix timestamp)
    pub updated_at: i64,
}

/// Error type for Mustache operations
///
/// PLX-118: this type already existed but was unreachable from the wire — every
/// method flattened its failures into `MustacheEvent::Error { message }` because
/// the stream was the only channel an error could travel down (PLX-109 §2).
/// Since PLX-110 it is the activation's `E`, and the single
/// `impl From<MustacheError> for TurnError` below is the one place mustache
/// shapes an error for the wire.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum MustacheError {
    #[error("Template not found: {0}")]
    TemplateNotFound(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Render error: {0}")]
    RenderError(String),

    #[error("Invalid template: {0}")]
    InvalidTemplate(String),
}

impl From<String> for MustacheError {
    fn from(s: String) -> Self {
        MustacheError::StorageError(s)
    }
}

impl From<&str> for MustacheError {
    fn from(s: &str) -> Self {
        MustacheError::StorageError(s.to_string())
    }
}

impl From<MustacheError> for plexus_core::runtime::TurnError {
    fn from(e: MustacheError) -> Self {
        let code = match e {
            MustacheError::TemplateNotFound(_) => "mustache.template_not_found",
            MustacheError::StorageError(_) => "mustache.storage_error",
            MustacheError::RenderError(_) => "mustache.render_error",
            MustacheError::InvalidTemplate(_) => "mustache.invalid_template",
        };
        plexus_core::runtime::TurnError::structured(code, e.to_string(), &e)
    }
}

// PLX-118: `MustacheEvent` is deleted. All five mustache methods yielded
// exactly once, so each now returns its payload directly as the terminal of a
// unary `Result` (PLX-110) — `String` for `render` and `get_template`,
// `TemplateInfo` for `register_template`, `Vec<TemplateInfo>` for
// `list_templates`, `usize` for `delete_template`. The `Error` and `NotFound`
// variants — the flattened error channel this migration exists to delete —
// became `MustacheError` variants above.
