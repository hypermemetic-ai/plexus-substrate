use plexus_core::runtime::TurnError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Health check result
///
/// This is a plain domain type - no trait implementations needed.
/// Since PLX-118 `check` returns this as its terminal value rather than
/// yielding it as an update.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HealthEvent {
    /// Current health status
    Status {
        status: String,
        uptime_seconds: u64,
        timestamp: i64,
    },
}

// Keep old name for backwards compatibility
pub type HealthStatus = HealthEvent;

/// The single error type for the health activation.
///
/// PLX-118: `check` has no reachable failure today, but the unary arm requires
/// an `E` and PLX-110 fixed the bound at `E: Into<TurnError>`. Keeping a named
/// error type with one `From` impl means the error shaping is in exactly one
/// place — the property PLX-114 needs so that whichever way the envelope's
/// `code` field lands, the fix is a single edit rather than a smear across
/// call sites.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum HealthError {
    /// The uptime clock could not be read.
    #[error("health check failed: {0}")]
    Unavailable(String),
}

impl From<HealthError> for TurnError {
    fn from(e: HealthError) -> Self {
        TurnError::structured("health.unavailable", e.to_string(), &e)
    }
}
