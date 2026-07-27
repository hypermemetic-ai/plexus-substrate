//! Health activation — the reference *minimal* activation on the vNext turn runtime.
//!
//! PLX-118: this was one of only two hand-written `impl Activation` holdouts in
//! substrate (PLX-106 named the other, `CelestialBodyActivation`). It is now on
//! `#[plexus_macros::activation]` like the rest. None of PLX-106's cycle
//! reasoning applies here — `Health` has no `#[child]` at all, so nothing can
//! reach `Self` and the hand-authored `activation_ir!` escape hatch is not
//! needed.
//!
//! `check` yields exactly once, so per PLX-110 it is spelled as what it is: a
//! unary `Result<HealthEvent, HealthError>` that emits **no** updates and one
//! terminal carrying the `HealthEvent`. `HealthError` has no inhabited failure
//! today (reading a monotonic clock cannot fail) but it exists so the error
//! shaping lives in one `impl From<HealthError> for TurnError` — the shape
//! PLX-114 needs if the envelope's `code` field moves.

use super::types::{HealthError, HealthEvent};
use std::time::Instant;

/// Health activation - minimal reference implementation
#[derive(Clone)]
pub struct Health {
    start_time: Instant,
}

impl Health {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
        }
    }
}

impl Default for Health {
    fn default() -> Self {
        Self::new()
    }
}

#[plexus_macros::activation(namespace = "health",
version = "1.0.0",
description = "Check hub health and uptime")]
impl Health {
    /// Check the health status of the hub and return uptime
    #[plexus_macros::method(description = "Check the health status of the hub and return uptime")]
    async fn check(&self) -> Result<HealthEvent, HealthError> {
        Ok(HealthEvent::Status {
            status: "healthy".to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
            timestamp: chrono::Utc::now().timestamp(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plexus::Activation;

    #[test]
    fn test_health_activation_trait() {
        let health = Health::new();
        assert_eq!(health.namespace(), "health");
        assert_eq!(health.version(), "1.0.0");
        assert!(health.methods().contains(&"check"));
    }

    #[test]
    fn test_health_namespace_constant() {
        assert_eq!(Health::NAMESPACE, "health");
    }

    #[tokio::test]
    async fn test_check_is_unary_and_reports_healthy() {
        let health = Health::new();
        let HealthEvent::Status { status, .. } = health.check().await.unwrap();
        assert_eq!(status, "healthy");
    }
}
