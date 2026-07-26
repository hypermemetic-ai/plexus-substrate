//! Interactive activation - demonstrates bidirectional communication patterns
//!
//! This activation showcases:
//! - User confirmations via `ctx.confirm()`
//! - Text prompts via `ctx.prompt()`
//! - Selection menus via `ctx.select()`
//! - Graceful handling of non-bidirectional transports
//!
//! The methods use `StandardBidirChannel` for common UI patterns.
//! For custom request/response types, see the `ImageProcessor` example.

use super::types::{ConfirmEvent, DeleteEvent, WizardEvent};
use async_stream::stream;
use futures::Stream;
use plexus_core::plexus::bidirectional::{
    bidir_error_message, BidirError, SelectOption, StandardBidirChannel,
};
use std::sync::Arc;

/// Interactive activation demonstrating bidirectional UI patterns
#[derive(Clone)]
pub struct Interactive;

impl Interactive {
    pub const fn new() -> Self {
        Interactive
    }
}

impl Default for Interactive {
    fn default() -> Self {
        Self::new()
    }
}

/// Hub-macro generates the Activation trait and RPC implementations.
/// The `bidirectional` attribute on methods enables server→client requests.
#[plexus_macros::activation(namespace = "interactive",
version = "1.0.0",
description = "Interactive methods demonstrating bidirectional communication")]
impl Interactive {
    /// Multi-step setup wizard demonstrating all bidirectional patterns
    ///
    /// This method demonstrates:
    /// - Text prompts (ctx.prompt)
    /// - Selection menus (ctx.select)
    /// - Confirmations (ctx.confirm)
    /// - Graceful error handling
    #[plexus_macros::method(bidirectional, streaming)]
    async fn wizard(
        &self,
        ctx: &Arc<StandardBidirChannel>,
    ) -> impl Stream<Item = WizardEvent> + Send + 'static {
        let ctx = ctx.clone();
        stream! {
            yield WizardEvent::Started;

            // Step 1: Get project name
            let name = match ctx.prompt("Enter project name:").await {
                Ok(n) if n.is_empty() => {
                    yield WizardEvent::Error { message: "Name cannot be empty".into() };
                    return;
                }
                Ok(n) => n,
                Err(BidirError::NotSupported) => {
                    yield WizardEvent::Error {
                        message: "Interactive mode required. Use a bidirectional transport.".into()
                    };
                    return;
                }
                Err(BidirError::Cancelled) => {
                    yield WizardEvent::Cancelled;
                    return;
                }
                Err(e) => {
                    yield WizardEvent::Error { message: bidir_error_message(&e) };
                    return;
                }
            };
            yield WizardEvent::NameCollected { name: name.clone() };

            // Step 2: Select template
            let templates = vec![
                SelectOption::new("minimal", "Minimal").with_description("Bare-bones starter"),
                SelectOption::new("full", "Full Featured").with_description("All features included"),
                SelectOption::new("api", "API Only").with_description("Backend API template"),
            ];

            let template = match ctx.select("Choose template:", templates).await {
                Ok(mut selected) if !selected.is_empty() => selected.remove(0),
                Ok(_) => {
                    yield WizardEvent::Error { message: "No template selected".into() };
                    return;
                }
                Err(BidirError::Cancelled) => {
                    yield WizardEvent::Cancelled;
                    return;
                }
                Err(e) => {
                    yield WizardEvent::Error { message: bidir_error_message(&e) };
                    return;
                }
            };
            yield WizardEvent::TemplateSelected { template: template.clone() };

            // Step 3: Confirm creation
            let confirmed = match ctx.confirm(&format!(
                "Create project '{name}' with '{template}' template?")).await {
                Ok(c) => c,
                Err(BidirError::Cancelled) => {
                    yield WizardEvent::Cancelled;
                    return;
                }
                Err(e) => {
                    yield WizardEvent::Error { message: bidir_error_message(&e) };
                    return;
                }
            };

            if !confirmed {
                yield WizardEvent::Cancelled;
                return;
            }

            // Success!
            yield WizardEvent::Created { name, template };
            yield WizardEvent::Done;
        }
    }

    /// Delete files with confirmation
    ///
    /// Demonstrates confirmation before destructive operations.
    #[plexus_macros::method(bidirectional, streaming)]
    async fn delete(
        &self,
        ctx: &Arc<StandardBidirChannel>,
        paths: Vec<String>,
    ) -> impl Stream<Item = DeleteEvent> + Send + 'static {
        let ctx = ctx.clone();
        stream! {
            if paths.is_empty() {
                yield DeleteEvent::Done;
                return;
            }

            // Confirm deletion
            let message = if paths.len() == 1 {
                format!("Delete '{}'?", paths[0])
            } else {
                format!("Delete {} files?", paths.len())
            };

            match ctx.confirm(&message).await {
                Ok(true) => {
                    // User confirmed - proceed with deletion
                    for path in paths {
                        // In real implementation, would actually delete files
                        yield DeleteEvent::Deleted { path };
                    }
                    yield DeleteEvent::Done;
                }
                Ok(false) | Err(BidirError::Cancelled) => {
                    yield DeleteEvent::Cancelled;
                }
                Err(BidirError::NotSupported) => {
                    // Non-interactive mode - skip deletion for safety
                    yield DeleteEvent::Cancelled;
                }
                Err(_) => {
                    yield DeleteEvent::Cancelled;
                }
            }
        }
    }

    /// Simple confirmation method for testing
    ///
    /// Just asks a yes/no question and returns the result.
    #[plexus_macros::method(bidirectional)]
    async fn confirm(
        &self,
        ctx: &Arc<StandardBidirChannel>,
        message: String,
    ) -> impl Stream<Item = ConfirmEvent> + Send + 'static {
        let ctx = ctx.clone();
        stream! {
            match ctx.confirm(&message).await {
                Ok(true) => yield ConfirmEvent::Confirmed,
                Ok(false) => yield ConfirmEvent::Declined,
                Err(e) => yield ConfirmEvent::Error { message: bidir_error_message(&e) },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plexus_core::plexus::bidirectional::{
        auto_respond_channel, StandardRequest, StandardResponse,
    };
    use futures::StreamExt;

    #[tokio::test]
    async fn test_wizard_with_auto_responses() {
        let ctx = auto_respond_channel(|req: &StandardRequest| match req {
            StandardRequest::Prompt { .. } => StandardResponse::Text {
                value: serde_json::Value::String("my-project".into()),
            },
            StandardRequest::Select { options, .. } => StandardResponse::Selected {
                values: vec![options[0].value.clone()],
            },
            StandardRequest::Confirm { .. } => StandardResponse::Confirmed { value: true },
            StandardRequest::Custom { data } => StandardResponse::Custom { data: data.clone() },
        });

        let interactive = Interactive::new();
        let events: Vec<WizardEvent> = interactive.wizard(&ctx).await.collect().await;

        // Should complete successfully
        assert!(matches!(events.last(), Some(WizardEvent::Done)));

        // Check we got expected events
        assert!(events.iter().any(|e| matches!(e, WizardEvent::Started)));
        assert!(events.iter().any(|e| matches!(e, WizardEvent::NameCollected { name } if name == "my-project")));
        assert!(events.iter().any(|e| matches!(e, WizardEvent::TemplateSelected { .. })));
        assert!(events.iter().any(|e| matches!(e, WizardEvent::Created { .. })));
    }

    #[tokio::test]
    async fn test_wizard_cancelled() {
        let ctx = auto_respond_channel(|req: &StandardRequest| match req {
            StandardRequest::Prompt { .. } => StandardResponse::Cancelled,
            StandardRequest::Select { .. } => StandardResponse::Cancelled,
            StandardRequest::Confirm { .. } => StandardResponse::Cancelled,
            StandardRequest::Custom { .. } => StandardResponse::Cancelled,
        });

        let interactive = Interactive::new();
        let events: Vec<WizardEvent> = interactive.wizard(&ctx).await.collect().await;

        assert!(matches!(events.last(), Some(WizardEvent::Cancelled)));
    }

    #[tokio::test]
    async fn test_delete_confirmed() {
        let ctx = auto_respond_channel(|_: &StandardRequest| StandardResponse::Confirmed {
            value: true,
        });

        let interactive = Interactive::new();
        let paths = vec!["file1.txt".into(), "file2.txt".into()];
        let events: Vec<DeleteEvent> = interactive.delete(&ctx, paths).await.collect().await;

        // Should have deleted both files
        assert!(events.iter().any(|e| matches!(e, DeleteEvent::Deleted { path } if path == "file1.txt")));
        assert!(events.iter().any(|e| matches!(e, DeleteEvent::Deleted { path } if path == "file2.txt")));
        assert!(matches!(events.last(), Some(DeleteEvent::Done)));
    }

    #[tokio::test]
    async fn test_delete_declined() {
        let ctx = auto_respond_channel(|_: &StandardRequest| StandardResponse::Confirmed {
            value: false,
        });

        let interactive = Interactive::new();
        let paths = vec!["file.txt".into()];
        let events: Vec<DeleteEvent> = interactive.delete(&ctx, paths).await.collect().await;

        // Should be cancelled without deleting
        assert!(matches!(events.last(), Some(DeleteEvent::Cancelled)));
        assert!(!events.iter().any(|e| matches!(e, DeleteEvent::Deleted { .. })));
    }

    #[tokio::test]
    async fn test_confirm_yes() {
        let ctx = auto_respond_channel(|_: &StandardRequest| StandardResponse::Confirmed {
            value: true,
        });

        let interactive = Interactive::new();
        let events: Vec<ConfirmEvent> = interactive
            .confirm(&ctx, "Proceed?".into())
            .await
            .collect()
            .await;

        assert!(matches!(events.first(), Some(ConfirmEvent::Confirmed)));
    }

    #[tokio::test]
    async fn test_confirm_no() {
        let ctx = auto_respond_channel(|_: &StandardRequest| StandardResponse::Confirmed {
            value: false,
        });

        let interactive = Interactive::new();
        let events: Vec<ConfirmEvent> = interactive
            .confirm(&ctx, "Proceed?".into())
            .await
            .collect()
            .await;

        assert!(matches!(events.first(), Some(ConfirmEvent::Declined)));
    }
}
