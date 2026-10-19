//! Echo activation - demonstrates hub-macro usage with caller-wraps streaming
//!
//! This is a minimal example showing how to create an activation using the
//! `#[hub_methods]` macro. The macro generates:
//!
//! - RPC trait and server implementation
//! - Activation trait implementation
//! - Method enum with JSON schemas
//!
//! Event types are plain domain types (no special traits needed).
//! The macro handles wrapping with `wrap_stream()` at the call site.
// PLEXUS-6: the 0.6 macros' legacy form (MACRO-13) — these activations keep
// `namespace = …` and their own type names; the name rule's trait would need
// one event type per activation. Its deprecation note is allowed here.
#![allow(deprecated)]

use super::types::EchoEvent;
use async_stream::stream;
use futures::Stream;
use std::time::Duration;

/// Echo activation - echoes messages back
#[derive(Clone)]
pub struct Echo;

impl Echo {
    pub const fn new() -> Self {
        Echo
    }
}

impl Default for Echo {
    fn default() -> Self {
        Self::new()
    }
}

/// Hub-macro generates all the boilerplate for this impl block:
/// - `EchoRpc` trait with JSON-RPC subscription methods
/// - `EchoRpcServer` implementation
/// - Activation trait implementation
/// - `EchoMethod` enum with JSON schemas
// PLEXUS-6: harmless — a viewer may use it to see that it is in.
#[plexus_macros::activation(namespace = "echo",
version = "1.0.0",
description = "Echo messages back - demonstrates plexus-macros usage",
roles(viewer = ["echo", "once", "ping"]))]
impl Echo {
    /// Echo a message back
    #[plexus_macros::method(description = "Echo a message back the specified number of times",
    params(
        message = "The message to echo",
        count = "Number of times to repeat (default: 1)"
    ))]
    async fn echo(
        &self,
        message: String,
        count: u32,
    ) -> impl Stream<Item = EchoEvent> + Send + 'static {
        let count = if count == 0 { 1 } else { count };
        stream! {
            for i in 0..count {
                if i > 0 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                yield EchoEvent::Echo {
                    message: message.clone(),
                    count: i + 1,
                };
            }
        }
    }

    /// Echo a simple message once
    #[plexus_macros::method(description = "Echo a message once",
    params(message = "The message to echo"))]
    async fn once(&self, message: String) -> impl Stream<Item = EchoEvent> + Send + 'static {
        stream! {
            yield EchoEvent::Echo {
                message,
                count: 1,
            };
        }
    }

    /// Ping — returns a Pong response
    #[plexus_macros::method(description = "Ping — returns a Pong response")]
    async fn ping(&self) -> impl Stream<Item = EchoEvent> + Send + 'static {
        stream! {
            yield EchoEvent::Pong;
        }
    }
}
