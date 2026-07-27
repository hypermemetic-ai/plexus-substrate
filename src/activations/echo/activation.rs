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
#[plexus_macros::activation(namespace = "echo",
version = "1.0.0",
description = "Echo messages back - demonstrates plexus-macros usage")]
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
    ///
    /// PLX-110: the first unary method in substrate. It yields exactly one
    /// value, so before this build it paid full stream ceremony —
    /// `stream! { yield .. }`, an `impl Stream` return, and a turn projection of
    /// one `.update` followed by an empty `.terminal` — to say one thing once.
    /// As `-> Result<EchoEvent, TurnError>` it emits **no updates** and one
    /// terminal carrying the `EchoEvent`, and its `MethodIr` is now observably a
    /// terminal-without-updates (`is_streaming() == false`), which is the
    /// property PLX-107 named as the precondition for the IR ever becoming
    /// authoritative on transport routing.
    ///
    /// `E` is `TurnError` here because `ping` has no domain failure to model;
    /// the general bound is `E: Into<TurnError>`, and a method with a real error
    /// type supplies one `impl From<MyError> for TurnError` that chooses the
    /// code, the message and the structured `details` payload.
    ///
    /// `MethodSchema.streaming` is untouched: it was never declared for `ping`
    /// and it is still false, so plexus-transport keeps serving this method as
    /// JSON exactly as before (PLX-107's contract).
    #[plexus_macros::method(description = "Ping — returns a Pong response")]
    async fn ping(&self) -> Result<EchoEvent, plexus_core::runtime::TurnError> {
        Ok(EchoEvent::Pong)
    }
}
