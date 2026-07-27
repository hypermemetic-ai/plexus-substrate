//! `ChatEvent` → `session/update`: standardizing the miniature.
//!
//! PLX-140's whole argument for choosing `claudecode` over a greenfield agent
//! is that it is **already an unstandardized ACP in miniature**. This module is
//! where that claim stops being rhetoric and becomes a table.
//!
//! [`ChatEvent`](crate::activations::claudecode::ChatEvent) is claudecode's own
//! streaming vocabulary, shipped long before ACP was on the roadmap. ACP's
//! [`SessionUpdate`] is the standardized one. The mapping is almost entirely
//! one-to-one, and that near-identity is the evidence:
//!
//! | `ChatEvent` | `SessionUpdate` | note |
//! |---|---|---|
//! | `Content { text }` | `AgentMessageChunk` | the streaming token path |
//! | `Thinking { thinking }` | `AgentThoughtChunk` | ACP has the same distinction |
//! | `ToolUse { .. }` | `ToolCall` | `tool_use_id` **is** `toolCallId` |
//! | `ToolResult { .. }` | `ToolCallUpdate` | same id, terminal status |
//! | `Err { message }` | `AgentMessageChunk` | see below |
//! | `Start` / `Complete` | *declined* | turn lifecycle, not content |
//! | `Passthrough` | *declined* | see below |
//!
//! # Why two variants are declined rather than invented
//!
//! [`UpdateProjection::project`] returning `None` is not a silent drop —
//! PLX-138 made declines **counted** ([`PromptTurn::declined`](plexus_acp::v1::runtime::session::PromptTurn::declined))
//! precisely so that "this had no ACP spelling" is observable.
//!
//! - `Start` and `Complete` are **turn lifecycle**. ACP already expresses both:
//!   the turn began when the client's `session/prompt` request went out, and it
//!   ended when the response carrying `stopReason` came back. Emitting a
//!   content update for either would put the same fact on the wire twice, in
//!   two vocabularies. `Complete` also carries `claude_session_id` and `usage`,
//!   which are agent-internal bookkeeping.
//! - `Passthrough` is, by its own doc comment, "unrecognized Claude Code
//!   events". Projecting an event we could not recognise into a *typed* ACP
//!   update would be guessing, which is exactly the failure mode
//!   [`UpdateProjection`]'s doc names when it says this crate should not guess.
//!
//! # Why an error becomes a message chunk and not a failure
//!
//! `ChatEvent::Err` is an error *inside* a turn that keeps going — the stream
//! does not end there. The turn's own terminal is the thing that says whether
//! the turn failed, and it travels on the `stopReason`/error channel that
//! RFC 002 §6.7.1 governs. Projecting a mid-stream `Err` into anything other
//! than content would let a recoverable hiccup masquerade as a terminal state.

use plexus_acp::v1::runtime::session::UpdateProjection;
use plexus_acp::v1::schema::{
    ContentBlock, ContentChunk, SessionUpdate, ToolCall, ToolCallContent, ToolCallStatus,
    ToolCallUpdate, ToolCallUpdateFields,
};
use serde_json::Value;

use crate::activations::claudecode::ChatEvent;

/// The projection claudecode's chat stream uses.
///
/// Deliberately a unit struct with no configuration: there is exactly one
/// correct mapping from claudecode's vocabulary onto ACP's, and making it
/// tunable would let two deployments disagree about what a `tool_use` is.
#[derive(Debug, Clone, Copy, Default)]
pub struct ChatEventProjection;

impl ChatEventProjection {
    /// The typed half, exposed so a test can assert the table directly rather
    /// than through a JSON round-trip.
    #[must_use]
    pub fn project_event(event: ChatEvent) -> Option<SessionUpdate> {
        match event {
            ChatEvent::Content { text } => Some(SessionUpdate::AgentMessageChunk(
                ContentChunk::new(ContentBlock::from(text)),
            )),
            ChatEvent::Thinking { thinking } => Some(SessionUpdate::AgentThoughtChunk(
                ContentChunk::new(ContentBlock::from(thinking)),
            )),
            ChatEvent::ToolUse {
                tool_name,
                tool_use_id,
                input,
            } => Some(SessionUpdate::ToolCall(
                // `tool_use_id` IS `toolCallId`. No translation layer, no side
                // map — the same string the CLI minted is the one the editor
                // correlates on, which is what makes the `ToolCallUpdate`
                // below land on this call rather than beside it.
                ToolCall::new(tool_use_id, tool_name)
                    .status(ToolCallStatus::InProgress)
                    .raw_input(input),
            )),
            ChatEvent::ToolResult {
                tool_use_id,
                output,
                is_error,
            } => Some(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                tool_use_id,
                ToolCallUpdateFields::new()
                    .status(if is_error {
                        ToolCallStatus::Failed
                    } else {
                        ToolCallStatus::Completed
                    })
                    .content(vec![ToolCallContent::from(ContentBlock::from(output))]),
            ))),
            // See the module docs: an error inside a turn that continues is
            // content. The terminal is what says the turn failed.
            ChatEvent::Err { message } => Some(SessionUpdate::AgentMessageChunk(
                ContentChunk::new(ContentBlock::from(message)),
            )),
            // Declined, and counted. Lifecycle facts ACP already carries.
            ChatEvent::Start { .. } | ChatEvent::Complete { .. } => None,
            // Declined, and counted. We did not recognise it either.
            ChatEvent::Passthrough { .. } => None,
        }
    }
}

impl UpdateProjection for ChatEventProjection {
    fn project(&self, content: Value) -> Option<SessionUpdate> {
        // A turn update is arbitrary JSON by construction — `plexus_core`'s
        // runtime deliberately knows no protocol's vocabulary. Anything that
        // is not a `ChatEvent` is declined rather than coerced.
        Self::project_event(serde_json::from_value(content).ok()?)
    }
}
