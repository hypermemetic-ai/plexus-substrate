// PLEXUS-6: the 0.6 macros' legacy form (MACRO-13) — these activations keep
// `namespace = …` and their own type names; the name rule's trait would need
// one event type per activation. Its deprecation note is allowed here.
#![allow(deprecated)]
use super::{
    executor::{ClaudeCodeExecutor, LaunchConfig},
    sessions,
    storage::ClaudeCodeStorage,
    types::{ResolveResult, NodeEvent, ClaudeCodeConfig, ChatEvent, MessageRole, Position, RawClaudeEvent, StreamEventInner, StreamDelta, StreamContentBlock, RawContentBlock, ChatUsage, Model, CreateResult, ClaudeCodeError, GetResult, ListResult, DeleteResult, ForkResult, ChatStartResult, StreamId, PollResult, ClaudeCodeId, StreamListResult, GetTreeResult, RenderResult, SessionsListResult, SessionsGetResult, SessionsImportResult, SessionsExportResult, SessionsDeleteResult, StreamStatus},
};
use crate::activations::arbor::{NodeId, TreeId};
use crate::plexus::{AuthContext, Gate, Grant, HubContext, NoParent};
use async_stream::stream;
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};
use tracing::Instrument;

/// `ClaudeCode` activation - manages Claude Code sessions with Arbor-backed history
///
/// Generic over `P: HubContext` to allow different parent contexts:
/// - `Weak<DynamicHub>` when registered with a `DynamicHub`
/// - Custom context types for sub-hubs
/// - `NoParent` for standalone testing
#[derive(Clone)]
pub struct ClaudeCode<P: HubContext = NoParent> {
    pub storage: Arc<ClaudeCodeStorage>,
    executor: ClaudeCodeExecutor,
    /// Hub reference for resolving foreign handles when walking arbor trees
    hub: Arc<OnceLock<P>>,
    /// PLEXUS-6: the hub's gate, once the hub is built behind one — asked
    /// whether a caller is an admin (holds [`ANY_SESSION`]).
    gate: Arc<OnceLock<Arc<Gate>>>,
    _phantom: PhantomData<P>,
}

/// PLEXUS-6: the grant that reaches every session, whoever created it. No
/// role lists it, so only `admin` (which holds every grant) holds it.
pub const ANY_SESSION: &str = "claudecode.any_session";

/// PLEXUS-6: who is asking, as a session's ownership sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Caller {
    /// In process (no auth context: orcha, a hub without a gate) — every
    /// session; what it creates belongs to no one (an admin's to reach).
    System,
    /// An admin (holds [`ANY_SESSION`]): every session; what it creates is
    /// its own.
    Admin(String),
    /// Anyone else: only the sessions it created.
    Principal(String),
}

impl Caller {
    /// The owner a lookup is held to (`None`: every session).
    fn scope(&self) -> Option<String> {
        match self {
            Self::Principal(p) => Some(p.clone()),
            Self::System | Self::Admin(_) => None,
        }
    }

    /// Who a session it creates belongs to.
    fn owner(&self) -> Option<String> {
        match self {
            Self::System => None,
            Self::Admin(p) | Self::Principal(p) => Some(p.clone()),
        }
    }
}

impl<P: HubContext> ClaudeCode<P> {
    /// Create a new `ClaudeCode` with a specific parent context type
    pub fn with_context_type(storage: Arc<ClaudeCodeStorage>) -> Self {
        Self {
            storage,
            executor: ClaudeCodeExecutor::new(),
            hub: Arc::new(OnceLock::new()),
            gate: Arc::new(OnceLock::new()),
            _phantom: PhantomData,
        }
    }

    /// Create with custom executor and parent context type
    pub fn with_executor_and_context(storage: Arc<ClaudeCodeStorage>, executor: ClaudeCodeExecutor) -> Self {
        Self {
            storage,
            executor,
            hub: Arc::new(OnceLock::new()),
            gate: Arc::new(OnceLock::new()),
            _phantom: PhantomData,
        }
    }

    /// Inject parent context for resolving foreign handles
    ///
    /// Called during hub construction (e.g., via `Arc::new_cyclic` for `DynamicHub`).
    /// This allows `ClaudeCode` to resolve handles from other activations when walking arbor trees.
    pub fn inject_parent(&self, parent: P) {
        let _ = self.hub.set(parent);
    }

    /// PLEXUS-6: the hub's gate, after the hub is built behind one; every
    /// clone shares it. The first call wins.
    pub fn set_gate(&self, gate: Arc<Gate>) {
        let _ = self.gate.set(gate);
    }

    /// PLEXUS-6: the caller of a wire call (`auth`), or in process (`None`).
    /// A context that is not signed in owns nothing and is shown nothing.
    fn caller(&self, auth: Option<&AuthContext>) -> Caller {
        let Some(ctx) = auth else { return Caller::System };
        if !ctx.is_authenticated() {
            return Caller::Principal(String::new());
        }
        let principal = ctx.principal().map_or_else(|| ctx.user_id.clone(), |p| p.to_string());
        let admin = self.gate.get().is_some_and(|g| g.check(Some(ctx), &Grant::new(ANY_SESSION), None).is_ok());
        if admin { Caller::Admin(principal) } else { Caller::Principal(principal) }
    }

    /// PLEXUS-6: whether `caller` reaches the session `session_id` — its
    /// owner, an admin, or in process. A session that is not the caller's
    /// answers as one that does not exist.
    async fn reaches(&self, caller: &Caller, session_id: &ClaudeCodeId) -> Result<(), ClaudeCodeError> {
        let Some(me) = caller.scope() else { return Ok(()) };
        match self.storage.session_owner(session_id).await? {
            Some(owner) if owner == me => Ok(()),
            _ => Err(ClaudeCodeError::SessionNotFound { identifier: session_id.to_string() }),
        }
    }

    /// Check if parent context has been injected
    pub fn has_parent(&self) -> bool {
        self.hub.get().is_some()
    }

    /// Get a reference to the parent context
    ///
    /// Returns None if `inject_parent` hasn't been called yet.
    pub fn parent(&self) -> Option<&P> {
        self.hub.get()
    }

    /// Resolve a claudecode handle to its message content
    ///
    /// Called by the macro-generated `resolve_handle` method.
    /// Handle format: {plugin_id}@1.0.0::chat:msg-{uuid}:{role}:{name}
    pub async fn resolve_handle_impl(
        &self,
        handle: &crate::types::Handle,
    ) -> Result<crate::plexus::PlexusStream, crate::plexus::PlexusError> {
        use crate::plexus::{PlexusError, wrap_stream};
        use async_stream::stream;

        let storage = self.storage.clone();

        // Join meta parts into colon-separated identifier
        // Format: "msg-{uuid}:{role}:{name}"
        if handle.meta.is_empty() {
            return Err(PlexusError::ExecutionError(
                "ClaudeCode handle missing message ID in meta".to_string()
            ));
        }
        let identifier = handle.meta.join(":");

        // Extract name from meta if present (for response)
        let name = handle.meta.get(2).cloned();

        let result_stream = stream! {
            match storage.resolve_message_handle(&identifier).await {
                Ok(message) => {
                    yield ResolveResult::Message {
                        id: message.id.to_string(),
                        role: message.role.as_str().to_string(),
                        content: message.content,
                        model: message.model_id,
                        name: name.unwrap_or_else(|| message.role.as_str().to_string()),
                    };
                }
                Err(e) => {
                    yield ResolveResult::Error {
                        message: format!("Failed to resolve handle: {e}"),
                    };
                }
            }
        };

        Ok(wrap_stream(result_stream, "claudecode.resolve_handle", vec!["claudecode".into()]))
    }
}

/// Convenience constructors for `ClaudeCode` with `NoParent` (standalone/testing)
impl ClaudeCode<NoParent> {
    pub fn new(storage: Arc<ClaudeCodeStorage>) -> Self {
        Self::with_context_type(storage)
    }

    pub fn with_executor(storage: Arc<ClaudeCodeStorage>, executor: ClaudeCodeExecutor) -> Self {
        Self::with_executor_and_context(storage, executor)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ARBOR EVENT CAPTURE HELPERS (Milestone 2)
// ═══════════════════════════════════════════════════════════════════════════

/// Create an arbor text node for a chat event
async fn create_event_node(
    arbor: &crate::activations::arbor::ArborStorage,
    tree_id: &crate::activations::arbor::TreeId,
    parent_id: &crate::activations::arbor::NodeId,
    event: &NodeEvent,
) -> Result<crate::activations::arbor::NodeId, String> {
    let json = serde_json::to_string(event)
        .map_err(|e| format!("Failed to serialize event: {e}"))?;

    arbor.node_create_text(tree_id, Some(*parent_id), json, None)
        .await
        .map_err(|e| e.to_string())
}

// ═══════════════════════════════════════════════════════════════════════════
// Shared chat stream body (IR-18)
//
// The `chat_stream_for_config` helper owns the full per-turn Claude
// interaction: user-message persistence, arbor event-node chain, launching
// the Claude CLI, relaying the stream, storing the assistant response, and
// advancing the session head.
//
// Flat `ClaudeCode::chat(name, ...)` resolves by name and delegates here.
// The new per-session `SessionChild::chat(prompt, ...)` (IR-18) takes
// its pre-resolved `ClaudeCodeConfig` and delegates here as well. This keeps
// the two entry points behaviorally identical without copy-paste.
// ═══════════════════════════════════════════════════════════════════════════

/// Run a Claude chat turn against an already-resolved session config, streaming
/// `ChatEvent`s to the caller.
///
/// Callers resolve the session by whatever key makes sense (name for flat
/// `chat`, `session_id` for the typed `session` child-gate) and hand a fully
/// materialized `ClaudeCodeConfig` in; this helper owns the rest of the turn.
fn chat_stream_for_config(
    storage: Arc<ClaudeCodeStorage>,
    executor: ClaudeCodeExecutor,
    config: ClaudeCodeConfig,
    prompt: String,
    ephemeral: Option<bool>,
    allowed_tools: Option<Vec<String>>,
) -> impl Stream<Item = ChatEvent> + Send + 'static {
    stream! {
        let is_ephemeral = ephemeral.unwrap_or(false);
        let session_id = config.id;

        // 2. Store user message in our database (ephemeral if requested)
        let user_msg = if is_ephemeral {
            match storage.message_create_ephemeral(
                &session_id,
                MessageRole::User,
                prompt.clone(),
                None, None, None, None,
            ).await {
                Ok(m) => m,
                Err(e) => {
                    yield ChatEvent::Err { message: e.to_string() };
                    return;
                }
            }
        } else {
            match storage.message_create(
                &session_id,
                MessageRole::User,
                prompt.clone(),
                None, None, None, None,
            ).await {
                Ok(m) => m,
                Err(e) => {
                    yield ChatEvent::Err { message: e.to_string() };
                    return;
                }
            }
        };

        // 3. Create user message node in Arbor (ephemeral if requested)
        let user_handle = ClaudeCodeStorage::message_to_handle(&user_msg, "user");
        let user_node_id = if is_ephemeral {
            match storage.arbor().node_create_external_ephemeral(
                &config.head.tree_id,
                Some(config.head.node_id),
                user_handle,
                None,
            ).await {
                Ok(id) => id,
                Err(e) => {
                    yield ChatEvent::Err { message: e.to_string() };
                    return;
                }
            }
        } else {
            match storage.arbor().node_create_external(
                &config.head.tree_id,
                Some(config.head.node_id),
                user_handle,
                None,
            ).await {
                Ok(id) => id,
                Err(e) => {
                    yield ChatEvent::Err { message: e.to_string() };
                    return;
                }
            }
        };

        let user_position = Position::new(config.head.tree_id, user_node_id);

        // Track current parent for event node chain
        let mut current_parent = user_node_id;

        let user_event = NodeEvent::UserMessage { content: prompt.clone() };
        if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &user_event).await {
            current_parent = node_id;
        }

        let start_event = NodeEvent::AssistantStart;
        if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &start_event).await {
            current_parent = node_id;
        }

        // 4. Emit Start
        yield ChatEvent::Start {
            id: session_id,
            user_position,
        };

        // 5. Build launch config
        let launch_config = LaunchConfig {
            query: prompt,
            session_id: config.claude_session_id.clone(),
            fork_session: false,
            model: config.model,
            working_dir: config.working_dir.clone(),
            system_prompt: config.system_prompt.clone(),
            mcp_config: config.mcp_config.clone(),
            loopback_enabled: config.loopback_enabled,
            loopback_session_id: if config.loopback_enabled {
                config.loopback_session_id.clone()
            } else {
                None
            },
            allowed_tools: allowed_tools.unwrap_or_default(),
            ..Default::default()
        };

        // 6. Launch Claude and stream events
        let prev_claude_session_id = config.claude_session_id.clone();
        let mut response_content = String::new();
        let mut claude_session_id = config.claude_session_id.clone();
        let mut cost_usd = None;
        let mut num_turns = None;

        let mut raw_stream = executor.launch(launch_config).await;

        let mut current_tool_id: Option<String> = None;
        let mut current_tool_name: Option<String> = None;
        let mut current_tool_input = String::new();

        while let Some(event) = raw_stream.next().await {
            match event {
                RawClaudeEvent::System { session_id: sid, .. } => {
                    if let Some(id) = sid {
                        claude_session_id = Some(id);
                    }
                }
                RawClaudeEvent::StreamEvent { event: inner, session_id: sid } => {
                    if let Some(id) = sid {
                        claude_session_id = Some(id);
                    }
                    match inner {
                        StreamEventInner::ContentBlockDelta { delta, .. } => {
                            match delta {
                                StreamDelta::TextDelta { text } => {
                                    response_content.push_str(&text);

                                    let event = NodeEvent::ContentText { text: text.clone() };
                                    if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                                        current_parent = node_id;
                                    }

                                    yield ChatEvent::Content { text };
                                }
                                StreamDelta::InputJsonDelta { partial_json } => {
                                    current_tool_input.push_str(&partial_json);
                                }
                            }
                        }
                        StreamEventInner::ContentBlockStart {
                            content_block: Some(StreamContentBlock::ToolUse { id, name, .. }),
                            ..
                        } => {
                            current_tool_id = Some(id);
                            current_tool_name = Some(name);
                            current_tool_input.clear();
                        }
                        StreamEventInner::ContentBlockStop { .. } => {
                            if let (Some(id), Some(name)) = (current_tool_id.take(), current_tool_name.take()) {
                                let input: Value = serde_json::from_str(&current_tool_input)
                                    .unwrap_or(Value::Object(serde_json::Map::new()));

                                let event = NodeEvent::ContentToolUse {
                                    id: id.clone(),
                                    name: name.clone(),
                                    input: input.clone(),
                                };
                                if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                                    current_parent = node_id;
                                }

                                yield ChatEvent::ToolUse {
                                    tool_name: name,
                                    tool_use_id: id,
                                    input,
                                };
                                current_tool_input.clear();
                            }
                        }
                        _ => {}
                    }
                }
                RawClaudeEvent::Assistant { message } => {
                    if let Some(msg) = message {
                        if let Some(content) = msg.content {
                            for block in content {
                                match block {
                                    RawContentBlock::Text { text } => {
                                        if response_content.is_empty() {
                                            response_content.push_str(&text);

                                            let event = NodeEvent::ContentText { text: text.clone() };
                                            if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                                                current_parent = node_id;
                                            }

                                            yield ChatEvent::Content { text };
                                        }
                                    }
                                    RawContentBlock::ToolUse { id, name, input } => {
                                        let event = NodeEvent::ContentToolUse {
                                            id: id.clone(),
                                            name: name.clone(),
                                            input: input.clone(),
                                        };
                                        if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                                            current_parent = node_id;
                                        }

                                        yield ChatEvent::ToolUse {
                                            tool_name: name,
                                            tool_use_id: id,
                                            input,
                                        };
                                    }
                                    RawContentBlock::ToolResult { tool_use_id, content, is_error } => {
                                        let event = NodeEvent::UserToolResult {
                                            tool_use_id: tool_use_id.clone(),
                                            content: content.clone().unwrap_or_default(),
                                            is_error: is_error.unwrap_or(false),
                                        };
                                        if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                                            current_parent = node_id;
                                        }

                                        yield ChatEvent::ToolResult {
                                            tool_use_id,
                                            output: content.unwrap_or_default(),
                                            is_error: is_error.unwrap_or(false),
                                        };
                                    }
                                    RawContentBlock::Thinking { thinking, .. } => {
                                        let event = NodeEvent::ContentThinking { thinking: thinking.clone() };
                                        if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                                            current_parent = node_id;
                                        }

                                        yield ChatEvent::Thinking { thinking };
                                    }
                                }
                            }
                        }
                    }
                }
                RawClaudeEvent::Result {
                    session_id: sid,
                    cost_usd: cost,
                    num_turns: turns,
                    is_error,
                    error,
                    ..
                } => {
                    if let Some(id) = sid {
                        claude_session_id = Some(id);
                    }
                    cost_usd = cost;
                    num_turns = turns;

                    if is_error == Some(true) {
                        if let Some(err_msg) = error {
                            yield ChatEvent::Err { message: err_msg };
                            return;
                        }
                    }
                }
                RawClaudeEvent::Unknown { event_type, data } => {
                    match storage.unknown_event_store(Some(&session_id), &event_type, &data).await {
                        Ok(handle) => {
                            tracing::debug!(event_type = %event_type, handle = %handle, "Unknown Claude event stored");
                            yield ChatEvent::Passthrough { event_type, handle, data };
                        }
                        Err(e) => {
                            tracing::warn!(event_type = %event_type, error = %e, "Failed to store unknown event");
                            yield ChatEvent::Passthrough {
                                event_type,
                                handle: "storage-failed".to_string(),
                                data,
                            };
                        }
                    }
                }
                RawClaudeEvent::User { .. } => {}
                RawClaudeEvent::LaunchCommand { command } => {
                    let event = NodeEvent::LaunchCommand { command };
                    if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                        current_parent = node_id;
                    }
                }
                RawClaudeEvent::Stderr { text } => {
                    tracing::warn!(stderr = %text, "Claude stderr");
                    let event = NodeEvent::ClaudeStderr { text };
                    if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                        current_parent = node_id;
                    }
                }
            }
        }

        // 6b. If we captured a new Claude session UUID, persist it for future --resume
        if let Some(ref new_id) = claude_session_id {
            if prev_claude_session_id.as_deref() != Some(new_id.as_str()) {
                let _ = storage.session_update_claude_id(&session_id, new_id.clone()).await;
            }
        }

        // Guard: if stream produced nothing, emit error instead of ghost Complete
        if response_content.is_empty() && claude_session_id.is_none() {
            yield ChatEvent::Err {
                message: "Claude process produced no response. Check substrate logs for details.".to_string(),
            };
            return;
        }

        // 7. Store assistant response (ephemeral if requested)
        let model_id = format!("claude-code-{}", config.model.as_str());
        let assistant_msg = if is_ephemeral {
            match storage.message_create_ephemeral(
                &session_id,
                MessageRole::Assistant,
                response_content,
                Some(model_id),
                None,
                None,
                cost_usd,
            ).await {
                Ok(m) => m,
                Err(e) => {
                    yield ChatEvent::Err { message: e.to_string() };
                    return;
                }
            }
        } else {
            match storage.message_create(
                &session_id,
                MessageRole::Assistant,
                response_content,
                Some(model_id),
                None,
                None,
                cost_usd,
            ).await {
                Ok(m) => m,
                Err(e) => {
                    yield ChatEvent::Err { message: e.to_string() };
                    return;
                }
            }
        };

        // AssistantComplete event node
        let complete_event = NodeEvent::AssistantComplete { usage: None };
        if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &complete_event).await {
            current_parent = node_id;
        }

        // 8. Create assistant node in Arbor (ephemeral if requested)
        let assistant_handle = ClaudeCodeStorage::message_to_handle(&assistant_msg, "assistant");
        let assistant_node_id = if is_ephemeral {
            match storage.arbor().node_create_external_ephemeral(
                &config.head.tree_id,
                Some(current_parent),
                assistant_handle,
                None,
            ).await {
                Ok(id) => id,
                Err(e) => {
                    yield ChatEvent::Err { message: e.to_string() };
                    return;
                }
            }
        } else {
            match storage.arbor().node_create_external(
                &config.head.tree_id,
                Some(current_parent),
                assistant_handle,
                None,
            ).await {
                Ok(id) => id,
                Err(e) => {
                    yield ChatEvent::Err { message: e.to_string() };
                    return;
                }
            }
        };

        let new_head = Position::new(config.head.tree_id, assistant_node_id);

        // 9. Update session head and Claude session ID (skip for ephemeral)
        if !is_ephemeral {
            if let Err(e) = storage.session_update_head(&session_id, assistant_node_id, claude_session_id.clone()).await {
                yield ChatEvent::Err { message: e.to_string() };
                return;
            }
        }

        // 10. Emit Complete
        yield ChatEvent::Complete {
            new_head: if is_ephemeral { config.head } else { new_head },
            claude_session_id: claude_session_id.unwrap_or_default(),
            usage: Some(ChatUsage {
                input_tokens: None,
                output_tokens: None,
                cost_usd,
                num_turns,
            }),
        };
    }
}

// PLEXUS-6: each method its own grant. A viewer reads its own sessions, an
// editor creates and drives them (a session is its creator's: the methods
// below hold every lookup to it, an admin reaching all). Unlisted — the
// sessions_* methods over this machine's Claude Code files, and a session's
// dynamic child path — only admin holds.
#[plexus_macros::activation(namespace = "claudecode",
version = "1.0.0",
description = "Manage Claude Code sessions with Arbor-backed conversation history",
resolve_handle,
roles(
    viewer = ["get", "list", "poll", "streams", "get_tree", "render_context"],
    editor = [viewer, "create", "chat", "chat_async", "fork", "delete"]
))]
impl<P: HubContext> ClaudeCode<P> {
    /// Create a new Claude Code session
    #[plexus_macros::method(params(
        name = "Human-readable name for the session",
        working_dir = "Working directory for Claude Code",
        model = "Model to use (opus, sonnet, haiku)",
        system_prompt = "Optional system prompt / instructions",
        loopback_enabled = "Enable loopback mode - routes tool permissions through parent for approval",
        loopback_session_id = "Session ID for loopback MCP URL correlation (e.g., orcha-xxx-claude-yyy)"
    ))]
    pub async fn create(
        &self,
        auth: Option<&AuthContext>,
        name: String,
        working_dir: String,
        model: Model,
        system_prompt: Option<String>,
        loopback_enabled: Option<bool>,
        loopback_session_id: Option<String>,
    ) -> impl Stream<Item = CreateResult> + Send + 'static {
        let storage = self.storage.clone();
        let loopback = loopback_enabled.unwrap_or(false);
        let owner = self.caller(auth).owner();

        stream! {
            // PLEXUS-6: a session belongs to a signed-in principal.
            if owner.as_deref() == Some("") {
                yield CreateResult::Err { message: "sign in first: a session belongs to the principal that creates it".into() };
                return;
            }

            // Resolve relative paths to absolute before storing
            let working_dir = match std::fs::canonicalize(&working_dir) {
                Ok(p) => p.to_string_lossy().into_owned(),
                Err(e) => {
                    yield CreateResult::Err {
                        message: ClaudeCodeError::PathResolution {
                            path: working_dir,
                            source: e,
                        }.to_string(),
                    };
                    return;
                }
            };

            // Fail fast: if loopback is requested, the MCP server must be reachable.
            // Without it Claude cannot resolve the permission-prompt tool and will
            // return empty output instead of an error.
            if loopback {
                if let Err(e) = super::executor::check_mcp_reachable().await {
                    yield CreateResult::Err { message: e };
                    return;
                }
            }

            // claude_session_id is None initially; populated after first chat with real Claude UUID
            match storage.session_create(name, working_dir, model, system_prompt, None, loopback, None, loopback_session_id, None).await {
                Ok(config) => {
                    if let Some(owner) = &owner {
                        if let Err(e) = storage.session_set_owner(&config.id, owner).await {
                            // Unowned, it is an admin's alone: say so rather than hand it over.
                            yield CreateResult::Err { message: e.to_string() };
                            return;
                        }
                    }
                    yield CreateResult::Ok {
                        id: config.id,
                        head: config.head,
                    };
                }
                Err(e) => {
                    yield CreateResult::Err { message: e.to_string() };
                }
            }
        }
    }

    /// Chat with a session, streaming tokens like Cone
    #[plexus_macros::method(streaming,
    params(
        name = "Session name to chat with",
        prompt = "User message / prompt to send",
        ephemeral = "If true, creates nodes but doesn't advance head and marks for deletion",
        allowed_tools = "Optional list of tools to allow (e.g. [\"WebSearch\", \"Read\"])"
    ))]
    pub async fn chat(
        &self,
        auth: Option<&AuthContext>,
        name: String,
        prompt: String,
        ephemeral: Option<bool>,
        allowed_tools: Option<Vec<String>>,
    ) -> impl Stream<Item = ChatEvent> + Send + 'static {
        let storage = self.storage.clone();
        let executor = self.executor.clone();

        // Resolve by name before entering the stream (matches pre-IR-18 semantics).
        // PLEXUS-6: among the caller's own sessions.
        let resolve_result = storage.session_get_by_name_owned_by(&name, self.caller(auth).scope().as_deref()).await;

        stream! {
            let config = match resolve_result {
                Ok(c) => c,
                Err(e) => {
                    yield ChatEvent::Err { message: e.to_string() };
                    return;
                }
            };

            // Hand off to the shared per-turn helper (IR-18).
            let mut inner = Box::pin(chat_stream_for_config(
                storage,
                executor,
                config,
                prompt,
                ephemeral,
                allowed_tools,
            ));
            while let Some(ev) = inner.next().await {
                yield ev;
            }
        }
    }

    /// Get session configuration details
    #[plexus_macros::method]
    async fn get(&self, auth: Option<&AuthContext>, name: String) -> impl Stream<Item = GetResult> + Send + 'static {
        let result = self.storage.session_get_by_name_owned_by(&name, self.caller(auth).scope().as_deref()).await;

        stream! {
            match result {
                Ok(config) => {
                    yield GetResult::Ok { config };
                }
                Err(e) => {
                    yield GetResult::Err { message: e.to_string() };
                }
            }
        }
    }

    /// List all Claude Code sessions
    #[plexus_macros::method]
    async fn list(&self, auth: Option<&AuthContext>) -> impl Stream<Item = ListResult> + Send + 'static {
        let storage = self.storage.clone();
        let scope = self.caller(auth).scope();

        stream! {
            match storage.session_list_owned_by(scope.as_deref()).await {
                Ok(sessions) => {
                    yield ListResult::Ok { sessions };
                }
                Err(e) => {
                    yield ListResult::Err { message: e.to_string() };
                }
            }
        }
    }

    /// Delete a session
    #[plexus_macros::method]
    async fn delete(&self, auth: Option<&AuthContext>, name: String) -> impl Stream<Item = DeleteResult> + Send + 'static {
        let storage = self.storage.clone();
        let resolve_result = storage.session_get_by_name_owned_by(&name, self.caller(auth).scope().as_deref()).await;

        stream! {
            let config = match resolve_result {
                Ok(c) => c,
                Err(e) => {
                    yield DeleteResult::Err { message: e.to_string() };
                    return;
                }
            };

            match storage.session_delete(&config.id).await {
                Ok(_) => {
                    yield DeleteResult::Ok { id: config.id };
                }
                Err(e) => {
                    yield DeleteResult::Err { message: e.to_string() };
                }
            }
        }
    }

    /// Fork a session to create a branch point
    #[plexus_macros::method]
    async fn fork(
        &self,
        auth: Option<&AuthContext>,
        name: String,
        new_name: String,
    ) -> impl Stream<Item = ForkResult> + Send + 'static {
        let storage = self.storage.clone();
        let caller = self.caller(auth);
        let resolve_result = storage.session_get_by_name_owned_by(&name, caller.scope().as_deref()).await;
        let owner = caller.owner();

        stream! {
            // Get parent session
            let parent = match resolve_result {
                Ok(c) => c,
                Err(e) => {
                    yield ForkResult::Err { message: e.to_string() };
                    return;
                }
            };

            // Create new session starting at parent's head position
            // The new session will fork Claude's session on first chat
            let new_config = match storage.session_create(
                new_name,
                parent.working_dir.clone(),
                parent.model,
                parent.system_prompt.clone(),
                parent.mcp_config.clone(),
                parent.loopback_enabled,
                None, // claude_session_id - will be set on first chat with fork_session=true
                None, // loopback_session_id
                None, // metadata
            ).await {
                Ok(mut c) => {
                    // PLEXUS-6: the fork is the caller's.
                    if let Some(owner) = &owner {
                        if let Err(e) = storage.session_set_owner(&c.id, owner).await {
                            yield ForkResult::Err { message: e.to_string() };
                            return;
                        }
                    }
                    // Update head to parent's position (share the same tree point)
                    // This creates a branch - the new session diverges from here
                    if let Err(e) = storage.session_update_head(&c.id, parent.head.node_id, None).await {
                        yield ForkResult::Err { message: e.to_string() };
                        return;
                    }
                    c.head = parent.head;
                    c
                }
                Err(e) => {
                    yield ForkResult::Err { message: e.to_string() };
                    return;
                }
            };

            yield ForkResult::Ok {
                id: new_config.id,
                head: new_config.head,
            };
        }
    }

    /// Start an async chat - returns immediately with `stream_id` for polling
    ///
    /// This is the non-blocking version of chat, designed for loopback scenarios
    /// where the parent needs to poll for events and handle tool approvals.
    #[plexus_macros::method(params(
        name = "Session name to chat with",
        prompt = "User message / prompt to send",
        ephemeral = "If true, creates nodes but doesn't advance head and marks for deletion"
    ))]
    async fn chat_async(
        &self,
        auth: Option<&AuthContext>,
        name: String,
        prompt: String,
        ephemeral: Option<bool>,
    ) -> impl Stream<Item = ChatStartResult> + Send + 'static {
        let storage = self.storage.clone();
        let executor = self.executor.clone();

        // Resolve session before entering stream (PLEXUS-6: among the caller's own)
        let resolve_result = storage.session_get_by_name_owned_by(&name, self.caller(auth).scope().as_deref()).await;

        stream! {
            let is_ephemeral = ephemeral.unwrap_or(false);

            // 1. Resolve session
            let config = match resolve_result {
                Ok(c) => c,
                Err(e) => {
                    yield ChatStartResult::Err { message: e.to_string() };
                    return;
                }
            };

            let session_id = config.id;

            // 2. Create stream buffer
            let stream_id = match storage.stream_create(session_id).await {
                Ok(id) => id,
                Err(e) => {
                    yield ChatStartResult::Err { message: e.to_string() };
                    return;
                }
            };

            // 3. Spawn background task to run the chat
            let storage_bg = storage.clone();
            let executor_bg = executor.clone();
            let prompt_bg = prompt.clone();
            let config_bg = config.clone();
            let stream_id_bg = stream_id;

            tokio::spawn(async move {
                Self::run_chat_background(
                    storage_bg,
                    executor_bg,
                    config_bg,
                    prompt_bg,
                    is_ephemeral,
                    stream_id_bg,
                ).await;
            }.instrument(tracing::info_span!("chat_async_bg", stream_id = %stream_id)));

            // 4. Return immediately with stream_id
            yield ChatStartResult::Ok {
                stream_id,
                session_id,
            };
        }
    }

    /// Poll a stream for new events
    ///
    /// Returns events since the last poll (or from the specified offset).
    /// Use this to read events from an async chat started with `chat_async`.
    #[plexus_macros::method(params(
        stream_id = "Stream ID returned from chat_async",
        from_seq = "Optional: start reading from this sequence number",
        limit = "Optional: max events to return (default 100)"
    ))]
    async fn poll(
        &self,
        auth: Option<&AuthContext>,
        stream_id: StreamId,
        from_seq: Option<u64>,
        limit: Option<u64>,
    ) -> impl Stream<Item = PollResult> + Send + 'static {
        let storage = self.storage.clone();
        // PLEXUS-6: a stream is its session's; another's is not found.
        let caller = self.caller(auth);
        let reached = match storage.stream_get_info(&stream_id).await {
            Ok(info) => self.reaches(&caller, &info.session_id).await
                .map_err(|_| ClaudeCodeError::SessionNotFound { identifier: format!("stream:{stream_id}") }),
            Err(e) => Err(e),
        };

        stream! {
            if let Err(e) = reached {
                yield PollResult::Err { message: e.to_string() };
                return;
            }
            let limit_usize = limit.map(|l| l as usize);

            match storage.stream_poll(&stream_id, from_seq, limit_usize).await {
                Ok((info, events)) => {
                    let has_more = info.read_position < info.event_count;
                    yield PollResult::Ok {
                        status: info.status,
                        events,
                        read_position: info.read_position,
                        total_events: info.event_count,
                        has_more,
                    };
                }
                Err(e) => {
                    yield PollResult::Err { message: e.to_string() };
                }
            }
        }
    }

    /// List active streams
    ///
    /// Returns all active streams, optionally filtered by session.
    #[plexus_macros::method(params(
        session_id = "Optional: filter by session ID"
    ))]
    async fn streams(
        &self,
        auth: Option<&AuthContext>,
        session_id: Option<ClaudeCodeId>,
    ) -> impl Stream<Item = StreamListResult> + Send + 'static {
        let storage = self.storage.clone();
        let caller = self.caller(auth);
        let all = if let Some(sid) = session_id {
            storage.stream_list_for_session(&sid).await
        } else {
            storage.stream_list().await
        };
        // PLEXUS-6: only the streams of the caller's own sessions.
        let mut streams = Vec::with_capacity(all.len());
        for s in all {
            if self.reaches(&caller, &s.session_id).await.is_ok() {
                streams.push(s);
            }
        }

        stream! {
            yield StreamListResult::Ok { streams };
        }
    }

    /// Get arbor tree information for a session
    #[plexus_macros::method(params(
        name = "Session name"
    ))]
    async fn get_tree(
        &self,
        auth: Option<&AuthContext>,
        name: String,
    ) -> impl Stream<Item = GetTreeResult> + Send + 'static {
        let storage = self.storage.clone();
        let scope = self.caller(auth).scope();

        stream! {
            let config = match storage.session_get_by_name_owned_by(&name, scope.as_deref()).await {
                Ok(c) => c,
                Err(e) => {
                    yield GetTreeResult::Err { message: e.to_string() };
                    return;
                }
            };

            yield GetTreeResult::Ok {
                tree_id: config.head.tree_id,
                head: config.head.node_id,
            };
        }
    }

    /// Render arbor tree as Claude API messages
    #[plexus_macros::method(params(
        name = "Session name",
        start = "Optional start node (default: root)",
        end = "Optional end node (default: head)"
    ))]
    async fn render_context(
        &self,
        auth: Option<&AuthContext>,
        name: String,
        start: Option<NodeId>,
        end: Option<NodeId>,
    ) -> impl Stream<Item = RenderResult> + Send + 'static {
        let storage = self.storage.clone();
        let scope = self.caller(auth).scope();

        stream! {
            // Get session config (PLEXUS-6: among the caller's own)
            let config = match storage.session_get_by_name_owned_by(&name, scope.as_deref()).await {
                Ok(c) => c,
                Err(e) => {
                    yield RenderResult::Err { message: e.to_string() };
                    return;
                }
            };

            let tree_id = config.head.tree_id;
            let end_node = end.unwrap_or(config.head.node_id);

            // Get root if start not specified
            let start_node = if let Some(s) = start {
                s
            } else {
                match storage.arbor().tree_get(&tree_id).await {
                    Ok(tree) => tree.root,
                    Err(e) => {
                        yield RenderResult::Err { message: e.to_string() };
                        return;
                    }
                }
            };

            // Render messages
            let messages = match storage.render_messages(&tree_id, &start_node, &end_node).await {
                Ok(m) => m,
                Err(e) => {
                    yield RenderResult::Err { message: e.to_string() };
                    return;
                }
            };

            yield RenderResult::Ok { messages };
        }
    }

    /// List all session files for a project
    #[plexus_macros::method(params(
        project_path = "Project path (e.g., '-workspace-hypermemetic-hub-codegen')"
    ))]
    async fn sessions_list(
        &self,
        project_path: String,
    ) -> impl Stream<Item = SessionsListResult> + Send + 'static {
        stream! {
            match sessions::list_sessions(&project_path).await {
                Ok(sessions) => {
                    yield SessionsListResult::Ok { sessions };
                }
                Err(e) => {
                    yield SessionsListResult::Err { message: e };
                }
            }
        }
    }

    /// Get events from a session file
    #[plexus_macros::method(params(
        project_path = "Project path",
        session_id = "Session ID (UUID)"
    ))]
    async fn sessions_get(
        &self,
        project_path: String,
        session_id: String,
    ) -> impl Stream<Item = SessionsGetResult> + Send + 'static {
        stream! {
            match sessions::read_session(&project_path, &session_id).await {
                Ok(events) => {
                    let event_count = events.len();
                    // Convert to JSON values for transport
                    let events_json: Vec<serde_json::Value> = events.into_iter()
                        .filter_map(|e| serde_json::to_value(e).ok())
                        .collect();

                    yield SessionsGetResult::Ok {
                        session_id: session_id.clone(),
                        event_count,
                        events: events_json,
                    };
                }
                Err(e) => {
                    yield SessionsGetResult::Err { message: e };
                }
            }
        }
    }

    /// Import a session file into arbor
    #[plexus_macros::method(params(
        project_path = "Project path",
        session_id = "Session ID to import",
        owner_id = "Owner ID for the new tree (default: 'claudecode')"
    ))]
    async fn sessions_import(
        &self,
        project_path: String,
        session_id: String,
        owner_id: Option<String>,
    ) -> impl Stream<Item = SessionsImportResult> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            let owner = owner_id.unwrap_or_else(|| "claudecode".to_string());

            match sessions::import_to_arbor(storage.arbor(), &project_path, &session_id, &owner).await {
                Ok(tree_id) => {
                    yield SessionsImportResult::Ok {
                        tree_id,
                        session_id,
                    };
                }
                Err(e) => {
                    yield SessionsImportResult::Err { message: e };
                }
            }
        }
    }

    /// Export an arbor tree to a session file
    #[plexus_macros::method(params(
        tree_id = "Arbor tree ID to export",
        project_path = "Project path",
        session_id = "Session ID for the exported file"
    ))]
    async fn sessions_export(
        &self,
        tree_id: TreeId,
        project_path: String,
        session_id: String,
    ) -> impl Stream<Item = SessionsExportResult> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            match sessions::export_from_arbor(storage.arbor(), &tree_id, &project_path, &session_id).await {
                Ok(()) => {
                    yield SessionsExportResult::Ok {
                        tree_id,
                        session_id,
                    };
                }
                Err(e) => {
                    yield SessionsExportResult::Err { message: e };
                }
            }
        }
    }

    /// Delete a session file
    #[plexus_macros::method(params(
        project_path = "Project path",
        session_id = "Session ID to delete"
    ))]
    async fn sessions_delete(
        &self,
        project_path: String,
        session_id: String,
    ) -> impl Stream<Item = SessionsDeleteResult> + Send + 'static {
        stream! {
            match sessions::delete_session(&project_path, &session_id).await {
                Ok(()) => {
                    yield SessionsDeleteResult::Ok {
                        session_id,
                        deleted: true,
                    };
                }
                Err(e) => {
                    yield SessionsDeleteResult::Err { message: e };
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Dynamic child gate — sessions as typed sub-namespaces (IR-18).
    //
    // `claudecode.session <id>.chat(…)` now resolves through the
    // macro-generated `ChildRouter` to a `SessionChild`. Flat methods
    // (`chat`, `list`, `delete`, `fork`, etc.) remain unchanged — this gate
    // is purely additive.
    // ═══════════════════════════════════════════════════════════════════════

    /// Look up a session by its ID and return a typed per-session namespace.
    ///
    /// The returned `SessionChild` exposes per-session methods
    /// (`chat`, `get`, `delete`) under the `session` namespace. Resolution
    /// fails with `None` if the ID doesn't parse as a UUID or no session
    /// with that ID exists.
    #[plexus_macros::child(list = "session_ids")]
    async fn session(&self, id: &str) -> Option<SessionChild> {
        let session_id = ClaudeCodeId::parse_str(id).ok()?;
        match self.storage.session_exists(&session_id).await {
            Ok(true) => Some(SessionChild::new(
                session_id,
                self.storage.clone(),
                self.executor.clone(),
            )),
            _ => None,
        }
    }

    /// Enumerate session IDs for `ChildRouter::list_children` tab-completion.
    ///
    /// Streams as `String`s because `ChildRouter::list_children` is typed
    /// that way (routing keys are always stringly-typed on the wire).
    async fn session_ids(&self) -> impl Stream<Item = String> + Send + '_ {
        let ids = self.storage.list_session_ids().await.unwrap_or_default();
        futures::stream::iter(ids.into_iter().map(|id| id.to_string()))
    }
}

// Background task implementation (outside the hub_methods block)
impl<P: HubContext> ClaudeCode<P> {
    /// Run chat in background, pushing events to stream buffer
    async fn run_chat_background(
        storage: Arc<ClaudeCodeStorage>,
        executor: ClaudeCodeExecutor,
        config: ClaudeCodeConfig,
        prompt: String,
        is_ephemeral: bool,
        stream_id: StreamId,
    ) {
        let session_id = config.id;

        // 1. Store user message
        let user_msg = if is_ephemeral {
            match storage.message_create_ephemeral(
                &session_id,
                MessageRole::User,
                prompt.clone(),
                None, None, None, None,
            ).await {
                Ok(m) => m,
                Err(e) => {
                    if let Err(e2) = storage.stream_push_event(&stream_id, ChatEvent::Err { message: e.to_string() }).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to push error event to stream");
                    }
                    if let Err(e2) = storage.stream_set_status(&stream_id, StreamStatus::Failed, Some(e.to_string())).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to update stream status to Failed");
                    }
                    return;
                }
            }
        } else {
            match storage.message_create(
                &session_id,
                MessageRole::User,
                prompt.clone(),
                None, None, None, None,
            ).await {
                Ok(m) => m,
                Err(e) => {
                    if let Err(e2) = storage.stream_push_event(&stream_id, ChatEvent::Err { message: e.to_string() }).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to push error event to stream");
                    }
                    if let Err(e2) = storage.stream_set_status(&stream_id, StreamStatus::Failed, Some(e.to_string())).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to update stream status to Failed");
                    }
                    return;
                }
            }
        };

        // 2. Create user node in Arbor
        let user_handle = ClaudeCodeStorage::message_to_handle(&user_msg, "user");
        let user_node_id = if is_ephemeral {
            match storage.arbor().node_create_external_ephemeral(
                &config.head.tree_id,
                Some(config.head.node_id),
                user_handle,
                None,
            ).await {
                Ok(id) => id,
                Err(e) => {
                    if let Err(e2) = storage.stream_push_event(&stream_id, ChatEvent::Err { message: e.to_string() }).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to push error event to stream");
                    }
                    if let Err(e2) = storage.stream_set_status(&stream_id, StreamStatus::Failed, Some(e.to_string())).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to update stream status to Failed");
                    }
                    return;
                }
            }
        } else {
            match storage.arbor().node_create_external(
                &config.head.tree_id,
                Some(config.head.node_id),
                user_handle,
                None,
            ).await {
                Ok(id) => id,
                Err(e) => {
                    if let Err(e2) = storage.stream_push_event(&stream_id, ChatEvent::Err { message: e.to_string() }).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to push error event to stream");
                    }
                    if let Err(e2) = storage.stream_set_status(&stream_id, StreamStatus::Failed, Some(e.to_string())).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to update stream status to Failed");
                    }
                    return;
                }
            }
        };

        let user_position = Position::new(config.head.tree_id, user_node_id);

        // Track current parent for event node chain (Milestone 2)
        let mut current_parent = user_node_id;

        // Create UserMessage event node (Milestone 2)
        let user_event = NodeEvent::UserMessage { content: prompt.clone() };
        if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &user_event).await {
            current_parent = node_id;
        }

        // Create AssistantStart event node (Milestone 2)
        let start_event = NodeEvent::AssistantStart;
        if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &start_event).await {
            current_parent = node_id;
        }

        // Update stream with user position
        if let Err(e) = storage.stream_set_user_position(&stream_id, user_position).await {
            tracing::error!(stream_id = %stream_id, error = %e, "Failed to set user position on stream");
        }

        // 3. Push Start event
        if let Err(e) = storage.stream_push_event(&stream_id, ChatEvent::Start {
            id: session_id,
            user_position,
        }).await {
            tracing::error!(stream_id = %stream_id, error = %e, "Failed to push event to stream");
        }

        // 4. Build launch config
        let launch_config = LaunchConfig {
            query: prompt,
            session_id: config.claude_session_id.clone(),
            fork_session: false,
            model: config.model,
            working_dir: config.working_dir.clone(),
            system_prompt: config.system_prompt.clone(),
            mcp_config: config.mcp_config.clone(),
            loopback_enabled: config.loopback_enabled,
            // Use claude_session_id for MCP URL transparency (e.g., orcha-xxx)
            loopback_session_id: if config.loopback_enabled {
                config.claude_session_id.clone()
            } else {
                None
            },
            ..Default::default()
        };

        // 5. Launch Claude and stream events to buffer
        let mut response_content = String::new();
        let mut claude_session_id = config.claude_session_id.clone();
        let mut cost_usd = None;
        let mut num_turns = None;

        let mut raw_stream = executor.launch(launch_config).await;

        // Track current tool use for streaming
        let mut current_tool_id: Option<String> = None;
        let mut current_tool_name: Option<String> = None;
        let mut current_tool_input = String::new();

        while let Some(event) = raw_stream.next().await {
            match event {
                RawClaudeEvent::System { session_id: sid, .. } => {
                    if let Some(id) = sid {
                        claude_session_id = Some(id);
                    }
                }
                RawClaudeEvent::StreamEvent { event: inner, session_id: sid } => {
                    if let Some(id) = sid {
                        claude_session_id = Some(id);
                    }
                    match inner {
                        StreamEventInner::ContentBlockDelta { delta, .. } => {
                            match delta {
                                StreamDelta::TextDelta { text } => {
                                    response_content.push_str(&text);

                                    // Create arbor node for text content (Milestone 2)
                                    let event = NodeEvent::ContentText { text: text.clone() };
                                    if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                                        current_parent = node_id;
                                    }

                                    if let Err(e) = storage.stream_push_event(&stream_id, ChatEvent::Content { text }).await {
                                        tracing::error!(stream_id = %stream_id, error = %e, "Failed to push event to stream");
                                    }
                                }
                                StreamDelta::InputJsonDelta { partial_json } => {
                                    current_tool_input.push_str(&partial_json);
                                }
                            }
                        }
                        StreamEventInner::ContentBlockStart {
                            content_block: Some(StreamContentBlock::ToolUse { id, name, .. }),
                            ..
                        } => {
                            current_tool_id = Some(id);
                            current_tool_name = Some(name);
                            current_tool_input.clear();
                        }
                        StreamEventInner::ContentBlockStop { .. } => {
                            if let (Some(id), Some(name)) = (current_tool_id.take(), current_tool_name.take()) {
                                let input: Value = serde_json::from_str(&current_tool_input)
                                    .unwrap_or(Value::Object(serde_json::Map::new()));

                                // Check if this is a loopback_permit call (tool waiting for approval)
                                if name == "mcp__plexus__loopback_permit" {
                                    if let Err(e) = storage.stream_set_status(&stream_id, StreamStatus::AwaitingPermission, None).await {
                                        tracing::error!(stream_id = %stream_id, error = %e, "Failed to update stream status");
                                    }
                                }

                                // Create arbor node for tool use (Milestone 2)
                                let event = NodeEvent::ContentToolUse {
                                    id: id.clone(),
                                    name: name.clone(),
                                    input: input.clone(),
                                };
                                if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                                    current_parent = node_id;
                                }

                                if let Err(e) = storage.stream_push_event(&stream_id, ChatEvent::ToolUse {
                                    tool_name: name,
                                    tool_use_id: id,
                                    input,
                                }).await {
                                    tracing::error!(stream_id = %stream_id, error = %e, "Failed to push event to stream");
                                }
                                current_tool_input.clear();
                            }
                        }
                        StreamEventInner::MessageDelta { delta } => {
                            // If stop_reason is tool_use with loopback, mark as awaiting
                            if delta.stop_reason == Some("tool_use".to_string()) {
                                // Check if we're in loopback mode (already marked above)
                            }
                        }
                        _ => {}
                    }
                }
                RawClaudeEvent::Assistant { message } => {
                    if let Some(msg) = message {
                        if let Some(content) = msg.content {
                            for block in content {
                                match block {
                                    RawContentBlock::Text { text } => {
                                        if response_content.is_empty() {
                                            response_content.push_str(&text);

                                            // Create arbor node for text content (Milestone 2)
                                            let event = NodeEvent::ContentText { text: text.clone() };
                                            if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                                                current_parent = node_id;
                                            }

                                            if let Err(e) = storage.stream_push_event(&stream_id, ChatEvent::Content { text }).await {
                                                tracing::error!(stream_id = %stream_id, error = %e, "Failed to push event to stream");
                                            }
                                        }
                                    }
                                    RawContentBlock::ToolUse { id, name, input } => {
                                        // Create arbor node for tool use (Milestone 2)
                                        let event = NodeEvent::ContentToolUse {
                                            id: id.clone(),
                                            name: name.clone(),
                                            input: input.clone(),
                                        };
                                        if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                                            current_parent = node_id;
                                        }

                                        if let Err(e) = storage.stream_push_event(&stream_id, ChatEvent::ToolUse {
                                            tool_name: name,
                                            tool_use_id: id,
                                            input,
                                        }).await {
                                            tracing::error!(stream_id = %stream_id, error = %e, "Failed to push event to stream");
                                        }
                                    }
                                    RawContentBlock::ToolResult { tool_use_id, content, is_error } => {
                                        // Tool completed - back to running if was awaiting
                                        if let Err(e) = storage.stream_set_status(&stream_id, StreamStatus::Running, None).await {
                                            tracing::error!(stream_id = %stream_id, error = %e, "Failed to update stream status");
                                        }

                                        // Create arbor node for tool result (Milestone 2)
                                        let event = NodeEvent::UserToolResult {
                                            tool_use_id: tool_use_id.clone(),
                                            content: content.clone().unwrap_or_default(),
                                            is_error: is_error.unwrap_or(false),
                                        };
                                        if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                                            current_parent = node_id;
                                        }

                                        if let Err(e) = storage.stream_push_event(&stream_id, ChatEvent::ToolResult {
                                            tool_use_id,
                                            output: content.unwrap_or_default(),
                                            is_error: is_error.unwrap_or(false),
                                        }).await {
                                            tracing::error!(stream_id = %stream_id, error = %e, "Failed to push event to stream");
                                        }
                                    }
                                    RawContentBlock::Thinking { thinking, .. } => {
                                        // Create arbor node for thinking (Milestone 2)
                                        let event = NodeEvent::ContentThinking { thinking: thinking.clone() };
                                        if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                                            current_parent = node_id;
                                        }

                                        if let Err(e) = storage.stream_push_event(&stream_id, ChatEvent::Thinking { thinking }).await {
                                            tracing::error!(stream_id = %stream_id, error = %e, "Failed to push event to stream");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                RawClaudeEvent::Result {
                    session_id: sid,
                    cost_usd: cost,
                    num_turns: turns,
                    is_error,
                    error,
                    ..
                } => {
                    if let Some(id) = sid {
                        claude_session_id = Some(id);
                    }
                    cost_usd = cost;
                    num_turns = turns;

                    if is_error == Some(true) {
                        if let Some(err_msg) = error {
                            if let Err(e) = storage.stream_push_event(&stream_id, ChatEvent::Err { message: err_msg.clone() }).await {
                                tracing::error!(stream_id = %stream_id, error = %e, "Failed to push error event to stream");
                            }
                            if let Err(e) = storage.stream_set_status(&stream_id, StreamStatus::Failed, Some(err_msg)).await {
                                tracing::error!(stream_id = %stream_id, error = %e, "Failed to update stream status to Failed");
                            }
                            return;
                        }
                    }
                }
                RawClaudeEvent::Unknown { event_type, data } => {
                    match storage.unknown_event_store(Some(&session_id), &event_type, &data).await {
                        Ok(handle) => {
                            if let Err(e) = storage.stream_push_event(&stream_id, ChatEvent::Passthrough { event_type, handle, data }).await {
                                tracing::error!(stream_id = %stream_id, error = %e, "Failed to push event to stream");
                            }
                        }
                        Err(_) => {
                            if let Err(e) = storage.stream_push_event(&stream_id, ChatEvent::Passthrough {
                                event_type,
                                handle: "storage-failed".to_string(),
                                data,
                            }).await {
                                tracing::error!(stream_id = %stream_id, error = %e, "Failed to push event to stream");
                            }
                        }
                    }
                }
                RawClaudeEvent::User { .. } => {}
                RawClaudeEvent::LaunchCommand { command } => {
                    let event = NodeEvent::LaunchCommand { command };
                    if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                        current_parent = node_id;
                    }
                }
                RawClaudeEvent::Stderr { text } => {
                    tracing::warn!(stderr = %text, "Claude stderr");
                    let event = NodeEvent::ClaudeStderr { text };
                    if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &event).await {
                        current_parent = node_id;
                    }
                }
            }
        }

        // 6. Store assistant response
        let model_id = format!("claude-code-{}", config.model.as_str());
        let assistant_msg = if is_ephemeral {
            match storage.message_create_ephemeral(
                &session_id,
                MessageRole::Assistant,
                response_content,
                Some(model_id),
                None,
                None,
                cost_usd,
            ).await {
                Ok(m) => m,
                Err(e) => {
                    if let Err(e2) = storage.stream_push_event(&stream_id, ChatEvent::Err { message: e.to_string() }).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to push error event to stream");
                    }
                    if let Err(e2) = storage.stream_set_status(&stream_id, StreamStatus::Failed, Some(e.to_string())).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to update stream status to Failed");
                    }
                    return;
                }
            }
        } else {
            match storage.message_create(
                &session_id,
                MessageRole::Assistant,
                response_content,
                Some(model_id),
                None,
                None,
                cost_usd,
            ).await {
                Ok(m) => m,
                Err(e) => {
                    if let Err(e2) = storage.stream_push_event(&stream_id, ChatEvent::Err { message: e.to_string() }).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to push error event to stream");
                    }
                    if let Err(e2) = storage.stream_set_status(&stream_id, StreamStatus::Failed, Some(e.to_string())).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to update stream status to Failed");
                    }
                    return;
                }
            }
        };

        // Create AssistantComplete event node (Milestone 2)
        let complete_event = NodeEvent::AssistantComplete { usage: None };
        if let Ok(node_id) = create_event_node(storage.arbor(), &config.head.tree_id, &current_parent, &complete_event).await {
            current_parent = node_id;
        }

        // 7. Create assistant node in Arbor
        let assistant_handle = ClaudeCodeStorage::message_to_handle(&assistant_msg, "assistant");
        let assistant_node_id = if is_ephemeral {
            match storage.arbor().node_create_external_ephemeral(
                &config.head.tree_id,
                Some(current_parent),
                assistant_handle,
                None,
            ).await {
                Ok(id) => id,
                Err(e) => {
                    if let Err(e2) = storage.stream_push_event(&stream_id, ChatEvent::Err { message: e.to_string() }).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to push error event to stream");
                    }
                    if let Err(e2) = storage.stream_set_status(&stream_id, StreamStatus::Failed, Some(e.to_string())).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to update stream status to Failed");
                    }
                    return;
                }
            }
        } else {
            match storage.arbor().node_create_external(
                &config.head.tree_id,
                Some(current_parent),
                assistant_handle,
                None,
            ).await {
                Ok(id) => id,
                Err(e) => {
                    if let Err(e2) = storage.stream_push_event(&stream_id, ChatEvent::Err { message: e.to_string() }).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to push error event to stream");
                    }
                    if let Err(e2) = storage.stream_set_status(&stream_id, StreamStatus::Failed, Some(e.to_string())).await {
                        tracing::error!(stream_id = %stream_id, error = %e2, "Failed to update stream status to Failed");
                    }
                    return;
                }
            }
        };

        let new_head = Position::new(config.head.tree_id, assistant_node_id);

        // 8. Update session head (skip for ephemeral)
        if !is_ephemeral {
            if let Err(e) = storage.session_update_head(&session_id, assistant_node_id, claude_session_id.clone()).await {
                if let Err(e2) = storage.stream_push_event(&stream_id, ChatEvent::Err { message: e.to_string() }).await {
                    tracing::error!(stream_id = %stream_id, error = %e2, "Failed to push error event to stream");
                }
                if let Err(e2) = storage.stream_set_status(&stream_id, StreamStatus::Failed, Some(e.to_string())).await {
                    tracing::error!(stream_id = %stream_id, error = %e2, "Failed to update stream status to Failed");
                }
                return;
            }
        }

        // 9. Push Complete event and mark stream as complete
        if let Err(e) = storage.stream_push_event(&stream_id, ChatEvent::Complete {
            new_head: if is_ephemeral { config.head } else { new_head },
            claude_session_id: claude_session_id.unwrap_or_default(),
            usage: Some(ChatUsage {
                input_tokens: None,
                output_tokens: None,
                cost_usd,
                num_turns,
            }),
        }).await {
            tracing::error!(stream_id = %stream_id, error = %e, "Failed to push event to stream");
        }

        if let Err(e) = storage.stream_set_status(&stream_id, StreamStatus::Complete, None).await {
            tracing::error!(stream_id = %stream_id, error = %e, "Failed to update stream status");
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SessionChild — typed per-session namespace (IR-18)
//
// Acts as the child activation returned by `ClaudeCode::session(id)`. Methods
// here delegate to the same storage / executor as the flat `ClaudeCode`
// methods; the ID is pinned on construction so callers don't pass it into
// every method.
// ═══════════════════════════════════════════════════════════════════════════

/// Per-session activation returned by the `claudecode.session` child gate.
///
/// Scoped to a single session by construction, so method arguments carry
/// only per-turn data (prompt, ephemeral flag, etc.). Cheap to clone — all
/// state is `Arc`/cheap-copy.
#[derive(Clone)]
pub struct SessionChild {
    session_id: ClaudeCodeId,
    storage: Arc<ClaudeCodeStorage>,
    executor: ClaudeCodeExecutor,
}

impl SessionChild {
    /// Construct a new `SessionChild` bound to `session_id`.
    ///
    /// Constructed by `ClaudeCode::session(id)` after confirming the session
    /// exists; callers outside the child gate should prefer that path so
    /// nonexistent IDs surface as `None`.
    pub const fn new(
        session_id: ClaudeCodeId,
        storage: Arc<ClaudeCodeStorage>,
        executor: ClaudeCodeExecutor,
    ) -> Self {
        Self { session_id, storage, executor }
    }

    /// The session this activation is bound to.
    ///
    /// Public accessor for `HandleEnum` integration — see IR-18. Mirrors the
    /// cone precedent (`ConeChild::cone_id`), which is reachable via
    /// `pub use` re-export on `activations::cone`. SUB-CLEAN-2 re-exported
    /// `SessionChild` for parity, so this accessor is now reachable
    /// from the public API.
    pub const fn session_id(&self) -> ClaudeCodeId {
        self.session_id
    }
}

#[plexus_macros::activation(
    namespace = "session",
    version = "1.0.0",
    description = "Per-session typed namespace for a claudecode session"
)]
impl SessionChild {
    /// Send a prompt to this session.
    ///
    /// Mirrors `ClaudeCode::chat(name, prompt, …)` but pins the session by
    /// its ID at construction. Delegates to the shared `chat_stream_for_config`
    /// helper so wire behavior is identical to the flat path.
    #[plexus_macros::method(streaming,
    params(
        prompt = "User message / prompt to send",
        ephemeral = "If true, creates nodes but doesn't advance head and marks for deletion",
        allowed_tools = "Optional list of tools to allow (e.g. [\"WebSearch\", \"Read\"])"
    ))]
    pub(super) async fn chat(
        &self,
        prompt: String,
        ephemeral: Option<bool>,
        allowed_tools: Option<Vec<String>>,
    ) -> impl Stream<Item = ChatEvent> + Send + 'static {
        let storage = self.storage.clone();
        let executor = self.executor.clone();
        let session_id = self.session_id;

        // Resolve by ID up-front so a missing session surfaces an error event
        // (matching the flat path's error shape).
        let resolve_result = storage.session_get(&session_id).await;

        stream! {
            let config = match resolve_result {
                Ok(c) => c,
                Err(e) => {
                    yield ChatEvent::Err { message: e.to_string() };
                    return;
                }
            };

            let mut inner = Box::pin(chat_stream_for_config(
                storage,
                executor,
                config,
                prompt,
                ephemeral,
                allowed_tools,
            ));
            while let Some(ev) = inner.next().await {
                yield ev;
            }
        }
    }

    /// Fetch this session's configuration.
    #[plexus_macros::method]
    pub(super) async fn get(&self) -> impl Stream<Item = GetResult> + Send + 'static {
        let storage = self.storage.clone();
        let session_id = self.session_id;

        stream! {
            match storage.session_get(&session_id).await {
                Ok(config) => yield GetResult::Ok { config },
                Err(e) => yield GetResult::Err { message: e.to_string() },
            }
        }
    }

    /// Delete this session.
    ///
    /// Note: session deletion is a lifecycle operation; after this call
    /// further method invocations on the same `SessionChild` will
    /// return `SessionNotFound`-style errors.
    #[plexus_macros::method]
    pub(super) async fn delete(&self) -> impl Stream<Item = DeleteResult> + Send + 'static {
        let storage = self.storage.clone();
        let session_id = self.session_id;

        stream! {
            match storage.session_delete(&session_id).await {
                Ok(()) => yield DeleteResult::Ok { id: session_id },
                Err(e) => yield DeleteResult::Err { message: e.to_string() },
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// IR-18 tests — schema role tag + ChildRouter lookup
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod ir18_tests {
    use super::*;
    use crate::activations::arbor::{ArborConfig, ArborStorage};
    use crate::activations::claudecode::storage::ClaudeCodeStorageConfig;
    use crate::activations::claudecode::types::Model;
    use crate::plexus::{Activation, ChildRouter, MethodRole};

    /// Spin up a `ClaudeCode` backed by temp-file storage for router tests.
    async fn setup_claudecode() -> (ClaudeCode<crate::plexus::NoParent>, std::path::PathBuf, std::path::PathBuf) {
        let temp_dir = std::env::temp_dir();
        let test_id = uuid::Uuid::new_v4();
        let arbor_path = temp_dir.join(format!("test_ir18_arbor_{test_id}.db"));
        let claudecode_path = temp_dir.join(format!("test_ir18_claudecode_{test_id}.db"));

        let arbor_config = ArborConfig {
            db_path: arbor_path.clone(),
            scheduled_deletion_window: 604_800,
            archive_window: 2_592_000,
            auto_cleanup: false,
            cleanup_interval: 3600,
        };
        let arbor = Arc::new(ArborStorage::new(arbor_config).await.unwrap());

        let storage = Arc::new(
            ClaudeCodeStorage::new(
                ClaudeCodeStorageConfig { db_path: claudecode_path.clone() },
                arbor,
            )
            .await
            .unwrap(),
        );

        (ClaudeCode::<crate::plexus::NoParent>::new(storage), arbor_path, claudecode_path)
    }

    // AC #3: `plugin_schema()` on ClaudeCode contains a `session` method tagged
    // `MethodRole::DynamicChild { list_method: Some("session_ids"), search_method: None }`.
    #[tokio::test]
    async fn claudecode_schema_has_session_dynamic_child_with_list() {
        let (claudecode, _a, _c) = setup_claudecode().await;
        let schema = claudecode.plugin_schema();

        let session_method = schema
            .methods
            .iter()
            .find(|m| m.name == "session")
            .expect("ClaudeCode schema must expose a `session` method");

        match &session_method.role {
            MethodRole::DynamicChild { list_method, search_method } => {
                assert_eq!(
                    list_method.as_deref(),
                    Some("session_ids"),
                    "`session` must opt into `list = \"session_ids\"`"
                );
                assert!(
                    search_method.is_none(),
                    "`session` should not declare a search method (got {search_method:?})"
                );
            }
            other => panic!("`session` must be DynamicChild, got {other:?}"),
        }
    }

    // AC #4 (Some): a valid session ID resolves through `get_child`.
    #[tokio::test]
    async fn claudecode_get_child_valid_session_returns_some() {
        let (claudecode, _a, _c) = setup_claudecode().await;

        // Create a real session so `session_exists` returns true.
        let config = claudecode
            .storage
            .session_create(
                "ir18-test".to_string(),
                std::env::temp_dir().to_string_lossy().to_string(),
                Model::Sonnet,
                None,
                None,
                false,
                None,
                None,
                None,
            )
            .await
            .expect("session_create must succeed");

        let child = claudecode.get_child(&config.id.to_string()).await;
        assert!(
            child.is_some(),
            "get_child(<valid session id>) must return Some(SessionChild)"
        );
    }

    // AC #4 (None): a bogus session ID (valid UUID but absent from storage)
    // returns None — distinguishes "no such session" from "malformed id".
    #[tokio::test]
    async fn claudecode_get_child_unknown_session_returns_none() {
        let (claudecode, _a, _c) = setup_claudecode().await;
        let bogus = uuid::Uuid::new_v4().to_string();
        let child = claudecode.get_child(&bogus).await;
        assert!(
            child.is_none(),
            "get_child on an unknown id must yield None"
        );
    }

    // AC #4 (None): a malformed ID (not a UUID) returns None without panicking.
    #[tokio::test]
    async fn claudecode_get_child_malformed_id_returns_none() {
        let (claudecode, _a, _c) = setup_claudecode().await;
        let child = claudecode.get_child("not-a-uuid").await;
        assert!(
            child.is_none(),
            "get_child on a malformed id must yield None (not panic)"
        );
    }
}
