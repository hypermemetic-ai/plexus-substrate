use super::methods::ConeIdentifier;
use super::storage::{ConeStorage, ConeStorageConfig};
use super::types::{
    ChatEvent, ChatUsage, ConeError, ConeId, CreateResult, DeleteResult, GetResult,
    ListResult, MessageRole, RegistryResult, ResolveResult, SetHeadResult,
};
use crate::activations::arbor::{Node, NodeId, NodeType};
use crate::activations::bash::Bash;
use async_stream::stream;
use cllient::{Message, ModelRegistry};
use futures::Stream;
use std::sync::Arc;

/// Cone activation - orchestrates LLM conversations with Arbor context
///
/// PLX-117: no longer generic over `P: HubContext`. Cone is a handle-resolution
/// *provider*, never a consumer — `Cone::parent()` and `has_parent()` had zero
/// callers anywhere in `src/` or `tests/` (PLX-106, re-verified by PLX-111), and
/// `resolve_handle_impl` below touches nothing but its own storage.
#[derive(Clone)]
pub struct Cone {
    storage: Arc<ConeStorage>,
    llm_registry: Arc<ModelRegistry>,
}

impl Cone {
    /// Create a new Cone
    pub async fn new(
        config: ConeStorageConfig,
        arbor: Arc<crate::activations::arbor::ArborStorage>,
    ) -> Result<Self, String> {
        let storage = ConeStorage::new(config, arbor)
            .await
            .map_err(|e| format!("Failed to initialize cone storage: {e}"))?;

        let llm_registry = ModelRegistry::new()
            .map_err(|e| format!("Failed to initialize LLM registry: {e}"))?;

        Ok(Self {
            storage: Arc::new(storage),
            llm_registry: Arc::new(llm_registry),
        })
    }

    /// Get access to the underlying storage
    ///
    /// Useful for testing and direct storage operations.
    pub const fn storage(&self) -> &Arc<ConeStorage> {
        &self.storage
    }

    /// Register default templates with the mustache plugin
    ///
    /// Call this during initialization to register Cone's default templates
    /// for rendering resolved messages and events.
    pub async fn register_default_templates(
        &self,
        mustache: &crate::activations::mustache::Mustache,
    ) -> Result<(), String> {
        let plugin_id = Self::PLUGIN_ID;

        mustache.register_templates(plugin_id, &[
            // Chat method - resolved message template
            ("chat", "default", "[{{role}}] {{#name}}({{name}}) {{/name}}{{content}}"),
            ("chat", "markdown", "**{{role}}**{{#name}} ({{name}}){{/name}}\n\n{{content}}"),
            ("chat", "json", r#"{"role":"{{role}}","content":"{{content}}","name":"{{name}}"}"#),

            // Create method - cone created event
            ("create", "default", "Cone created: {{cone_id}} (head: {{head.tree_id}}/{{head.node_id}})"),

            // List method - cone list event
            ("list", "default", "{{#cones}}{{name}} ({{id}}) - {{model_id}}\n{{/cones}}"),
        ]).await
    }
}

impl Cone {
    /// Resolve a cone handle to its message content
    ///
    /// Called by the macro-generated `resolve_handle` method.
    /// Handle format: `cone@1.0.0::chat:msg-{uuid}:{role}:{name`}
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
                "Cone handle missing message ID in meta".to_string()
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

        Ok(wrap_stream(result_stream, "cone.resolve_handle", vec!["cone".into()]))
    }
}

#[plexus_macros::activation(namespace = "cone",
version = "1.0.0",
description = "LLM cone with persistent conversation context",
resolve_handle)]
impl Cone {
    /// Create a new cone (LLM agent with persistent conversation context)
    #[plexus_macros::method(params(
        name = "Human-readable name for the cone",
        model_id = "LLM model ID (e.g., 'gpt-4o-mini', 'claude-3-haiku-20240307')",
        system_prompt = "Optional system prompt / instructions",
        metadata = "Optional configuration metadata"
    ))]
    async fn create(
        &self,
        name: String,
        model_id: String,
        system_prompt: Option<String>,
        metadata: Option<serde_json::Value>,
    ) -> Result<CreateResult, ConeError> {
        // Validate model exists before creating cone
        if let Err(e) = self.llm_registry.from_id(&model_id) {
            return Err(ConeError::InvalidState {
                message: format!("Invalid model_id '{model_id}': {e}"),
            });
        }

        let cone = self
            .storage
            .cone_create(name, model_id, system_prompt, metadata)
            .await?;
        Ok(CreateResult::Created {
            cone_id: cone.id,
            head: cone.head,
        })
    }

    /// Get cone configuration by name or ID
    #[plexus_macros::method(params(identifier = "Cone name or UUID (e.g., 'my-assistant' or '550e8400-e29b-...')"))]
    async fn get(
        &self,
        identifier: ConeIdentifier,
    ) -> Result<GetResult, ConeError> {
        let cone_id = self.storage.resolve_cone_identifier(&identifier).await?;
        let cone = self.storage.cone_get(&cone_id).await?;
        Ok(GetResult::Data { cone })
    }

    /// List all cones
    #[plexus_macros::method]
    async fn list(&self) -> Result<ListResult, ConeError> {
        let cones = self.storage.cone_list().await?;
        Ok(ListResult::List { cones })
    }

    /// Delete a cone (associated tree is preserved)
    #[plexus_macros::method(params(identifier = "Cone name or UUID (e.g., 'my-assistant' or '550e8400-e29b-...')"))]
    async fn delete(
        &self,
        identifier: ConeIdentifier,
    ) -> Result<DeleteResult, ConeError> {
        let cone_id = self.storage.resolve_cone_identifier(&identifier).await?;
        self.storage.cone_delete(&cone_id).await?;
        Ok(DeleteResult::Deleted { cone_id })
    }

    /// Chat with a cone - appends prompt to context, calls LLM, advances head
    #[plexus_macros::method(streaming,
    params(
        identifier = "Cone name or UUID (e.g., 'my-assistant' or '550e8400-e29b-...')",
        prompt = "User message / prompt to send to the LLM",
        ephemeral = "If true, creates nodes but doesn't advance head and marks for deletion"
    ))]
    async fn chat(
        &self,
        identifier: ConeIdentifier,
        prompt: String,
        ephemeral: Option<bool>,
    ) -> impl Stream<Item = ChatEvent> + Send + 'static {
        let storage = self.storage.clone();
        let llm_registry = self.llm_registry.clone();

        stream! {
            let is_ephemeral = ephemeral.unwrap_or(false);

            // Resolve identifier to ConeId
            let cone_id = match storage.resolve_cone_identifier(&identifier).await {
                Ok(id) => id,
                Err(e) => {
                    yield ChatEvent::Error { message: e.to_string() };
                    return;
                }
            };

            // 1. Load cone config
            let cone = match storage.cone_get(&cone_id).await {
                Ok(a) => a,
                Err(e) => {
                    yield ChatEvent::Error { message: format!("Failed to get cone: {e}") };
                    return;
                }
            };

            // 2. Build context from arbor path (handles only)
            let context_nodes = match storage.arbor().context_get_path(&cone.head.tree_id, &cone.head.node_id).await {
                Ok(nodes) => nodes,
                Err(e) => {
                    yield ChatEvent::Error { message: format!("Failed to get context path: {e}") };
                    return;
                }
            };

            // Resolve handles to messages
            let messages = match resolve_context_to_messages(&storage, &context_nodes, &cone.system_prompt).await {
                Ok(msgs) => msgs,
                Err(e) => {
                    yield ChatEvent::Error { message: format!("Failed to resolve context: {e}") };
                    return;
                }
            };

            // 3. Store user message in cone database (ephemeral if requested)
            let user_message = if is_ephemeral {
                match storage.message_create_ephemeral(
                    &cone_id,
                    MessageRole::User,
                    prompt.clone(),
                    None,
                    None,
                    None,
                ).await {
                    Ok(msg) => msg,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to store user message: {e}") };
                        return;
                    }
                }
            } else {
                match storage.message_create(
                    &cone_id,
                    MessageRole::User,
                    prompt.clone(),
                    None,
                    None,
                    None,
                ).await {
                    Ok(msg) => msg,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to store user message: {e}") };
                        return;
                    }
                }
            };

            // Create external node with handle pointing to user message (ephemeral if requested)
            let user_handle = ConeStorage::message_to_handle(&user_message, "user");
            let user_node_id = if is_ephemeral {
                match storage.arbor().node_create_external_ephemeral(
                    &cone.head.tree_id,
                    Some(cone.head.node_id),
                    user_handle,
                    None,
                ).await {
                    Ok(id) => id,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to create user node: {e}") };
                        return;
                    }
                }
            } else {
                match storage.arbor().node_create_external(
                    &cone.head.tree_id,
                    Some(cone.head.node_id),
                    user_handle,
                    None,
                ).await {
                    Ok(id) => id,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to create user node: {e}") };
                        return;
                    }
                }
            };

            let user_position = cone.head.advance(user_node_id);

            // Signal chat start
            yield ChatEvent::Start {
                cone_id,
                user_position,
            };

            // 4. Build LLM request with resolved messages + new user prompt
            let mut llm_messages = messages;
            llm_messages.push(Message::user(&prompt));

            let request_builder = match llm_registry.from_id(&cone.model_id) {
                Ok(rb) => rb,
                Err(e) => {
                    yield ChatEvent::Error { message: format!("Failed to create request builder: {e}") };
                    return;
                }
            };

            let mut builder = request_builder;
            if let Some(ref sys) = cone.system_prompt {
                builder = builder.system(sys);
            }
            builder = builder.messages(llm_messages);

            // Stream the response
            let mut stream_result = match builder.stream().await {
                Ok(s) => s,
                Err(e) => {
                    yield ChatEvent::Error { message: format!("Failed to start LLM stream: {e}") };
                    return;
                }
            };

            let mut full_response = String::new();
            let mut input_tokens: Option<i64> = None;
            let mut output_tokens: Option<i64> = None;

            use futures::StreamExt;
            while let Some(event) = stream_result.next().await {
                match event {
                    Ok(cllient::streaming::StreamEvent::Content(text)) => {
                        full_response.push_str(&text);
                        yield ChatEvent::Content {
                            cone_id,
                            content: text,
                        };
                    }
                    Ok(cllient::streaming::StreamEvent::Usage { input_tokens: inp, output_tokens: out, .. }) => {
                        input_tokens = inp.map(i64::from);
                        output_tokens = out.map(i64::from);
                    }
                    Ok(cllient::streaming::StreamEvent::Error(e)) => {
                        yield ChatEvent::Error { message: format!("LLM error: {e}") };
                        return;
                    }
                    Ok(_) => {
                        // Ignore other events (Start, Finish, Role, Raw)
                    }
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Stream error: {e}") };
                        return;
                    }
                }
            }

            // 5. Store assistant response in cone database (ephemeral if requested)
            let assistant_message = if is_ephemeral {
                match storage.message_create_ephemeral(
                    &cone_id,
                    MessageRole::Assistant,
                    full_response,
                    Some(cone.model_id.clone()),
                    input_tokens,
                    output_tokens,
                ).await {
                    Ok(msg) => msg,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to store assistant message: {e}") };
                        return;
                    }
                }
            } else {
                match storage.message_create(
                    &cone_id,
                    MessageRole::Assistant,
                    full_response,
                    Some(cone.model_id.clone()),
                    input_tokens,
                    output_tokens,
                ).await {
                    Ok(msg) => msg,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to store assistant message: {e}") };
                        return;
                    }
                }
            };

            // Create external node with handle pointing to assistant message (ephemeral if requested)
            let assistant_handle = ConeStorage::message_to_handle(&assistant_message, &cone.name);
            let response_node_id = if is_ephemeral {
                match storage.arbor().node_create_external_ephemeral(
                    &cone.head.tree_id,
                    Some(user_node_id),
                    assistant_handle,
                    None,
                ).await {
                    Ok(id) => id,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to create response node: {e}") };
                        return;
                    }
                }
            } else {
                match storage.arbor().node_create_external(
                    &cone.head.tree_id,
                    Some(user_node_id),
                    assistant_handle,
                    None,
                ).await {
                    Ok(id) => id,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to create response node: {e}") };
                        return;
                    }
                }
            };

            let new_head = user_position.advance(response_node_id);

            // 6. Update canonical_head (skip for ephemeral)
            if !is_ephemeral {
                if let Err(e) = storage.cone_update_head(&cone_id, response_node_id).await {
                    yield ChatEvent::Error { message: format!("Failed to update head: {e}") };
                    return;
                }
            }

            let usage_info = if input_tokens.is_some() || output_tokens.is_some() {
                Some(ChatUsage {
                    input_tokens: input_tokens.map(|t| t as u64),
                    output_tokens: output_tokens.map(|t| t as u64),
                    total_tokens: input_tokens.and_then(|i| output_tokens.map(|o| (i + o) as u64)),
                })
            } else {
                None
            };

            // For ephemeral, return original head (not the ephemeral node)
            yield ChatEvent::Complete {
                cone_id,
                new_head: if is_ephemeral { cone.head } else { new_head },
                usage: usage_info,
            };
        }
    }

    /// Move cone's canonical head to a different node in the tree
    #[plexus_macros::method(params(
        identifier = "Cone name or UUID (e.g., 'my-assistant' or '550e8400-e29b-...')",
        node_id = "UUID of the target node to set as the new head"
    ))]
    async fn set_head(
        &self,
        identifier: ConeIdentifier,
        node_id: NodeId,
    ) -> Result<SetHeadResult, ConeError> {
        let cone_id = self.storage.resolve_cone_identifier(&identifier).await?;

        // Get current head first
        let old_head = self.storage.cone_get(&cone_id).await?.head;

        // Advance to new node in same tree
        let new_head = old_head.advance(node_id);

        self.storage.cone_update_head(&cone_id, node_id).await?;
        Ok(SetHeadResult::Updated {
            cone_id,
            old_head,
            new_head,
        })
    }

    /// Get available LLM services and models
    #[plexus_macros::method]
    async fn registry(&self) -> Result<RegistryResult, ConeError> {
        Ok(RegistryResult::Registry(self.llm_registry.export()))
    }

    /// Look up a specific cone by identifier (name or UUID) and return it as a
    /// nested typed activation.
    ///
    /// Enables the nested-namespace syntax: `cone.of abc123.chat "…"` routes
    /// through this gate, then dispatches `chat` on the returned
    /// [`ConeActivation`]. Flat methods on `Cone` remain the primary API;
    /// this gate is additive (IR-19).
    ///
    /// The sibling `cone_ids` method supplies completions for
    /// `ChildRouter::list_children`.
    #[plexus_macros::child(list = "cone_ids")]
    async fn of(&self, id: &str) -> Option<ConeActivation> {
        // Accept either UUID or (partial) name — mirrors the flat methods'
        // `ConeIdentifier` resolution so the child gate is as forgiving as
        // `cone.chat(identifier=..)`.
        let identifier = match uuid::Uuid::parse_str(id) {
            Ok(uuid) => ConeIdentifier::ById { id: uuid },
            Err(_) => ConeIdentifier::ByName { name: id.to_string() },
        };

        let cone_id = self
            .storage
            .resolve_cone_identifier(&identifier)
            .await
            .ok()?;

        // `resolve_cone_identifier` accepts any syntactically valid UUID in
        // the `ById` branch without consulting the DB; probe `cone_get` here
        // so `get_child("<unknown-uuid>")` returns `None` rather than a
        // `ConeActivation` bound to a nonexistent cone.
        self.storage.cone_get(&cone_id).await.ok()?;

        Some(ConeActivation::new(
            cone_id,
            self.storage.clone(),
            self.llm_registry.clone(),
        ))
    }

    /// Stream cone identifiers (UUIDs) for `ChildRouter::list_children`.
    ///
    /// Used by the `of` child gate's `list = "cone_ids"` opt-in to advertise
    /// the set of addressable cones for tab-completion in generated clients.
    async fn cone_ids(&self) -> impl Stream<Item = String> + Send + '_ {
        let storage = self.storage.clone();
        stream! {
            match storage.cone_list().await {
                Ok(cones) => {
                    for cone in cones {
                        yield cone.id.to_string();
                    }
                }
                Err(e) => {
                    tracing::warn!("cone_ids: failed to list cones: {}", e);
                }
            }
        }
    }
}

// ============================================================================
// ConeActivation — per-cone typed namespace (IR-19)
// ============================================================================

/// Per-cone typed activation exposed through `Cone::of(id)`.
///
/// Wraps a single cone's identity + shared storage/LLM-registry handles so
/// nested calls like `cone.of {id}.chat(...)` can dispatch without re-passing
/// the cone identifier. The flat methods on [`Cone`] remain the source of
/// truth for creation/listing/deletion-at-root; `ConeActivation` is an
/// additive, per-instance view (IR-19).
#[derive(Clone)]
pub struct ConeActivation {
    cone_id: ConeId,
    storage: Arc<ConeStorage>,
    llm_registry: Arc<ModelRegistry>,
}

impl ConeActivation {
    /// Construct a new per-cone activation.
    ///
    /// Callers should only produce this through [`Cone::of`], which performs
    /// the identifier-to-`ConeId` resolution.
    pub const fn new(
        cone_id: ConeId,
        storage: Arc<ConeStorage>,
        llm_registry: Arc<ModelRegistry>,
    ) -> Self {
        Self {
            cone_id,
            storage,
            llm_registry,
        }
    }

    /// The underlying cone identifier this activation is bound to.
    pub const fn cone_id(&self) -> ConeId {
        self.cone_id
    }
}

#[plexus_macros::activation(namespace = "cone", version = "1.0.0",
    description = "Typed view of a single cone — IR-19 dynamic child gate")]
impl ConeActivation {
    /// Return this cone's configuration.
    #[plexus_macros::method]
    async fn get(&self) -> Result<GetResult, ConeError> {
        let cone = self.storage.cone_get(&self.cone_id).await?;
        Ok(GetResult::Data { cone })
    }

    /// Delete this cone. The associated conversation tree is preserved.
    #[plexus_macros::method]
    async fn delete(&self) -> Result<DeleteResult, ConeError> {
        self.storage.cone_delete(&self.cone_id).await?;
        Ok(DeleteResult::Deleted {
            cone_id: self.cone_id,
        })
    }

    /// Move this cone's canonical head to a different node in its tree.
    #[plexus_macros::method(params(
        node_id = "UUID of the target node to set as the new head"
    ))]
    async fn set_head(
        &self,
        node_id: NodeId,
    ) -> Result<SetHeadResult, ConeError> {
        let cone_id = self.cone_id;
        let old_head = self.storage.cone_get(&cone_id).await?.head;
        let new_head = old_head.advance(node_id);

        self.storage.cone_update_head(&cone_id, node_id).await?;
        Ok(SetHeadResult::Updated {
            cone_id,
            old_head,
            new_head,
        })
    }

    /// Chat with this cone — appends prompt to context, calls LLM, advances head.
    ///
    /// Per-cone mirror of `Cone::chat(identifier, prompt, ephemeral)` without
    /// the identifier parameter (the cone is fixed by the child gate).
    #[plexus_macros::method(streaming,
    params(
        prompt = "User message / prompt to send to the LLM",
        ephemeral = "If true, creates nodes but doesn't advance head and marks for deletion"
    ))]
    async fn chat(
        &self,
        prompt: String,
        ephemeral: Option<bool>,
    ) -> impl Stream<Item = ChatEvent> + Send + 'static {
        let storage = self.storage.clone();
        let llm_registry = self.llm_registry.clone();
        let cone_id = self.cone_id;

        stream! {
            let is_ephemeral = ephemeral.unwrap_or(false);

            // 1. Load cone config
            let cone = match storage.cone_get(&cone_id).await {
                Ok(a) => a,
                Err(e) => {
                    yield ChatEvent::Error { message: format!("Failed to get cone: {e}") };
                    return;
                }
            };

            // 2. Build context from arbor path (handles only)
            let context_nodes = match storage.arbor().context_get_path(&cone.head.tree_id, &cone.head.node_id).await {
                Ok(nodes) => nodes,
                Err(e) => {
                    yield ChatEvent::Error { message: format!("Failed to get context path: {e}") };
                    return;
                }
            };

            // Resolve handles to messages
            let messages = match resolve_context_to_messages(&storage, &context_nodes, &cone.system_prompt).await {
                Ok(msgs) => msgs,
                Err(e) => {
                    yield ChatEvent::Error { message: format!("Failed to resolve context: {e}") };
                    return;
                }
            };

            // 3. Store user message in cone database (ephemeral if requested)
            let user_message = if is_ephemeral {
                match storage.message_create_ephemeral(
                    &cone_id,
                    MessageRole::User,
                    prompt.clone(),
                    None,
                    None,
                    None,
                ).await {
                    Ok(msg) => msg,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to store user message: {e}") };
                        return;
                    }
                }
            } else {
                match storage.message_create(
                    &cone_id,
                    MessageRole::User,
                    prompt.clone(),
                    None,
                    None,
                    None,
                ).await {
                    Ok(msg) => msg,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to store user message: {e}") };
                        return;
                    }
                }
            };

            // Create external node with handle pointing to user message
            let user_handle = ConeStorage::message_to_handle(&user_message, "user");
            let user_node_id = if is_ephemeral {
                match storage.arbor().node_create_external_ephemeral(
                    &cone.head.tree_id,
                    Some(cone.head.node_id),
                    user_handle,
                    None,
                ).await {
                    Ok(id) => id,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to create user node: {e}") };
                        return;
                    }
                }
            } else {
                match storage.arbor().node_create_external(
                    &cone.head.tree_id,
                    Some(cone.head.node_id),
                    user_handle,
                    None,
                ).await {
                    Ok(id) => id,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to create user node: {e}") };
                        return;
                    }
                }
            };

            let user_position = cone.head.advance(user_node_id);

            yield ChatEvent::Start {
                cone_id,
                user_position,
            };

            // 4. Build LLM request with resolved messages + new user prompt
            let mut llm_messages = messages;
            llm_messages.push(Message::user(&prompt));

            let request_builder = match llm_registry.from_id(&cone.model_id) {
                Ok(rb) => rb,
                Err(e) => {
                    yield ChatEvent::Error { message: format!("Failed to create request builder: {e}") };
                    return;
                }
            };

            let mut builder = request_builder;
            if let Some(ref sys) = cone.system_prompt {
                builder = builder.system(sys);
            }
            builder = builder.messages(llm_messages);

            let mut stream_result = match builder.stream().await {
                Ok(s) => s,
                Err(e) => {
                    yield ChatEvent::Error { message: format!("Failed to start LLM stream: {e}") };
                    return;
                }
            };

            let mut full_response = String::new();
            let mut input_tokens: Option<i64> = None;
            let mut output_tokens: Option<i64> = None;

            use futures::StreamExt;
            while let Some(event) = stream_result.next().await {
                match event {
                    Ok(cllient::streaming::StreamEvent::Content(text)) => {
                        full_response.push_str(&text);
                        yield ChatEvent::Content {
                            cone_id,
                            content: text,
                        };
                    }
                    Ok(cllient::streaming::StreamEvent::Usage { input_tokens: inp, output_tokens: out, .. }) => {
                        input_tokens = inp.map(i64::from);
                        output_tokens = out.map(i64::from);
                    }
                    Ok(cllient::streaming::StreamEvent::Error(e)) => {
                        yield ChatEvent::Error { message: format!("LLM error: {e}") };
                        return;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Stream error: {e}") };
                        return;
                    }
                }
            }

            // 5. Store assistant response
            let assistant_message = if is_ephemeral {
                match storage.message_create_ephemeral(
                    &cone_id,
                    MessageRole::Assistant,
                    full_response,
                    Some(cone.model_id.clone()),
                    input_tokens,
                    output_tokens,
                ).await {
                    Ok(msg) => msg,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to store assistant message: {e}") };
                        return;
                    }
                }
            } else {
                match storage.message_create(
                    &cone_id,
                    MessageRole::Assistant,
                    full_response,
                    Some(cone.model_id.clone()),
                    input_tokens,
                    output_tokens,
                ).await {
                    Ok(msg) => msg,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to store assistant message: {e}") };
                        return;
                    }
                }
            };

            let assistant_handle = ConeStorage::message_to_handle(&assistant_message, &cone.name);
            let response_node_id = if is_ephemeral {
                match storage.arbor().node_create_external_ephemeral(
                    &cone.head.tree_id,
                    Some(user_node_id),
                    assistant_handle,
                    None,
                ).await {
                    Ok(id) => id,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to create response node: {e}") };
                        return;
                    }
                }
            } else {
                match storage.arbor().node_create_external(
                    &cone.head.tree_id,
                    Some(user_node_id),
                    assistant_handle,
                    None,
                ).await {
                    Ok(id) => id,
                    Err(e) => {
                        yield ChatEvent::Error { message: format!("Failed to create response node: {e}") };
                        return;
                    }
                }
            };

            let new_head = user_position.advance(response_node_id);

            if !is_ephemeral {
                if let Err(e) = storage.cone_update_head(&cone_id, response_node_id).await {
                    yield ChatEvent::Error { message: format!("Failed to update head: {e}") };
                    return;
                }
            }

            let usage_info = if input_tokens.is_some() || output_tokens.is_some() {
                Some(ChatUsage {
                    input_tokens: input_tokens.map(|t| t as u64),
                    output_tokens: output_tokens.map(|t| t as u64),
                    total_tokens: input_tokens.and_then(|i| output_tokens.map(|o| (i + o) as u64)),
                })
            } else {
                None
            };

            yield ChatEvent::Complete {
                cone_id,
                new_head: if is_ephemeral { cone.head } else { new_head },
                usage: usage_info,
            };
        }
    }
}

/// Resolve arbor context path to cllient messages by resolving handles
async fn resolve_context_to_messages(
    storage: &ConeStorage,
    nodes: &[Node],
    _system_prompt: &Option<String>,
) -> Result<Vec<Message>, String> {
    let mut messages = Vec::new();

    for node in nodes {
        match &node.data {
            NodeType::Text { content } => {
                // Text nodes shouldn't exist in the new design, but handle gracefully
                // Skip empty root nodes
                if !content.is_empty() {
                    messages.push(Message::user(content));
                }
            }
            NodeType::External { handle } => {
                // Resolve handle based on plugin_id
                if handle.plugin_id == Cone::PLUGIN_ID {
                    // Resolve cone message handle - format: "msg-{uuid}:{role}:{name}"
                    let identifier = handle.meta.join(":");
                    let msg = storage
                        .resolve_message_handle(&identifier)
                        .await
                        .map_err(|e| format!("Failed to resolve message handle: {e}"))?;

                    let cllient_msg = match msg.role {
                        MessageRole::User => Message::user(&msg.content),
                        MessageRole::Assistant => Message::assistant(&msg.content),
                        MessageRole::System => Message::system(&msg.content),
                    };
                    messages.push(cllient_msg);
                } else if handle.plugin_id == Bash::PLUGIN_ID {
                    // TODO: Resolve bash output when bash plugin integration is added
                    let cmd_id = handle.meta.first().map_or("unknown", std::string::String::as_str);
                    messages.push(Message::user(&format!(
                        "[Tool output from bash: {cmd_id}]"
                    )));
                } else {
                    // Unknown handle plugin - include as reference using Display
                    messages.push(Message::user(&format!(
                        "[External reference: {handle}]"
                    )));
                }
            }
        }
    }

    Ok(messages)
}
