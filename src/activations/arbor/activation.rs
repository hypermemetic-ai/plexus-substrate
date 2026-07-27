use super::storage::{ArborConfig, ArborStorage};
use super::types::{ArborError, ArborEvent, Handle, NodeId, TreeId, TreeSkeleton};
use crate::plexus::{Activation, PlexusError, PlexusStream, PlexusStreamItem};
use futures::StreamExt;
use futures::future::BoxFuture;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

// ============================================================================
// HandleResolvers — the data that replaced the parent hub handle (PLX-111)
// ============================================================================

/// One activation's ability to resolve a [`Handle`] it owns.
///
/// Shaped identically to `Activation::resolve_handle`, because that is exactly
/// what it wraps: `DynamicHub::do_resolve_handle` looked a `plugin_id` up in a
/// registry and called that method. The registry lookup is the map below; this
/// is the value it stored.
pub type HandleResolver =
    Arc<dyn Fn(&Handle) -> BoxFuture<'static, Result<PlexusStream, PlexusError>> + Send + Sync>;

/// A `plugin_id -> resolver` map: the whole of what Arbor's parent handle was.
///
/// PLX-111 measured it. `Arbor::tree_render` was the ONE real reader of the
/// injected `HubContext` in all of substrate, it asked for exactly one thing —
/// `resolve_handle` on the `Handle` in each `NodeType::External` node — and
/// `DynamicHub::do_resolve_handle` routed that on `handle.plugin_id` **alone**.
/// So the parent handle was a `Uuid -> resolver` lookup table wearing a
/// capability's clothes, and the replacement is data, not a runtime feature.
///
/// This is also why the substitution is tenant-safe for M4 (PLX-96): a resolver
/// minted by the mount layer closes over that tenant's own storages, so there is
/// no edge to gate. An upward-dispatch capability would have routed a
/// caller-supplied `plugin_id` (`arbor.node_create_external` is a public RPC
/// method) through a single global registry — a cross-tenant read.
///
/// **Known limit, measured as zero today**: a provider registered *after*
/// composition will not resolve, because the map is built once at the
/// composition root. Nothing in substrate registers a handle provider at
/// runtime; the only register chain is `builder.rs`.
#[derive(Clone, Default)]
pub struct HandleResolvers {
    by_plugin_id: HashMap<Uuid, HandleResolver>,
}

impl HandleResolvers {
    /// An empty map — the `NoParent` case. `tree_render` degrades to
    /// `tree.render()`, exactly as it did with no parent injected.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an activation as the resolver for its own `plugin_id`.
    ///
    /// The key is `Activation::plugin_id()`, which is the same value the
    /// `#[activation]` macro emits as `PLUGIN_ID` and the same value a `Handle`
    /// carries — the macro derives both as `uuid_v5(NAMESPACE_OID,
    /// "{namespace}@{major}")`.
    pub fn register<A: Activation>(&mut self, activation: Arc<A>) {
        let plugin_id = activation.plugin_id();
        self.by_plugin_id.insert(
            plugin_id,
            Arc::new(move |handle: &Handle| {
                let activation = activation.clone();
                let handle = handle.clone();
                Box::pin(async move { activation.resolve_handle(&handle).await })
            }),
        );
    }

    /// Builder form of [`register`](Self::register).
    #[must_use]
    pub fn with<A: Activation>(mut self, activation: Arc<A>) -> Self {
        self.register(activation);
        self
    }

    /// True when nothing can resolve — the old "no parent injected" state.
    pub fn is_empty(&self) -> bool {
        self.by_plugin_id.is_empty()
    }

    /// Resolve a handle, or report the same miss the hub's registry reported.
    ///
    /// A miss yields `PlexusError::ActivationNotFound(plugin_id)`, byte-for-byte
    /// what `DynamicHub::do_resolve_handle`'s `ok_or_else` produced, so
    /// `tree_render`'s `[unresolved: …]` string is unchanged.
    pub async fn resolve(&self, handle: &Handle) -> Result<PlexusStream, PlexusError> {
        match self.by_plugin_id.get(&handle.plugin_id) {
            Some(resolver) => resolver(handle).await,
            None => Err(PlexusError::ActivationNotFound(
                handle.plugin_id.to_string(),
            )),
        }
    }
}

/// Arbor activation - manages conversation trees
///
/// PLX-117: no longer generic over `P: HubContext`. Handle resolution arrives as
/// data ([`HandleResolvers`]) rather than as an injected parent, so the
/// `OnceLock`, the `PhantomData` and `Arc::new_cyclic` are all gone.
#[derive(Clone)]
pub struct Arbor {
    storage: Arc<ArborStorage>,
    /// Resolvers for handles found in `NodeType::External` nodes while rendering.
    resolvers: HandleResolvers,
}

impl Arbor {
    /// Create a new Arbor activation with its own storage
    pub async fn new(config: ArborConfig) -> Result<Self, String> {
        let storage = ArborStorage::new(config)
            .await
            .map_err(|e| format!("Failed to initialize Arbor storage: {e}"))?;

        Ok(Self {
            storage: Arc::new(storage),
            resolvers: HandleResolvers::new(),
        })
    }

    /// Create an Arbor activation with a shared storage instance
    pub fn with_storage(storage: Arc<ArborStorage>) -> Self {
        Self {
            storage,
            resolvers: HandleResolvers::new(),
        }
    }

    /// Get the underlying storage (for sharing with other activations)
    pub fn storage(&self) -> Arc<ArborStorage> {
        self.storage.clone()
    }

    /// Attach the handle resolvers, at the composition root.
    ///
    /// Consuming rather than interior-mutable on purpose: the map is fixed once,
    /// which is the property that makes the "registered after composition"
    /// caveat above a static fact rather than a race.
    #[must_use]
    pub fn with_resolvers(mut self, resolvers: HandleResolvers) -> Self {
        self.resolvers = resolvers;
        self
    }
}

#[plexus_macros::activation(namespace = "arbor",
version = "1.0.0",
description = "Manage conversation trees with context tracking")]
impl Arbor {
    /// Create a new conversation tree
    #[plexus_macros::method(params(
        metadata = "Optional tree-level metadata (name, description, etc.)",
        owner_id = "Owner identifier (default: 'system')"
    ))]
    async fn tree_create(
        &self,
        metadata: Option<Value>,
        owner_id: String,
    ) -> Result<ArborEvent, ArborError> {
        let tree_id = self.storage.tree_create(metadata, &owner_id).await?;
        Ok(ArborEvent::TreeCreated { tree_id })
    }

    /// Retrieve a complete tree with all nodes
    #[plexus_macros::method(params(tree_id = "UUID of the tree to retrieve"))]
    async fn tree_get(&self, tree_id: TreeId) -> Result<ArborEvent, ArborError> {
        let tree = self.storage.tree_get(&tree_id).await?;
        Ok(ArborEvent::TreeData { tree })
    }

    /// Get lightweight tree structure without node data
    #[plexus_macros::method(params(tree_id = "UUID of the tree to retrieve"))]
    async fn tree_get_skeleton(&self, tree_id: TreeId) -> Result<ArborEvent, ArborError> {
        let tree = self.storage.tree_get(&tree_id).await?;
        Ok(ArborEvent::TreeSkeleton {
            skeleton: TreeSkeleton::from(&tree),
        })
    }

    /// List all active trees
    #[plexus_macros::method]
    async fn tree_list(&self) -> Result<ArborEvent, ArborError> {
        let tree_ids = self.storage.tree_list(false).await?;
        Ok(ArborEvent::TreeList { tree_ids })
    }

    /// Update tree metadata
    #[plexus_macros::method(params(
        tree_id = "UUID of the tree to update",
        metadata = "New metadata to set"
    ))]
    async fn tree_update_metadata(
        &self,
        tree_id: TreeId,
        metadata: Value,
    ) -> Result<ArborEvent, ArborError> {
        self.storage.tree_update_metadata(&tree_id, metadata).await?;
        Ok(ArborEvent::TreeUpdated { tree_id })
    }

    /// Claim ownership of a tree (increment reference count)
    #[plexus_macros::method(params(
        tree_id = "UUID of the tree to claim",
        owner_id = "Owner identifier",
        count = "Number of references to add (default: 1)"
    ))]
    async fn tree_claim(
        &self,
        tree_id: TreeId,
        owner_id: String,
        count: i64,
    ) -> Result<ArborEvent, ArborError> {
        let new_count = self.storage.tree_claim(&tree_id, &owner_id, count).await?;
        Ok(ArborEvent::TreeClaimed {
            tree_id,
            owner_id,
            new_count,
        })
    }

    /// Release ownership of a tree (decrement reference count)
    #[plexus_macros::method(params(
        tree_id = "UUID of the tree to release",
        owner_id = "Owner identifier",
        count = "Number of references to remove (default: 1)"
    ))]
    async fn tree_release(
        &self,
        tree_id: TreeId,
        owner_id: String,
        count: i64,
    ) -> Result<ArborEvent, ArborError> {
        let new_count = self.storage.tree_release(&tree_id, &owner_id, count).await?;
        Ok(ArborEvent::TreeReleased {
            tree_id,
            owner_id,
            new_count,
        })
    }

    /// List trees scheduled for deletion
    #[plexus_macros::method]
    async fn tree_list_scheduled(&self) -> Result<ArborEvent, ArborError> {
        let tree_ids = self.storage.tree_list(true).await?;
        Ok(ArborEvent::TreesScheduled { tree_ids })
    }

    /// List archived trees
    #[plexus_macros::method]
    async fn tree_list_archived(&self) -> Result<ArborEvent, ArborError> {
        let tree_ids = self.storage.tree_list(true).await?;
        Ok(ArborEvent::TreesArchived { tree_ids })
    }

    /// Create a text node in a tree
    #[plexus_macros::method(params(
        tree_id = "UUID of the tree",
        parent = "Parent node ID (None for root-level)",
        content = "Text content for the node",
        metadata = "Optional node metadata"
    ))]
    async fn node_create_text(
        &self,
        tree_id: TreeId,
        parent: Option<NodeId>,
        content: String,
        metadata: Option<Value>,
    ) -> Result<ArborEvent, ArborError> {
        let node_id = self
            .storage
            .node_create_text(&tree_id, parent, content, metadata)
            .await?;
        Ok(ArborEvent::NodeCreated {
            tree_id,
            node_id,
            parent,
        })
    }

    /// Create an external node in a tree
    #[plexus_macros::method(params(
        tree_id = "UUID of the tree",
        parent = "Parent node ID (None for root-level)",
        handle = "Handle to external data",
        metadata = "Optional node metadata"
    ))]
    async fn node_create_external(
        &self,
        tree_id: TreeId,
        parent: Option<NodeId>,
        handle: Handle,
        metadata: Option<Value>,
    ) -> Result<ArborEvent, ArborError> {
        let node_id = self
            .storage
            .node_create_external(&tree_id, parent, handle, metadata)
            .await?;
        Ok(ArborEvent::NodeCreated {
            tree_id,
            node_id,
            parent,
        })
    }

    /// Get a node by ID
    #[plexus_macros::method(params(
        tree_id = "UUID of the tree",
        node_id = "UUID of the node"
    ))]
    async fn node_get(&self, tree_id: TreeId, node_id: NodeId) -> Result<ArborEvent, ArborError> {
        let node = self.storage.node_get(&tree_id, &node_id).await?;
        Ok(ArborEvent::NodeData { tree_id, node })
    }

    /// Get the children of a node
    #[plexus_macros::method(params(
        tree_id = "UUID of the tree",
        node_id = "UUID of the node"
    ))]
    async fn node_get_children(
        &self,
        tree_id: TreeId,
        node_id: NodeId,
    ) -> Result<ArborEvent, ArborError> {
        let children = self.storage.node_get_children(&tree_id, &node_id).await?;
        Ok(ArborEvent::NodeChildren {
            tree_id,
            node_id,
            children,
        })
    }

    /// Get the parent of a node
    #[plexus_macros::method(params(
        tree_id = "UUID of the tree",
        node_id = "UUID of the node"
    ))]
    async fn node_get_parent(
        &self,
        tree_id: TreeId,
        node_id: NodeId,
    ) -> Result<ArborEvent, ArborError> {
        let parent = self.storage.node_get_parent(&tree_id, &node_id).await?;
        Ok(ArborEvent::NodeParent {
            tree_id,
            node_id,
            parent,
        })
    }

    /// Get the path from root to a node
    #[plexus_macros::method(params(
        tree_id = "UUID of the tree",
        node_id = "UUID of the node"
    ))]
    async fn node_get_path(
        &self,
        tree_id: TreeId,
        node_id: NodeId,
    ) -> Result<ArborEvent, ArborError> {
        let path = self.storage.node_get_path(&tree_id, &node_id).await?;
        Ok(ArborEvent::ContextPath { tree_id, path })
    }

    /// List all leaf nodes in a tree
    #[plexus_macros::method(params(tree_id = "UUID of the tree"))]
    async fn context_list_leaves(&self, tree_id: TreeId) -> Result<ArborEvent, ArborError> {
        let leaves = self.storage.context_list_leaves(&tree_id).await?;
        Ok(ArborEvent::ContextLeaves { tree_id, leaves })
    }

    /// Get the full path data from root to a node
    #[plexus_macros::method(params(
        tree_id = "UUID of the tree",
        node_id = "UUID of the target node"
    ))]
    async fn context_get_path(
        &self,
        tree_id: TreeId,
        node_id: NodeId,
    ) -> Result<ArborEvent, ArborError> {
        let nodes = self.storage.context_get_path(&tree_id, &node_id).await?;
        Ok(ArborEvent::ContextPathData { tree_id, nodes })
    }

    /// Get all external handles in the path to a node
    #[plexus_macros::method(params(
        tree_id = "UUID of the tree",
        node_id = "UUID of the target node"
    ))]
    async fn context_get_handles(
        &self,
        tree_id: TreeId,
        node_id: NodeId,
    ) -> Result<ArborEvent, ArborError> {
        let handles = self.storage.context_get_handles(&tree_id, &node_id).await?;
        Ok(ArborEvent::ContextHandles { tree_id, handles })
    }

    /// Render tree as text visualization
    ///
    /// If handle resolvers are available, automatically resolves handles to show
    /// actual content. Otherwise, shows handle references.
    #[plexus_macros::method(params(tree_id = "UUID of the tree to render"))]
    async fn tree_render(&self, tree_id: TreeId) -> Result<ArborEvent, ArborError> {
        let tree = self.storage.tree_get(&tree_id).await?;

        // PLX-111/PLX-117: the injected `HandleResolvers` map stands in for the
        // parent handle. The two branches and the fallback strings below are the
        // pre-migration ones verbatim; only the source of the resolution moved.
        let render = if self.resolvers.is_empty() {
            // No resolvers - use simple render (shows handle references)
            tree.render()
        } else {
            // Resolve handles through the injected map
            let resolvers = &self.resolvers;
            tree.render_resolved(|handle| {
                let handle = handle.clone();
                async move { resolve_handle_to_string(resolvers, &handle).await }
            })
            .await
        };

        Ok(ArborEvent::TreeRender { tree_id, render })
    }
}

/// Resolve a handle through the injected resolvers and extract a display string
async fn resolve_handle_to_string(resolvers: &HandleResolvers, handle: &Handle) -> String {
    match resolvers.resolve(handle).await {
        Ok(mut stream) => {
            // Collect the first data item from the stream
            while let Some(item) = stream.next().await {
                match item {
                    PlexusStreamItem::Data { content, .. } => {
                        // Try to extract a meaningful display string from the resolved content
                        return extract_display_content(&content);
                    }
                    PlexusStreamItem::Error { message, .. } => {
                        return format!("[error: {message}]");
                    }
                    PlexusStreamItem::Done { .. } => break,
                    _ => {}
                }
            }
            format!("[empty: {handle}]")
        }
        Err(e) => {
            format!("[unresolved: {} - {}]", handle.method, e)
        }
    }
}

/// Extract display content from resolved handle data
fn extract_display_content(content: &Value) -> String {
    // Try common patterns for resolved content

    // Pattern 1: { "type": "message", "role": "...", "content": "..." }
    if let Some(msg_content) = content.get("content").and_then(|v| v.as_str()) {
        let role = content.get("role").and_then(|v| v.as_str()).unwrap_or("unknown");
        let name = content.get("name").and_then(|v| v.as_str());

        let truncated = if msg_content.len() > 60 {
            format!("{}...", &msg_content[..57])
        } else {
            msg_content.to_string()
        };

        return if let Some(n) = name {
            format!("[{}:{}] {}", role, n, truncated.replace('\n', "↵"))
        } else {
            format!("[{}] {}", role, truncated.replace('\n', "↵"))
        };
    }

    // Pattern 2: { "type": "...", ... } - use type as label
    if let Some(type_str) = content.get("type").and_then(|v| v.as_str()) {
        return format!("[{type_str}]");
    }

    // Fallback: show truncated JSON
    let json_str = content.to_string();
    if json_str.len() > 50 {
        format!("{}...", &json_str[..47])
    } else {
        json_str
    }
}
