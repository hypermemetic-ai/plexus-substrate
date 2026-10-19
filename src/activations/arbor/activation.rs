// PLEXUS-6: the 0.6 macros' legacy form (MACRO-13) — these activations keep
// `namespace = …` and their own type names; the name rule's trait would need
// one event type per activation. Its deprecation note is allowed here.
#![allow(deprecated)]
use super::storage::{ArborConfig, ArborStorage};
use super::types::{ArborEvent, Handle, NodeId, TreeId, TreeSkeleton};
use crate::plexus::{HubContext, NoParent, PlexusStreamItem};
use async_stream::stream;
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

/// Arbor activation - manages conversation trees
///
/// Generic over `P: HubContext` to support parent context injection for
/// resolving handles when rendering trees.
#[derive(Clone)]
pub struct Arbor<P: HubContext = NoParent> {
    storage: Arc<ArborStorage>,
    /// Hub reference for resolving handles when rendering trees
    hub: Arc<OnceLock<P>>,
    _phantom: PhantomData<P>,
}

impl<P: HubContext> Arbor<P> {
    /// Create a new Arbor activation with its own storage and specific context type
    pub async fn with_context_type(config: ArborConfig) -> Result<Self, String> {
        let storage = ArborStorage::new(config)
            .await
            .map_err(|e| format!("Failed to initialize Arbor storage: {e}"))?;

        Ok(Self {
            storage: Arc::new(storage),
            hub: Arc::new(OnceLock::new()),
            _phantom: PhantomData,
        })
    }

    /// Create an Arbor activation with a shared storage instance
    pub fn with_storage(storage: Arc<ArborStorage>) -> Self {
        Self {
            storage,
            hub: Arc::new(OnceLock::new()),
            _phantom: PhantomData,
        }
    }

    /// Get the underlying storage (for sharing with other activations)
    pub fn storage(&self) -> Arc<ArborStorage> {
        self.storage.clone()
    }

    /// Inject parent context for resolving handles
    ///
    /// Called during hub construction (e.g., via `Arc::new_cyclic` for `DynamicHub`).
    pub fn inject_parent(&self, parent: P) {
        if self.hub.set(parent).is_err() {
            tracing::warn!("Arbor parent context already set");
        }
    }

    /// Check if parent context has been injected
    pub fn has_parent(&self) -> bool {
        self.hub.get().is_some()
    }

    /// Get a reference to the parent context
    pub fn parent(&self) -> Option<&P> {
        self.hub.get()
    }
}

/// Convenience constructor for Arbor with `NoParent` (standalone/testing)
impl Arbor<NoParent> {
    pub async fn new(config: ArborConfig) -> Result<Self, String> {
        Self::with_context_type(config).await
    }
}

#[plexus_macros::activation(namespace = "arbor",
version = "1.0.0",
description = "Manage conversation trees with context tracking")]
impl<P: HubContext> Arbor<P> {
    /// Create a new conversation tree
    #[plexus_macros::method(params(
        metadata = "Optional tree-level metadata (name, description, etc.)",
        owner_id = "Owner identifier (default: 'system')"
    ))]
    async fn tree_create(
        &self,
        metadata: Option<Value>,
        owner_id: String,
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.tree_create(metadata, &owner_id).await {
                Ok(tree_id) => yield ArborEvent::TreeCreated { tree_id },
                Err(e) => {
                    eprintln!("Error creating tree: {e}");
                    yield ArborEvent::TreeCreated { tree_id: TreeId::nil() };
                }
            }
        }
    }

    /// Retrieve a complete tree with all nodes
    #[plexus_macros::method(params(tree_id = "UUID of the tree to retrieve"))]
    async fn tree_get(&self, tree_id: TreeId) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.tree_get(&tree_id).await {
                Ok(tree) => yield ArborEvent::TreeData { tree },
                Err(e) => {
                    eprintln!("Error getting tree: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
    }

    /// Get lightweight tree structure without node data
    #[plexus_macros::method(params(tree_id = "UUID of the tree to retrieve"))]
    async fn tree_get_skeleton(
        &self,
        tree_id: TreeId,
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.tree_get(&tree_id).await {
                Ok(tree) => yield ArborEvent::TreeSkeleton { skeleton: TreeSkeleton::from(&tree) },
                Err(e) => {
                    eprintln!("Error getting tree skeleton: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
    }

    /// List all active trees
    #[plexus_macros::method]
    async fn tree_list(&self) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.tree_list(false).await {
                Ok(tree_ids) => yield ArborEvent::TreeList { tree_ids },
                Err(e) => {
                    eprintln!("Error listing trees: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
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
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.tree_update_metadata(&tree_id, metadata).await {
                Ok(_) => yield ArborEvent::TreeUpdated { tree_id },
                Err(e) => {
                    eprintln!("Error updating tree metadata: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
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
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.tree_claim(&tree_id, &owner_id, count).await {
                Ok(new_count) => yield ArborEvent::TreeClaimed { tree_id, owner_id, new_count },
                Err(e) => {
                    eprintln!("Error claiming tree: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
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
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.tree_release(&tree_id, &owner_id, count).await {
                Ok(new_count) => yield ArborEvent::TreeReleased { tree_id, owner_id, new_count },
                Err(e) => {
                    eprintln!("Error releasing tree: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
    }

    /// List trees scheduled for deletion
    #[plexus_macros::method]
    async fn tree_list_scheduled(&self) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.tree_list(true).await {
                Ok(tree_ids) => yield ArborEvent::TreesScheduled { tree_ids },
                Err(e) => {
                    eprintln!("Error listing scheduled trees: {e}");
                    yield ArborEvent::TreesScheduled { tree_ids: vec![] };
                }
            }
        }
    }

    /// List archived trees
    #[plexus_macros::method]
    async fn tree_list_archived(&self) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.tree_list(true).await {
                Ok(tree_ids) => yield ArborEvent::TreesArchived { tree_ids },
                Err(e) => {
                    eprintln!("Error listing archived trees: {e}");
                    yield ArborEvent::TreesArchived { tree_ids: vec![] };
                }
            }
        }
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
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.node_create_text(&tree_id, parent, content, metadata).await {
                Ok(node_id) => yield ArborEvent::NodeCreated { tree_id, node_id, parent },
                Err(e) => {
                    eprintln!("Error creating text node: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
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
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.node_create_external(&tree_id, parent, handle, metadata).await {
                Ok(node_id) => yield ArborEvent::NodeCreated { tree_id, node_id, parent },
                Err(e) => {
                    eprintln!("Error creating external node: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
    }

    /// Get a node by ID
    #[plexus_macros::method(params(
        tree_id = "UUID of the tree",
        node_id = "UUID of the node"
    ))]
    async fn node_get(
        &self,
        tree_id: TreeId,
        node_id: NodeId,
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.node_get(&tree_id, &node_id).await {
                Ok(node) => yield ArborEvent::NodeData { tree_id, node },
                Err(e) => {
                    eprintln!("Error getting node: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
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
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.node_get_children(&tree_id, &node_id).await {
                Ok(children) => yield ArborEvent::NodeChildren { tree_id, node_id, children },
                Err(e) => {
                    eprintln!("Error getting node children: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
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
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.node_get_parent(&tree_id, &node_id).await {
                Ok(parent) => yield ArborEvent::NodeParent { tree_id, node_id, parent },
                Err(e) => {
                    eprintln!("Error getting node parent: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
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
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.node_get_path(&tree_id, &node_id).await {
                Ok(path) => yield ArborEvent::ContextPath { tree_id, path },
                Err(e) => {
                    eprintln!("Error getting node path: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
    }

    /// List all leaf nodes in a tree
    #[plexus_macros::method(params(tree_id = "UUID of the tree"))]
    async fn context_list_leaves(
        &self,
        tree_id: TreeId,
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.context_list_leaves(&tree_id).await {
                Ok(leaves) => yield ArborEvent::ContextLeaves { tree_id, leaves },
                Err(e) => {
                    eprintln!("Error listing leaf nodes: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
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
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.context_get_path(&tree_id, &node_id).await {
                Ok(nodes) => yield ArborEvent::ContextPathData { tree_id, nodes },
                Err(e) => {
                    eprintln!("Error getting context path: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
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
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.context_get_handles(&tree_id, &node_id).await {
                Ok(handles) => yield ArborEvent::ContextHandles { tree_id, handles },
                Err(e) => {
                    eprintln!("Error getting context handles: {e}");
                    yield ArborEvent::TreeList { tree_ids: vec![] };
                }
            }
        }
    }

    /// Render tree as text visualization
    ///
    /// If parent context is available, automatically resolves handles to show
    /// actual content. Otherwise, shows handle references.
    #[plexus_macros::method(params(tree_id = "UUID of the tree to render"))]
    async fn tree_render(
        &self,
        tree_id: TreeId,
    ) -> impl Stream<Item = ArborEvent> + Send + 'static {
        let storage = self.storage.clone();
        let hub = self.hub.clone();

        stream! {
            match storage.tree_get(&tree_id).await {
                Ok(tree) => {
                    // Check if we have parent context for handle resolution
                    let render = if let Some(parent) = hub.get() {
                        // Resolve handles through parent context
                        tree.render_resolved(|handle| {
                            let parent = parent.clone();
                            let handle = handle.clone();
                            async move {
                                resolve_handle_to_string(&parent, &handle).await
                            }
                        }).await
                    } else {
                        // No parent context - use simple render (shows handle references)
                        tree.render()
                    };
                    yield ArborEvent::TreeRender { tree_id, render };
                }
                Err(e) => {
                    eprintln!("Error rendering tree: {e}");
                    yield ArborEvent::TreeRender { tree_id, render: format!("Error: {e}") };
                }
            }
        }
    }

}

/// Resolve a handle through `HubContext` and extract a display string
async fn resolve_handle_to_string<P: HubContext>(parent: &P, handle: &Handle) -> String {
    match parent.resolve_handle(handle).await {
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
