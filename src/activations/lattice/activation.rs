use super::storage::{LatticeStorage, LatticeStorageConfig};
use super::types::{GraphId, NodeSpec, NodeId, EdgeCondition, LatticeError, LatticeEventEnvelope, LatticeGraph, NodeOutput, Token, GetGraphResult, GraphStatus};
use futures::Stream;
use serde_json::Value;
use std::sync::Arc;

/// Lattice — DAG execution engine
///
/// Manages graph topology and drives topological execution.
/// Nodes become "ready" when all predecessor nodes are complete.
/// The caller (e.g. Orcha) interprets node specs and drives actual execution.
///
/// PLX-118: every method except `execute` yields exactly once and is therefore
/// spelled as a unary `Result<T, LatticeError>` (PLX-110) — no updates, one
/// terminal carrying the value. `execute` is the one genuine stream here: it is
/// a long-lived sequenced event feed that emits until `GraphDone`/`GraphFailed`,
/// so it keeps `impl Stream`.
#[derive(Clone)]
pub struct Lattice {
    storage: Arc<LatticeStorage>,
}

impl Lattice {
    pub async fn new(config: LatticeStorageConfig) -> Result<Self, String> {
        let storage = LatticeStorage::new(config).await?;
        Ok(Self {
            storage: Arc::new(storage),
        })
    }

    /// Expose the underlying storage for library consumers (e.g. Orcha).
    pub fn storage(&self) -> Arc<LatticeStorage> {
        self.storage.clone()
    }
}

#[plexus_macros::activation(namespace = "lattice",
version = "1.0.0",
description = "DAG execution engine — manages graph topology and drives topological execution")]
impl Lattice {
    /// Create an empty graph
    #[plexus_macros::method(params(
        metadata = "Arbitrary metadata to attach to this graph"
    ))]
    async fn create(
        &self,
        metadata: Value,
    ) -> Result<GraphId, LatticeError> {
        Ok(self.storage.create_graph(metadata).await?)
    }

    /// Add a node to the graph
    ///
    /// spec carries the typed node execution semantics (Task, Scatter, Gather, `SubGraph`).
    /// `node_id` is optional — a UUID is generated if not provided.
    #[plexus_macros::method(params(
        graph_id = "ID of the graph to add the node to",
        spec = "Node specification: typed enum (task/scatter/gather/subgraph)",
        node_id = "Optional node ID hint; a UUID is generated if not provided"
    ))]
    async fn add_node(
        &self,
        graph_id: GraphId,
        spec: NodeSpec,
        node_id: Option<NodeId>,
    ) -> Result<NodeId, LatticeError> {
        Ok(self.storage.add_node(&graph_id, node_id, &spec).await?)
    }

    /// Add a dependency edge: `to_node` waits for `from_node` to complete
    ///
    /// condition optionally filters which token colors are routed on this edge.
    /// None (default) passes any token; Some(color) routes only matching-color tokens.
    #[plexus_macros::method(params(
        graph_id = "ID of the graph",
        from_node_id = "Predecessor node — must complete before to_node becomes ready",
        to_node_id = "Dependent node — becomes ready when all predecessors are complete",
        condition = "Optional edge condition: filter tokens by color (null = pass any)"
    ))]
    async fn add_edge(
        &self,
        graph_id: GraphId,
        from_node_id: NodeId,
        to_node_id: NodeId,
        condition: Option<EdgeCondition>,
    ) -> Result<(), LatticeError> {
        self.storage
            .add_edge(&graph_id, &from_node_id, &to_node_id, condition.as_ref())
            .await?;
        Ok(())
    }

    /// Start execution — long-lived stream of sequenced events.
    ///
    /// **Fresh start** (`after_seq` omitted, graph is Pending):
    /// Seeds root nodes as Ready, persists `NodeReady` events, then streams live.
    ///
    /// **Reconnect** (`after_seq = <last seq received>`):
    /// Replays every event that occurred after that sequence number, then streams live.
    /// Pass the last `seq` from a `LatticeEventEnvelope` you successfully processed.
    ///
    /// **Replay from beginning** (`after_seq = 0`, or omitted on an already-Running graph):
    /// Replays the complete event history then streams live.
    ///
    /// The stream closes when `GraphDone` or `GraphFailed` is emitted.
    ///
    /// PLX-118: this is the one lattice method that genuinely multi-shots, so it
    /// keeps `impl Stream`. It does **not** declare `streaming` and that is left
    /// exactly as found — the flag drives `MethodSchema` and therefore
    /// plexus-transport's SSE-vs-JSON decision (PLX-107), so adding it here
    /// would change routing behaviour, which PLX-118's T3 forbids.
    #[plexus_macros::method(params(
        graph_id = "ID of the graph to execute",
        after_seq = "Cursor for reconnect replay — omit for fresh start, or pass last received seq"
    ))]
    async fn execute(
        &self,
        graph_id: GraphId,
        after_seq: Option<u64>,
    ) -> impl Stream<Item = LatticeEventEnvelope> + Send + 'static {
        LatticeStorage::execute_stream(self.storage.clone(), graph_id, after_seq)
    }

    /// Signal that a node finished successfully
    ///
    /// output carries typed token(s) to route to successor nodes.
    /// Triggers `NodeReady` for any newly unblocked successors.
    #[plexus_macros::method(params(
        graph_id = "ID of the graph",
        node_id = "ID of the completed node",
        output = "Optional output: Single(token) or Many(tokens) for fan-out"
    ))]
    async fn node_complete(
        &self,
        graph_id: GraphId,
        node_id: NodeId,
        output: Option<NodeOutput>,
    ) -> Result<(), LatticeError> {
        self.storage
            .advance_graph(&graph_id, &node_id, output, None)
            .await?;
        Ok(())
    }

    /// Signal that a node failed — triggers `GraphFailed`
    #[plexus_macros::method(params(
        graph_id = "ID of the graph",
        node_id = "ID of the failed node",
        error = "Error message describing the failure"
    ))]
    async fn node_failed(
        &self,
        graph_id: GraphId,
        node_id: NodeId,
        error: String,
    ) -> Result<(), LatticeError> {
        self.storage
            .advance_graph(&graph_id, &node_id, None, Some(error))
            .await?;
        Ok(())
    }

    /// Get raw input tokens for a node — what arrived on all inbound edges.
    ///
    /// Returns Token { color, payload: Data { value } | Handle | None }.
    /// Callers that need handle resolution should use Orcha's `resolve_node_inputs` instead.
    #[plexus_macros::method(params(
        graph_id = "ID of the graph",
        node_id = "ID of the node to inspect inputs for"
    ))]
    async fn get_node_inputs(
        &self,
        graph_id: GraphId,
        node_id: NodeId,
    ) -> Result<Vec<Token>, LatticeError> {
        // Validate node belongs to graph
        let nodes = self.storage.get_nodes(&graph_id).await?;
        if !nodes.iter().any(|n| n.id == node_id) {
            return Err(LatticeError::NodeNotInGraph { graph_id, node_id });
        }
        Ok(self.storage.get_node_inputs(&node_id).await?)
    }

    /// Get graph state and all its nodes
    #[plexus_macros::method(params(
        graph_id = "ID of the graph to inspect"
    ))]
    async fn get(
        &self,
        graph_id: GraphId,
    ) -> Result<GetGraphResult, LatticeError> {
        let graph = self.storage.get_graph(&graph_id).await?;
        let nodes = self.storage.get_nodes(&graph_id).await?;
        Ok(GetGraphResult { graph, nodes })
    }

    /// List all graphs
    #[plexus_macros::method]
    async fn list(&self) -> Result<Vec<LatticeGraph>, LatticeError> {
        Ok(self.storage.list_graphs().await?)
    }

    /// Cancel a running graph
    #[plexus_macros::method(params(
        graph_id = "ID of the graph to cancel"
    ))]
    async fn cancel(
        &self,
        graph_id: GraphId,
    ) -> Result<(), LatticeError> {
        self.storage
            .update_graph_status(&graph_id, GraphStatus::Cancelled)
            .await?;
        Ok(())
    }

    /// Add a `SubGraph` node — when dispatched, runs the child graph to completion.
    ///
    /// On child success, the parent node receives `{"child_graph_id": "..."}` as output.
    /// On child failure, the parent node is failed (error edge fires if present).
    #[plexus_macros::method(params(
        parent_id = "ID of the parent graph",
        metadata = "Arbitrary JSON metadata attached to the graph"
    ))]
    async fn create_child_graph(
        &self,
        parent_id: String,
        metadata: Value,
    ) -> Result<GraphId, LatticeError> {
        Ok(self.storage.create_child_graph(&parent_id, metadata).await?)
    }

    /// List all child graphs of a parent graph
    #[plexus_macros::method(params(
        parent_id = "ID of the parent graph"
    ))]
    async fn get_child_graphs(
        &self,
        parent_id: String,
    ) -> Result<Vec<LatticeGraph>, LatticeError> {
        Ok(self.storage.get_child_graphs(&parent_id).await?)
    }
}
