// PLEXUS-6: the 0.6 macros' legacy form (MACRO-13) — these activations keep
// `namespace = …` and their own type names; the name rule's trait would need
// one event type per activation. Its deprecation note is allowed here.
#![allow(deprecated)]
use super::storage::{LatticeStorage, LatticeStorageConfig};
use super::types::{CreateResult, GraphId, NodeSpec, NodeId, AddNodeResult, EdgeCondition, AddEdgeResult, LatticeEventEnvelope, NodeOutput, NodeUpdateResult, GetNodeInputsResult, GetGraphResult, ListGraphsResult, CancelResult, GraphStatus, CreateChildGraphResult, GetChildGraphsResult};
use async_stream::stream;
use futures::Stream;
use serde_json::Value;
use std::sync::Arc;

/// Lattice — DAG execution engine
///
/// Manages graph topology and drives topological execution.
/// Nodes become "ready" when all predecessor nodes are complete.
/// The caller (e.g. Orcha) interprets node specs and drives actual execution.
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
    ) -> impl Stream<Item = CreateResult> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.create_graph(metadata).await {
                Ok(graph_id) => yield CreateResult::Ok { graph_id },
                Err(e) => yield CreateResult::Err { message: e },
            }
        }
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
    ) -> impl Stream<Item = AddNodeResult> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.add_node(&graph_id, node_id, &spec).await {
                Ok(node_id) => yield AddNodeResult::Ok { node_id },
                Err(e) => yield AddNodeResult::Err { message: e },
            }
        }
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
    ) -> impl Stream<Item = AddEdgeResult> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.add_edge(&graph_id, &from_node_id, &to_node_id, condition.as_ref()).await {
                Ok(()) => yield AddEdgeResult::Ok,
                Err(e) => yield AddEdgeResult::Err { message: e },
            }
        }
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
    ) -> impl Stream<Item = NodeUpdateResult> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.advance_graph(&graph_id, &node_id, output, None).await {
                Ok(()) => yield NodeUpdateResult::Ok,
                Err(e) => yield NodeUpdateResult::Err { message: e },
            }
        }
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
    ) -> impl Stream<Item = NodeUpdateResult> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.advance_graph(&graph_id, &node_id, None, Some(error)).await {
                Ok(()) => yield NodeUpdateResult::Ok,
                Err(e) => yield NodeUpdateResult::Err { message: e },
            }
        }
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
    ) -> impl Stream<Item = GetNodeInputsResult> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            // Validate node belongs to graph
            let nodes = match storage.get_nodes(&graph_id).await {
                Ok(ns) => ns,
                Err(e) => { yield GetNodeInputsResult::Err { message: e }; return; }
            };
            if !nodes.iter().any(|n| n.id == node_id) {
                yield GetNodeInputsResult::Err {
                    message: format!("Node {node_id} not found in graph {graph_id}"),
                };
                return;
            }
            match storage.get_node_inputs(&node_id).await {
                Ok(inputs) => yield GetNodeInputsResult::Ok { inputs },
                Err(e) => yield GetNodeInputsResult::Err { message: e },
            }
        }
    }

    /// Get graph state and all its nodes
    #[plexus_macros::method(params(
        graph_id = "ID of the graph to inspect"
    ))]
    async fn get(
        &self,
        graph_id: GraphId,
    ) -> impl Stream<Item = GetGraphResult> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            let graph = match storage.get_graph(&graph_id).await {
                Ok(g) => g,
                Err(e) => { yield GetGraphResult::Err { message: e }; return; }
            };
            let nodes = match storage.get_nodes(&graph_id).await {
                Ok(n) => n,
                Err(e) => { yield GetGraphResult::Err { message: e }; return; }
            };
            yield GetGraphResult::Ok { graph, nodes };
        }
    }

    /// List all graphs
    #[plexus_macros::method]
    async fn list(&self) -> impl Stream<Item = ListGraphsResult> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.list_graphs().await {
                Ok(graphs) => yield ListGraphsResult::Ok { graphs },
                Err(e) => yield ListGraphsResult::Err { message: e },
            }
        }
    }

    /// Cancel a running graph
    #[plexus_macros::method(params(
        graph_id = "ID of the graph to cancel"
    ))]
    async fn cancel(
        &self,
        graph_id: GraphId,
    ) -> impl Stream<Item = CancelResult> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.update_graph_status(&graph_id, GraphStatus::Cancelled).await {
                Ok(()) => yield CancelResult::Ok,
                Err(e) => yield CancelResult::Err { message: e },
            }
        }
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
    ) -> impl Stream<Item = CreateChildGraphResult> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.create_child_graph(&parent_id, metadata).await {
                Ok(graph_id) => yield CreateChildGraphResult::Ok { graph_id },
                Err(e) => yield CreateChildGraphResult::Err { message: e },
            }
        }
    }

    /// List all child graphs of a parent graph
    #[plexus_macros::method(params(
        parent_id = "ID of the parent graph"
    ))]
    async fn get_child_graphs(
        &self,
        parent_id: String,
    ) -> impl Stream<Item = GetChildGraphsResult> + Send + 'static {
        let storage = self.storage.clone();
        stream! {
            match storage.get_child_graphs(&parent_id).await {
                Ok(graphs) => yield GetChildGraphsResult::Ok { graphs },
                Err(e) => yield GetChildGraphsResult::Err { message: e },
            }
        }
    }
}
