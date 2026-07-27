use crate::activations::lattice::{LatticeStorage, NodeSpec, NodeStatus};
use crate::activations::orcha::{OrchaError, OrchaNodeKind};
use async_stream::stream;
use futures::Stream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use super::storage::PmStorage;

// ─── Result types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub(super) struct PmTicketStatus {
    pub ticket_id: String,
    pub node_id: String,
    pub status: String,
    pub kind: String,
    pub label: Option<String>,
    pub child_graph_id: Option<String>,
}

/// The terminal value of `graph_status`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub(super) struct PmGraphStatus {
    pub graph_id: String,
    pub graph_status: String,
    pub tickets: Vec<PmTicketStatus>,
}

/// The terminal value of `what_next`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub(super) struct PmWhatNext {
    pub graph_id: String,
    pub tickets: Vec<PmTicketStatus>,
}

/// The terminal value of `inspect_ticket`; `None` replaces the old `NotFound`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub(super) struct PmTicketDetail {
    pub ticket_id: String,
    pub node_id: String,
    pub status: String,
    pub kind: String,
    pub task: Option<String>,
    pub command: Option<String>,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub child_graph_id: Option<String>,
}

/// The terminal value of `why_blocked`; an empty `blocked_by` is the old
/// `NotBlocked` variant.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub(super) struct PmBlockers {
    pub ticket_id: String,
    pub blocked_by: Vec<PmTicketStatus>,
}

/// The terminal value of `get_ticket_source`; `None` replaces the old
/// `not_found` tagged object.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub(super) struct PmTicketSource {
    pub graph_id: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub(super) struct PmGraphSummary {
    pub graph_id: String,
    pub status: String,
    pub metadata: Value,
    pub ticket_count: usize,
    pub created_at: i64,
    /// Original task description passed to `run_plan` / `run_tickets` (first 200 chars).
    pub source: Option<String>,
}


// ─── Pm activation ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Pm {
    pm_storage: Arc<PmStorage>,
    lattice_storage: Arc<LatticeStorage>,
}

impl Pm {
    pub const fn new(pm_storage: Arc<PmStorage>, lattice_storage: Arc<LatticeStorage>) -> Self {
        Self { pm_storage, lattice_storage }
    }

    /// Save ticket→node mappings for a graph (called by Orcha after build).
    pub async fn save_ticket_map(
        &self,
        graph_id: &str,
        map: &HashMap<String, String>,
    ) -> Result<(), String> {
        self.pm_storage.save_ticket_map(graph_id, map).await
    }

    /// Fetch the `ticket_id→node_id` map for a graph.
    pub async fn get_ticket_map(&self, graph_id: &str) -> Result<HashMap<String, String>, String> {
        self.pm_storage.get_ticket_map(graph_id).await
    }

    /// Return all graph IDs known to PM (regardless of status), most-recent first.
    ///
    /// Used by the startup recovery pass to find graphs that should be re-watched.
    pub async fn list_all_graph_ids(&self) -> Result<Vec<String>, String> {
        let entries = self.pm_storage.list_ticket_maps(usize::MAX).await?;
        Ok(entries.into_iter().map(|(id, _)| id).collect())
    }

    /// Save the raw ticket source for a graph (called by `run_tickets` / `run_tickets_async`).
    pub async fn save_ticket_source(&self, graph_id: &str, source: &str) -> Result<(), String> {
        self.pm_storage.save_ticket_source(graph_id, source).await
    }

    /// Fetch the raw ticket source for a graph.
    pub async fn get_ticket_source_raw(&self, graph_id: &str) -> Result<Option<String>, String> {
        self.pm_storage.get_ticket_source(graph_id).await
    }

    /// Append a single event to the node execution log.
    ///
    /// Called from `dispatch_task` for each `ChatEvent` and the final outcome.
    pub async fn log_node_event(
        &self,
        graph_id: &str,
        node_id: &str,
        ticket_id: Option<&str>,
        seq: i64,
        event_type: &str,
        event_data: serde_json::Value,
    ) {
        let data_str = serde_json::to_string(&event_data).unwrap_or_default();
        if let Err(e) = self.pm_storage
            .append_node_log(graph_id, node_id, ticket_id, seq, event_type, &data_str)
            .await
        {
            tracing::warn!("log_node_event failed for {}/{}: {}", graph_id, node_id, e);
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

const fn node_status_str(status: &NodeStatus) -> &'static str {
    match status {
        NodeStatus::Pending => "pending",
        NodeStatus::Ready => "ready",
        NodeStatus::Running => "running",
        NodeStatus::Complete => "complete",
        NodeStatus::Failed => "failed",
    }
}

fn extract_kind_and_label(spec: &NodeSpec) -> (String, Option<String>) {
    match spec {
        NodeSpec::Task { data, .. } => {
            match serde_json::from_value::<OrchaNodeKind>(data.clone()) {
                Ok(OrchaNodeKind::Task { task, .. }) => {
                    let label = task.chars().take(80).collect::<String>();
                    ("task".to_string(), Some(label))
                }
                Ok(OrchaNodeKind::Synthesize { task, .. }) => {
                    let label = task.chars().take(80).collect::<String>();
                    ("synthesize".to_string(), Some(label))
                }
                Ok(OrchaNodeKind::Validate { command, .. }) => {
                    let label = command.chars().take(80).collect::<String>();
                    ("validate".to_string(), Some(label))
                }
                Ok(OrchaNodeKind::Review { prompt }) => {
                    let label = prompt.chars().take(80).collect::<String>();
                    ("review".to_string(), Some(label))
                }
                Ok(OrchaNodeKind::Plan { task }) => {
                    let label = task.chars().take(80).collect::<String>();
                    ("plan".to_string(), Some(label))
                }
                Err(_) => ("task".to_string(), None),
            }
        }
        NodeSpec::Gather { .. } => ("gather".to_string(), None),
        NodeSpec::Scatter { .. } => ("scatter".to_string(), None),
        NodeSpec::SubGraph { .. } => ("subgraph".to_string(), None),
    }
}

// ─── Hub methods ─────────────────────────────────────────────────────────────

#[plexus_macros::activation(namespace = "pm",
version = "1.0.0",
description = "Project management view of orcha graph execution in ticket vocabulary")]
impl Pm {
    /// Get the status of all tickets in a graph.
    #[plexus_macros::method(params(
        graph_id   = "The lattice graph ID returned by build_tickets or run_tickets",
        recursive  = "Optional: when true, include child_graph_id from completed node outputs (default false)"
    ))]
    async fn graph_status(
        &self,
        graph_id: String,
        recursive: Option<bool>,
    ) -> Result<PmGraphStatus, OrchaError> {
        let pm_storage = self.pm_storage.clone();
        let lattice_storage = self.lattice_storage.clone();

        let ticket_map = pm_storage.get_ticket_map(&graph_id).await?;

        let mut tickets = Vec::new();
        let mut has_pending = false;
        let mut has_ready = false;
        let mut has_running = false;
        let mut has_failed = false;
        let mut all_complete = true;

        for (ticket_id, node_id) in &ticket_map {
            let node = lattice_storage.get_node(node_id).await.map_err(|e| {
                OrchaError::storage("get_node", format!("Failed to get node {node_id}: {e}"))
            })?;
            match node.status {
                NodeStatus::Pending  => { has_pending  = true; all_complete = false; }
                NodeStatus::Ready    => { has_ready    = true; all_complete = false; }
                NodeStatus::Running  => { has_running  = true; all_complete = false; }
                NodeStatus::Failed   => { has_failed   = true; all_complete = false; }
                NodeStatus::Complete => {}
            }
            let (kind, label) = extract_kind_and_label(&node.spec);
            let child_graph_id = if recursive.unwrap_or(false) && node.status == NodeStatus::Complete {
                node.output.as_ref().and_then(|o| {
                    if let crate::activations::lattice::NodeOutput::Single(token) = o {
                        if let Some(crate::activations::lattice::TokenPayload::Data { value }) = &token.payload {
                            value.get("child_graph_id").and_then(|v| v.as_str()).map(std::string::ToString::to_string)
                        } else { None }
                    } else { None }
                })
            } else {
                None
            };
            tickets.push(PmTicketStatus {
                ticket_id: ticket_id.clone(),
                node_id: node_id.clone(),
                status: node_status_str(&node.status).to_string(),
                kind,
                label,
                child_graph_id,
            });
        }

        let graph_status = if has_failed {
            "failed"
        } else if has_running || has_ready {
            "running"
        } else if has_pending {
            "pending"
        } else if all_complete && !ticket_map.is_empty() {
            "complete"
        } else {
            "pending"
        };

        Ok(PmGraphStatus {
            graph_id,
            graph_status: graph_status.to_string(),
            tickets,
        })
    }

    /// Get tickets that are ready or running (next actionable items).
    #[plexus_macros::method(params(
        graph_id = "The lattice graph ID returned by build_tickets or run_tickets"
    ))]
    async fn what_next(
        &self,
        graph_id: String,
    ) -> Result<PmWhatNext, OrchaError> {
        let pm_storage = self.pm_storage.clone();
        let lattice_storage = self.lattice_storage.clone();

        let ticket_map = pm_storage.get_ticket_map(&graph_id).await?;

        let mut tickets = Vec::new();
        for (ticket_id, node_id) in &ticket_map {
            let node = lattice_storage.get_node(node_id).await.map_err(|e| {
                OrchaError::storage("get_node", format!("Failed to get node {node_id}: {e}"))
            })?;
            if matches!(node.status, NodeStatus::Ready | NodeStatus::Running) {
                let (kind, label) = extract_kind_and_label(&node.spec);
                tickets.push(PmTicketStatus {
                    ticket_id: ticket_id.clone(),
                    node_id: node_id.clone(),
                    status: node_status_str(&node.status).to_string(),
                    kind,
                    label,
                    child_graph_id: None,
                });
            }
        }

        Ok(PmWhatNext { graph_id, tickets })
    }

    /// Inspect a single ticket in detail.
    #[plexus_macros::method(params(
        graph_id = "The lattice graph ID returned by build_tickets or run_tickets",
        ticket_id = "The ticket ID (as used in the ticket file)"
    ))]
    async fn inspect_ticket(
        &self,
        graph_id: String,
        ticket_id: String,
    ) -> Result<Option<PmTicketDetail>, OrchaError> {
        let pm_storage = self.pm_storage.clone();
        let lattice_storage = self.lattice_storage.clone();

        let ticket_map = pm_storage.get_ticket_map(&graph_id).await?;

        let Some(node_id) = ticket_map.get(&ticket_id).cloned() else {
            return Ok(None);
        };

        let node = lattice_storage.get_node(&node_id).await.map_err(|e| {
            OrchaError::storage("get_node", format!("Failed to get node: {e}"))
        })?;

        let status = node_status_str(&node.status).to_string();
        let output = node.output.as_ref()
            .map(|o| serde_json::to_value(o).unwrap_or(Value::Null));
        let error = node.error.clone();

        let child_graph_id = output.as_ref()
            .and_then(|o| o.get("payload"))
            .and_then(|p| p.get("value"))
            .and_then(|v| v.get("child_graph_id"))
            .and_then(|id| id.as_str())
            .map(std::string::ToString::to_string);

        // (kind, task, command) — the only thing the old match arms differed on.
        let (kind, task, command) = match &node.spec {
            NodeSpec::Task { data, .. } => {
                match serde_json::from_value::<OrchaNodeKind>(data.clone()) {
                    Ok(OrchaNodeKind::Task { task, .. }) => ("task", Some(task), None),
                    Ok(OrchaNodeKind::Synthesize { task, .. }) => ("synthesize", Some(task), None),
                    Ok(OrchaNodeKind::Validate { command, .. }) => ("validate", None, Some(command)),
                    Ok(OrchaNodeKind::Review { prompt }) => ("review", Some(prompt), None),
                    Ok(OrchaNodeKind::Plan { task }) => ("plan", Some(task), None),
                    Err(_) => ("task", None, None),
                }
            }
            NodeSpec::Gather { .. } => ("gather", None, None),
            _ => ("other", None, None),
        };

        Ok(Some(PmTicketDetail {
            ticket_id, node_id, status,
            kind: kind.to_string(),
            task, command, output, error,
            child_graph_id,
        }))
    }

    /// Explain why a ticket is blocked.
    #[plexus_macros::method(params(
        graph_id = "The lattice graph ID returned by build_tickets or run_tickets",
        ticket_id = "The ticket ID to investigate"
    ))]
    async fn why_blocked(
        &self,
        graph_id: String,
        ticket_id: String,
    ) -> Result<PmBlockers, OrchaError> {
        let pm_storage = self.pm_storage.clone();
        let lattice_storage = self.lattice_storage.clone();

        let ticket_map = pm_storage.get_ticket_map(&graph_id).await?;

        let Some(node_id) = ticket_map.get(&ticket_id).cloned() else {
            return Err(OrchaError::ValidationError {
                detail: format!("Ticket not found: {ticket_id}"),
            });
        };

        let predecessors = lattice_storage.get_inbound_edges(&node_id).await.map_err(|e| {
            OrchaError::storage("get_inbound_edges", format!("Failed to get predecessors: {e}"))
        })?;

        let mut blocked_by = Vec::new();
        for pred_id in predecessors {
            let Ok(pred_node) = lattice_storage.get_node(&pred_id).await else {
                continue;
            };

            if pred_node.status == NodeStatus::Complete {
                continue;
            }

            let pred_ticket_id = pm_storage
                .get_ticket_for_node(&graph_id, &pred_id)
                .await
                .unwrap_or(None)
                .unwrap_or_else(|| pred_id.clone());

            let (kind, label) = extract_kind_and_label(&pred_node.spec);
            blocked_by.push(PmTicketStatus {
                ticket_id: pred_ticket_id,
                node_id: pred_id,
                status: node_status_str(&pred_node.status).to_string(),
                kind,
                label,
                child_graph_id: None,
            });
        }

        // An empty `blocked_by` is the old `NotBlocked` variant.
        Ok(PmBlockers { ticket_id, blocked_by })
    }

    /// Get the raw ticket source for a graph.
    #[plexus_macros::method(params(
        graph_id = "The lattice graph ID"
    ))]
    async fn get_ticket_source(
        &self,
        graph_id: String,
    ) -> Result<Option<PmTicketSource>, OrchaError> {
        let source = self.pm_storage.get_ticket_source(&graph_id).await?;
        Ok(source.map(|source| PmTicketSource { graph_id, source }))
    }

    /// List graphs tracked by the pm layer, optionally filtered by project metadata.
    #[plexus_macros::method(params(
        project   = "Optional: filter by metadata.project string",
        limit     = "Optional: max results (default 20)",
        root_only = "Optional: when true (default), only return root graphs (no parent); set false to include subgraphs",
        status    = "Optional: filter by graph status (running, complete, failed)"
    ))]
    async fn list_graphs(
        &self,
        project: Option<String>,
        limit: Option<usize>,
        root_only: Option<bool>,
        status: Option<String>,
    ) -> Result<Vec<PmGraphSummary>, OrchaError> {
        let pm_storage = self.pm_storage.clone();
        let lattice_storage = self.lattice_storage.clone();

        {
            let limit = limit.unwrap_or(20);

            let entries = pm_storage.list_ticket_maps(limit).await?;

            let mut graphs = Vec::new();

            for (graph_id, created_at) in entries {
                let Ok(lattice_graph) = lattice_storage.get_graph(&graph_id).await else {
                    continue;
                };

                // Apply root_only filter (default true — skip child graphs).
                if root_only.unwrap_or(true) && lattice_graph.parent_graph_id.is_some() {
                    continue;
                }

                // Apply optional status filter.
                if let Some(ref status_filter) = status {
                    if lattice_graph.status.to_string() != *status_filter {
                        continue;
                    }
                }

                // Apply optional project filter.
                if let Some(ref project_filter) = project {
                    let graph_project = lattice_graph.metadata.get("project")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if graph_project != project_filter.as_str() {
                        continue;
                    }
                }

                let ticket_map = pm_storage.get_ticket_map(&graph_id).await.unwrap_or_default();

                let status = lattice_graph.status.to_string();

                let source = pm_storage.get_ticket_source(&graph_id).await
                    .ok()
                    .flatten()
                    .map(|s: String| {
                        // Truncate to 200 chars for summary display
                        let trimmed = s.trim().to_string();
                        if trimmed.len() > 200 {
                            format!("{}…", &trimmed[..197])
                        } else {
                            trimmed
                        }
                    });

                graphs.push(PmGraphSummary {
                    graph_id,
                    status,
                    metadata: lattice_graph.metadata,
                    ticket_count: ticket_map.len(),
                    created_at,
                    source,
                });
            }

            Ok(graphs)
        }
    }

    /// Retrieve the full execution log for a node.
    ///
    /// Returns all events recorded by `dispatch_task` in sequence order:
    /// "prompt" (task sent to Claude), "start" (session created), "`tool_use`",
    /// "`tool_result`", "complete", "error", "passthrough", "outcome" (final result).
    ///
    /// Use this to diagnose why a node failed or produced unexpected output.
    #[plexus_macros::method(params(
        graph_id = "Graph ID (from GraphStarted event or pm.list_graphs)",
        node_id  = "Lattice node ID (from NodeStarted event or pm.graph_status)"
    ))]
    async fn get_node_log(
        &self,
        graph_id: String,
        node_id: String,
    ) -> impl Stream<Item = Value> + Send + 'static {
        let pm_storage = self.pm_storage.clone();
        stream! {
            match pm_storage.get_node_log(&graph_id, &node_id).await {
                Ok(entries) => {
                    for entry in entries {
                        let data: Value = serde_json::from_str(&entry.event_data)
                            .unwrap_or(serde_json::json!({ "raw": entry.event_data }));
                        yield serde_json::json!({
                            "seq": entry.seq,
                            "event_type": entry.event_type,
                            "data": data,
                            "created_at": entry.created_at,
                        });
                    }
                }
                Err(e) => {
                    yield serde_json::json!({ "type": "err", "message": e });
                }
            }
        }
    }
}
