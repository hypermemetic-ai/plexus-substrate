use super::graph_runner;
use super::graph_runtime::GraphRuntime;
use super::orchestrator::run_orchestration_task;
use super::pm;
use super::storage::OrchaStorage;
use super::ticket_compiler;
use super::types::{OrchaEvent, OrchaError, RunTaskRequest, CreateSessionRequest, SessionCreated, AgentMode, SessionId, SessionInfo, SessionState, GetSessionRequest, ValidationArtifact, RetryStatus, SessionRef, MonitorTreeInfo, CheckStatusRequest, StatusSummary, AgentSummary, SpawnAgentRequest, SpawnedAgent, ListAgentsRequest, GetAgentRequest, ListApprovalsRequest, ApprovalInfo, ApproveRequest, ApprovalOutcome, DenyRequest, GraphRef, NodeRef, GatherStrategy, OrchaNodeDef, OrchaEdgeDef, OrchaNodeSpec, ValidationResult, AgentInfo};
use crate::activations::claudecode::{ClaudeCode, Model};
use crate::activations::claudecode_loopback::ClaudeCodeLoopback;
use crate::plexus::{HubContext, NoParent};
use async_stream::stream;
use futures::Stream;
use futures::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::process::Command;
use uuid::Uuid;

/// Registry of cancellation senders keyed by `graph_id`.
///
/// When `cancel_graph` is called, the sender's value is set to `true`, which all
/// running node tasks observe via their cloned `Receiver<bool>` and exit early.
type CancelRegistry = Arc<tokio::sync::Mutex<HashMap<String, tokio::sync::watch::Sender<bool>>>>;

/// Orcha activation - Full orchestration with approval loops and validation
///
/// Provides both full orchestration (`run_task`) and coordination helpers.
#[derive(Clone)]
pub struct Orcha<P: HubContext = NoParent> {
    storage: Arc<OrchaStorage>,
    claudecode: Arc<ClaudeCode>,
    loopback: Arc<ClaudeCodeLoopback>,
    arbor_storage: Arc<crate::activations::arbor::ArborStorage>,
    graph_runtime: Arc<GraphRuntime>,
    pm: Arc<pm::Pm>,
    /// Cancellation registry: `graph_id` → watch sender (true = cancel).
    cancel_registry: CancelRegistry,
}

impl<P: HubContext> Orcha<P> {
    /// Create a new Orcha activation
    pub fn new(
        storage: Arc<OrchaStorage>,
        claudecode: Arc<ClaudeCode>,
        loopback: Arc<ClaudeCodeLoopback>,
        arbor_storage: Arc<crate::activations::arbor::ArborStorage>,
        graph_runtime: Arc<GraphRuntime>,
        pm: Arc<pm::Pm>,
    ) -> Self {
        Self {
            storage,
            claudecode,
            loopback,
            arbor_storage,
            graph_runtime,
            pm,
            cancel_registry: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Register a new cancellation token for a graph, returning the receiver.
    ///
    /// If a token already exists for this `graph_id`, it is replaced.
    async fn register_cancel(&self, graph_id: &str) -> tokio::sync::watch::Receiver<bool> {
        let (tx, rx) = tokio::sync::watch::channel(false);
        self.cancel_registry.lock().await.insert(graph_id.to_string(), tx);
        rx
    }

    /// Remove the cancellation token for a graph (called on normal completion/failure).
    #[allow(dead_code)]
    async fn unregister_cancel(&self, graph_id: &str) {
        self.cancel_registry.lock().await.remove(graph_id);
    }

    /// Best-effort startup recovery for graphs that were running when the substrate
    /// last shut down.
    ///
    /// For each graph that is tracked by PM and still has `status = 'running'` in the
    /// lattice DB:
    ///   1. Any node stuck in `running` is marked failed with
    ///      "interrupted: substrate restarted" so the lattice can propagate the error.
    ///   2. Any node stuck in `ready` has a fresh `NodeReady` event emitted so the new
    ///      watcher can pick it up.
    ///   3. A new `run_graph_execution` task is spawned for the graph, reconnecting
    ///      Orcha's dispatch logic to the live event stream.
    ///
    /// This is fire-and-forget: errors are logged and skipped, never propagated.
    pub async fn recover_running_graphs(&self)
    where
        P: 'static,
    {
        use crate::activations::lattice::NodeStatus;
        use futures::StreamExt;

        let lattice_storage = self.graph_runtime.storage();

        // Find all graph IDs known to PM (started via run_tickets / build_tickets).
        let pm_graph_ids = match self.pm.list_all_graph_ids().await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!("recovery: failed to list PM graph IDs: {}", e);
                return;
            }
        };

        // Find which of those are currently 'running' in the lattice.
        let running_ids = match lattice_storage.get_running_graph_ids().await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!("recovery: failed to query running graphs: {}", e);
                return;
            }
        };

        // Intersect: only recover graphs known to PM.
        let pm_set: std::collections::HashSet<String> = pm_graph_ids.into_iter().collect();
        let to_recover: Vec<String> = running_ids
            .into_iter()
            .filter(|id| pm_set.contains(id))
            .collect();

        if to_recover.is_empty() {
            tracing::debug!("recovery: no running PM graphs to recover");
            return;
        }

        tracing::info!("recovery: recovering {} running graph(s)", to_recover.len());

        for graph_id in to_recover {
            let storage = lattice_storage.clone();
            let graph_id_clone = graph_id.clone();

            // Step 1: inspect nodes and fix up state.
            let nodes = match storage.get_nodes(&graph_id).await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!("recovery: get_nodes({}) failed: {}", graph_id, e);
                    continue;
                }
            };

            for node in &nodes {
                match node.status {
                    NodeStatus::Running => {
                        tracing::info!(
                            "recovery: re-dispatching interrupted node {} in graph {}",
                            node.id, graph_id
                        );
                        if let Err(e) = storage.reset_running_to_ready(&graph_id, &node.id).await {
                            tracing::warn!(
                                "recovery: reset_running_to_ready failed for node {} in {}: {}",
                                node.id, graph_id, e
                            );
                        }
                    }
                    NodeStatus::Ready => {
                        // Re-emit a NodeReady event so the fresh watcher dispatches it.
                        tracing::info!(
                            "recovery: re-emitting NodeReady for node {} in graph {}",
                            node.id, graph_id
                        );
                        if let Err(e) = storage.reemit_ready_nodes(&graph_id).await {
                            tracing::warn!(
                                "recovery: reemit_ready_nodes({}) failed: {}",
                                graph_id, e
                            );
                        }
                        // reemit handles all ready nodes at once; break out of per-node loop.
                        break;
                    }
                    _ => {} // Pending / Complete / Failed — no action needed.
                }
            }

            // Step 2: spawn run_graph_execution to re-attach the dispatch watcher.
            let graph = Arc::new(self.graph_runtime.open_graph(graph_id_clone.clone()));
            let cc = self.claudecode.clone();
            let arbor = self.arbor_storage.clone();
            let lb = self.loopback.storage();
            let cancel_registry = self.cancel_registry.clone();
            let pm_for_recovery = self.pm.clone();
            let graph_runtime_recovery = self.graph_runtime.clone();

            // Load persisted run config from graph metadata.
            let graph_meta = lattice_storage.get_graph(&graph_id_clone).await.ok()
                .and_then(|g| g.metadata.get("_plexus_run_config").cloned());

            let model_enum = graph_meta.as_ref()
                .and_then(|c| c.get("model"))
                .and_then(|m| m.as_str())
                .map_or(crate::activations::claudecode::Model::Sonnet, |s| match s {
                    "opus" => crate::activations::claudecode::Model::Opus,
                    "haiku" => crate::activations::claudecode::Model::Haiku,
                    _ => crate::activations::claudecode::Model::Sonnet,
                });

            let working_directory = graph_meta.as_ref()
                .and_then(|c| c.get("working_directory"))
                .and_then(|w| w.as_str())
                .unwrap_or("/workspace")
                .to_string();

            // Load persisted ticket map and invert to node_id → ticket_id.
            let node_to_ticket: std::collections::HashMap<String, String> = pm_for_recovery
                .get_ticket_map(&graph_id_clone)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|(ticket_id, node_id)| (node_id, ticket_id))
                .collect();

            tokio::spawn(async move {
                tracing::info!("recovery: spawning run_graph_execution for {}", graph_id_clone);
                // Register a cancel token so this recovered graph can be cancelled.
                let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
                cancel_registry.lock().await.insert(graph_id_clone.clone(), cancel_tx);

                let execution = graph_runner::run_graph_execution(
                    graph,
                    cc,
                    arbor,
                    lb,
                    pm_for_recovery,
                    graph_runtime_recovery,
                    cancel_registry.clone(),
                    model_enum,
                    working_directory,
                    cancel_rx,
                    node_to_ticket,
                );
                tokio::pin!(execution);
                while let Some(_event) = execution.next().await {}
                cancel_registry.lock().await.remove(&graph_id_clone);
                tracing::info!("recovery: graph {} execution complete", graph_id_clone);
            });
        }
    }
}

/// Internal helper: watch one graph's lattice events and forward them as `OrchaEvents` into `tx`.
///
/// Used by `watch_graph_tree` to multiplex root + all child graphs into one channel.
async fn watch_single_graph(
    gid: String,
    after_seq: Option<u64>,
    graph_runtime: Arc<GraphRuntime>,
    pm: Arc<pm::Pm>,
    tx: tokio::sync::mpsc::UnboundedSender<OrchaEvent>,
) {
    fn calc_pct(complete: usize, total: usize) -> Option<u32> {
        if total == 0 {
            None
        } else {
            Some((complete as f32 / total as f32 * 100.0) as u32)
        }
    }

    let graph = graph_runtime.open_graph(gid.clone());
    let node_to_ticket: HashMap<String, String> = pm
        .get_ticket_map(&gid)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(ticket_id, node_id)| (node_id, ticket_id))
        .collect();

    let total_nodes = graph.count_nodes().await.unwrap_or(0);
    let mut complete_nodes: usize = 0;

    let event_stream = graph.watch(after_seq);
    tokio::pin!(event_stream);

    while let Some(crate::activations::lattice::LatticeEventEnvelope { event, .. }) =
        event_stream.next().await
    {
        let evt = match event {
            crate::activations::lattice::LatticeEvent::NodeReady { node_id, .. } => {
                let ticket_id = node_to_ticket.get(&node_id).cloned();
                Some(OrchaEvent::NodeStarted {
                    node_id,
                    label: None,
                    ticket_id,
                    percentage: calc_pct(complete_nodes, total_nodes),
                })
            }
            crate::activations::lattice::LatticeEvent::NodeStarted { .. } => None,
            crate::activations::lattice::LatticeEvent::NodeDone { node_id, .. } => {
                complete_nodes += 1;
                let ticket_id = node_to_ticket.get(&node_id).cloned();
                Some(OrchaEvent::NodeComplete {
                    node_id,
                    label: None,
                    ticket_id,
                    output_summary: None,
                    percentage: calc_pct(complete_nodes, total_nodes),
                })
            }
            crate::activations::lattice::LatticeEvent::NodeFailed { node_id, error } => {
                complete_nodes += 1;
                let ticket_id = node_to_ticket.get(&node_id).cloned();
                Some(OrchaEvent::NodeFailed {
                    node_id,
                    label: None,
                    ticket_id,
                    error,
                    percentage: calc_pct(complete_nodes, total_nodes),
                })
            }
            crate::activations::lattice::LatticeEvent::GraphDone { graph_id } => {
                Some(OrchaEvent::Complete { session_id: graph_id })
            }
            crate::activations::lattice::LatticeEvent::GraphFailed {
                graph_id,
                node_id,
                error,
            } => Some(OrchaEvent::Failed {
                session_id: graph_id,
                error: format!("Node {node_id} failed: {error}"),
            }),
        };
        if let Some(e) = evt {
            if tx.send(e).is_err() {
                break;
            }
        }
    }
}

#[plexus_macros::activation(namespace = "orcha",
version = "1.0.0",
description = "Full task orchestration with approval loops and validation")]
impl<P: HubContext> Orcha<P> {
    /// Project management subsystem.
    #[plexus_macros::child]
    fn pm(&self) -> pm::Pm {
        (*self.pm).clone()
    }

    /// Run a complete orchestration task
    ///
    /// This is the main entry point for running tasks with the full orcha pattern:
    /// - Creates sessions
    /// - Runs task with approval handling
    /// - Extracts and executes validation
    /// - Auto-retries on failure
    #[plexus_macros::method]
    async fn run_task(
        &self,
        request: RunTaskRequest,
    ) -> impl Stream<Item = OrchaEvent> + Send + 'static {
        run_orchestration_task(
            self.storage.clone(),
            self.arbor_storage.clone(),
            self.claudecode.clone(),
            self.loopback.clone(),
            request,
            None, // Let orchestrator generate session_id
        ).await
    }
    /// Create a new orchestration session
    ///
    /// Creates a session record to track orchestration state. The client should
    /// then create a corresponding claudecode session with loopback enabled.
    #[plexus_macros::method]
    async fn create_session(
        &self,
        request: CreateSessionRequest,
    ) -> Result<SessionCreated, OrchaError> {
        // Generate unique session ID
        let session_id = format!("orcha-{}", Uuid::new_v4());

        // Determine agent mode
        let agent_mode = if request.multi_agent {
            AgentMode::Multi
        } else {
            AgentMode::Single
        };

        // Create session in storage
        let session = self.storage.create_session(
            session_id.clone(),
            request.model.clone(),
            request.working_directory.clone(),
            request.rules.clone(),
            request.max_retries,
            agent_mode,
            None, // tree_id (created by run_orchestration_task)
        ).await.map_err(|e| OrchaError::storage("create_session", e))?;

        Ok(SessionCreated {
            session_id,
            created_at: session.created_at,
        })
    }

    /// Update session state
    ///
    /// Called by the client to update the current state of the session
    #[plexus_macros::method]
    async fn update_session_state(
        &self,
        session_id: SessionId,
        state: SessionState,
    ) -> Result<(), OrchaError> {
        self.storage
            .update_state(&session_id, state)
            .await
            .map_err(|e| OrchaError::storage("update_state", e))?;
        Ok(())
    }

    /// Get session information
    #[plexus_macros::method]
    async fn get_session(
        &self,
        request: GetSessionRequest,
    ) -> Result<SessionInfo, OrchaError> {
        self.storage
            .get_session(&request.session_id)
            .await
            .map_err(|_| OrchaError::SessionNotFound {
                session_id: request.session_id,
            })
    }

    /// Extract validation artifact from text
    ///
    /// Scans text for {"`orcha_validate"`: {...}} pattern and extracts test command
    ///
    /// `Ok(None)` is the old `NotFound` variant: no artifact present is a normal
    /// outcome, not a failure, so it stays on the success side of the `Result`.
    #[plexus_macros::method]
    async fn extract_validation(
        &self,
        text: String,
    ) -> Result<Option<ValidationArtifact>, OrchaError> {
        Ok(extract_validation_artifact(&text))
    }

    /// Run a validation test
    ///
    /// Executes a test command and returns the result
    #[plexus_macros::method]
    async fn run_validation(
        &self,
        artifact: ValidationArtifact,
    ) -> Result<ValidationResult, OrchaError> {
        Ok(run_validation_test(&artifact).await)
    }

    /// Increment retry counter for a session
    ///
    /// Called when validation fails and the client wants to retry
    #[plexus_macros::method]
    async fn increment_retry(
        &self,
        session_id: SessionId,
    ) -> Result<RetryStatus, OrchaError> {
        let retry_count = self
            .storage
            .increment_retry(&session_id)
            .await
            .map_err(|e| OrchaError::storage("increment_retry", e))?;

        let max_retries = match self.storage.get_session(&session_id).await {
            Ok(s) => s.max_retries,
            Err(e) => {
                tracing::warn!("Failed to get session {} for max_retries lookup: {}", session_id, e);
                3
            }
        };

        Ok(RetryStatus {
            retry_count,
            max_retries,
            exceeded: retry_count >= max_retries,
        })
    }

    /// List all sessions
    #[plexus_macros::method]
    async fn list_sessions(&self) -> Result<Vec<SessionInfo>, OrchaError> {
        Ok(self.storage.list_sessions().await)
    }

    /// Delete a session
    #[plexus_macros::method]
    async fn delete_session(&self, session_id: SessionId) -> Result<(), OrchaError> {
        self.storage
            .delete_session(&session_id)
            .await
            .map_err(|e| OrchaError::storage("delete_session", e))
    }

    /// Run a task asynchronously - returns immediately with `session_id`
    ///
    /// Like `run_task` but non-blocking. Returns the `session_id` immediately
    /// and the task runs in the background. Use `check_status` or `get_session`
    /// to check on progress.
    #[plexus_macros::method]
    async fn run_task_async(
        &self,
        request: RunTaskRequest,
    ) -> Result<SessionRef, OrchaError> {
        let storage = self.storage.clone();
        let arbor_storage = self.arbor_storage.clone();
        let claudecode = self.claudecode.clone();
        let loopback = self.loopback.clone();

        // Generate session ID that will be used by the orchestrator
        let session_id = format!("orcha-{}", Uuid::new_v4());
        let session_id_for_spawn = session_id.clone();

        // Spawn the orchestration task in the background
        let req = request.clone();
        tokio::spawn(async move {
            let stream = run_orchestration_task(
                storage,
                arbor_storage,
                claudecode,
                loopback,
                req,
                Some(session_id_for_spawn), // Pass the session_id to orchestrator
            ).await;
            tokio::pin!(stream);

            // Consume the stream in the background
            while let Some(_event) = stream.next().await {
                // Events are discarded in async mode
                // Use get_session or check_status to monitor
            }
        });

        // Return immediately with session_id
        Ok(SessionRef { session_id })
    }

    /// List all orcha monitor trees
    ///
    /// Returns all arbor trees created by orcha for monitoring sessions
    #[plexus_macros::method]
    async fn list_monitor_trees(&self) -> Result<Vec<MonitorTreeInfo>, OrchaError> {
        let arbor_storage = self.arbor_storage.clone();

        // Query arbor for trees with metadata type="orcha_monitor"
        let filter = serde_json::json!({"type": "orcha_monitor"});

        // A query failure is reported today as an empty list, not an error;
        // preserved verbatim (T3).
        let Ok(tree_ids) = arbor_storage.tree_query_by_metadata(&filter).await else {
            return Ok(vec![]);
        };

        let mut trees = Vec::new();

        // Get metadata for each tree
        for tree_id in tree_ids {
            if let Ok(tree) = arbor_storage.tree_get(&tree_id).await {
                if let Some(metadata) = &tree.metadata {
                    let session_id = metadata.get("session_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let tree_path = metadata.get("tree_path")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();

                    trees.push(MonitorTreeInfo {
                        tree_id: tree.id.to_string(),
                        session_id,
                        tree_path,
                    });
                }
            }
        }

        Ok(trees)
    }

    /// Check status of a running session by asking Claude to summarize
    ///
    /// Creates an ephemeral forked session to generate a summary of what's happening,
    /// and saves the summary to an arbor monitoring tree for historical tracking.
    #[plexus_macros::method]
    async fn check_status(
        &self,
        request: CheckStatusRequest,
    ) -> Result<StatusSummary, OrchaError> {
        let claudecode = self.claudecode.clone();
        let arbor_storage = self.arbor_storage.clone();
        let storage = self.storage.clone();
        let session_id = request.session_id;

        {
            // First, get the actual session state from storage
            let session_info = storage.get_session(&session_id).await.map_err(|_| {
                OrchaError::SessionNotFound { session_id: session_id.clone() }
            })?;

            // Branch based on agent mode
            if session_info.agent_mode == AgentMode::Multi {
                // Multi-agent status check
                let agents = storage
                    .list_agents(&session_id)
                    .await
                    .map_err(|e| OrchaError::storage("list_agents", e))?;

                if agents.is_empty() {
                    return Err(OrchaError::OrchestrationError {
                        detail: "No agents found in session".to_string(),
                    });
                }

                // Generate summaries for each agent in parallel
                let summary_futures: Vec<_> = agents.iter().map(|agent| {
                    generate_agent_summary(&claudecode, &arbor_storage, agent.clone())
                }).collect();

                let agent_summaries: Vec<AgentSummary> = futures::future::join_all(summary_futures)
                    .await
                    .into_iter()
                    .filter_map(|r| match r {
                        Ok(summary) => Some(summary),
                        Err(e) => {
                            tracing::warn!("Failed to generate agent summary: {}", e);
                            None
                        }
                    })
                    .collect();

                // Generate overall meta-summary
                let overall_summary = generate_overall_summary(
                    &claudecode,
                    &session_id,
                    &agent_summaries,
                ).await;

                let summary = overall_summary.unwrap_or_else(|| "Unable to generate summary".to_string());

                // Save to arbor monitoring tree — a save failure is logged, never
                // propagated: the summary is still returned (T3, verbatim).
                if let Err(e) = save_status_summary_to_arbor(&arbor_storage, &session_id, &summary).await {
                    tracing::warn!("Failed to save summary to arbor: {}", e);
                }

                return Ok(StatusSummary { summary, agent_summaries });
            }

            // Single-agent mode (original logic below)


            // Format session state as context for Claude and extract stream_id for arbor lookup
            let (state_description, stream_id_opt) = match &session_info.state {
                SessionState::Idle => ("idle (not currently executing)".to_string(), None),
                SessionState::Running { stream_id, sequence, active_agents, completed_agents, failed_agents } => {
                    let agent_info = if *active_agents > 0 || *completed_agents > 0 || *failed_agents > 0 {
                        format!(" (agents: {active_agents} active, {completed_agents} complete, {failed_agents} failed)")
                    } else {
                        String::new()
                    };
                    (format!("running (stream: {stream_id}, sequence: {sequence}{agent_info})"), Some(stream_id.clone()))
                }
                SessionState::WaitingApproval { approval_id } => {
                    (format!("waiting for approval (approval_id: {approval_id})"), None)
                }
                SessionState::Validating { test_command } => {
                    (format!("validating with command: {test_command}"), None)
                }
                SessionState::Complete => ("completed successfully".to_string(), None),
                SessionState::Failed { error } => {
                    (format!("failed with error: {error}"), None)
                }
            };

            // Try to get the conversation tree from claudecode if we have a stream_id
            let conversation_context = if let Some(stream_id) = stream_id_opt {
                // Get the claudecode session to find its arbor tree
                match claudecode.storage.session_get_by_name(&stream_id).await {
                    Ok(cc_session) => {
                        // Get and render the arbor tree as a formatted conversation
                        match arbor_storage.tree_get(&cc_session.head.tree_id).await {
                            Ok(tree) => {
                                let formatted = format_conversation_from_tree(&tree);
                                Some(formatted)
                            }
                            Err(e) => {
                                tracing::warn!("Failed to get arbor tree for claudecode session {}: {}", stream_id, e);
                                None
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to get claudecode session {}: {}", stream_id, e);
                        None
                    }
                }
            } else {
                None
            };

            // Create an ephemeral session to ask for a summary
            let summary_session = format!("orcha-check-{}", Uuid::new_v4());
            let summary_session_id = format!("{}-check-{}", session_id, Uuid::new_v4());

            // Create the session - using Haiku for fast, cheap summaries
            let create_stream = claudecode.create(
                summary_session.clone(),
                "/workspace".to_string(), // Default, doesn't matter for ephemeral
                crate::activations::claudecode::Model::Haiku,
                None,
                Some(false), // No loopback needed for summary
                Some(summary_session_id), // Track ephemeral session under parent
            ).await;
            tokio::pin!(create_stream);

            let mut create_ok = false;
            while let Some(result) = create_stream.next().await {
                if let crate::activations::claudecode::CreateResult::Ok { .. } = result {
                    create_ok = true;
                }
            }

            if !create_ok {
                return Err(OrchaError::OrchestrationError {
                    detail: "Failed to create summary session".to_string(),
                });
            }

            // Ask Claude to summarize the session with actual context
            let prompt = if let Some(conversation) = conversation_context {
                format!(
                    "An orcha orchestration session has the following status:\n\n\
                     - Session ID: {}\n\
                     - Model: {}\n\
                     - State: {}\n\
                     - Retry count: {}/{}\n\
                     - Created at: {} (unix timestamp)\n\
                     - Last activity: {} (unix timestamp)\n\n\
                     Here is the actual conversation tree showing what the agent has been doing:\n\n\
                     {}\n\n\
                     Generate a brief, natural language summary (2-3 sentences) of what's happening in this session.\n\
                     Focus on what the agent is currently doing or has accomplished. Be specific about the task.",
                    session_id,
                    session_info.model,
                    state_description,
                    session_info.retry_count,
                    session_info.max_retries,
                    session_info.created_at,
                    session_info.last_activity,
                    conversation
                )
            } else {
                format!(
                    "An orcha orchestration session has the following status:\n\n\
                     - Session ID: {}\n\
                     - Model: {}\n\
                     - State: {}\n\
                     - Retry count: {}/{}\n\
                     - Created at: {} (unix timestamp)\n\
                     - Last activity: {} (unix timestamp)\n\n\
                     Generate a brief, natural language summary (2-3 sentences) of what's happening in this session.\n\
                     Focus on the current state and what the agent is doing or has done.",
                    session_id,
                    session_info.model,
                    state_description,
                    session_info.retry_count,
                    session_info.max_retries,
                    session_info.created_at,
                    session_info.last_activity
                )
            };

            let chat_stream = claudecode.chat(
                summary_session.clone(),
                prompt,
                Some(true), // Ephemeral - don't save to history
                None,
            ).await;
            tokio::pin!(chat_stream);

            let mut summary = String::new();
            while let Some(event) = chat_stream.next().await {
                if let crate::activations::claudecode::ChatEvent::Content { text } = event {
                    summary.push_str(&text);
                }
            }

            if summary.is_empty() {
                return Err(OrchaError::OrchestrationError {
                    detail: "Failed to generate summary".to_string(),
                });
            }

            // Save summary to arbor monitoring tree.  Still return the summary
            // even if the arbor save fails (T3, verbatim).
            if let Err(e) = save_status_summary_to_arbor(&arbor_storage, &session_id, &summary).await {
                tracing::warn!("Failed to save summary to arbor: {}", e);
            }

            Ok(StatusSummary {
                summary,
                agent_summaries: vec![],  // Single-agent mode
            })
        }
    }

    /// Spawn a new agent in an existing session (multi-agent mode)
    ///
    /// Creates a new `ClaudeCode` session and tracks it as an agent within the orcha session.
    /// Can be called explicitly via API or by agents themselves requesting helpers.
    #[plexus_macros::method]
    async fn spawn_agent(
        &self,
        request: SpawnAgentRequest,
    ) -> Result<SpawnedAgent, OrchaError> {
        let storage = self.storage.clone();
        let claudecode = self.claudecode.clone();
        let loopback = self.loopback.clone();

        {
            // Verify session exists and is in multi-agent mode
            let session = storage.get_session(&request.session_id).await.map_err(|_| {
                OrchaError::SessionNotFound { session_id: request.session_id.clone() }
            })?;

            if session.agent_mode != AgentMode::Multi {
                return Err(OrchaError::OrchestrationError {
                    detail: "Session is not in multi-agent mode".to_string(),
                });
            }

            // Parse model
            let model = match session.model.as_str() {
                "opus" => crate::activations::claudecode::Model::Opus,
                "sonnet" => crate::activations::claudecode::Model::Sonnet,
                "haiku" => crate::activations::claudecode::Model::Haiku,
                _ => crate::activations::claudecode::Model::Sonnet,
            };

            // Create ClaudeCode session for this agent
            let cc_session_name = format!("orcha-agent-{}", Uuid::new_v4());
            let agent_session_id = format!("{}-agent-{}", session.session_id, Uuid::new_v4());

            let create_stream = claudecode.create(
                cc_session_name.clone(),
                "/workspace".to_string(),  // TODO: Get from session
                model,
                None,
                Some(true), // Loopback enabled
                Some(agent_session_id), // Track agent under parent session
            ).await;
            tokio::pin!(create_stream);

            let mut create_ok = false;
            while let Some(result) = create_stream.next().await {
                if let crate::activations::claudecode::CreateResult::Ok { .. } = result {
                    create_ok = true;
                    break;
                }
            }

            if !create_ok {
                return Err(OrchaError::OrchestrationError {
                    detail: "Failed to create ClaudeCode session".to_string(),
                });
            }

            // Create agent record
            let agent = storage.create_agent(
                &request.session_id,
                cc_session_name.clone(),
                request.subtask.clone(),
                false, // Not primary
                request.parent_agent_id,
            ).await.map_err(|e| OrchaError::storage("create_agent", e))?;

            // Spawn background task to run this agent
            let config = super::orchestrator::AgentConfig {
                model,
                working_directory: "/workspace".to_string(),
                max_retries: session.max_retries,
                task_context: request.subtask.clone(),
                auto_approve: true, // TODO: Store in session and retrieve
            };

            super::orchestrator::spawn_agent_task(
                storage.clone(),
                claudecode.clone(),
                loopback.clone(),
                agent.clone(),
                request.subtask.clone(),
                config,
            );

            Ok(SpawnedAgent {
                agent_id: agent.agent_id,
                claudecode_session_id: cc_session_name,
            })
        }
    }

    /// List all agents in a session
    #[plexus_macros::method]
    async fn list_agents(
        &self,
        request: ListAgentsRequest,
    ) -> Result<Vec<AgentInfo>, OrchaError> {
        self.storage
            .list_agents(&request.session_id)
            .await
            .map_err(|e| OrchaError::storage("list_agents", e))
    }

    /// Get specific agent info
    #[plexus_macros::method]
    async fn get_agent(
        &self,
        request: GetAgentRequest,
    ) -> Result<AgentInfo, OrchaError> {
        self.storage
            .get_agent(&request.agent_id)
            .await
            .map_err(|e| OrchaError::storage("get_agent", e))
    }

    /// List pending approval requests for a session
    ///
    /// Returns all approval requests awaiting manual approval.
    /// Only relevant when `auto_approve` is disabled.
    #[plexus_macros::method]
    async fn list_pending_approvals(
        &self,
        request: ListApprovalsRequest,
    ) -> Result<Vec<ApprovalInfo>, OrchaError> {
        let loopback = self.loopback.clone();
        let session_id = request.session_id;

        let approvals = loopback
            .storage()
            .list_pending(Some(&session_id))
            .await
            .map_err(|e| OrchaError::storage("list_pending", e))?;

        Ok(approvals
            .into_iter()
            .map(|approval| ApprovalInfo {
                approval_id: approval.id.to_string(),
                session_id: approval.session_id,
                tool_name: approval.tool_name,
                tool_use_id: approval.tool_use_id,
                tool_input: approval.input,
                created_at: chrono::DateTime::from_timestamp(approval.created_at, 0).map_or_else(|| approval.created_at.to_string(), |dt| dt.to_rfc3339()),
            })
            .collect())
    }

    /// Approve a pending request
    ///
    /// Approves a tool use request and unblocks the waiting agent.
    /// The `approval_id` comes from `list_pending_approvals`.
    #[plexus_macros::method]
    async fn approve_request(
        &self,
        request: ApproveRequest,
    ) -> Result<ApprovalOutcome, OrchaError> {
        let loopback = self.loopback.clone();
        let approval_id = request.approval_id.clone();
        let message = request.message;

        let uuid_id = uuid::Uuid::parse_str(&approval_id).map_err(|_| {
            OrchaError::ValidationError {
                detail: format!("Invalid approval_id format: {approval_id}"),
            }
        })?;

        loopback
            .storage()
            .resolve_approval(&uuid_id, true, message.clone())
            .await
            .map_err(|e| OrchaError::storage("resolve_approval", e))?;

        Ok(ApprovalOutcome {
            approval_id,
            message: Some("Approved".to_string()),
        })
    }

    /// Deny a pending request
    ///
    /// Denies a tool use request. The agent will receive an error
    /// and may adapt or fail depending on its error handling.
    #[plexus_macros::method]
    async fn deny_request(
        &self,
        request: DenyRequest,
    ) -> Result<ApprovalOutcome, OrchaError> {
        let loopback = self.loopback.clone();
        let approval_id = request.approval_id.clone();
        let reason = request.reason;

        let uuid_id = uuid::Uuid::parse_str(&approval_id).map_err(|_| {
            OrchaError::ValidationError {
                detail: format!("Invalid approval_id format: {approval_id}"),
            }
        })?;

        loopback
            .storage()
            .resolve_approval(&uuid_id, false, reason.clone())
            .await
            .map_err(|e| OrchaError::storage("resolve_approval", e))?;

        Ok(ApprovalOutcome {
            approval_id,
            message: reason.or(Some("Denied".to_string())),
        })
    }

    /// Subscribe to pending approval requests for a graph — push stream for human-in-the-loop UIs.
    ///
    /// Unlike `list_pending_approvals` which is a snapshot, this method streams
    /// `ApprovalPending` events whenever a new approval arrives for the graph.
    /// Use this to drive a UI that shows "Claude wants to run Bash — approve?".
    ///
    /// The stream yields all currently-pending approvals immediately, then waits
    /// for new ones via a `Notify`-based wake-up. Closes after `timeout_secs`
    /// of silence (default: 300 seconds).
    #[plexus_macros::method(params(
        graph_id = "Graph ID to watch for approval requests",
        timeout_secs = "How long to wait before closing (default: 300)"
    ))]
    async fn subscribe_approvals(
        &self,
        graph_id: String,
        timeout_secs: Option<u64>,
    ) -> impl Stream<Item = OrchaEvent> + Send + 'static {
        let loopback_storage = self.loopback.storage();
        let timeout = std::time::Duration::from_secs(timeout_secs.unwrap_or(300));

        stream! {
            let notifier = loopback_storage.get_or_create_notifier(&graph_id);
            let deadline = std::time::Instant::now() + timeout;
            let mut seen_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

            loop {
                // Yield all currently-pending approvals for this graph (skip already-sent ones)
                match loopback_storage.list_pending(Some(&graph_id)).await {
                    Ok(approvals) => {
                        for approval in approvals {
                            let id_str = approval.id.to_string();
                            if seen_ids.contains(&id_str) {
                                continue;
                            }
                            seen_ids.insert(id_str);
                            let created_at = chrono::DateTime::from_timestamp(approval.created_at, 0).map_or_else(|| approval.created_at.to_string(), |dt| dt.to_rfc3339());
                            yield OrchaEvent::ApprovalPending {
                                approval_id: approval.id.to_string(),
                                graph_id: graph_id.clone(),
                                tool_name: approval.tool_name,
                                tool_input: approval.input,
                                created_at,
                            };
                        }
                    }
                    Err(e) => {
                        tracing::warn!("subscribe_approvals: failed to list pending: {}", e);
                    }
                }

                // Wait for a new approval notification or timeout
                let remaining = match deadline.checked_duration_since(std::time::Instant::now()) {
                    Some(d) => d,
                    None => break, // timeout expired
                };

                tokio::select! {
                    _ = notifier.notified() => {
                        // New approval arrived — loop to re-list and yield
                    }
                    _ = tokio::time::sleep(remaining) => {
                        // Timeout reached — close stream
                        break;
                    }
                }
            }
        }
    }

    /// Execute a lattice graph — dispatches nodes by type using Orcha's execution logic.
    ///
    /// Node types:
    /// - `"task"`: run a `ClaudeCode` session with `spec.data.task` as the prompt
    /// - `"synthesize"`: like task, with optional prior-work context from `spec.handle`
    /// - `"validate"`: run a shell command from `spec.data.command`
    ///
    /// Streams `OrchaEvent` progress events until the graph completes or fails.
    #[plexus_macros::method(params(
        graph_id = "ID of the lattice graph to execute",
        model = "Model for task nodes: opus, sonnet, haiku (default: sonnet)",
        working_directory = "Working directory for task nodes (default: /workspace)"
    ))]
    async fn run_graph(
        &self,
        graph_id: String,
        model: Option<String>,
        working_directory: Option<String>,
    ) -> impl Stream<Item = OrchaEvent> + Send + 'static {
        let model_enum = match model.as_deref().unwrap_or("sonnet") {
            "opus" => Model::Opus,
            "haiku" => Model::Haiku,
            _ => Model::Sonnet,
        };
        let wd = working_directory.unwrap_or_else(|| "/workspace".to_string());

        let cancel_rx = self.register_cancel(&graph_id).await;
        let cancel_registry = self.cancel_registry.clone();
        let graph = Arc::new(self.graph_runtime.open_graph(graph_id.clone()));
        let claudecode = self.claudecode.clone();
        let arbor_storage = self.arbor_storage.clone();
        let loopback_storage = self.loopback.storage();
        let pm = self.pm.clone();
        let graph_runtime = self.graph_runtime.clone();
        stream! {
            let execution = graph_runner::run_graph_execution(
                graph,
                claudecode,
                arbor_storage,
                loopback_storage,
                pm,
                graph_runtime,
                cancel_registry.clone(),
                model_enum,
                wd,
                cancel_rx,
                std::collections::HashMap::new(),
            );
            tokio::pin!(execution);
            while let Some(event) = execution.next().await {
                yield event;
            }
            cancel_registry.lock().await.remove(&graph_id);
        }
    }

    /// Run a complete orchestration task driven by a single planning prompt.
    ///
    /// This is the single-call counterpart to the three-step sequence:
    /// `create_graph` → `add_plan_node` → `run_graph`.
    ///
    /// A Plan node is created that asks Claude to generate and execute a child
    /// graph from the supplied `task` description. Streams `OrchaEvent` progress
    /// events until the graph completes or fails.
    #[plexus_macros::method(params(
        task = "Natural-language task — passed directly to Claude as the planning prompt",
        model = "Model for all nodes: opus, sonnet, haiku (default: sonnet)",
        working_directory = "Working directory for task nodes (default: /workspace)"
    ))]
    async fn run_plan(
        &self,
        task: String,
        model: Option<String>,
        working_directory: Option<String>,
    ) -> impl Stream<Item = OrchaEvent> + Send + 'static {
        let model_str = model.as_deref().unwrap_or("sonnet").to_string();
        let model_enum = match model_str.as_str() {
            "opus" => Model::Opus,
            "haiku" => Model::Haiku,
            _ => Model::Sonnet,
        };
        let wd = working_directory.unwrap_or_else(|| "/workspace".to_string());
        let graph_runtime = self.graph_runtime.clone();
        let cancel_registry = self.cancel_registry.clone();
        let claudecode = self.claudecode.clone();
        let arbor = self.arbor_storage.clone();
        let lb = self.loopback.storage();
        let pm = self.pm.clone();

        stream! {
            let metadata = serde_json::json!({
                "_plexus_run_config": {
                    "model": model_str,
                    "working_directory": wd,
                }
            });
            let graph = match graph_runtime.create_graph(metadata).await {
                Ok(g) => Arc::new(g),
                Err(e) => {
                    yield OrchaEvent::Failed { session_id: String::new(), error: e };
                    return;
                }
            };
            let graph_id = graph.graph_id.clone();

            let node_id = match graph.add_plan(task.clone()).await {
                Ok(id) => id,
                Err(e) => {
                    yield OrchaEvent::Failed { session_id: graph_id, error: e };
                    return;
                }
            };

            let ticket_map =
                std::collections::HashMap::from([("plan".to_string(), node_id.clone())]);
            let _ = pm.save_ticket_map(&graph_id, &ticket_map).await;
            let _ = pm.save_ticket_source(&graph_id, &task).await;

            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
            cancel_registry.lock().await.insert(graph_id.clone(), cancel_tx);

            let node_to_ticket =
                std::collections::HashMap::from([(node_id, "plan".to_string())]);
            let execution = graph_runner::run_graph_execution(
                graph, claudecode, arbor, lb, pm,
                graph_runtime, cancel_registry.clone(),
                model_enum, wd, cancel_rx, node_to_ticket,
            );
            tokio::pin!(execution);
            while let Some(event) = execution.next().await {
                yield event;
            }
            cancel_registry.lock().await.remove(&graph_id);
        }
    }

    /// Stop a running graph and all its agent tasks.
    ///
    /// Sends a cancellation signal to all node tasks currently executing within the
    /// graph. Each task abandons its chat stream and returns an error, which causes
    /// the lattice to mark those nodes as failed and ultimately fail the graph.
    ///
    /// If the graph is not currently running (no cancel token registered), yields
    /// `OrchaEvent::Failed` with a "not found" error.
    #[plexus_macros::method(params(
        graph_id = "Lattice graph ID to cancel"
    ))]
    async fn cancel_graph(&self, graph_id: String) -> Result<GraphRef, OrchaError> {
        let cancel_registry = self.cancel_registry.clone();
        let lattice_storage = self.graph_runtime.storage();

        // BFS to collect the root graph and all descendant graph IDs.
        let mut all_graph_ids: Vec<String> = Vec::new();
        let mut to_visit: std::collections::VecDeque<String> = std::collections::VecDeque::new();
        to_visit.push_back(graph_id.clone());
        while let Some(gid) = to_visit.pop_front() {
            all_graph_ids.push(gid.clone());
            if let Ok(children) = lattice_storage.get_child_graphs(&gid).await {
                for child in children {
                    to_visit.push_back(child.id);
                }
            }
        }

        // Lock the registry once and cancel all collected graphs.
        let mut registry = cancel_registry.lock().await;
        let root_cancelled = registry.contains_key(&graph_id);
        for gid in all_graph_ids {
            if let Some(cancel_tx) = registry.remove(&gid) {
                let _ = cancel_tx.send(true);
            }
        }

        if root_cancelled {
            Ok(GraphRef { graph_id })
        } else {
            Err(OrchaError::OrchestrationError {
                detail: "Graph not found in cancel registry (not running or already finished)".to_string(),
            })
        }
    }

    /// Subscribe to execution events for a graph — reconnectable observation stream.
    ///
    /// Replays all persisted events from `after_seq` (exclusive) and then tails
    /// live events until the graph reaches `GraphDone` or `GraphFailed`.
    ///
    /// Use `after_seq = 0` (or omit) to start from the beginning.  On reconnect,
    /// pass the last sequence number seen to resume without gaps.
    ///
    /// This is observation only — no nodes are dispatched.  Pair with
    /// `run_tickets_async` or `run_graph` (spawned) to drive execution.
    ///
    /// Client workflow:
    /// ```text
    /// graph_id = run_tickets_async(...)     # fires and forgets
    /// events   = subscribe_graph(graph_id)  # observe
    /// # on disconnect:
    /// events   = subscribe_graph(graph_id, after_seq=last_seen_seq)
    /// ```
    #[plexus_macros::method(params(
        graph_id = "Lattice graph ID from run_tickets_async or build_tickets",
        after_seq = "Sequence number to resume from (0 or omit to start from beginning)"
    ))]
    async fn subscribe_graph(
        &self,
        graph_id: String,
        after_seq: Option<u64>,
    ) -> impl Stream<Item = OrchaEvent> + Send + 'static {
        let graph = self.graph_runtime.open_graph(graph_id.clone());
        let pm = self.pm.clone();
        stream! {
            // Load the ticket_id→node_id map from pm storage and invert it to node_id→ticket_id.
            let node_to_ticket: HashMap<String, String> = pm.get_ticket_map(&graph_id).await
                .unwrap_or_default()
                .into_iter()
                .map(|(ticket_id, node_id)| (node_id, ticket_id))
                .collect();

            let event_stream = graph.watch(after_seq);
            tokio::pin!(event_stream);

            // Progress tracking: count total nodes once, then track completions.
            let total_nodes: usize = graph.count_nodes().await.unwrap_or(0);
            let mut complete_nodes: usize = 0;

            fn calc_percentage(complete: usize, total: usize) -> Option<u32> {
                if total == 0 { return None; }
                Some((complete as f32 / total as f32 * 100.0) as u32)
            }

            while let Some(crate::activations::lattice::LatticeEventEnvelope { event, .. }) = event_stream.next().await {
                match event {
                    crate::activations::lattice::LatticeEvent::NodeReady { node_id, .. } => {
                        let ticket_id = node_to_ticket.get(&node_id).cloned();
                        yield OrchaEvent::NodeStarted {
                            node_id,
                            label: None,
                            ticket_id,
                            percentage: calc_percentage(complete_nodes, total_nodes),
                        };
                    }
                    crate::activations::lattice::LatticeEvent::NodeStarted { .. } => {
                        // Already emitted NodeStarted on NodeReady; suppress duplicate.
                    }
                    crate::activations::lattice::LatticeEvent::NodeDone { node_id, .. } => {
                        complete_nodes += 1;
                        let ticket_id = node_to_ticket.get(&node_id).cloned();
                        yield OrchaEvent::NodeComplete {
                            node_id,
                            label: None,
                            ticket_id,
                            output_summary: None,
                            percentage: calc_percentage(complete_nodes, total_nodes),
                        };
                    }
                    crate::activations::lattice::LatticeEvent::NodeFailed { node_id, error } => {
                        complete_nodes += 1;
                        let ticket_id = node_to_ticket.get(&node_id).cloned();
                        yield OrchaEvent::NodeFailed {
                            node_id,
                            label: None,
                            ticket_id,
                            error,
                            percentage: calc_percentage(complete_nodes, total_nodes),
                        };
                    }
                    crate::activations::lattice::LatticeEvent::GraphDone { graph_id } => {
                        yield OrchaEvent::Complete { session_id: graph_id };
                        return;
                    }
                    crate::activations::lattice::LatticeEvent::GraphFailed { graph_id, node_id, error } => {
                        yield OrchaEvent::Failed {
                            session_id: graph_id,
                            error: format!("Node {node_id} failed: {error}"),
                        };
                        return;
                    }
                }
            }
        }
    }

    /// Like `subscribe_graph` but recursively follows child graphs created by Plan nodes,
    /// multiplexing all events into one stream. Ends only when the ROOT graph completes or
    /// fails.
    ///
    /// When a Plan node runs, it creates a child graph and executes it.
    /// `subscribe_graph` only sees the Plan node completing — all of the child graph's
    /// NodeStarted/NodeComplete/NodeFailed events are invisible to the caller.
    /// `watch_graph_tree` fixes this by polling for newly created child graphs every 500 ms
    /// and subscribing to each one as it appears.
    #[plexus_macros::method(params(
        graph_id = "Root graph ID to watch (recursively includes all child graphs)",
        after_seq = "Sequence number for the root graph to resume from (0 or omit)"
    ))]
    async fn watch_graph_tree(
        &self,
        graph_id: String,
        after_seq: Option<u64>,
    ) -> impl Stream<Item = OrchaEvent> + Send + 'static {
        let graph_runtime = self.graph_runtime.clone();
        let pm = self.pm.clone();
        let root_id = graph_id;
        stream! {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<OrchaEvent>();
            let known_ids: Arc<tokio::sync::Mutex<std::collections::HashSet<String>>> =
                Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));

            // Spawn watcher for the root graph.
            known_ids.lock().await.insert(root_id.clone());
            {
                let gr = graph_runtime.clone();
                let pm_w = pm.clone();
                let tx_w = tx.clone();
                let rid = root_id.clone();
                tokio::spawn(async move {
                    watch_single_graph(rid, after_seq, gr, pm_w, tx_w).await;
                });
            }

            // Discovery task: poll every 500 ms for newly-created child graphs.
            {
                let lattice_storage = graph_runtime.storage();
                let known = known_ids.clone();
                let tx_disc = tx.clone();
                let gr_disc = graph_runtime.clone();
                let pm_disc = pm.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                        if tx_disc.is_closed() {
                            break;
                        }
                        let current_known: Vec<String> =
                            known.lock().await.iter().cloned().collect();
                        for gid in current_known {
                            if let Ok(children) = lattice_storage.get_child_graphs(&gid).await {
                                for child in children {
                                    let mut guard = known.lock().await;
                                    if !guard.contains(&child.id) {
                                        guard.insert(child.id.clone());
                                        drop(guard);
                                        let gr = gr_disc.clone();
                                        let pm_c = pm_disc.clone();
                                        let tx_c = tx_disc.clone();
                                        let cid = child.id;
                                        tokio::spawn(async move {
                                            watch_single_graph(cid, None, gr, pm_c, tx_c).await;
                                        });
                                    }
                                }
                            }
                        }
                    }
                });
            }

            // Forward events; stop when the root graph reports terminal status.
            while let Some(event) = rx.recv().await {
                let is_root_terminal = matches!(&event,
                    OrchaEvent::Complete { session_id } | OrchaEvent::Failed { session_id, .. }
                    if session_id == &root_id
                );
                yield event;
                if is_root_terminal {
                    break;
                }
            }
        }
    }

    // ─── Graph Builder API ───────────────────────────────────────────────────────

    /// Create an empty Orcha execution graph.
    #[plexus_macros::method(params(
        metadata = "Arbitrary JSON metadata attached to the graph"
    ))]
    async fn create_graph(&self, metadata: Value) -> Result<GraphRef, OrchaError> {
        let graph = self.graph_runtime.create_graph(metadata).await?;
        Ok(GraphRef { graph_id: graph.graph_id })
    }

    /// Add a task node — Claude runs `task` as a prompt.
    #[plexus_macros::method(params(
        graph_id = "Graph to add the node to",
        task = "Prompt for Claude to execute"
    ))]
    async fn add_task_node(
        &self,
        graph_id: String,
        task: String,
    ) -> Result<NodeRef, OrchaError> {
        let graph = self.graph_runtime.open_graph(graph_id);
        Ok(NodeRef { node_id: graph.add_task(task, None).await? })
    }

    /// Add a synthesize node — like task, with `prior_work` context prepended from input tokens.
    #[plexus_macros::method(params(
        graph_id = "Graph to add the node to",
        task = "Synthesis prompt for Claude"
    ))]
    async fn add_synthesize_node(
        &self,
        graph_id: String,
        task: String,
    ) -> Result<NodeRef, OrchaError> {
        let graph = self.graph_runtime.open_graph(graph_id);
        Ok(NodeRef { node_id: graph.add_synthesize(task, None).await? })
    }

    /// Add a validate node — runs `command` in a shell.
    #[plexus_macros::method(params(
        graph_id = "Graph to add the node to",
        command = "Shell command to validate (exit 0 = pass)",
        cwd = "Working directory (default: /workspace)"
    ))]
    async fn add_validate_node(
        &self,
        graph_id: String,
        command: String,
        cwd: Option<String>,
    ) -> Result<NodeRef, OrchaError> {
        let graph = self.graph_runtime.open_graph(graph_id);
        Ok(NodeRef { node_id: graph.add_validate(command, cwd, None).await? })
    }

    /// Add a gather node — engine-internal, auto-executes when all inbound tokens arrive.
    #[plexus_macros::method(params(
        graph_id = "Graph to add the node to",
        strategy = "Gather strategy: {\"type\":\"all\"} or {\"type\":\"first\",\"n\":N}"
    ))]
    async fn add_gather_node(
        &self,
        graph_id: String,
        strategy: GatherStrategy,
    ) -> Result<NodeRef, OrchaError> {
        let graph = self.graph_runtime.open_graph(graph_id);
        Ok(NodeRef { node_id: graph.add_gather(strategy).await? })
    }

    /// Add a `SubGraph` node — when dispatched, runs the child graph to completion.
    ///
    /// On child success, the parent node receives `{"child_graph_id": "..."}` as output.
    /// On child failure, the parent node is failed (error edge fires if present).
    #[plexus_macros::method(params(
        graph_id = "Graph to add the node to",
        child_graph_id = "ID of the graph to run as a sub-graph"
    ))]
    async fn add_subgraph_node(
        &self,
        graph_id: String,
        child_graph_id: String,
    ) -> Result<NodeRef, OrchaError> {
        let graph = self.graph_runtime.open_graph(graph_id);
        Ok(NodeRef { node_id: graph.add_subgraph(child_graph_id).await? })
    }

    /// Declare that `dependent_node_id` waits for `dependency_node_id` to complete.
    #[plexus_macros::method(params(
        graph_id = "Graph containing both nodes",
        dependent_node_id = "Node that must wait",
        dependency_node_id = "Node that must complete first"
    ))]
    async fn add_dependency(
        &self,
        graph_id: String,
        dependent_node_id: String,
        dependency_node_id: String,
    ) -> Result<(), OrchaError> {
        let graph = self.graph_runtime.open_graph(graph_id);
        graph.depends_on(&dependent_node_id, &dependency_node_id).await?;
        Ok(())
    }

    /// Compile a ticket file and build the lattice graph without executing it.
    ///
    /// Returns the `graph_id`.  Use `orcha.run_graph(graph_id)` to execute it
    /// separately, or `orcha.run_tickets` to build and run in one call.
    #[plexus_macros::method(params(
        tickets = "Raw ticket file content",
        metadata = "Arbitrary JSON metadata attached to the graph"
    ))]
    async fn build_tickets(
        &self,
        tickets: String,
        metadata: Value,
    ) -> Result<GraphRef, OrchaError> {
        let graph_runtime = self.graph_runtime.clone();
        let pm = self.pm.clone();

        let compiled = ticket_compiler::compile_tickets(&tickets).map_err(|e| {
            OrchaError::ValidationError { detail: format!("Ticket compile error: {e}") }
        })?;

        let (graph_id, id_map) = build_graph_from_definition(
            graph_runtime, metadata, compiled.nodes, compiled.edges,
        ).await?;

        let _ = pm.save_ticket_map(&graph_id, &id_map).await;
        Ok(GraphRef { graph_id })
    }

    /// Compile a ticket file and execute the resulting graph.
    ///
    /// Parses the ticket DSL, builds a graph, and streams execution events.
    ///
    /// # Ticket Format
    ///
    /// ```text
    /// --- <id> [<type>] [> dep1, dep2, ...]
    /// task: <text>
    /// validate: <shell command>   (optional; auto-creates sibling validate node)
    /// ```
    ///
    /// Types: `agent`, `agent/synthesize`, `prog`
    #[plexus_macros::method(params(
        tickets = "Raw ticket file content",
        metadata = "Arbitrary JSON metadata attached to the graph",
        model = "Model for task nodes: opus, sonnet, haiku (default: sonnet)",
        working_directory = "Working directory for task nodes (default: /workspace)"
    ))]
    async fn run_tickets(
        &self,
        tickets: String,
        metadata: Value,
        model: Option<String>,
        working_directory: Option<String>,
    ) -> impl Stream<Item = OrchaEvent> + Send + 'static {
        let graph_runtime = self.graph_runtime.clone();
        let claudecode = self.claudecode.clone();
        let arbor_storage = self.arbor_storage.clone();
        let loopback_storage = self.loopback.storage();
        let pm = self.pm.clone();
        let cancel_registry = self.cancel_registry.clone();
        stream! {
            let compiled = match ticket_compiler::compile_tickets(&tickets) {
                Ok(c) => c,
                Err(e) => {
                    yield OrchaEvent::Failed {
                        session_id: "tickets".to_string(),
                        error: format!("Ticket compile error: {e}"),
                    };
                    return;
                }
            };
            let model_str = model.as_deref().unwrap_or("sonnet").to_string();
            let wd = working_directory.unwrap_or_else(|| "/workspace".to_string());
            let mut enriched_metadata = if metadata.is_object() {
                metadata.clone()
            } else {
                serde_json::json!({})
            };
            enriched_metadata["_plexus_run_config"] = serde_json::json!({
                "model": model_str,
                "working_directory": wd,
            });
            let (graph_id, id_map) = match build_graph_from_definition(
                graph_runtime.clone(), enriched_metadata, compiled.nodes, compiled.edges,
            ).await {
                Ok(pair) => pair,
                Err(e) => {
                    yield OrchaEvent::Failed {
                        session_id: "tickets".to_string(),
                        error: e,
                    };
                    return;
                }
            };
            let _ = pm.save_ticket_map(&graph_id, &id_map).await;
            let _ = pm.save_ticket_source(&graph_id, &tickets).await;

            yield OrchaEvent::GraphStarted { graph_id: graph_id.clone() };

            let model_enum = match model_str.as_str() {
                "opus" => Model::Opus,
                "haiku" => Model::Haiku,
                _ => Model::Sonnet,
            };

            // Validate working directory early — before the graph starts executing —
            // so the caller gets a clear error instead of every node failing with an
            // opaque Claude CLI exit message.
            if !std::path::Path::new(&wd).is_dir() {
                yield OrchaEvent::Failed {
                    session_id: "tickets".to_string(),
                    error: format!(
                        "Working directory does not exist: '{wd}'. \
                         Create it before running tickets or pass an existing path."
                    ),
                };
                return;
            }

            // Build a node_id → ticket_id map from id_map (which is ticket_id → node_id).
            let node_to_ticket: std::collections::HashMap<String, String> = id_map
                .iter()
                .map(|(ticket, node)| (node.clone(), ticket.clone()))
                .collect();

            let graph = Arc::new(graph_runtime.open_graph(graph_id.clone()));

            // Register a cancel token so this graph can be stopped via cancel_graph.
            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
            cancel_registry.lock().await.insert(graph_id.clone(), cancel_tx);

            // Spawn execution in background — caller can disconnect safely and use
            // subscribe_graph(graph_id) to re-attach at any time.
            tokio::spawn(async move {
                let execution = graph_runner::run_graph_execution(
                    graph,
                    claudecode,
                    arbor_storage,
                    loopback_storage,
                    pm,
                    graph_runtime,
                    cancel_registry.clone(),
                    model_enum,
                    wd,
                    cancel_rx,
                    node_to_ticket,
                );
                tokio::pin!(execution);
                while let Some(event) = execution.next().await {
                    match &event {
                        OrchaEvent::Failed { error, .. } => {
                            tracing::error!("run_tickets graph {} failed: {}", graph_id, error);
                        }
                        OrchaEvent::Complete { .. } => {
                            tracing::info!("run_tickets graph {} complete", graph_id);
                        }
                        _ => {}
                    }
                }
                cancel_registry.lock().await.remove(&graph_id);
            });
        }
    }

    /// Compile a ticket file, build the lattice graph, and start execution in the background.
    ///
    /// Returns immediately after yielding a single `GraphStarted { graph_id }` event.
    /// Execution continues in a detached tokio task; use `subscribe_graph(graph_id)` to
    /// observe progress, or `pm.graph_status(graph_id)` to poll completion.
    ///
    /// This is the fire-and-forget counterpart to `run_tickets`, which blocks until the
    /// graph completes.  Use `run_tickets_async` when the caller cannot hold the connection
    /// open for the full duration of a long-running graph.
    #[plexus_macros::method(params(
        tickets = "Raw ticket file content",
        metadata = "Arbitrary JSON metadata",
        model = "Model: opus, sonnet, haiku (default: sonnet)",
        working_directory = "Working directory (default: /workspace)"
    ))]
    async fn run_tickets_async(
        &self,
        tickets: String,
        metadata: Value,
        model: Option<String>,
        working_directory: Option<String>,
    ) -> Result<GraphRef, OrchaError> {
        let graph_runtime = self.graph_runtime.clone();
        let claudecode = self.claudecode.clone();
        let arbor_storage = self.arbor_storage.clone();
        let loopback_storage = self.loopback.storage();
        let pm = self.pm.clone();
        let cancel_registry = self.cancel_registry.clone();
        {
            let compiled = ticket_compiler::compile_tickets(&tickets).map_err(|e| {
                OrchaError::ValidationError { detail: format!("Ticket compile error: {e}") }
            })?;

            let model_str = model.as_deref().unwrap_or("sonnet").to_string();
            let wd = working_directory.unwrap_or_else(|| "/workspace".to_string());

            // Validate working directory before building the graph so the caller
            // gets a clear error rather than every node failing with an opaque message.
            if !std::path::Path::new(&wd).is_dir() {
                return Err(OrchaError::ValidationError {
                    detail: format!(
                        "Working directory does not exist: '{wd}'. \
                         Create it before running tickets or pass an existing path."
                    ),
                });
            }

            let mut enriched_metadata = if metadata.is_object() {
                metadata.clone()
            } else {
                serde_json::json!({})
            };
            enriched_metadata["_plexus_run_config"] = serde_json::json!({
                "model": model_str,
                "working_directory": wd,
            });

            let (graph_id, id_map) = build_graph_from_definition(
                graph_runtime.clone(), enriched_metadata, compiled.nodes, compiled.edges,
            ).await?;

            let _ = pm.save_ticket_map(&graph_id, &id_map).await;
            let _ = pm.save_ticket_source(&graph_id, &tickets).await;

            let started = GraphRef { graph_id: graph_id.clone() };

            let model_enum = match model_str.as_str() {
                "opus" => Model::Opus,
                "haiku" => Model::Haiku,
                _ => Model::Sonnet,
            };

            let graph = Arc::new(graph_runtime.open_graph(graph_id.clone()));

            // Build a node_id → ticket_id map from id_map (which is ticket_id → node_id).
            let node_to_ticket: std::collections::HashMap<String, String> = id_map
                .iter()
                .map(|(ticket, node)| (node.clone(), ticket.clone()))
                .collect();

            // Register a cancel token so this graph can be stopped via cancel_graph.
            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
            cancel_registry.lock().await.insert(graph_id.clone(), cancel_tx);

            tokio::spawn(async move {
                let execution = graph_runner::run_graph_execution(
                    graph,
                    claudecode,
                    arbor_storage,
                    loopback_storage,
                    pm,
                    graph_runtime,
                    cancel_registry.clone(),
                    model_enum,
                    wd,
                    cancel_rx,
                    node_to_ticket,
                );
                tokio::pin!(execution);
                while let Some(event) = execution.next().await {
                    match &event {
                        OrchaEvent::Failed { error, .. } => {
                            tracing::error!(
                                "run_tickets_async graph {} failed: {}",
                                graph_id, error
                            );
                        }
                        OrchaEvent::Complete { .. } => {
                            tracing::info!("run_tickets_async graph {} complete", graph_id);
                        }
                        _ => {}
                    }
                }
                cancel_registry.lock().await.remove(&graph_id);
            });

            Ok(started)
        }
    }

    /// Read one or more ticket files from disk, concatenate them, compile, and run.
    ///
    /// Equivalent to reading each file and passing the joined content to `run_tickets`.
    /// Files are joined with a blank line separator; the compiler ignores preamble and
    /// section boundaries so cross-file `blocked_by` references work correctly.
    ///
    /// Streams `OrchaEvents` until the graph completes or fails.
    #[plexus_macros::method(params(
        paths = "Absolute paths to ticket markdown files, e.g. [\"/workspace/plans/batch.tickets.md\"]",
        metadata = "Arbitrary JSON metadata attached to the graph",
        model = "Model for task nodes: opus, sonnet, haiku (default: sonnet)",
        working_directory = "Working directory for task nodes (default: /workspace)"
    ))]
    async fn run_tickets_files(
        &self,
        paths: Vec<String>,
        metadata: Value,
        model: Option<String>,
        working_directory: Option<String>,
    ) -> impl Stream<Item = OrchaEvent> + Send + 'static {
        let graph_runtime = self.graph_runtime.clone();
        let claudecode = self.claudecode.clone();
        let arbor_storage = self.arbor_storage.clone();
        let loopback_storage = self.loopback.storage();
        let pm = self.pm.clone();
        let cancel_registry = self.cancel_registry.clone();
        stream! {
            // Read and concatenate all files.
            let mut parts: Vec<String> = Vec::new();
            for path in &paths {
                match tokio::fs::read_to_string(path).await {
                    Ok(content) => parts.push(content),
                    Err(e) => {
                        yield OrchaEvent::Failed {
                            session_id: "tickets".to_string(),
                            error: format!("Failed to read '{path}': {e}"),
                        };
                        return;
                    }
                }
            }
            let tickets = parts.join("\n\n");

            let compiled = match ticket_compiler::compile_tickets(&tickets) {
                Ok(c) => c,
                Err(e) => {
                    yield OrchaEvent::Failed {
                        session_id: "tickets".to_string(),
                        error: format!("Ticket compile error: {e}"),
                    };
                    return;
                }
            };
            let model_str = model.as_deref().unwrap_or("sonnet").to_string();
            let wd = working_directory.unwrap_or_else(|| "/workspace".to_string());
            let mut enriched_metadata = if metadata.is_object() { metadata.clone() } else { serde_json::json!({}) };
            enriched_metadata["_plexus_run_config"] = serde_json::json!({
                "model": model_str,
                "working_directory": wd,
            });
            let (graph_id, id_map) = match build_graph_from_definition(
                graph_runtime.clone(), enriched_metadata, compiled.nodes, compiled.edges,
            ).await {
                Ok(pair) => pair,
                Err(e) => {
                    yield OrchaEvent::Failed { session_id: "tickets".to_string(), error: e };
                    return;
                }
            };
            let _ = pm.save_ticket_map(&graph_id, &id_map).await;
            let _ = pm.save_ticket_source(&graph_id, &tickets).await;

            yield OrchaEvent::GraphStarted { graph_id: graph_id.clone() };

            let model_enum = match model_str.as_str() {
                "opus" => Model::Opus,
                "haiku" => Model::Haiku,
                _ => Model::Sonnet,
            };
            if !std::path::Path::new(&wd).is_dir() {
                yield OrchaEvent::Failed {
                    session_id: "tickets".to_string(),
                    error: format!("Working directory does not exist: '{wd}'"),
                };
                return;
            }
            let node_to_ticket: std::collections::HashMap<String, String> = id_map
                .iter().map(|(t, n)| (n.clone(), t.clone())).collect();
            let graph = Arc::new(graph_runtime.open_graph(graph_id.clone()));
            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
            cancel_registry.lock().await.insert(graph_id.clone(), cancel_tx);
            let execution = graph_runner::run_graph_execution(
                graph, claudecode, arbor_storage, loopback_storage, pm,
                graph_runtime, cancel_registry.clone(),
                model_enum, wd, cancel_rx, node_to_ticket,
            );
            tokio::pin!(execution);
            while let Some(event) = execution.next().await {
                yield event;
            }
            cancel_registry.lock().await.remove(&graph_id);
        }
    }

    /// Like `run_tickets_files` but fire-and-forget — returns `GraphStarted` immediately.
    ///
    /// Use `subscribe_graph(graph_id)` to observe progress after this call returns.
    #[plexus_macros::method(params(
        paths = "Absolute paths to ticket markdown files",
        metadata = "Arbitrary JSON metadata attached to the graph",
        model = "Model for task nodes: opus, sonnet, haiku (default: sonnet)",
        working_directory = "Working directory for task nodes (default: /workspace)"
    ))]
    async fn run_tickets_async_files(
        &self,
        paths: Vec<String>,
        metadata: Value,
        model: Option<String>,
        working_directory: Option<String>,
    ) -> impl Stream<Item = OrchaEvent> + Send + 'static {
        let graph_runtime = self.graph_runtime.clone();
        let claudecode = self.claudecode.clone();
        let arbor_storage = self.arbor_storage.clone();
        let loopback_storage = self.loopback.storage();
        let pm = self.pm.clone();
        let cancel_registry = self.cancel_registry.clone();
        stream! {
            let mut parts: Vec<String> = Vec::new();
            for path in &paths {
                match tokio::fs::read_to_string(path).await {
                    Ok(content) => parts.push(content),
                    Err(e) => {
                        yield OrchaEvent::Failed {
                            session_id: "tickets".to_string(),
                            error: format!("Failed to read '{path}': {e}"),
                        };
                        return;
                    }
                }
            }
            let tickets = parts.join("\n\n");

            let compiled = match ticket_compiler::compile_tickets(&tickets) {
                Ok(c) => c,
                Err(e) => {
                    yield OrchaEvent::Failed {
                        session_id: "tickets".to_string(),
                        error: format!("Ticket compile error: {e}"),
                    };
                    return;
                }
            };
            let model_str = model.as_deref().unwrap_or("sonnet").to_string();
            let wd = working_directory.unwrap_or_else(|| "/workspace".to_string());
            let mut enriched_metadata = if metadata.is_object() { metadata.clone() } else { serde_json::json!({}) };
            enriched_metadata["_plexus_run_config"] = serde_json::json!({
                "model": model_str,
                "working_directory": wd,
            });
            let (graph_id, id_map) = match build_graph_from_definition(
                graph_runtime.clone(), enriched_metadata, compiled.nodes, compiled.edges,
            ).await {
                Ok(pair) => pair,
                Err(e) => {
                    yield OrchaEvent::Failed { session_id: "tickets".to_string(), error: e };
                    return;
                }
            };
            let _ = pm.save_ticket_map(&graph_id, &id_map).await;
            let _ = pm.save_ticket_source(&graph_id, &tickets).await;

            yield OrchaEvent::GraphStarted { graph_id: graph_id.clone() };

            let model_enum = match model_str.as_str() {
                "opus" => Model::Opus,
                "haiku" => Model::Haiku,
                _ => Model::Sonnet,
            };
            if !std::path::Path::new(&wd).is_dir() {
                yield OrchaEvent::Failed {
                    session_id: "tickets".to_string(),
                    error: format!("Working directory does not exist: '{wd}'"),
                };
                return;
            }
            let node_to_ticket: std::collections::HashMap<String, String> = id_map
                .iter().map(|(t, n)| (n.clone(), t.clone())).collect();
            let graph = Arc::new(graph_runtime.open_graph(graph_id.clone()));
            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
            cancel_registry.lock().await.insert(graph_id.clone(), cancel_tx);
            tokio::spawn(async move {
                let execution = graph_runner::run_graph_execution(
                    graph, claudecode, arbor_storage, loopback_storage, pm,
                    graph_runtime, cancel_registry.clone(),
                    model_enum, wd, cancel_rx, node_to_ticket,
                );
                tokio::pin!(execution);
                while let Some(event) = execution.next().await {
                    match &event {
                        OrchaEvent::Failed { error, .. } => {
                            tracing::error!("run_tickets_async_files graph {} failed: {}", graph_id, error);
                        }
                        OrchaEvent::Complete { .. } => {
                            tracing::info!("run_tickets_async_files graph {} complete", graph_id);
                        }
                        _ => {}
                    }
                }
                cancel_registry.lock().await.remove(&graph_id);
            });
        }
    }

    /// Build and execute a graph from an inline definition.
    ///
    /// Nodes use caller-supplied string ids; edges reference those ids.
    /// Streams `OrchaEvents`. The `graph_id` appears in progress and complete/failed events.
    #[plexus_macros::method(params(
        metadata = "Arbitrary JSON metadata attached to the graph",
        model = "Model for task nodes: opus, sonnet, haiku (default: sonnet)",
        working_directory = "Working directory for task nodes (default: /workspace)",
        nodes = "Array of OrchaNodeDef: [{\"id\":\"...\",\"spec\":{\"type\":\"task\",\"task\":\"...\"}}]",
        edges = "Array of OrchaEdgeDef: [{\"from\":\"id1\",\"to\":\"id2\"}]"
    ))]
    async fn run_graph_definition(
        &self,
        metadata: Value,
        model: Option<String>,
        working_directory: Option<String>,
        nodes: Vec<OrchaNodeDef>,
        edges: Vec<OrchaEdgeDef>,
    ) -> impl Stream<Item = OrchaEvent> + Send + 'static {
        build_and_run_graph_definition(
            self.graph_runtime.clone(),
            self.claudecode.clone(),
            self.arbor_storage.clone(),
            self.loopback.storage(),
            self.cancel_registry.clone(),
            self.pm.clone(),
            metadata,
            model,
            working_directory,
            nodes,
            edges,
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Graph Construction (shared pure-build step)
// ═══════════════════════════════════════════════════════════════════════════

/// Build a lattice graph from a node+edge definition.
///
/// Creates the graph, adds all nodes (returning their lattice node-ids),
/// and wires all edges.  Returns `(graph_id, ticket_id→node_id map)` on success.
///
/// This is the shared foundation used by `build_tickets`, `build_graph_definition`,
/// and `build_and_run_graph_definition`.
async fn build_graph_from_definition(
    graph_runtime: Arc<GraphRuntime>,
    metadata: Value,
    nodes: Vec<OrchaNodeDef>,
    edges: Vec<OrchaEdgeDef>,
) -> Result<(String, HashMap<String, String>), String> {
    let graph = graph_runtime
        .create_graph(metadata)
        .await
        .map_err(|e| format!("Failed to create graph: {e}"))?;
    let graph_id = graph.graph_id.clone();

    let mut id_map: HashMap<String, String> = HashMap::new();
    for OrchaNodeDef { id, spec } in nodes {
        let result = match spec {
            OrchaNodeSpec::Task { task, max_retries } => graph.add_task(task, max_retries).await,
            OrchaNodeSpec::Synthesize { task, max_retries } => graph.add_synthesize(task, max_retries).await,
            OrchaNodeSpec::Validate { command, cwd, max_retries } => graph.add_validate(command, cwd, max_retries).await,
            OrchaNodeSpec::Gather { strategy } => graph.add_gather(strategy).await,
            OrchaNodeSpec::Review { prompt } => graph.add_review(prompt).await,
            OrchaNodeSpec::Plan { task } => graph.add_plan(task).await,
        };
        let lattice_id = match result {
            Ok(lid) => lid,
            Err(e) => return Err(format!("Failed to add node '{id}': {e}")),
        };
        id_map.insert(id, lattice_id);
    }

    for OrchaEdgeDef { from, to } in edges {
        let dep_id = id_map
            .get(&from)
            .ok_or_else(|| format!("Unknown node id in edge.from: '{from}'"))?
            .clone();
        let node_id = id_map
            .get(&to)
            .ok_or_else(|| format!("Unknown node id in edge.to: '{to}'"))?
            .clone();
        graph
            .depends_on(&node_id, &dep_id)
            .await
            .map_err(|e| format!("Failed to add edge {from} → {to}: {e}"))?;
    }

    Ok((graph_id, id_map))
}

// ─── Build + run ─────────────────────────────────────────────────────────────

fn build_and_run_graph_definition(
    graph_runtime: Arc<GraphRuntime>,
    claudecode: Arc<ClaudeCode>,
    arbor_storage: Arc<crate::activations::arbor::ArborStorage>,
    loopback_storage: Arc<crate::activations::claudecode_loopback::LoopbackStorage>,
    cancel_registry: CancelRegistry,
    pm: Arc<super::pm::Pm>,
    metadata: Value,
    model: Option<String>,
    working_directory: Option<String>,
    nodes: Vec<OrchaNodeDef>,
    edges: Vec<OrchaEdgeDef>,
) -> impl Stream<Item = OrchaEvent> + Send + 'static {
    stream! {
        let (graph_id, _) = match build_graph_from_definition(
            graph_runtime.clone(), metadata, nodes, edges,
        ).await {
            Ok(pair) => pair,
            Err(e) => {
                yield OrchaEvent::Failed {
                    session_id: "graph_definition".to_string(),
                    error: e,
                };
                return;
            }
        };

        yield OrchaEvent::Progress {
            message: format!("Graph {graph_id} ready, starting execution"),
            percentage: None,
        };

        let model_enum = match model.as_deref().unwrap_or("sonnet") {
            "opus" => Model::Opus,
            "haiku" => Model::Haiku,
            _ => Model::Sonnet,
        };
        let wd = working_directory.unwrap_or_else(|| "/workspace".to_string());

        // Register a cancel token so this graph can be stopped via cancel_graph.
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        cancel_registry.lock().await.insert(graph_id.clone(), cancel_tx);

        let execution = graph_runner::run_graph_execution(
            Arc::new(graph_runtime.open_graph(graph_id.clone())),
            claudecode,
            arbor_storage,
            loopback_storage,
            pm,
            graph_runtime,
            cancel_registry.clone(),
            model_enum,
            wd,
            cancel_rx,
            std::collections::HashMap::new(),
        );
        tokio::pin!(execution);
        while let Some(event) = execution.next().await {
            yield event;
        }
        cancel_registry.lock().await.remove(&graph_id);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Helper Functions (original)
// ═══════════════════════════════════════════════════════════════════════════

/// Extract validation artifact from accumulated text
fn extract_validation_artifact(text: &str) -> Option<ValidationArtifact> {
    // Look for {"orcha_validate": {...}} pattern
    use regex::Regex;

    let re = match Regex::new(r#"\{"orcha_validate"\s*:\s*(\{[^}]+\})\}"#) {
        Ok(re) => re,
        Err(e) => {
            tracing::warn!("Failed to compile orcha_validate regex: {}", e);
            return None;
        }
    };
    let captures = re.captures(text)?;
    let json_str = captures.get(1)?.as_str();

    match serde_json::from_str::<ValidationArtifact>(json_str) {
        Ok(artifact) => Some(artifact),
        Err(e) => {
            tracing::warn!("Failed to parse validation artifact JSON '{}': {}", json_str, e);
            None
        }
    }
}

/// Run a validation test command
async fn run_validation_test(artifact: &ValidationArtifact) -> ValidationResult {
    let output = Command::new("sh")
        .arg("-c")
        .arg(&artifact.test_command)
        .current_dir(&artifact.cwd)
        .output()
        .await;

    match output {
        Ok(output) => ValidationResult {
            success: output.status.success(),
            output: String::from_utf8_lossy(&output.stdout).to_string()
                + String::from_utf8_lossy(&output.stderr).as_ref(),
            exit_code: output.status.code(),
        },
        Err(e) => ValidationResult {
            success: false,
            output: format!("Failed to execute command: {e}"),
            exit_code: None,
        },
    }
}

/// Format an arbor tree into a readable conversation
///
/// Converts the JSON-based arbor tree structure into a human-readable conversation format
fn format_conversation_from_tree(tree: &crate::activations::arbor::Tree) -> String {
    use crate::activations::arbor::NodeType;

    // Walk the tree in order
    fn walk_nodes(
        tree: &crate::activations::arbor::Tree,
        node_id: &crate::activations::arbor::NodeId,
        output: &mut String,
        current_role: &mut String,
        message_text: &mut String,
        tool_uses: &mut Vec<String>,
    ) {
        if let Some(node) = tree.nodes.get(node_id) {
            if let NodeType::Text { content } = &node.data {
                // Try to parse as JSON to extract event type
                if let Ok(event) = serde_json::from_str::<serde_json::Value>(content) {
                    if let Some(event_type) = event.get("type").and_then(|v| v.as_str()) {
                        match event_type {
                            "user_message" => {
                                // Flush previous message
                                flush_message(output, current_role, message_text, tool_uses);

                                *current_role = "User".to_string();
                                if let Some(content) = event.get("content").and_then(|v| v.as_str()) {
                                    *message_text = content.to_string();
                                }
                            }
                            "assistant_start" => {
                                // Flush previous message
                                flush_message(output, current_role, message_text, tool_uses);

                                *current_role = "Assistant".to_string();
                                *message_text = String::new();
                            }
                            "content_text" => {
                                if let Some(text) = event.get("text").and_then(|v| v.as_str()) {
                                    message_text.push_str(text);
                                }
                            }
                            "content_tool_use" => {
                                if let Some(name) = event.get("name").and_then(|v| v.as_str()) {
                                    let mut tool_str = format!("[Tool: {name}]");
                                    if let Some(input) = event.get("input") {
                                        if let Ok(input_str) = serde_json::to_string_pretty(input) {
                                            // Limit tool input to 200 chars
                                            let trimmed = if input_str.len() > 200 {
                                                format!("{}...", &input_str[..200])
                                            } else {
                                                input_str
                                            };
                                            tool_str.push_str(&format!(" {trimmed}"));
                                        }
                                    }
                                    tool_uses.push(tool_str);
                                }
                            }
                            _ => {} // Ignore other event types
                        }
                    }
                }
            }

            // Recursively walk children
            for child_id in &node.children {
                walk_nodes(tree, child_id, output, current_role, message_text, tool_uses);
            }
        }
    }

    fn flush_message(
        output: &mut String,
        current_role: &str,
        message_text: &str,
        tool_uses: &mut Vec<String>,
    ) {
        if !current_role.is_empty() && (!message_text.is_empty() || !tool_uses.is_empty()) {
            output.push_str(&format!("{current_role}:\n"));
            if !message_text.is_empty() {
                output.push_str(message_text);
                output.push('\n');
            }
            for tool in tool_uses.drain(..) {
                output.push_str(&format!("  {tool}\n"));
            }
            output.push('\n');
        }
    }

    let mut output = String::new();
    let mut current_role = String::new();
    let mut message_text = String::new();
    let mut tool_uses = Vec::new();

    // Start walking from root
    walk_nodes(tree, &tree.root, &mut output, &mut current_role, &mut message_text, &mut tool_uses);

    // Flush any remaining message
    flush_message(&mut output, &current_role, &message_text, &mut tool_uses);

    output
}

/// Save a status summary to the arbor monitoring tree
///
/// Creates a monitoring tree for the session (if it doesn't exist) and appends
/// the summary as a new text node with timestamp.
async fn save_status_summary_to_arbor(
    arbor_storage: &crate::activations::arbor::ArborStorage,
    session_id: &str,
    summary: &str,
) -> Result<(), String> {
    use crate::activations::arbor::TreeId;

    // Generate deterministic tree ID from path: orcha.<session-id>.monitor
    let tree_path = format!("orcha.{session_id}.monitor");
    let tree_uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, tree_path.as_bytes());
    let monitor_tree_id = TreeId::from(tree_uuid);

    // Try to get existing tree, create if it doesn't exist
    let tree = if let Ok(tree) = arbor_storage.tree_get(&monitor_tree_id).await { tree } else {
        // Tree doesn't exist, create it with our deterministic ID
        let metadata = serde_json::json!({
            "type": "orcha_monitor",
            "session_id": session_id,
            "tree_path": tree_path
        });

        let created_tree_id = arbor_storage.tree_create_with_id(
            Some(monitor_tree_id),
            Some(metadata),
            "orcha",
        ).await.map_err(|e| e.to_string())?;

        arbor_storage.tree_get(&created_tree_id).await
            .map_err(|e| e.to_string())?
    };

    // Find the latest summary node to append to, or use root
    let parent = tree.nodes.values()
        .filter(|n| matches!(n.data, crate::activations::arbor::NodeType::Text { .. }))
        .max_by_key(|n| n.created_at)
        .map_or(tree.root, |n| n.id);

    // Append summary as a text node with timestamp
    let timestamp = chrono::Utc::now().to_rfc3339();
    let summary_content = format!(
        "[{}] {}\n",
        timestamp,
        summary.trim()
    );

    arbor_storage.node_create_text(
        &tree.id,
        Some(parent),
        summary_content,
        None,
    ).await.map_err(|e| e.to_string())?;

    Ok(())
}

/// Generate summary for a single agent
async fn generate_agent_summary(
    claudecode: &ClaudeCode,
    arbor_storage: &crate::activations::arbor::ArborStorage,
    agent: AgentInfo,
) -> Result<AgentSummary, String> {
    use futures::StreamExt;

    // Get conversation tree for this agent's ClaudeCode session
    let cc_session = claudecode.storage.session_get_by_name(&agent.claudecode_session_id).await
        .map_err(|e| format!("Failed to get CC session: {e}"))?;

    let tree = arbor_storage.tree_get(&cc_session.head.tree_id).await
        .map_err(|e| format!("Failed to get tree: {e}"))?;

    let conversation = format_conversation_from_tree(&tree);

    // Create ephemeral session to generate summary
    let summary_session = format!("orcha-agent-summary-{}", Uuid::new_v4());
    let summary_session_id = format!("{}-agent-summary-{}", agent.session_id, Uuid::new_v4());

    let create_stream = claudecode.create(
        summary_session.clone(),
        "/workspace".to_string(),
        crate::activations::claudecode::Model::Haiku,
        None,
        Some(false),
        Some(summary_session_id), // Track ephemeral summary session
    ).await;
    tokio::pin!(create_stream);

    // Wait for creation
    let mut created = false;
    while let Some(result) = create_stream.next().await {
        if let crate::activations::claudecode::CreateResult::Ok { .. } = result {
            created = true;
            break;
        }
    }

    if !created {
        return Err("Failed to create summary session".to_string());
    }

    // Ask for summary
    let prompt = format!(
        "Summarize this agent's work in 2-3 sentences:\n\n\
         Subtask: {}\n\
         State: {:?}\n\n\
         Conversation:\n{}\n\n\
         Be concise and focus on what was accomplished or is in progress.",
        agent.subtask,
        agent.state,
        conversation
    );

    let chat_stream = claudecode.chat(summary_session, prompt, Some(true), None).await;
    tokio::pin!(chat_stream);

    let mut summary = String::new();
    while let Some(event) = chat_stream.next().await {
        if let crate::activations::claudecode::ChatEvent::Content { text } = event {
            summary.push_str(&text);
        }
    }

    Ok(AgentSummary {
        agent_id: agent.agent_id,
        subtask: agent.subtask,
        state: agent.state,
        summary,
    })
}

/// Generate overall meta-summary combining all agent work
async fn generate_overall_summary(
    claudecode: &ClaudeCode,
    session_id: &SessionId,
    agent_summaries: &[AgentSummary],
) -> Option<String> {
    use futures::StreamExt;

    let summary_session = format!("orcha-meta-summary-{}", Uuid::new_v4());
    let meta_summary_session_id = format!("{}-meta-summary-{}", session_id, Uuid::new_v4());

    // Create session
    let create_stream = claudecode.create(
        summary_session.clone(),
        "/workspace".to_string(),
        crate::activations::claudecode::Model::Haiku,
        None,
        Some(false),
        Some(meta_summary_session_id), // Track meta-summary under parent session
    ).await;
    tokio::pin!(create_stream);

    let mut created = false;
    while let Some(result) = create_stream.next().await {
        if let crate::activations::claudecode::CreateResult::Ok { .. } = result {
            created = true;
            break;
        }
    }

    if !created {
        return None;
    }

    // Build prompt with all agent summaries
    let mut agent_list = String::new();
    for (i, summary) in agent_summaries.iter().enumerate() {
        agent_list.push_str(&format!(
            "{}. {} ({:?})\n   {}\n\n",
            i + 1,
            summary.subtask,
            summary.state,
            summary.summary
        ));
    }

    let prompt = format!(
        "This is a multi-agent orchestration session with {} agents working on different subtasks.\n\n\
         Agent summaries:\n{}\n\
         Provide a 2-4 sentence overall summary of the session's progress and coordination.\n\
         Focus on: what's the big picture? What's been accomplished? What's still in progress?",
        agent_summaries.len(),
        agent_list
    );

    let chat_stream = claudecode.chat(summary_session, prompt, Some(true), None).await;
    tokio::pin!(chat_stream);

    let mut summary = String::new();
    while let Some(event) = chat_stream.next().await {
        if let crate::activations::claudecode::ChatEvent::Content { text } = event {
            summary.push_str(&text);
        }
    }

    Some(summary)
}
