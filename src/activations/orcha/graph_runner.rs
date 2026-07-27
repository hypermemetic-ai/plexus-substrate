use crate::activations::arbor::ArborStorage;
use crate::activations::claudecode::{ChatEvent, ClaudeCode, CreateResult, Model};
use crate::activations::claudecode_loopback::LoopbackStorage;
use crate::activations::lattice::{LatticeEvent, LatticeEventEnvelope, NodeOutput, NodeSpec, Token, TokenPayload};
use async_stream::stream;
use futures::{Stream, StreamExt};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use super::graph_runtime::{GraphRuntime, OrchaGraph};
use super::pm::Pm;
use super::types::{OrchaEvent, OrchaNodeKind};

type CancelRegistry = Arc<tokio::sync::Mutex<HashMap<String, tokio::sync::watch::Sender<bool>>>>;

/// Run a DAG graph with Orcha's node dispatch logic.
///
/// Watches `graph.watch()` and for each `NodeReady` event:
/// - Dispatches based on the typed `OrchaNodeKind` serialized in `NodeSpec::Task { data }`
/// - Spawns a tokio task per node; the task calls `graph.complete_node` / `fail_node`
/// - Tracks dispatched nodes to prevent double-dispatch on reconnect
///
/// `cancel_rx`: a watch receiver; when its value flips to `true`, all spawned node tasks
/// will abandon their current chat stream and return an error, causing the graph to fail.
///
/// Returns a stream of `OrchaEvent` for monitoring.
/// The stream closes when the graph reaches `GraphDone` or `GraphFailed`.
pub(super) fn run_graph_execution(
    graph: Arc<OrchaGraph>,
    claudecode: Arc<ClaudeCode>,
    arbor_storage: Arc<ArborStorage>,
    loopback_storage: Arc<LoopbackStorage>,
    pm: Arc<Pm>,
    graph_runtime: Arc<GraphRuntime>,
    cancel_registry: CancelRegistry,
    model: Model,
    working_directory: String,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    node_to_ticket: HashMap<String, String>,
) -> impl Stream<Item = OrchaEvent> + Send + 'static {
    stream! {
        let event_stream = graph.watch(None);
        tokio::pin!(event_stream);

        // Channel for events emitted from within spawned dispatch tasks (e.g. Retrying, NodeOutput).
        // The receiver is drained in the select! loop below alongside the lattice event stream.
        let (node_event_tx, mut output_rx) = tokio::sync::mpsc::unbounded_channel::<OrchaEvent>();

        // Pre-populate dispatched from nodes already complete/failed (recovery safety).
        // On reconnect, the event log replays NodeReady for every node that was ever ready;
        // without this, already-finished nodes would be re-dispatched.
        let mut dispatched: HashSet<String> = graph.get_terminal_node_ids().await
            .unwrap_or_default()
            .into_iter()
            .collect::<HashSet<String>>();

        // Progress tracking: re-fetched on each completion to stay accurate for live graphs.
        let mut total_nodes: usize = graph.count_nodes().await.unwrap_or(0);
        let mut complete_nodes: usize = 0;

        /// Compute percentage as integer 0–100.
        fn calc_percentage(complete: usize, total: usize) -> Option<u32> {
            if total == 0 {
                return None;
            }
            Some((complete as f32 / total as f32 * 100.0) as u32)
        }

        loop {
            tokio::select! {
                envelope = event_stream.next() => {
                    let Some(LatticeEventEnvelope { event, .. }) = envelope else { break };
                    match event {
                        LatticeEvent::NodeReady { node_id, spec } => {
                            // Skip if already dispatched (reconnect replay)
                            if dispatched.contains(&node_id) {
                                continue;
                            }
                            dispatched.insert(node_id.clone());

                            let ticket_id = node_to_ticket.get(&node_id).cloned();
                            yield OrchaEvent::NodeStarted {
                                node_id: node_id.clone(),
                                label: None,
                                ticket_id: ticket_id.clone(),
                                percentage: calc_percentage(complete_nodes, total_nodes),
                            };

                            let g = graph.clone();
                            let cc = claudecode.clone();
                            let arbor = arbor_storage.clone();
                            let lb = loopback_storage.clone();
                            let pm_log = pm.clone();
                            let gr = graph_runtime.clone();
                            let cr = cancel_registry.clone();
                            let nid = node_id.clone();
                            let wd = working_directory.clone();
                            let tx = node_event_tx.clone();
                            let cancel = cancel_rx.clone();

                            tokio::spawn(async move {
                                // Emit NodeStarted before executing
                                let _ = g.start_node(&nid).await;

                                let result = dispatch_node(cc, arbor, lb, pm_log, gr, cr, &g, &spec, &nid, model, wd, tx, cancel, ticket_id).await;
                                match result {
                                    Ok(output) => {
                                        if let Err(e) = g.complete_node(&nid, output).await {
                                            tracing::error!("complete_node failed for {}: {}", nid, e);
                                        }
                                    }
                                    Err(e) => {
                                        let _ = g.fail_node(&nid, e).await;
                                    }
                                }
                            });
                        }

                        LatticeEvent::NodeDone { node_id, output } => {
                            complete_nodes += 1;
                            total_nodes = graph.count_nodes().await.unwrap_or(total_nodes);
                            let ticket_id = node_to_ticket.get(&node_id).cloned();
                            let summary = output.as_ref().and_then(|o| {
                                output_text(o).map(|s| s.chars().take(200).collect::<String>())
                            });
                            yield OrchaEvent::NodeComplete {
                                node_id,
                                label: None,
                                ticket_id,
                                output_summary: summary,
                                percentage: calc_percentage(complete_nodes, total_nodes),
                            };
                        }

                        LatticeEvent::NodeFailed { node_id, error } => {
                            complete_nodes += 1;
                            total_nodes = graph.count_nodes().await.unwrap_or(total_nodes);
                            let ticket_id = node_to_ticket.get(&node_id).cloned();
                            yield OrchaEvent::NodeFailed {
                                node_id,
                                label: None,
                                ticket_id,
                                error,
                                percentage: calc_percentage(complete_nodes, total_nodes),
                            };
                        }

                        LatticeEvent::GraphDone { graph_id } => {
                            yield OrchaEvent::Complete { session_id: graph_id };
                            return;
                        }

                        LatticeEvent::GraphFailed { graph_id, node_id, error } => {
                            yield OrchaEvent::Failed {
                                session_id: graph_id,
                                error: format!("Node {node_id} failed: {error}"),
                            };
                            return;
                        }

                        LatticeEvent::NodeStarted { .. } => {}
                    }
                }
                chunk_event = output_rx.recv() => {
                    if let Some(evt) = chunk_event {
                        yield evt;
                    }
                }
            }
        }
    }
}

/// Dispatch a single node to its type handler.
async fn dispatch_node(
    claudecode: Arc<ClaudeCode>,
    arbor: Arc<ArborStorage>,
    loopback_storage: Arc<LoopbackStorage>,
    pm: Arc<Pm>,
    graph_runtime: Arc<GraphRuntime>,
    cancel_registry: CancelRegistry,
    graph: &OrchaGraph,
    spec: &NodeSpec,
    node_id: &str,
    model: Model,
    working_directory: String,
    output_tx: tokio::sync::mpsc::UnboundedSender<OrchaEvent>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    ticket_id: Option<String>,
) -> Result<Option<NodeOutput>, String> {
    // Early-return for SubGraph — raw NodeSpec variant, not OrchaNodeKind
    if let NodeSpec::SubGraph { graph_id } = spec {
        return dispatch_subgraph(claudecode, arbor, loopback_storage, pm, graph_runtime, cancel_registry, graph, graph_id.clone(), node_id, model, working_directory, cancel_rx).await;
    }

    let data = match spec {
        NodeSpec::Task { data, .. } | NodeSpec::Scatter { data, .. } => data,
        _ => return Err("Engine-internal node type reached Orcha dispatcher".to_string()),
    };

    let kind: OrchaNodeKind = serde_json::from_value(data.clone())
        .map_err(|e| format!("Node data is not a valid OrchaNodeKind: {e}"))?;

    // Fetch and resolve input tokens (replaces old handle_context mechanism)
    let resolved_inputs = graph.get_resolved_inputs(node_id, &arbor).await?;

    match kind {
        OrchaNodeKind::Task { task, max_retries, .. } => {
            dispatch_task_with_retry(claudecode, loopback_storage, pm, task, resolved_inputs, node_id, model, working_directory, &graph.graph_id, output_tx, cancel_rx, ticket_id, max_retries.unwrap_or(0) as usize).await
        }
        OrchaNodeKind::Synthesize { task, max_retries, .. } => {
            dispatch_synthesize_with_retry(claudecode, arbor, loopback_storage, pm, graph, task, resolved_inputs, node_id, model, working_directory, output_tx, cancel_rx, ticket_id, max_retries.unwrap_or(0) as usize).await
        }
        OrchaNodeKind::Validate { command, cwd, max_retries } => {
            dispatch_validate_with_retry(claudecode, arbor, loopback_storage, pm, graph, node_id, command, cwd, model, working_directory, output_tx, cancel_rx, max_retries.unwrap_or(3) as usize).await
        }
        OrchaNodeKind::Review { prompt } => {
            dispatch_review(loopback_storage, &graph.graph_id, prompt, output_tx, cancel_rx).await
        }
        OrchaNodeKind::Plan { task } => {
            dispatch_plan(claudecode, arbor, loopback_storage, pm, graph_runtime, cancel_registry, graph, task, resolved_inputs, node_id, model, working_directory, output_tx, cancel_rx, ticket_id).await
        }
    }
}

/// Dispatch a "review" node — human-in-the-loop gate.
///
/// Creates a loopback approval record keyed on the `graph_id`, emits an
/// `ApprovalPending` event, then polls every second until the approval is
/// resolved.  On approval returns `Token::ok()`; on denial returns an error.
async fn dispatch_review(
    loopback_storage: Arc<LoopbackStorage>,
    graph_id: &str,
    prompt: String,
    output_tx: tokio::sync::mpsc::UnboundedSender<OrchaEvent>,
    mut cancel_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<Option<NodeOutput>, String> {
    // Use a generated UUID as the tool_use_id for this review gate.
    let tool_use_id = uuid::Uuid::new_v4().to_string();
    let input = serde_json::json!({ "prompt": prompt });

    let record = loopback_storage
        .create_approval(graph_id, "review", &tool_use_id, &input)
        .await
        .map_err(|e| format!("Failed to create review approval: {e}"))?;

    let approval_id = record.id;

    let _ = output_tx.send(OrchaEvent::ApprovalPending {
        approval_id: approval_id.to_string(),
        graph_id: graph_id.to_string(),
        tool_name: "review".to_string(),
        tool_input: input,
        created_at: chrono::Utc::now().to_rfc3339(),
    });

    loop {
        tokio::select! {
            result = cancel_rx.changed() => {
                if result.is_ok() && *cancel_rx.borrow() {
                    return Err("Graph cancelled".to_string());
                }
                // sender dropped — continue polling
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                match loopback_storage.get_approval(&approval_id).await {
                    Ok(r) => {
                        use crate::activations::claudecode_loopback::ApprovalStatus;
                        match r.status {
                            ApprovalStatus::Approved => {
                                return Ok(Some(NodeOutput::Single(Token::ok())));
                            }
                            ApprovalStatus::Denied => {
                                let reason = r.response_message.unwrap_or_default();
                                return Err(format!("Review denied: {reason}"));
                            }
                            _ => {} // still pending or timed out — keep polling
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to poll review approval {}: {}", approval_id, e);
                    }
                }
            }
        }
    }
}

/// Dispatch a "task" or "synthesize" node — creates a `ClaudeCode` session and runs the prompt.
///
/// Any resolved input tokens with `{"text": "..."}` data are concatenated as `<prior_work>`.
/// Loopback is enabled so tool-use approval requests are routed through the orcha approval API,
/// keyed by `graph_id` so callers can poll `list_pending_approvals(session_id=graph_id)`.
async fn dispatch_task(
    claudecode: Arc<ClaudeCode>,
    loopback_storage: Arc<LoopbackStorage>,
    pm: Arc<Pm>,
    task: String,
    resolved_inputs: Vec<crate::activations::lattice::ResolvedToken>,
    node_id: &str,
    model: Model,
    working_directory: String,
    graph_id: &str,
    output_tx: tokio::sync::mpsc::UnboundedSender<OrchaEvent>,
    mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    ticket_id: Option<String>,
) -> Result<Option<NodeOutput>, String> {
    // Build prior_work context from resolved input tokens
    let prior_work: Vec<String> = resolved_inputs
        .into_iter()
        .filter_map(|t| {
            t.data.as_ref()
                .and_then(|v| v.get("text"))
                .and_then(|v| v.as_str())
                .map(std::string::ToString::to_string)
        })
        .collect();

    let prompt = if prior_work.is_empty() {
        task
    } else {
        format!("<prior_work>\n{}\n</prior_work>\n\n{}", prior_work.join("\n\n"), task)
    };

    // Validate working directory before attempting to create a Claude session.
    // A missing directory causes the Claude CLI process to exit immediately with a
    // terse error that gives no hint of the root cause.
    if !std::path::Path::new(&working_directory).is_dir() {
        return Err(format!(
            "Working directory does not exist: '{working_directory}'. \
             Create it before running tickets or pass an existing path."
        ));
    }

    // Early-exit if the graph was already cancelled before this task started.
    if *cancel_rx.borrow() {
        return Err("Graph cancelled".to_string());
    }

    // Log the prompt and invocation context — seq 0.
    let model_str = format!("{model:?}").to_lowercase();
    pm.log_node_event(
        graph_id, node_id, ticket_id.as_deref(), 0, "prompt",
        serde_json::json!({
            "task": prompt,
            "model": model_str,
            "working_directory": working_directory,
            "prior_work_count": prior_work.len(),
        }),
    ).await;

    let mut log_seq: i64 = 1;

    let session_name = format!("lattice-node-{node_id}");

    // Spawn background auto-approver: watches loopback approvals for this graph session
    // and immediately approves them so Claude can proceed without manual intervention.
    // Uses a Notify-driven wakeup instead of polling: create_approval calls notify_one(),
    // so the approver wakes up only when a new approval request arrives.
    let lb = loopback_storage.clone();
    let gid = graph_id.to_string();
    let (approver_stop_tx, mut approver_stop_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let notifier = lb.get_or_create_notifier(&gid);
        loop {
            tokio::select! {
                _ = &mut approver_stop_rx => break,
                _ = notifier.notified() => {
                    if let Ok(pending) = lb.list_pending(Some(&gid)).await {
                        for approval in pending {
                            let _ = lb.resolve_approval(&approval.id, true, None).await;
                        }
                    }
                }
            }
        }
    });

    let create_stream = claudecode
        .create(session_name.clone(), working_directory, model, None, Some(true), Some(graph_id.to_string()))
        .await;
    tokio::pin!(create_stream);

    if let Some(result) = create_stream.next().await {
        match result {
            CreateResult::Ok { .. } => {}
            CreateResult::Err { message } => {
                let _ = approver_stop_tx.send(());
                return Err(format!("Failed to create claudecode session: {message}"));
            }
        }
    }

    let chat_stream = claudecode.chat(session_name, prompt, None, None).await;
    tokio::pin!(chat_stream);

    let mut output_text = String::new();
    let mut chat_error: Option<String> = None;

    loop {
        tokio::select! {
            // Watch for graph-level cancellation signal
            result = cancel_rx.changed() => {
                if result.is_ok() && *cancel_rx.borrow() {
                    let _ = approver_stop_tx.send(());
                    return Err("Graph cancelled".to_string());
                }
                // If the sender was dropped the graph is finishing — continue
            }
            // Read the next chat event
            maybe_event = chat_stream.next() => {
                match maybe_event {
                    None => break,
                    Some(ChatEvent::Content { text }) => {
                        let _ = output_tx.send(OrchaEvent::NodeOutput {
                            node_id: node_id.to_string(),
                            ticket_id: ticket_id.clone(),
                            chunk: text.clone(),
                        });
                        output_text.push_str(&text);
                    }
                    Some(ChatEvent::Start { id, user_position }) => {
                        pm.log_node_event(
                            graph_id, node_id, ticket_id.as_deref(), log_seq, "start",
                            serde_json::json!({ "session_id": id, "user_position": user_position }),
                        ).await;
                        log_seq += 1;
                    }
                    Some(ChatEvent::ToolUse { tool_name, tool_use_id, input }) => {
                        pm.log_node_event(
                            graph_id, node_id, ticket_id.as_deref(), log_seq, "tool_use",
                            serde_json::json!({
                                "tool_name": tool_name,
                                "tool_use_id": tool_use_id,
                                "input": input,
                            }),
                        ).await;
                        log_seq += 1;
                    }
                    Some(ChatEvent::ToolResult { tool_use_id, output, is_error }) => {
                        pm.log_node_event(
                            graph_id, node_id, ticket_id.as_deref(), log_seq, "tool_result",
                            serde_json::json!({
                                "tool_use_id": tool_use_id,
                                "is_error": is_error,
                                // Truncate to avoid huge log entries for file reads etc.
                                "output_preview": output.chars().take(500).collect::<String>(),
                                "output_length": output.len(),
                            }),
                        ).await;
                        log_seq += 1;
                    }
                    Some(ChatEvent::Complete { claude_session_id, usage, .. }) => {
                        pm.log_node_event(
                            graph_id, node_id, ticket_id.as_deref(), log_seq, "complete",
                            serde_json::json!({
                                "claude_session_id": claude_session_id,
                                "output_length": output_text.len(),
                                "usage": usage,
                            }),
                        ).await;
                        log_seq += 1;
                        break;
                    }
                    Some(ChatEvent::Err { message }) => {
                        pm.log_node_event(
                            graph_id, node_id, ticket_id.as_deref(), log_seq, "error",
                            serde_json::json!({ "message": message }),
                        ).await;
                        log_seq += 1;
                        chat_error = Some(message);
                        break;
                    }
                    Some(ChatEvent::Passthrough { event_type, data, .. }) => {
                        pm.log_node_event(
                            graph_id, node_id, ticket_id.as_deref(), log_seq, "passthrough",
                            serde_json::json!({ "event_type": event_type, "data": data }),
                        ).await;
                        log_seq += 1;
                    }
                    Some(_) => {}
                }
            }
        }
    }

    let _ = approver_stop_tx.send(());

    if let Some(e) = chat_error {
        pm.log_node_event(
            graph_id, node_id, ticket_id.as_deref(), log_seq, "outcome",
            serde_json::json!({ "status": "error", "error": e }),
        ).await;
        return Err(e);
    }

    if output_text.is_empty() {
        let msg = "Task produced no output — Claude session returned empty text".to_string();
        pm.log_node_event(
            graph_id, node_id, ticket_id.as_deref(), log_seq, "outcome",
            serde_json::json!({ "status": "error", "error": msg }),
        ).await;
        return Err(msg);
    }

    pm.log_node_event(
        graph_id, node_id, ticket_id.as_deref(), log_seq, "outcome",
        serde_json::json!({
            "status": "ok",
            "output_length": output_text.len(),
            "output_preview": output_text.chars().take(500).collect::<String>(),
        }),
    ).await;

    Ok(Some(NodeOutput::Single(Token::ok_data(
        serde_json::json!({ "text": output_text }),
    ))))
}

/// Dispatch a "synthesize" node — like task, but prepends a `<join_context>` block
/// listing the intent of upstream task/synthesize nodes so the join agent knows
/// what each contributing branch was trying to accomplish.
async fn dispatch_synthesize(
    claudecode: Arc<ClaudeCode>,
    _arbor: Arc<ArborStorage>,
    loopback_storage: Arc<LoopbackStorage>,
    pm: Arc<Pm>,
    graph: &OrchaGraph,
    task: String,
    resolved_inputs: Vec<crate::activations::lattice::ResolvedToken>,
    node_id: &str,
    model: Model,
    working_directory: String,
    output_tx: tokio::sync::mpsc::UnboundedSender<OrchaEvent>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    ticket_id: Option<String>,
) -> Result<Option<NodeOutput>, String> {
    let upstream_ids = find_upstream_tasks(graph, node_id).await;

    let join_context = if upstream_ids.is_empty() {
        String::new()
    } else {
        let mut lines: Vec<String> = Vec::new();
        for id in &upstream_ids {
            let Ok(spec) = graph.get_node_spec(id).await else { continue };
            let task_text = match spec {
                NodeSpec::Task { data, .. } => {
                    let Ok(kind) = serde_json::from_value::<OrchaNodeKind>(data) else { continue };
                    match kind {
                        OrchaNodeKind::Task { task, .. } | OrchaNodeKind::Synthesize { task, .. } => {
                            task.chars().take(200).collect::<String>()
                        }
                        _ => continue,
                    }
                }
                _ => continue,
            };
            lines.push(format!("- {task_text}"));
        }
        if lines.is_empty() {
            String::new()
        } else {
            format!(
                "<join_context>\nYou are synthesizing the outputs of these upstream tasks:\n{}\n</join_context>\n\n",
                lines.join("\n")
            )
        }
    };

    let prompt = format!("{join_context}{task}");
    dispatch_task(claudecode, loopback_storage, pm, prompt, resolved_inputs, node_id, model, working_directory, &graph.graph_id, output_tx, cancel_rx, ticket_id).await
}

/// Dispatch a "subgraph" node — runs the child graph to completion.
///
/// On child `GraphDone` → emits `Token::ok_data({"child_graph_id": "..."})`.
/// On child `GraphFailed` → returns `Err(...)` so the parent node is failed.
async fn dispatch_subgraph(
    claudecode: Arc<ClaudeCode>,
    arbor: Arc<ArborStorage>,
    loopback_storage: Arc<LoopbackStorage>,
    pm: Arc<Pm>,
    graph_runtime: Arc<GraphRuntime>,
    cancel_registry: CancelRegistry,
    graph: &OrchaGraph,
    child_graph_id: String,
    _node_id: &str,
    model: Model,
    working_directory: String,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<Option<NodeOutput>, String> {
    let child = Arc::new(graph.open_child_graph(child_graph_id.clone()));
    // Child graphs don't have a ticket map — pass an empty map.
    let events = run_graph_execution(child, claudecode, arbor, loopback_storage, pm, graph_runtime, cancel_registry, model, working_directory, cancel_rx, HashMap::new());
    tokio::pin!(events);

    while let Some(event) = events.next().await {
        match event {
            OrchaEvent::Complete { .. } => {
                return Ok(Some(NodeOutput::Single(Token::ok_data(
                    serde_json::json!({ "child_graph_id": child_graph_id }),
                ))));
            }
            OrchaEvent::Failed { error, .. } => {
                return Err(format!("Child graph failed: {error}"));
            }
            _ => {}
        }
    }

    Err("Child graph stream ended without completion".to_string())
}

/// Dispatch a "plan" node — uses Claude to generate a ticket file, compiles it
/// into a child graph, executes that child graph, and streams its events.
///
/// Phases:
/// 1. Run Claude with the plan prompt → ticket source (raw text)
/// 2. Compile the ticket source into nodes + edges
/// 3. Build a child graph under the current graph
/// 4. Execute the child graph, forwarding events to the parent stream
async fn dispatch_plan(
    claudecode: Arc<ClaudeCode>,
    arbor: Arc<ArborStorage>,
    loopback_storage: Arc<LoopbackStorage>,
    pm: Arc<Pm>,
    graph_runtime: Arc<GraphRuntime>,
    cancel_registry: CancelRegistry,
    graph: &OrchaGraph,
    task: String,
    resolved_inputs: Vec<crate::activations::lattice::ResolvedToken>,
    node_id: &str,
    model: Model,
    working_directory: String,
    output_tx: tokio::sync::mpsc::UnboundedSender<OrchaEvent>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    ticket_id: Option<String>,
) -> Result<Option<NodeOutput>, String> {
    // Phase 1 — run Claude to generate ticket source
    let ticket_result = dispatch_task(
        claudecode.clone(),
        loopback_storage.clone(),
        pm.clone(),
        task,
        resolved_inputs,
        node_id,
        model,
        working_directory.clone(),
        &graph.graph_id,
        output_tx.clone(),
        cancel_rx.clone(),
        ticket_id,
    )
    .await?;

    let ticket_source = match ticket_result {
        Some(NodeOutput::Single(ref token)) => {
            token
                .payload
                .as_ref()
                .and_then(|p| match p {
                    TokenPayload::Data { value } => {
                        value.get("text").and_then(|v| v.as_str()).map(std::string::ToString::to_string)
                    }
                    TokenPayload::Handle(_) => None,
                })
                .ok_or_else(|| "Plan task produced no text output".to_string())?
        }
        _ => return Err("Plan task produced no output".to_string()),
    };

    // Phase 2 — compile ticket source
    let compiled = crate::activations::orcha::ticket_compiler::compile_tickets(&ticket_source)
        .map_err(|e| format!("Plan ticket compile error: {e}"))?;

    // Phase 3 — build child graph
    let child_metadata = serde_json::json!({
        "_plexus_run_config": {
            "model": format!("{model:?}").to_lowercase(),
            "working_directory": working_directory,
        },
        "parent_graph_id": graph.graph_id,
        "plan_node_id": node_id,
    });

    let (child_graph_id, id_map) = graph_runtime
        .build_child_graph(&graph.graph_id, child_metadata, compiled.nodes, compiled.edges)
        .await?;

    let node_to_ticket: HashMap<String, String> = id_map
        .iter()
        .map(|(ticket, node)| (node.clone(), ticket.clone()))
        .collect();

    pm.save_ticket_map(&child_graph_id, &id_map)
        .await
        .map_err(|e| format!("Failed to save ticket map: {e}"))?;
    pm.save_ticket_source(&child_graph_id, &ticket_source)
        .await
        .map_err(|e| format!("Failed to save ticket source: {e}"))?;

    // Phase 4 — execute child graph
    // Register a cancel token so the child can be cancelled via cancel_graph.
    let (child_cancel_tx, _child_cancel_rx) = tokio::sync::watch::channel(false);
    cancel_registry.lock().await.insert(child_graph_id.clone(), child_cancel_tx);

    let child_arc = Arc::new(graph_runtime.open_graph(child_graph_id.clone()));
    // Pass the parent cancel_rx — if the parent is cancelled, the child stops too.
    let events = run_graph_execution(
        child_arc,
        claudecode,
        arbor,
        loopback_storage,
        pm,
        graph_runtime,
        cancel_registry.clone(),
        model,
        working_directory,
        cancel_rx,
        node_to_ticket,
    );
    tokio::pin!(events);

    while let Some(event) = events.next().await {
        match event {
            OrchaEvent::Complete { .. } => {
                cancel_registry.lock().await.remove(&child_graph_id);
                return Ok(Some(NodeOutput::Single(Token::ok_data(
                    serde_json::json!({ "child_graph_id": child_graph_id }),
                ))));
            }
            OrchaEvent::Failed { error, .. } => {
                cancel_registry.lock().await.remove(&child_graph_id);
                return Err(format!("Plan child graph failed: {error}"));
            }
            evt => {
                let _ = output_tx.send(evt);
            }
        }
    }

    cancel_registry.lock().await.remove(&child_graph_id);
    Err("Plan child graph stream ended without completion".to_string())
}

/// Dispatch a "validate" node — runs a shell command and checks the exit code.
async fn dispatch_validate(
    command: String,
    cwd: Option<String>,
) -> Result<Option<NodeOutput>, String> {
    let cwd = cwd.unwrap_or_else(|| "/workspace".to_string());

    let output = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(&command)
        .current_dir(&cwd)
        .output()
        .await
        .map_err(|e| format!("Failed to run validate command: {e}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let combined = format!("{stdout}{stderr}").trim().to_string();

    if output.status.success() {
        Ok(Some(NodeOutput::Single(Token::ok())))
    } else {
        Err(format!(
            "Validation failed (exit {}): {}",
            output.status.code().unwrap_or(-1),
            combined
        ))
    }
}

/// Walk backwards from `start_id` (exclusive) through gather/validate/review
/// nodes, collecting all reachable task/synthesize node IDs.
///
/// This handles:
/// - Simple case: validate → task  (one level up)
/// - Join case:   validate → gather → [task-A, task-B]  (traverse through gather)
/// - Chained:     validate → validate → task  (traverse through intermediate validates)
async fn find_upstream_tasks(graph: &OrchaGraph, start_id: &str) -> Vec<String> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut tasks: Vec<String> = Vec::new();

    let direct = graph.get_inbound_node_ids(start_id).await.unwrap_or_default();
    queue.extend(direct);

    while let Some(node_id) = queue.pop_front() {
        if !visited.insert(node_id.clone()) {
            continue;
        }
        let Ok(spec) = graph.get_node_spec(&node_id).await else { continue };
        if let NodeSpec::Task { data, .. } = spec {
            let Ok(kind) = serde_json::from_value::<OrchaNodeKind>(data) else { continue };
            match kind {
                OrchaNodeKind::Task { .. } | OrchaNodeKind::Synthesize { .. } => {
                    tasks.push(node_id);
                    // Stop here — don't traverse further up through a task
                }
                // Validate/Review node in disguise — keep traversing
                _ => {
                    let preds = graph.get_inbound_node_ids(&node_id).await.unwrap_or_default();
                    queue.extend(preds);
                }
            }
        } else {
            // Gather / Scatter / SubGraph — engine-internal, traverse through
            let preds = graph.get_inbound_node_ids(&node_id).await.unwrap_or_default();
            queue.extend(preds);
        }
    }

    tasks
}

/// Extract the `{"text": "..."}` string from a `NodeOutput`, if present.
fn output_text(output: &NodeOutput) -> Option<String> {
    if let NodeOutput::Single(token) = output {
        if let Some(TokenPayload::Data { value }) = &token.payload {
            return value.get("text").and_then(|v| v.as_str()).map(std::string::ToString::to_string);
        }
    }
    None
}

fn is_empty_output(output: &NodeOutput) -> bool {
    output_text(output).is_none_or(|t| t.is_empty())
}

async fn dispatch_task_with_retry(
    claudecode: Arc<ClaudeCode>,
    loopback_storage: Arc<LoopbackStorage>,
    pm: Arc<Pm>,
    task: String,
    resolved_inputs: Vec<crate::activations::lattice::ResolvedToken>,
    node_id: &str,
    model: Model,
    working_directory: String,
    graph_id: &str,
    output_tx: tokio::sync::mpsc::UnboundedSender<OrchaEvent>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    ticket_id: Option<String>,
    max_retries: usize,
) -> Result<Option<NodeOutput>, String> {
    let mut last_error: Option<String> = None;
    for attempt in 0..=max_retries {
        if attempt > 0 {
            let secs = 1u64 << (attempt - 1).min(3);
            tokio::time::sleep(tokio::time::Duration::from_secs(secs)).await;
            let _ = output_tx.send(OrchaEvent::Retrying {
                node_id: node_id.to_string(),
                ticket_id: ticket_id.clone(),
                attempt,
                max_attempts: max_retries,
                error: last_error.clone().unwrap_or_default(),
            });
        }
        match dispatch_task(
            claudecode.clone(), loopback_storage.clone(), pm.clone(),
            task.clone(), resolved_inputs.clone(), node_id, model,
            working_directory.clone(), graph_id, output_tx.clone(), cancel_rx.clone(),
            ticket_id.clone(),
        ).await {
            Ok(output) => {
                let empty = output.as_ref().is_none_or(is_empty_output);
                if !empty {
                    return Ok(output);
                }
                last_error = Some("Task produced no output".to_string());
            }
            Err(e) => {
                if attempt >= max_retries {
                    return Err(e);
                }
                last_error = Some(e);
            }
        }
    }
    Err(last_error.unwrap_or_else(|| "Task failed after all retries".to_string()))
}

async fn dispatch_synthesize_with_retry(
    claudecode: Arc<ClaudeCode>,
    arbor: Arc<ArborStorage>,
    loopback_storage: Arc<LoopbackStorage>,
    pm: Arc<Pm>,
    graph: &OrchaGraph,
    task: String,
    resolved_inputs: Vec<crate::activations::lattice::ResolvedToken>,
    node_id: &str,
    model: Model,
    working_directory: String,
    output_tx: tokio::sync::mpsc::UnboundedSender<OrchaEvent>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    ticket_id: Option<String>,
    max_retries: usize,
) -> Result<Option<NodeOutput>, String> {
    let mut last_error: Option<String> = None;
    for attempt in 0..=max_retries {
        if attempt > 0 {
            let secs = 1u64 << (attempt - 1).min(3);
            tokio::time::sleep(tokio::time::Duration::from_secs(secs)).await;
            let _ = output_tx.send(OrchaEvent::Retrying {
                node_id: node_id.to_string(),
                ticket_id: ticket_id.clone(),
                attempt,
                max_attempts: max_retries,
                error: last_error.clone().unwrap_or_default(),
            });
        }
        match dispatch_synthesize(
            claudecode.clone(), arbor.clone(), loopback_storage.clone(), pm.clone(),
            graph, task.clone(), resolved_inputs.clone(), node_id, model,
            working_directory.clone(), output_tx.clone(), cancel_rx.clone(),
            ticket_id.clone(),
        ).await {
            Ok(output) => {
                let empty = output.as_ref().is_none_or(is_empty_output);
                if !empty {
                    return Ok(output);
                }
                last_error = Some("Synthesize produced no output".to_string());
            }
            Err(e) => {
                if attempt >= max_retries {
                    return Err(e);
                }
                last_error = Some(e);
            }
        }
    }
    Err(last_error.unwrap_or_else(|| "Synthesize failed after all retries".to_string()))
}

/// Validate with automatic agent retry on failure.
///
/// Two improvements over a bare validate call:
///
/// 1. **Retry across joins**: Uses BFS to find ALL upstream task/synthesize
///    nodes, traversing through gather nodes and chained validates.  All of
///    them are re-run on each retry so the validation has the latest work from
///    every contributing agent.
///
/// 2. **Token passthrough**: On success, emits `Token::ok_data({"text": ...})`
///    carrying the concatenated latest output from every upstream task, instead
///    of bare `Token::ok()`.  A downstream `[agent/synthesize]` blocked on this
///    validate node therefore receives fresh `<prior_work>` context regardless
///    of how many retries occurred.
async fn dispatch_validate_with_retry(
    claudecode: Arc<ClaudeCode>,
    arbor: Arc<ArborStorage>,
    loopback_storage: Arc<LoopbackStorage>,
    pm: Arc<Pm>,
    graph: &OrchaGraph,
    validate_node_id: &str,
    command: String,
    cwd: Option<String>,
    model: Model,
    working_directory: String,
    output_tx: tokio::sync::mpsc::UnboundedSender<OrchaEvent>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    max_retries: usize,
) -> Result<Option<NodeOutput>, String> {

    // BFS: find every task/synthesize node that (transitively) feeds this validate.
    let task_ids = find_upstream_tasks(graph, validate_node_id).await;

    // Seed task_outputs from whatever is already stored in the lattice.
    // These will be overwritten on each successful retry dispatch.
    let mut task_outputs: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for tid in &task_ids {
        if let Ok(Some(output)) = graph.get_node_output(tid).await {
            if let Some(text) = output_text(&output) {
                task_outputs.insert(tid.clone(), text);
            }
        }
    }

    let mut error_context: Option<String> = None;

    for attempt in 0..=max_retries {
        if let Some(ref err) = error_context {
            let _ = output_tx.send(OrchaEvent::Retrying {
                node_id: validate_node_id.to_string(),
                ticket_id: None,
                attempt,
                max_attempts: max_retries,
                error: err.clone(),
            });
            if task_ids.is_empty() {
                return Err(format!(
                    "Validation failed with no upstream task to retry: {err}"
                ));
            }

            // Re-run every upstream task sequentially with the error as context.
            for tid in &task_ids {
                let Ok(spec) = graph.get_node_spec(tid).await else { continue };
                let data = match spec {
                    NodeSpec::Task { data, .. } => data,
                    _ => continue,
                };
                let Ok(kind) = serde_json::from_value::<OrchaNodeKind>(data) else { continue };
                let task_text = match kind {
                    OrchaNodeKind::Task { task, .. } | OrchaNodeKind::Synthesize { task, .. } => task,
                    _ => continue,
                };

                let resolved = graph.get_resolved_inputs(tid, &arbor).await.unwrap_or_default();

                let retry_prompt = format!(
                    "{task_text}\n\n\
                     <validation_error attempt=\"{attempt}\">\n\
                     The validation command `{command}` failed:\n\
                     {err}\n\
                     Please fix the issue and try again.\n\
                     </validation_error>"
                );

                if let Ok(Some(ref output)) = dispatch_task(
                    claudecode.clone(), loopback_storage.clone(), pm.clone(), retry_prompt, resolved,
                    tid, model, working_directory.clone(), &graph.graph_id, output_tx.clone(), cancel_rx.clone(), None,
                ).await {
                    if let Some(text) = output_text(output) {
                        task_outputs.insert(tid.clone(), text);
                    }
                }
            }
        }

        match dispatch_validate(command.clone(), cwd.clone()).await {
            Ok(_) => {
                // Pass the latest task output text through as this node's token.
                // Downstream synthesize nodes receive fresh <prior_work> context.
                let combined: String = task_outputs.values()
                    .filter(|t| !t.is_empty())
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("\n\n");

                let token = if combined.is_empty() {
                    Token::ok()
                } else {
                    Token::ok_data(serde_json::json!({ "text": combined }))
                };
                return Ok(Some(NodeOutput::Single(token)));
            }
            Err(e) => {
                if attempt >= max_retries {
                    return Err(format!(
                        "Validation failed after {max_retries} retries. Last error: {e}"
                    ));
                }
                error_context = Some(e);
            }
        }
    }

    unreachable!()
}
