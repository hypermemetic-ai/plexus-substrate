use super::context::OrchaContext;
use super::storage::OrchaStorage;
use super::types::{RunTaskRequest, OrchaEvent, AgentMode, SessionState, ValidationArtifact, ValidationResult, AgentInfo, AgentState, SessionId};
use crate::activations::arbor::ArborStorage;
use crate::activations::claudecode::{ChatEvent, ClaudeCode, Model};
use crate::activations::claudecode_loopback::ClaudeCodeLoopback;
use async_stream::stream;
use futures::Stream;
use futures::StreamExt;
use std::sync::Arc;
use uuid::Uuid;

/// Run a complete orchestration task with full approval loop and validation
///
/// This is the main orchestration function that:
/// 1. Creates a claudecode session with intelligent approval handling via loopback
/// 2. Starts the chat and polls for events
/// 3. Handles approval requests automatically
/// 4. Extracts validation artifacts from agent output
/// 5. Runs tests and auto-retries on failure
///
/// Returns a stream of `OrchaEvent` items showing progress
pub(super) async fn run_orchestration_task(
    storage: Arc<OrchaStorage>,
    arbor: Arc<ArborStorage>,
    claudecode: Arc<ClaudeCode>,
    loopback: Arc<ClaudeCodeLoopback>,
    request: RunTaskRequest,
    session_id_override: Option<String>,
) -> impl Stream<Item = OrchaEvent> + Send + 'static {
    stream! {
        // 1. Create Orcha context for tracking orchestration events
        let session_id = session_id_override.unwrap_or_else(|| format!("orcha-{}", Uuid::new_v4()));
        let ctx = match OrchaContext::new(
            arbor.clone(),
            session_id.clone(),
            request.task.clone(),
            request.model.clone(),
        ).await {
            Ok(c) => Arc::new(c),
            Err(e) => {
                yield OrchaEvent::Failed {
                    session_id: session_id.clone(),
                    error: e,
                };
                return;
            }
        };

        // 2. Create orcha session to track state
        let _session = match storage.create_session(
            session_id.clone(),
            request.model.clone(),
            request.working_directory.clone(),
            request.rules.clone(),
            request.max_retries,
            AgentMode::Single, // run_task uses single-agent mode
            Some(ctx.tree_id()),
        ).await {
            Ok(s) => s,
            Err(e) => {
                yield OrchaEvent::Failed {
                    session_id: session_id.clone(),
                    error: format!("Failed to create orcha session: {e}"),
                };
                return;
            }
        };

        if request.verbose {
            yield OrchaEvent::StateChange {
                session_id: session_id.clone(),
                state: SessionState::Idle,
            };
        }

        // Parse model string to Model enum (once, stable across retries)
        let model = match request.model.as_str() {
            "opus" => Model::Opus,
            "sonnet" => Model::Sonnet,
            "haiku" => Model::Haiku,
            _ => Model::Sonnet,
        };

        // Stable session name for this Orcha session's claudecode record.
        let cc_session_name = format!("{session_id}-cc");

        // 2. Create claudecode session with loopback enabled (once before retry loop)
        if request.verbose {
            yield OrchaEvent::Progress {
                message: format!("Creating Claude Code session: {cc_session_name}"),
                percentage: Some(10.0),
            };
        }

        // loopback_session_id will be set to cc_session_id after create() returns.
        // We pass None here and update storage directly after, since create() doesn't
        // know the cc_session_id until after the DB insert.
        let create_stream = claudecode.create(
            cc_session_name.clone(),
            request.working_directory.clone(),
            model,
            None, // system_prompt
            Some(true), // loopback_enabled
            None, // loopback_session_id set below after we have cc_session_id
        ).await;
        tokio::pin!(create_stream);

        let mut cc_session_id: Option<String> = None;
        while let Some(result) = create_stream.next().await {
            match result {
                crate::activations::claudecode::CreateResult::Ok { id, .. } => {
                    let id_str = id.to_string();
                    // Use cc_session_id as the loopback session_id for MCP URL correlation.
                    // Register the parent so Orcha notifiers fire when child approvals arrive.
                    loopback.storage().register_session_parent(&id_str, &session_id);
                    // Update the claudecode session's loopback_session_id
                    let _ = claudecode.storage.session_update_loopback_id(&id, id_str.clone()).await;
                    cc_session_id = Some(id_str.clone());
                    ctx.claude_session_created(id_str, cc_session_name.clone()).await;
                    if request.verbose {
                        yield OrchaEvent::Progress {
                            message: format!("Created Claude Code session: {id}"),
                            percentage: Some(20.0),
                        };
                    }
                }
                crate::activations::claudecode::CreateResult::Err { message } => {
                    yield OrchaEvent::Failed {
                        session_id: session_id.clone(),
                        error: format!("Failed to create Claude Code session: {message}"),
                    };
                    return;
                }
            }
        }

        let cc_session_id = if let Some(id) = cc_session_id { id } else {
            yield OrchaEvent::Failed {
                session_id: session_id.clone(),
                error: "Failed to create Claude Code session: no response".to_string(),
            };
            return;
        };

        // Retry loop for validation failures
        let mut retry_count = 0;
        loop {
            // 3. Start the chat
            if request.verbose {
                yield OrchaEvent::Progress {
                    message: "Starting task execution".to_string(),
                    percentage: Some(30.0),
                };
            }

            let task_prompt = if retry_count > 0 {
                format!(
                    "{}\n\n[RETRY {}/{}] The previous attempt failed validation. Please fix the issues and try again.",
                    request.task, retry_count, request.max_retries
                )
            } else {
                request.task.clone()
            };

            // Record task prompt in arbor via context
            ctx.prompt_created(task_prompt.clone(), retry_count).await;

            let chat_stream = claudecode.chat(
                cc_session_name.clone(),
                task_prompt,
                None, // ephemeral
                None,
            ).await;
            tokio::pin!(chat_stream);

            // Track accumulated text for validation extraction
            let mut accumulated_text = String::new();
            let mut validation_artifact: Option<ValidationArtifact> = None;

            // Update orcha session state to Running
            if let Err(e) = storage.update_state(&session_id, SessionState::Running {
                stream_id: cc_session_name.clone(),
                sequence: 0,
                active_agents: 0,  // Single-agent mode
                completed_agents: 0,
                failed_agents: 0,
            }).await {
                tracing::warn!("Failed to update session {} state to Running: {}", session_id, e);
            }

            if request.verbose {
                yield OrchaEvent::StateChange {
                    session_id: session_id.clone(),
                    state: SessionState::Running {
                        stream_id: cc_session_name.clone(),
                        sequence: 0,
                        active_agents: 0,  // Single-agent mode
                        completed_agents: 0,
                        failed_agents: 0,
                    },
                };
            }

            // 4. Process chat events
            while let Some(event) = chat_stream.next().await {
                match event {
                    ChatEvent::Start { .. } => {
                        if request.verbose {
                            yield OrchaEvent::Progress {
                                message: "Agent started processing task".to_string(),
                                percentage: Some(40.0),
                            };
                        }
                    }
                    ChatEvent::Content { text } => {
                        accumulated_text.push_str(&text);

                        // Only emit output if verbose
                        if request.verbose {
                            yield OrchaEvent::Output { text };
                        }

                        // Try to extract validation artifact
                        if validation_artifact.is_none() {
                            if let Some(artifact) = extract_validation_artifact(&accumulated_text) {
                                if request.verbose {
                                    yield OrchaEvent::ValidationArtifact {
                                        test_command: artifact.test_command.clone(),
                                        cwd: artifact.cwd.clone(),
                                    };
                                }
                                validation_artifact = Some(artifact);
                            }
                        }
                    }
                    ChatEvent::ToolUse { tool_name, tool_use_id, input } => {
                        // Register tool_use_id with loopback for approval tracking
                        loopback.storage().register_tool_session(&tool_use_id, &cc_session_id);

                        // Spawn background task to handle approval decision
                        let loopback_clone = loopback.clone();
                        let claudecode_clone = claudecode.clone();
                        let session_id_clone = session_id.clone();
                        let cc_session_id_clone = cc_session_id.clone();
                        let tool_name_clone = tool_name.clone();
                        let tool_use_id_clone = tool_use_id.clone();
                        let input_clone = input.clone();
                        let task_context = request.task.clone();
                        let auto_approve = request.auto_approve;

                        tokio::spawn(async move {
                            handle_tool_approval(
                                loopback_clone,
                                claudecode_clone,
                                session_id_clone,
                                cc_session_id_clone,
                                tool_name_clone,
                                tool_use_id_clone,
                                input_clone,
                                task_context,
                                auto_approve,
                            ).await;
                        });

                        // Only emit tool events if verbose
                        if request.verbose {
                            yield OrchaEvent::ToolUse {
                                tool_id: tool_use_id,
                                tool_name,
                                input,
                            };
                        }
                    }
                    ChatEvent::ToolResult { tool_use_id, output, is_error } => {
                        // Only emit tool results if verbose
                        if request.verbose {
                            yield OrchaEvent::ToolResult {
                                tool_id: tool_use_id,
                                content: output,
                                is_error,
                            };
                        }
                    }
                    ChatEvent::Complete { .. } => {
                        // Record Claude session completion in arbor via context
                        ctx.claude_session_complete(cc_session_id.clone()).await;

                        if request.verbose {
                            yield OrchaEvent::Progress {
                                message: "Agent completed task".to_string(),
                                percentage: Some(70.0),
                            };
                        }
                        break;
                    }
                    ChatEvent::Err { message } => {
                        yield OrchaEvent::Failed {
                            session_id: session_id.clone(),
                            error: format!("Chat error: {message}"),
                        };
                        return;
                    }
                    _ => {
                        // Ignore other events (Thinking, Passthrough, etc.)
                    }
                }
            }

            // 5. Run validation if artifact was found
            if let Some(artifact) = validation_artifact {
                if request.verbose {
                    yield OrchaEvent::ValidationStarted {
                        test_command: artifact.test_command.clone(),
                    };
                }

                if let Err(e) = storage.update_state(&session_id, SessionState::Validating {
                    test_command: artifact.test_command.clone(),
                }).await {
                    tracing::warn!("Failed to update session {} state to Validating: {}", session_id, e);
                }

                // Record validation start in arbor via context
                ctx.validation_started(artifact.test_command.clone(), artifact.cwd.clone()).await;

                // Run the validation test
                let validation_result = run_validation_test(&artifact).await;

                // Record validation result in arbor via context
                ctx.validation_result(validation_result.success, validation_result.output.clone()).await;

                if request.verbose {
                    yield OrchaEvent::ValidationResult {
                        success: validation_result.success,
                        output: validation_result.output.clone(),
                    };
                }

                if validation_result.success {
                    // Success! Mark complete and exit
                    if let Err(e) = storage.update_state(&session_id, SessionState::Complete).await {
                        tracing::warn!("Failed to update session {} state to Complete: {}", session_id, e);
                    }

                    // Record session completion in arbor via context
                    ctx.session_complete("success").await;

                    if request.verbose {
                        yield OrchaEvent::StateChange {
                            session_id: session_id.clone(),
                            state: SessionState::Complete,
                        };
                    }

                    yield OrchaEvent::Complete {
                        session_id: session_id.clone(),
                    };
                    return;
                }
                // Validation failed - check if we can retry
                retry_count += 1;

                if retry_count >= request.max_retries {
                    // Max retries exceeded
                    if let Err(e) = storage.update_state(&session_id, SessionState::Failed {
                        error: format!("Validation failed after {retry_count} retries"),
                    }).await {
                        tracing::warn!("Failed to update session {} state to Failed: {}", session_id, e);
                    }

                    yield OrchaEvent::Failed {
                        session_id: session_id.clone(),
                        error: format!("Validation failed after {retry_count} retries"),
                    };
                    return;
                }
                // Increment retry counter and loop again
                if let Err(e) = storage.increment_retry(&session_id).await {
                    tracing::warn!("Failed to increment retry counter for session {}: {}", session_id, e);
                }

                if request.verbose {
                    yield OrchaEvent::RetryAttempt {
                        attempt: retry_count,
                        max_retries: request.max_retries,
                        reason: "Validation test failed".to_string(),
                    };
                }

                // Continue to next iteration of the retry loop
                continue;
            }
            // No validation artifact found - treat as success
            if let Err(e) = storage.update_state(&session_id, SessionState::Complete).await {
                tracing::warn!("Failed to update session {} state to Complete: {}", session_id, e);
            }

            // Record session completion in arbor via context
            ctx.session_complete("success_no_validation").await;

            if request.verbose {
                yield OrchaEvent::StateChange {
                    session_id: session_id.clone(),
                    state: SessionState::Complete,
                };
            }

            yield OrchaEvent::Complete {
                session_id: session_id.clone(),
            };
            return;
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Helper Functions
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
    use tokio::process::Command;

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

/// Handle tool approval by spawning an ephemeral Claude session to decide
///
/// This function:
/// 1. Waits for the approval request to be created by loopback
/// 2. Spawns an ephemeral Claude session with orcha context
/// 3. Asks Claude whether to approve/deny the tool use
/// 4. Resolves the approval accordingly
async fn handle_tool_approval(
    loopback: Arc<ClaudeCodeLoopback>,
    claudecode: Arc<ClaudeCode>,
    orcha_session_id: String,
    cc_session_id: String,
    tool_name: String,
    tool_use_id: String,
    tool_input: serde_json::Value,
    task_context: String,
    auto_approve: bool,
) {
    use futures::StreamExt;

    // Wait for loopback.permit() to create the approval record.
    // Approvals are keyed by cc_session_id (the Claude Code session that owns the tool call).
    // The cc_session's notifier also propagates to orcha_session_id via register_session_parent.
    let notifier = loopback.storage().get_or_create_notifier(&cc_session_id);
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(3600);

    let approval_id = loop {
        let approvals = loopback.storage().get_pending_approvals(&cc_session_id).await;
        if let Some(a) = approvals.iter().find(|a| a.tool_use_id == tool_use_id) {
            break a.id;
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            tracing::warn!("Timed out waiting for approval for tool_use_id: {}", tool_use_id);
            return;
        }

        let remaining = deadline - now;
        let _ = tokio::time::timeout(remaining, notifier.notified()).await;
    };

    // Manual approval mode: do nothing, wait for external approval
    if !auto_approve {
        tracing::info!(
            "Auto-approval disabled for session {}. Waiting for manual approval of tool: {}",
            orcha_session_id,
            tool_name
        );
        return;
    }

    // Auto-approval mode: spawn Haiku decision agent
    // Create ephemeral session for approval decision
    let decision_session = format!("orcha-approval-{}", uuid::Uuid::new_v4());
    let decision_session_id = format!("{}-approval-{}", orcha_session_id, uuid::Uuid::new_v4());

    let create_stream = claudecode.create(
        decision_session.clone(),
        "/workspace".to_string(),
        crate::activations::claudecode::Model::Haiku, // Fast decision with Haiku
        None,
        Some(false), // No loopback for the decision agent
        Some(decision_session_id), // Track decision agent under parent session
    ).await;
    tokio::pin!(create_stream);

    // Wait for session creation
    let mut created = false;
    while let Some(result) = create_stream.next().await {
        if let crate::activations::claudecode::CreateResult::Ok { .. } = result {
            created = true;
            break;
        }
    }

    if !created {
        tracing::error!("Failed to create approval decision session");
        // Auto-deny if we can't create decision session
        if let Err(e) = loopback.storage().resolve_approval(
            &approval_id,
            false,
            Some("Failed to create approval decision session".to_string()),
        ).await {
            tracing::warn!("Failed to resolve approval {} after decision session creation failure: {}", approval_id, e);
        }
        return;
    }

    // Ask Claude to decide
    let prompt = format!(
        "You are an approval agent for an autonomous orcha orchestration system.\n\n\
         The main task is: \"{}\"\n\n\
         The agent is requesting permission to use this tool:\n\
         - Tool: {}\n\
         - Input: {}\n\n\
         Decide whether to APPROVE or DENY this tool use.\n\
         Respond with EXACTLY ONE WORD: either \"APPROVE\" or \"DENY\".\n\n\
         Guidelines:\n\
         - APPROVE if the tool use is safe and relevant to the task\n\
         - DENY if the tool could be dangerous, destructive, or irrelevant\n\
         - DENY any attempts to access sensitive files, run dangerous commands, or modify system files\n\
         - APPROVE standard development operations like Write, Read, Edit, Bash for build/test commands",
        task_context,
        tool_name,
        serde_json::to_string_pretty(&tool_input).unwrap_or_else(|_| tool_input.to_string())
    );

    let chat_stream = claudecode.chat(
        decision_session.clone(),
        prompt,
        Some(true), // Ephemeral
        None,
    ).await;
    tokio::pin!(chat_stream);

    let mut decision = String::new();
    while let Some(event) = chat_stream.next().await {
        if let crate::activations::claudecode::ChatEvent::Content { text } = event {
            decision.push_str(&text);
        }
    }

    // Parse decision
    let approved = decision.trim().to_uppercase().contains("APPROVE");

    // Resolve approval
    let message = if approved {
        format!("Approved by orcha approval agent: {}", decision.trim())
    } else {
        format!("Denied by orcha approval agent: {}", decision.trim())
    };

    match loopback.storage().resolve_approval(&approval_id, approved, Some(message)).await {
        Ok(_) => {
            tracing::info!("Tool use {} {} for session {}", tool_use_id, if approved { "approved" } else { "denied" }, orcha_session_id);
        }
        Err(e) => {
            tracing::error!("Failed to resolve approval: {}", e);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Multi-Agent Orchestration Functions
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration for running an agent task
#[allow(dead_code)]
pub(super) struct AgentConfig {
    pub model: Model,
    pub working_directory: String,
    pub max_retries: u32,
    pub task_context: String,
    pub auto_approve: bool,
}

/// Result of running an agent task
#[allow(dead_code)]
pub(super) enum AgentTaskResult {
    Success { validation_result: Option<ValidationResult> },
    Failed { error: String },
}

/// Run a single agent task (extracted core from `run_orchestration_task`)
///
/// This handles the lifecycle of ONE agent:
/// - Creates claudecode session
/// - Runs chat with approval handling
/// - Extracts and runs validation
/// - Handles retries
/// - Updates agent state (not session state)
///
/// Returns success/failure result (does not stream events)
pub(super) async fn run_agent_task(
    storage: Arc<OrchaStorage>,
    claudecode: Arc<ClaudeCode>,
    loopback: Arc<ClaudeCodeLoopback>,
    agent_info: AgentInfo,
    task: String,
    config: AgentConfig,
) -> AgentTaskResult {
    use futures::StreamExt;

    let mut retry_count = 0;

    loop {
        // Update agent state to Running
        if let Err(e) = storage.update_agent_state(&agent_info.agent_id, AgentState::Running {
            sequence: 0,
        }).await {
            tracing::warn!("Failed to update agent {} state to Running: {}", agent_info.agent_id, e);
        }

        // Build task prompt with retry context if needed
        let task_prompt = if retry_count > 0 {
            format!(
                "{}\n\n[RETRY {}/{}] Previous validation failed. Fix issues and try again.",
                task, retry_count, config.max_retries
            )
        } else {
            task.clone()
        };

        // Start the chat
        let chat_stream = claudecode.chat(
            agent_info.claudecode_session_id.clone(),
            task_prompt,
            None, // Not ephemeral
            None,
        ).await;
        tokio::pin!(chat_stream);

        let mut accumulated_text = String::new();
        let mut validation_artifact: Option<ValidationArtifact> = None;

        // Process chat events
        while let Some(event) = chat_stream.next().await {
            match event {
                ChatEvent::Content { text } => {
                    accumulated_text.push_str(&text);

                    // Try to extract validation artifact
                    if validation_artifact.is_none() {
                        validation_artifact = extract_validation_artifact(&accumulated_text);
                    }
                }
                ChatEvent::ToolUse { tool_name, tool_use_id, input } => {
                    // Check if this is a spawn_helper_agent request
                    if tool_name == "spawn_helper_agent" {
                        handle_agent_spawn_request(
                            storage.clone(),
                            claudecode.clone(),
                            loopback.clone(),
                            &agent_info,
                            input,
                            config.task_context.clone(),
                        ).await;
                    } else {
                        // Normal tool approval flow
                        loopback.storage().register_tool_session(&tool_use_id, &agent_info.session_id);

                        // Spawn approval handler
                        let loopback_clone = loopback.clone();
                        let claudecode_clone = claudecode.clone();
                        let session_id_clone = agent_info.session_id.clone();
                        let cc_session_id_clone = agent_info.session_id.clone();
                        let tool_name_clone = tool_name.clone();
                        let tool_use_id_clone = tool_use_id.clone();
                        let input_clone = input.clone();
                        let task_context = config.task_context.clone();
                        let auto_approve = config.auto_approve;

                        tokio::spawn(async move {
                            handle_tool_approval(
                                loopback_clone,
                                claudecode_clone,
                                session_id_clone,
                                cc_session_id_clone,
                                tool_name_clone,
                                tool_use_id_clone,
                                input_clone,
                                task_context,
                                auto_approve,
                            ).await;
                        });
                    }
                }
                ChatEvent::Complete { .. } => break,
                ChatEvent::Err { message } => {
                    if let Err(e) = storage.update_agent_state(
                        &agent_info.agent_id,
                        AgentState::Failed {
                            error: format!("Chat error: {message}"),
                        }
                    ).await {
                        tracing::warn!("Failed to update agent {} state to Failed: {}", agent_info.agent_id, e);
                    }

                    return AgentTaskResult::Failed {
                        error: message,
                    };
                }
                _ => {
                    // Ignore other events
                }
            }
        }

        // Run validation if found
        if let Some(artifact) = validation_artifact {
            if let Err(e) = storage.update_agent_state(
                &agent_info.agent_id,
                AgentState::Validating {
                    test_command: artifact.test_command.clone(),
                }
            ).await {
                tracing::warn!("Failed to update agent {} state to Validating: {}", agent_info.agent_id, e);
            }

            let validation_result = run_validation_test(&artifact).await;

            if validation_result.success {
                // Success!
                if let Err(e) = storage.update_agent_state(
                    &agent_info.agent_id,
                    AgentState::Complete,
                ).await {
                    tracing::warn!("Failed to update agent {} state to Complete: {}", agent_info.agent_id, e);
                }

                return AgentTaskResult::Success {
                    validation_result: Some(validation_result),
                };
            }
            // Validation failed - retry?
            retry_count += 1;

            if retry_count >= config.max_retries {
                if let Err(e) = storage.update_agent_state(
                    &agent_info.agent_id,
                    AgentState::Failed {
                        error: format!("Validation failed after {retry_count} retries"),
                    }
                ).await {
                    tracing::warn!("Failed to update agent {} state to Failed: {}", agent_info.agent_id, e);
                }

                return AgentTaskResult::Failed {
                    error: format!("Validation failed after {retry_count} retries"),
                };
            }
            // Loop and retry
            continue;
        }
        // No validation - treat as success
        if let Err(e) = storage.update_agent_state(
            &agent_info.agent_id,
            AgentState::Complete,
        ).await {
            tracing::warn!("Failed to update agent {} state to Complete: {}", agent_info.agent_id, e);
        }

        return AgentTaskResult::Success {
            validation_result: None,
        };
    }
}

/// Spawn a background task to run an agent
pub(super) fn spawn_agent_task(
    storage: Arc<OrchaStorage>,
    claudecode: Arc<ClaudeCode>,
    loopback: Arc<ClaudeCodeLoopback>,
    agent_info: AgentInfo,
    task: String,
    config: AgentConfig,
) {
    tokio::spawn(async move {
        // Run the agent task
        let result = run_agent_task(
            storage.clone(),
            claudecode,
            loopback,
            agent_info.clone(),
            task,
            config,
        ).await;

        match result {
            AgentTaskResult::Success { .. } => {
                tracing::info!("Agent {} completed successfully", agent_info.agent_id);
            }
            AgentTaskResult::Failed { error } => {
                tracing::error!("Agent {} failed: {}", agent_info.agent_id, error);
            }
        }

        // Check if session should complete (all agents done)
        check_session_completion(&storage, &agent_info.session_id).await;
    });
}

/// Check if all agents in a session are complete
pub(super) async fn check_session_completion(storage: &OrchaStorage, session_id: &SessionId) {
    match storage.get_agent_counts(session_id).await {
        Ok((active, completed, failed)) => {
            if active == 0 {
                // All agents are done (either completed or failed)
                let session = match storage.get_session(session_id).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!("Failed to get session {} during completion check: {}", session_id, e);
                        return;
                    }
                };

                // Only update if in multi-agent mode
                if session.agent_mode != AgentMode::Multi {
                    return;
                }

                // If ALL agents completed successfully, mark session complete
                if completed > 0 && failed == 0 {
                    if let Err(e) = storage.update_state(session_id, SessionState::Complete).await {
                        tracing::warn!("Failed to update session {} state to Complete: {}", session_id, e);
                    }
                } else if failed > 0 && completed == 0 {
                    // All failed
                    if let Err(e) = storage.update_state(
                        session_id,
                        SessionState::Failed {
                            error: format!("{failed} agents failed"),
                        }
                    ).await {
                        tracing::warn!("Failed to update session {} state to Failed: {}", session_id, e);
                    }
                } else {
                    // Mixed results - mark as complete (some succeeded)
                    if let Err(e) = storage.update_state(session_id, SessionState::Complete).await {
                        tracing::warn!("Failed to update session {} state to Complete: {}", session_id, e);
                    }
                }
            } else {
                // Update session state with agent counts
                let session = match storage.get_session(session_id).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!("Failed to get session {} during agent count update: {}", session_id, e);
                        return;
                    }
                };

                if let SessionState::Running { stream_id, .. } = &session.state {
                    if let Err(e) = storage.update_state(
                        session_id,
                        SessionState::Running {
                            stream_id: stream_id.clone(),
                            sequence: 0,
                            active_agents: active,
                            completed_agents: completed,
                            failed_agents: failed,
                        }
                    ).await {
                        tracing::warn!("Failed to update session {} agent counts: {}", session_id, e);
                    }
                }
            }
        }
        Err(e) => {
            tracing::error!("Failed to get agent counts for session {}: {}", session_id, e);
        }
    }
}

/// Handle agent spawn request (when an agent wants to spawn a helper)
async fn handle_agent_spawn_request(
    storage: Arc<OrchaStorage>,
    claudecode: Arc<ClaudeCode>,
    loopback: Arc<ClaudeCodeLoopback>,
    parent_agent: &AgentInfo,
    input: serde_json::Value,
    task_context: String,
) {
    use futures::StreamExt;

    // Extract subtask from input
    let subtask = if let Some(s) = input.get("subtask").and_then(|v| v.as_str()) { s.to_string() } else {
        tracing::warn!("spawn_helper_agent called without subtask");
        return;
    };

    // Get session to check model and working_directory
    let session = match storage.get_session(&parent_agent.session_id).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to get session for spawn request: {}", e);
            return;
        }
    };

    // Parse model
    let model = match session.model.as_str() {
        "opus" => Model::Opus,
        "sonnet" => Model::Sonnet,
        "haiku" => Model::Haiku,
        _ => Model::Sonnet,
    };

    // Create ClaudeCode session for this helper agent
    let cc_session_name = format!("orcha-agent-{}", Uuid::new_v4());

    let create_stream = claudecode.create(
        cc_session_name.clone(),
        "/workspace".to_string(), // TODO: Get from session
        model,
        None,
        Some(true), // Loopback enabled
        Some(session.session_id.clone()), // Use parent session_id for MCP URL transparency
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
        tracing::error!("Failed to create claudecode session for helper agent");
        return;
    }

    // Create agent record
    match storage.create_agent(
        &parent_agent.session_id,
        cc_session_name.clone(),
        subtask.clone(),
        false, // Not primary
        Some(parent_agent.agent_id.clone()),
    ).await {
        Ok(agent) => {
            tracing::info!(
                "Agent {} spawned helper agent {} for subtask: {}",
                parent_agent.agent_id,
                agent.agent_id,
                subtask
            );

            // Spawn background task to run this agent
            let config = AgentConfig {
                model,
                working_directory: "/workspace".to_string(),
                max_retries: session.max_retries,
                task_context,
                auto_approve: true, // TODO: Store in session and retrieve
            };

            spawn_agent_task(
                storage,
                claudecode,
                loopback,
                agent,
                subtask,
                config,
            );
        }
        Err(e) => {
            tracing::error!("Failed to create helper agent: {}", e);
        }
    }
}
