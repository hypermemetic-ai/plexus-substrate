use super::storage::{LoopbackStorage, LoopbackStorageConfig};
use super::types::{ApprovalStatus, ApprovalId, LoopbackError, RespondOk, PendingOk, WaitOutcome, ConfigureOk};
use async_stream::stream;
use futures::Stream;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// `ClaudeCode` Loopback - routes tool permissions back to parent for approval
#[derive(Clone)]
pub struct ClaudeCodeLoopback {
    storage: Arc<LoopbackStorage>,
    mcp_url: String,
}

impl ClaudeCodeLoopback {
    pub async fn new(config: LoopbackStorageConfig) -> Result<Self, String> {
        let storage = LoopbackStorage::new(config).await?;
        let mcp_url = std::env::var("PLEXUS_MCP_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:4445/mcp".to_string());

        Ok(Self {
            storage: Arc::new(storage),
            mcp_url,
        })
    }

    pub fn with_mcp_url(mut self, url: String) -> Self {
        self.mcp_url = url;
        self
    }

    /// Get the underlying storage (for sharing with `ClaudeCode`)
    pub fn storage(&self) -> Arc<LoopbackStorage> {
        self.storage.clone()
    }
}

#[plexus_macros::activation(namespace = "loopback",
version = "1.0.0",
description = "Route tool permissions to parent for approval")]
impl ClaudeCodeLoopback {
    /// Permission prompt handler - blocks until parent approves/denies
    ///
    /// This is called by Claude Code CLI via --permission-prompt-tool.
    /// It blocks (polls) until the parent calls `loopback.respond()`.
    ///
    /// Returns a JSON string (not object) because Claude Code expects the MCP response
    /// to have the permission JSON already stringified in content[0].text.
    /// See: <https://github.com/anthropics/claude-code/blob/main/docs/permission-prompt-tool.md>
    ///
    /// # PLX-116 RESIDUAL — deliberately NOT converted to a unary `Result`
    ///
    /// `permit` yields exactly once on every path, so by T1 it is a unary
    /// method. It is left as `impl Stream` anyway, because converting it is a
    /// change to the permission path and PLX-116 reserves those for PLX-105.
    ///
    /// The reason is measured, not cautious. A turn is projected onto the
    /// legacy `PlexusStream` with **the terminal emitted as another `Data`
    /// item** (`plexus-core/src/runtime/bridge.rs:333-381`), and the MCP
    /// gateway builds `content[0].text` from a *count-dependent* rule over the
    /// buffered items (`plexus-transport/src/mcp/bridge.rs:563-587`): exactly
    /// one item is rendered as that item, more than one as a pretty-printed
    /// array. Converting `permit` moves it from two items (the yielded string
    /// plus an empty terminal) to one (a terminal object wrapping the string
    /// under `value`), so the text the spawned Claude CLI reads back from its
    /// `--permission-prompt-tool` changes shape. That is the CLI's own
    /// protocol, not ours to alter here.
    ///
    /// # PLX-105 — measured, and it is worse than PLX-116 could see
    ///
    /// The path is no longer unguarded: `tests/plx105_permission_wire.rs` drives
    /// this method through `Activation::call_arc` and the real gateway rule and
    /// asserts the bytes. What it measures is that **the CLI contract is already
    /// broken, and was broken by the turn switchover rather than by any change
    /// to this file**.
    ///
    /// Since a `#[method]` returning `impl Stream` is dispatched through the
    /// turn runtime, the projection emits the yielded payload as a `.update`
    /// `Data` item *and the turn's terminal as a second `Data` item*. The MCP
    /// gateway buffers both — it never reads `content_type` — and two
    /// mixed-type items render as a pretty-printed JSON **array**. So
    /// `content[0].text` is
    /// `["{\"behavior\":\"allow\",…}", {"stop":{"kind":"complete"},"value":null}]`
    /// where the CLI's `--permission-prompt-tool` contract requires the object
    /// alone.
    ///
    /// Converting this method to a unary `Result` does **not** fix that: one
    /// buffered item is the terminal object, so the text becomes
    /// `{"stop":…,"value":…}` — a different wrong shape. Neither shape restores
    /// the contract, because the payload is only recoverable if the gateway
    /// learns to distinguish an update from a terminal, i.e. to read
    /// `content_type`. **That is a gateway-wide protocol decision affecting
    /// every MCP tool, not a loopback change**, and PLX-105 stopped rather than
    /// make it unilaterally. See the PLX-105 report.
    ///
    /// The poll loop below is therefore left in place: replacing it with an
    /// awaited turn callback is separately blocked (see the module README), and
    /// removing it while the response shape is broken would change two things at
    /// once.
    #[plexus_macros::method(params(
        tool_name = "Name of the tool being requested",
        tool_use_id = "Unique ID for this tool invocation",
        input = "Tool input parameters",
        _connection = "HTTP connection metadata (optional)" // Added for transparent query param forwarding
    ))]
    // `_connection` is load-bearing as a wire-facing parameter name (Plexus
    // transport injects connection metadata here for methods that opt in via
    // the `_`-prefix convention). The underscore prefix is part of the wire
    // contract, not a "kept for future use" marker — we do read the value
    // below when present. See workspace-level `clippy::used_underscore_binding
    // = "allow"` for the full justification.
    async fn permit(
        &self,
        tool_name: String,
        tool_use_id: String,
        input: Value,
        _connection: Option<Value>,
    ) -> impl Stream<Item = String> + Send + 'static {
        // IMMEDIATE DEBUG: Log before stream starts
        tracing::debug!("[LOOPBACK] permit called: tool={}, tool_use_id={}", tool_name, tool_use_id);

        let storage = self.storage.clone();

        // Try to get session_id from HTTP connection metadata first (transparent approach).
        // If not available, fall back to tool_use_id mapping (legacy approach).
        let session_id = _connection
            .as_ref()
            .and_then(|conn| conn.get("query.session_id"))
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string)
            .or_else(|| std::env::var("PLEXUS_SESSION_ID").ok())
            .or_else(|| storage.lookup_session_by_tool(&tool_use_id))
            .unwrap_or_else(|| "unknown".to_string());

        stream! {
            // DEBUG: Log the lookup result
            tracing::debug!("[LOOPBACK] permit: tool_use_id={} mapped to session_id={}", tool_use_id, session_id);

            // Create approval request
            let approval = match storage.create_approval(
                &session_id,
                &tool_name,
                &tool_use_id,
                &input,
            ).await {
                Ok(a) => a,
                Err(e) => {
                    // Return deny response as JSON string
                    let response = json!({
                        "behavior": "deny",
                        "message": format!("Failed to create approval: {}", e)
                    });
                    yield response.to_string();
                    return;
                }
            };

            let approval_id = approval.id;
            let timeout_secs = 300u64; // 5 minute timeout
            let poll_interval = Duration::from_secs(1);
            let start = std::time::Instant::now();

            // Blocking poll loop - like HumanLayer's hlyr
            loop {
                // Check timeout
                if start.elapsed().as_secs() > timeout_secs {
                    let _ = storage.resolve_approval(&approval_id, false, Some("Timed out".to_string())).await;
                    let response = json!({
                        "behavior": "deny",
                        "message": "Approval request timed out"
                    });
                    yield response.to_string();
                    return;
                }

                // Poll for resolution
                match storage.get_approval(&approval_id).await {
                    Ok(current) => {
                        match current.status {
                            ApprovalStatus::Approved => {
                                // Return allow response as JSON string
                                // Claude Code expects: {"behavior": "allow", "updatedInput": {...}}
                                let response = json!({
                                    "behavior": "allow",
                                    "updatedInput": input.clone()
                                });
                                yield response.to_string();
                                return;
                            }
                            ApprovalStatus::Denied => {
                                let response = json!({
                                    "behavior": "deny",
                                    "message": current.response_message.unwrap_or_else(|| "Denied by parent".to_string())
                                });
                                yield response.to_string();
                                return;
                            }
                            ApprovalStatus::TimedOut => {
                                let response = json!({
                                    "behavior": "deny",
                                    "message": "Approval timed out"
                                });
                                yield response.to_string();
                                return;
                            }
                            ApprovalStatus::Pending => {
                                // Continue polling
                            }
                        }
                    }
                    Err(e) => {
                        let response = json!({
                            "behavior": "deny",
                            "message": format!("Failed to check approval: {}", e)
                        });
                        yield response.to_string();
                        return;
                    }
                }

                sleep(poll_interval).await;
            }
        }
    }

    /// Respond to a pending approval request
    #[plexus_macros::method(params(
        approval_id = "ID of the approval request",
        approve = "Whether to approve (true) or deny (false)",
        message = "Optional message/reason"
    ))]
    async fn respond(
        &self,
        approval_id: ApprovalId,
        approve: bool,
        message: Option<String>,
    ) -> Result<RespondOk, LoopbackError> {
        // NOTE for PLX-105: `approve == false` is a *decision*, not a failure —
        // it resolves successfully here and the denial travels to the CLI as an
        // `Ok`-valued `{"behavior":"deny"}` payload out of `permit`. The `Err`
        // arm below is reserved for storage failures.
        self.storage.resolve_approval(&approval_id, approve, message).await?;
        Ok(RespondOk { approval_id })
    }

    /// List pending approval requests
    #[plexus_macros::method(params(
        session_id = "Optional session ID to filter by"
    ))]
    async fn pending(
        &self,
        session_id: Option<String>,
    ) -> Result<PendingOk, LoopbackError> {
        let approvals = self.storage.list_pending(session_id.as_deref()).await?;
        Ok(PendingOk { approvals })
    }

    /// Wait for a new approval request to arrive for a session
    ///
    /// This method blocks until a new approval arrives or the timeout is reached.
    /// Unlike `pending` which returns a snapshot, this waits for new approvals
    /// and returns immediately when one arrives.
    ///
    /// Use case: Claude Code can call this once and block, eliminating polling overhead.
    #[plexus_macros::method(params(
        session_id = "Session ID to wait for approvals",
        timeout_secs = "Optional timeout in seconds (default: 300 = 5 minutes)"
    ))]
    async fn wait_for_approval(
        &self,
        session_id: String,
        timeout_secs: Option<u64>,
    ) -> Result<WaitOutcome, LoopbackError> {
        let storage = self.storage.clone();
        let timeout = Duration::from_secs(timeout_secs.unwrap_or(300));

        // Get or create notifier for this session
        let notifier = storage.get_or_create_notifier(&session_id);

        // Record start time for timeout
        let start = std::time::Instant::now();

        loop {
            // First check if there are already pending approvals
            let approvals = storage.list_pending(Some(&session_id)).await?;
            if !approvals.is_empty() {
                // Found pending approval(s), return immediately
                return Ok(WaitOutcome::Ok { approvals });
            }

            // Check if we've exceeded timeout
            if start.elapsed() >= timeout {
                return Ok(WaitOutcome::Timeout {
                    message: format!("No approval received within {} seconds", timeout.as_secs()),
                });
            }

            // Wait for notification or timeout
            // Use tokio::select! to race between notification and timeout
            tokio::select! {
                () = notifier.notified() => {
                    // New approval arrived, loop will check pending again
                }
                () = sleep(timeout.saturating_sub(start.elapsed())) => {
                    // Timeout reached
                    return Ok(WaitOutcome::Timeout {
                        message: format!("No approval received within {} seconds", timeout.as_secs()),
                    });
                }
            }
        }
    }

    /// Generate MCP configuration for a loopback session
    ///
    /// PLX-116: `ConfigureResult::Err` was never constructed — this method
    /// cannot fail. `E` is `LoopbackError` for uniformity with the rest of the
    /// activation; it is simply never produced.
    #[plexus_macros::method(params(
        session_id = "Session ID for correlation"
    ))]
    async fn configure(
        &self,
        session_id: String,
    ) -> Result<ConfigureOk, LoopbackError> {
        // Include session_id in env config for correlation
        let config = json!({
            "mcpServers": {
                "plexus": {
                    "type": "http",
                    "url": self.mcp_url
                }
            },
            "env": {
                "LOOPBACK_SESSION_ID": session_id
            }
        });

        Ok(ConfigureOk { mcp_config: config })
    }
}
