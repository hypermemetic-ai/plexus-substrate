use super::types::{Model, RawClaudeEvent};
use async_stream::stream;
use futures::Stream;
use serde_json::Value;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::sync::Mutex;

/// Errors from the Claude Code executor
#[derive(Debug, Error)]
pub(super) enum ExecutorError {
    #[error("failed to spawn claude process (binary='{binary}', cwd='{cwd}'): {source}")]
    SpawnFailed {
        binary: String,
        cwd: String,
        source: std::io::Error,
    },

    #[error("failed to write MCP config to '{path}': {reason}")]
    McpConfigWrite {
        path: String,
        reason: String,
    },
}

// ─── MCP Reachability Check ───────────────────────────────────────────────────

/// Extract `host:port` from a URL like `http://127.0.0.1:4444/mcp`.
fn mcp_host_port_from_url(url: &str) -> String {
    let without_scheme = url
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let host_port = without_scheme.split('/').next().unwrap_or("127.0.0.1:4444");
    if host_port.contains(':') {
        host_port.to_string()
    } else {
        format!("{host_port}:4444")
    }
}

/// Check that the Plexus MCP server is reachable via TCP.
///
/// Reads `PLEXUS_MCP_URL` (default `http://127.0.0.1:4444/mcp`) to determine
/// the host:port.  Attempts a TCP connect with a 2-second timeout.
///
/// Returns an actionable error message if the server is not reachable, so
/// callers can fail fast before spawning Claude with a broken MCP config.
pub async fn check_mcp_reachable() -> Result<(), String> {
    let url = std::env::var("PLEXUS_MCP_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:4444/mcp".to_string());
    let addr = mcp_host_port_from_url(&url);

    match tokio::time::timeout(
        std::time::Duration::from_secs(2),
        TcpStream::connect(&addr),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(format!(
            "MCP server not reachable at {url} ({e}). \
             Start the substrate without --no-mcp so the permission-prompt tool is available."
        )),
        Err(_) => Err(format!(
            "MCP server connection timed out at {url}. \
             Start the substrate without --no-mcp so the permission-prompt tool is available."
        )),
    }
}

/// Configuration for a Claude Code session launch
#[derive(Debug, Clone)]
pub struct LaunchConfig {
    /// The query/prompt to send
    pub query: String,
    /// Resume an existing Claude session
    pub session_id: Option<String>,
    /// Fork the session instead of resuming
    pub fork_session: bool,
    /// Model to use
    pub model: Model,
    /// Working directory
    pub working_dir: String,
    /// System prompt
    pub system_prompt: Option<String>,
    /// MCP configuration (written to temp file)
    pub mcp_config: Option<Value>,
    /// Permission prompt tool name
    pub permission_prompt_tool: Option<String>,
    /// Allowed tools
    ///
    /// Caller-supplied, and passed straight through to `--allowedTools`. This
    /// is a *capability selector*, not a security boundary — see the note on
    /// `disallowed_tools` below for where the boundary actually is.
    pub allowed_tools: Vec<String>,
    // `disallowed_tools` was removed here (PLX-144 c5).
    //
    // It existed on this struct, was read when building the args, and was
    // populated by no code path anywhere in the tree — PLX-130 counted four
    // references: the field, its empty default, and the read. A lever with
    // nothing attached is worse than no lever: it reads like a safety control
    // and enforces nothing.
    //
    // It was deleted rather than wired, deliberately. Wiring it would have put
    // a *security* boundary inside the Claude CLI's own permission model,
    // which PLX-130 established is not a boundary substrate controls:
    // `allowed_tools` is caller-supplied, `--permission-prompt-tool` routes to
    // another Claude session (a probabilistic gate), and `.current_dir()`
    // constrains the launcher, not what the launcher spawns. Populating this
    // field would have manufactured assurance instead of enforcement.
    //
    // The enforcement point is process-level confinement: the `plexus-sandbox`
    // crate (`../plexus-sandbox`), one container per tenant, exactly one bind
    // mount at the tenant root. This launcher is NOT yet wired to it; that is
    // a separate, smaller change, and until it lands `claudecode` must stay
    // out of tenant mounts (PLX-127's exclusion list).
    /// Max turns
    pub max_turns: Option<i32>,
    /// Enable loopback mode - routes tool permissions through Plexus for parent approval
    pub loopback_enabled: bool,
    /// Session ID for loopback correlation
    pub loopback_session_id: Option<String>,
}

impl Default for LaunchConfig {
    fn default() -> Self {
        Self {
            query: String::new(),
            session_id: None,
            fork_session: false,
            model: Model::Sonnet,
            working_dir: ".".to_string(),
            system_prompt: None,
            mcp_config: None,
            permission_prompt_tool: None,
            allowed_tools: Vec::new(),
            max_turns: None,
            loopback_enabled: false,
            loopback_session_id: None,
        }
    }
}

/// Executor that wraps the Claude Code CLI
#[derive(Clone)]
pub struct ClaudeCodeExecutor {
    claude_path: String,
}

impl ClaudeCodeExecutor {
    pub fn new() -> Self {
        Self {
            claude_path: Self::find_claude_binary().unwrap_or_else(|| "claude".to_string()),
        }
    }

    pub const fn with_path(path: String) -> Self {
        Self { claude_path: path }
    }

    /// Discover the Claude binary location
    fn find_claude_binary() -> Option<String> {
        // Check common locations
        let home = dirs::home_dir()?;

        let candidates = [
            home.join(".claude/local/claude"),
            home.join(".npm/bin/claude"),
            home.join(".bun/bin/claude"),
            home.join(".local/bin/claude"),
            PathBuf::from("/usr/local/bin/claude"),
            PathBuf::from("/opt/homebrew/bin/claude"),
        ];

        for candidate in &candidates {
            if candidate.exists() {
                return candidate.to_str().map(std::string::ToString::to_string);
            }
        }

        // Try PATH
        which::which("claude")
            .ok()
            .and_then(|p| p.to_str().map(std::string::ToString::to_string))
    }

    /// Build command line arguments from config
    fn build_args(&self, config: &LaunchConfig) -> Vec<String> {
        let mut args = vec![
            "--output-format".to_string(),
            "stream-json".to_string(),
            "--include-partial-messages".to_string(),
            "--verbose".to_string(),
            "--print".to_string(),
        ];

        // Session resumption
        if let Some(ref session_id) = config.session_id {
            args.push("--resume".to_string());
            args.push(session_id.clone());

            if config.fork_session {
                args.push("--fork-session".to_string());
            }
        }

        // Model
        args.push("--model".to_string());
        args.push(config.model.as_str().to_string());

        // Max turns
        if let Some(max) = config.max_turns {
            args.push("--max-turns".to_string());
            args.push(max.to_string());
        }

        // System prompt
        if let Some(ref prompt) = config.system_prompt {
            args.push("--system-prompt".to_string());
            args.push(prompt.clone());
        }

        // Permission prompt tool - loopback takes precedence
        if config.loopback_enabled {
            args.push("--permission-prompt-tool".to_string());
            args.push("mcp__plexus__loopback_permit".to_string());
        } else if let Some(ref tool) = config.permission_prompt_tool {
            args.push("--permission-prompt-tool".to_string());
            args.push(tool.clone());
        }

        // Allowed tools
        if !config.allowed_tools.is_empty() {
            args.push("--allowedTools".to_string());
            args.push(config.allowed_tools.join(","));
        }

        // `--disallowedTools` is deliberately never emitted (PLX-144 c5); see
        // the note on `LaunchConfig::allowed_tools`. Confinement is the
        // `plexus-sandbox` crate's job, not the CLI's flag surface.

        // Query must be last
        args.push("--".to_string());
        args.push(config.query.clone());

        args
    }

    /// Write MCP config to a temp file and return the path
    #[allow(dead_code)]
    async fn write_mcp_config(&self, config: &Value) -> Result<String, String> {
        let temp_dir = std::env::temp_dir();
        let temp_path = temp_dir.join(format!("mcp-config-{}.json", uuid::Uuid::new_v4()));

        let json = serde_json::to_string_pretty(config)
            .map_err(|e| format!("Failed to serialize MCP config: {e}"))?;

        tokio::fs::write(&temp_path, json)
            .await
            .map_err(|e| format!("Failed to write MCP config: {e}"))?;

        Ok(temp_path.to_string_lossy().to_string())
    }

    /// Launch a Claude Code session and stream raw events
    pub async fn launch(
        &self,
        config: LaunchConfig,
    ) -> Pin<Box<dyn Stream<Item = RawClaudeEvent> + Send + 'static>> {
        let mut args = self.build_args(&config);
        let claude_path = self.claude_path.clone();
        let working_dir = config.working_dir.clone();
        let loopback_enabled = config.loopback_enabled;
        let loopback_session_id = config.loopback_session_id.clone();

        // Build MCP config - merge loopback config if enabled
        let mcp_config = if loopback_enabled {
            let base_url = std::env::var("PLEXUS_MCP_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:4444/mcp".to_string());

            // Include session_id in URL for correlation when loopback_permit is called
            let plexus_url = if let Some(ref sid) = loopback_session_id {
                format!("{base_url}?session_id={sid}")
            } else {
                base_url
            };

            let loopback_mcp = if let Some(ref sid) = loopback_session_id {
                serde_json::json!({
                    "mcpServers": {
                        "plexus": {
                            "type": "http",
                            "url": plexus_url
                        }
                    },
                    "env": {
                        "PLEXUS_SESSION_ID": sid
                    }
                })
            } else {
                serde_json::json!({
                    "mcpServers": {
                        "plexus": {
                            "type": "http",
                            "url": plexus_url
                        }
                    }
                })
            };

            // Merge with existing config if present
            match config.mcp_config {
                Some(existing) => {
                    // Merge mcpServers from both
                    let mut merged = existing;
                    if let (Some(existing_servers), Some(loopback_servers)) = (
                        merged.get_mut("mcpServers"),
                        loopback_mcp.get("mcpServers")
                    ) {
                        if let (Some(existing_obj), Some(loopback_obj)) = (
                            existing_servers.as_object_mut(),
                            loopback_servers.as_object()
                        ) {
                            for (k, v) in loopback_obj {
                                existing_obj.insert(k.clone(), v.clone());
                            }
                        }
                    } else {
                        merged["mcpServers"] = loopback_mcp["mcpServers"].clone();
                    }
                    Some(merged)
                }
                None => Some(loopback_mcp)
            }
        } else {
            config.mcp_config.clone()
        };

        Box::pin(stream! {
            macro_rules! yield_error {
                ($err:expr) => {{
                    let err: ExecutorError = $err;
                    tracing::error!(error = %err, "Claude executor error");
                    yield RawClaudeEvent::Result {
                        subtype: Some("error".to_string()),
                        session_id: None,
                        cost_usd: None,
                        is_error: Some(true),
                        duration_ms: None,
                        num_turns: None,
                        result: None,
                        error: Some(err.to_string()),
                    };
                }};
            }

            // Fail fast if loopback is enabled but the MCP server is not reachable.
            // Without a live MCP server Claude cannot call the permission-prompt tool
            // and will return empty output (silent failure).
            if loopback_enabled {
                if let Err(e) = check_mcp_reachable().await {
                    yield RawClaudeEvent::Result {
                        subtype: Some("error".to_string()),
                        session_id: None,
                        cost_usd: None,
                        is_error: Some(true),
                        duration_ms: None,
                        num_turns: None,
                        result: None,
                        error: Some(e),
                    };
                    return;
                }
            }

            // Handle MCP config if present
            let mcp_path = if let Some(ref mcp) = mcp_config {
                match Self::write_mcp_config_sync(mcp) {
                    Ok(path) => {
                        // Insert MCP config args before the "--" separator
                        if let Some(pos) = args.iter().position(|a| a == "--") {
                            args.insert(pos, path.clone());
                            args.insert(pos, "--mcp-config".to_string());
                        }
                        Some(path)
                    }
                    Err(e) => {
                        yield_error!(ExecutorError::McpConfigWrite {
                            path: std::env::temp_dir().to_string_lossy().to_string(),
                            reason: e,
                        });
                        return;
                    }
                }
            } else {
                None
            };

            // Spawn Claude process via shell to ensure clean process context
            // This avoids any potential issues with nested Claude sessions
            fn shell_escape(s: &str) -> String {
                // Escape by wrapping in single quotes and escaping any single quotes
                format!("'{}'", s.replace('\'', "'\\''"))
            }

            let shell_cmd = format!(
                "{} {}",
                shell_escape(&claude_path),
                args.iter()
                    .map(|a| shell_escape(a))
                    .collect::<Vec<_>>()
                    .join(" ")
            );

            tracing::debug!(cmd = %shell_cmd, "Launching Claude Code");

            // Emit the launch command as an event (captured in arbor for debugging)
            yield RawClaudeEvent::LaunchCommand { command: shell_cmd.clone() };

            let mut cmd = Command::new("bash");
            cmd.args(["-c", &shell_cmd])
                .current_dir(&working_dir)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .stdin(Stdio::null())
                // Unset CLAUDECODE so nested Claude sessions are allowed
                .env_remove("CLAUDECODE");

            // Set loopback session ID env var if loopback is enabled
            if loopback_enabled {
                if let Some(ref session_id) = loopback_session_id {
                    cmd.env("PLEXUS_SESSION_ID", session_id);
                }
            }

            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => {
                    yield_error!(ExecutorError::SpawnFailed {
                        binary: claude_path.clone(),
                        cwd: working_dir.clone(),
                        source: e,
                    });
                    return;
                }
            };

            let stdout = child.stdout.take().expect("stdout");
            let mut reader = BufReader::with_capacity(10 * 1024 * 1024, stdout).lines(); // 10MB buffer

            // Capture stderr in a background task to prevent pipe buffer blocking
            let stderr_buffer: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let stderr = child.stderr.take().expect("stderr");
            let stderr_buf = stderr_buffer.clone();
            tokio::spawn(async move {
                let mut stderr_reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = stderr_reader.next_line().await {
                    let mut buf = stderr_buf.lock().await;
                    if buf.len() < 100 {
                        buf.push(line);
                    }
                }
            });

            // Stream events from stdout
            while let Ok(Some(line)) = reader.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }

                match serde_json::from_str::<RawClaudeEvent>(&line) {
                    Ok(event) => {
                        let is_result = matches!(event, RawClaudeEvent::Result { .. });
                        yield event;
                        if is_result {
                            break;
                        }
                    }
                    Err(_) => {
                        // Try to parse as generic JSON and wrap as Unknown event
                        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) {
                            let event_type = value.get("type")
                                .and_then(|t| t.as_str())
                                .unwrap_or("unknown_json")
                                .to_string();
                            yield RawClaudeEvent::Unknown {
                                event_type,
                                data: value,
                            };
                        } else {
                            // Non-JSON output (raw text, errors, etc.)
                            yield RawClaudeEvent::Unknown {
                                event_type: "raw_output".to_string(),
                                data: serde_json::Value::String(line),
                            };
                        }
                    }
                }
            }

            // Drain stderr and emit as events (captures error messages from Claude)
            if let Some(stderr) = child.stderr.take() {
                let mut stderr_reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = stderr_reader.next_line().await {
                    if !line.trim().is_empty() {
                        yield RawClaudeEvent::Stderr { text: line };
                    }
                }
            }

            // Cleanup
            let _ = child.wait().await;

            if let Some(path) = mcp_path {
                let _ = tokio::fs::remove_file(path).await;
            }
        })
    }

    /// Sync version of `write_mcp_config` for use in async stream
    fn write_mcp_config_sync(config: &Value) -> Result<String, String> {
        let temp_dir = std::env::temp_dir();
        let temp_path = temp_dir.join(format!("mcp-config-{}.json", uuid::Uuid::new_v4()));

        let json = serde_json::to_string_pretty(config)
            .map_err(|e| format!("Failed to serialize MCP config: {e}"))?;

        std::fs::write(&temp_path, json)
            .map_err(|e| format!("Failed to write MCP config: {e}"))?;

        Ok(temp_path.to_string_lossy().to_string())
    }
}

impl Default for ClaudeCodeExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_args_basic() {
        let executor = ClaudeCodeExecutor::with_path("/usr/bin/claude".to_string());
        let config = LaunchConfig {
            query: "hello".to_string(),
            model: Model::Sonnet,
            working_dir: "/tmp".to_string(),
            ..Default::default()
        };

        let args = executor.build_args(&config);

        assert!(args.contains(&"--output-format".to_string()));
        assert!(args.contains(&"stream-json".to_string()));
        assert!(args.contains(&"--model".to_string()));
        assert!(args.contains(&"sonnet".to_string()));
        assert!(args.contains(&"--".to_string()));
        assert!(args.contains(&"hello".to_string()));
    }

    #[test]
    fn test_build_args_with_resume() {
        let executor = ClaudeCodeExecutor::with_path("/usr/bin/claude".to_string());
        let config = LaunchConfig {
            query: "continue".to_string(),
            session_id: Some("sess_123".to_string()),
            model: Model::Haiku,
            working_dir: "/tmp".to_string(),
            ..Default::default()
        };

        let args = executor.build_args(&config);

        assert!(args.contains(&"--resume".to_string()));
        assert!(args.contains(&"sess_123".to_string()));
        assert!(args.contains(&"haiku".to_string()));
    }

    #[test]
    fn test_build_args_with_fork() {
        let executor = ClaudeCodeExecutor::with_path("/usr/bin/claude".to_string());
        let config = LaunchConfig {
            query: "branch".to_string(),
            session_id: Some("sess_123".to_string()),
            fork_session: true,
            model: Model::Opus,
            working_dir: "/tmp".to_string(),
            ..Default::default()
        };

        let args = executor.build_args(&config);

        assert!(args.contains(&"--resume".to_string()));
        assert!(args.contains(&"--fork-session".to_string()));
        assert!(args.contains(&"opus".to_string()));
    }
}
