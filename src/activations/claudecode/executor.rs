use super::types::{Model, RawClaudeEvent};
use async_stream::stream;
use futures::Stream;
use plexus_sandbox::{
    NetworkPolicy, ResourceLimits, Sandbox, SandboxError, SandboxProcess, SandboxSpec, TenantRoot,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
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

    /// PLX-151. A confined launch that could not be built or started. This is
    /// deliberately a *hard* error and never a fallback to the host: a
    /// confinement that degrades to an unconfined spawn on failure is not a
    /// confinement. The commonest case is the interesting one — a tenant chose
    /// a `working_dir` outside its own root, which `TenantRoot::contain`
    /// refuses (`SandboxError::PathEscape`).
    #[error("confined launch refused for tenant '{tenant}': {source}")]
    Confinement {
        tenant: String,
        #[source]
        source: SandboxError,
    },

    /// PLX-151 c4. The tenant was not admitted — no record, or a record that is
    /// not active. Checked at **every launch**, not once when the mount was
    /// built, so suspending a tenant stops its next turn rather than only its
    /// next mount.
    #[error("confined launch refused: {0}")]
    NotAdmitted(#[from] crate::tenancy::AdmissionRefused),
}

// ═══════════════════════════════════════════════════════════════════════════
// PLX-151 — the confinement
// ═══════════════════════════════════════════════════════════════════════════

/// Where a tenant's `claude` CLI actually runs.
///
/// # Why this type exists at all
///
/// PLX-130 measured the attack and PLX-144 escalated it: `claudecode.chat`
/// passes a caller-supplied `allowed_tools` straight to `--allowedTools`, so a
/// tenant asks for `["Bash"]` and gets a general-purpose read primitive; and
/// `.current_dir(&working_dir)` constrains **the launcher, not what the
/// launcher spawns**. Nothing inside substrate closes that, because the
/// enforcement point was a third-party CLI's permission model.
///
/// The fix is not a better flag. It is that the CLI itself runs inside a
/// kernel confinement with exactly one host directory bound into it. Then
/// every process the CLI spawns — bash, and whatever bash spawns — is inside
/// too, by construction rather than by configuration.
///
/// # What is *not* here, and why
///
/// There is no `disallowed_tools`. PLX-144 c5 deleted it, and this ticket does
/// not re-open it: populating it would place a security boundary inside the
/// CLI's own permission model, manufacturing assurance instead of enforcement.
///
/// # The obligation this type discharges for `plexus-sandbox`
///
/// `TenantRoot` takes a `&TenantId`, not a `TenantRecord`, because the sealed
/// record lives in plexus-idp which sits *above* substrate. PLX-144 stated the
/// residual: the caller must resolve the record, check `is_active()`, and pass
/// `record.id()`. [`TenantAdmission`] is where substrate does that, and it is
/// the only way to obtain the `TenantRoot` this struct holds.
#[derive(Clone)]
pub struct Confinement {
    sandbox: Arc<dyn Sandbox>,
    admission: Arc<crate::tenancy::TenantAdmission>,
    tenant: plexus_auth_core::tenant::TenantId,
    cli: String,
    env: BTreeMap<String, String>,
    network: NetworkPolicy,
    limits: ResourceLimits,
}

impl std::fmt::Debug for Confinement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Confinement")
            .field("runtime", &self.sandbox.runtime())
            .field("tenant", &self.tenant.as_str())
            .field("cli", &self.cli)
            // Env VALUES are omitted on purpose — this is where a per-tenant
            // API key lands, and Debug output reaches logs — so the keys stand
            // in for the field and `finish_non_exhaustive` says so.
            .field("env_keys", &self.env.keys().collect::<Vec<_>>())
            .field("network", &self.network)
            .field("limits", &self.limits)
            .field("admission", &self.admission)
            .finish_non_exhaustive()
    }
}

impl Confinement {
    /// Confine `tenant`'s launches to `sandbox`, admitting through `admission`.
    ///
    /// # Why the tenant root is not a constructor argument
    ///
    /// It would have to be resolved *here*, once, when the mount is composed —
    /// and then a tenant suspended afterwards would keep launching until the
    /// process restarted. Instead this holds the [`TenantAdmission`] and
    /// resolves the root **at every launch**, which is where PLX-151 c4's
    /// `is_active()` check therefore also lives. `TenantRoot` cannot be built
    /// from an unresolved path (`plexus_sandbox::CanonPath` has no constructor
    /// that skips `canonicalize`), so there is no way to reach `launch` with a
    /// root that was not both resolved and contained.
    #[must_use]
    pub fn new(
        sandbox: Arc<dyn Sandbox>,
        admission: Arc<crate::tenancy::TenantAdmission>,
        tenant: plexus_auth_core::tenant::TenantId,
    ) -> Self {
        Self {
            sandbox,
            admission,
            tenant,
            cli: "claude".to_owned(),
            env: BTreeMap::new(),
            // A `claude` turn that cannot reach the model API is not a product.
            // This is a REAL widening and it is named rather than buried:
            // `NetworkPolicy` has only `Denied` and `RuntimeDefault` because
            // PLX-144 refused to add a "restricted to the plexus gateway"
            // variant every backend would reject. That residual is still open;
            // see PLX-151's report. Filesystem isolation is unaffected by it —
            // the bind mount is the same either way.
            network: NetworkPolicy::RuntimeDefault,
            limits: ResourceLimits {
                pids: Some(512),
                memory_bytes: Some(2 * 1024 * 1024 * 1024),
                millicores: None,
            },
        }
    }

    /// Name the CLI *inside* the confinement. Defaults to `claude` on the
    /// image's `PATH`.
    ///
    /// The host's `claude_path` is meaningless inside a container — it is a
    /// path in the operator's `$HOME`, which is precisely what is not mounted.
    #[must_use]
    pub fn with_cli(mut self, cli: impl Into<String>) -> Self {
        self.cli = cli.into();
        self
    }

    /// Add one environment variable to the confined process.
    ///
    /// The confined environment is *exactly* what is set here. PLX-130 finding
    /// C: substrate's launchers never called `env_clear`, so `ANTHROPIC_API_KEY`
    /// and `PLEXUS_MCP_URL` were readable by `env` inside a tenant's shell.
    /// Nothing is inherited now; a per-tenant credential must be named.
    #[must_use]
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    /// Override the network policy.
    #[must_use]
    pub const fn with_network(mut self, network: NetworkPolicy) -> Self {
        self.network = network;
        self
    }

    /// Override the resource limits.
    #[must_use]
    pub const fn with_limits(mut self, limits: ResourceLimits) -> Self {
        self.limits = limits;
        self
    }

    /// The tenant this confinement belongs to.
    #[must_use]
    pub fn tenant(&self) -> &str {
        self.tenant.as_str()
    }

    /// Resolve this tenant's root, **checking the sealed record is active**.
    ///
    /// # Errors
    ///
    /// [`crate::tenancy::AdmissionRefused`] for an unknown or suspended tenant,
    /// an unsafe id segment, or an unusable root.
    pub async fn tenant_root(&self) -> Result<TenantRoot, crate::tenancy::AdmissionRefused> {
        self.admission.tenant_root(&self.tenant).await
    }

    /// Build the spec for one launch.
    ///
    /// `working_dir` is the tenant's own choice (`claudecode.create`). It is
    /// passed to `workdir_host`, which calls `TenantRoot::contain` — resolve,
    /// **then** constrain, then translate to the in-sandbox path. A
    /// `working_dir` outside the tenant root is a `PathEscape`, not a launch.
    fn spec(
        &self,
        root: TenantRoot,
        cli_args: &[String],
        working_dir: &str,
    ) -> Result<SandboxSpec, SandboxError> {
        let mut argv = Vec::with_capacity(cli_args.len() + 1);
        argv.push(self.cli.clone());
        argv.extend(cli_args.iter().cloned());

        let mut builder = SandboxSpec::builder(root)
            .argv(argv)
            .network(self.network)
            .limits(self.limits);

        // An empty or "." working_dir means "the tenant root", which is the
        // sandbox default. Anything else must be contained.
        if !working_dir.is_empty() && working_dir != "." {
            builder = builder.workdir_host(working_dir);
        }

        for (key, value) in &self.env {
            builder = builder.env(key, value);
        }

        builder.build()
    }

    /// One confined launch: admit, then confine, then start.
    ///
    /// The order is load-bearing and is the whole of c4 at the caller: the
    /// record is resolved and `is_active()` is checked **before** a
    /// `TenantRoot` exists, and the `TenantRoot` is what the spec is built
    /// from — so there is no launch that skipped the check.
    async fn launch(
        &self,
        cli_args: &[String],
        working_dir: &str,
    ) -> Result<SandboxProcess, ExecutorError> {
        let root = self.tenant_root().await?;
        let spec = self
            .spec(root, cli_args, working_dir)
            .map_err(|source| ExecutorError::Confinement {
                tenant: self.tenant.as_str().to_owned(),
                source,
            })?;
        self.sandbox
            .launch(&spec)
            .await
            .map_err(|source| ExecutorError::Confinement {
                tenant: self.tenant.as_str().to_owned(),
                source,
            })
    }
}

/// A process that has been started, either confined or on the host.
///
/// The two arms exist so the *stream-reading* half of `launch` is written once.
/// They are not interchangeable and the type says which is which.
enum Spawned {
    /// PLX-151: inside a `plexus_sandbox::Sandbox`. Everything this process
    /// spawns is inside too.
    Confined(SandboxProcess),
    /// The untenanted path (PLX-151 c5). See
    /// [`ClaudeCodeExecutor::spawn_on_host_unconfined`] for exactly what
    /// confinement this does and does not get: **none**.
    HostUnconfined(Box<tokio::process::Child>),
}

impl Spawned {
    fn take_stdout(&mut self) -> Option<Box<dyn AsyncRead + Send + Unpin>> {
        match self {
            Self::Confined(p) => p.take_stdout(),
            Self::HostUnconfined(c) => c
                .stdout
                .take()
                .map(|s| Box::new(s) as Box<dyn AsyncRead + Send + Unpin>),
        }
    }

    fn take_stderr(&mut self) -> Option<Box<dyn AsyncRead + Send + Unpin>> {
        match self {
            Self::Confined(p) => p.take_stderr(),
            Self::HostUnconfined(c) => c
                .stderr
                .take()
                .map(|s| Box::new(s) as Box<dyn AsyncRead + Send + Unpin>),
        }
    }

    async fn reap(&mut self) {
        match self {
            Self::Confined(p) => {
                let _ = p.wait().await;
            }
            Self::HostUnconfined(c) => {
                let _ = c.wait().await;
            }
        }
    }
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
///
/// # PLX-151: two launch paths, and only one of them is a boundary
///
/// * **Confined** (`confinement: Some`) — the CLI runs inside a
///   [`plexus_sandbox::Sandbox`], with the tenant root as the single exposed
///   host directory. Everything the CLI spawns, including the `bash` that
///   `allowed_tools: ["Bash"]` buys, is inside the same kernel confinement.
///   This is the only path a tenant mount ever reaches: `compose_tenant_hub`
///   registers `claudecode` only when `TenantSurface::claudecode_is_sandboxed`
///   is set, and `build_plexus_rpc` sets that only when it has a
///   [`Confinement`] to hand the activation.
///
/// * **Untenanted** (`confinement: None`) — the pre-existing host spawn. It
///   gets **no confinement**; see
///   [`Self::spawn_on_host_unconfined`], which says so in its own name so that
///   no reader has to infer it. PLX-151 c5: single-tenant deployments must not
///   be forced to run Docker, and pretending `.current_dir()` is a boundary is
///   what PLX-130 measured as false.
#[derive(Clone)]
pub struct ClaudeCodeExecutor {
    claude_path: String,
    confinement: Option<Confinement>,
}

impl ClaudeCodeExecutor {
    pub fn new() -> Self {
        Self {
            claude_path: Self::find_claude_binary().unwrap_or_else(|| "claude".to_string()),
            confinement: None,
        }
    }

    pub const fn with_path(path: String) -> Self {
        Self {
            claude_path: path,
            confinement: None,
        }
    }

    /// Return a copy of this executor that launches inside `confinement`.
    ///
    /// Per-tenant by construction: a [`Confinement`] names exactly one
    /// `TenantRoot`, so a shared executor cannot accidentally serve two
    /// tenants from one confinement.
    #[must_use]
    pub fn confined_to(&self, confinement: Confinement) -> Self {
        Self {
            claude_path: self.claude_path.clone(),
            confinement: Some(confinement),
        }
    }

    /// The confinement this executor launches inside, if any.
    ///
    /// Exposed so a composition can be *asserted* rather than assumed — the
    /// tenant-hub test checks that a tenant's `claudecode` has one.
    #[must_use]
    pub const fn confinement(&self) -> Option<&Confinement> {
        self.confinement.as_ref()
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
        let confinement = self.confinement.clone();
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

            // ─── PLX-151: the launch. Confined, or explicitly not. ───────────
            //
            // This is the site PLX-130 row A2 named:
            //   Command::new("bash").args(["-c", &shell_cmd]).current_dir(&working_dir)
            // `.current_dir()` constrained the launcher and nothing the
            // launcher spawned, and `working_dir` was the tenant's own choice.
            // A tenant's launch no longer takes that branch at all.
            let mut spawned = if let Some(c) = confinement.as_ref() {
                {
                    // The argv is a LIST, not a shell string: there is no
                    // `bash -c` wrapper on this path, so there is no quoting
                    // to get wrong. `shell_escape` below exists only for the
                    // untenanted path.
                    yield RawClaudeEvent::LaunchCommand {
                        command: format!(
                            "[confined:{} tenant={}] {} {}",
                            c.sandbox.runtime(),
                            c.tenant(),
                            c.cli,
                            args.join(" ")
                        ),
                    };

                    match c.launch(&args, &working_dir).await {
                        Ok(p) => Spawned::Confined(p),
                        Err(e) => {
                            // NEVER fall back to the host. A confinement that
                            // degrades to an unconfined spawn on error is not
                            // a confinement.
                            yield_error!(e);
                            return;
                        }
                    }
                }
            } else {
                {
                    let shell_cmd = Self::unconfined_shell_command(&claude_path, &args);
                    tracing::debug!(cmd = %shell_cmd, "Launching Claude Code (UNCONFINED host path)");

                    // Emit the launch command as an event (captured in arbor for debugging)
                    yield RawClaudeEvent::LaunchCommand { command: shell_cmd.clone() };

                    match Self::spawn_on_host_unconfined(
                        &shell_cmd,
                        &working_dir,
                        loopback_enabled.then(|| loopback_session_id.clone()).flatten(),
                    ) {
                        Ok(child) => Spawned::HostUnconfined(Box::new(child)),
                        Err(e) => {
                            yield_error!(ExecutorError::SpawnFailed {
                                binary: claude_path.clone(),
                                cwd: working_dir.clone(),
                                source: e,
                            });
                            return;
                        }
                    }
                }
            };

            let stdout = spawned.take_stdout().expect("stdout");
            let mut reader = BufReader::with_capacity(10 * 1024 * 1024, stdout).lines(); // 10MB buffer

            // Capture stderr in a background task to prevent pipe buffer blocking.
            //
            // Both pipes must be drained CONCURRENTLY — PLX-144 measured a
            // seven-minute hang from draining one to EOF before the other.
            let stderr_buffer: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
            let stderr = spawned.take_stderr().expect("stderr");
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

            // Reap the process (or the container) before reporting stderr, so
            // the background drainer above has seen everything.
            spawned.reap().await;

            // Emit whatever the launch wrote to stderr.
            //
            // PLX-151: this block previously read `child.stderr.take()`, which
            // was ALWAYS `None` — the handle had been moved into the background
            // drainer sixty lines earlier — so no `Stderr` event was ever
            // emitted and every diagnostic the CLI wrote was discarded. It is
            // now read from the buffer that actually holds them. That matters
            // here specifically: on the confined path a launch failure is a
            // *runtime* message ("no such image", "cannot connect to the Docker
            // daemon"), and PLX-151 c4 requires it be legible to an operator
            // rather than silent.
            {
                let lines = stderr_buffer.lock().await.clone();
                for line in lines {
                    if !line.trim().is_empty() {
                        yield RawClaudeEvent::Stderr { text: line };
                    }
                }
            }

            if let Some(path) = mcp_path {
                let _ = tokio::fs::remove_file(path).await;
            }
        })
    }

    /// The `bash -c` string used **only** by the untenanted host path.
    ///
    /// The confined path passes an argv *list* to `SandboxSpec::argv`, so it
    /// has no shell and no quoting to get wrong. This exists so the one
    /// remaining shell-string construction in the launcher is named, findable,
    /// and unreachable from a tenant.
    fn unconfined_shell_command(claude_path: &str, args: &[String]) -> String {
        fn shell_escape(s: &str) -> String {
            // Escape by wrapping in single quotes and escaping any single quotes
            format!("'{}'", s.replace('\'', "'\\''"))
        }
        format!(
            "{} {}",
            shell_escape(claude_path),
            args.iter()
                .map(|a| shell_escape(a))
                .collect::<Vec<_>>()
                .join(" ")
        )
    }

    /// Spawn the CLI on the host with **no confinement whatsoever** (PLX-151 c5).
    ///
    /// # What this path gets, stated rather than implied
    ///
    /// It gets the substrate process's own uid, its whole `$HOME`, its
    /// environment minus `CLAUDECODE`, and the host network. `.current_dir()`
    /// is set to `working_dir` as a **convenience**, and PLX-130 measured that
    /// it is not a boundary: it constrains the launcher, not what the launcher
    /// spawns. Nothing here is a security control and the name says so.
    ///
    /// # Why it still exists
    ///
    /// Not every deployment is multi-tenant. A single-tenant `make start` has
    /// exactly one principal, that principal already owns the machine, and
    /// making a Docker daemon a hard requirement to run `claudecode.chat`
    /// locally would be a cost with no isolation bought — there is no second
    /// tenant to isolate from.
    ///
    /// # Why it is not a hole
    ///
    /// It is unreachable from a tenant mount, and structurally so rather than
    /// by discipline: `compose_tenant_hub` registers `claudecode` only when
    /// `TenantSurface::claudecode_is_sandboxed` is set, and `build_plexus_rpc`
    /// sets that flag only in the branch where it has built a [`Confinement`].
    /// A tenant's executor therefore has `confinement: Some(_)`, and this
    /// function is not on its path.
    fn spawn_on_host_unconfined(
        shell_cmd: &str,
        working_dir: &str,
        loopback_session_id: Option<String>,
    ) -> Result<tokio::process::Child, std::io::Error> {
        let mut cmd = Command::new("bash");
        cmd.args(["-c", shell_cmd])
            // NOT a boundary. See this function's docs.
            .current_dir(working_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::null())
            // Unset CLAUDECODE so nested Claude sessions are allowed
            .env_remove("CLAUDECODE");

        if let Some(session_id) = loopback_session_id {
            cmd.env("PLEXUS_SESSION_ID", session_id);
        }

        cmd.spawn()
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
