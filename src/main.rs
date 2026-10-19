use plexus_login::login::Provider;
use plexus_substrate::login::{self as substrate_login, LoginConfig, StartGrant};
use plexus_transport::TransportServer;
use clap::Parser;

/// CLI arguments for substrate
#[derive(Parser, Debug)]
#[command(name = "substrate")]
#[command(about = "Substrate Plexus RPC server - JSON-RPC over WebSocket, under login")]
struct Args {
    /// Refused (PLEXUS-6): stdio cannot carry a credential.
    #[arg(long, hide = true)]
    stdio: bool,

    /// Port for the WebSocket
    #[arg(short, long, default_value = "4444")]
    port: u16,

    /// Accepted and changes nothing (PLEXUS-6): under login the MCP HTTP
    /// gateway is not served — it cannot carry a caller to the gate.
    #[arg(long)]
    no_mcp: bool,

    /// Retired (PLEXUS-6): the shared bearer token is gone; callers sign in
    /// through a login provider. Given, substrate refuses to start.
    #[arg(long, env = "PLEXUS_API_KEY", hide = true, hide_env_values = true)]
    api_key: Option<String>,

    /// PLEXUS-6: admit callers through this login provider (repeatable;
    /// `;`-separated in the variable): `key` — agents' Ed25519 key proofs
    /// naming this socket (synapse `_login substrate --key NAME`) — and
    /// people's providers as plexus-login takes them (`idp=<issuer>,<aud>`,
    /// `workos=<client>`, `oidc:<name>=…`, `auth0=…`, `nostr=…`). At least
    /// one: there is no open mode.
    #[arg(long = "auth-provider", env = "SUBSTRATE_AUTH_PROVIDERS", value_name = "SPEC", value_delimiter = ';')]
    auth_provider: Vec<Provider>,

    /// PLEXUS-6: a principal that is admin everywhere from the start
    /// (`key:<hex>` — `synapse _key show NAME` —, `idp:<uuid>`, …).
    #[arg(long, env = "SUBSTRATE_ADMINS", value_name = "PRINCIPAL", value_delimiter = ';')]
    admin: Vec<String>,

    /// PLEXUS-6: a standing grant, `ROLE=PRINCIPAL` (`viewer`, `editor`,
    /// `admin`) — e.g. the chat bridge's key as `editor`: claudecode
    /// sessions of its own, nothing else.
    #[arg(long, env = "SUBSTRATE_GRANTS", value_name = "ROLE=PRINCIPAL", value_delimiter = ';')]
    grant: Vec<StartGrant>,

    /// PLEXUS-6: serve the login page for people on this port (agents need
    /// none).
    #[arg(long, env = "SUBSTRATE_LOGIN_PORT")]
    login_port: Option<u16>,

    /// PLEXUS-6: where people reach the login page (default
    /// `http://127.0.0.1:<login-port>`).
    #[arg(long, env = "SUBSTRATE_PUBLIC_URL")]
    public_url: Option<String>,

    /// PLEXUS-6: keep every database under this directory instead of
    /// `~/.plexus/substrate` (a throwaway substrate).
    #[arg(long, env = "SUBSTRATE_DATA_DIR")]
    data_dir: Option<std::path::PathBuf>,
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse CLI arguments
    let args = Args::parse();

    // Load .env file if present (silently ignore if not found)
    dotenvy::dotenv().ok();

    // Initialize tracing with filtering
    // In debug builds, enable debug logging for substrate by default
    // In stdio mode, reduce verbosity to avoid polluting stdout
    let filter = if args.stdio {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(
                    "substrate=warn,jsonrpsee=warn"
                )
            })
    } else {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| {
                // Set base level to warn, then enable specific modules
                // This hides sqlx and other noisy deps by default
                #[cfg(debug_assertions)]
                let default_filter = "warn,substrate=trace,plexus_macros=trace";
                #[cfg(not(debug_assertions))]
                let default_filter = "warn,substrate=debug,plexus_macros=debug";
                tracing_subscriber::EnvFilter::new(default_filter)
            })
    };

    // In stdio mode, send logs to stderr to keep stdout clean for JSON-RPC
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    // Log start time first
    tracing::info!("Starting substrate at {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"));

    // Log level calibration sequence
    tracing::error!("▓▓▓ SUBSTRATE BOOT SEQUENCE ▓▓▓");
    tracing::warn!("  ├─ warn  :: caution signals armed");
    tracing::info!("  ├─ info  :: telemetry online");
    tracing::debug!("  ├─ debug :: introspection enabled");
    tracing::trace!("  └─ trace :: full observability unlocked");

    // PLEXUS-6: no open mode, no shared token, no stdio.
    if args.api_key.is_some() {
        anyhow::bail!("--api-key / PLEXUS_API_KEY is retired: substrate takes callers through a login provider (--auth-provider key, synapse _login)");
    }
    if let Some(why) = substrate_login::refusal(&args.auth_provider, args.stdio) {
        anyhow::bail!(why);
    }
    if let Some(dir) = &args.data_dir {
        plexus_substrate::activations::storage::set_data_dir(dir.clone());
        tracing::info!("Data:      {}", dir.display());
    }

    let cfg = LoginConfig {
        providers: args.auth_provider.clone(),
        admins: args.admin.clone(),
        grants: args.grant.clone(),
        ws_port: args.port,
        login_port: args.login_port,
        public_url: args.public_url.clone(),
    };
    // Build the Plexus RPC hub behind the gate (returns Arc<DynamicHub>)
    let (hub, login) = substrate_login::build(&cfg, None).await?;
    let activations = hub.list_activations_info();
    let methods = hub.list_methods();
    let plexus_hash = hub.compute_hash();

    // Log activation info
    tracing::info!("Plexus hash: {}", plexus_hash);
    tracing::info!("");
    tracing::info!("Activations ({}):", activations.len());
    for activation in &activations {
        tracing::info!("  {} v{} - {}",
            activation.namespace,
            activation.version,
            activation.description
        );
        for method in &activation.methods {
            tracing::info!("    - {}_{}", activation.namespace, method);
        }
    }
    tracing::info!("");
    tracing::info!("Total methods: {}", methods.len());

    let rpc_converter = |arc| {
        use plexus_core::plexus::DynamicHub;
        DynamicHub::arc_into_rpc_module(arc)
            .map_err(|e| anyhow::anyhow!("Failed to create RPC module: {e}"))
    };

    let names: Vec<&str> = args.auth_provider.iter().map(Provider::name).collect();
    tracing::info!("Substrate Plexus RPC server started");
    tracing::info!("  WebSocket: ws://127.0.0.1:{}", args.port);
    tracing::info!("  Login:     {} — anonymous callers see only the auth door", names.join(", "));
    tracing::info!("  MCP HTTP:  not served under login{}", if args.no_mcp { "" } else { " (it cannot carry a caller to the gate)" });
    if let (Some(port), Some(router)) = (args.login_port, login.router()) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
        tracing::info!("  Login page: http://127.0.0.1:{port}/login");
        tokio::spawn(async move {
            if let Err(e) = axum08::serve(listener, router).await {
                tracing::error!("login page exited: {e}");
            }
        });
    }
    let builder = TransportServer::builder(hub, rpc_converter).with_websocket(args.port);
    login.apply(builder).build().await?.serve().await
}
