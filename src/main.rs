use plexus_substrate::build_plexus_rpc;
use plexus_transport::TransportServer;
#[cfg(feature = "mcp-gateway")]
use plexus_transport::{serve_combined, RouteFn};
use clap::Parser;
#[cfg(feature = "mcp-gateway")]
use std::sync::Arc;

/// CLI arguments for substrate
#[derive(Parser, Debug)]
#[command(name = "substrate")]
#[command(about = "Substrate Plexus RPC server - JSON-RPC over WebSocket or stdio")]
struct Args {
    /// Run in stdio mode for MCP compatibility (line-delimited JSON-RPC over stdin/stdout)
    #[arg(long)]
    stdio: bool,

    /// Port for WebSocket + MCP HTTP server (ignored in stdio mode)
    #[arg(short, long, default_value = "4444")]
    port: u16,

    /// Disable built-in MCP HTTP server (WebSocket only)
    #[arg(long)]
    no_mcp: bool,

    /// Bearer token required on all WebSocket and MCP HTTP connections.
    /// Also read from the `PLEXUS_API_KEY` environment variable.
    /// When neither is provided, no authentication is required.
    #[arg(long, env = "PLEXUS_API_KEY")]
    api_key: Option<String>,
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

    if args.api_key.is_some() {
        // AUTHZ-BEARER-1: when this substrate is rebuilt against the
        // plexus-transport version that ships AUTHZ-BEARER-1, the api_key
        // is enforced on a dedicated header (default `X-Plexus-API-Key`),
        // not on `Authorization: Bearer`. `Authorization: Bearer` becomes
        // reserved for SessionValidator user-identity tokens. The string
        // literal is hard-coded here because this binary may be linked
        // against the pre-AUTHZ-BEARER-1 published transport during the
        // upgrade window; once both are in sync, switch to
        // `plexus_transport::DEFAULT_API_KEY_HEADER`.
        tracing::info!(
            "Authentication: static admission key configured (header: X-Plexus-API-Key); \
             Authorization: Bearer is reserved for SessionValidator (AUTHZ-BEARER-1)",
        );
    } else {
        tracing::warn!(
            "Authentication: DISABLED — set --api-key or PLEXUS_API_KEY to require an \
             admission key (sent on the X-Plexus-API-Key header)"
        );
    }

    // Build Plexus RPC hub (returns Arc<DynamicHub>)
    let hub = build_plexus_rpc().await;
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

    if args.stdio {
        // Stdio mode: line-delimited JSON-RPC over stdin/stdout
        tracing::info!("Starting stdio transport (MCP-compatible)");
        TransportServer::builder(hub, rpc_converter)
            .with_stdio()
            .build().await?.serve().await
    } else if args.no_mcp {
        // WebSocket only
        tracing::info!("Substrate Plexus RPC server started");
        tracing::info!("  WebSocket: ws://127.0.0.1:{}", args.port);
        tracing::info!("  MCP HTTP:  disabled");
        TransportServer::builder(hub, rpc_converter)
            .with_websocket(args.port)
            .with_api_key(args.api_key)
            .build().await?.serve().await
    } else {
        #[cfg(feature = "mcp-gateway")]
        {
            // Combined WebSocket + MCP HTTP on same port
            use plexus_core::plexus::DynamicHub;
            let flat_schemas = hub.list_plugin_schemas();
            let module = DynamicHub::arc_into_rpc_module(hub.clone())
                .map_err(|e| anyhow::anyhow!("Failed to create RPC module: {e}"))?;
            let hub_route = hub.clone();
            // PLX-28: transport's RouteFn went 4-arg in PLX-16/17 (per-user AuthContext +
            // RawRequestContext threading). Forward both into route_with_ctx so identity
            // and origin context reach the hub when a gateway supplies them.
            let route_fn: RouteFn = Arc::new(move |method, params, auth, raw_ctx| {
                let hub = hub_route.clone();
                Box::pin(async move {
                    hub.route_with_ctx(&method, params, auth.as_deref(), raw_ctx.as_ref())
                        .await
                })
            });
            let addr: std::net::SocketAddr = format!("127.0.0.1:{}", args.port).parse()?;
            tracing::info!("Substrate Plexus RPC server started");
            tracing::info!("  WebSocket: ws://127.0.0.1:{}", args.port);
            tracing::info!("  MCP HTTP:  http://127.0.0.1:{}/mcp", args.port);
            // PLX-28: serve_combined grew a session_validator param (PLX-17). None preserves
            // substrate's existing behavior — no per-user validator is wired today.
            let handle = serve_combined(module, hub, Some(flat_schemas), Some(route_fn), addr, args.api_key, false, None).await?;
            handle.stopped().await;
            Ok(())
        }
        #[cfg(not(feature = "mcp-gateway"))]
        {
            // Fallback: WebSocket + separate MCP HTTP on next port
            tracing::info!("Substrate Plexus RPC server started");
            tracing::info!("  WebSocket: ws://127.0.0.1:{}", args.port);
            tracing::info!("  MCP HTTP:  http://127.0.0.1:{}/mcp", args.port + 1);
            TransportServer::builder(hub, rpc_converter)
                .with_websocket(args.port)
                .with_mcp_http(args.port + 1)
                .with_api_key(args.api_key)
                .build().await?.serve().await
        }
    }
}
