//! `acp-stdio` — the service an editor spawns to talk to `claudecode`.
//!
//! PLX-140 c1 asks for "an editor-shaped client spawns the service, opens a
//! session, prompts, and receives streamed `session/update` notifications
//! terminated by a `stopReason`". This is the service it spawns.
//!
//! It is a real binary rather than an example for PLX-139's reason: an
//! in-process test exercises the dispatch table and nothing else — not the
//! NDJSON framing, not the line buffering, not the fact that a `println!`
//! anywhere in `plexus-substrate` or its dependency graph lands in the same
//! file descriptor as the protocol. Those are what break in the field, so the
//! thing under test has to be a process.
//!
//! # Configuration
//!
//! | env | meaning |
//! |---|---|
//! | `PLEXUS_ACP_STATE_DIR` | where the sqlite files go. Required. |
//! | `PLEXUS_ACP_CLAUDE_BIN` | the `claude` binary. Defaults to discovery. |
//!
//! `PLEXUS_ACP_CLAUDE_BIN` exists so `tests/acp_claudecode.rs` can point the
//! executor at a scripted CLI. Only the **model** is substituted that way —
//! the same line `tests/tenant_confinement.rs` draws — while substrate's
//! executor, activation, storage and `ChatEvent` stream are all the real ones,
//! and everything above them (the turn, the projection, the session runtime,
//! the transport) is real without qualification.
//!
//! # This binary is deliberately UNTENANTED
//!
//! It composes one `ClaudeCode` and one agent. Multi-tenant ACP is
//! `builder.rs`'s composition — one agent per tenant, minted behind the
//! `TenantSubtreeFactory` — and it is exercised in-process by
//! `tests/acp_tenancy.rs`, which is where an attack can actually be mounted.
//! Serving two tenants down **one** pair of file descriptors would require an
//! authenticated `initialize`, which ACP v1 does not have. Saying so here is
//! better than shipping a binary that looks multi-tenant and is not.

use std::sync::Arc;

use plexus_acp::v1::transport::{serve, Peer, ProtocolChannel};
use plexus_acp::v1::Agent;
use plexus_acp::v1::schema::Error;
use plexus_substrate::acp::ClaudeCodeAcpAgent;
use plexus_substrate::activations::arbor::{ArborConfig, ArborStorage};
use plexus_substrate::activations::claudecode::{
    ClaudeCode, ClaudeCodeExecutor, ClaudeCodeStorage, ClaudeCodeStorageConfig,
};

fn main() -> Result<(), Error> {
    // FIRST, before anything else in this process can print. After this line
    // fd 1 is stderr and the protocol channel is a private duplicate that no
    // in-process code — ours or a dependency's — can reach. `plexus-substrate`
    // drags a large graph; this is what makes that safe.
    let channel = ProtocolChannel::take().expect("the ACP protocol channel");

    eprintln!("[acp-stdio] plexus-substrate, claudecode over ACP v1");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            let peer = Peer::new();
            let claudecode = Arc::new(build_claudecode().await);
            let agent: Arc<dyn Agent> = Arc::new(ClaudeCodeAcpAgent::new(claudecode, peer.clone()));
            serve(agent, peer, channel).await
        })
}

async fn build_claudecode() -> ClaudeCode {
    let state = std::env::var("PLEXUS_ACP_STATE_DIR")
        .expect("PLEXUS_ACP_STATE_DIR must name a directory for this agent's state");
    let state = std::path::PathBuf::from(state);
    std::fs::create_dir_all(&state).expect("state dir");

    let arbor = ArborStorage::new(ArborConfig {
        db_path: state.join("arbor.db"),
        ..Default::default()
    })
    .await
    .expect("arbor storage");

    let storage = ClaudeCodeStorage::new(
        ClaudeCodeStorageConfig {
            db_path: state.join("claudecode.db"),
        },
        Arc::new(arbor),
    )
    .await
    .expect("claudecode storage");

    let executor = match std::env::var("PLEXUS_ACP_CLAUDE_BIN") {
        Ok(path) => ClaudeCodeExecutor::with_path(path),
        Err(_) => ClaudeCodeExecutor::new(),
    };

    ClaudeCode::with_executor_and_context(Arc::new(storage), executor)
}
