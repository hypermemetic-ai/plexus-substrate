//! `claudecode` as the reference ACP agent — PLX-140, the composition site.
//!
//! # What this file is
//!
//! Every ACP node before this one built a part. PLX-135 the vocabulary, PLX-136
//! the `#[acp_agent]` macro, PLX-137 the typed client handle, PLX-138 the
//! session runtime and the `Indexed` edge, PLX-139 the NDJSON-over-stdio
//! transport. None of them touched a real activation, by design. **This is
//! where they meet a product.**
//!
//! The agent under the protocol is `claudecode` — the one PLX-140 chose because
//! it is "already an unstandardized ACP in miniature": streaming events, tool
//! call updates, a permission ask. [`ChatEventProjection`] is the table that
//! makes that literal.
//!
//! # The two lines PLX-138 promised
//!
//! PLX-138 shipped [`SessionMount`] with a boundary written into its own docs:
//! it is *not* an [`Activation`], because that would drag the legacy jsonrpsee
//! surface into a protocol crate. It supplies `connectome_edge()` and
//! `resolve()`, and said "ACP·F wires them in two lines".
//!
//! Here they are:
//!
//! - [`ClaudeCodeAcpAgent::prompt`] resolves the session through
//!   `self.mount.resolve(..)` — the **only** path from a caller-supplied
//!   `sessionId` to a session object.
//! - the [`Activation`] impl at the bottom of this file answers
//!   `connectome_edge()` with `self.mount.connectome_edge()`, so ACP sessions
//!   render on the Connectome as RFC 002 §5.1's `Indexed` family — one path
//!   template and an `id_field` of `sessionId`, and **no instance ids**.
//!
//! The `Activation` impl is hand-written rather than macro-generated for
//! exactly the reason `plexus_core`'s own `TenantMount` is: `#[activation]` and
//! `#[acp_agent]` are two independent attributes with two independent
//! allowlists (plexus-macros says so in `acp_agent.rs`'s own comments), and
//! neither can emit the other's output. They compose by sitting on **separate
//! impl blocks of the same type**, which is what PLX-136 means when it says
//! `#[acp_agent]` does not subsume `#[activation]`.
//!
//! # Where the peer is, and why that is the whole point
//!
//! PLX-105 is parked on a measured blocker: `loopback.permit`'s turn is opened
//! **by the spawned CLI**, so a callback raised inside it would ask the asker.
//! That is a real constraint and this build does not pretend otherwise.
//!
//! On the ACP path the direction is right, and it is right *structurally*
//! rather than by care: the turn is opened by the **editor's** `session/prompt`
//! request, so the turn's callback channel reaches the editor. Asking for
//! permission is therefore [`AcpClient::request_permission`] — one line, no
//! correlation — and a denial is
//! [`PermissionDenied`](plexus_acp::v1::runtime::PermissionDenied), whose
//! `IntoTurnStop` makes the turn terminate `StopKind::Refused` and reach the
//! wire as `{"stopReason":"refusal"}` with **no `error` key** (RFC 002 §6.7.1).
//!
//! This is the first `StopKind::Refused` in plexus-substrate. Before it, the
//! only two mentions in this crate were doc comments recording its absence.
//!
//! **`loopback_enabled` is `false` on this path, always** — see
//! [`ClaudeCodeAcpAgent::new_session`]. The permission ask the editor answers is
//! the *launch* ask, which is the runtime tier PLX-83 identified as taking over
//! where static disclosure ends at `proc:spawn`. What this build does NOT do is
//! convert `loopback.permit` itself; see the crate-level `acp` module docs.
//!
//! # Tenancy
//!
//! There is no tenant check in this file, and that is deliberate. The agent
//! holds an `Arc<ClaudeCode>` and a `SessionMount` that were **built for one
//! tenant** by `builder.rs`'s `TenantSubtreeFactory` — a closure reachable only
//! with an `AdmittedTenant` in hand. A second tenant gets a second agent with a
//! second mount. Isolation is therefore *not having the other tenant's
//! sessions*, rather than *declining to serve them*, which is PLX-127's
//! standard: **absence, not denial**. `tests/acp_tenancy.rs` attacks it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use plexus_acp::v1::runtime::{AcpClient, AcpSessionRuntime, SessionEdge, SessionMount};
use plexus_acp::v1::schema::{
    CancelNotification, ClientCapabilities, ContentBlock, Error, InitializeRequest,
    ListSessionsRequest, ListSessionsResponse, NewSessionRequest,
    NewSessionResponse, PermissionOption, PermissionOptionKind, PromptRequest, PromptResponse,
    RequestPermissionRequest, SessionId, SessionInfo, StopReason, ToolCallUpdate,
    ToolCallUpdateFields,
};
use plexus_acp::v1::transport::Peer;
use plexus_acp::v1::Result as AcpResult;
use plexus_core::capability::Permission;
use plexus_core::ir::{ActivationIr, AuthRequirementIr, ChildEdge, MethodIr};
use plexus_core::runtime::{
    entry, DeclaredHandler, HandlerTable, IntoTurnStop, TurnError, TurnOutcome, TurnRequest,
};

use super::projection::ChatEventProjection;
use crate::activations::claudecode::{ChatEvent, ClaudeCode, Model};

/// The namespace this agent renders under on the Connectome.
pub const ACP_NAMESPACE: &str = "claudecode_acp";

/// The dotted name of the `session/prompt` turn, as it appears in the
/// `ActivationIr` the turn is entered against.
const PROMPT_METHOD: &str = "prompt";

/// The two permission options the editor is offered. Allow-versus-deny is read
/// off **these** by PLX-137's classifier, never from an id's spelling — so
/// renaming them cannot silently turn a denial into an approval.
const ALLOW_ID: &str = "allow-launch";
const DENY_ID: &str = "reject-launch";

// ===========================================================================
// The agent
// ===========================================================================

/// `claudecode`, speaking ACP v1.
///
/// One of these per tenant. See the module docs for why that sentence is the
/// whole of criterion c4.
pub struct ClaudeCodeAcpAgent {
    /// The activation this agent is a protocol face for. In a tenant hub this
    /// is the *confined* instance — `ClaudeCode::confined_to(..)` — because
    /// `builder.rs` builds it that way and this type never unwraps it.
    claudecode: Arc<ClaudeCode>,
    /// PLX-138's `Indexed` edge. The only route from a `sessionId` to a
    /// session, and the thing `connectome_edge()` renders.
    mount: SessionMount<AcpSessionRuntime>,
    /// The transport's handle on the editor. `updates()` is the notification
    /// sink, `callbacks()` the outlet that puts a `session/request_permission`
    /// on the wire and answers the `TurnControl` when the editor replies.
    peer: Peer,
    /// What the client said it could do at `initialize`. Stored because it is
    /// the handle's input; **not** consulted before asking for permission,
    /// because `session/request_permission` is ungated (PLX-135's asymmetry).
    negotiated: Mutex<ClientCapabilities>,
    /// Distinguishes sessions minted by this agent instance.
    next_id: AtomicU64,
}

impl std::fmt::Debug for ClaudeCodeAcpAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClaudeCodeAcpAgent")
            .field("sessions", &self.mount.len())
            .field("confined", &self.claudecode.confinement().is_some())
            .finish_non_exhaustive()
    }
}

impl ClaudeCodeAcpAgent {
    /// Build an agent over `claudecode`, speaking to `peer`.
    #[must_use]
    pub fn new(claudecode: Arc<ClaudeCode>, peer: Peer) -> Self {
        // The edge declares `session/list`, so `sessionCapabilities.list`
        // becomes true — derived from ONE input, per PLX-138 c4. There is no
        // second place to say it.
        let edge = SessionEdge::new(ActivationIr::new(ACP_NAMESPACE, "1.0.0"))
            .with_list_method(format!("{ACP_NAMESPACE}.session.list"));
        Self {
            claudecode,
            mount: SessionMount::new(edge),
            peer,
            negotiated: Mutex::new(ClientCapabilities::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// The mount, exposed so a test can assert isolation rather than assume it.
    #[must_use]
    pub const fn mount(&self) -> &SessionMount<AcpSessionRuntime> {
        &self.mount
    }

    /// The activation underneath, exposed for the same reason.
    #[must_use]
    pub const fn claudecode(&self) -> &Arc<ClaudeCode> {
        &self.claudecode
    }

    /// Mint a session id.
    ///
    /// # A v4 uuid, and it is load-bearing rather than stylistic
    ///
    /// The first version of this minted `acp-{pid}-{counter}` from a
    /// per-agent counter — and `tests/acp_tenancy.rs` caught it immediately:
    /// **two tenants in one process both minted `acp-{pid}-1`**, so tenant B
    /// asking for "tenant A's session id" resolved to B's own session and the
    /// escape test failed. Nothing crossed, but the id had stopped identifying
    /// a session process-wide, and any consumer that treated it as an
    /// identifier — a log, a cache, an operator reading two transcripts — would
    /// have conflated two tenants' sessions.
    ///
    /// A per-agent counter is the natural thing to write and the wrong thing to
    /// ship the moment there is more than one agent. `Uuid::new_v4` is what
    /// `claudecode` itself already mints for a session, which is also what
    /// PLX-138's spike found ACP expects: the **agent** assigns the id.
    fn mint(&self) -> SessionId {
        SessionId::new(format!(
            "acp-{}-{}",
            self.next_id.fetch_add(1, Ordering::SeqCst),
            uuid::Uuid::new_v4()
        ))
    }

    /// The claudecode session name for an ACP `sessionId`.
    ///
    /// **They are the same string.** PLX-138 c1 praised exactly this in the
    /// runtime — "the agent mints the id and THE SAME STRING is what the edge
    /// resolves — no translation layer" — and the same argument applies here:
    /// a side map from ACP ids to claudecode names would be a second registry,
    /// and a second registry is a second thing that can disagree.
    fn session_name(id: &SessionId) -> String {
        id.0.to_string()
    }
}

// ===========================================================================
// The ACP surface
// ===========================================================================

// `initialize` is deliberately absent: `#[acp_agent]` generates it, and it is
// the only writer of `AgentCapabilities`. `load_session`, `authenticate` and
// the rest are absent too, and their absence IS their advertisement — omitting
// an optional item is what makes `agentCapabilities.loadSession` false.
#[plexus_macros::acp_agent(name = "plexus-claudecode", version = "1.0.0")]
impl ClaudeCodeAcpAgent {
    /// Open a session: a real `claudecode` session, wrapped in ACP's runtime.
    async fn new_session(&self, request: NewSessionRequest) -> AcpResult<NewSessionResponse> {
        let id = self.mint();
        let name = Self::session_name(&id);
        let cwd = request.cwd.to_string_lossy().into_owned();

        // A REAL claudecode session, through the activation's own method. Note
        // the two `false`/`None` arguments: `loopback_enabled` and
        // `loopback_session_id`.
        //
        // THIS IS CRITERION c3 AT ITS ONLY HONEST POINT. The loopback's
        // correlation — the `?session_id=` query param, the
        // `PLEXUS_SESSION_ID` env var, the 1s x 300s poll — is reachable only
        // when `loopback_enabled` is true. On the ACP path it never is, so
        // none of it is on this path at all. The permission ask that replaces
        // it is `AcpClient::request_permission`, below, and it asks the
        // EDITOR.
        self.claudecode
            .create(
                name,
                cwd,
                // ACP's `session/new` has no model field, so the session takes
                // claudecode's own working default rather than this file
                // inventing a mapping the protocol does not carry.
                Model::Sonnet,
                None,
                Some(false),
                None,
            )
            .await
            .map_err(|e| Error::internal_error().data(e.to_string()))?;

        let runtime = AcpSessionRuntime::new(
            id.clone(),
            self.peer.updates(),
            Arc::new(ChatEventProjection),
        )
        .with_callbacks(self.peer.callbacks());

        self.mount.insert(&id, Arc::new(runtime));
        Ok(NewSessionResponse::new(id))
    }

    /// Enumerate this agent's sessions.
    ///
    /// Advertised **iff** the edge declares a `list_method` (PLX-138 c4). The
    /// list is the mount's, so it is one tenant's — see `tests/acp_tenancy.rs`.
    async fn list_sessions(&self, _request: ListSessionsRequest) -> AcpResult<ListSessionsResponse> {
        Ok(ListSessionsResponse::new(
            self.mount
                .list()
                .into_iter()
                .map(|id| SessionInfo::new(id, "/"))
                .collect::<Vec<_>>(),
        ))
    }

    /// One prompt turn.
    async fn prompt(&self, request: PromptRequest) -> AcpResult<PromptResponse> {
        // THE ONLY route from a caller-supplied id to a session. A session
        // this mount did not mint resolves to `None`, and the error below is
        // BYTE-IDENTICAL to the one a never-minted id gets — because
        // confirming that some *other* mount has it would be the disclosure
        // PLX-127 c2 exists to prevent.
        let Some(runtime) = self.mount.resolve(request.session_id.0.as_ref()) else {
            return Err(Error::invalid_params().data("unknown sessionId"));
        };

        let text = prompt_text(&request);
        let session = request.session_id.clone();
        let name = Self::session_name(&session);
        let claudecode = Arc::clone(&self.claudecode);
        let peer = self.peer.clone();
        let caps = self.negotiated();

        let handler = DeclaredHandler::new::<(Permission,), _, _>(move |input| {
            let text = text.clone();
            let name = name.clone();
            let session = session.clone();
            let claudecode = Arc::clone(&claudecode);
            let peer = peer.clone();
            let caps = caps.clone();
            async move {
                // Turn-scoped by construction: the capability client, the
                // cancellation token and the correlation table all come from
                // this turn, and this turn was opened by the editor's
                // `session/prompt`.
                let acp = AcpClient::from_turn(&input.turn, session, caps, peer.updates());

                // ── The runtime permission tier (PLX-83). ─────────────────
                // Static disclosure ends at `proc:spawn`; this is the tier
                // that takes over. One line, no correlation: no id, no map,
                // no poll interval, no query param, no env var, no status
                // flag.
                let decision = acp
                    .request_permission(RequestPermissionRequest::new(
                        acp.session_id().clone(),
                        ToolCallUpdate::new(
                            "claudecode-launch",
                            ToolCallUpdateFields::new()
                                .title(format!("Run claude in session {name}"))
                                .raw_input(serde_json::json!({ "prompt": text })),
                        ),
                        vec![
                            PermissionOption::new(ALLOW_ID, "Allow", PermissionOptionKind::AllowOnce),
                            PermissionOption::new(DENY_ID, "Reject", PermissionOptionKind::RejectOnce),
                        ],
                    ))
                    .await
                    .map_err(|e| TurnError::callback_failed(e.to_string()))?;

                // A denial is a REFUSAL, not a failure. `PermissionDenied`
                // decides its own kind through PLX-112's `IntoTurnStop`;
                // nothing here names one. On the wire this is
                // `{"stopReason":"refusal"}` with no `error` key.
                match decision.allowed() {
                    Ok(_) => {}
                    Err(denied) => return denied.into_turn_stop().into_handler_result(),
                }

                // ── The turn proper. ──────────────────────────────────────
                // claudecode's own streaming vocabulary goes out as turn
                // updates; the session runtime projects each one onto
                // `session/update` through `ChatEventProjection`.
                let mut events = Box::pin(claudecode.chat(name, text, Some(false), None).await);
                while let Some(event) = events.next().await {
                    let fatal = matches!(event, ChatEvent::Err { .. });
                    if let Ok(value) = serde_json::to_value(&event) {
                        input.turn.emit(value).await.ok();
                    }
                    if fatal {
                        // A chat error is a FAILURE, and it must not be
                        // confusable with the refusal above. It takes the
                        // `Err` half, which PLX-112 leaves defined as
                        // `Failed`.
                        if let ChatEvent::Err { message } = event {
                            return Err(TurnError::new("claudecode_chat_failed", message));
                        }
                    }
                }

                Ok(TurnOutcome::complete())
            }
        });

        let ir = ActivationIr::new(ACP_NAMESPACE, "1.0.0").with_method(
            handler.declare(
                MethodIr::new(PROMPT_METHOD, format!("{ACP_NAMESPACE}.{PROMPT_METHOD}"))
                    .with_auth(AuthRequirementIr::Public),
            ),
        );
        let handlers = HandlerTable::new([(PROMPT_METHOD, handler.into_handler())]);
        let turn = entry(&ir, &handlers, TurnRequest::new(PROMPT_METHOD))
            .map_err(|e| Error::internal_error().data(e.to_string()))?;

        let outcome = runtime.prompt(turn).await;
        Ok(PromptResponse::new(
            outcome.stop_reason().unwrap_or(StopReason::EndTurn),
        ))
    }

    /// `session/cancel` — a notification, so it never errors.
    async fn cancel(&self, notification: CancelNotification) -> AcpResult<()> {
        if let Some(runtime) = self.mount.resolve(notification.session_id.0.as_ref()) {
            // Reaches the RUNNING TURN, not a flag — PLX-138 c3.
            runtime.cancel();
        }
        Ok(())
    }
}

// ===========================================================================
// `initialize`'s one side effect
// ===========================================================================

impl ClaudeCodeAcpAgent {
    /// Record what the client negotiated.
    ///
    /// `#[acp_agent]` owns `initialize` — it is the only writer of
    /// `AgentCapabilities`, which is what makes "advertised" and "implemented"
    /// one act. So the *client's* half is captured here, by the transport
    /// wrapper, rather than by an `initialize` this type is forbidden to write.
    pub fn negotiate(&self, request: &InitializeRequest) {
        if let Ok(mut guard) = self.negotiated.lock() {
            guard.clone_from(&request.client_capabilities);
        }
    }

    /// What the client negotiated, for assertions.
    #[must_use]
    pub fn negotiated(&self) -> ClientCapabilities {
        self.negotiated
            .lock()
            .map_or_else(|_| ClientCapabilities::new(), |g| g.clone())
    }

    /// **The other of PLX-138's two lines.**
    ///
    /// RFC 002 §5.1's `Indexed` family: one path template and an `id_field` of
    /// `sessionId`, and **no instance ids**. Rendering the ids here would make
    /// the Connectome an enumeration surface, and PLX-127 c2's finding is that
    /// enumeration is disclosure — `connectome` takes no `AuthContext`, so
    /// anything named in it is named to everybody who can read it.
    #[must_use]
    pub fn acp_connectome_edge(&self) -> ChildEdge {
        self.mount.connectome_edge()
    }
}

// ===========================================================================
// The plexus surface — hand-written, for the reason `TenantMount` is
// ===========================================================================

#[async_trait::async_trait]
impl plexus_core::plexus::Activation for ClaudeCodeAcpAgent {
    type Methods = AcpAgentMethod;

    fn namespace(&self) -> &str {
        ACP_NAMESPACE
    }

    fn version(&self) -> &'static str {
        "1.0.0"
    }

    fn description(&self) -> &'static str {
        "claudecode, speaking Agent Client Protocol v1"
    }

    fn methods(&self) -> Vec<&str> {
        // The ACP surface is not a jsonrpsee surface. Sessions are reached
        // over the ACP transport, not by dotted RPC name, so advertising
        // method names here would advertise a route that does not exist.
        Vec::new()
    }

    fn plugin_id(&self) -> uuid::Uuid {
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_DNS,
            ACP_NAMESPACE.as_bytes(),
        )
    }

    async fn call(
        &self,
        method: &str,
        _params: serde_json::Value,
        _auth: Option<&plexus_core::plexus::AuthContext>,
        _raw: Option<&plexus_core::request::RawRequestContext>,
    ) -> Result<plexus_core::plexus::PlexusStream, plexus_core::plexus::PlexusError> {
        Err(plexus_core::plexus::PlexusError::MethodNotFound {
            activation: ACP_NAMESPACE.to_string(),
            method: method.to_string(),
        })
    }

    fn into_rpc_methods(self) -> jsonrpsee::core::server::Methods {
        // Deliberately empty, for `TenantMount`'s reason: the instance family
        // is unbounded and is reached through the ACP transport.
        jsonrpsee::core::server::Methods::new()
    }

    /// **PLX-138's first promised line.**
    fn connectome_edge(&self) -> Option<ChildEdge> {
        Some(self.mount.connectome_edge())
    }
}

/// The (empty) method enum the `Activation` trait's schema derivation needs.
///
/// ACP methods are not jsonrpsee methods — see `Activation::methods` above —
/// so this names none of them rather than mirroring the ACP surface in a
/// second vocabulary. `plexus_core`'s own `TenantMountMethods` is the same
/// shape for the same reason: a surface that is one `Indexed` edge has no
/// method enum to carry.
#[derive(Debug, Clone, schemars::JsonSchema)]
pub struct AcpAgentMethod;

impl plexus_core::plexus::MethodEnumSchema for AcpAgentMethod {
    fn method_names() -> &'static [&'static str] {
        &[]
    }

    fn schema_with_consts() -> serde_json::Value {
        serde_json::json!({ "type": "object", "properties": {} })
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

/// The prompt's text blocks, joined. ACP allows several content blocks per
/// prompt; `claudecode.chat` takes one string.
fn prompt_text(request: &PromptRequest) -> String {
    request
        .prompt
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text(text) => Some(text.text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}
