//! PLEXUS-6: substrate takes callers the way menger does (IDENTITY-14,
//! AUTH-7, ACCESS-24) — through `plexus-login`, menger's login lifted for
//! any hub, with its ready-made global-role rules ([`SimpleAccess`]).
//!
//! - **No open mode.** A login provider is required: `key` (agents'
//!   Ed25519 key proofs, synapse's `_login`), and people's providers
//!   (`idp=…`, `workos=…`, `oidc:<name>=…`, …) where they apply. Without
//!   one, substrate does not start ([`refusal`]).
//! - **The gate on every call.** The hub is built behind plexus-core's gate
//!   ([`crate::builder::BuildOptions::auth`]): an anonymous connection
//!   reaches only the auth door (`auth.providers`, `auth.login`,
//!   `auth.signup`); a credential presented and refused is a 401 at the
//!   upgrade; a signed-in caller reaches each method whose grant a role it
//!   holds lists, and is refused typed (`error_kind: forbidden`) otherwise.
//! - **Roles.** Each method is its own grant (`<activation>.<method>`);
//!   `viewer` / `editor` are what the activations declare in `roles(...)`
//!   (health, echo, claudecode's own sessions); `admin` holds every grant.
//!   Roles are global (`*`); `--admin` names the first admins and `--grant
//!   ROLE=PRINCIPAL` the standing grants (a bridge's key: `editor`). An
//!   admin grants more at runtime with `auth.grant` (forgotten at restart).

use std::str::FromStr;
use std::sync::Arc;

use plexus_core::plexus::{Activation, AuthSetup, DynamicHub};
use plexus_login::login::Provider;
use plexus_login::simple::{SimpleAccess, SimpleRole};
use plexus_login::{Branding, LoginAccess, LoginOptions, Wired, Wiring};
use plexus_transport::TransportServerBuilder;

use crate::builder::{build_plexus_rpc_with, BuildOptions};

/// The app as the login page and the session cookies name it.
pub const BRANDING: Branding = Branding { app: "substrate", version: env!("CARGO_PKG_VERSION"), mark: "s", motto: "" };

/// A standing grant given at startup, `ROLE=PRINCIPAL` (e.g.
/// `editor=key:54a9…`): a global role held from the start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartGrant {
    pub role: SimpleRole,
    pub principal: String,
}

impl FromStr for StartGrant {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        let (role, principal) = s
            .split_once('=')
            .ok_or_else(|| format!("`{s}` is not ROLE=PRINCIPAL (e.g. editor=key:<hex>)"))?;
        let principal = principal.trim();
        principal
            .parse::<plexus_auth_core::identity::Principal>()
            .map_err(|e| format!("`{principal}` is not a principal ({e})"))?;
        Ok(Self { role: role.parse()?, principal: principal.to_string() })
    }
}

/// How substrate takes callers.
#[derive(Clone, Debug, Default)]
pub struct LoginConfig {
    /// `--auth-provider`s; at least one.
    pub providers: Vec<Provider>,
    /// `--admin` principals: admin everywhere from the start.
    pub admins: Vec<String>,
    /// `--grant ROLE=PRINCIPAL`s.
    pub grants: Vec<StartGrant>,
    /// The WebSocket's port: a key proof names this socket.
    pub ws_port: u16,
    /// `--login-port`: where the login page for people is served, if
    /// anywhere (agents need none).
    pub login_port: Option<u16>,
    /// `--public-url`: where people reach the login page.
    pub public_url: Option<String>,
}

/// Why this start is refused, if it is: no login provider (there is no open
/// mode), or stdio (it cannot carry a credential).
pub fn refusal(providers: &[Provider], stdio: bool) -> Option<String> {
    if stdio {
        return Some(
            "substrate under login does not serve stdio: it cannot carry a credential — \
             reach it over the WebSocket with a key (synapse _login, --auth-provider key)"
                .into(),
        );
    }
    providers.is_empty().then(|| {
        "substrate has no open mode: give it a login provider with --auth-provider <SPEC> \
         (SUBSTRATE_AUTH_PROVIDERS) — `--auth-provider key` admits agents' Ed25519 keys, \
         with `--admin key:<hex>` for the operator"
            .into()
    })
}

/// The login, wired: the providers' adapters, the session table, the auth
/// backend and the rules.
pub struct Login {
    pub wired: Wired<SimpleAccess>,
    pub access: SimpleAccess,
}

impl Login {
    /// Wire the login from `cfg`, its standing grants given.
    pub async fn new(cfg: &LoginConfig) -> anyhow::Result<Self> {
        if let Some(why) = refusal(&cfg.providers, false) {
            anyhow::bail!(why);
        }
        let mut opts = LoginOptions::new(BRANDING, cfg.ws_port, cfg.login_port.unwrap_or(cfg.ws_port));
        opts.providers = cfg.providers.clone();
        opts.page = cfg.login_port.is_some();
        opts.public_url = cfg.public_url.clone();
        let wiring = Wiring::new(opts)?;
        let access = SimpleAccess::new(wiring.sessions(), &cfg.admins)?;
        for g in &cfg.grants {
            access.dev_grant(&g.principal, "*", g.role).await.map_err(anyhow::Error::msg)?;
        }
        let wired = wiring.finish(access.clone(), None)?;
        Ok(Self { wired, access })
    }

    /// What the hub's `auth` activation and gate are built from.
    pub fn auth_setup(&self) -> AuthSetup {
        self.access.auth_setup(self.wired.backend.clone())
    }

    /// The upgrade's half on a transport builder: the gate, the anonymous
    /// view, the protected resource, the allowed origins.
    pub fn apply<H: Activation>(&self, builder: TransportServerBuilder<H>) -> TransportServerBuilder<H> {
        self.wired.apply(builder)
    }

    /// The login page's routes, when there is a page.
    pub fn router(&self) -> Option<axum08::Router> {
        self.wired.router()
    }
}

/// The hub behind the login: every activation, the `auth` activation, the
/// gate, and the roles the activations declare seeded.
pub async fn build(cfg: &LoginConfig, claude_path: Option<String>) -> anyhow::Result<(Arc<DynamicHub>, Login)> {
    let login = Login::new(cfg).await?;
    let hub = build_plexus_rpc_with(BuildOptions { auth: Some(login.auth_setup()), claude_path }).await;
    login.access.seed_roles(&hub);
    Ok((hub, login))
}
