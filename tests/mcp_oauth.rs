//! TRANSPORT-13: MCP OAuth end to end — an MCP client is sent to plexus-idp
//! by substrate's `/mcp`, signs in as a person, and comes back with a token
//! bound to `/mcp` that the mounted bridge admits and the gate decides on.
//!
//! The client side is rmcp 3.8's own `AuthorizationManager`: discovery from
//! the 401's `resource_metadata`, registration, the authorization URL with
//! PKCE S256 and `resource`, the exchange with the issuer checked (RFC 9207),
//! then `AuthClient` presenting the token on every MCP request. The test
//! plays the person: it loads the form and posts the password.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ed25519_dalek::SigningKey;
use plexus_auth_core::adapters::key_principal;
use plexus_core::plexus::DynamicHub;
use plexus_idp::core::IdentityCore;
use plexus_idp::keys::KeyStore;
use plexus_idp::store::IdentityStore;
use plexus_idp::tokens::TokenIssuer;
use plexus_substrate::login::{self, LoginConfig, StartGrant};
use plexus_transport::plexus_transport_mcp::OAuthConfig;
use plexus_transport::{McpConfig, TransportServer};
use rmcp::model::CallToolRequestParams;
use rmcp::transport::auth::{AuthClient, AuthorizationManager};
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::ServiceExt;
use serde_json::{json, Value};

/// The operator: `--admin`.
const OPERATOR: [u8; 32] = [0x11; 32];
const PASSWORD: &str = "hunter2hunter2";

fn principal_of(seed: &[u8; 32]) -> String {
    key_principal(SigningKey::from_bytes(seed).verifying_key().as_bytes()).to_string()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// plexus-idp in this process: its OIDC surface on an ephemeral port, the
/// MCP resource a registered audience, one person provisioned.
struct Idp {
    issuer: String,
    /// alice's id: her principal is `idp:<id>`.
    alice: String,
    _dir: tempfile::TempDir,
}

async fn idp(resource: &str) -> Idp {
    let dir = tempfile::tempdir().unwrap();
    let store = IdentityStore::open(dir.path().join("idp.db").to_str().unwrap()).await.unwrap();
    let keys = Arc::new(KeyStore::load_or_generate(&dir.path().join("keys")).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let token_issuer = Arc::new(TokenIssuer::new(issuer.clone(), "plexus:idp".into(), keys.clone()).with_audiences([resource]));
    let core = IdentityCore::new(store, token_issuer);
    let alice = core.provision_user("alice".into(), PASSWORD.into(), None, None, None).await.unwrap();
    let router = plexus_idp::oidc::router(core, keys, issuer.clone());
    tokio::spawn(async move { axum08::serve(listener, router).await.unwrap() });
    Idp { issuer, alice, _dir: dir }
}

/// substrate under login — `key` for agents, plexus-idp for people — with
/// `/mcp` mounted as an OAuth 2.1 resource server naming the idp.
struct Served {
    mcp: SocketAddr,
    mcp_url: String,
    idp: Idp,
    _dir: tempfile::TempDir,
}

async fn serve() -> Served {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let stub = dir.path().join("claude-stub");
    std::fs::write(
        &stub,
        concat!(
            "#!/bin/sh\n",
            "echo '{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"stub-session\",\"model\":\"stub\",\"cwd\":\".\",\"tools\":[]}'\n",
            "echo '{\"type\":\"result\",\"subtype\":\"success\",\"session_id\":\"stub-session\",\"is_error\":false,\"result\":\"stubbed answer\"}'\n",
        ),
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    assert!(plexus_substrate::activations::storage::set_data_dir(data), "the data directory is this file's");

    let ws = free_port();
    let mcp: SocketAddr = ([127, 0, 0, 1], free_port()).into();
    let mcp_url = format!("http://{mcp}/mcp");
    let idp = idp(&mcp_url).await;

    let cfg = LoginConfig {
        providers: vec!["key".parse().unwrap(), format!("idp={},{}", idp.issuer, mcp_url).parse().unwrap()],
        admins: vec![principal_of(&OPERATOR)],
        // Roles merge by name: echo declares a `viewer` list, claudecode an
        // `editor` one, so alice holds both.
        grants: vec![
            format!("editor=idp:{}", idp.alice).parse::<StartGrant>().unwrap(),
            format!("viewer=idp:{}", idp.alice).parse::<StartGrant>().unwrap(),
        ],
        ws_port: ws,
        login_port: None,
        public_url: None,
    };
    let (service, login) = login::build(&cfg, Some(stub.to_string_lossy().into_owned())).await.expect("the service behind the login");
    let config = McpConfig::listen(mcp).with_oauth(OAuthConfig {
        resource: mcp_url.clone(),
        authorization_servers: vec![idp.issuer.clone()],
        scopes: None,
    });
    let builder = TransportServer::builder(service, |s| DynamicHub::arc_into_rpc_module(s).map_err(|e| anyhow::anyhow!("{e}")))
        .with_websocket(ws)
        .without_registry()
        .with_mcp(config);
    let server = login.apply(builder).build().await.expect("the socket and the mount");
    tokio::spawn(async move {
        server.serve().await.expect("serves");
    });
    for port in [ws, mcp.port()] {
        let mut up = false;
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                up = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(up, "port {port} listens");
    }
    Served { mcp, mcp_url, idp, _dir: dir }
}

fn http() -> reqwest::Client {
    reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap()
}

/// A raw `initialize` at `/mcp`, with `bearer` when given.
async fn raw_initialize(mcp_url: &str, bearer: Option<&str>) -> reqwest::Response {
    let mut req = http()
        .post(mcp_url)
        .header("accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}}));
    if let Some(b) = bearer {
        req = req.bearer_auth(b);
    }
    req.send().await.unwrap()
}

/// `application/x-www-form-urlencoded`, by hand: substrate's reqwest has no `form` feature.
fn encoded(fields: &[(String, String)]) -> String {
    let mut s = reqwest::Url::parse("http://x/").unwrap();
    s.query_pairs_mut().extend_pairs(fields.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    s.query().unwrap_or_default().to_string()
}

fn query_of(url: &str) -> std::collections::HashMap<String, String> {
    reqwest::Url::parse(url).unwrap().query_pairs().map(|(k, v)| (k.into_owned(), v.into_owned())).collect()
}

fn text(r: &rmcp::model::CallToolResult) -> String {
    r.content.iter().filter_map(|c| c.as_text().map(|t| t.text.clone())).collect::<Vec<_>>().join("")
}

#[tokio::test(flavor = "multi_thread")]
async fn an_mcp_client_signs_in_at_plexus_idp_and_calls_as_that_person() {
    let s = serve().await;

    // 1. No token: the 401 names the resource metadata; the document names the idp.
    let r = raw_initialize(&s.mcp_url, None).await;
    assert_eq!(r.status(), 401);
    let challenge = r.headers()["www-authenticate"].to_str().unwrap().to_string();
    let metadata_url = format!("http://{}/.well-known/oauth-protected-resource/mcp", s.mcp);
    assert!(challenge.contains(&format!("resource_metadata=\"{metadata_url}\"")), "{challenge}");
    let doc: Value = http().get(&metadata_url).send().await.unwrap().json().await.unwrap();
    assert_eq!(doc["resource"], s.mcp_url);
    assert_eq!(doc["authorization_servers"], json!([s.idp.issuer]));

    // 2. rmcp's client discovers the authorization server through that document.
    let mut manager = AuthorizationManager::new_allowing_private_addresses(&s.mcp_url).await.unwrap();
    let resolution = manager.resolve_metadata_from_challenge(Some(&challenge)).await.expect("the AS is found through the document");
    assert_eq!(resolution.metadata.issuer.as_deref(), Some(s.idp.issuer.as_str()));
    assert_eq!(resolution.metadata.authorization_endpoint, format!("{}/oauth/authorize", s.idp.issuer));
    manager.set_metadata(resolution.metadata);

    // 3. Registration; the authorization URL with S256 and the resource.
    let config = manager.register_client("substrate-test", "http://localhost:1/callback", &[]).await.unwrap();
    manager.configure_client(config).unwrap();
    let url = manager.get_authorization_url(&[]).await.unwrap();
    let q = query_of(&url);
    assert_eq!(q["code_challenge_method"], "S256");
    assert_eq!(q["resource"], s.mcp_url, "RFC 8707: the token is asked for /mcp");
    let state = q["state"].clone();

    // 4. The person: the form, then the password; the code comes back with iss.
    let page = http().get(&url).send().await.unwrap();
    assert_eq!(page.status(), 200);
    assert!(page.text().await.unwrap().contains("substrate-test"));
    let mut form: Vec<(String, String)> = q.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    form.push(("username".into(), "alice".into()));
    form.push(("password".into(), PASSWORD.into()));
    let back = http()
        .post(format!("{}/oauth/authorize", s.idp.issuer))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(encoded(&form))
        .send()
        .await
        .unwrap();
    assert_eq!(back.status(), 302);
    let cb = query_of(back.headers()["location"].to_str().unwrap());
    assert_eq!(cb["state"], state);
    assert_eq!(cb["iss"], s.idp.issuer);
    manager.exchange_code_for_token_with_issuer(&cb["code"], &cb["state"], Some(&cb["iss"])).await.expect("the exchange");
    let access = manager.get_access_token().await.unwrap();
    let payload: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(access.split('.').nth(1).unwrap()).unwrap()).unwrap();
    assert_eq!(payload["aud"], s.mcp_url, "audience-bound to /mcp");
    assert_eq!(payload["username"], "alice");

    // 5. MCP as alice: rmcp's AuthClient presents the token on every request.
    let auth_client = AuthClient::new(reqwest::Client::default(), manager);
    let transport = StreamableHttpClientTransport::with_client(auth_client, StreamableHttpClientTransportConfig::with_uri(s.mcp_url.clone()));
    let alice = ().serve(transport).await.expect("initialize as alice");
    let mut tools: Vec<String> = alice.peer().list_all_tools().await.unwrap().into_iter().map(|t| t.name.to_string()).collect();
    tools.sort();
    for expected in ["echo.once", "echo.ping", "health.check", "auth.whoami"] {
        assert!(tools.contains(&expected.to_string()), "{expected} in {tools:?}");
    }
    assert!(!tools.contains(&"claudecode.sessions_list".to_string()), "admin's alone: {tools:?}");
    let r = alice
        .peer()
        .call_tool(CallToolRequestParams::new("echo.once").with_arguments(json!({"message": "hi"}).as_object().cloned().unwrap()))
        .await
        .expect("echo.once is answered");
    assert_ne!(r.is_error, Some(true), "{}", text(&r));
    assert!(text(&r).contains("hi"), "{}", text(&r));
    // Who the gate sees: the person, not a key.
    let who = alice.peer().call_tool(CallToolRequestParams::new("auth.whoami")).await.expect("auth.whoami");
    assert!(text(&who).contains(&format!("idp:{}", s.idp.alice)), "{}", text(&who));
    alice.cancel().await.unwrap();

    // 6. A token for another audience: refused at the HTTP layer.
    let other: Value = http()
        .post(format!("{}/oauth/token", s.idp.issuer))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(encoded(&[("grant_type".into(), "password".into()), ("username".into(), "alice".into()), ("password".into(), PASSWORD.into()), ("audience".into(), "plexus:idp".into())]))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let r = raw_initialize(&s.mcp_url, other["access_token"].as_str()).await;
    assert_eq!(r.status(), 401);
    assert!(r.headers()["www-authenticate"].to_str().unwrap().contains("error=\"invalid_token\""));
}
