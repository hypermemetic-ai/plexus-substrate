//! TRANSPORT-18: `/mcp` under the login, on a throwaway server — the mounted
//! bridge (plexus-transport-mcp through `with_mcp`), driven by an rmcp 3.x
//! client over streamable HTTP.
//!
//! The cases: the builder's key (an `editor`) signs in over the socket as
//! synapse does — the proof at the upgrade, `auth.login {session}` — and
//! presents the session on `/mcp`: it lists what its role holds and calls
//! `echo.once`; no credential sees the auth door only; a refused credential
//! is a 401 with RFC 6750's challenge.

use std::net::SocketAddr;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::ws_client::{HeaderMap, HeaderValue, WsClient, WsClientBuilder};
use plexus_auth_core::adapters::{key_principal, sign_key_proof};
use plexus_core::plexus::DynamicHub;
use plexus_substrate::login::{self, LoginConfig, StartGrant};
use plexus_transport::{McpConfig, TransportServer};
use rmcp::model::CallToolRequestParams;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::ServiceExt;
use serde_json::{json, Value};

/// The operator: `--admin`.
const OPERATOR: [u8; 32] = [0x11; 32];
/// The builder's key: `editor`.
const BUILDER: [u8; 32] = [0x22; 32];

fn principal_of(seed: &[u8; 32]) -> String {
    key_principal(SigningKey::from_bytes(seed).verifying_key().as_bytes()).to_string()
}

fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64
}

/// A nonce no other proof in this process used.
fn nonce() -> [u8; 32] {
    static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
    let mut n = [0u8; 32];
    n[..4].copy_from_slice(&N.fetch_add(1, std::sync::atomic::Ordering::Relaxed).to_le_bytes());
    n[4..8].copy_from_slice(&std::process::id().to_le_bytes());
    n[8..24].copy_from_slice(&std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos().to_le_bytes());
    n
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// The throwaway server: substrate under login (`key`), the operator admin,
/// the builder an editor, `/mcp` mounted beside the WebSocket.
struct Served {
    ws: u16,
    mcp: SocketAddr,
    _dir: tempfile::TempDir,
}

async fn serve() -> Served {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // The stub Claude Code: one turn, in stream-json.
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
    let cfg = LoginConfig {
        providers: vec!["key".parse().unwrap()],
        admins: vec![principal_of(&OPERATOR)],
        // echo declares only a `viewer` list and claudecode an `editor` one
        // (roles merge by name across activations), so the builder holds both.
        grants: vec![
            format!("editor={}", principal_of(&BUILDER)).parse::<StartGrant>().unwrap(),
            format!("viewer={}", principal_of(&BUILDER)).parse::<StartGrant>().unwrap(),
        ],
        ws_port: ws,
        login_port: None,
        public_url: None,
    };
    let (service, login) = login::build(&cfg, Some(stub.to_string_lossy().into_owned())).await.expect("the service behind the login");
    let builder = TransportServer::builder(service, |s| DynamicHub::arc_into_rpc_module(s).map_err(|e| anyhow::anyhow!("{e}")))
        .with_websocket(ws)
        .without_registry()
        .with_mcp(McpConfig::listen(mcp));
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
    Served { ws, mcp, _dir: dir }
}

type Client = rmcp::service::RunningService<rmcp::RoleClient, ()>;

/// An MCP client at `/mcp`, with `bearer` on every request when given.
async fn client(s: &Served, bearer: Option<String>) -> Client {
    let mut config = StreamableHttpClientTransportConfig::with_uri(format!("http://{}/mcp", s.mcp));
    if let Some(b) = bearer {
        config = config.auth_header(b);
    }
    let transport = StreamableHttpClientTransport::with_client(reqwest::Client::default(), config);
    ().serve(transport).await.expect("initialize")
}

/// A fresh proof by `seed` naming the server's socket — what synapse's
/// `_login` presents.
fn proof(s: &Served, seed: &[u8; 32]) -> String {
    sign_key_proof(seed, &format!("ws://127.0.0.1:{}", s.ws), "test", &nonce(), now())
}

/// The session an agent carries: the proof at the upgrade, then
/// `auth.login {flow: {session: {}}}` — what synapse's `_login` does. A proof
/// itself works once (its nonce is spent), so it is never the Bearer of a
/// second HTTP request.
async fn session_token(s: &Served, seed: &[u8; 32]) -> String {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_str(&format!("Bearer {}", proof(s, seed))).unwrap());
    let c: WsClient = WsClientBuilder::default().set_headers(headers).build(format!("ws://127.0.0.1:{}", s.ws)).await.expect("a key proof is let in");
    let mut p = ObjectParams::new();
    p.insert("method", "auth.login").unwrap();
    p.insert("params", json!({ "flow": { "session": {} } })).unwrap();
    let mut sub: Subscription<Value> = c.subscribe("substrate.call", p, "substrate.call_unsub").await.expect("auth.login");
    let mut token = None;
    while let Some(item) = tokio::time::timeout(Duration::from_secs(30), sub.next()).await.expect("an item in time") {
        let item = item.expect("an item");
        match item["type"].as_str() {
            Some("data") => token = item["content"]["token"].as_str().map(str::to_string),
            Some("error") => panic!("auth.login refused: {item}"),
            Some("done") => break,
            _ => {}
        }
    }
    token.expect("a session token")
}

async fn names(c: &Client) -> Vec<String> {
    let mut names: Vec<String> = c.peer().list_all_tools().await.unwrap().into_iter().map(|t| t.name.to_string()).collect();
    names.sort();
    names
}

fn text(r: &rmcp::model::CallToolResult) -> String {
    r.content.iter().filter_map(|c| c.as_text().map(|t| t.text.clone())).collect::<Vec<_>>().join("")
}

#[tokio::test(flavor = "multi_thread")]
async fn the_mounted_mcp_takes_the_logins_callers() {
    let s = serve().await;

    // The builder's session: an editor's view, and a call as the builder.
    let builder = client(&s, Some(session_token(&s, &BUILDER).await)).await;
    let tools = names(&builder).await;
    for expected in ["echo.once", "echo.ping", "echo.echo", "health.check", "claudecode.create", "claudecode.chat", "auth.whoami"] {
        assert!(tools.contains(&expected.to_string()), "{expected} in {tools:?}");
    }
    // Listed by no role (admin alone): not a tool for this caller.
    assert!(!tools.contains(&"claudecode.sessions_list".to_string()), "admin's alone: {tools:?}");
    let r = builder
        .peer()
        .call_tool(CallToolRequestParams::new("echo.once").with_arguments(json!({"message": "hi"}).as_object().cloned().unwrap()))
        .await
        .expect("echo.once is answered");
    assert_ne!(r.is_error, Some(true), "{}", text(&r));
    assert!(text(&r).contains("hi"), "{}", text(&r));
    // A method the editor's role lacks is not callable by name either.
    let refused = builder
        .peer()
        .call_tool(CallToolRequestParams::new("claudecode.sessions_list").with_arguments(json!({"project_path": "x"}).as_object().cloned().unwrap()))
        .await;
    assert!(refused.is_err(), "outside the editor's view");

    // No credential: the auth door only.
    let nobody = client(&s, None).await;
    assert_eq!(names(&nobody).await, ["auth.login", "auth.providers", "auth.signup"]);

    // A refused credential: 401 with the challenge, at the HTTP layer.
    let r = reqwest::Client::default()
        .post(format!("http://{}/mcp", s.mcp))
        .bearer_auth("mallory")
        .header("accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}}}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    let challenge = r.headers()["www-authenticate"].to_str().unwrap().to_string();
    assert!(challenge.contains("error=\"invalid_token\""), "{challenge}");
    let _: Value = json!(null);
}
