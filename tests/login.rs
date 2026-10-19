//! PLEXUS-6: substrate under login, on a throwaway server.
//!
//! One substrate for the whole file, on its own port and its own data
//! directory, wired as `main` wires it: `--auth-provider key`, the
//! operator's key as `--admin`, two agents' keys granted `editor` (the
//! bridge's role) and a fourth key holding nothing. `claudecode` runs a stub
//! in place of Claude Code, which answers one turn in stream-json.
//!
//! The cases: an anonymous call is refused; a wrong key is refused (a proof
//! made for another socket, a forged one, and a valid key holding no role);
//! the builder's key creates a session and chats in it; a second key with
//! the very same role can neither see nor drive it; the operator (admin)
//! reaches it.

use std::sync::{mpsc, LazyLock};
use std::time::Duration;

use ed25519_dalek::SigningKey;
use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::ws_client::{HeaderMap, HeaderValue, WsClient, WsClientBuilder};
use plexus_auth_core::adapters::{key_principal, sign_key_proof};
use plexus_core::plexus::DynamicHub;
use plexus_substrate::login::{self, LoginConfig, StartGrant};
use plexus_transport::TransportServer;
use serde_json::{json, Value};

/// The operator: `--admin`.
const OPERATOR: [u8; 32] = [0x11; 32];
/// The builder's key: `editor`.
const BUILDER: [u8; 32] = [0x22; 32];
/// A second agent's key: `editor` too — the same role, not the same owner.
const OTHER: [u8; 32] = [0x33; 32];
/// A key that signs in fine and holds nothing.
const STRANGER: [u8; 32] = [0x44; 32];

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

/// The throwaway server: its port, and the directory it keeps (held for the
/// process's life).
struct Server {
    port: u16,
    work: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

/// One substrate for every test, on a runtime of its own (a test's runtime
/// ends with the test).
static SERVER: LazyLock<Server> = LazyLock::new(|| {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    // The stub Claude Code: one turn, in stream-json.
    let stub = dir.path().join("claude-stub");
    std::fs::write(
        &stub,
        concat!(
            "#!/bin/sh\n",
            "echo '{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"stub-session\",\"model\":\"stub\",\"cwd\":\".\",\"tools\":[]}'\n",
            "echo '{\"type\":\"assistant\",\"message\":{\"id\":\"m1\",\"role\":\"assistant\",\"model\":\"stub\",\"content\":[{\"type\":\"text\",\"text\":\"stubbed answer\"}]}}'\n",
            "echo '{\"type\":\"result\",\"subtype\":\"success\",\"session_id\":\"stub-session\",\"is_error\":false,\"result\":\"stubbed answer\"}'\n",
        ),
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    assert!(plexus_substrate::activations::storage::set_data_dir(data), "the data directory is this file's");
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let (tx, rx) = mpsc::channel();
    let stub = stub.to_string_lossy().into_owned();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        rt.block_on(async move {
            let cfg = LoginConfig {
                providers: vec!["key".parse().unwrap()],
                admins: vec![principal_of(&OPERATOR)],
                grants: vec![
                    format!("editor={}", principal_of(&BUILDER)).parse::<StartGrant>().unwrap(),
                    format!("editor={}", principal_of(&OTHER)).parse::<StartGrant>().unwrap(),
                ],
                ws_port: port,
                login_port: None,
                public_url: None,
            };
            let (hub, login) = login::build(&cfg, Some(stub)).await.expect("the hub behind the login");
            let builder = TransportServer::builder(hub, |h| DynamicHub::arc_into_rpc_module(h).map_err(|e| anyhow::anyhow!("{e}")))
                .with_websocket(port)
                .without_registry();
            let server = login.apply(builder).build().await.expect("the socket");
            tx.send(()).unwrap();
            server.serve().await.expect("serves");
        });
    });
    rx.recv_timeout(Duration::from_secs(120)).expect("the server was built");
    // Listening once it serves.
    let up = (0..200).any(|_| {
        std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() || {
            std::thread::sleep(Duration::from_millis(50));
            false
        }
    });
    assert!(up, "the server listens on {port}");
    Server { port, work, _dir: dir }
});

fn url() -> String {
    format!("ws://127.0.0.1:{}", SERVER.port)
}

/// `Authorization: Bearer <proof>`.
fn bearer(proof: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("authorization", HeaderValue::from_str(&format!("Bearer {proof}")).unwrap());
    h
}

/// A fresh proof by `seed` naming this server's socket.
fn proof(seed: &[u8; 32]) -> String {
    sign_key_proof(seed, &url(), "test", &nonce(), now())
}

/// A client signed in as `seed`.
async fn signed_in(seed: &[u8; 32]) -> WsClient {
    WsClientBuilder::default().set_headers(bearer(&proof(seed))).build(url()).await.expect("a key proof is let in")
}

/// A client with no credential (the anonymous view).
async fn anonymous() -> WsClient {
    WsClientBuilder::default().build(url()).await.expect("no credential is let into the anonymous view")
}

/// Call `method` through `substrate.call`: its data items, or the error the
/// call was refused with.
async fn call(c: &WsClient, method: &str, params: Value) -> Result<Vec<Value>, String> {
    let mut p = ObjectParams::new();
    p.insert("method", method).unwrap();
    p.insert("params", params).unwrap();
    let mut sub: Subscription<Value> =
        c.subscribe("substrate.call", p, "substrate.call_unsub").await.map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    while let Some(item) = tokio::time::timeout(Duration::from_secs(60), sub.next()).await.expect("an item in time") {
        let item = item.map_err(|e| e.to_string())?;
        match item["type"].as_str() {
            Some("data") => out.push(item["content"].clone()),
            Some("error") => return Err(item.to_string()),
            Some("done") => break,
            _ => {}
        }
    }
    Ok(out)
}

/// The one data item of a unary answer.
fn one(items: &[Value]) -> &Value {
    assert_eq!(items.len(), 1, "one answer: {items:?}");
    &items[0]
}

/// Create a session named `name` as `c`.
async fn create(c: &WsClient, name: &str) -> Value {
    let work = SERVER.work.to_string_lossy().into_owned();
    let items = call(c, "claudecode.create", json!({ "name": name, "working_dir": work, "model": "haiku" })).await.expect("create is an editor's");
    one(&items).clone()
}

fn is_not_found(v: &Value) -> bool {
    v["type"] == "error" && v["message"].as_str().is_some_and(|m| m.contains("not found"))
}

#[tokio::test]
async fn an_anonymous_call_is_refused() {
    let c = anonymous().await;
    let refused = call(&c, "claudecode.list", json!({})).await.expect_err("anonymous reaches only the auth door");
    assert!(refused.to_lowercase().contains("unauthenticated") || refused.contains("sign in"), "{refused}");
    let refused = call(&c, "bash.execute", json!({ "command": "true" })).await.expect_err("nor bash");
    assert!(!refused.is_empty());
    // The auth door is open: providers answers.
    let door = call(&c, "auth.providers", json!({})).await.expect("the auth door");
    assert!(!door.is_empty(), "auth.providers answers anonymous");
}

#[tokio::test]
async fn a_wrong_key_is_refused() {
    let server = &*SERVER;
    // A proof made for another socket: refused at the upgrade (401).
    let elsewhere = sign_key_proof(&BUILDER, "ws://127.0.0.1:1", "test", &nonce(), now());
    assert!(WsClientBuilder::default().set_headers(bearer(&elsewhere)).build(url()).await.is_err(), "a proof for another socket");
    // A forged proof: the builder's proof with another key's name on it.
    let forged = forge(&proof(&BUILDER), &OTHER);
    assert!(WsClientBuilder::default().set_headers(bearer(&forged)).build(url()).await.is_err(), "a forged proof");
    // Garbage.
    assert!(WsClientBuilder::default().set_headers(bearer("plexus-key.not-a-proof")).build(url()).await.is_err(), "garbage");
    // A real key that holds nothing: in, and refused every method, typed.
    let c = signed_in(&STRANGER).await;
    let refused = call(&c, "claudecode.list", json!({})).await.expect_err("a key holding no role");
    assert!(refused.contains("forbidden"), "typed refusal: {refused}");
    let refused = call(&c, "claudecode.create", json!({ "name": "s", "working_dir": ".", "model": "haiku" })).await.expect_err("no create either");
    assert!(refused.contains("forbidden"), "{refused}");
    let _ = server.port;
}

/// `proof` with `seed`'s public key put in place of its signer's: the
/// signature no longer matches the key it names.
fn forge(proof: &str, seed: &[u8; 32]) -> String {
    use base64::Engine;
    let b64 = &base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let body = proof.strip_prefix("plexus-key.").expect("a key proof");
    let mut json: Value = serde_json::from_slice(&b64.decode(body).unwrap()).unwrap();
    let hex: String = SigningKey::from_bytes(seed).verifying_key().as_bytes().iter().map(|b| format!("{b:02x}")).collect();
    json["key"] = Value::String(hex);
    format!("plexus-key.{}", b64.encode(serde_json::to_vec(&json).unwrap()))
}

#[tokio::test]
async fn the_builder_creates_a_session_and_chats_in_it() {
    let c = signed_in(&BUILDER).await;
    let created = create(&c, "builder-chat").await;
    assert_eq!(created["type"], "created", "{created}");
    let events = call(&c, "claudecode.chat", json!({ "name": "builder-chat", "prompt": "hello" })).await.expect("chat is an editor's");
    let kinds: Vec<&str> = events.iter().filter_map(|e| e["type"].as_str()).collect();
    assert!(kinds.contains(&"start"), "{events:?}");
    assert!(events.iter().any(|e| e["type"] == "content" && e["text"] == "stubbed answer"), "the stub's answer: {events:?}");
    assert!(kinds.contains(&"complete"), "{events:?}");
    // Its own list holds it; get answers it.
    let list = call(&c, "claudecode.list", json!({})).await.unwrap();
    assert!(one(&list)["sessions"].as_array().unwrap().iter().any(|s| s["name"] == "builder-chat"), "{list:?}");
    let got = call(&c, "claudecode.get", json!({ "name": "builder-chat" })).await.unwrap();
    assert_eq!(one(&got)["type"], "ok", "{got:?}");
    // The bridge's role holds no more than it needs: bash and arbor are admin's.
    assert!(call(&c, "bash.execute", json!({ "command": "true" })).await.expect_err("bash").contains("forbidden"));
    assert!(call(&c, "arbor.tree_list", json!({})).await.expect_err("arbor").contains("forbidden"));
    assert!(call(&c, "claudecode.sessions_list", json!({ "project_path": "x" })).await.expect_err("machine files").contains("forbidden"));
}

#[tokio::test]
async fn a_second_key_can_neither_see_nor_drive_the_session() {
    let builder = signed_in(&BUILDER).await;
    let created = create(&builder, "builder-private").await;
    assert_eq!(created["type"], "created", "{created}");
    let id = created["id"].as_str().unwrap().to_string();
    let started = call(&builder, "claudecode.chat_async", json!({ "name": "builder-private", "prompt": "hi" })).await.unwrap();
    let stream_id = one(&started)["stream_id"].as_str().expect("a stream").to_string();

    let other = signed_in(&OTHER).await;
    // Not seen: not listed, not found by name or by prefix.
    let list = call(&other, "claudecode.list", json!({})).await.expect("list is the other's role too");
    assert!(!one(&list)["sessions"].as_array().unwrap().iter().any(|s| s["name"] == "builder-private"), "{list:?}");
    for (method, params) in [
        ("claudecode.get", json!({ "name": "builder-private" })),
        ("claudecode.get", json!({ "name": "builder-pri" })),
        ("claudecode.get_tree", json!({ "name": "builder-private" })),
        ("claudecode.render_context", json!({ "name": "builder-private" })),
        ("claudecode.poll", json!({ "stream_id": stream_id })),
    ] {
        let answer = call(&other, method, params).await.unwrap_or_else(|e| panic!("{method}: {e}"));
        assert!(is_not_found(one(&answer)), "{method} answers not found: {answer:?}");
    }
    let streams = call(&other, "claudecode.streams", json!({})).await.unwrap();
    assert!(!one(&streams)["streams"].as_array().unwrap().iter().any(|s| s["stream_id"] == stream_id), "{streams:?}");
    // Not driven: chat, chat_async, fork and delete find nothing.
    let chat = call(&other, "claudecode.chat", json!({ "name": "builder-private", "prompt": "take over" })).await.unwrap();
    assert!(is_not_found(one(&chat)), "{chat:?}");
    for (method, params) in [
        ("claudecode.chat_async", json!({ "name": "builder-private", "prompt": "take over" })),
        ("claudecode.fork", json!({ "name": "builder-private", "new_name": "stolen" })),
        ("claudecode.delete", json!({ "name": "builder-private" })),
    ] {
        let answer = call(&other, method, params).await.unwrap_or_else(|e| panic!("{method}: {e}"));
        assert!(is_not_found(one(&answer)), "{method} answers not found: {answer:?}");
    }
    // Nor through the session's own path: that is admin's alone.
    for path in [format!("claudecode.{id}.get"), format!("claudecode.{id}.chat"), format!("claudecode.session.{id}.get")] {
        let refused = call(&other, &path, json!({ "prompt": "take over" })).await;
        assert!(refused.is_err() || refused.as_ref().is_ok_and(|v| v.iter().all(|e| e["type"] == "error")), "{path}: {refused:?}");
    }
    // The builder still has it, whole.
    let got = call(&builder, "claudecode.get", json!({ "name": "builder-private" })).await.unwrap();
    assert_eq!(one(&got)["type"], "ok", "{got:?}");
    // And the operator (admin) reaches every session.
    let operator = signed_in(&OPERATOR).await;
    let got = call(&operator, "claudecode.get", json!({ "name": "builder-private" })).await.unwrap();
    assert_eq!(one(&got)["type"], "ok", "admin reaches it: {got:?}");
}

#[tokio::test]
async fn sessions_with_one_name_prefix_stay_apart() {
    // Two owners, names sharing a prefix: each one's prefix lookup finds its
    // own and never names the other's in an ambiguity.
    let builder = signed_in(&BUILDER).await;
    let other = signed_in(&OTHER).await;
    assert_eq!(create(&builder, "prefix-builder").await["type"], "created");
    assert_eq!(create(&other, "prefix-other").await["type"], "created");
    let got = call(&other, "claudecode.get", json!({ "name": "prefix-" })).await.unwrap();
    assert_eq!(one(&got)["type"], "ok", "{got:?}");
    assert_eq!(one(&got)["config"]["name"], "prefix-other");
}
