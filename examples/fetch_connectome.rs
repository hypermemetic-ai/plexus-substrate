//! PLX-142 — fetch a Connectome document from a **running** substrate, over the
//! wire, and write it out for `connectome-hs`'s conformance checker.
//!
//! This is a wire client, not a test fixture, and the distinction is the whole
//! point of the ticket. PLX-106's `tests/connectome_export.rs` builds a
//! document *in process* from `Solar::activation_ir()`; a checker run over that
//! artifact proves the builder is conformant and says nothing about whether any
//! client could ever obtain it. Until this build, no client could: `ActivationIr`
//! appeared on no wire method, and `{ns}.schema`'s `ChildSummary` is three
//! fields and shallow by design.
//!
//! # Usage
//!
//! ```text
//! # terminal 1
//! cargo run --bin plexus-substrate -- --port 4499 --no-mcp
//!
//! # terminal 2
//! cargo run --example fetch_connectome -- ws://127.0.0.1:4499 /tmp/substrate_connectome.json
//! # or, for one child's document:
//! cargo run --example fetch_connectome -- ws://127.0.0.1:4499 /tmp/solar.json solar
//! ```
//!
//! Exits non-zero and prints why if the document does not arrive or does not
//! decode as an `ActivationIr`.

use jsonrpsee::core::client::{ClientT, SubscriptionClientT};
use jsonrpsee::core::params::ObjectParams;
use jsonrpsee::ws_client::WsClientBuilder;
use plexus_core::ir::ActivationIr;
use plexus_core::plexus::types::PlexusStreamItem;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let url = args.next().unwrap_or_else(|| "ws://127.0.0.1:4444".to_string());
    let out = args
        .next()
        .unwrap_or_else(|| "/tmp/substrate_connectome.json".to_string());
    let child = args.next();

    let client = WsClientBuilder::default().build(&url).await?;

    // Prove the connection is live and the server is the one we think it is
    // before asking for the document, so a transport failure cannot be mistaken
    // for an absent method.
    let _: serde_json::Value = client
        .request("_info", jsonrpsee::rpc_params![])
        .await
        .map_err(|e| format!("_info failed against {url}: {e}"))?;

    let mut params = ObjectParams::new();
    if let Some(ns) = &child {
        params.insert("namespace", ns)?;
    }

    let mut sub = client
        .subscribe::<PlexusStreamItem, _>(
            "substrate.connectome",
            params,
            "substrate.connectome_unsub",
        )
        .await
        .map_err(|e| {
            format!("substrate.connectome is not served by {url}: {e}")
        })?;

    let mut document: Option<serde_json::Value> = None;
    while let Some(item) = sub.next().await {
        match item? {
            PlexusStreamItem::Data {
                content,
                content_type,
                ..
            } => {
                println!("received frame content_type={content_type}");
                document = Some(content);
            }
            PlexusStreamItem::Error { message, .. } => {
                return Err(format!("server error: {message}").into());
            }
            PlexusStreamItem::Done { .. } => break,
            _ => {}
        }
    }

    let document = document.ok_or("the stream carried no document")?;

    // Decode with the real type, so "it arrived" and "it is a Connectome" are
    // not the same claim.
    let ir: ActivationIr = serde_json::from_value(document.clone())
        .map_err(|e| format!("the payload is not an ActivationIr: {e}"))?;

    println!(
        "namespace={} ir_version={:?} hash_algorithm={:?} ir_hash={:?}",
        ir.namespace, ir.ir_version, ir.hash_algorithm, ir.ir_hash
    );
    println!("methods={} children={}", ir.methods.len(), ir.children.len());
    for c in &ir.children {
        let kind = match c {
            plexus_core::ir::ChildEdge::Static(_) => "static",
            plexus_core::ir::ChildEdge::Dynamic { .. } => "dynamic",
            plexus_core::ir::ChildEdge::Indexed { .. } => "indexed",
        };
        println!("  edge {kind:<8} {}", c.namespace());
    }

    let text = serde_json::to_string_pretty(&document)?;
    std::fs::write(&out, format!("{text}\n"))?;
    println!("wrote {out}");
    Ok(())
}
