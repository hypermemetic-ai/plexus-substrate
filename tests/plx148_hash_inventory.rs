//! PLX-148 — dump every activation and method hash in the served document.
//!
//! The mechanical form of "no hash moved": run it on the pristine tree, run it
//! after the change, diff. PLX-150 set this standard and this is the same move
//! over the Connectome rather than the legacy schema.
use plexus_core::ir::{ActivationIr, ChildEdge};

fn walk(ir: &ActivationIr, path: &str, out: &mut Vec<String>) {
    out.push(format!("NODE\t{path}\t{}", ir.hash));
    for m in &ir.methods {
        out.push(format!("METHOD\t{path}.{}\t{}", m.name, m.hash));
    }
    for c in &ir.children {
        let p = format!("{path}/{}", c.namespace());
        match c {
            ChildEdge::Static(sub) => walk(sub, &p, out),
            ChildEdge::Dynamic { hash, .. } => out.push(format!("EDGE\t{p}\t{hash}")),
            ChildEdge::Indexed { template, .. } => {
                out.push(format!("EDGE-INDEXED\t{p}"));
                walk(template, &p, out);
            }
        }
    }
}

#[tokio::test]
async fn inventory() {
    let hub = plexus_substrate::builder::build_plexus_rpc().await;
    let doc = hub.connectome();
    let mut out = Vec::new();
    walk(&doc, "root", &mut out);
    out.sort();
    println!("BEGIN-INVENTORY");
    println!("{}", out.join("\n"));
    println!("END-INVENTORY");
    println!("DOC-HASH\t{}", doc.ir_hash.as_deref().unwrap_or("-"));

    // Not only a dump: an empty hash anywhere is the defect PLX-90 fixed on the
    // IR path and PLX-150 on the legacy one — a cache key that can never hit,
    // and two of them on one descent read as a cycle.
    let empty: Vec<&String> = out
        .iter()
        .filter(|l| l.ends_with('\t') || l.split('\t').nth(2).is_some_and(str::is_empty))
        .collect();
    assert!(empty.is_empty(), "empty hash in the served document: {empty:?}");
}
