//! PLX-148 — the served document is a complete map, and stays one.
//!
//! PLX-121 was the first client to walk the served Connectome and found **0 of
//! 5** Dynamic edges fetchable; PLX-124 re-measured it over the wire as *9 lazy
//! attempts, 9 unfetchable, 0 cache hits*. RFC 002 §5.1 requires a Dynamic edge
//! to be *sufficient to fetch and cache the child lazily*, so the letter held
//! and the sufficiency clause did not.
//!
//! These assert against the **composed substrate**, not a fixture, because the
//! gap was a property of substrate's composition — two activations nobody
//! declared, and three nested edges the wire had no route to.

use plexus_core::ir::{ActivationIr, ChildEdge};
use plexus_substrate::activations::solar::Solar;

/// Every Dynamic edge in the document, as `(path, advertised hash)`.
fn dynamic_edges(ir: &ActivationIr, path: &str, out: &mut Vec<(String, String)>) {
    for c in &ir.children {
        let p = if path.is_empty() {
            c.namespace().to_string()
        } else {
            format!("{path}/{}", c.namespace())
        };
        match c {
            ChildEdge::Static(sub) => dynamic_edges(sub, &p, out),
            ChildEdge::Indexed { template, .. } => dynamic_edges(template, &p, out),
            ChildEdge::Dynamic { hash, .. } => out.push((p, hash.clone())),
        }
    }
}

/// **The criterion, as a test.** Every Dynamic edge the document advertises can
/// be fetched from the document alone, and what comes back is the document the
/// edge named — same hash, no substitute.
///
/// It is also the guard on this build's residual: the supply side is
/// hand-registered, one line per edge in `activations::connectome`, so a new
/// `#[child]` gate anywhere in the tree is unfetchable until someone adds a
/// line. This test is what makes that debt loud instead of silent.
#[tokio::test]
async fn every_dynamic_edge_in_the_served_document_is_fetchable() {
    let hub = plexus_substrate::builder::build_plexus_rpc().await;
    let doc = hub.connectome();
    let mut edges = Vec::new();
    dynamic_edges(&doc, "", &mut edges);

    assert!(
        !edges.is_empty(),
        "the document must still carry Dynamic edges — if it carries none, this \
         test has stopped measuring anything and the completeness claim is vacuous"
    );

    let mut unfetchable = Vec::new();
    let mut disagreeing = Vec::new();
    for (path, advertised) in &edges {
        match hub.child_connectome(path) {
            None => unfetchable.push(path.clone()),
            Some(fetched) => {
                if &fetched.hash != advertised {
                    disagreeing.push(format!(
                        "{path}: advertised {advertised}, fetched {}",
                        fetched.hash
                    ));
                }
            }
        }
    }

    assert!(
        unfetchable.is_empty(),
        "{} of {} Dynamic edges are unfetchable: {unfetchable:?}",
        unfetchable.len(),
        edges.len()
    );
    // §5.1's hash is a cache key or it is decoration. PLX-150 measured what an
    // advertised hash that disagrees with the child's own costs: a cache that
    // can never hit, silently, forever.
    assert!(
        disagreeing.is_empty(),
        "advertised hash disagrees with the fetched document: {disagreeing:?}"
    );
}

/// PLX-142's second residual, retired: *"two Dynamic edges advertise a 16-hex
/// legacy hash rather than a CONNECTOME-HASH/1 digest — so never compare a
/// Dynamic edge's hash against a Connectome node hash."*
///
/// `health` and `registry` were the two. They were registered activations that
/// nothing declared, so the hub fell back to advertising their legacy
/// `PluginSchema::hash`. Comparing the two kinds of hash is now not merely safe
/// but the point, which the test above depends on.
#[tokio::test]
async fn no_dynamic_edge_advertises_a_legacy_hash() {
    let hub = plexus_substrate::builder::build_plexus_rpc().await;
    let mut edges = Vec::new();
    dynamic_edges(&hub.connectome(), "", &mut edges);

    let legacy: Vec<_> = edges
        .iter()
        .filter(|(_, h)| h.len() != 64 || !h.chars().all(|c| c.is_ascii_hexdigit()))
        .collect();
    assert!(
        legacy.is_empty(),
        "every advertised hash must be a CONNECTOME-HASH/1 digest: {legacy:?}"
    );
}

/// PLX-148 — a celestial body's document had **zero methods**, silently.
///
/// `activation_ir!` was handed `async fn info(&self) -> …;` — a signature with
/// no body — which `syn` parses as `ImplItem::Verbatim`, and
/// `ActivationIrSpec::from_impl` skips with `continue`. Nothing caught it
/// because nothing could: the document had no wire route until `solar/body`
/// became fetchable, and a fetchable document describing no methods is a worse
/// answer than an honest refusal.
#[tokio::test]
async fn a_celestial_body_declares_its_one_method() {
    let hub = plexus_substrate::builder::build_plexus_rpc().await;
    let body = hub
        .child_connectome("solar/body")
        .expect("solar/body is fetchable");
    assert_eq!(body.namespace, "celestial");
    assert_eq!(
        body.methods.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
        vec!["info"],
        "the planet document must describe the method a planet actually has"
    );
}

/// **The `solar` finding, pinned rather than described.**
///
/// The legacy wire advertises **eight** planets — `plugin_children()` is
/// hand-written for exactly that reason (PLX-150) — and the Connectome
/// advertises **one** child edge. That is not a bug in the fold, and this test
/// is the record of why:
///
/// - `solar.body` is `#[child(list = "body_names")]`, a *family* of instances
///   enumerated at runtime. The instance set is a runtime fact, and §4.8 makes
///   the child set part of the hash preimage — so putting eight planet names in
///   the document would make `ir_hash` a function of runtime state. PLX-127
///   established the same position from the security side: the `tenants` mount
///   renders as one template and *no* instance ids, and its criterion is
///   absence.
/// - What §5.1 *does* offer for this shape is the **Indexed** edge, which
///   carries the enumeration method's **name** (not its result), an `id_field`,
///   a `path_template` and one template subtree. `solar.body` declares all four
///   and they reach no wire, because `ir_parse/signature.rs` matches
///   `ChildMethodKind::Dynamic` **before** it looks at `list_fn` and drops the
///   enumeration facts on the floor. `claudecode.session` (`session_ids`) and
///   `cone.of` (`cone_ids`) have the same defect.
///
/// So the eight cannot be *listed*, and today they cannot be *enumerated from
/// the document* either. The first is an RFC finding; the second is a fixable
/// defect in `plexus-macros`, which this build does not own. This test asserts
/// both halves and **fails when the second is fixed**, which is how PLX-124
/// wanted a gap recorded.
#[tokio::test]
async fn solar_advertises_one_edge_where_the_legacy_wire_advertises_eight() {
    let solar = Solar::new();
    #[allow(deprecated)]
    let legacy = solar.plugin_children();
    assert_eq!(legacy.len(), 8, "the legacy wire advertises eight planets");

    let hub = plexus_substrate::builder::build_plexus_rpc().await;
    let doc = hub.connectome();
    let ChildEdge::Static(solar_ir) = doc.child("solar").expect("solar is a child") else {
        panic!("solar's subtree is embedded");
    };
    assert_eq!(
        solar_ir.children.len(),
        1,
        "solar declares exactly one child edge in the Connectome"
    );

    // NEGATIVE PIN — invert this when `#[child(list = ..)]` emits Indexed.
    assert!(
        matches!(solar_ir.children[0], ChildEdge::Dynamic { .. }),
        "solar/body is emitted as Dynamic; §5.1's Indexed is the shape it should \
         reach for, and the enumeration facts it already declares are dropped by \
         plexus-macros before they reach the wire"
    );

    // No planet name appears anywhere in the served bytes — the document is not
    // a function of the instance set, and this is what makes `ir_hash` stable.
    let bytes = serde_json::to_string(&doc).expect("the document serializes");
    for planet in legacy.iter().map(|c| c.namespace.as_str()) {
        assert!(
            !bytes.contains(&format!("\"{planet}\"")),
            "no instance id belongs in the document, and `{planet}` is one"
        );
    }

    // The eight are reachable rather than listed: the shape every planet has is
    // one fetch away, and it is a real document now.
    assert!(hub.child_connectome("solar/body").is_some());
}
