//! PLX-106 — export a substrate activation's Connectome document.
//!
//! Substrate had no Connectome export path at all before this build (grep for
//! "connectome" across `src/` returned nothing). The vNext `#[activation]`
//! macro emits `activation_ir()`, and a Connectome document *is* a serialized
//! root `ActivationIr` — there is no separate exporter type.
//!
//! `Solar` is the migrated activation: its `#[plexus_macros::child(list =
//! "body_names")]` edge reads `CelestialBodyActivation::__plexus_activation_ir()`,
//! the hand-authored IR added by PLX-106. Exporting Solar therefore exercises
//! both halves of the migration.
//!
//! The written file is the artifact fed to connectome-hs's conformance checker
//! (`Plexus.Connectome.Check.checkConnectome`), which is the independent
//! judge — substrate does not self-attest.
//!
//! Override the destination with `SOLAR_CONNECTOME_OUT`.

use plexus_substrate::activations::solar::Solar;

#[test]
fn solar_connectome_exports_and_round_trips() {
    let ir = Solar::activation_ir();

    // Root facts RFC 002 §3.3 requires of a *document* (as opposed to an
    // embedded node) must be present.
    assert_eq!(ir.namespace, "solar");
    assert!(ir.ir_hash.is_some(), "a document carries an ir_hash");
    assert_eq!(
        ir.hash_algorithm.as_deref(),
        Some("CONNECTOME-HASH/1"),
        "the hash algorithm must be named in the document"
    );

    // The `body` child edge is the point of the migration: it exists only
    // because `CelestialBodyActivation` now exposes `__plexus_activation_ir`.
    let body = ir
        .child("body")
        .expect("solar declares a `body` child edge");
    assert!(
        !body.edge_hash().is_empty(),
        "a Dynamic edge MUST advertise the child's node hash (RFC 002 §5.1)"
    );

    let json = serde_json::to_string_pretty(ir).expect("the IR serializes");

    // §3.4 round-trip.
    let back: plexus_core::ir::ActivationIr =
        serde_json::from_str(&json).expect("the document round-trips");
    assert_eq!(&back, ir);

    let out = std::env::var("SOLAR_CONNECTOME_OUT").unwrap_or_else(|_| {
        std::env::temp_dir()
            .join("solar_connectome.json")
            .to_string_lossy()
            .into_owned()
    });
    std::fs::write(&out, &json).expect("the document is writable");
    println!("wrote {out}");
}
