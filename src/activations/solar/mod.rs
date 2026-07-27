//! Solar system activation module
//!
//! Demonstrates nested plugin hierarchy via the coalgebraic structure.

mod activation;
mod celestial;
mod types;

pub use activation::Solar;
pub use types::{BodyType, SolarEvent};

/// PLX-148 — the Connectome document behind solar's `body` Dynamic edge.
///
/// `solar/body` is one of the three nested Dynamic edges PLX-121 measured as
/// unfetchable. The document exists — `#[child(list = "body_names")]` builds it
/// in order to take its hash, then discards everything else — but
/// `CelestialBodyActivation` is `pub(super)`, so the composition root cannot
/// name the type in order to declare it.
///
/// This hands the document over without widening the type's visibility: the
/// child is still solar's business, and only its IR leaves the module.
pub(crate) fn body_activation_ir(
) -> Result<plexus_core::ir::ActivationIr, plexus_core::ir::SchemaRefError> {
    celestial::CelestialBodyActivation::__plexus_activation_ir()
}
