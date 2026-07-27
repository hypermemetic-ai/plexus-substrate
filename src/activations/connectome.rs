//! PLX-142 — declare each activation's Connectome subtree to the hub.
//!
//! # Why this file exists
//!
//! The `#[activation]` macro has built every activation's `ActivationIr` since
//! PLX-91, but it emits it as an **inherent** associated function
//! (`T::activation_ir()`), not as a trait method. `DynamicHub` stores its
//! children as `Arc<dyn ActivationObject>`, so the erasure cannot reach an
//! inherent function: the document existed in every one of these crates and was
//! unreachable from the one place that serves a wire.
//!
//! The composition root is the only place that still knows the concrete type,
//! so it is the only place that can hand the IR over without inventing a second
//! way to declare one. PLX-113 owns `#[activation]`'s root-fact attributes;
//! this build deliberately adds no macro surface, and this module is the price
//! of that.
//!
//! # Why a failure here is a warning and not a panic
//!
//! `activation_ir()` panics when an IR cannot be built — a parameter, update or
//! terminal type whose schema carries no type information (PLX-75). That is the
//! right posture for a test or a tool and the wrong one for a server's boot
//! path: one unresolvable parameter type in one activation would take the whole
//! substrate down. So this uses the reporting accessor, logs what failed, and
//! leaves that activation as a `Dynamic` edge advertising its legacy hash —
//! which is exactly what the hub does for any child whose subtree it does not
//! have. Nothing is manufactured to fill the gap (RFC 002 §5.2).

use plexus_core::ir::ActivationIr;

use crate::plexus::DynamicHub;

/// Declare the Connectome subtree of every activation that can produce one.
///
/// Call this **after** every `register`, on the fully-built hub: the hub keys
/// declarations by namespace and only emits an edge for a namespace that is
/// actually registered, so a declaration for an unregistered activation is
/// inert rather than misleading.
pub fn declare_connectomes(hub: DynamicHub) -> DynamicHub {
    use crate::activations::arbor::Arbor;
    use crate::activations::bash::Bash;
    use crate::activations::changelog::Changelog;
    use crate::activations::claudecode::ClaudeCode;
    use crate::activations::claudecode_loopback::ClaudeCodeLoopback;
    use crate::activations::cone::Cone;
    use crate::activations::echo::Echo;
    use crate::activations::interactive::Interactive;
    use crate::activations::lattice::Lattice;
    use crate::activations::mustache::Mustache;
    use crate::activations::orcha::Orcha;
    use crate::activations::solar::Solar;

    let hub = declare::<Arbor>(hub, Arbor::__plexus_activation_ir_cached());
    let hub = declare::<Bash>(hub, Bash::__plexus_activation_ir_cached());
    let hub = declare::<Changelog>(hub, Changelog::__plexus_activation_ir_cached());
    let hub = declare::<ClaudeCode>(hub, ClaudeCode::__plexus_activation_ir_cached());
    let hub = declare::<ClaudeCodeLoopback>(
        hub,
        ClaudeCodeLoopback::__plexus_activation_ir_cached(),
    );
    let hub = declare::<Cone>(hub, Cone::__plexus_activation_ir_cached());
    let hub = declare::<Echo>(hub, Echo::__plexus_activation_ir_cached());
    let hub = declare::<Interactive>(hub, Interactive::__plexus_activation_ir_cached());
    let hub = declare::<Lattice>(hub, Lattice::__plexus_activation_ir_cached());
    let hub = declare::<Mustache>(hub, Mustache::__plexus_activation_ir_cached());
    let hub = declare::<Orcha>(hub, Orcha::__plexus_activation_ir_cached());
    declare::<Solar>(hub, Solar::__plexus_activation_ir_cached())
}

fn declare<T>(hub: DynamicHub, ir: Result<&'static ActivationIr, String>) -> DynamicHub {
    match ir {
        Ok(ir) => hub.declare_ir(ir.clone()),
        Err(why) => {
            tracing::warn!(
                activation = std::any::type_name::<T>(),
                error = %why,
                "PLX-142: no Connectome subtree could be built for this activation; \
                 it will appear on the hub's .connectome as a Dynamic edge advertising its \
                 legacy hash rather than as an embedded subtree"
            );
            hub
        }
    }
}
