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
    let hub = declare::<Solar>(hub, Solar::__plexus_activation_ir_cached());

    declare_lazy_connectomes(hub)
}

/// PLX-148 — the five Dynamic edges, given the document each one advertises.
///
/// PLX-121 was the first client to walk the served document and found **0 of 5**
/// Dynamic edges fetchable; PLX-124 re-measured it on the wire as 9 lazy
/// attempts, 9 unfetchable, 0 cache hits. RFC 002 §5.1 requires a Dynamic edge
/// to be *sufficient to fetch and cache the child lazily*, so the letter was
/// satisfied — the edges carried namespace and hash — and the sufficiency
/// clause was not. Every one of the five answered *"no Connectome document is
/// declared."*
///
/// The five split into two causes, and both are supply:
///
/// **`health` and `registry` were simply never declared.** They are registered
/// activations that [`declare_connectomes`] does not list, so the hub had no
/// subtree for them and fell back to advertising their 16-hex legacy
/// `PluginSchema::hash` — the second residual PLX-142 recorded. Declaring them
/// *lazily* rather than adding them to the list above is deliberate: `health`
/// and `registry` are leaf services a navigating client rarely descends into,
/// and embedding them would grow every root fetch for every client to save a
/// round trip most of them never take. The edge stays `Dynamic`, and it now
/// advertises a real `CONNECTOME-HASH/1` digest that equals the hash of the
/// document `{"namespace": "health"}` returns.
///
/// **`claudecode/session`, `cone/of` and `solar/body` are nested**, one level
/// below a Static edge, and the wire's `namespace` parameter resolved hub-level
/// activations only — so they had no route at all, whatever they carried. Each
/// is a `#[child(list = ..)]` gate whose edge the macro emits from
/// `<ChildTy>::__plexus_activation_ir()`, keeping the hash and discarding the
/// document. The composition root is the only place the concrete type is still
/// in scope, exactly as this module's header explains for hub-level children,
/// so it is the place that hands the document over.
///
/// Nothing here is synthesized (§5.2). Every document is the child's own,
/// built by the same macro that would have embedded it.
///
/// # The residual, stated
///
/// This is hand-registration, one line per Dynamic edge, and a new `#[child]`
/// gate anywhere in the tree is unfetchable until someone adds a line. That is
/// the same structural debt [`declare_connectomes`] carries and for the same
/// reason — an inherent associated function is unreachable through
/// `Arc<dyn ActivationObject>` — and it is one level worse here, because a
/// nested child is not registered at all. `plx148_every_dynamic_edge_is_fetchable`
/// is the guard: it walks the composed document and fails on any Dynamic edge
/// this function forgot, so the debt cannot grow silently.
fn declare_lazy_connectomes(hub: DynamicHub) -> DynamicHub {
    use crate::activations::claudecode::SessionActivation;
    use crate::activations::cone::ConeActivation;
    use crate::activations::health::Health;
    use registry::Registry;

    // Hub-level: registered, not embedded, now fetchable by namespace.
    let hub = declare_at::<Health>(hub, "health", Health::__plexus_activation_ir());
    let hub = declare_at::<Registry>(hub, "registry", Registry::__plexus_activation_ir());

    // Nested: one level below a Static edge, fetchable by path.
    let hub = declare_at::<SessionActivation>(
        hub,
        "claudecode/session",
        SessionActivation::__plexus_activation_ir(),
    );
    let hub = declare_at::<ConeActivation>(
        hub,
        "cone/of",
        ConeActivation::__plexus_activation_ir(),
    );
    declare_at::<crate::activations::solar::Solar>(
        hub,
        "solar/body",
        crate::activations::solar::body_activation_ir(),
    )
}

/// PLX-148 — the same five documents, at the paths they appear at inside the
/// `tenants` mount's template.
///
/// The template is a whole second document embedded as an
/// [`ChildEdge::Indexed`](plexus_core::ir::ChildEdge::Indexed) template
/// (PLX-127), so its Dynamic edges are advertised at `tenants/health`,
/// `tenants/cone/of` and so on. They are the *same documents* — the tenant hub
/// is a different composition of the same activation objects — but a client
/// walks to them by a different path, and a path is what the wire resolves.
///
/// Called on the host hub only, after the mount is registered, because only
/// there does a `tenants` node exist to walk through.
///
/// `bash`, `orcha` and `chaos` are excluded from the tenant surface (PLX-130),
/// and `claudecode` is present only when the deployment sandboxes it. A path
/// declared for a namespace the template does not carry is inert — it names a
/// node no client can reach — so the set is declared unconditionally and the
/// absent ones simply never come up.
pub fn declare_tenant_template_connectomes(hub: DynamicHub) -> DynamicHub {
    use crate::activations::claudecode::SessionActivation;
    use crate::activations::cone::ConeActivation;
    use crate::activations::health::Health;
    use registry::Registry;

    let hub = declare_at::<Health>(hub, "tenants/health", Health::__plexus_activation_ir());
    let hub = declare_at::<Registry>(
        hub,
        "tenants/registry",
        Registry::__plexus_activation_ir(),
    );
    let hub = declare_at::<SessionActivation>(
        hub,
        "tenants/claudecode/session",
        SessionActivation::__plexus_activation_ir(),
    );
    let hub = declare_at::<ConeActivation>(
        hub,
        "tenants/cone/of",
        ConeActivation::__plexus_activation_ir(),
    );
    declare_at::<crate::activations::solar::Solar>(
        hub,
        "tenants/solar/body",
        crate::activations::solar::body_activation_ir(),
    )
}

/// Declare one lazily-served document, or log why it could not be built.
///
/// Same posture as [`declare`]: a failure here is a warning, never a panic. An
/// activation whose IR cannot be built keeps the edge it had before — a
/// `Dynamic` edge advertising its legacy hash, unfetchable — which is a visible
/// gap rather than a hidden one.
fn declare_at<T>(
    hub: DynamicHub,
    path: &str,
    ir: Result<ActivationIr, plexus_core::ir::SchemaRefError>,
) -> DynamicHub {
    match ir {
        Ok(ir) => hub.declare_ir_at(path, ir),
        Err(why) => {
            tracing::warn!(
                activation = std::any::type_name::<T>(),
                path,
                error = %why,
                "PLX-148: no Connectome document could be built for this Dynamic \
                 edge; it stays unfetchable, advertising its legacy hash"
            );
            hub
        }
    }
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
