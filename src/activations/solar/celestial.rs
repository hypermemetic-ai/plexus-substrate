//! Celestial body definitions
//!
//! Each celestial body is a plugin that can have children (moons).
//! This demonstrates the coalgebraic structure: bodies are observed
//! to reveal their properties and children.

use crate::plexus::{
    Activation, ChildRouter, ChildSummary, MethodSchema, MethodEnumSchema, PlexusError, PlexusStream,
    PluginSchema, wrap_stream,
};
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use jsonrpsee::core::server::Methods;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::types::{BodyType, SolarEvent};

/// A celestial body in the solar system
#[derive(Debug, Clone)]
pub(super) struct CelestialBody {
    pub name: String,
    pub body_type: BodyType,
    pub mass_kg: f64,
    pub radius_km: f64,
    pub orbital_period_days: Option<f64>,
    pub parent: Option<String>,
    pub children: Vec<CelestialBody>,
}

impl CelestialBody {
    /// Create a new star (root of a system)
    pub(super) fn star(name: &str, mass_kg: f64, radius_km: f64) -> Self {
        Self {
            name: name.to_string(),
            body_type: BodyType::Star,
            mass_kg,
            radius_km,
            orbital_period_days: None,
            parent: None,
            children: Vec::new(),
        }
    }

    /// Create a new planet
    pub(super) fn planet(name: &str, mass_kg: f64, radius_km: f64, orbital_period_days: f64) -> Self {
        Self {
            name: name.to_string(),
            body_type: BodyType::Planet,
            mass_kg,
            radius_km,
            orbital_period_days: Some(orbital_period_days),
            parent: None,
            children: Vec::new(),
        }
    }

    /// Create a new moon
    pub(super) fn moon(name: &str, mass_kg: f64, radius_km: f64, orbital_period_days: f64) -> Self {
        Self {
            name: name.to_string(),
            body_type: BodyType::Moon,
            mass_kg,
            radius_km,
            orbital_period_days: Some(orbital_period_days),
            parent: None,
            children: Vec::new(),
        }
    }

    /// Add a child body (moon to planet, planet to star)
    pub(super) fn with_child(mut self, mut child: CelestialBody) -> Self {
        child.parent = Some(self.name.clone());
        self.children.push(child);
        self
    }

    /// Add multiple children
    pub(super) fn with_children(mut self, children: Vec<CelestialBody>) -> Self {
        for mut child in children {
            child.parent = Some(self.name.clone());
            self.children.push(child);
        }
        self
    }

    /// Check if this body has children
    pub(super) const fn has_children(&self) -> bool {
        !self.children.is_empty()
    }

    /// Count all descendants recursively
    pub(super) fn descendant_count(&self) -> usize {
        self.children.iter()
            .map(|c| 1 + c.descendant_count())
            .sum()
    }

    /// Get description for this body
    fn description(&self) -> String {
        match self.body_type {
            BodyType::Star => format!("{} - the central star", self.name),
            BodyType::Planet => format!("{} - planet", self.name),
            BodyType::DwarfPlanet => format!("{} - dwarf planet", self.name),
            BodyType::Moon => format!("{} - moon of {}", self.name,
                self.parent.as_deref().unwrap_or("unknown")),
        }
    }

    /// Convert to a child summary (for parent's schema)
    pub(super) fn to_child_summary(&self) -> ChildSummary {
        let schema = self.to_plugin_schema();
        ChildSummary {
            namespace: schema.namespace,
            description: schema.description,
            hash: schema.hash,
        }
    }

    /// Generate the `PluginSchema` for this celestial body
    pub(super) fn to_plugin_schema(&self) -> PluginSchema {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Create method for observing this body
        let info_desc = format!("Get information about {}", self.name);
        let mut hasher = DefaultHasher::new();
        "info".hash(&mut hasher);
        info_desc.hash(&mut hasher);
        let info_hash = format!("{:016x}", hasher.finish());

        let methods = vec![
            MethodSchema::new("info", &info_desc, info_hash)
                .with_returns(schemars::schema_for!(SolarEvent)),
        ];

        let namespace = self.name.to_lowercase().replace(' ', "_");
        let version = "1.0.0";
        let description = self.description();

        if self.has_children() {
            // Hub: children as summaries
            let child_summaries: Vec<ChildSummary> = self.children
                .iter()
                .map(CelestialBody::to_child_summary)
                .collect();

            PluginSchema::hub(namespace, version, description, methods, child_summaries)
        } else {
            // Leaf: no children
            PluginSchema::leaf(namespace, version, description, methods)
        }
    }
}

// ============================================================================
// CelestialBodyActivation - makes CelestialBody callable
// ============================================================================

/// Method enum for celestial body - just "info"
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "method", rename_all = "snake_case")]
pub(super) enum CelestialBodyMethod {
    Info,
}

impl MethodEnumSchema for CelestialBodyMethod {
    fn method_names() -> &'static [&'static str] {
        &["info"]
    }

    fn schema_with_consts() -> Value {
        serde_json::to_value(schemars::schema_for!(CelestialBodyMethod))
            .expect("Schema should serialize")
    }
}


/// Activation wrapper for `CelestialBody`
///
/// This makes a celestial body callable as a plugin.
/// It implements both Activation (for method dispatch) and
/// `ChildRouter` (for nested routing to moons).
#[derive(Clone)]
pub(super) struct CelestialBodyActivation {
    body: CelestialBody,
    namespace: String,
}

impl CelestialBodyActivation {
    pub(super) fn new(body: CelestialBody) -> Self {
        let namespace = body.name.to_lowercase().replace(' ', "_");
        Self { body, namespace }
    }

    /// The vNext IR contract for this activation — hand-authored, deliberately.
    ///
    /// PLX-106. `Solar` declares `#[plexus_macros::child(list = "body_names")]`,
    /// and that attribute emits `<CelestialBodyActivation>::__plexus_activation_ir()`
    /// into `Solar`'s own IR constructor (`plexus-macros/src/ir_parse/emit.rs`,
    /// `child_edge_expr`, the `ChildEdgeSpec::Dynamic` arm). Every activation
    /// that participates in a `#[child]` edge must therefore expose this
    /// associated function. `#[plexus_macros::activation]` normally emits it
    /// (`plexus-macros/src/codegen/mod.rs`, `activation_ir_fn`).
    ///
    /// # Why this one is hand-authored rather than macro-generated
    ///
    /// A celestial body's children are celestial bodies: `ChildRouter::get_child`
    /// below returns another `CelestialBodyActivation`. Expressing that with
    /// `#[plexus_macros::child]` would emit
    /// `<CelestialBodyActivation>::__plexus_activation_ir()?` *inside*
    /// `CelestialBodyActivation::__plexus_activation_ir()` — unbounded
    /// self-recursion. A finite IR document cannot embed a self-recursive
    /// subtree, and the macro has no cycle break.
    ///
    /// The macro's documented escape hatch for "keep my hand-written
    /// `ChildRouter`" is `#[plexus_macros::activation(hub, ...)]`, which skips
    /// `ChildRouter` generation (`plexus-macros/src/codegen/activation.rs`, the
    /// `hub_explicit && child_methods.is_empty()` gate). That flag is
    /// `#[deprecated]` as of IR-5 and substrate sets `warnings = "deny"`
    /// (`Cargo.toml`, `[lints.rust]`), so taking it would reintroduce the
    /// module-level `#![allow(deprecated)]` that `solar/activation.rs`
    /// documents as removed. Hand-authoring the IR is the supported
    /// alternative (`plexus-macros/src/ir_parse/emit.rs` header, and the
    /// `Session` fixture in `plexus-macros/tests/plx79_ir_parse_tests.rs`).
    ///
    /// The bodies below are never executed — `activation_ir!` reads signatures
    /// only and discards bodies. Dispatch stays on the hand-written
    /// `impl Activation` beneath.
    #[doc(hidden)]
    #[allow(non_snake_case)]
    pub(crate) fn __plexus_activation_ir(
    ) -> Result<plexus_core::ir::ActivationIr, plexus_core::ir::SchemaRefError> {
        plexus_macros::activation_ir!(
            namespace = "celestial",
            version = "1.0.0",
            description = "A celestial body observed as a plugin",
            impl CelestialBodyActivation {
                /// Get information about this celestial body.
                #[method]
                async fn info(&self) -> impl Stream<Item = SolarEvent> + Send + 'static;
            }
        )
    }

    fn info_stream(&self) -> impl Stream<Item = SolarEvent> + Send + 'static {
        let body = self.body.clone();
        stream! {
            yield SolarEvent::Body {
                name: body.name,
                body_type: body.body_type,
                mass_kg: body.mass_kg,
                radius_km: body.radius_km,
                orbital_period_days: body.orbital_period_days,
                parent: body.parent,
            };
        }
    }
}

#[async_trait]
impl Activation for CelestialBodyActivation {
    type Methods = CelestialBodyMethod;

    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn version(&self) -> &'static str {
        "1.0.0"
    }

    fn description(&self) -> &'static str {
        // Can't return dynamic string without lifetime issues
        // The schema has the full description
        "Celestial body"
    }

    fn methods(&self) -> Vec<&str> {
        vec!["info", "schema"]
    }

    fn method_help(&self, method: &str) -> Option<String> {
        match method {
            "info" => Some(format!("Get information about {}", self.body.name)),
            "schema" => Some("Get plugin or method schema. Pass {\"method\": \"name\"} for a specific method.".to_string()),
            _ => None,
        }
    }

    async fn call(&self, method: &str, params: Value, _auth: Option<&plexus_core::plexus::AuthContext>, _raw_ctx: Option<&plexus_core::request::RawRequestContext>) -> Result<PlexusStream, PlexusError> { match method {
        "info" => {
            let stream = self.info_stream();
            Ok(wrap_stream(stream, "celestial.info", vec![self.namespace.clone()]))
        }
        "schema" => {
            use crate::plexus::SchemaResult;
    
            // PROT schema unification (PLX-13): `.schema` always yields the single
            // unified PluginSchema; no `method`-param per-method branch.
            let ns = self.namespace.clone();
            let result = SchemaResult::Plugin(self.plugin_schema());

            Ok(wrap_stream(
                futures::stream::once(async move { result }),
                "celestial.schema",
                vec![ns]
            ))
        }
        _ => {
            // PROT schema unification (PLX-13): the `{method}.schema` strip-suffix
            // shortcut (and its `.method_schema` content-type) is deleted; an
            // unknown method falls straight through to child routing.
            // Try routing to child
            crate::plexus::route_to_child(self, method, params, None, None).await
        }
    } }

    fn into_rpc_methods(self) -> Methods {
        // Celestial bodies don't register their own RPC methods
        // They're called through the parent (Solar) routing
        Methods::new()
    }

    fn plugin_schema(&self) -> PluginSchema {
        self.body.to_plugin_schema()
    }
}

#[async_trait]
impl ChildRouter for CelestialBodyActivation {
    fn router_namespace(&self) -> &str {
        &self.namespace
    }

    async fn router_call(&self, method: &str, params: Value, auth: Option<&plexus_core::plexus::AuthContext>, raw_ctx: Option<&plexus_core::request::RawRequestContext>) -> Result<PlexusStream, PlexusError> {
            // Delegate to Activation::call which handles local methods + nested routing
            Activation::call(self, method, params, auth, raw_ctx).await
        }

    async fn get_child(&self, name: &str) -> Option<Box<dyn ChildRouter>> {
        let normalized = name.to_lowercase();
        self.body.children.iter()
            .find(|c| c.name.to_lowercase() == normalized)
            .map(|c| Box::new(CelestialBodyActivation::new(c.clone())) as Box<dyn ChildRouter>)
    }
}

// ============================================================================
// Solar System Data
// ============================================================================

/// Build the real solar system with accurate data
pub(super) fn build_solar_system() -> CelestialBody {
    CelestialBody::star("Sol", 1.989e30, 696_340.0)
        .with_children(vec![
            // Mercury - no moons
            CelestialBody::planet("Mercury", 3.285e23, 2_439.7, 87.97),

            // Venus - no moons
            CelestialBody::planet("Venus", 4.867e24, 6_051.8, 224.7),

            // Earth with Moon
            CelestialBody::planet("Earth", 5.972e24, 6_371.0, 365.25)
                .with_child(
                    CelestialBody::moon("Luna", 7.342e22, 1_737.4, 27.32)
                ),

            // Mars with moons
            CelestialBody::planet("Mars", 6.39e23, 3_389.5, 687.0)
                .with_children(vec![
                    CelestialBody::moon("Phobos", 1.0659e16, 11.267, 0.319),
                    CelestialBody::moon("Deimos", 1.4762e15, 6.2, 1.263),
                ]),

            // Jupiter with major moons (Galilean)
            CelestialBody::planet("Jupiter", 1.898e27, 69_911.0, 4_333.0)
                .with_children(vec![
                    CelestialBody::moon("Io", 8.932e22, 1_821.6, 1.769),
                    CelestialBody::moon("Europa", 4.8e22, 1_560.8, 3.551),
                    CelestialBody::moon("Ganymede", 1.4819e23, 2_634.1, 7.155),
                    CelestialBody::moon("Callisto", 1.0759e23, 2_410.3, 16.689),
                ]),

            // Saturn with major moons
            CelestialBody::planet("Saturn", 5.683e26, 58_232.0, 10_759.0)
                .with_children(vec![
                    CelestialBody::moon("Titan", 1.3452e23, 2_574.7, 15.945),
                    CelestialBody::moon("Enceladus", 1.08e20, 252.1, 1.370),
                    CelestialBody::moon("Mimas", 3.749e19, 198.2, 0.942),
                ]),

            // Uranus with major moons
            CelestialBody::planet("Uranus", 8.681e25, 25_362.0, 30_687.0)
                .with_children(vec![
                    CelestialBody::moon("Titania", 3.527e21, 788.9, 8.706),
                    CelestialBody::moon("Oberon", 3.014e21, 761.4, 13.463),
                    CelestialBody::moon("Miranda", 6.59e19, 235.8, 1.413),
                ]),

            // Neptune with Triton
            CelestialBody::planet("Neptune", 1.024e26, 24_622.0, 60_190.0)
                .with_child(
                    CelestialBody::moon("Triton", 2.14e22, 1_353.4, 5.877)
                ),
        ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_solar_system() {
        let sol = build_solar_system();
        assert_eq!(sol.name, "Sol");
        assert_eq!(sol.body_type, BodyType::Star);
        assert_eq!(sol.children.len(), 8); // 8 planets
    }

    #[test]
    fn test_earth_has_moon() {
        let sol = build_solar_system();
        let earth = sol.children.iter().find(|p| p.name == "Earth").unwrap();
        assert_eq!(earth.children.len(), 1);
        assert_eq!(earth.children[0].name, "Luna");
    }

    #[test]
    fn test_descendant_count() {
        let sol = build_solar_system();
        let total = sol.descendant_count();
        // 8 planets + 1 + 2 + 4 + 3 + 3 + 1 = 8 + 14 = 22 bodies (excluding Sol)
        assert_eq!(total, 22);
    }

    #[test]
    fn test_plugin_schema_hierarchy() {
        let sol = build_solar_system();
        let schema = sol.to_plugin_schema();

        // `CelestialBody::to_plugin_schema()` is a hand-written helper that
        // composes a `PluginSchema` for the solar hierarchy without going
        // through `#[plexus_macros::child]`. It therefore does not emit
        // `MethodRole::StaticChild` / `DynamicChild` tags and
        // `PluginSchema::is_hub_by_role()` correctly reports `false` here.
        // The semantic coverage this test owns — "Sol has 8 planet children,
        // each with a stable per-planet hash" — is asserted directly against
        // the `CelestialBody` tree (the source of truth) plus the namespace
        // / version surface of the produced schema. Neither assertion reads
        // the deprecated `is_hub()` / `children` surface.
        assert_eq!(schema.namespace, "sol");
        assert_eq!(sol.children.len(), 8); // 8 planets

        // Produce per-planet `ChildSummary` objects the same way the
        // production path does — via `CelestialBody::to_child_summary` — and
        // verify individual planets are present with non-empty hashes.
        let child_summaries: Vec<_> = sol
            .children
            .iter()
            .map(CelestialBody::to_child_summary)
            .collect();
        assert_eq!(child_summaries.len(), 8);

        // Mercury is in the list
        let mercury = child_summaries
            .iter()
            .find(|c| c.namespace == "mercury")
            .unwrap();
        assert!(mercury.description.contains("planet"));

        // Earth is in the list, with a non-empty deterministic hash
        let earth = child_summaries
            .iter()
            .find(|c| c.namespace == "earth")
            .unwrap();
        assert!(earth.description.contains("planet"));
        assert!(!earth.hash.is_empty());
    }
}
