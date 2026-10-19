//! Solar system activation - demonstrates nested plugin hierarchy
//!
//! This activation shows the coalgebraic plugin structure where plugins
//! can have children. The solar system is a natural hierarchy:
//! - Sol (star) contains planets
//! - Planets contain moons
//!
//! Each level implements the F-coalgebra structure map via `plugin_schema()`.
//!
//! # plexus-macros 0.5.3+: no module-level `#![allow(deprecated)]` needed
//!
//! Solar's hand-written `plugin_children()` is `#[deprecated]` (IR-8) so
//! downstream Rust callers get migration nudges. The
//! `#[plexus_macros::activation]` macro generates
//! `impl Activation for Solar` whose body calls `self.plugin_children()`;
//! as of plexus-macros 0.5.3 (IR-16) that call is wrapped in an
//! `#[allow(deprecated)]` block *inside the macro's output*, so the
//! substrate build does not need — and must not have — a module-level
//! suppression.
// PLEXUS-6: the 0.6 macros' legacy form (MACRO-13) — these activations keep
// `namespace = …` and their own type names; the name rule's trait would need
// one event type per activation. Its deprecation note is allowed here.
#![allow(deprecated)]

use super::celestial::{build_solar_system, CelestialBody, CelestialBodyActivation};
use super::types::{BodyType, SolarEvent};
use crate::plexus::ChildSummary;
use async_stream::stream;
use futures::Stream;

/// Solar system activation - demonstrates nested plugin children
#[derive(Clone)]
pub struct Solar {
    system: CelestialBody,
}

impl Solar {
    pub fn new() -> Self {
        Self {
            system: build_solar_system(),
        }
    }

    /// Find a body by path (e.g., "earth" or "jupiter.io")
    fn find_body(&self, path: &str) -> Option<&CelestialBody> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = &self.system;

        for part in parts {
            let normalized = part.to_lowercase();
            if current.name.to_lowercase() == normalized {
                continue;
            }
            current = current.children.iter()
                .find(|c| c.name.to_lowercase() == normalized)?;
        }
        Some(current)
    }

    /// Count all moons in the system
    fn moon_count(&self) -> usize {
        fn count_moons(body: &CelestialBody) -> usize {
            let mine: usize = body.children.iter()
                .filter(|c| c.body_type == BodyType::Moon)
                .count();
            let nested: usize = body.children.iter()
                .map(count_moons)
                .sum();
            mine + nested
        }
        count_moons(&self.system)
    }
}

impl Default for Solar {
    fn default() -> Self {
        Self::new()
    }
}

/// Solar system model - demonstrates nested plugin hierarchy
#[plexus_macros::activation(namespace = "solar", version = "1.0.0")]
impl Solar {
    /// Get an overview of the solar system
    #[plexus_macros::method]
    async fn observe(&self) -> impl Stream<Item = SolarEvent> + Send + 'static {
        let star = self.system.name.clone();
        let planet_count = self.system.children.len();
        let moon_count = self.moon_count();
        let total_bodies = 1 + self.system.descendant_count();

        stream! {
            yield SolarEvent::System {
                star,
                planet_count,
                moon_count,
                total_bodies,
            };
        }
    }

    /// Get detailed information about a celestial body at `path`
    /// (e.g., `"earth"`, `"jupiter.io"`, `"saturn.titan"`).
    #[plexus_macros::method]
    async fn info(
        &self,
        path: String,
    ) -> impl Stream<Item = SolarEvent> + Send + 'static {
        let body = self.find_body(&path).cloned();

        stream! {
            if let Some(b) = body {
                yield SolarEvent::Body {
                    name: b.name,
                    body_type: b.body_type,
                    mass_kg: b.mass_kg,
                    radius_km: b.radius_km,
                    orbital_period_days: b.orbital_period_days,
                    parent: b.parent,
                };
            }
        }
    }

    /// Look up a celestial body by name (case-insensitive).
    #[plexus_macros::child(list = "body_names")]
    async fn body(&self, name: &str) -> Option<CelestialBodyActivation> {
        let normalized = name.to_lowercase();
        self.system
            .children
            .iter()
            .find(|c| c.name.to_lowercase() == normalized)
            .map(|c| CelestialBodyActivation::new(c.clone()))
    }

    /// Stream the configured planet names for `ChildRouter::list_children`.
    async fn body_names(&self) -> impl Stream<Item = String> + Send + '_ {
        let names: Vec<String> = self
            .system
            .children
            .iter()
            .map(|c| c.name.clone())
            .collect();
        futures::stream::iter(names)
    }

    /// Hand-written child summary list.
    ///
    /// Preserved (not macro-synthesized) so each planet's `hash` field carries
    /// the deterministic digest computed from that planet's own sub-schema.
    /// The macro's synthesis path only covers static `#[child]` methods and
    /// would emit empty-string hashes; Solar's children are dynamic.
    ///
    /// # Deprecated (IR-8)
    ///
    /// This override remains load-bearing until hashes move to the runtime
    /// (HASH-1). It is annotated as `#[deprecated]` so the compile-time
    /// warning nudges callers migrating to the role-tagged schema. The body
    /// is unchanged from its pre-IR-8 definition.
    #[deprecated(
        since = "0.5.0",
        note = "Solar's children are derivable from #[child]-tagged methods. This override is retained for backward compatibility until plugin_children is removed from the schema."
    )]
    #[plexus_macros::removed_in("0.6")]
    pub fn plugin_children(&self) -> Vec<ChildSummary> {
        self.system.children.iter()
            .map(super::celestial::CelestialBody::to_child_summary)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plexus::{Activation, DynamicHub, MethodRole};

    /// Predicate: is this method a child-gate (i.e., non-Rpc role)?
    ///
    /// `MethodRole` is `#[non_exhaustive]` (IR-2). Explicit match arms cover
    /// the known child-gate variants; the catch-all treats unknown future
    /// roles as non-child, which is the conservative default.
    fn is_child_role(role: &MethodRole) -> bool {
        matches!(
            role,
            MethodRole::StaticChild | MethodRole::DynamicChild { .. }
        )
    }

    #[test]
    fn solar_is_hub_with_planets_via_role() {
        let solar = Solar::new();
        let schema = solar.plugin_schema();

        // IR-8: Solar is a hub by virtue of its role-tagged #[child] method,
        // not the legacy `children` side-table.
        assert!(
            schema.is_hub_by_role(),
            "solar should be a hub via MethodRole"
        );

        // Solar declares exactly one child-gate (`body`), tagged DynamicChild.
        let child_methods: Vec<&_> = schema
            .methods
            .iter()
            .filter(|m| is_child_role(&m.role))
            .collect();
        assert_eq!(
            child_methods.len(),
            1,
            "solar should have exactly one role-tagged child method; got {child_methods:?}"
        );
        let body = child_methods[0];
        assert_eq!(body.name, "body", "the child-gate method should be `body`");
        assert!(
            matches!(body.role, MethodRole::DynamicChild { .. }),
            "`body` must be DynamicChild; got {:?}",
            body.role
        );

        // Verify all 8 planets are configured with deterministic per-planet
        // hashes. Source-of-truth is the `CelestialBody` tree; we derive
        // child summaries the same way `CelestialBody::to_plugin_schema`
        // does — no dependency on Solar's deprecated `plugin_children()`
        // override.
        let _ = solar; // Solar's methods-under-test are the role-tagged ones above.
        let system = build_solar_system();
        let planets: Vec<_> = system
            .children
            .iter()
            .map(CelestialBody::to_child_summary)
            .collect();
        assert_eq!(planets.len(), 8, "solar should have 8 configured planets");
        let jupiter = planets
            .iter()
            .find(|c| c.namespace == "jupiter")
            .expect("jupiter planet should be present");
        assert!(jupiter.description.contains("planet"));
        assert!(!jupiter.hash.is_empty());
    }

    // DynamicHub aggregates registered activations and exposes them via its
    // own (non-deprecated) `plugin_children()` method on `DynamicHub`. That
    // method is the replacement read-path for the soon-to-be-removed
    // `schema.children` side-table field — it returns the same
    // `Vec<ChildSummary>` without any deprecation surface.
    #[test]
    fn solar_registered_with_dynamic_hub() {
        let hub = DynamicHub::new("plexus").register(Solar::new());

        // `DynamicHub::plugin_children()` is the replacement API for reading
        // the registrant roster. It is not deprecated (plexus-core 0.5.2 +).
        let children = hub.plugin_children();

        let solar = children
            .iter()
            .find(|c| c.namespace == "solar")
            .expect("solar should be registered under DynamicHub");
        assert!(solar.description.contains("Solar system"));
        assert!(!solar.hash.is_empty());
    }

    // IR-8 AC #6: the derived query returns the same hub-ness Solar had
    // before the migration. Deliberately separate from the field-access
    // assertions so downstream consumers have a compact, role-only fixture.
    #[test]
    fn solar_is_hub_by_role_returns_true() {
        let solar = Solar::new();
        assert!(
            solar.plugin_schema().is_hub_by_role(),
            "Solar must be a hub by MethodRole (IR-8 AC #6)"
        );
    }

    // IR-8 AC #6: the count of child-gate methods matches the number of
    // `#[child]`-annotated methods on Solar (currently 1: `body`).
    #[test]
    fn solar_has_one_child_gate_method_by_role() {
        let solar = Solar::new();
        let schema = solar.plugin_schema();
        let gate_count = schema
            .methods
            .iter()
            .filter(|m| is_child_role(&m.role))
            .count();
        assert_eq!(
            gate_count, 1,
            "Solar should report exactly one non-Rpc role-tagged method"
        );
    }

    // IR-8 AC #3: verify the source-level deprecation annotations on the
    // hand-written `plugin_children` override are present. This is a
    // source-text assertion because `plugin_children` is not a `#[method]`
    // and therefore does not surface through `PluginSchema` metadata.
    #[test]
    fn plugin_children_is_source_annotated_deprecated() {
        let src = include_str!("activation.rs");
        // Locate the function definition and verify both companion
        // attributes precede it on the same item.
        let fn_pos = src
            .find("pub fn plugin_children(&self)")
            .expect("plugin_children definition must be present");
        let preamble = &src[..fn_pos];

        // Grab the last attribute-block bounded by the preceding blank line
        // (the method's attribute list), so we only inspect attrs on this
        // specific function.
        let block_start = preamble
            .rfind("\n\n")
            .map_or(0, |i| i + 2);
        let attr_block = &preamble[block_start..];

        assert!(
            attr_block.contains("#[deprecated("),
            "plugin_children must carry #[deprecated(...)] (IR-8 AC #3)"
        );
        assert!(
            attr_block.contains("since = \"0.5.0\""),
            "plugin_children must declare `since = \"0.5.0\"` (IR-8 AC #3; semver-compliant)"
        );
        assert!(
            attr_block.contains("#[plexus_macros::removed_in(\"0.6\")]"),
            "plugin_children must carry #[plexus_macros::removed_in(\"0.6\")] (IR-8 AC #3)"
        );
    }

    #[test]
    fn solar_hash_changes_with_structure() {
        let solar1 = Solar::new();
        let solar2 = Solar::new();

        // Same structure = same hash
        assert_eq!(
            solar1.plugin_schema().hash,
            solar2.plugin_schema().hash
        );
    }

    #[test]
    fn print_solar_schema() {
        let solar = Solar::new();
        let schema = solar.plugin_schema();
        let json = serde_json::to_string_pretty(&schema).unwrap();
        println!("Solar system schema:\n{json}");
    }

    #[tokio::test]
    async fn test_nested_routing_mercury() {
        let solar = Solar::new();
        let result = Activation::call(&solar, "mercury.info", serde_json::json!({}), None, None).await;
        assert!(result.is_ok(), "mercury.info should be callable: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_nested_routing_jupiter_io() {
        let solar = Solar::new();

        // Call solar.call("jupiter.io.info", {}) - should route jupiter → io
        let result = Activation::call(&solar, "jupiter.io.info", serde_json::json!({}), None, None).await;
        assert!(result.is_ok(), "jupiter.io.info should be callable");
    }

    #[tokio::test]
    async fn test_nested_routing_earth_luna() {
        let solar = Solar::new();

        // Call solar.call("earth.luna.info", {}) - should route earth → luna
        let result = Activation::call(&solar, "earth.luna.info", serde_json::json!({}), None, None).await;
        assert!(result.is_ok(), "earth.luna.info should be callable");
    }

    #[tokio::test]
    async fn test_nested_routing_invalid_child() {
        let solar = Solar::new();

        // Call with invalid child
        let result = Activation::call(&solar, "pluto.info", serde_json::json!({}), None, None).await;
        assert!(result.is_err(), "pluto.info should fail - not a planet");
    }

    // CHILD-7: `list_children` capability exposes configured planet names.
    #[tokio::test]
    async fn solar_list_children_returns_configured_planets() {
        use crate::plexus::ChildRouter;
        use futures::StreamExt;

        let solar = Solar::new();
        let listed: Vec<String> = solar
            .list_children()
            .await
            .expect("list_children must be Some for Solar (CHILD-4 opt-in)")
            .collect()
            .await;

        assert!(!listed.is_empty(), "list_children must yield at least one name");
        assert_eq!(listed.len(), 8, "Solar has 8 configured planets");
        for expected in [
            "Mercury", "Venus", "Earth", "Mars",
            "Jupiter", "Saturn", "Uranus", "Neptune",
        ] {
            assert!(
                listed.iter().any(|n| n == expected),
                "list_children should include {expected}; got {listed:?}"
            );
        }
    }

    // CHILD-7: case-insensitive child lookup is preserved through the
    // migrated `#[child]` method body.
    #[tokio::test]
    async fn solar_get_child_case_insensitive() {
        use crate::plexus::ChildRouter;

        let solar = Solar::new();
        assert!(
            solar.get_child("Mercury").await.is_some(),
            "`Mercury` should resolve"
        );
        assert!(
            solar.get_child("mercury").await.is_some(),
            "`mercury` should resolve (case-insensitive)"
        );
        assert!(
            solar.get_child("MERCURY").await.is_some(),
            "`MERCURY` should resolve (case-insensitive)"
        );
        assert!(
            solar.get_child("pluto").await.is_none(),
            "`pluto` should not resolve"
        );
    }
}
