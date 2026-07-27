# PLX-148 — why `solar`'s eight planets are not eight edges (2026-07-27)

**Status: RFC finding, recorded rather than fixed. Half of it is a defect with a
named owner and half of it is not a defect at all.**

## The measurement

| surface | what `solar` advertises |
|---|---|
| legacy wire (`solar.schema` → `PluginSchema.children`) | **8** — mercury … neptune, each with its own hash |
| Connectome (`substrate.connectome`) | **1** child edge, `body`, kind `Dynamic` |

`solar` hand-writes `plugin_children()` precisely so those eight carry real
hashes; PLX-150 confirmed it (the macro's synthesis path would have emitted
empty strings, which is the bug PLX-150 fixed for `orcha`). So the eight on the
legacy wire are deliberate, and the one on the Connectome is not a fold error.

## Finding 1 — the eight instances cannot appear, and should not

`solar.body` is `#[plexus_macros::child(list = "body_names")] async fn body(&self,
name: &str) -> Option<CelestialBodyActivation>`: a **family of instances resolved
at runtime**, not eight declared children.

- §4.8 makes a node's child set part of its hash preimage. Eight planet names in
  the document would make `ir_hash` a function of runtime state, and PLX-157
  proved live that it is not one today — two fetches either side of a mutation
  attempt returned a byte-identical document.
- PLX-127 reached the same position from the security side and made it a
  criterion: the `tenants` mount renders as **one template and no instance ids**,
  and its test asserts the serialized bytes contain neither tenant's id while
  still containing the template. Enumeration is disclosure.
- §5.1's Indexed edge is explicit that what it carries is the enumeration
  method's **name**, not its result — the same point PLX-121 made when it
  refused to invent an identity for a runtime query.

So: the eight are **reachable, not listed**, and that is the RFC's position
rather than a compromise. `plx148_completeness.rs::solar_advertises_one_edge_
where_the_legacy_wire_advertises_eight` asserts it, including that no planet name
appears anywhere in the served bytes.

## Finding 2 — the *mechanism* to enumerate them is missing, and that IS a defect

`solar.body` declares everything §5.1's **Indexed** edge needs and none of it
reaches the wire:

| §5.1 Indexed fact | declared as | on the wire |
|---|---|---|
| `list_method` | `list = "body_names"` | **dropped** |
| `search_method` | `search = …` (available) | **dropped** |
| `id_field` | defaulted `"name"` | **dropped** |
| `path_template` | `body/{id}` | **dropped** |
| `template` | `CelestialBodyActivation` | reduced to its hash |

The cause is one branch. `plexus-macros/src/ir_parse/signature.rs::child_edge`
matches `ChildMethodKind::Dynamic` **before** it consults `info.list_fn`, and only
the `Static` arm reaches `ChildEdgeSpec::Indexed`. A dynamic child with a `list`
argument is therefore emitted as `Dynamic` and its enumeration facts are
discarded.

`claudecode.session` (`list = "session_ids"`) and `cone.of` (`list = "cone_ids"`)
have the identical defect. PLX-127 built the Indexed producer on the hub side and
observed that §5.1's Indexed facts were *"vocabulary with no producer"*; this is
the second producer, and it is one `match` arm away.

**Not fixed here, deliberately.** It lives in `plexus-macros`, which this build
does not own; it would move three activation hashes and the root; and converting
all three would leave substrate with **zero** Dynamic edges, deleting the very
evidence PLX-148 c1 requires. The negative pin in `plx148_completeness.rs` fails
when it is fixed, which is how PLX-124 wanted a gap recorded.

## Finding 3 — a planet's document had zero methods, silently (FIXED here)

`CelestialBodyActivation::__plexus_activation_ir` passed `activation_ir!` a
**bodyless** signature (`async fn info(&self) -> …;`). `syn` parses that inside an
`impl` block as `ImplItem::Verbatim`, and `ActivationIrSpec::from_impl` skips
every item that is not `ImplItem::Fn` with `continue` — so the activation's only
method was dropped without a warning and `celestial` built as a document with
zero methods.

Nothing caught it because nothing could: the document had no wire route. It got
one when `solar/body` became fetchable, and a fetchable document describing no
methods is worse than an honest refusal. The call site is fixed (a `{ … }` body);
`a_celestial_body_declares_its_one_method` pins it. **The underlying defect —
`from_impl` silently omitting an `impl` item it cannot read, rather than
erroring — is in `plexus-macros` and is not fixed here.**

## Related, found while measuring

The `tenants` Indexed edge advertises `list_method: "tenants.list"`. PLX-157
recorded that `tenants.list` answers `-32601`. An Indexed edge naming a method
that does not exist is the same class of problem as this document's: a fact on
the wire a client cannot act on.
