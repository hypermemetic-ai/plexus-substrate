# arbor

Manage conversation trees with context tracking.

## Overview

Arbor owns substrate's conversation-tree data model. A tree is a rooted DAG
of `Node`s; each node is either a `Text` node (inline content) or an
`External` node (a `Handle` referencing data owned by another activation —
e.g. a Cone message or a ClaudeCode event). Arbor tracks ownership with
reference counts (`tree_claim` / `tree_release`) and has a scheduled-deletion
/ archive lifecycle driven by `ArborConfig`.

Handle resolution is the cross-activation integration point. When an Arbor
tree is rendered via `tree_render`, each external node's handle is resolved
through an injected `HandleResolvers` map (PLX-117) — so `arbor.tree_render`
of a conversation built by Cone and ClaudeCode shows the actual message
content, not just handle references.

Arbor's `ArborStorage` is intentionally **not** addressed through Plexus RPC
for internal use; other activations that build on top of Arbor
(`Cone`, `ClaudeCode`) receive an `Arc<ArborStorage>` at construction time
and call the storage methods directly. Plexus-RPC is reserved for cross-
plugin handle resolution and external callers. See the module-level doc on
`ArborStorage` for the full rationale.

## Namespace

`arbor` — invoked via `synapse <backend> arbor.<method>`.

## Methods

### Tree operations

| Method | Params | Returns | Description |
|---|---|---|---|
| `tree_create` | `metadata: Option<Value>, owner_id: String` | `Stream<Item=ArborEvent>` | Create a new conversation tree. |
| `tree_get` | `tree_id: TreeId` | `Stream<Item=ArborEvent>` | Retrieve a complete tree with all nodes. |
| `tree_get_skeleton` | `tree_id: TreeId` | `Stream<Item=ArborEvent>` | Get lightweight tree structure without node data. |
| `tree_list` | — | `Stream<Item=ArborEvent>` | List all active trees. |
| `tree_update_metadata` | `tree_id: TreeId, metadata: Value` | `Stream<Item=ArborEvent>` | Update tree metadata. |
| `tree_claim` | `tree_id: TreeId, owner_id: String, count: i64` | `Stream<Item=ArborEvent>` | Increment reference count for a tree owner. |
| `tree_release` | `tree_id: TreeId, owner_id: String, count: i64` | `Stream<Item=ArborEvent>` | Decrement reference count for a tree owner. |
| `tree_list_scheduled` | — | `Stream<Item=ArborEvent>` | List trees scheduled for deletion. |
| `tree_list_archived` | — | `Stream<Item=ArborEvent>` | List archived trees. |
| `tree_render` | `tree_id: TreeId` | `Stream<Item=ArborEvent>` | Render a tree as text; resolves external handles via parent context when available. |

### Node operations

| Method | Params | Returns | Description |
|---|---|---|---|
| `node_create_text` | `tree_id: TreeId, parent: Option<NodeId>, content: String, metadata: Option<Value>` | `Stream<Item=ArborEvent>` | Create a text node. |
| `node_create_external` | `tree_id: TreeId, parent: Option<NodeId>, handle: Handle, metadata: Option<Value>` | `Stream<Item=ArborEvent>` | Create an external node carrying a handle. |
| `node_get` | `tree_id: TreeId, node_id: NodeId` | `Stream<Item=ArborEvent>` | Fetch a single node. |
| `node_get_children` | `tree_id: TreeId, node_id: NodeId` | `Stream<Item=ArborEvent>` | Direct children of a node. |
| `node_get_parent` | `tree_id: TreeId, node_id: NodeId` | `Stream<Item=ArborEvent>` | Parent of a node (if any). |
| `node_get_path` | `tree_id: TreeId, node_id: NodeId` | `Stream<Item=ArborEvent>` | Node-id path from root to the target. |

### Context operations

| Method | Params | Returns | Description |
|---|---|---|---|
| `context_list_leaves` | `tree_id: TreeId` | `Stream<Item=ArborEvent>` | All leaf nodes in a tree. |
| `context_get_path` | `tree_id: TreeId, node_id: NodeId` | `Stream<Item=ArborEvent>` | Full node data from root to the target. |
| `context_get_handles` | `tree_id: TreeId, node_id: NodeId` | `Stream<Item=ArborEvent>` | All external handles on the root-to-target path. |

## Storage

- Backend: SQLite
- Config: `ArborConfig { scheduled_deletion_window, archive_window, db_path, auto_cleanup, cleanup_interval }`
- Lifecycle: active → scheduled → archived, with a background cleanup task
  driven by `auto_cleanup` / `cleanup_interval`.
- See `src/activations/arbor/storage.rs`.

## Composition

- `HandleResolvers` — a `plugin_id -> resolver` map handed to Arbor at the
  composition root (`builder.rs`, after Cone and ClaudeCode are built) so
  `tree_render` can resolve foreign handles. It replaced the injected parent
  `HubContext`: PLX-111 measured that the parent handle resolved exactly one
  thing, routed on `plugin_id` alone, so the replacement is data, not a
  runtime capability. An empty map degrades to `tree.render()`.
- `Cone` and `ClaudeCode` hold `Arc<ArborStorage>` injected at their own
  construction time and call storage methods directly (in-process), not via
  Plexus-RPC.

## Example

```bash
synapse --port 44104 lforge substrate arbor.tree_create '{"owner_id":"demo"}'
synapse --port 44104 lforge substrate arbor.tree_render '{"tree_id":"<uuid>"}'
```

## Source

- `activation.rs` — RPC method surface + handle-resolution helpers
- `methods.rs` — supporting method helpers
- `storage.rs` — SQLite persistence + `ArborConfig` + lifecycle
- `types.rs` — `Tree`, `Node`, `NodeType`, `Handle`, `ArborEvent`, ID newtypes
- `views.rs` — range / collapse / resolve views
- `mod.rs` — module exports
