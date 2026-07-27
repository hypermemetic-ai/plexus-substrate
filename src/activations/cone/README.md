# cone

LLM cone with persistent conversation context.

## Overview

Cone is substrate's generic LLM-agent runtime: a named "cone" holds a model
id, a system prompt, and a `Position` (tree + node) into an Arbor
conversation tree. Chatting advances the head along the tree by writing a
user message + an assistant response as two new external nodes whose
`Handle`s point back at the Cone database row carrying the full text.

Cone is a handle-resolution *provider*: it carries the `resolve_handle` flag
and `resolve_handle_impl`, and it never needed a parent to do it (PLX-117
deleted the unused parent-injection ritual). Context assembly walks
from the root of the tree to the current head, resolves every external
handle to its owning activation's content, and hands the resulting
`Vec<cllient::Message>` to the configured model via `cllient::ModelRegistry`.

Cone also implements `resolve_handle` for its own `ConeHandle::Message`
variant — so an Arbor tree that contains cone messages authored by another
conversation can be rendered with content inlined when viewed.

Sessions can be addressed either flatly (`cone.chat(identifier=…)`) or
through the dynamic child gate `cone.of(<id-or-name>).chat(…)` — see
`ConeActivation` below.

## Namespace

`cone` — invoked via `synapse <backend> cone.<method>`, or per-cone via
`synapse <backend> cone.of <id>.<method>`.

## Methods (Cone)

| Method | Params | Returns | Description |
|---|---|---|---|
| `create` | `name: String, model_id: String, system_prompt: Option<String>, metadata: Option<Value>` | `Stream<Item=CreateResult>` | Create a new cone. Validates `model_id` against the LLM registry before persisting. |
| `get` | `identifier: ConeIdentifier` | `Stream<Item=GetResult>` | Get cone configuration by name or UUID. |
| `list` | — | `Stream<Item=ListResult>` | List all cones. |
| `delete` | `identifier: ConeIdentifier` | `Stream<Item=DeleteResult>` | Delete a cone; the associated Arbor tree is preserved. |
| `chat` | `identifier: ConeIdentifier, prompt: String, ephemeral: Option<bool>` | `Stream<Item=ChatEvent>` (streaming) | Send a prompt; emits `Start`, `Content` chunks, and final commit events. If `ephemeral=true`, nodes are created but the head is not advanced and nodes are marked for deletion. |
| `set_head` | `identifier: ConeIdentifier, node_id: NodeId` | `Stream<Item=SetHeadResult>` | Move the cone's canonical head to a different node in the same tree. |
| `registry` | — | `Stream<Item=RegistryResult>` | Dump available LLM services and models. |

## Children

| Child | Kind | list method | search method | Description |
|---|---|---|---|---|
| `of` | dynamic | `cone_ids` | `of` | Look up a cone by name or UUID and return a `ConeActivation` — a typed per-cone namespace. Introduced in IR-19. |

## Methods (ConeActivation — per-cone)

Exposed via `cone.of(<id>).<method>`:

| Method | Params | Returns | Description |
|---|---|---|---|
| `get` | — | `Stream<Item=GetResult>` | Return this cone's configuration. |
| `delete` | — | `Stream<Item=DeleteResult>` | Delete this cone. |
| `set_head` | `node_id: NodeId` | `Stream<Item=SetHeadResult>` | Move this cone's head. |
| `chat` | `prompt: String, ephemeral: Option<bool>` | `Stream<Item=ChatEvent>` (streaming) | Send a prompt. Mirror of the flat `chat`, with the cone fixed by the child gate. |

## Handle system

Cone derives `ConeHandle` with `#[derive(HandleEnum)]`. The one variant —
`Message { message_id, role, name }` — encodes as
`cone@1.0.0::chat:msg-{uuid}:{role}:{name}` and resolves through
`resolve_handle` to a structured `ResolveResult::Message { id, role,
content, model, name }`.

## Storage

- Backend: SQLite
- Config: `ConeStorageConfig { db_path }`; construction takes
  `Arc<ArborStorage>` in addition.
- Schema: cones keyed by UUID (name index); messages keyed by UUID with
  `cone_id`, `role`, `content`, `model_id`, and optional token counts. See
  `src/activations/cone/storage.rs`.

## Composition

- `Arc<ArborStorage>` — injected at construction; every chat writes
  external nodes to Arbor via direct (in-process) storage calls.
- `cllient::ModelRegistry` — resolves `model_id` to a concrete LLM client.
- `Mustache` — `Cone::register_default_templates(&mustache)` installs
  `chat.default`, `chat.markdown`, `chat.json`, `create.default`,
  `list.default`.

## Example

```bash
synapse --port 44104 lforge substrate cone.create \
  '{"name":"asst","model_id":"gpt-4o-mini"}'
synapse --port 44104 lforge substrate cone.chat \
  '{"identifier":{"by_name":{"name":"asst"}},"prompt":"hello"}'

# Per-cone child gate
synapse --port 44104 lforge substrate cone.of asst.chat '{"prompt":"hello"}'
```

## Source

- `activation.rs` — flat `Cone<P>` + per-cone `ConeActivation` RPC surfaces,
  handle resolution, context assembly
- `methods.rs` — `ConeIdentifier` (by id / by name)
- `storage.rs` — SQLite persistence + `ConeStorageConfig`
- `types.rs` — `ConeHandle` (`HandleEnum`), `ConeConfig`, `Message`,
  `MessageRole`, `Position`, result enums, `ConeError`
- `tests.rs` — in-process integration tests
- `mod.rs` — module exports
