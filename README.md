# Substrate

A Plexus RPC server. Write Rust methods with `#[hub_method]`, get a
self-describing streaming RPC server with WebSocket, MCP, and CLI access
— no separate schema files, no drift.

---

## Architecture

Three layers. Each knows only about the layer below it.

```
┌────────────────────────────────────────────────────────┐
│  Activations                                           │
│  Pluggable modules. Each exposes typed, streaming      │
│  methods via the hub macro. Orcha, Lattice, Arbor, ... │
├────────────────────────────────────────────────────────┤
│  Plexus RPC                                            │
│  Self-describing, streaming-first RPC protocol.        │
│  Code is schema. Runtime JSON Schema per method.       │
│  Language-agnostic clients via hub-codegen.            │
├────────────────────────────────────────────────────────┤
│  Transport                                             │
│  WebSocket + MCP on the same port (4444).              │
│  Synapse CLI — dynamic, schema-driven command line.    │
└────────────────────────────────────────────────────────┘
```

---

## Activations

| Activation | Purpose |
|---|---|
| **orcha** | Multi-agent orchestration — run ticket plans as parallel agent DAGs, human approval gates, child graphs. See [`docs/activations/orcha/README.md`](docs/activations/orcha/README.md). |
| **lattice** | DAG execution engine underlying Orcha. Nodes, edges, typed tokens, scatter/gather, join types. |
| **arbor** | Conversation tree storage. Backs agent session history. |
| **claudecode** | Claude Code CLI session wrapper. Spawns and manages Claude sessions. |
| **claudecode_loopback** | Tool-use approval routing. Claude sessions request permission; routed through the approval API. |
| **bash** | Shell command execution. |
| **changelog** | API hash tracking — logs when the method schema changes between restarts. |
| **mustache** | Template rendering. |

---

## Access

Everything is exposed on port `4444`:

- **WebSocket** — `ws://localhost:4444`
- **MCP** — `http://localhost:4444/mcp` (all methods appear as MCP tools)
- **Synapse CLI** — `synapse substrate <namespace> <method> [--param value]`
- **In-process Rust** — `DynamicHub::call(method, params)`

---

## Quickstart

```bash
# Start
substrate-start

# Explore available methods
LANG=C.UTF-8 synapse substrate

# Run an agent graph from a ticket plan
LANG=C.UTF-8 synapse substrate orcha run_tickets_files \
  --ticket_files '["plans/TDD/TDD-1.md"]' \
  --model sonnet \
  --working_directory /workspace/hypermemetic/plexus-substrate
```

---

## Wire compatibility — breaking change in M2

**If you parse substrate responses, read this before upgrading.**

M2 converted 86 methods from "yield exactly once, then end the stream" to a unary
`Result`. Routing did not change — a method that was served as JSON is still
served as JSON, and a method that was served as SSE is still served as SSE — but
**the response body changed shape** for every one of those 86 methods:

| | JSON body |
|---|---|
| before | `{"data":[payload, {"stop":…,"value":null}]}` |
| after  | `{"data":[{"stop":…,"value":payload}]}` |

Two things moved at once: the `data` array is **one element shorter**, and the
payload is now **nested inside `stop.value`** instead of sitting alongside an
empty terminal. A client that read `data[0]` for the payload will now read the
terminal; a client that read `data[data.length - 1].value` expecting `null` will
now get the payload. `echo.ping` shipped this first; the rest followed.

Failures moved with it. A method that previously signalled failure by yielding a
domain `…Result::Err { message }` on the success channel now terminates the turn
with an error: the code is at `content.stop.error.code` (a dotted string such as
`claudecode.session_not_found`), the human message at `.message`, and a
serializable domain error at `.details`.

Two further changes in the same release:

- **`lattice.execute` and `bash.execute` now answer `text/event-stream`** over
  the HTTP gateway instead of one buffered `application/json` document. Both
  genuinely stream; they had simply never declared it. In-process callers and
  the MCP gateway are unaffected.
- **`orcha.run_tickets` and `orcha.run_tickets_async_files` no longer emit
  `GraphStarted` before failing** on a nonexistent `working_directory`. That path
  used to emit `GraphStarted` and then `Failed`; it now emits `Failed` alone, and
  fails before a graph is built, so no `graph_id` is allocated for a run that
  cannot start.

---

## See also

- [`docs/activations/orcha/README.md`](docs/activations/orcha/README.md) — Orcha: multi-agent orchestration
- [`docs/architecture/intro-lattice-orcha-tdd.md`](docs/architecture/intro-lattice-orcha-tdd.md) — full stack walkthrough
- [`docs/architecture/__index.md`](docs/architecture/__index.md) — architecture doc index
- [`docs/QUICKSTART.md`](docs/QUICKSTART.md) — getting started guide
- [`docs/architecture/16678373036159325695_plugin-development-guide.md`](docs/architecture/16678373036159325695_plugin-development-guide.md) — how to write a new activation

## License

MIT
