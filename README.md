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
- **MCP** — `http://localhost:4444/mcp` (all methods appear as MCP tools; not served under login, below)
- **Synapse CLI** — `synapse substrate <namespace> <method> [--param value]`
- **In-process Rust** — `DynamicHub::call(method, params)`

### Login (PLEXUS-6)

There is no open mode and no shared `--api-key`: substrate takes callers
through `plexus-login` (menger's login) and plexus-core's gate.

```bash
substrate --port 4444 --auth-provider key \
  --admin "$(synapse _key show operator)" \
  --grant "editor=$(synapse _key show builder)" \
  --data-dir ~/.local/share/substrate
synapse -P 4444 _login substrate --key builder --as builder
```

- An anonymous connection reaches only `auth.*`; a refused credential is a
  401 at the upgrade; a method outside the caller's roles is `forbidden`.
- `viewer`: `health`, `echo`, and reading one's own claudecode sessions;
  `editor`: viewer plus create / chat / fork / delete of one's own sessions.
  Everything else (bash, arbor, cone, orcha, `sessions_*`, …) is `admin`.
- A claudecode session belongs to the principal that created it; another
  key sees `session not found`. An admin reaches every session.
- `--grant ROLE=PRINCIPAL` (`SUBSTRATE_GRANTS`) gives standing grants;
  `auth.grant` at runtime is forgotten at restart. `--login-port` serves
  the login page for people's providers (`idp=…`, `workos=…`).
- Under login, stdio and the MCP HTTP gateway are not served.

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

## See also

- [`docs/activations/orcha/README.md`](docs/activations/orcha/README.md) — Orcha: multi-agent orchestration
- [`docs/architecture/intro-lattice-orcha-tdd.md`](docs/architecture/intro-lattice-orcha-tdd.md) — full stack walkthrough
- [`docs/architecture/__index.md`](docs/architecture/__index.md) — architecture doc index
- [`docs/QUICKSTART.md`](docs/QUICKSTART.md) — getting started guide
- [`docs/architecture/16678373036159325695_plugin-development-guide.md`](docs/architecture/16678373036159325695_plugin-development-guide.md) — how to write a new activation

## License

MIT
