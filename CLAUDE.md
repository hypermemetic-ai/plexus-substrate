Often I will say "open this" in reference to code use `codium --goto` to take me there

if you need to see the dir structure use the `tre` command

## Architecture Documentation Naming Convention

Architecture documents in `docs/architecture/` use reverse-chronological naming to ensure newest documents appear first in alphabetical sorting.

**Naming formula**: `(u64::MAX - nanotime)_title.md`

Where:
- `nanotime` = current Unix timestamp in nanoseconds
- This creates a descending numeric prefix (newer = smaller number = sorts first)
- Example: `16681577588676290559_type-system.md`

**To generate a filename**:
```python
import time
nanotime = int(time.time() * 1_000_000_000)
filename = (2**64 - 1) - nanotime
print(f'{filename}_your-title.md')
```

This "chronological bubbling" helps prioritize recent architectural decisions.

## Plexus RPC Terminology

**Plexus RPC** is the protocol name. Use this terminology consistently:

| Term | Definition | Usage |
|------|------------|-------|
| **Plexus RPC** | The RPC protocol itself | "Services communicate via Plexus RPC" |
| **Plexus RPC server** | A server implementing Plexus RPC | "Substrate is a Plexus RPC server" |
| **Activation** | A service/capability implementing the Activation trait | "Arbor is an activation" |
| **DynamicHub** | Routes calls to registered activations (formerly "Plexus") | "DynamicHub routes to multiple activations" |
| **Hub activation** | An activation with children (implements ChildRouter) | "Solar is a hub activation with planet children" |
| **Substrate** | The reference Plexus RPC server implementation | "Substrate provides arbor, cone, and bash" |
| **Synapse** | CLI for Plexus RPC servers | "Use synapse to connect to any Plexus RPC server" |

**Library naming:**
- `hub-core`, `hub-macro`, `hub-transport` - Keep these names (backwards compatibility)
- Update descriptions to mention "Plexus RPC"
- Future codegen tools: `plexus-codegen-{language}` pattern

See `docs/architecture/16676565123400000000_plexus-rpc-ecosystem-naming.md` for complete strategy.

## Driving substrate via synapse (real usage)

substrate runs on **:4444** and registers as **`substrate`**. Drive it with the `synapse` CLI — **synapse is a normal CLI; every parameter is its own `--flag`** (you do NOT pass a `-p '{json}'` blob). Navigate `synapse [global-flags] substrate <activation> <method> --param value`. Full grammar lives in the synapse skill (`../skills/skills/synapse/SKILL.md`); this is substrate's concrete surface.

Activations: **arbor** (tree/node store), **bash** (command execution), **changelog**, **claudecode** (Claude Code sessions).

```bash
# discover what's exposed — let the backend tell you the shape
synapse -i substrate                          # full method map (irMethods keys: arbor.*, bash.*, …)
synapse substrate bash execute                # bare call → prints this method's --flags + docs
synapse substrate bash execute --help         # same, as help text

# real calls — each param is a flag (kebab-case)
synapse -j substrate bash execute --command "ls -la"
synapse -j substrate arbor tree_list
synapse -j substrate arbor tree_get   --tree-id <UUID>
synapse -j substrate arbor node_create_text --tree-id <UUID> --parent-id <UUID> --text "…"
```

- **Global flags before `substrate`** (`-j` raw JSON, `-i` IR map, `-s` schema, `-P 4444` to bypass a stale registry entry); **method `--params` after the method.**
- List-valued params repeat the flag (`--tags a --tags b`); a param whose value is a nested JSON object takes it inline (`--meta '{"k":"v"}'`). Everything else is a plain `--flag value`.
- Responses stream as newline-delimited `{"type":"data",…}` envelopes ending in `{"type":"done"}`; pipe through `jq`/python to parse.

## Key Architecture Documents

- **Plexus RPC Ecosystem Naming** (`docs/architecture/16676565123400000000_plexus-rpc-ecosystem-naming.md`): Complete naming strategy for the Plexus RPC ecosystem. Covers protocol naming, library naming, codegen tools, and migration path.

- **Plugin Development Guide** (`docs/architecture/16678373036159325695_plugin-development-guide.md`): Complete guide for creating Plexus RPC activations. Covers hub macros, manual implementation, hub plugins with children, parent context injection, handle system, and Synapse CLI impact. Use this when creating new activations.

This project consists of a few pieces: Substrate, Symbols, and Cllient. All should be in the parent which should be called `controlflow`
When I say "create an architecture doc and link it" I mean to create the same file in whichever subprojects it makes sense to add to. We then have a claude instance which reads them there and this steers cross project development. Ideally when I say "write a doc" that implicitly includes linking it and also mentioning it in the commit you add it to. usually we write a doc when we are going to commit anyway. if I'm asking for one determine if this is at a commit boundary

## Planning Documents Convention

Planning docs live in `plans/<EPIC>/` where EPIC is a short prefix (e.g., MCP, ARBOR, CONE).

**Structure:**
```
plans/
  MCP/
    MCP-1.md   # Epic overview (the "master plan")
    MCP-2.md   # Individual ticket
    MCP-3.md   # Individual ticket
    ...
```

**Ticket format:** `<EPIC>-<N>.md` where N is sequential within the epic.

**Epic overview (X-1.md)** contains:
- High-level goal and context
- Dependency DAG showing which tickets unlock others
- Phase breakdown

**Individual tickets (X-N.md)** contain:
- `blocked_by: [X-2, X-3]` - tickets that must complete first
- `unlocks: [X-5, X-6]` - tickets that can start once this completes
- Scope, acceptance criteria, implementation notes

**Design for parallelism:** Structure tickets so completing one unlocks multiple concurrent tickets. The DAG should fan out, not be linear. Identify the critical path and minimize its length.

```
     X-2 (foundation)
      │
  ┌───┼───┬───┐
  ▼   ▼   ▼   ▼
 X-3 X-4 X-5 X-6  ← parallel work
  │   │   │   │
  └───┴───┼───┘
          ▼
        X-7 (integration)
```

When a ticket is completed, check its `unlocks` field to identify newly unblocked work.