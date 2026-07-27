//! ACP — `claudecode` as the reference Agent Client Protocol agent (PLX-140).
//!
//! ```text
//!   editor  ──spawns──▶  acp-stdio  ──▶  ClaudeCodeAcpAgent  ──▶  ClaudeCode
//!      ▲                                        │                  (confined,
//!      │  session/update  ◀────────────────────┘                   per tenant)
//!      │  session/request_permission  ◀── the turn's callback
//! ```
//!
//! See [`agent`] for the design. This page records the one criterion this
//! build could **not** discharge in full, because a residual stated is worth
//! more than a residual papered over.
//!
//! # What is gone, and what is not (criterion c3)
//!
//! PLX-140 c3 asks that "claudecode's hand-rolled loopback correlation (query
//! param, env var, poll loop) is gone".
//!
//! **On the ACP path it is gone, and gone structurally.** A session opened by
//! `session/new` is created with `loopback_enabled = false`
//! ([`ClaudeCodeAcpAgent::new_session`](agent::ClaudeCodeAcpAgent)), and every
//! piece of that correlation lives behind that flag:
//!
//! | mechanism | where | reachable from ACP? |
//! |---|---|---|
//! | `?session_id=` on the MCP URL | `claudecode/executor.rs:623` | no — inside `if loopback_enabled` |
//! | `PLEXUS_SESSION_ID` in the MCP env | `claudecode/executor.rs:637` | no — same block |
//! | `PLEXUS_SESSION_ID` on the child | `claudecode/executor.rs:948` | no — `loopback_session_id` is `None` |
//! | `--permission-prompt-tool` | `claudecode/executor.rs:563-565` | no — `loopback_enabled` gates it |
//! | 1s × 300s poll loop | `claudecode_loopback/activation.rs:161-224` | no — `permit` is never called |
//! | `StreamStatus::AwaitingPermission` | `claudecode/activation.rs:1282-1283` | no — guarded on the loopback tool name |
//!
//! `tests/acp_claudecode.rs::the_acp_path_carries_no_loopback_correlation`
//! asserts this mechanically rather than by reading.
//!
//! **In the tree it is NOT gone**, and this build did not remove it. The
//! reason is measured, not cautious:
//!
//! 1. **PLX-105 is parked on a blocker this ticket forbade re-opening.**
//!    `loopback.permit`'s turn is opened *by the spawned CLI*, so a callback
//!    raised inside it would ask the asker. The correlation the ticket wants
//!    deleted is between two *different* turns, and `plexus-core`'s per-turn
//!    router refuses cross-turn delivery **by construction**. Deleting the
//!    correlation without a cross-turn mechanism would not re-plumb the path;
//!    it would break it.
//! 2. **PLX-146 c3 measured that the live MCP bridge has no responder**, so
//!    there is nothing on the deployed path to answer a turn callback raised
//!    from an MCP tool even if the direction were right.
//! 3. **PLX-145 measured that the `--permission-prompt-tool` contract is
//!    already broken** by M2's turn switchover, in a way neither shape of
//!    `permit` fixes. Changing `permit` here would change two things at once.
//!
//! So the legacy `claudecode.chat`-over-MCP path keeps its loopback, unchanged
//! and untouched by this build — PLX-105's three wire tests still pin it — and
//! the ACP path never enters it. **c3 is discharged on the path this ticket
//! creates and is NOT discharged tree-wide.** PLX-105 remains the owner.

pub mod agent;
pub mod projection;

pub use agent::{ClaudeCodeAcpAgent, ACP_NAMESPACE};
pub use projection::ChatEventProjection;
