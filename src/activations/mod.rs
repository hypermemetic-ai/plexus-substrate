// Common storage utilities for activations
pub mod storage;

// Chaos provides fault injection and observability for anti-fragility testing.
// Gated behind the `chaos` feature — opt-in because it pulls in libc + unsafe
// signal primitives. Default builds omit this module entirely.
#[cfg(feature = "chaos")]
pub mod chaos;

// Health is the reference implementation for the new architecture (manual impl)
pub mod health;

// Echo demonstrates plexus-macros usage with the new architecture
pub mod echo;

// Solar demonstrates nested plugin hierarchy (plugins with children)
pub mod solar;

// Arbor manages conversation trees
pub mod arbor;

// Bash executes shell commands
pub mod bash;

// Cone orchestrates LLM conversations with Arbor context
pub mod cone;

// ClaudeCode manages Claude Code sessions with Arbor-backed history
pub mod claudecode;

// Mustache provides template rendering for handle values
pub mod mustache;

// ClaudeCode Loopback routes tool permissions back to parent for approval
pub mod claudecode_loopback;

// Orcha orchestrates Claude sub-agents with approval loops and validation
pub mod orcha;

// Interactive demonstrates bidirectional communication patterns
pub mod interactive;

// Lattice is a DAG execution engine for multi-agent orchestration
pub mod lattice;

// Changelog tracks plexus hash transitions and planned changes
pub mod changelog;

// PLX-142: hands each activation's macro-built ActivationIr to the hub, so
// `substrate.connectome` can serve a CONNECTOME RFC 002 document
pub mod connectome;
