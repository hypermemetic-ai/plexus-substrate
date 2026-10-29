pub mod activations;
pub mod builder;
pub mod login;
pub mod plexus;
pub mod plugin_system;
pub mod types;

// Re-export serde helpers for macro-generated code
// This allows the hub_methods macro to reference serde helpers via crate::serde_helpers
pub use plexus_core::serde_helpers;

// Re-export commonly used items
pub use builder::{build_plexus_rpc, build_plexus_rpc_with, BuildOptions};
pub use types::{Envelope, Handle, Origin};
