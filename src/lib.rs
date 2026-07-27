// PLX-140 (ACP·F) — claudecode as the reference ACP agent. See `acp`'s module
// docs for exactly what criterion c3 does and does not close.
pub mod acp;
pub mod activations;
pub mod builder;
pub mod mcp_bridge;
pub mod mcp_session;
pub mod plexus;
pub mod plugin_system;
// PLX-151 (M4·H2) — where the sealed TenantRecord is resolved and `is_active()`
// is checked before a confinement's tenant root is built.
pub mod tenancy;
pub mod types;

// Re-export serde helpers for macro-generated code
// This allows the hub_methods macro to reference serde helpers via crate::serde_helpers
pub use plexus_core::serde_helpers;

// Re-export commonly used items
pub use builder::build_plexus_rpc;
// PLX-127 (M4·C) — the two compositions and the exclusion list between them.
pub use builder::{
    build_activations, compose_host_hub, compose_tenant_hub, SubstrateActivations, TenantSurface,
    TENANT_EXCLUDED_ACTIVATIONS,
};
// PLX-128/PLX-129 (M4·D/M4·E) — per-tenant instances over per-tenant storage.
pub use activations::claudecode::sessions::SessionRoot;
pub use activations::storage::StorageScope;
pub use builder::{build_activations_in, build_plexus_rpc_with_admission};
pub use tenancy::{SubstrateTenantStorage, TenantStorageRoot};
// PLX-151 (M4·H2) — the confined composition and the deployment that owns it.
pub use builder::{build_plexus_rpc_with_tenancy, compose_tenant_hub_confined, TenantExecution};
// PLX-151 (M4·H2) — the confinement and the admission that gates it.
pub use activations::claudecode::Confinement;
pub use tenancy::{AdmissionRefused, TenantAdmission};
// PLX-140 (ACP·F) — the reference ACP agent and its projection.
pub use acp::{ChatEventProjection, ClaudeCodeAcpAgent, ACP_NAMESPACE};
pub use mcp_bridge::PlexusMcpBridge;
pub use mcp_session::{SqliteSessionManager, SqliteSessionConfig};
pub use types::{Envelope, Handle, Origin};
