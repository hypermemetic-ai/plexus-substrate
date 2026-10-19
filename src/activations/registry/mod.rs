//! Registry — backend discovery. PLEXUS-6: vendored from `plexus-registry`
//! 0.1 (`../plexus-registry` @ a28b6d3), which is on the plexus 0.5 line and
//! cannot be registered on a 0.6 hub; the source is its own, adapted only
//! for the 0.6 macros.

mod activation;
mod storage;
mod types;

pub use activation::Registry;
pub use types::{BackendInfo, BackendSource, RegistryEvent, RegistryStorageConfig};
