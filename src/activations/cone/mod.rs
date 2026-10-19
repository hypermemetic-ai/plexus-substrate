mod activation;
mod methods;
mod storage;
mod types;

#[cfg(test)]
mod tests;

pub use activation::{Cone, ConeChild, ConeChildMethod, ConeMethod};
pub use methods::ConeIdentifier;
pub use storage::{ConeStorage, ConeStorageConfig};
pub use types::{
    // Method-specific return types (preferred)
    ChatEvent, CreateResult, DeleteResult, GetResult, ListResult,
    RegistryResult, ResolveResult, SetHeadResult,
    // Shared types
    ChatUsage, ConeConfig, ConeError, ConeId, ConeInfo,
    Message, MessageId, MessageRole, Position,
    // Handle types
    ConeHandle,
};
