mod methods;
mod activation;
mod storage;
mod types;
mod views;

#[cfg(test)]
mod tests;

pub use activation::{Arbor, ArborMethod, HandleResolver, HandleResolvers};
// Keep methods module for any helper types if needed
pub use storage::{ArborConfig, ArborStorage};
pub use types::{
    ArborError, ArborEvent, ArborId, Node, NodeId, NodeType, ResourceRefs, ResourceState, Tree,
    TreeId, TreeSkeleton,
};
pub use views::{
    CollapseType, RangeContent, RangeHandle, RangeSpec, ResolveMode, TextRun,
};

// Re-export Handle from crate::types for consistency
pub use crate::types::Handle;
