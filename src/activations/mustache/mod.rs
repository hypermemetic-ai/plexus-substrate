//! Mustache activation module
//!
//! Provides mustache template rendering as a core plugin for the handle
//! resolution and rendering system. Other plugins can register their
//! default templates and use this plugin to render handle values.

mod activation;
mod storage;
mod types;

pub use activation::Mustache;
pub use storage::{MustacheStorage, MustacheStorageConfig};
pub use types::{MustacheError, TemplateInfo};
