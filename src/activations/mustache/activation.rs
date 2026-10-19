//! Mustache activation - template rendering for handle values
//!
//! This activation provides mustache template rendering as a core plugin.
//! Other plugins can register their default templates and use this to render
//! handle values consistently.
// PLEXUS-6: the 0.6 macros' legacy form (MACRO-13) — these activations keep
// `namespace = …` and their own type names; the name rule's trait would need
// one event type per activation. Its deprecation note is allowed here.
#![allow(deprecated)]

use super::storage::{MustacheStorage, MustacheStorageConfig};
use super::types::MustacheEvent;
use async_stream::stream;
use futures::Stream;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

/// Mustache activation - renders values using mustache templates
pub struct Mustache {
    storage: Arc<MustacheStorage>,
}

impl Mustache {
    /// Create a new Mustache activation with the given storage config
    pub async fn new(config: MustacheStorageConfig) -> Result<Self, String> {
        let storage = MustacheStorage::new(config)
            .await
            .map_err(|e| format!("Failed to initialize mustache storage: {e}"))?;

        Ok(Self {
            storage: Arc::new(storage),
        })
    }

    /// Create with default configuration
    pub async fn with_defaults() -> Result<Self, String> {
        Self::new(MustacheStorageConfig::default()).await
    }

    /// Get access to the underlying storage
    pub fn storage(&self) -> &MustacheStorage {
        &self.storage
    }

    /// Register a template directly (non-streaming, for initialization)
    ///
    /// Use this method during plugin initialization to register default templates.
    /// Unlike the RPC `register_template` method, this doesn't return a stream.
    pub async fn register_template_direct(
        &self,
        plugin_id: Uuid,
        method: &str,
        name: &str,
        template: &str,
    ) -> Result<(), String> {
        // Validate the template compiles
        if let Err(e) = mustache::compile_str(template) {
            return Err(format!("Invalid template: {e}"));
        }

        self.storage
            .set_template(&plugin_id, method, name, template)
            .await
            .map_err(|e| format!("Failed to register template: {e}"))?;

        Ok(())
    }

    /// Register multiple templates for a plugin
    ///
    /// Convenience method for registering multiple templates at once.
    /// Takes a slice of (method, name, template) tuples.
    pub async fn register_templates(
        &self,
        plugin_id: Uuid,
        templates: &[(&str, &str, &str)],
    ) -> Result<(), String> {
        for (method, name, template) in templates {
            self.register_template_direct(plugin_id, method, name, template).await?;
        }
        Ok(())
    }
}

impl Clone for Mustache {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
        }
    }
}

/// Hub-macro generates all the boilerplate for this impl block
#[plexus_macros::activation(namespace = "mustache",
version = "1.0.0",
description = "Mustache template rendering for handle values")]
impl Mustache {
    /// Render a value using a template
    ///
    /// Looks up the template for the given plugin/method/name combination
    /// and renders the value using mustache templating. If `template_name`
    /// is None, uses "default".
    #[plexus_macros::method(description = "Render a value using a registered mustache template",
    params(
        plugin_id = "UUID of the plugin that owns the template",
        method = "Method name the template is for",
        template_name = "Template name (defaults to 'default' if not specified)",
        value = "JSON value to render with the template"
    ))]
    async fn render(
        &self,
        plugin_id: Uuid,
        method: String,
        template_name: Option<String>,
        value: Value,
    ) -> impl Stream<Item = MustacheEvent> + Send + 'static {
        let storage = Arc::clone(&self.storage);
        let name = template_name.unwrap_or_else(|| "default".to_string());

        stream! {
            // Look up the template
            match storage.get_template(&plugin_id, &method, &name).await {
                Ok(Some(template_str)) => {
                    // Compile and render the template
                    match mustache::compile_str(&template_str) {
                        Ok(template) => {
                            let mut output = Vec::new();
                            match template.render(&mut output, &value) {
                                Ok(()) => {
                                    match String::from_utf8(output) {
                                        Ok(rendered) => {
                                            yield MustacheEvent::Rendered { output: rendered };
                                        }
                                        Err(e) => {
                                            yield MustacheEvent::Error {
                                                message: format!("UTF-8 conversion error: {e}"),
                                            };
                                        }
                                    }
                                }
                                Err(e) => {
                                    yield MustacheEvent::Error {
                                        message: format!("Template render error: {e}"),
                                    };
                                }
                            }
                        }
                        Err(e) => {
                            yield MustacheEvent::Error {
                                message: format!("Template compile error: {e}"),
                            };
                        }
                    }
                }
                Ok(None) => {
                    yield MustacheEvent::NotFound {
                        message: format!(
                            "Template not found: plugin={plugin_id}, method={method}, name={name}"
                        ),
                    };
                }
                Err(e) => {
                    yield MustacheEvent::Error {
                        message: format!("Storage error: {e}"),
                    };
                }
            }
        }
    }

    /// Register a template for a plugin/method
    ///
    /// Templates are identified by (`plugin_id`, method, name). If a template
    /// with the same identifier already exists, it will be updated.
    #[plexus_macros::method(description = "Register a mustache template for a plugin method",
    params(
        plugin_id = "UUID of the plugin registering the template",
        method = "Method name this template is for",
        name = "Template name (e.g., 'default', 'compact', 'verbose')",
        template = "Mustache template content"
    ))]
    async fn register_template(
        &self,
        plugin_id: Uuid,
        method: String,
        name: String,
        template: String,
    ) -> impl Stream<Item = MustacheEvent> + Send + 'static {
        let storage = Arc::clone(&self.storage);

        stream! {
            // Validate the template compiles
            if let Err(e) = mustache::compile_str(&template) {
                yield MustacheEvent::Error {
                    message: format!("Invalid template: {e}"),
                };
                return;
            }

            match storage.set_template(&plugin_id, &method, &name, &template).await {
                Ok(info) => {
                    yield MustacheEvent::Registered { template: info };
                }
                Err(e) => {
                    yield MustacheEvent::Error {
                        message: format!("Failed to register template: {e}"),
                    };
                }
            }
        }
    }

    /// List all templates for a plugin
    #[plexus_macros::method(description = "List all templates registered for a plugin",
    params(plugin_id = "UUID of the plugin to list templates for"))]
    async fn list_templates(
        &self,
        plugin_id: Uuid,
    ) -> impl Stream<Item = MustacheEvent> + Send + 'static {
        let storage = Arc::clone(&self.storage);

        stream! {
            match storage.list_templates(&plugin_id).await {
                Ok(templates) => {
                    yield MustacheEvent::Templates { templates };
                }
                Err(e) => {
                    yield MustacheEvent::Error {
                        message: format!("Failed to list templates: {e}"),
                    };
                }
            }
        }
    }

    /// Get a specific template
    #[plexus_macros::method(description = "Get a specific template by plugin, method, and name",
    params(
        plugin_id = "UUID of the plugin that owns the template",
        method = "Method name the template is for",
        name = "Template name"
    ))]
    async fn get_template(
        &self,
        plugin_id: Uuid,
        method: String,
        name: String,
    ) -> impl Stream<Item = MustacheEvent> + Send + 'static {
        let storage = Arc::clone(&self.storage);

        stream! {
            match storage.get_template(&plugin_id, &method, &name).await {
                Ok(Some(template)) => {
                    yield MustacheEvent::Template { template };
                }
                Ok(None) => {
                    yield MustacheEvent::NotFound {
                        message: format!(
                            "Template not found: plugin={plugin_id}, method={method}, name={name}"
                        ),
                    };
                }
                Err(e) => {
                    yield MustacheEvent::Error {
                        message: format!("Failed to get template: {e}"),
                    };
                }
            }
        }
    }

    /// Delete a template
    #[plexus_macros::method(description = "Delete a specific template",
    params(
        plugin_id = "UUID of the plugin that owns the template",
        method = "Method name the template is for",
        name = "Template name"
    ))]
    async fn delete_template(
        &self,
        plugin_id: Uuid,
        method: String,
        name: String,
    ) -> impl Stream<Item = MustacheEvent> + Send + 'static {
        let storage = Arc::clone(&self.storage);

        stream! {
            match storage.delete_template(&plugin_id, &method, &name).await {
                Ok(deleted) => {
                    yield MustacheEvent::Deleted {
                        count: usize::from(deleted),
                    };
                }
                Err(e) => {
                    yield MustacheEvent::Error {
                        message: format!("Failed to delete template: {e}"),
                    };
                }
            }
        }
    }
}
