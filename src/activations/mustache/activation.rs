//! Mustache activation - template rendering for handle values
//!
//! This activation provides mustache template rendering as a core plugin.
//! Other plugins can register their default templates and use this to render
//! handle values consistently.

use super::storage::{MustacheStorage, MustacheStorageConfig};
use super::types::{MustacheError, TemplateInfo};
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
    ) -> Result<String, MustacheError> {
        let name = template_name.unwrap_or_else(|| "default".to_string());

        // Look up the template
        let template_str = self
            .storage
            .get_template(&plugin_id, &method, &name)
            .await
            .map_err(|e| MustacheError::StorageError(format!("Storage error: {e}")))?
            .ok_or_else(|| {
                MustacheError::TemplateNotFound(format!(
                    "plugin={plugin_id}, method={method}, name={name}"
                ))
            })?;

        // Compile and render the template
        let template = mustache::compile_str(&template_str)
            .map_err(|e| MustacheError::InvalidTemplate(format!("Template compile error: {e}")))?;
        let mut output = Vec::new();
        template
            .render(&mut output, &value)
            .map_err(|e| MustacheError::RenderError(format!("Template render error: {e}")))?;
        String::from_utf8(output)
            .map_err(|e| MustacheError::RenderError(format!("UTF-8 conversion error: {e}")))
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
    ) -> Result<TemplateInfo, MustacheError> {
        // Validate the template compiles
        mustache::compile_str(&template)
            .map_err(|e| MustacheError::InvalidTemplate(format!("Invalid template: {e}")))?;

        self.storage
            .set_template(&plugin_id, &method, &name, &template)
            .await
            .map_err(|e| {
                MustacheError::StorageError(format!("Failed to register template: {e}"))
            })
    }

    /// List all templates for a plugin
    #[plexus_macros::method(description = "List all templates registered for a plugin",
    params(plugin_id = "UUID of the plugin to list templates for"))]
    async fn list_templates(
        &self,
        plugin_id: Uuid,
    ) -> Result<Vec<TemplateInfo>, MustacheError> {
        self.storage
            .list_templates(&plugin_id)
            .await
            .map_err(|e| MustacheError::StorageError(format!("Failed to list templates: {e}")))
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
    ) -> Result<String, MustacheError> {
        self.storage
            .get_template(&plugin_id, &method, &name)
            .await
            .map_err(|e| MustacheError::StorageError(format!("Failed to get template: {e}")))?
            .ok_or_else(|| {
                MustacheError::TemplateNotFound(format!(
                    "plugin={plugin_id}, method={method}, name={name}"
                ))
            })
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
    ) -> Result<usize, MustacheError> {
        self.storage
            .delete_template(&plugin_id, &method, &name)
            .await
            .map(usize::from)
            .map_err(|e| MustacheError::StorageError(format!("Failed to delete template: {e}")))
    }
}
