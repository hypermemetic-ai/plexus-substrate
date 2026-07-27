use super::executor::BashExecutor;
use super::types::BashEvent;
use futures::Stream;

/// Bash activation - execute shell commands and stream output
#[derive(Clone)]
pub struct Bash {
    executor: BashExecutor,
}

impl Bash {
    pub const fn new() -> Self {
        Self {
            executor: BashExecutor::new(),
        }
    }

    /// Register default templates with the mustache plugin
    ///
    /// Call this during initialization to register Bash's default templates
    /// for rendering command output.
    pub async fn register_default_templates(
        &self,
        mustache: &crate::activations::mustache::Mustache,
    ) -> Result<(), String> {
        let plugin_id = Self::PLUGIN_ID;

        mustache.register_templates(plugin_id, &[
            // Execute method - command output
            ("execute", "default", "{{#stdout}}{{stdout}}{{/stdout}}{{#stderr}}\n[stderr] {{stderr}}{{/stderr}}{{#exit_code}}\n[exit: {{exit_code}}]{{/exit_code}}"),
            ("execute", "compact", "{{stdout}}"),
            ("execute", "verbose", "$ {{command}}\n{{stdout}}{{#stderr}}\n--- stderr ---\n{{stderr}}{{/stderr}}\n[exit code: {{exit_code}}]"),
        ]).await
    }
}

impl Default for Bash {
    fn default() -> Self {
        Self::new()
    }
}

#[plexus_macros::activation(namespace = "bash",
version = "1.0.0",
description = "Execute bash commands and stream output")]
impl Bash {
    /// Execute a bash command and stream stdout, stderr, and exit code
    ///
    /// PLX-133 item 6: `streaming` is declared. This method genuinely multi-shots
    /// — the point of it is incremental stdout/stderr — and PLX-109 §2b's "the
    /// declared flag is 100% precise in substrate" was a claim about precision,
    /// not recall: this method and `lattice.execute` streamed without declaring
    /// it. **Wire consequence**: over the HTTP gateway `bash.execute` now answers
    /// `text/event-stream` instead of one buffered `application/json` body, so a
    /// long-running command's output arrives as it is produced instead of after
    /// the process exits. The MCP gateway does not read this flag.
    #[plexus_macros::method(streaming)]
    async fn execute(&self, command: String) -> impl Stream<Item = BashEvent> + Send + 'static {
        self.executor.execute(&command).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plexus::Activation;

    #[test]
    fn test_bash_activation_trait() {
        let bash = Bash::new();
        assert_eq!(bash.namespace(), "bash");
        assert_eq!(bash.version(), "1.0.0");
        assert!(bash.methods().contains(&"execute"));
    }

    #[test]
    fn test_bash_method_help() {
        let bash = Bash::new();
        let help = bash.method_help("execute");
        assert!(help.is_some());
        assert!(help.unwrap().contains("Execute"));
    }

    #[test]
    fn test_bash_namespace_constant() {
        assert_eq!(Bash::NAMESPACE, "bash");
    }

    #[test]
    fn test_generated_method_enum() {
        let names = BashMethod::all_method_names();
        assert!(names.contains(&"execute"));
    }

    #[test]
    fn test_plugin_schema_with_return_types() {
        use crate::plexus::Activation;
        let bash = Bash::new();
        let schema = bash.plugin_schema();

        assert_eq!(schema.namespace, "bash");
        assert_eq!(schema.version, "1.0.0");
        // 2 methods: user-defined "execute" + auto-generated "schema"
        assert_eq!(schema.methods.len(), 2);
        assert!(schema.is_leaf(), "bash should be a leaf plugin");

        let execute = schema.methods.iter().find(|m| m.name == "execute")
            .expect("should have an 'execute' method");
        assert!(execute.params.is_some(), "should have params schema");
        assert!(execute.returns.is_some(), "should have returns schema");

        // Print the plugin schema
        let json = serde_json::to_string_pretty(&schema).unwrap();
        println!("Bash plugin_schema():\n{json}");

        // Verify returns includes BashEvent variants
        let returns_json = serde_json::to_string(&execute.returns).unwrap();
        assert!(returns_json.contains("stdout") || returns_json.contains("Stdout"));
        assert!(returns_json.contains("stderr") || returns_json.contains("Stderr"));
        assert!(returns_json.contains("exit") || returns_json.contains("Exit"));
    }
}
