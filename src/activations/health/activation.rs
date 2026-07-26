use super::methods::HealthMethod;
use super::types::HealthEvent;
use crate::plexus::{wrap_stream, PlexusError, PlexusStream, Activation, PlexusStreamItem, StreamMetadata, PlexusContext, MethodSchema, PluginSchema, SchemaResult};
use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use jsonrpsee::core::{server::Methods, SubscriptionResult};
use jsonrpsee::{proc_macros::rpc, PendingSubscriptionSink};
use serde_json::Value;
use std::pin::Pin;
use std::time::Instant;

/// Health RPC interface
#[rpc(server, namespace = "health")]
pub trait HealthRpc {
    /// Check health status (streaming subscription)
    #[subscription(name = "check", unsubscribe = "unsubscribe_check", item = serde_json::Value)]
    async fn check(&self) -> SubscriptionResult;
}

/// Health activation - minimal reference implementation
///
/// This activation demonstrates the caller-wraps architecture.
/// The `check_stream` method returns typed domain events (`HealthEvent`),
/// and the `call` method wraps them using `wrap_stream`.
#[derive(Clone)]
pub struct Health {
    start_time: Instant,
}

impl Health {
    /// Namespace for the health plugin
    pub const NAMESPACE: &'static str = "health";
    /// Version of the health plugin
    pub const VERSION: &'static str = "1.0.0";
    /// Stable plugin instance ID for handle routing (same formula as `hub_methods` macro)
    /// Generated from "health@1" (namespace + major version)
    pub const PLUGIN_ID: uuid::Uuid = uuid::uuid!("dc560257-b7c5-575b-b893-b448c87ca797");

    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
        }
    }

    /// Returns typed stream - caller will wrap with metadata
    fn check_stream(
        &self,
    ) -> Pin<Box<dyn Stream<Item = HealthEvent> + Send + 'static>> {
        let uptime = self.start_time.elapsed().as_secs();

        Box::pin(stream! {
            yield HealthEvent::Status {
                status: "healthy".to_string(),
                uptime_seconds: uptime,
                timestamp: chrono::Utc::now().timestamp(),
            };
        })
    }
}

impl Default for Health {
    fn default() -> Self {
        Self::new()
    }
}

/// RPC server implementation
#[async_trait]
impl HealthRpcServer for Health {
    async fn check(&self, pending: PendingSubscriptionSink) -> SubscriptionResult {
        let sink = pending.accept().await?;

        // Get wrapped stream
        let stream = self.check_stream();
        let wrapped = wrap_stream(stream, "health.status", vec!["health".into()]);

        // Forward all items to sink
        tokio::spawn(async move {
            let mut stream = wrapped;
            while let Some(item) = stream.next().await {
                if let Ok(raw_value) = serde_json::value::to_raw_value(&item) {
                    if sink.send(raw_value).await.is_err() {
                        break;
                    }
                }
            }
            // Send done event
            let done = PlexusStreamItem::Done {
                metadata: StreamMetadata::new(vec!["health".into()], PlexusContext::hash()),
            };
            if let Ok(raw_value) = serde_json::value::to_raw_value(&done) {
                let _ = sink.send(raw_value).await;
            }
        });

        Ok(())
    }
}

/// Activation trait implementation - unified interface for Plexus
#[async_trait]
impl Activation for Health {
    type Methods = HealthMethod;

    fn namespace(&self) -> &'static str {
        "health"
    }

    fn version(&self) -> &'static str {
        "1.0.0"
    }

    fn plugin_id(&self) -> uuid::Uuid {
        Self::PLUGIN_ID
    }

    fn description(&self) -> &'static str {
        "Check hub health and uptime"
    }

    fn methods(&self) -> Vec<&str> {
        vec!["check", "schema"]
    }

    fn method_help(&self, method: &str) -> Option<String> {
        match method {
            "schema" => Some("Get plugin or method schema. Pass {\"method\": \"name\"} for a specific method.".to_string()),
            _ => HealthMethod::description(method).map(std::string::ToString::to_string),
        }
    }

    async fn call(&self, method: &str, _params: Value, _auth: Option<&plexus_core::plexus::AuthContext>, _raw_ctx: Option<&plexus_core::request::RawRequestContext>) -> Result<PlexusStream, PlexusError> { match method {
        "check" => {
            let stream = self.check_stream();
            Ok(wrap_stream(stream, "health.status", vec!["health".into()]))
        }
        "schema" => {
            use crate::plexus::SchemaResult;
    
            // PROT schema unification (PLX-13): `.schema` always yields the single
            // unified PluginSchema; no `method`-param per-method branch.
            let result = SchemaResult::Plugin(self.plugin_schema());

            Ok(wrap_stream(
                futures::stream::once(async move { result }),
                "health.schema",
                vec!["health".into()]
            ))
        }
        _ => {
            // PROT schema unification (PLX-13): the `{method}.schema` strip-suffix
            // shortcut (and its `.method_schema` content-type) is deleted; an
            // unknown method is method-not-found.
            Err(PlexusError::MethodNotFound {
                activation: "health".to_string(),
                method: method.to_string(),
            })
        }
    } }

    fn into_rpc_methods(self) -> Methods {
        // Register RPC subscription methods
        self.into_rpc().into()
    }

    fn plugin_schema(&self) -> PluginSchema {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // check method
        let check_desc = "Check the health status of the hub and return uptime";
        let mut hasher = DefaultHasher::new();
        "check".hash(&mut hasher);
        check_desc.hash(&mut hasher);
        let check_hash = format!("{:016x}", hasher.finish());

        // schema method
        let schema_desc = "Get plugin or method schema. Pass {\"method\": \"name\"} for a specific method.";
        let mut hasher = DefaultHasher::new();
        "schema".hash(&mut hasher);
        schema_desc.hash(&mut hasher);
        let schema_hash = format!("{:016x}", hasher.finish());

        let methods = vec![
            MethodSchema::new("check", check_desc, check_hash)
                .with_returns(schemars::schema_for!(HealthEvent)),
            MethodSchema::new("schema", schema_desc, schema_hash)
                .with_returns(schemars::schema_for!(SchemaResult)),
        ];

        PluginSchema::leaf(self.namespace(), self.version(), self.description(), methods)
    }
}

#[async_trait]
impl plexus_core::plexus::ChildRouter for Health {
    fn router_namespace(&self) -> &'static str {
        "health"
    }

    async fn router_call(&self, method: &str, params: Value, auth: Option<&plexus_core::plexus::AuthContext>, raw_ctx: Option<&plexus_core::request::RawRequestContext>) -> Result<PlexusStream, PlexusError> {
        Activation::call(self, method, params, auth, raw_ctx).await
    }

    async fn get_child(&self, _name: &str) -> Option<Box<dyn plexus_core::plexus::ChildRouter>> {
        None
    }
}
