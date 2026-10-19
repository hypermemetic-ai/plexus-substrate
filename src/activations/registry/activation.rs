// PLEXUS-6: the 0.6 macros' legacy form (MACRO-13); its deprecation note is allowed here.
#![allow(deprecated)]
use super::storage::RegistryStorage;
use super::types::{BackendSource, RegistryEvent, RegistryStorageConfig};
use async_stream::stream;
use futures::Stream;
use plexus_macros::activation;
use std::sync::Arc;

/// Registry activation - backend discovery and registration service
#[derive(Clone)]
pub struct Registry {
    storage: Arc<RegistryStorage>,
}

impl Registry {
    /// Create a new Registry instance with the given configuration
    pub async fn new(config: RegistryStorageConfig) -> Result<Self, String> {
        let storage = RegistryStorage::new(config).await?;

        // Load backends from config file
        storage.load_config().await?;

        Ok(Self {
            storage: Arc::new(storage),
        })
    }

    /// Create with default configuration
    pub async fn with_defaults() -> Result<Self, String> {
        Self::new(RegistryStorageConfig::default()).await
    }

    /// Set the local backend name (called by the host)
    pub fn set_local_backend(&self, _name: String, _description: Option<String>) {
        // Store in memory - this is the backend hosting this registry instance
        // We can't get this from the DB since the registry doesn't know who's hosting it
        // TODO: implement local backend tracking
    }
}

#[activation(
    namespace = "registry",
    version = "1.0.0",
    description = "Backend discovery and registration service for Plexus hubs",
    crate_path = "plexus_core"
)]
impl Registry {
    /// Register a new backend
    #[plexus_macros::method(description = "Register a new Plexus backend for discovery")]
    async fn register(
        &self,
        name: String,
        host: String,
        port: u16,
        protocol: Option<String>,
        description: Option<String>,
        namespace: Option<String>,
    ) -> impl Stream<Item = RegistryEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            let protocol = protocol.unwrap_or_else(|| "ws".to_string());

            match storage
                .register(
                    name,
                    host,
                    port,
                    protocol,
                    description,
                    namespace,
                    BackendSource::Manual,
                )
                .await
            {
                Ok(backend) => {
                    yield RegistryEvent::BackendRegistered { backend };
                }
                Err(e) => {
                    tracing::error!("Failed to register backend: {}", e);
                    yield RegistryEvent::Error { message: e };
                }
            }
        }
    }

    /// List all registered backends
    #[plexus_macros::method(description = "List all registered backends")]
    async fn list(
        &self,
        active_only: Option<bool>,
    ) -> impl Stream<Item = RegistryEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            let active_only = active_only.unwrap_or(true);

            match storage.list(active_only).await {
                Ok(backends) => {
                    yield RegistryEvent::Backends { backends };
                }
                Err(e) => {
                    tracing::error!("Failed to list backends: {}", e);
                    yield RegistryEvent::Error { message: e };
                }
            }
        }
    }

    /// Get a specific backend by name
    #[plexus_macros::method(description = "Get information about a specific backend by name")]
    async fn get(
        &self,
        name: String,
    ) -> impl Stream<Item = RegistryEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            match storage.get(&name).await {
                Ok(backend) => {
                    yield RegistryEvent::Backend { backend };
                }
                Err(e) => {
                    tracing::error!("Failed to get backend {}: {}", name, e);
                    yield RegistryEvent::Error { message: e };
                }
            }
        }
    }

    /// Update an existing backend
    #[plexus_macros::method(description = "Update an existing backend's connection information")]
    async fn update(
        &self,
        name: String,
        host: Option<String>,
        port: Option<u16>,
        protocol: Option<String>,
        description: Option<String>,
        namespace: Option<String>,
    ) -> impl Stream<Item = RegistryEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            match storage.update(&name, host, port, protocol, description, namespace).await {
                Ok(Some(backend)) => {
                    yield RegistryEvent::BackendUpdated { backend };
                }
                Ok(None) => {
                    yield RegistryEvent::Error {
                        message: format!("Backend not found: {}", name),
                    };
                }
                Err(e) => {
                    tracing::error!("Failed to update backend {}: {}", name, e);
                    yield RegistryEvent::Error { message: e };
                }
            }
        }
    }

    /// Delete a backend
    #[plexus_macros::method(description = "Remove a backend from the registry")]
    async fn delete(
        &self,
        name: String,
    ) -> impl Stream<Item = RegistryEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            match storage.delete(&name).await {
                Ok(true) => {
                    yield RegistryEvent::BackendDeleted { name };
                }
                Ok(false) => {
                    yield RegistryEvent::Error {
                        message: format!("Backend not found: {}", name),
                    };
                }
                Err(e) => {
                    tracing::error!("Failed to delete backend {}: {}", name, e);
                    yield RegistryEvent::Error { message: e };
                }
            }
        }
    }

    /// Ping a backend to update its last_seen timestamp
    #[plexus_macros::method(description = "Update the health check timestamp for a backend")]
    async fn ping(
        &self,
        name: String,
    ) -> impl Stream<Item = RegistryEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            match storage.ping(&name).await {
                Ok(true) => {
                    yield RegistryEvent::Ping {
                        name,
                        success: true,
                        message: "Backend health check updated".to_string(),
                    };
                }
                Ok(false) => {
                    yield RegistryEvent::Ping {
                        name: name.clone(),
                        success: false,
                        message: format!("Backend not found: {}", name),
                    };
                }
                Err(e) => {
                    tracing::error!("Failed to ping backend {}: {}", name, e);
                    yield RegistryEvent::Error { message: e };
                }
            }
        }
    }

    /// Reload the configuration file
    #[plexus_macros::method(description = "Reload backends from the configuration file")]
    async fn reload(&self) -> impl Stream<Item = RegistryEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            match storage.reload_config().await {
                Ok(count) => {
                    yield RegistryEvent::Reloaded { count };
                }
                Err(e) => {
                    tracing::error!("Failed to reload config: {}", e);
                    yield RegistryEvent::Error { message: e };
                }
            }
        }
    }
}
