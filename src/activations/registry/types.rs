use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Source of a backend registration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BackendSource {
    /// Automatically registered (self-registration at startup)
    Auto,
    /// Loaded from config file
    File,
    /// Manually registered via RPC
    Manual,
    /// Loaded from environment variable
    Env,
}

impl BackendSource {
    pub fn as_str(&self) -> &str {
        match self {
            BackendSource::Auto => "auto",
            BackendSource::File => "file",
            BackendSource::Manual => "manual",
            BackendSource::Env => "env",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "auto" => Some(BackendSource::Auto),
            "file" => Some(BackendSource::File),
            "manual" => Some(BackendSource::Manual),
            "env" => Some(BackendSource::Env),
            _ => None,
        }
    }
}

/// Backend connection information
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BackendInfo {
    /// Unique identifier (UUID)
    pub id: String,
    /// Human-readable name (unique)
    pub name: String,
    /// Host address
    pub host: String,
    /// Port number
    pub port: u16,
    /// Protocol (ws or wss)
    pub protocol: String,
    /// Optional description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional routing namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Plexus version (from hash)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// JSON metadata (extensibility)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    /// Source of registration
    pub source: BackendSource,
    /// Whether this backend is active
    pub is_active: bool,
    /// Timestamp when registered (Unix seconds)
    pub registered_at: i64,
    /// Timestamp when last seen (Unix seconds, for health checks)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<i64>,
    /// Timestamp when created (Unix seconds)
    pub created_at: i64,
    /// Timestamp when last updated (Unix seconds)
    pub updated_at: i64,
}

impl BackendInfo {
    /// Build WebSocket URL from backend info
    pub fn url(&self) -> String {
        format!("{}://{}:{}", self.protocol, self.host, self.port)
    }
}

/// Configuration for a backend from TOML file
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BackendConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    #[serde(default = "default_protocol")]
    pub protocol: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub namespace: Option<String>,
}

fn default_protocol() -> String {
    "ws".to_string()
}

/// Registry configuration from TOML file
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RegistryConfig {
    #[serde(default)]
    pub backend: Vec<BackendConfig>,
}

/// Events emitted by registry methods
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum RegistryEvent {
    /// Backend was registered
    #[serde(rename = "backend_registered")]
    BackendRegistered { backend: BackendInfo },

    /// Backend was updated
    #[serde(rename = "backend_updated")]
    BackendUpdated { backend: BackendInfo },

    /// Backend was deleted
    #[serde(rename = "backend_deleted")]
    BackendDeleted { name: String },

    /// List of backends
    #[serde(rename = "backends")]
    Backends { backends: Vec<BackendInfo> },

    /// Single backend info
    #[serde(rename = "backend")]
    Backend { backend: Option<BackendInfo> },

    /// Ping response
    #[serde(rename = "ping")]
    Ping {
        name: String,
        success: bool,
        message: String,
    },

    /// Config reloaded
    #[serde(rename = "reloaded")]
    Reloaded { count: usize },

    /// Error occurred
    #[serde(rename = "error")]
    Error { message: String },
}

/// Configuration for Registry storage
#[derive(Debug, Clone)]
pub struct RegistryStorageConfig {
    /// Path to SQLite database
    pub db_path: std::path::PathBuf,
    /// Optional path to TOML config file
    pub config_path: Option<std::path::PathBuf>,
}

impl Default for RegistryStorageConfig {
    fn default() -> Self {
        // PLEXUS-6: a throwaway substrate's --data-dir holds its registry too.
        let config_dir = crate::activations::storage::data_dir().map_or_else(
            || dirs::config_dir().unwrap_or_else(|| std::path::PathBuf::from(".")).join("plexus"),
            |d| d.join("registry"),
        );

        Self {
            db_path: config_dir.join("registry.db"),
            config_path: Some(config_dir.join("backends.toml")),
        }
    }
}
