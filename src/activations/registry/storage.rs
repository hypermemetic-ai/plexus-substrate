use super::types::{BackendConfig, BackendInfo, BackendSource, RegistryConfig, RegistryStorageConfig};
use sqlx::{sqlite::{SqliteConnectOptions, SqlitePool}, ConnectOptions, Row};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Storage layer for backend registry
pub(crate) struct RegistryStorage {
    pool: SqlitePool,
    config_path: Option<std::path::PathBuf>,
}

impl RegistryStorage {
    /// Create a new registry storage instance
    pub(crate) async fn new(config: RegistryStorageConfig) -> Result<Self, String> {
        // Ensure config directory exists
        if let Some(parent) = config.db_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create config directory: {}", e))?;
        }

        // Initialize database
        let db_url = format!("sqlite:{}?mode=rwc", config.db_path.display());
        let connect_options: SqliteConnectOptions = db_url
            .parse()
            .map_err(|e| format!("Failed to parse database URL: {}", e))?;
        let connect_options = connect_options.disable_statement_logging();

        let pool = SqlitePool::connect_with(connect_options)
            .await
            .map_err(|e| format!("Failed to connect to registry database: {}", e))?;

        let storage = Self {
            pool,
            config_path: config.config_path,
        };

        storage.run_migrations().await?;
        Ok(storage)
    }

    /// Run database migrations
    async fn run_migrations(&self) -> Result<(), String> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS backends (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                protocol TEXT NOT NULL,
                description TEXT,
                namespace TEXT,
                version TEXT,
                metadata TEXT,
                source TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                registered_at INTEGER NOT NULL,
                last_seen INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_backends_name ON backends(name);
            CREATE INDEX IF NOT EXISTS idx_backends_active ON backends(is_active);
            CREATE INDEX IF NOT EXISTS idx_backends_source ON backends(source);
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| format!("Failed to run migrations: {}", e))?;

        Ok(())
    }

    /// Register a new backend
    pub(crate) async fn register(
        &self,
        name: String,
        host: String,
        port: u16,
        protocol: String,
        description: Option<String>,
        namespace: Option<String>,
        source: BackendSource,
    ) -> Result<BackendInfo, String> {
        let id = Uuid::new_v4().to_string();
        let now = current_timestamp();

        sqlx::query(
            r#"
            INSERT INTO backends (
                id, name, host, port, protocol, description, namespace,
                version, metadata, source, is_active, registered_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, 1, ?, ?, ?)
            "#,
        )
        .bind(&id)
        .bind(&name)
        .bind(&host)
        .bind(port as i64)
        .bind(&protocol)
        .bind(&description)
        .bind(&namespace)
        .bind(source.as_str())
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("Failed to register backend: {}", e))?;

        Ok(BackendInfo {
            id,
            name,
            host,
            port,
            protocol,
            description,
            namespace,
            version: None,
            metadata: None,
            source,
            is_active: true,
            registered_at: now,
            last_seen: None,
            created_at: now,
            updated_at: now,
        })
    }

    /// List all backends (optionally filter by active status)
    pub(crate) async fn list(&self, active_only: bool) -> Result<Vec<BackendInfo>, String> {
        let query = if active_only {
            "SELECT * FROM backends WHERE is_active = 1 ORDER BY created_at DESC"
        } else {
            "SELECT * FROM backends ORDER BY created_at DESC"
        };

        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| format!("Failed to list backends: {}", e))?;

        rows.into_iter()
            .map(|row| row_to_backend_info(row))
            .collect()
    }

    /// Get a backend by name
    pub(crate) async fn get(&self, name: &str) -> Result<Option<BackendInfo>, String> {
        let row = sqlx::query("SELECT * FROM backends WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("Failed to get backend: {}", e))?;

        match row {
            Some(row) => Ok(Some(row_to_backend_info(row)?)),
            None => Ok(None),
        }
    }

    /// Update a backend
    pub(crate) async fn update(
        &self,
        name: &str,
        host: Option<String>,
        port: Option<u16>,
        protocol: Option<String>,
        description: Option<String>,
        namespace: Option<String>,
    ) -> Result<Option<BackendInfo>, String> {
        // Get current backend
        let current = match self.get(name).await? {
            Some(backend) => backend,
            None => return Ok(None),
        };

        let new_host = host.unwrap_or(current.host);
        let new_port = port.unwrap_or(current.port);
        let new_protocol = protocol.unwrap_or(current.protocol);
        let new_description = description.or(current.description);
        let new_namespace = namespace.or(current.namespace);
        let now = current_timestamp();

        sqlx::query(
            r#"
            UPDATE backends
            SET host = ?, port = ?, protocol = ?, description = ?, namespace = ?, updated_at = ?
            WHERE name = ?
            "#,
        )
        .bind(&new_host)
        .bind(new_port as i64)
        .bind(&new_protocol)
        .bind(&new_description)
        .bind(&new_namespace)
        .bind(now)
        .bind(name)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("Failed to update backend: {}", e))?;

        self.get(name).await
    }

    /// Delete a backend
    pub(crate) async fn delete(&self, name: &str) -> Result<bool, String> {
        let result = sqlx::query("DELETE FROM backends WHERE name = ?")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("Failed to delete backend: {}", e))?;

        Ok(result.rows_affected() > 0)
    }

    /// Update last_seen timestamp for health checks
    pub(crate) async fn ping(&self, name: &str) -> Result<bool, String> {
        let now = current_timestamp();

        let result = sqlx::query("UPDATE backends SET last_seen = ? WHERE name = ?")
            .bind(now)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("Failed to ping backend: {}", e))?;

        Ok(result.rows_affected() > 0)
    }

    /// Load backends from TOML config file
    pub(crate) async fn load_config(&self) -> Result<Vec<BackendInfo>, String> {
        let config_path = match &self.config_path {
            Some(path) => path,
            None => return Ok(vec![]),
        };

        if !config_path.exists() {
            tracing::info!("Config file not found: {}", config_path.display());
            return Ok(vec![]);
        }

        let loaded = load_backends_from_toml(config_path)?;
        let mut registered = Vec::new();

        for backend in loaded {
            // Check if backend already exists
            if self.get(&backend.name).await?.is_some() {
                tracing::debug!("Backend {} already exists, skipping", backend.name);
                continue;
            }

            // Register new backend
            match self
                .register(
                    backend.name.clone(),
                    backend.host,
                    backend.port,
                    backend.protocol,
                    backend.description,
                    backend.namespace,
                    BackendSource::File,
                )
                .await
            {
                Ok(info) => {
                    tracing::info!("Loaded backend from config: {}", info.name);
                    registered.push(info);
                }
                Err(e) => {
                    tracing::error!("Failed to register backend {}: {}", backend.name, e);
                }
            }
        }

        Ok(registered)
    }

    /// Reload config file (re-load and register any new backends)
    pub(crate) async fn reload_config(&self) -> Result<usize, String> {
        let loaded = self.load_config().await?;
        Ok(loaded.len())
    }
}

/// Convert database row to BackendInfo
fn row_to_backend_info(row: sqlx::sqlite::SqliteRow) -> Result<BackendInfo, String> {
    let source_str: String = row.get("source");
    let source = BackendSource::from_str(&source_str)
        .ok_or_else(|| format!("Invalid backend source: {}", source_str))?;

    Ok(BackendInfo {
        id: row.get("id"),
        name: row.get("name"),
        host: row.get("host"),
        port: row.get::<i64, _>("port") as u16,
        protocol: row.get("protocol"),
        description: row.get("description"),
        namespace: row.get("namespace"),
        version: row.get("version"),
        metadata: row.get("metadata"),
        source,
        is_active: row.get::<i64, _>("is_active") != 0,
        registered_at: row.get("registered_at"),
        last_seen: row.get("last_seen"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

/// Load backends from TOML config file
fn load_backends_from_toml(path: &Path) -> Result<Vec<BackendConfig>, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read config file: {}", e))?;

    let config: RegistryConfig =
        toml::from_str(&content).map_err(|e| format!("Failed to parse TOML config: {}", e))?;

    Ok(config.backend)
}

/// Get current Unix timestamp in seconds
fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
