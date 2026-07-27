//! Common storage utilities for activation persistence
//!
//! This module provides shared infrastructure for SQLite-backed storage
//! across different activations, including standardized path management
//! and connection initialization.

use sqlx::{sqlite::{SqliteConnectOptions, SqlitePool}, ConnectOptions};
use std::path::{Path, PathBuf};

use plexus_auth_core::tenant::TenantId;

use crate::tenancy::TenantStorageRoot;

/// The host's own activation storage root: `$HOME/.plexus/substrate/activations`.
///
/// Reads `HOME` on every call, exactly as `activation_db_path` always has —
/// changing that would move a live deployment's databases.
#[must_use]
pub fn host_activation_root() -> PathBuf {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| ".".to_string());

    PathBuf::from(home)
        .join(".plexus")
        .join("substrate")
        .join("activations")
}

/// **PLX-129 — where one composition's storage lives, as a value.**
///
/// Before M4·E there was no such value: [`activation_db_path`] was a free
/// function taking `(activation, file)` and reading `HOME`, so *every*
/// composition — the host hub and every tenant's hub alike — resolved to the
/// same nine sqlite files. A `WHERE tenant_id = ?` that nobody wrote is not a
/// leak you can see; a shared file handle is. This type is the seam that makes
/// the difference expressible.
///
/// There are exactly two ways to make one, and they are not symmetric:
///
/// - [`StorageScope::host`] — free, because the host surface is the deployment
///   operator's own and always was.
/// - [`StorageScope::for_tenant`] — takes a [`TenantStorageRoot`], which has no
///   public constructor and is minted only by
///   [`TenantAdmission::tenant_storage`](crate::tenancy::TenantAdmission::tenant_storage)
///   *after* the sealed `TenantRecord` has been resolved and `is_active()`
///   checked. **The proof is the argument**, which is why there is no
///   `StorageScope::for_tenant_id(&TenantId)`: a `TenantId` is publicly
///   constructible (`TenantId::try_new("../tenant-a")` succeeds), so a scope
///   built from one would name nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageScope {
    /// Canonical for a tenant; `HOME`-derived and possibly not yet existing for
    /// the host, which is the pre-existing behaviour and is deliberately kept.
    root: PathBuf,
    tenant: Option<TenantId>,
}

impl StorageScope {
    /// The process-global host surface — `$HOME/.plexus/substrate/activations`,
    /// byte-identical to what [`activation_db_path`] produced before M4·E.
    #[must_use]
    pub fn host() -> Self {
        Self {
            root: host_activation_root(),
            tenant: None,
        }
    }

    /// One tenant's own surface.
    ///
    /// The argument is the whole security property: see the type docs.
    #[must_use]
    pub fn for_tenant(root: &TenantStorageRoot) -> Self {
        Self {
            root: root.activations_dir().to_path_buf(),
            tenant: Some(root.tenant().clone()),
        }
    }

    /// The tenant this scope belongs to, or `None` for the host surface.
    #[must_use]
    pub const fn tenant(&self) -> Option<&TenantId> {
        self.tenant.as_ref()
    }

    /// The directory that holds every activation's storage in this scope.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// `<root>/<activation>/<file>`.
    ///
    /// Both segments are `&'static str` literals written in
    /// [`crate::builder::build_activations_in`] — never caller input — which is
    /// why this join needs no segment check of its own. The *tenant* segment,
    /// which is caller-influenced, was checked and contained one level up, when
    /// the [`TenantStorageRoot`] was minted.
    #[must_use]
    pub fn db_path(&self, activation_name: &str, db_filename: &str) -> PathBuf {
        debug_assert!(
            !activation_name.contains('/') && activation_name != ".." && !activation_name.is_empty(),
            "activation names are literals; {activation_name:?} is not one"
        );
        self.root.join(activation_name).join(db_filename)
    }

    /// Where this scope's `claudecode` keeps Claude Code's own session
    /// transcripts.
    ///
    /// **The host answer is `~/.claude/projects`, unchanged** — that is not
    /// plexus's directory, it is the CLI's, and a host deployment must keep
    /// reading the sessions it already has. A *tenant* gets a directory inside
    /// its own root instead, because `claudecode.sessions_*` takes a
    /// caller-supplied `project_path` and joins it onto this base.
    #[must_use]
    pub fn claude_sessions_root(&self) -> PathBuf {
        match self.tenant {
            None => dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".claude")
                .join("projects"),
            Some(_) => self.root.join("claude-projects"),
        }
    }

    /// Where this scope's `registry` keeps its database and TOML.
    ///
    /// **The host answer is `RegistryStorageConfig::default()`, unchanged** —
    /// `dirs::config_dir()/plexus/{registry.db,backends.toml}`, a tree outside
    /// `~/.plexus` altogether. Moving it would relocate a live deployment's
    /// backend catalogue, which is not this ticket's business. A tenant gets
    /// its own pair inside its own root, because `registry` is registered on
    /// the tenant hub and `registry.register` writes.
    #[must_use]
    pub fn registry_config(&self) -> registry::RegistryStorageConfig {
        match self.tenant {
            None => registry::RegistryStorageConfig::default(),
            Some(_) => registry::RegistryStorageConfig {
                db_path: self.db_path("registry", "registry.db"),
                config_path: Some(self.db_path("registry", "backends.toml")),
            },
        }
    }
}

/// Generate a namespaced database path under ~/.plexus/
///
/// Returns: `~/.plexus/substrate/activations/{activation_name}/{db_filename}`
///
/// # Arguments
/// * `activation_name` - The name of the activation (e.g., "orcha", "claudecode")
/// * `db_filename` - The database filename (e.g., "orcha.db", "sessions.db")
///
/// # Example
/// ```ignore
/// let path = activation_db_path("orcha", "orcha.db");
/// // Returns: ~/.plexus/substrate/activations/orcha/orcha.db
/// ```
/// **This is the HOST path and only the host path.** It has no tenant
/// parameter and never will; a tenant's path comes from
/// [`StorageScope::db_path`], which cannot be reached without a
/// [`TenantStorageRoot`]. The `*Config::default()` impls still call it, and
/// those defaults are overwritten wholesale by
/// [`crate::builder::build_activations_in`] for any non-host scope.
pub fn activation_db_path(activation_name: &str, db_filename: &str) -> PathBuf {
    host_activation_root()
        .join(activation_name)
        .join(db_filename)
}

/// Extract activation name from module path
///
/// Extracts the activation name from a module path like:
/// - `plexus_substrate::activations::orcha::storage` → `"orcha"`
/// - `plexus_substrate::activations::claudecode_loopback::storage` → `"claudecode_loopback"`
///
/// # Arguments
/// * `module_path` - The module path (typically from `module_path!()` macro)
///
/// # Example
/// ```ignore
/// // Called from src/activations/orcha/storage.rs
/// let name = extract_activation_name(module_path!());
/// assert_eq!(name, "orcha");
/// ```
pub fn extract_activation_name(module_path: &str) -> &str {
    // Module path format: plexus_substrate::activations::{activation_name}::storage
    // or: crate::activations::{activation_name}::storage
    module_path
        .split("::")
        .skip_while(|&s| s != "activations")
        .nth(1)
        .unwrap_or("unknown")
}

/// Generate a namespaced database path from the calling module's path
///
/// This macro automatically extracts the activation name from the module structure
/// and generates the appropriate database path.
///
/// # Example
/// ```ignore
/// // Called from src/activations/orcha/storage.rs
/// let path = activation_db_path_from_module!("orcha.db");
/// // Returns: ~/.plexus/substrate/activations/orcha/orcha.db
/// ```
#[macro_export]
macro_rules! activation_db_path_from_module {
    ($db_filename:expr) => {
        $crate::activations::storage::activation_db_path(
            $crate::activations::storage::extract_activation_name(module_path!()),
            $db_filename
        )
    };
}

/// Initialize a `SQLite` connection pool with standard options
///
/// This helper:
/// 1. Creates parent directories if they don't exist
/// 2. Enables `create_if_missing` for the database
/// 3. Disables statement logging
/// 4. Returns a ready-to-use connection pool
///
/// # Arguments
/// * `db_path` - Path to the `SQLite` database file
///
/// # Errors
/// Returns an error if directory creation or database connection fails
pub async fn init_sqlite_pool(db_path: PathBuf) -> Result<SqlitePool, String> {
    // Ensure parent directory exists
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create database directory: {e}"))?;
    }

    // Parse connection options
    let db_url = format!("sqlite://{}", db_path.display());
    let options = db_url
        .parse::<SqliteConnectOptions>()
        .map_err(|e| format!("Failed to parse DB URL: {e}"))?;

    // Configure SQLite options
    let options = options
        .disable_statement_logging()
        .create_if_missing(true);

    // Connect to database
    SqlitePool::connect_with(options)
        .await
        .map_err(|e| format!("Failed to connect to database: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_activation_db_path() {
        let path = activation_db_path("orcha", "orcha.db");
        let path_str = path.to_string_lossy();

        assert!(path_str.contains(".plexus"));
        assert!(path_str.contains("substrate"));
        assert!(path_str.contains("activations"));
        assert!(path_str.contains("orcha"));
        assert!(path_str.ends_with("orcha.db"));
    }

    #[test]
    fn test_activation_db_path_different_names() {
        let path1 = activation_db_path("claudecode", "sessions.db");
        let path2 = activation_db_path("cone", "cones.db");

        assert!(path1.to_string_lossy().contains("claudecode/sessions.db"));
        assert!(path2.to_string_lossy().contains("cone/cones.db"));
        assert_ne!(path1, path2);
    }

    #[test]
    fn test_extract_activation_name() {
        assert_eq!(
            extract_activation_name("plexus_substrate::activations::orcha::storage"),
            "orcha"
        );
        assert_eq!(
            extract_activation_name("plexus_substrate::activations::claudecode_loopback::storage"),
            "claudecode_loopback"
        );
        assert_eq!(
            extract_activation_name("crate::activations::cone::storage"),
            "cone"
        );
    }

    #[tokio::test]
    async fn test_init_sqlite_pool() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let test_db = PathBuf::from(format!("/tmp/test_storage_{timestamp}.db"));

        let pool = init_sqlite_pool(test_db.clone()).await;
        assert!(pool.is_ok(), "Failed to initialize SQLite pool");

        // Cleanup
        let _ = std::fs::remove_file(test_db);
    }
}
