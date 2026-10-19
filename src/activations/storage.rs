//! Common storage utilities for activation persistence
//!
//! This module provides shared infrastructure for SQLite-backed storage
//! across different activations, including standardized path management
//! and connection initialization.

use sqlx::{sqlite::{SqliteConnectOptions, SqlitePool}, ConnectOptions};
use std::path::PathBuf;
use std::sync::OnceLock;

/// PLEXUS-6: where a throwaway substrate keeps its databases (`--data-dir`),
/// in place of `~/.plexus/substrate` and the registry's `~/.config/plexus`.
static DATA_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Set the data directory for this process, before the hub is built. The
/// first call wins; a later one answers `false` and changes nothing.
pub fn set_data_dir(dir: PathBuf) -> bool {
    DATA_DIR.set(dir).is_ok()
}

/// The data directory set by [`set_data_dir`], if any.
pub fn data_dir() -> Option<&'static PathBuf> {
    DATA_DIR.get()
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
pub fn activation_db_path(activation_name: &str, db_filename: &str) -> PathBuf {
    if let Some(dir) = data_dir() {
        return dir.join("activations").join(activation_name).join(db_filename);
    }
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| ".".to_string());

    PathBuf::from(home)
        .join(".plexus")
        .join("substrate")
        .join("activations")
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
