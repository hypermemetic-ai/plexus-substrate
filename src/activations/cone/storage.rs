use super::methods::ConeIdentifier;
use super::types::{ConeConfig, ConeError, ConeHandle, ConeId, ConeInfo, Message, MessageId, MessageRole, Position};
use crate::activations::arbor::{ArborStorage, NodeId, TreeId};
use crate::activations::storage::init_sqlite_pool;
use crate::activation_db_path_from_module;
use serde_json::Value;
use sqlx::{sqlite::SqlitePool, Row};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Configuration for Cone storage
#[derive(Debug, Clone)]
pub struct ConeStorageConfig {
    /// Path to `SQLite` database for cone configs
    pub db_path: PathBuf,
}

impl Default for ConeStorageConfig {
    fn default() -> Self {
        Self {
            db_path: activation_db_path_from_module!("cones.db"),
        }
    }
}

/// Storage layer for cone configurations
pub struct ConeStorage {
    pool: SqlitePool,
    arbor: Arc<ArborStorage>,
}

impl ConeStorage {
    /// Create a new cone storage instance with a shared Arbor storage
    pub async fn new(config: ConeStorageConfig, arbor: Arc<ArborStorage>) -> Result<Self, ConeError> {
        let pool = init_sqlite_pool(config.db_path).await?;

        let storage = Self { pool, arbor };
        storage.run_migrations().await?;

        Ok(storage)
    }

    /// Run database migrations
    async fn run_migrations(&self) -> Result<(), ConeError> {
        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS cones (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                model_id TEXT NOT NULL,
                system_prompt TEXT,
                tree_id TEXT NOT NULL,
                canonical_head TEXT NOT NULL,
                metadata TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                cone_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                model_id TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                created_at INTEGER NOT NULL,
                FOREIGN KEY (cone_id) REFERENCES cones(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_cones_name ON cones(name);
            CREATE INDEX IF NOT EXISTS idx_cones_tree ON cones(tree_id);
            CREATE INDEX IF NOT EXISTS idx_messages_cone ON messages(cone_id);
            ",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| ConeError::StorageError { operation: "migration".into(), detail: e.to_string() })?;

        Ok(())
    }

    /// Get access to the underlying arbor storage
    pub fn arbor(&self) -> &ArborStorage {
        &self.arbor
    }

    // ========================================================================
    // Cone CRUD Operations
    // ========================================================================

    /// Create a new cone with a new conversation tree
    ///
    /// If a cone with the given name already exists, automatically appends `#<uuid>`
    /// to make it unique. For example, "assistant" becomes "assistant#550e8400..."
    pub async fn cone_create(
        &self,
        name: String,
        model_id: String,
        system_prompt: Option<String>,
        metadata: Option<Value>,
    ) -> Result<ConeConfig, ConeError> {
        let cone_id = ConeId::new_v4();
        let now = current_timestamp();

        // Create a new tree for this cone
        let tree_id = self.arbor.tree_create(metadata.clone(), &cone_id.to_string()).await
            .map_err(|e| ConeError::ArborError { detail: format!("Failed to create tree: {e}") })?;

        // Get the root node as initial position
        let tree = self.arbor.tree_get(&tree_id).await
            .map_err(|e| ConeError::ArborError { detail: format!("Failed to get tree: {e}") })?;
        let head = Position::new(tree_id, tree.root);

        let metadata_json = metadata.as_ref().map(|m| serde_json::to_string(m).unwrap());

        // Try inserting with the original name first
        let final_name = match sqlx::query(
            "INSERT INTO cones (id, name, model_id, system_prompt, tree_id, canonical_head, metadata, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(cone_id.to_string())
        .bind(&name)
        .bind(&model_id)
        .bind(&system_prompt)
        .bind(head.tree_id.to_string())
        .bind(head.node_id.to_string())
        .bind(metadata_json.clone())
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await {
            Ok(_) => name,  // Success with original name
            Err(e) if e.to_string().contains("UNIQUE constraint failed") => {
                // Name collision - append #uuid to make it unique
                let unique_name = format!("{name}#{cone_id}");

                sqlx::query(
                    "INSERT INTO cones (id, name, model_id, system_prompt, tree_id, canonical_head, metadata, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(cone_id.to_string())
                .bind(&unique_name)
                .bind(&model_id)
                .bind(&system_prompt)
                .bind(head.tree_id.to_string())
                .bind(head.node_id.to_string())
                .bind(metadata_json)
                .bind(now)
                .bind(now)
                .execute(&self.pool)
                .await
                .map_err(|e| ConeError::StorageError { operation: "create_cone".into(), detail: e.to_string() })?;

                unique_name
            }
            Err(e) => return Err(ConeError::StorageError { operation: "create_cone".into(), detail: e.to_string() }),
        };

        Ok(ConeConfig {
            id: cone_id,
            name: final_name,
            model_id,
            system_prompt,
            head,
            metadata,
            created_at: now,
            updated_at: now,
        })
    }

    /// Resolve a cone identifier to a `ConeId`
    ///
    /// For name lookups, supports partial matching on the name portion before '#':
    /// - "assistant" matches "assistant" or "assistant#550e8400-..."
    /// - "assistant#550e" matches "assistant#550e8400-..."
    ///
    /// Fails if the pattern matches multiple cones (ambiguous).
    pub async fn resolve_cone_identifier(&self, identifier: &ConeIdentifier) -> Result<ConeId, ConeError> {
        match identifier {
            ConeIdentifier::ById { id } => Ok(*id),
            ConeIdentifier::ByName { name } => {
                // Try exact match first
                if let Some(row) = sqlx::query("SELECT id FROM cones WHERE name = ?")
                    .bind(name)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| ConeError::StorageError { operation: "resolve_by_name".into(), detail: e.to_string() })?
                {
                    let id_str: String = row.get("id");
                    return Uuid::parse_str(&id_str)
                        .map_err(|e| ConeError::StorageError { operation: "parse_cone_id".into(), detail: e.to_string() });
                }

                // Try partial match with LIKE pattern
                // Pattern: "name%" matches "name" or "name#uuid"
                let pattern = format!("{name}%");
                let rows = sqlx::query("SELECT id, name FROM cones WHERE name LIKE ?")
                    .bind(&pattern)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| ConeError::StorageError { operation: "resolve_by_pattern".into(), detail: e.to_string() })?;

                match rows.len() {
                    0 => Err(ConeError::SessionNotFound { name: name.clone() }),
                    1 => {
                        let id_str: String = rows[0].get("id");
                        Uuid::parse_str(&id_str)
                            .map_err(|e| ConeError::StorageError { operation: "parse_cone_id".into(), detail: e.to_string() })
                    }
                    _ => {
                        // Multiple matches - list them for user
                        let matches: Vec<String> = rows.iter().map(|r| r.get("name")).collect();
                        Err(ConeError::InvalidState {
                            message: format!(
                                "Ambiguous name '{}' matches multiple cones: {}. Use full name with #uuid to disambiguate.",
                                name,
                                matches.join(", ")
                            ),
                        })
                    }
                }
            }
        }
    }

    /// Get a cone by ID
    pub async fn cone_get(&self, cone_id: &ConeId) -> Result<ConeConfig, ConeError> {
        let row = sqlx::query(
            "SELECT id, name, model_id, system_prompt, tree_id, canonical_head, metadata, created_at, updated_at
             FROM cones WHERE id = ?",
        )
        .bind(cone_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ConeError::StorageError { operation: "fetch_cone".into(), detail: e.to_string() })?
        .ok_or_else(|| ConeError::SessionNotFound { name: cone_id.to_string() })?;

        self.row_to_cone_config(row)
    }

    /// Get a cone by identifier (name or UUID)
    pub async fn cone_get_by_identifier(&self, identifier: &ConeIdentifier) -> Result<ConeConfig, ConeError> {
        let cone_id = self.resolve_cone_identifier(identifier).await?;
        self.cone_get(&cone_id).await
    }

    /// List all cones
    pub async fn cone_list(&self) -> Result<Vec<ConeInfo>, ConeError> {
        let rows = sqlx::query(
            "SELECT id, name, model_id, tree_id, canonical_head, created_at FROM cones ORDER BY created_at DESC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| ConeError::StorageError { operation: "list_cones".into(), detail: e.to_string() })?;

        let cones: Result<Vec<ConeInfo>, ConeError> = rows
            .iter()
            .map(|row| {
                let id_str: String = row.get("id");
                let tree_id_str: String = row.get("tree_id");
                let head_str: String = row.get("canonical_head");

                let tree_id = TreeId::parse_str(&tree_id_str).map_err(|e| ConeError::StorageError { operation: "parse_tree_id".into(), detail: e })?;
                let node_id = NodeId::parse_str(&head_str).map_err(|e| ConeError::StorageError { operation: "parse_node_id".into(), detail: e })?;

                Ok(ConeInfo {
                    id: Uuid::parse_str(&id_str).map_err(|e| ConeError::StorageError { operation: "parse_cone_id".into(), detail: e.to_string() })?,
                    name: row.get("name"),
                    model_id: row.get("model_id"),
                    head: Position::new(tree_id, node_id),
                    created_at: row.get("created_at"),
                })
            })
            .collect();

        cones
    }

    /// Update cone's canonical head
    pub async fn cone_update_head(
        &self,
        cone_id: &ConeId,
        new_head: NodeId,
    ) -> Result<(), ConeError> {
        let now = current_timestamp();

        let result = sqlx::query(
            "UPDATE cones SET canonical_head = ?, updated_at = ? WHERE id = ?",
        )
        .bind(new_head.to_string())
        .bind(now)
        .bind(cone_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| ConeError::StorageError { operation: "update_head".into(), detail: e.to_string() })?;

        if result.rows_affected() == 0 {
            return Err(ConeError::SessionNotFound { name: cone_id.to_string() });
        }

        Ok(())
    }

    /// Update cone configuration
    pub async fn cone_update(
        &self,
        cone_id: &ConeId,
        name: Option<String>,
        model_id: Option<String>,
        system_prompt: Option<Option<String>>,
        metadata: Option<Value>,
    ) -> Result<(), ConeError> {
        let now = current_timestamp();

        // Get current cone
        let current = self.cone_get(cone_id).await?;

        let new_name = name.unwrap_or(current.name);
        let new_model = model_id.unwrap_or(current.model_id);
        let new_prompt = system_prompt.unwrap_or(current.system_prompt);
        let new_metadata = metadata.or(current.metadata);
        let metadata_json = new_metadata.as_ref().map(|m| serde_json::to_string(m).unwrap());

        sqlx::query(
            "UPDATE cones SET name = ?, model_id = ?, system_prompt = ?, metadata = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&new_name)
        .bind(&new_model)
        .bind(&new_prompt)
        .bind(metadata_json)
        .bind(now)
        .bind(cone_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| ConeError::StorageError { operation: "update_cone".into(), detail: e.to_string() })?;

        Ok(())
    }

    /// Delete an cone (does not delete the tree)
    pub async fn cone_delete(&self, cone_id: &ConeId) -> Result<(), ConeError> {
        let result = sqlx::query("DELETE FROM cones WHERE id = ?")
            .bind(cone_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| ConeError::StorageError { operation: "delete_cone".into(), detail: e.to_string() })?;

        if result.rows_affected() == 0 {
            return Err(ConeError::SessionNotFound { name: cone_id.to_string() });
        }

        Ok(())
    }

    // ========================================================================
    // Message Operations
    // ========================================================================

    /// Create a message and return its ID
    pub async fn message_create(
        &self,
        cone_id: &ConeId,
        role: MessageRole,
        content: String,
        model_id: Option<String>,
        input_tokens: Option<i64>,
        output_tokens: Option<i64>,
    ) -> Result<Message, ConeError> {
        let message_id = MessageId::new_v4();
        let now = current_timestamp();

        sqlx::query(
            "INSERT INTO messages (id, cone_id, role, content, model_id, input_tokens, output_tokens, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(message_id.to_string())
        .bind(cone_id.to_string())
        .bind(role.as_str())
        .bind(&content)
        .bind(&model_id)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|e| ConeError::StorageError { operation: "create_message".into(), detail: e.to_string() })?;

        Ok(Message {
            id: message_id,
            cone_id: *cone_id,
            role,
            content,
            created_at: now,
            model_id,
            input_tokens,
            output_tokens,
        })
    }

    /// Create an ephemeral message (marked for deletion) and return it
    pub async fn message_create_ephemeral(
        &self,
        cone_id: &ConeId,
        role: MessageRole,
        content: String,
        model_id: Option<String>,
        input_tokens: Option<i64>,
        output_tokens: Option<i64>,
    ) -> Result<Message, ConeError> {
        let message_id = MessageId::new_v4();
        let now = current_timestamp();

        // Use negative timestamp as ephemeral marker for cleanup
        let ephemeral_marker = -now;

        sqlx::query(
            "INSERT INTO messages (id, cone_id, role, content, model_id, input_tokens, output_tokens, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(message_id.to_string())
        .bind(cone_id.to_string())
        .bind(role.as_str())
        .bind(&content)
        .bind(&model_id)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(ephemeral_marker)
        .execute(&self.pool)
        .await
        .map_err(|e| ConeError::StorageError { operation: "create_ephemeral_message".into(), detail: e.to_string() })?;

        Ok(Message {
            id: message_id,
            cone_id: *cone_id,
            role,
            content,
            created_at: ephemeral_marker,
            model_id,
            input_tokens,
            output_tokens,
        })
    }

    /// Get a message by ID
    pub async fn message_get(&self, message_id: &MessageId) -> Result<Message, ConeError> {
        let row = sqlx::query(
            "SELECT id, cone_id, role, content, model_id, input_tokens, output_tokens, created_at
             FROM messages WHERE id = ?",
        )
        .bind(message_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| ConeError::StorageError { operation: "fetch_message".into(), detail: e.to_string() })?
        .ok_or_else(|| ConeError::SessionNotFound { name: format!("message:{message_id}") })?;

        self.row_to_message(row)
    }

    /// Resolve a message handle identifier to a Message
    /// Handle format: "msg-{message_id}:{role}:{name}"
    pub async fn resolve_message_handle(&self, identifier: &str) -> Result<Message, ConeError> {
        // Parse identifier: "msg-{uuid}:{role}:{name}"
        let parts: Vec<&str> = identifier.splitn(3, ':').collect();
        if parts.len() < 2 {
            return Err(format!("Invalid message handle format: {identifier}").into());
        }

        let msg_part = parts[0];
        if !msg_part.starts_with("msg-") {
            return Err(format!("Invalid message handle format: {identifier}").into());
        }

        let message_id_str = &msg_part[4..]; // Strip "msg-" prefix
        let message_id = Uuid::parse_str(message_id_str)
            .map_err(|e| ConeError::StorageError { operation: "parse_handle_id".into(), detail: e.to_string() })?;

        self.message_get(&message_id).await
    }

    /// Create a handle for a message
    ///
    /// Format: `{plugin_id}@1.0.0::chat:msg-{id}:{role}:{name}`
    /// Uses `ConeHandle` enum for type-safe handle creation.
    pub fn message_to_handle(message: &Message, name: &str) -> crate::types::Handle {
        ConeHandle::Message {
            message_id: format!("msg-{}", message.id),
            role: message.role.as_str().to_string(),
            name: name.to_string(),
        }.to_handle()
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    fn row_to_message(&self, row: sqlx::sqlite::SqliteRow) -> Result<Message, ConeError> {
        let id_str: String = row.get("id");
        let cone_id_str: String = row.get("cone_id");
        let role_str: String = row.get("role");

        Ok(Message {
            id: Uuid::parse_str(&id_str).map_err(|e| ConeError::StorageError { operation: "parse_message_id".into(), detail: e.to_string() })?,
            cone_id: Uuid::parse_str(&cone_id_str).map_err(|e| ConeError::StorageError { operation: "parse_cone_id".into(), detail: e.to_string() })?,
            role: MessageRole::from_str(&role_str).ok_or_else(|| ConeError::StorageError { operation: "parse_role".into(), detail: format!("Invalid role: {role_str}") })?,
            content: row.get("content"),
            created_at: row.get("created_at"),
            model_id: row.get("model_id"),
            input_tokens: row.get("input_tokens"),
            output_tokens: row.get("output_tokens"),
        })
    }

    fn row_to_cone_config(&self, row: sqlx::sqlite::SqliteRow) -> Result<ConeConfig, ConeError> {
        let id_str: String = row.get("id");
        let tree_id_str: String = row.get("tree_id");
        let head_str: String = row.get("canonical_head");
        let metadata_json: Option<String> = row.get("metadata");

        let tree_id = TreeId::parse_str(&tree_id_str).map_err(|e| ConeError::StorageError { operation: "parse_tree_id".into(), detail: e })?;
        let node_id = NodeId::parse_str(&head_str).map_err(|e| ConeError::StorageError { operation: "parse_node_id".into(), detail: e })?;

        Ok(ConeConfig {
            id: Uuid::parse_str(&id_str).map_err(|e| ConeError::StorageError { operation: "parse_cone_id".into(), detail: e.to_string() })?,
            name: row.get("name"),
            model_id: row.get("model_id"),
            system_prompt: row.get("system_prompt"),
            head: Position::new(tree_id, node_id),
            metadata: metadata_json.and_then(|s| serde_json::from_str(&s).ok()),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }
}

/// Get current Unix timestamp in seconds
fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::Cone;

    // ========================================================================
    // INVARIANT: Handle meta format consistency
    //
    // This is the critical invariant that was violated before the fix.
    // The meta parts created by message_to_handle, when joined with ':',
    // must match the format expected by resolve_message_handle.
    // ========================================================================

    #[test]
    fn invariant_handle_meta_format_matches_resolver() {
        // Create a mock message
        let message = Message {
            id: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            cone_id: Uuid::new_v4(),
            role: MessageRole::User,
            content: "test content".to_string(),
            created_at: 0,
            model_id: None,
            input_tokens: None,
            output_tokens: None,
        };

        // Create handle the way cone::chat does
        let handle = ConeStorage::message_to_handle(&message, "test-cone");

        // Join meta the way resolve_handle_impl does (after the fix)
        let identifier = handle.meta.join(":");

        // Verify the format matches what resolve_message_handle expects:
        // "msg-{uuid}:{role}:{name}"
        let parts: Vec<&str> = identifier.splitn(3, ':').collect();

        assert_eq!(parts.len(), 3, "identifier should have 3 parts: {identifier}");
        assert!(parts[0].starts_with("msg-"), "first part should start with 'msg-': {}", parts[0]);

        // The message ID should be extractable
        let msg_part = parts[0];
        let message_id_str = &msg_part[4..]; // Strip "msg-" prefix
        let parsed_id = Uuid::parse_str(message_id_str);
        assert!(parsed_id.is_ok(), "should be able to parse UUID from meta[0]");
        assert_eq!(parsed_id.unwrap(), message.id);

        // Role should be preserved
        assert_eq!(parts[1], "user");

        // Name should be preserved
        assert_eq!(parts[2], "test-cone");
    }

    #[test]
    fn invariant_handle_meta_roles() {
        // Test all roles produce valid meta format
        for (role, expected_str) in [
            (MessageRole::User, "user"),
            (MessageRole::Assistant, "assistant"),
            (MessageRole::System, "system"),
        ] {
            let message = Message {
                id: Uuid::new_v4(),
                cone_id: Uuid::new_v4(),
                role,
                content: "test".to_string(),
                created_at: 0,
                model_id: None,
                input_tokens: None,
                output_tokens: None,
            };

            let handle = ConeStorage::message_to_handle(&message, "cone");
            assert_eq!(handle.meta[1], expected_str);
        }
    }

    #[test]
    fn invariant_handle_plugin_method_fixed() {
        // Handles from message_to_handle always use "cone" plugin and "chat" method
        let message = Message {
            id: Uuid::new_v4(),
            cone_id: Uuid::new_v4(),
            role: MessageRole::User,
            content: "test".to_string(),
            created_at: 0,
            model_id: None,
            input_tokens: None,
            output_tokens: None,
        };

        let handle = ConeStorage::message_to_handle(&message, "any-name");

        // plugin_id should match Cone's PLUGIN_ID
        assert_eq!(handle.plugin_id, Cone::PLUGIN_ID);
        assert_eq!(handle.version, "1.0.0");
        assert_eq!(handle.method, "chat");
    }

    #[test]
    fn invariant_handle_meta_has_three_parts() {
        // All cone chat handles have exactly 3 meta parts
        let message = Message {
            id: Uuid::new_v4(),
            cone_id: Uuid::new_v4(),
            role: MessageRole::Assistant,
            content: "response".to_string(),
            created_at: 0,
            model_id: Some("gpt-4".to_string()),
            input_tokens: Some(10),
            output_tokens: Some(20),
        };

        let handle = ConeStorage::message_to_handle(&message, "my-cone");

        assert_eq!(handle.meta.len(), 3, "cone chat handle must have exactly 3 meta parts");
    }
}
