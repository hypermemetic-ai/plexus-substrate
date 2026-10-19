use super::types::{
    BufferedEvent, ChatEvent, ClaudeCodeConfig, ClaudeCodeError, ClaudeCodeHandle, ClaudeCodeId,
    ClaudeCodeInfo, ClaudeMessage, ContentBlock, Message, MessageId, MessageRole, Model,
    NodeEvent, Position, StreamId, StreamInfo, StreamStatus,
};
use crate::activations::arbor::{ArborStorage, NodeId, NodeType, TreeId};
use crate::activations::storage::init_sqlite_pool;
use crate::activation_db_path_from_module;
use serde_json::Value;
use sqlx::{sqlite::SqlitePool, Row};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use uuid::Uuid;

/// Helper to create parse errors
fn parse_err(context: &'static str, detail: impl std::fmt::Display) -> ClaudeCodeError {
    ClaudeCodeError::Parse { context, detail: detail.to_string() }
}

/// Helper to create database errors
const fn db_err(operation: &'static str, source: sqlx::Error) -> ClaudeCodeError {
    ClaudeCodeError::Database { operation, source }
}

/// Configuration for `ClaudeCode` storage
#[derive(Debug, Clone)]
pub struct ClaudeCodeStorageConfig {
    /// Path to `SQLite` database for `ClaudeCode` sessions
    pub db_path: PathBuf,
}

impl Default for ClaudeCodeStorageConfig {
    fn default() -> Self {
        Self {
            db_path: activation_db_path_from_module!("claudecode.db"),
        }
    }
}

/// In-memory buffer for an active stream
#[derive(Debug)]
struct ActiveStreamBuffer {
    /// Stream metadata
    info: StreamInfo,
    /// Buffered events (in-order by seq)
    events: Vec<BufferedEvent>,
}

/// Storage layer for `ClaudeCode` sessions
pub struct ClaudeCodeStorage {
    pool: SqlitePool,
    arbor: Arc<ArborStorage>,
    /// In-memory buffers for active streams
    streams: RwLock<HashMap<StreamId, ActiveStreamBuffer>>,
}

impl ClaudeCodeStorage {
    /// Create a new `ClaudeCode` storage instance with a shared Arbor storage
    pub async fn new(
        config: ClaudeCodeStorageConfig,
        arbor: Arc<ArborStorage>,
    ) -> Result<Self, ClaudeCodeError> {
        let pool = init_sqlite_pool(config.db_path).await
            .map_err(|e| db_err("connect", sqlx::Error::Configuration(e.into())))?;

        let storage = Self {
            pool,
            arbor,
            streams: RwLock::new(HashMap::new()),
        };
        storage.run_migrations().await?;

        Ok(storage)
    }

    /// Run database migrations
    async fn run_migrations(&self) -> Result<(), ClaudeCodeError> {
        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS claudecode_sessions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                claude_session_id TEXT,
                loopback_session_id TEXT,
                tree_id TEXT NOT NULL,
                canonical_head TEXT NOT NULL,
                working_dir TEXT NOT NULL,
                model TEXT NOT NULL,
                system_prompt TEXT,
                mcp_config TEXT,
                loopback_enabled INTEGER NOT NULL DEFAULT 0,
                metadata TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS claudecode_messages (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                model_id TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                cost_usd REAL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY (session_id) REFERENCES claudecode_sessions(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_claudecode_sessions_name ON claudecode_sessions(name);
            CREATE INDEX IF NOT EXISTS idx_claudecode_sessions_tree ON claudecode_sessions(tree_id);
            CREATE INDEX IF NOT EXISTS idx_claudecode_messages_session ON claudecode_messages(session_id);

            CREATE TABLE IF NOT EXISTS claudecode_unknown_events (
                id TEXT PRIMARY KEY,
                session_id TEXT,
                event_type TEXT NOT NULL,
                data TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY (session_id) REFERENCES claudecode_sessions(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_claudecode_unknown_events_session ON claudecode_unknown_events(session_id);
            CREATE INDEX IF NOT EXISTS idx_claudecode_unknown_events_type ON claudecode_unknown_events(event_type);
            ",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| db_err("run migrations", e))?;

        // Migration: add loopback_session_id if not present
        let _ = sqlx::query(
            "ALTER TABLE claudecode_sessions ADD COLUMN loopback_session_id TEXT",
        )
        .execute(&self.pool)
        .await;

        // PLEXUS-6: the principal that created a session. NULL for a session
        // made in process (orcha) or before login: only an admin reaches it.
        let _ = sqlx::query("ALTER TABLE claudecode_sessions ADD COLUMN owner TEXT")
            .execute(&self.pool)
            .await;

        Ok(())
    }

    /// Get access to the underlying arbor storage
    pub fn arbor(&self) -> &ArborStorage {
        &self.arbor
    }

    // ========================================================================
    // Session CRUD Operations
    // ========================================================================

    /// Create a new `ClaudeCode` session with a new conversation tree
    pub async fn session_create(
        &self,
        name: String,
        working_dir: String,
        model: Model,
        system_prompt: Option<String>,
        mcp_config: Option<Value>,
        loopback_enabled: bool,
        claude_session_id: Option<String>,
        loopback_session_id: Option<String>,
        metadata: Option<Value>,
    ) -> Result<ClaudeCodeConfig, ClaudeCodeError> {
        let session_id = ClaudeCodeId::new_v4();
        let now = current_timestamp();

        // Create a new tree for this session
        let tree_id = self
            .arbor
            .tree_create(metadata.clone(), &session_id.to_string())
            .await
            .map_err(|e| ClaudeCodeError::Arbor(e.to_string()))?;

        // Get the root node as initial position
        let tree = self
            .arbor
            .tree_get(&tree_id)
            .await
            .map_err(|e| ClaudeCodeError::Arbor(e.to_string()))?;
        let head = Position::new(tree_id, tree.root);

        let metadata_json = metadata.as_ref().map(|m| serde_json::to_string(m).unwrap());
        let mcp_config_json = mcp_config.as_ref().map(|m| serde_json::to_string(m).unwrap());

        // Try inserting with the original name first
        let final_name = match sqlx::query(
            "INSERT INTO claudecode_sessions (id, name, claude_session_id, loopback_session_id, tree_id, canonical_head, working_dir, model, system_prompt, mcp_config, loopback_enabled, metadata, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(session_id.to_string())
        .bind(&name)
        .bind(&claude_session_id)
        .bind(&loopback_session_id)
        .bind(head.tree_id.to_string())
        .bind(head.node_id.to_string())
        .bind(&working_dir)
        .bind(model.as_str())
        .bind(&system_prompt)
        .bind(mcp_config_json.clone())
        .bind(loopback_enabled)
        .bind(metadata_json.clone())
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        {
            Ok(_) => name,
            Err(e) if e.to_string().contains("UNIQUE constraint failed") => {
                // Name collision - append #uuid to make it unique
                let unique_name = format!("{name}#{session_id}");

                sqlx::query(
                    "INSERT INTO claudecode_sessions (id, name, claude_session_id, loopback_session_id, tree_id, canonical_head, working_dir, model, system_prompt, mcp_config, loopback_enabled, metadata, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(session_id.to_string())
                .bind(&unique_name)
                .bind(&claude_session_id)
                .bind(&loopback_session_id)
                .bind(head.tree_id.to_string())
                .bind(head.node_id.to_string())
                .bind(&working_dir)
                .bind(model.as_str())
                .bind(&system_prompt)
                .bind(mcp_config_json)
                .bind(loopback_enabled)
                .bind(metadata_json)
                .bind(now)
                .bind(now)
                .execute(&self.pool)
                .await
                .map_err(|e| db_err("create session (unique name)", e))?;

                unique_name
            }
            Err(e) => return Err(db_err("create session", e)),
        };

        Ok(ClaudeCodeConfig {
            id: session_id,
            name: final_name,
            claude_session_id,
            loopback_session_id,
            head,
            working_dir,
            model,
            system_prompt,
            mcp_config,
            loopback_enabled,
            metadata,
            created_at: now,
            updated_at: now,
        })
    }

    /// Get a session by ID
    pub async fn session_get(&self, session_id: &ClaudeCodeId) -> Result<ClaudeCodeConfig, ClaudeCodeError> {
        let row = sqlx::query(
            "SELECT id, name, claude_session_id, loopback_session_id, tree_id, canonical_head, working_dir, model, system_prompt, mcp_config, loopback_enabled, metadata, created_at, updated_at
             FROM claudecode_sessions WHERE id = ?",
        )
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| db_err("fetch session", e))?
        .ok_or_else(|| ClaudeCodeError::SessionNotFound { identifier: session_id.to_string() })?;

        self.row_to_config(row)
    }

    /// Get a session by name (supports partial matching)
    pub async fn session_get_by_name(&self, name: &str) -> Result<ClaudeCodeConfig, ClaudeCodeError> {
        self.session_get_by_name_owned_by(name, None).await
    }

    /// PLEXUS-6: [`Self::session_get_by_name`] among the sessions `owner`
    /// created (`None`: among all). Another's session is not found — the
    /// same answer as one that does not exist, and never named in an
    /// ambiguity.
    pub async fn session_get_by_name_owned_by(&self, name: &str, owner: Option<&str>) -> Result<ClaudeCodeConfig, ClaudeCodeError> {
        let mine = if owner.is_some() { " AND owner = ?" } else { "" };
        // Try exact match first
        let exact = format!(
            "SELECT id, name, claude_session_id, loopback_session_id, tree_id, canonical_head, working_dir, model, system_prompt, mcp_config, loopback_enabled, metadata, created_at, updated_at
             FROM claudecode_sessions WHERE name = ?{mine}",
        );
        let mut q = sqlx::query(&exact).bind(name);
        if let Some(o) = owner {
            q = q.bind(o);
        }
        if let Some(row) = q
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| db_err("fetch session by name", e))?
        {
            return self.row_to_config(row);
        }

        // Try partial match
        let pattern = format!("{name}%");
        let partial = format!(
            "SELECT id, name, claude_session_id, loopback_session_id, tree_id, canonical_head, working_dir, model, system_prompt, mcp_config, loopback_enabled, metadata, created_at, updated_at
             FROM claudecode_sessions WHERE name LIKE ?{mine}",
        );
        let mut q = sqlx::query(&partial).bind(&pattern);
        if let Some(o) = owner {
            q = q.bind(o);
        }
        let rows = q
        .fetch_all(&self.pool)
        .await
        .map_err(|e| db_err("fetch session by pattern", e))?;

        match rows.len() {
            0 => Err(ClaudeCodeError::SessionNotFound { identifier: name.to_string() }),
            1 => self.row_to_config(rows.into_iter().next().unwrap()),
            _ => {
                let matches: Vec<String> = rows.iter().map(|r| r.get("name")).collect();
                Err(ClaudeCodeError::AmbiguousSession {
                    name: name.to_string(),
                    matches: matches.join(", "),
                })
            }
        }
    }

    /// List all sessions
    pub async fn session_list(&self) -> Result<Vec<ClaudeCodeInfo>, ClaudeCodeError> {
        self.session_list_owned_by(None).await
    }

    /// PLEXUS-6: the sessions `owner` created (`None`: all of them).
    pub async fn session_list_owned_by(&self, owner: Option<&str>) -> Result<Vec<ClaudeCodeInfo>, ClaudeCodeError> {
        let mine = if owner.is_some() { "WHERE owner = ? " } else { "" };
        let sql = format!(
            "SELECT id, name, claude_session_id, tree_id, canonical_head, working_dir, model, loopback_enabled, created_at
             FROM claudecode_sessions {mine}ORDER BY created_at DESC",
        );
        let mut q = sqlx::query(&sql);
        if let Some(o) = owner {
            q = q.bind(o);
        }
        let rows = q
        .fetch_all(&self.pool)
        .await
        .map_err(|e| db_err("list sessions", e))?;

        let sessions: Result<Vec<ClaudeCodeInfo>, ClaudeCodeError> = rows
            .iter()
            .map(|row| {
                let id_str: String = row.get("id");
                let tree_id_str: String = row.get("tree_id");
                let head_str: String = row.get("canonical_head");
                let model_str: String = row.get("model");
                let loopback: i32 = row.get("loopback_enabled");

                let tree_id = TreeId::parse_str(&tree_id_str)
                    .map_err(|e| parse_err("tree ID", e))?;
                let node_id = NodeId::parse_str(&head_str)
                    .map_err(|e| parse_err("node ID", e))?;
                let model = Model::from_str(&model_str)
                    .ok_or_else(|| parse_err("model", &model_str))?;

                Ok(ClaudeCodeInfo {
                    id: Uuid::parse_str(&id_str).map_err(|e| parse_err("session ID", e))?,
                    name: row.get("name"),
                    model,
                    head: Position::new(tree_id, node_id),
                    claude_session_id: row.get("claude_session_id"),
                    working_dir: row.get("working_dir"),
                    loopback_enabled: loopback != 0,
                    created_at: row.get("created_at"),
                })
            })
            .collect();

        sessions
    }

    /// Update session's canonical head and optionally the Claude session ID
    pub async fn session_update_head(
        &self,
        session_id: &ClaudeCodeId,
        new_head: NodeId,
        claude_session_id: Option<String>,
    ) -> Result<(), ClaudeCodeError> {
        let now = current_timestamp();

        let result = if let Some(claude_id) = claude_session_id {
            sqlx::query(
                "UPDATE claudecode_sessions SET canonical_head = ?, claude_session_id = ?, updated_at = ? WHERE id = ?",
            )
            .bind(new_head.to_string())
            .bind(claude_id)
            .bind(now)
            .bind(session_id.to_string())
            .execute(&self.pool)
            .await
        } else {
            sqlx::query(
                "UPDATE claudecode_sessions SET canonical_head = ?, updated_at = ? WHERE id = ?",
            )
            .bind(new_head.to_string())
            .bind(now)
            .bind(session_id.to_string())
            .execute(&self.pool)
            .await
        }
        .map_err(|e| db_err("update session head", e))?;

        if result.rows_affected() == 0 {
            return Err(ClaudeCodeError::SessionNotFound { identifier: session_id.to_string() });
        }

        Ok(())
    }

    /// Update the `loopback_session_id` after session creation
    pub async fn session_update_loopback_id(
        &self,
        session_id: &ClaudeCodeId,
        loopback_session_id: String,
    ) -> Result<(), ClaudeCodeError> {
        let now = current_timestamp();
        sqlx::query(
            "UPDATE claudecode_sessions SET loopback_session_id = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&loopback_session_id)
        .bind(now)
        .bind(session_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| db_err("update loopback_session_id", e))?;
        Ok(())
    }

    /// Update the `claude_session_id` (real Claude UUID) after first successful chat
    pub async fn session_update_claude_id(
        &self,
        session_id: &ClaudeCodeId,
        claude_session_id: String,
    ) -> Result<(), ClaudeCodeError> {
        let now = current_timestamp();
        sqlx::query(
            "UPDATE claudecode_sessions SET claude_session_id = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&claude_session_id)
        .bind(now)
        .bind(session_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| db_err("update claude_session_id", e))?;
        Ok(())
    }

    /// Update session configuration
    pub async fn session_update(
        &self,
        session_id: &ClaudeCodeId,
        name: Option<String>,
        model: Option<Model>,
        system_prompt: Option<Option<String>>,
        mcp_config: Option<Value>,
        metadata: Option<Value>,
    ) -> Result<(), ClaudeCodeError> {
        let now = current_timestamp();
        let current = self.session_get(session_id).await?;

        let new_name = name.unwrap_or(current.name);
        let new_model = model.unwrap_or(current.model);
        let new_prompt = system_prompt.unwrap_or(current.system_prompt);
        let new_mcp = mcp_config.or(current.mcp_config);
        let new_metadata = metadata.or(current.metadata);

        let mcp_json = new_mcp.as_ref().map(|m| serde_json::to_string(m).unwrap());
        let metadata_json = new_metadata.as_ref().map(|m| serde_json::to_string(m).unwrap());

        sqlx::query(
            "UPDATE claudecode_sessions SET name = ?, model = ?, system_prompt = ?, mcp_config = ?, metadata = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&new_name)
        .bind(new_model.as_str())
        .bind(&new_prompt)
        .bind(mcp_json)
        .bind(metadata_json)
        .bind(now)
        .bind(session_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| db_err("update session", e))?;

        Ok(())
    }

    /// Check if a session with this ID exists.
    ///
    /// Used by the `session` child gate on `ClaudeCode` (IR-18) to reject
    /// routing to a nonexistent session without materializing the full
    /// `ClaudeCodeConfig`.
    pub async fn session_exists(&self, session_id: &ClaudeCodeId) -> Result<bool, ClaudeCodeError> {
        let row = sqlx::query("SELECT 1 as present FROM claudecode_sessions WHERE id = ?")
            .bind(session_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| db_err("check session exists", e))?;
        Ok(row.is_some())
    }

    /// PLEXUS-6: record the principal that created a session.
    pub async fn session_set_owner(&self, session_id: &ClaudeCodeId, owner: &str) -> Result<(), ClaudeCodeError> {
        sqlx::query("UPDATE claudecode_sessions SET owner = ? WHERE id = ?")
            .bind(owner)
            .bind(session_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| db_err("set session owner", e))?;
        Ok(())
    }

    /// PLEXUS-6: the principal that created a session (`None`: made in
    /// process, or before login); not found when there is no such session.
    pub async fn session_owner(&self, session_id: &ClaudeCodeId) -> Result<Option<String>, ClaudeCodeError> {
        let row = sqlx::query("SELECT owner FROM claudecode_sessions WHERE id = ?")
            .bind(session_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| db_err("fetch session owner", e))?
            .ok_or_else(|| ClaudeCodeError::SessionNotFound { identifier: session_id.to_string() })?;
        Ok(row.get("owner"))
    }

    /// Enumerate every session ID known to storage.
    ///
    /// Used by the `session_ids` list-method on `ClaudeCode` (IR-18) to
    /// populate the `ChildRouter::list_children` stream under the
    /// `#[plexus_macros::child(list = "session_ids")]` opt-in.
    pub async fn list_session_ids(&self) -> Result<Vec<ClaudeCodeId>, ClaudeCodeError> {
        let rows = sqlx::query("SELECT id FROM claudecode_sessions ORDER BY created_at DESC")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| db_err("list session ids", e))?;

        rows.iter()
            .map(|row| {
                let id_str: String = row.get("id");
                Uuid::parse_str(&id_str).map_err(|e| parse_err("session ID", e))
            })
            .collect()
    }

    /// Delete a session (does not delete the arbor tree)
    pub async fn session_delete(&self, session_id: &ClaudeCodeId) -> Result<(), ClaudeCodeError> {
        let result = sqlx::query("DELETE FROM claudecode_sessions WHERE id = ?")
            .bind(session_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| db_err("delete session", e))?;

        if result.rows_affected() == 0 {
            return Err(ClaudeCodeError::SessionNotFound { identifier: session_id.to_string() });
        }

        Ok(())
    }

    // ========================================================================
    // Message Operations
    // ========================================================================

    /// Create a message and return it
    pub async fn message_create(
        &self,
        session_id: &ClaudeCodeId,
        role: MessageRole,
        content: String,
        model_id: Option<String>,
        input_tokens: Option<i64>,
        output_tokens: Option<i64>,
        cost_usd: Option<f64>,
    ) -> Result<Message, ClaudeCodeError> {
        let message_id = MessageId::new_v4();
        let now = current_timestamp();

        sqlx::query(
            "INSERT INTO claudecode_messages (id, session_id, role, content, model_id, input_tokens, output_tokens, cost_usd, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(message_id.to_string())
        .bind(session_id.to_string())
        .bind(role.as_str())
        .bind(&content)
        .bind(&model_id)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(cost_usd)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|e| db_err("create message", e))?;

        Ok(Message {
            id: message_id,
            session_id: *session_id,
            role,
            content,
            created_at: now,
            model_id,
            input_tokens,
            output_tokens,
            cost_usd,
        })
    }

    /// Create an ephemeral message (marked for deletion) and return it
    pub async fn message_create_ephemeral(
        &self,
        session_id: &ClaudeCodeId,
        role: MessageRole,
        content: String,
        model_id: Option<String>,
        input_tokens: Option<i64>,
        output_tokens: Option<i64>,
        cost_usd: Option<f64>,
    ) -> Result<Message, ClaudeCodeError> {
        let message_id = MessageId::new_v4();
        let now = current_timestamp();

        // Insert with a special marker in metadata or a separate flag
        // For now, we'll use a negative timestamp as a deletion marker
        // Messages with negative created_at are ephemeral and should be cleaned up
        let ephemeral_marker = -now;

        sqlx::query(
            "INSERT INTO claudecode_messages (id, session_id, role, content, model_id, input_tokens, output_tokens, cost_usd, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(message_id.to_string())
        .bind(session_id.to_string())
        .bind(role.as_str())
        .bind(&content)
        .bind(&model_id)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(cost_usd)
        .bind(ephemeral_marker)
        .execute(&self.pool)
        .await
        .map_err(|e| db_err("create ephemeral message", e))?;

        Ok(Message {
            id: message_id,
            session_id: *session_id,
            role,
            content,
            created_at: ephemeral_marker,
            model_id,
            input_tokens,
            output_tokens,
            cost_usd,
        })
    }

    /// Get a message by ID
    pub async fn message_get(&self, message_id: &MessageId) -> Result<Message, ClaudeCodeError> {
        let row = sqlx::query(
            "SELECT id, session_id, role, content, model_id, input_tokens, output_tokens, cost_usd, created_at
             FROM claudecode_messages WHERE id = ?",
        )
        .bind(message_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| db_err("fetch message", e))?
        .ok_or_else(|| ClaudeCodeError::SessionNotFound { identifier: format!("message:{message_id}") })?;

        self.row_to_message(row)
    }

    /// Resolve a message handle identifier to a Message
    /// Handle format: "msg-{message_id}:{role}:{name}"
    pub async fn resolve_message_handle(&self, identifier: &str) -> Result<Message, ClaudeCodeError> {
        let parts: Vec<&str> = identifier.splitn(3, ':').collect();
        if parts.len() < 2 {
            return Err(parse_err("message handle", format!("invalid format: {identifier}")));
        }

        let msg_part = parts[0];
        if !msg_part.starts_with("msg-") {
            return Err(parse_err("message handle", format!("invalid prefix: {identifier}")));
        }

        let message_id_str = &msg_part[4..];
        let message_id = Uuid::parse_str(message_id_str)
            .map_err(|e| parse_err("message ID in handle", e))?;

        self.message_get(&message_id).await
    }

    /// Create a handle for a message
    ///
    /// Format: `{plugin_id}@1.0.0::chat:msg-{id}:{role}:{name}`
    /// Uses `ClaudeCodeHandle` enum for type-safe handle creation.
    pub fn message_to_handle(message: &Message, name: &str) -> crate::types::Handle {
        ClaudeCodeHandle::Message {
            message_id: format!("msg-{}", message.id),
            role: message.role.as_str().to_string(),
            name: name.to_string(),
        }.to_handle()
    }

    // ========================================================================
    // Unknown Event Operations
    // ========================================================================

    /// Store an unknown event and return its ID (handle)
    pub async fn unknown_event_store(
        &self,
        session_id: Option<&ClaudeCodeId>,
        event_type: &str,
        data: &Value,
    ) -> Result<String, ClaudeCodeError> {
        let id = Uuid::new_v4().to_string();
        let now = current_timestamp();
        let data_json = serde_json::to_string(data)?;

        sqlx::query(
            "INSERT INTO claudecode_unknown_events (id, session_id, event_type, data, created_at)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(session_id.map(std::string::ToString::to_string))
        .bind(event_type)
        .bind(&data_json)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|e| db_err("store unknown event", e))?;

        Ok(id)
    }

    /// Retrieve an unknown event by ID
    pub async fn unknown_event_get(&self, id: &str) -> Result<(String, Value), ClaudeCodeError> {
        let row = sqlx::query(
            "SELECT event_type, data FROM claudecode_unknown_events WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| db_err("fetch unknown event", e))?
        .ok_or_else(|| ClaudeCodeError::SessionNotFound { identifier: format!("unknown_event:{id}") })?;

        let event_type: String = row.get("event_type");
        let data_json: String = row.get("data");
        let data: Value = serde_json::from_str(&data_json)?;

        Ok((event_type, data))
    }

    /// List unknown events by type (for analysis/debugging)
    pub async fn unknown_events_by_type(&self, event_type: &str) -> Result<Vec<(String, Value)>, ClaudeCodeError> {
        let rows = sqlx::query(
            "SELECT id, data FROM claudecode_unknown_events WHERE event_type = ? ORDER BY created_at DESC",
        )
        .bind(event_type)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| db_err("list unknown events", e))?;

        rows.iter()
            .map(|row| {
                let id: String = row.get("id");
                let data_json: String = row.get("data");
                let data: Value = serde_json::from_str(&data_json)?;
                Ok((id, data))
            })
            .collect()
    }

    // ========================================================================
    // Stream Management (in-memory buffer for async chat)
    // ========================================================================

    /// Create a new stream buffer for async chat
    pub async fn stream_create(
        &self,
        session_id: ClaudeCodeId,
    ) -> Result<StreamId, ClaudeCodeError> {
        let stream_id = StreamId::new_v4();
        let now = current_timestamp();

        let info = StreamInfo {
            stream_id,
            session_id,
            status: StreamStatus::Running,
            user_position: None,
            event_count: 0,
            read_position: 0,
            started_at: now,
            ended_at: None,
            error: None,
        };

        let buffer = ActiveStreamBuffer {
            info,
            events: Vec::new(),
        };

        let mut streams = self.streams.write().await;
        streams.insert(stream_id, buffer);

        Ok(stream_id)
    }

    /// Set the user position for a stream (called after user message is created)
    pub async fn stream_set_user_position(
        &self,
        stream_id: &StreamId,
        position: Position,
    ) -> Result<(), ClaudeCodeError> {
        let mut streams = self.streams.write().await;
        let buffer = streams.get_mut(stream_id)
            .ok_or_else(|| ClaudeCodeError::SessionNotFound { identifier: format!("stream:{stream_id}") })?;
        buffer.info.user_position = Some(position);
        Ok(())
    }

    /// Push an event to a stream buffer
    pub async fn stream_push_event(
        &self,
        stream_id: &StreamId,
        event: ChatEvent,
    ) -> Result<u64, ClaudeCodeError> {
        let now = current_timestamp();
        let mut streams = self.streams.write().await;
        let buffer = streams.get_mut(stream_id)
            .ok_or_else(|| ClaudeCodeError::SessionNotFound { identifier: format!("stream:{stream_id}") })?;

        let seq = buffer.info.event_count;
        buffer.events.push(BufferedEvent {
            seq,
            event,
            timestamp: now,
        });
        buffer.info.event_count += 1;

        Ok(seq)
    }

    /// Update stream status
    pub async fn stream_set_status(
        &self,
        stream_id: &StreamId,
        status: StreamStatus,
        error: Option<String>,
    ) -> Result<(), ClaudeCodeError> {
        let now = current_timestamp();
        let mut streams = self.streams.write().await;
        let buffer = streams.get_mut(stream_id)
            .ok_or_else(|| ClaudeCodeError::SessionNotFound { identifier: format!("stream:{stream_id}") })?;

        buffer.info.status = status;
        if status == StreamStatus::Complete || status == StreamStatus::Failed {
            buffer.info.ended_at = Some(now);
        }
        if let Some(e) = error {
            buffer.info.error = Some(e);
        }

        Ok(())
    }

    /// Get stream info
    pub async fn stream_get_info(&self, stream_id: &StreamId) -> Result<StreamInfo, ClaudeCodeError> {
        let streams = self.streams.read().await;
        let buffer = streams.get(stream_id)
            .ok_or_else(|| ClaudeCodeError::SessionNotFound { identifier: format!("stream:{stream_id}") })?;
        Ok(buffer.info.clone())
    }

    /// Poll events from a stream
    /// Returns events starting from `from_seq` up to `limit` events
    pub async fn stream_poll(
        &self,
        stream_id: &StreamId,
        from_seq: Option<u64>,
        limit: Option<usize>,
    ) -> Result<(StreamInfo, Vec<BufferedEvent>), ClaudeCodeError> {
        let mut streams = self.streams.write().await;
        let buffer = streams.get_mut(stream_id)
            .ok_or_else(|| ClaudeCodeError::SessionNotFound { identifier: format!("stream:{stream_id}") })?;

        let start = from_seq.unwrap_or(buffer.info.read_position) as usize;
        let max_events = limit.unwrap_or(100);

        let events: Vec<BufferedEvent> = buffer.events
            .iter()
            .skip(start)
            .take(max_events)
            .cloned()
            .collect();

        // Update read position to the end of what we returned
        if !events.is_empty() {
            let last_seq = events.last().unwrap().seq;
            buffer.info.read_position = last_seq + 1;
        }

        Ok((buffer.info.clone(), events))
    }

    /// List all active streams
    pub async fn stream_list(&self) -> Vec<StreamInfo> {
        let streams = self.streams.read().await;
        streams.values().map(|b| b.info.clone()).collect()
    }

    /// List active streams for a session
    pub async fn stream_list_for_session(&self, session_id: &ClaudeCodeId) -> Vec<StreamInfo> {
        let streams = self.streams.read().await;
        streams
            .values()
            .filter(|b| &b.info.session_id == session_id)
            .map(|b| b.info.clone())
            .collect()
    }

    /// Remove a completed/failed stream from memory
    /// Returns the final stream info if found
    pub async fn stream_cleanup(&self, stream_id: &StreamId) -> Option<StreamInfo> {
        let mut streams = self.streams.write().await;
        streams.remove(stream_id).map(|b| b.info)
    }

    /// Check if a stream exists
    pub async fn stream_exists(&self, stream_id: &StreamId) -> bool {
        let streams = self.streams.read().await;
        streams.contains_key(stream_id)
    }

    // ========================================================================
    // Arbor Rendering (Milestone 3)
    // ========================================================================

    /// Render arbor tree path into Claude API messages format
    ///
    /// Walks from start to end node, parsing `NodeEvent` JSON from each node,
    /// and groups into Claude messages array.
    ///
    /// # Algorithm
    /// 1. Get path from start to end via arbor
    /// 2. Parse each node's content as `NodeEvent`
    /// 3. Group into messages based on event type
    /// 4. Return messages array
    pub async fn render_messages(
        &self,
        tree_id: &TreeId,
        start: &NodeId,
        end: &NodeId,
    ) -> Result<Vec<ClaudeMessage>, ClaudeCodeError> {
        // 1. Get path from root to end (returns Vec<NodeId>)
        let node_ids = self
            .arbor
            .node_get_path(tree_id, end)
            .await
            .map_err(|e| ClaudeCodeError::Arbor(e.to_string()))?;

        // Find the starting node in the path
        let start_idx = node_ids
            .iter()
            .position(|id| id == start)
            .ok_or_else(|| ClaudeCodeError::Arbor("start node not found in path from root to end".to_string()))?;

        let node_ids = &node_ids[start_idx..];

        // 2. Get full node data for each node ID and group into messages
        let mut messages: Vec<ClaudeMessage> = Vec::new();
        let mut current_role: Option<String> = None;
        let mut current_content: Vec<ContentBlock> = Vec::new();

        for node_id in node_ids {
            // Get the full node data
            let node = self
                .arbor
                .node_get(tree_id, node_id)
                .await
                .map_err(|e| ClaudeCodeError::Arbor(e.to_string()))?;

            // Extract content from node data
            let content = match &node.data {
                NodeType::Text { content } => content,
                NodeType::External { .. } => {
                    // Skip external nodes for now
                    continue;
                }
            };

            // Skip empty nodes (like root)
            if content.trim().is_empty() {
                continue;
            }

            // Parse node content as NodeEvent
            let event: NodeEvent = serde_json::from_str(content)?;

            match event {
                NodeEvent::UserMessage { content } => {
                    // Flush previous message if any
                    if let Some(role) = current_role.take() {
                        if !current_content.is_empty() {
                            messages.push(ClaudeMessage {
                                role,
                                content: current_content.clone(),
                            });
                            current_content.clear();
                        }
                    }

                    // Start new user message
                    current_role = Some("user".to_string());
                    current_content.push(ContentBlock::Text { text: content });
                }

                NodeEvent::AssistantStart => {
                    // Flush previous message if any
                    if let Some(role) = current_role.take() {
                        if !current_content.is_empty() {
                            messages.push(ClaudeMessage {
                                role,
                                content: current_content.clone(),
                            });
                            current_content.clear();
                        }
                    }

                    // Start new assistant message
                    current_role = Some("assistant".to_string());
                }

                NodeEvent::ContentText { text } => {
                    // Add text content to current message
                    current_content.push(ContentBlock::Text { text });
                }

                NodeEvent::ContentToolUse { id, name, input } => {
                    // Add tool use to current message
                    current_content.push(ContentBlock::ToolUse { id, name, input });
                }

                NodeEvent::ContentThinking { thinking } => {
                    // Add thinking block to current message
                    current_content.push(ContentBlock::Thinking { thinking });
                }

                NodeEvent::UserToolResult {
                    tool_use_id,
                    content,
                    is_error,
                } => {
                    // Flush current assistant message if any
                    if let Some(role) = current_role.take() {
                        if !current_content.is_empty() {
                            messages.push(ClaudeMessage {
                                role,
                                content: current_content.clone(),
                            });
                            current_content.clear();
                        }
                    }

                    // Tool results become separate user messages
                    messages.push(ClaudeMessage {
                        role: "user".to_string(),
                        content: vec![ContentBlock::ToolResult {
                            tool_use_id,
                            content,
                            is_error,
                        }],
                    });
                }

                NodeEvent::AssistantComplete { usage: _ } => {
                    // Flush current message if any
                    if let Some(role) = current_role.take() {
                        if !current_content.is_empty() {
                            messages.push(ClaudeMessage {
                                role,
                                content: current_content.clone(),
                            });
                            current_content.clear();
                        }
                    }
                }

                // Debug/observability nodes — not part of conversation history
                NodeEvent::LaunchCommand { .. } | NodeEvent::ClaudeStderr { .. } => {}
            }
        }

        // Flush any remaining message
        if let Some(role) = current_role {
            if !current_content.is_empty() {
                messages.push(ClaudeMessage {
                    role,
                    content: current_content,
                });
            }
        }

        Ok(messages)
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    fn row_to_message(&self, row: sqlx::sqlite::SqliteRow) -> Result<Message, ClaudeCodeError> {
        let id_str: String = row.get("id");
        let session_id_str: String = row.get("session_id");
        let role_str: String = row.get("role");

        Ok(Message {
            id: Uuid::parse_str(&id_str).map_err(|e| parse_err("message ID", e))?,
            session_id: Uuid::parse_str(&session_id_str)
                .map_err(|e| parse_err("session ID", e))?,
            role: MessageRole::from_str(&role_str)
                .ok_or_else(|| parse_err("role", &role_str))?,
            content: row.get("content"),
            created_at: row.get("created_at"),
            model_id: row.get("model_id"),
            input_tokens: row.get("input_tokens"),
            output_tokens: row.get("output_tokens"),
            cost_usd: row.get("cost_usd"),
        })
    }

    fn row_to_config(&self, row: sqlx::sqlite::SqliteRow) -> Result<ClaudeCodeConfig, ClaudeCodeError> {
        let id_str: String = row.get("id");
        let tree_id_str: String = row.get("tree_id");
        let head_str: String = row.get("canonical_head");
        let model_str: String = row.get("model");
        let metadata_json: Option<String> = row.get("metadata");
        let mcp_config_json: Option<String> = row.get("mcp_config");
        let loopback: i32 = row.get("loopback_enabled");

        let tree_id = TreeId::parse_str(&tree_id_str)
            .map_err(|e| parse_err("tree ID", e))?;
        let node_id = NodeId::parse_str(&head_str)
            .map_err(|e| parse_err("node ID", e))?;
        let model = Model::from_str(&model_str)
            .ok_or_else(|| parse_err("model", &model_str))?;

        Ok(ClaudeCodeConfig {
            id: Uuid::parse_str(&id_str).map_err(|e| parse_err("session ID", e))?,
            name: row.get("name"),
            claude_session_id: row.get("claude_session_id"),
            loopback_session_id: row.try_get("loopback_session_id").ok().flatten(),
            head: Position::new(tree_id, node_id),
            working_dir: row.get("working_dir"),
            model,
            system_prompt: row.get("system_prompt"),
            mcp_config: mcp_config_json.and_then(|s| serde_json::from_str(&s).ok()),
            loopback_enabled: loopback != 0,
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

    /// Test stream buffer in-memory operations (no database needed)
    #[tokio::test]
    async fn test_stream_buffer_operations() {
        // Create a minimal storage with just the streams buffer
        let streams: RwLock<HashMap<StreamId, ActiveStreamBuffer>> = RwLock::new(HashMap::new());

        // Create a stream
        let stream_id = StreamId::new_v4();
        let session_id = ClaudeCodeId::new_v4();
        let now = current_timestamp();

        let info = StreamInfo {
            stream_id,
            session_id,
            status: StreamStatus::Running,
            user_position: None,
            event_count: 0,
            read_position: 0,
            started_at: now,
            ended_at: None,
            error: None,
        };

        let buffer = ActiveStreamBuffer {
            info,
            events: Vec::new(),
        };

        streams.write().await.insert(stream_id, buffer);

        // Push some events
        {
            let mut streams = streams.write().await;
            let buffer = streams.get_mut(&stream_id).unwrap();

            buffer.events.push(BufferedEvent {
                seq: 0,
                event: ChatEvent::Start {
                    id: session_id,
                    user_position: Position::new(TreeId::new(), NodeId::new()),
                },
                timestamp: now,
            });
            buffer.info.event_count = 1;

            buffer.events.push(BufferedEvent {
                seq: 1,
                event: ChatEvent::Content { text: "Hello".to_string() },
                timestamp: now,
            });
            buffer.info.event_count = 2;
        }

        // Poll events
        {
            let mut streams = streams.write().await;
            let buffer = streams.get_mut(&stream_id).unwrap();

            let events: Vec<_> = buffer.events.iter().take(10).cloned().collect();
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].seq, 0);
            assert_eq!(events[1].seq, 1);

            // Update read position
            buffer.info.read_position = 2;
        }

        // Poll again - should get nothing new
        {
            let streams = streams.read().await;
            let buffer = streams.get(&stream_id).unwrap();

            let events_len = buffer.events.iter()
                .skip(buffer.info.read_position as usize)
                .take(10)
                .count();
            assert_eq!(events_len, 0);
        }

        // Add more events
        {
            let mut streams = streams.write().await;
            let buffer = streams.get_mut(&stream_id).unwrap();

            buffer.events.push(BufferedEvent {
                seq: 2,
                event: ChatEvent::Content { text: " World".to_string() },
                timestamp: now,
            });
            buffer.info.event_count = 3;
        }

        // Poll again - should get the new event
        {
            let mut streams = streams.write().await;
            let buffer = streams.get_mut(&stream_id).unwrap();

            let events: Vec<_> = buffer.events.iter()
                .skip(buffer.info.read_position as usize)
                .take(10)
                .cloned()
                .collect();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].seq, 2);

            // Update read position
            buffer.info.read_position = 3;
        }

        // Test status transitions
        {
            let mut streams = streams.write().await;
            let buffer = streams.get_mut(&stream_id).unwrap();

            assert_eq!(buffer.info.status, StreamStatus::Running);

            buffer.info.status = StreamStatus::AwaitingPermission;
            assert_eq!(buffer.info.status, StreamStatus::AwaitingPermission);

            buffer.info.status = StreamStatus::Complete;
            buffer.info.ended_at = Some(current_timestamp());
            assert_eq!(buffer.info.status, StreamStatus::Complete);
            assert!(buffer.info.ended_at.is_some());
        }
    }

    #[test]
    fn test_stream_status_serialization() {
        // Test that StreamStatus serializes correctly for MCP
        let status = StreamStatus::AwaitingPermission;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"awaiting_permission\"");

        let status = StreamStatus::Running;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"running\"");
    }
}

// ============================================================================
// Milestone 3 Tests: render_messages()
// ============================================================================

#[cfg(test)]
mod render_tests {
    use super::*;
    use crate::activations::arbor::{ArborConfig, ArborError};

    /// Helper to create test storage with arbor
    async fn create_test_storage() -> (ClaudeCodeStorage, PathBuf) {
        let temp_dir = std::env::temp_dir();
        let test_id = Uuid::new_v4();
        let arbor_path = temp_dir.join(format!("test_arbor_{test_id}.db"));
        let claudecode_path = temp_dir.join(format!("test_claudecode_{test_id}.db"));

        let arbor_config = ArborConfig {
            db_path: arbor_path.clone(),
            scheduled_deletion_window: 604_800,
            archive_window: 2_592_000,
            auto_cleanup: false, // Disable for tests
            cleanup_interval: 3600,
        };
        let arbor = Arc::new(ArborStorage::new(arbor_config).await.unwrap());

        let claudecode_config = ClaudeCodeStorageConfig {
            db_path: claudecode_path.clone(),
        };
        let storage = ClaudeCodeStorage::new(claudecode_config, arbor)
            .await
            .unwrap();

        (storage, arbor_path)
    }

    /// Helper to create a node with `NodeEvent` content
    async fn create_event_node(
        arbor: &ArborStorage,
        tree_id: &TreeId,
        parent_id: &NodeId,
        event: &NodeEvent,
    ) -> Result<NodeId, ArborError> {
        let content = serde_json::to_string(event).map_err(|e| e.to_string())?;
        arbor.node_create_text(tree_id, Some(*parent_id), content, None).await
    }

    #[tokio::test]
    async fn test_render_simple_exchange() {
        let (storage, _temp_path) = create_test_storage().await;
        let arbor = storage.arbor();

        // Create test arbor tree
        let tree_id = arbor.tree_create(None, "test-session").await.unwrap();
        let tree = arbor.tree_get(&tree_id).await.unwrap();
        let root = tree.root;

        // Build tree: root -> user -> assistant_start -> content -> assistant_complete
        let user_node = create_event_node(
            arbor,
            &tree_id,
            &root,
            &NodeEvent::UserMessage {
                content: "Hello".to_string(),
            },
        )
        .await
        .unwrap();

        let assistant_start = create_event_node(
            arbor,
            &tree_id,
            &user_node,
            &NodeEvent::AssistantStart,
        )
        .await
        .unwrap();

        let content_node = create_event_node(
            arbor,
            &tree_id,
            &assistant_start,
            &NodeEvent::ContentText {
                text: "Hi there!".to_string(),
            },
        )
        .await
        .unwrap();

        let complete_node = create_event_node(
            arbor,
            &tree_id,
            &content_node,
            &NodeEvent::AssistantComplete { usage: None },
        )
        .await
        .unwrap();

        // Render from root to end
        let messages = storage
            .render_messages(&tree_id, &root, &complete_node)
            .await
            .unwrap();

        // Verify: 2 messages (user + assistant)
        assert_eq!(messages.len(), 2);

        // Verify user message
        assert_eq!(messages[0].role, "user");
        assert_eq!(messages[0].content.len(), 1);
        if let ContentBlock::Text { text } = &messages[0].content[0] {
            assert_eq!(text, "Hello");
        } else {
            panic!("Expected text content block");
        }

        // Verify assistant message
        assert_eq!(messages[1].role, "assistant");
        assert_eq!(messages[1].content.len(), 1);
        if let ContentBlock::Text { text } = &messages[1].content[0] {
            assert_eq!(text, "Hi there!");
        } else {
            panic!("Expected text content block");
        }
    }

    #[tokio::test]
    async fn test_render_with_tool_use() {
        let (storage, _temp_path) = create_test_storage().await;
        let arbor = storage.arbor();

        // Create test arbor tree
        let tree_id = arbor.tree_create(None, "test-tool-session").await.unwrap();
        let tree = arbor.tree_get(&tree_id).await.unwrap();
        let root = tree.root;

        // Build tree with tool use:
        // root -> user -> assistant_start -> content -> tool_use -> assistant_complete
        //              -> user_tool_result -> assistant_start -> content -> assistant_complete
        let user_node = create_event_node(
            arbor,
            &tree_id,
            &root,
            &NodeEvent::UserMessage {
                content: "Write a file".to_string(),
            },
        )
        .await
        .unwrap();

        let assistant_start = create_event_node(
            arbor,
            &tree_id,
            &user_node,
            &NodeEvent::AssistantStart,
        )
        .await
        .unwrap();

        let text_node = create_event_node(
            arbor,
            &tree_id,
            &assistant_start,
            &NodeEvent::ContentText {
                text: "I'll write that file.".to_string(),
            },
        )
        .await
        .unwrap();

        let tool_use_node = create_event_node(
            arbor,
            &tree_id,
            &text_node,
            &NodeEvent::ContentToolUse {
                id: "tool_123".to_string(),
                name: "write_file".to_string(),
                input: serde_json::json!({"path": "test.txt", "content": "hello"}),
            },
        )
        .await
        .unwrap();

        let assistant_complete = create_event_node(
            arbor,
            &tree_id,
            &tool_use_node,
            &NodeEvent::AssistantComplete { usage: None },
        )
        .await
        .unwrap();

        let tool_result = create_event_node(
            arbor,
            &tree_id,
            &assistant_complete,
            &NodeEvent::UserToolResult {
                tool_use_id: "tool_123".to_string(),
                content: "File written successfully".to_string(),
                is_error: false,
            },
        )
        .await
        .unwrap();

        let assistant_start2 = create_event_node(
            arbor,
            &tree_id,
            &tool_result,
            &NodeEvent::AssistantStart,
        )
        .await
        .unwrap();

        let content_node2 = create_event_node(
            arbor,
            &tree_id,
            &assistant_start2,
            &NodeEvent::ContentText {
                text: "Done!".to_string(),
            },
        )
        .await
        .unwrap();

        let complete_node2 = create_event_node(
            arbor,
            &tree_id,
            &content_node2,
            &NodeEvent::AssistantComplete { usage: None },
        )
        .await
        .unwrap();

        // Render full conversation
        let messages = storage
            .render_messages(&tree_id, &root, &complete_node2)
            .await
            .unwrap();

        // Expected: user, assistant (text + tool_use), user (tool_result), assistant (text)
        assert_eq!(messages.len(), 4, "Expected 4 messages, got {}", messages.len());

        assert_eq!(messages[0].role, "user");
        assert_eq!(messages[1].role, "assistant");
        assert_eq!(messages[1].content.len(), 2); // text + tool_use
        assert_eq!(messages[2].role, "user"); // tool result
        assert_eq!(messages[3].role, "assistant");
    }
}
