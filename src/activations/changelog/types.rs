use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A changelog entry documenting a plexus hash transition
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ChangelogEntry {
    /// The `plexus_hash` this entry documents
    pub hash: String,

    /// The previous hash this transitioned from (None for initial entry)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_hash: Option<String>,

    /// Unix timestamp when this entry was added
    pub created_at: i64,

    /// Short summary of changes (one line)
    pub summary: String,

    /// Detailed list of changes (bullet points)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<String>,

    /// Who/what added this entry
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,

    /// Reference to a queue item this changelog completes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_id: Option<String>,
}

/// Status of a queued change
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueueStatus {
    /// Change is planned but not yet implemented
    Pending,
    /// Change has been implemented and documented
    Completed,
}

/// A queued change - a planned modification that systems should implement
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QueueEntry {
    /// Unique identifier for this queue item
    pub id: String,

    /// Description of the planned change
    pub description: String,

    /// Tags to identify which systems this change affects (e.g., "frontend", "api", "breaking")
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,

    /// Unix timestamp when this was queued
    pub created_at: i64,

    /// Current status of the queue item
    pub status: QueueStatus,

    /// The hash where this change was implemented (set when completed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_hash: Option<String>,

    /// Unix timestamp when this was completed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<i64>,
}

impl QueueEntry {
    pub fn new(id: String, description: String, tags: Vec<String>) -> Self {
        Self {
            id,
            description,
            tags,
            created_at: chrono::Utc::now().timestamp(),
            status: QueueStatus::Pending,
            completed_hash: None,
            completed_at: None,
        }
    }

    pub fn complete(mut self, hash: String) -> Self {
        self.status = QueueStatus::Completed;
        self.completed_hash = Some(hash);
        self.completed_at = Some(chrono::Utc::now().timestamp());
        self
    }
}

impl ChangelogEntry {
    pub fn new(hash: String, previous_hash: Option<String>, summary: String) -> Self {
        Self {
            hash,
            previous_hash,
            created_at: chrono::Utc::now().timestamp(),
            summary,
            details: Vec::new(),
            author: None,
            queue_id: None,
        }
    }

    pub fn with_details(mut self, details: Vec<String>) -> Self {
        self.details = details;
        self
    }

    pub fn with_author(mut self, author: String) -> Self {
        self.author = Some(author);
        self
    }

    pub fn with_queue_id(mut self, queue_id: String) -> Self {
        self.queue_id = Some(queue_id);
        self
    }
}

/// The single error type for the changelog activation.
///
/// PLX-118: changelog never had a per-method `Ok`/`Err` enum. Its failure
/// channel was worse — `tracing::error!` plus an **empty** stream, so a caller
/// saw a successful turn with no data and no way to tell a failure from a
/// no-op. Every method now returns `Result<ChangelogEvent, ChangelogError>`, so
/// a storage failure lands where PLX-110 put it: the terminal, with
/// `StopKind::Failed` and a structured payload. Shaping happens only in the one
/// `From` impl below.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum ChangelogError {
    /// A changelog storage operation failed.
    #[error("{0}")]
    Storage(String),

    /// `queue_complete` was given an id no queue entry has.
    #[error("Queue entry not found: {0}")]
    QueueEntryNotFound(String),
}

impl From<String> for ChangelogError {
    fn from(message: String) -> Self {
        Self::Storage(message)
    }
}

impl From<ChangelogError> for plexus_core::runtime::TurnError {
    fn from(e: ChangelogError) -> Self {
        let code = match e {
            ChangelogError::Storage(_) => "changelog.storage_error",
            ChangelogError::QueueEntryNotFound(_) => "changelog.queue_entry_not_found",
        };
        plexus_core::runtime::TurnError::structured(code, e.to_string(), &e)
    }
}

/// Events emitted by changelog operations
///
/// PLX-118: this is a domain event type, not a result type — it never carried an
/// error variant — so it survives verbatim as the terminal value of each unary
/// method, exactly as `EchoEvent` did for `echo.ping` (PLX-110).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChangelogEvent {
    /// Entry was added
    EntryAdded { entry: ChangelogEntry },

    /// List of entries
    Entries { entries: Vec<ChangelogEntry> },

    /// Current state check result
    Status {
        current_hash: String,
        previous_hash: Option<String>,
        is_documented: bool,
        entry: Option<ChangelogEntry>,
    },

    /// Startup check result
    StartupCheck {
        current_hash: String,
        previous_hash: Option<String>,
        hash_changed: bool,
        is_documented: bool,
        message: String,
    },

    /// Queue item was added
    QueueAdded { entry: QueueEntry },

    /// Queue item was updated (e.g., marked complete)
    QueueUpdated { entry: QueueEntry },

    /// List of queue items
    QueueEntries { entries: Vec<QueueEntry> },

    /// Single queue item
    QueueItem { entry: Option<QueueEntry> },
}
