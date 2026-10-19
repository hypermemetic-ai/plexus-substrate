// PLEXUS-6: the 0.6 macros' legacy form (MACRO-13) — these activations keep
// `namespace = …` and their own type names; the name rule's trait would need
// one event type per activation. Its deprecation note is allowed here.
#![allow(deprecated)]
use super::storage::{ChangelogStorage, ChangelogStorageConfig};
use super::types::{ChangelogEntry, ChangelogEvent, QueueEntry};
use async_stream::stream;
use futures::Stream;
use std::sync::Arc;

/// Changelog activation - tracks Plexus RPC server hash changes and enforces documentation
#[derive(Clone)]
pub struct Changelog {
    storage: Arc<ChangelogStorage>,
}

impl Changelog {
    pub async fn new(config: ChangelogStorageConfig) -> Result<Self, String> {
        let storage = ChangelogStorage::new(config).await?;
        Ok(Self {
            storage: Arc::new(storage),
        })
    }

    /// Run startup check - called when Plexus RPC server starts
    /// Returns (`hash_changed`, `is_documented`, message)
    pub async fn startup_check(&self, current_hash: &str) -> Result<(bool, bool, String), String> {
        let previous_hash = self.storage.get_last_hash().await?;

        // Update the stored hash to current
        self.storage.set_last_hash(current_hash).await?;

        match previous_hash {
            None => {
                // First run - no previous hash
                Ok((false, true, "First startup - no previous hash recorded".to_string()))
            }
            Some(prev) if prev == current_hash => {
                // No change
                Ok((false, true, "Plexus hash unchanged".to_string()))
            }
            Some(prev) => {
                // Hash changed - check if documented
                let is_documented = self.storage.is_documented(current_hash).await?;
                let message = if is_documented {
                    let entry = self.storage.get_entry(current_hash).await?.unwrap();
                    format!(
                        "Plexus changed: {} -> {} (documented: {})",
                        prev, current_hash, entry.summary
                    )
                } else {
                    format!(
                        "UNDOCUMENTED PLEXUS CHANGE: {prev} -> {current_hash}. Add changelog entry for hash '{current_hash}'"
                    )
                };
                Ok((true, is_documented, message))
            }
        }
    }

    /// Get the storage for direct access (used by builder for startup check)
    pub fn storage(&self) -> &ChangelogStorage {
        &self.storage
    }
}

#[plexus_macros::activation(namespace = "changelog",
version = "1.0.0",
description = "Track and document plexus configuration changes")]
impl Changelog {
    /// Add a changelog entry for a plexus hash transition
    #[plexus_macros::method(description = "Add a changelog entry documenting a plexus hash change")]
    async fn add(
        &self,
        hash: String,
        summary: String,
        previous_hash: Option<String>,
        details: Option<Vec<String>>,
        author: Option<String>,
        queue_id: Option<String>,
    ) -> impl Stream<Item = ChangelogEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            let mut entry = ChangelogEntry::new(hash.clone(), previous_hash, summary);
            if let Some(d) = details {
                entry = entry.with_details(d);
            }
            if let Some(a) = author {
                entry = entry.with_author(a);
            }
            if let Some(q) = queue_id.clone() {
                entry = entry.with_queue_id(q);
            }

            match storage.add_entry(&entry).await {
                Ok(()) => {
                    // If this completes a queue item, mark it complete
                    if let Some(qid) = queue_id {
                        if let Err(e) = storage.complete_queue_entry(&qid, &hash).await {
                            tracing::warn!("Failed to complete queue entry {}: {}", qid, e);
                        }
                    }
                    yield ChangelogEvent::EntryAdded { entry };
                }
                Err(e) => {
                    tracing::error!("Failed to add changelog entry: {}", e);
                }
            }
        }
    }

    /// List all changelog entries
    #[plexus_macros::method(description = "List all changelog entries (newest first)")]
    async fn list(&self) -> impl Stream<Item = ChangelogEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            match storage.list_entries().await {
                Ok(entries) => {
                    yield ChangelogEvent::Entries { entries };
                }
                Err(e) => {
                    tracing::error!("Failed to list changelog entries: {}", e);
                }
            }
        }
    }

    /// Get a specific changelog entry by hash
    #[plexus_macros::method(description = "Get a changelog entry for a specific plexus hash")]
    async fn get(
        &self,
        hash: String,
    ) -> impl Stream<Item = ChangelogEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            match storage.get_entry(&hash).await {
                Ok(entry) => {
                    let is_documented = entry.is_some();
                    let previous_hash = storage.get_last_hash().await.ok().flatten();
                    yield ChangelogEvent::Status {
                        current_hash: hash,
                        previous_hash,
                        is_documented,
                        entry,
                    };
                }
                Err(e) => {
                    tracing::error!("Failed to get changelog entry: {}", e);
                }
            }
        }
    }

    /// Check current status - is the current plexus hash documented?
    #[plexus_macros::method(description = "Check if the current plexus configuration is documented")]
    async fn check(
        &self,
        current_hash: String,
    ) -> impl Stream<Item = ChangelogEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            let previous_hash = storage.get_last_hash().await.ok().flatten();
            let hash_changed = previous_hash.as_ref() != Some(&current_hash);
            let is_documented = storage.is_documented(&current_hash).await.unwrap_or(false);

            let message = if !hash_changed {
                "Plexus hash unchanged".to_string()
            } else if is_documented {
                "Plexus change is documented".to_string()
            } else {
                format!("UNDOCUMENTED: Add changelog entry for hash '{current_hash}'")
            };

            yield ChangelogEvent::StartupCheck {
                current_hash,
                previous_hash,
                hash_changed,
                is_documented,
                message,
            };
        }
    }

    // ========== Queue Methods ==========

    /// Add a planned change to the queue
    #[plexus_macros::method(description = "Queue a planned change that systems should implement. Tags identify which systems are affected (e.g., 'frontend', 'api', 'breaking')")]
    async fn queue_add(
        &self,
        description: String,
        tags: Option<Vec<String>>,
    ) -> impl Stream<Item = ChangelogEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            let id = uuid::Uuid::new_v4().to_string();
            let entry = QueueEntry::new(id, description, tags.unwrap_or_default());

            match storage.add_queue_entry(&entry).await {
                Ok(()) => {
                    yield ChangelogEvent::QueueAdded { entry };
                }
                Err(e) => {
                    tracing::error!("Failed to add queue entry: {}", e);
                }
            }
        }
    }

    /// List all queue entries, optionally filtered by tag
    #[plexus_macros::method(description = "List all queued changes, optionally filtered by tag")]
    async fn queue_list(
        &self,
        tag: Option<String>,
    ) -> impl Stream<Item = ChangelogEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            match storage.list_queue_entries(tag.as_deref()).await {
                Ok(entries) => {
                    yield ChangelogEvent::QueueEntries { entries };
                }
                Err(e) => {
                    tracing::error!("Failed to list queue entries: {}", e);
                }
            }
        }
    }

    /// List pending queue entries, optionally filtered by tag
    #[plexus_macros::method(description = "List pending queued changes that haven't been completed yet")]
    async fn queue_pending(
        &self,
        tag: Option<String>,
    ) -> impl Stream<Item = ChangelogEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            match storage.list_pending_queue_entries(tag.as_deref()).await {
                Ok(entries) => {
                    yield ChangelogEvent::QueueEntries { entries };
                }
                Err(e) => {
                    tracing::error!("Failed to list pending queue entries: {}", e);
                }
            }
        }
    }

    /// Get a specific queue entry by ID
    #[plexus_macros::method(description = "Get a specific queued change by its ID")]
    async fn queue_get(
        &self,
        id: String,
    ) -> impl Stream<Item = ChangelogEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            match storage.get_queue_entry(&id).await {
                Ok(entry) => {
                    yield ChangelogEvent::QueueItem { entry };
                }
                Err(e) => {
                    tracing::error!("Failed to get queue entry: {}", e);
                }
            }
        }
    }

    /// Mark a queue entry as complete
    #[plexus_macros::method(description = "Mark a queued change as complete, linking it to the hash where it was implemented")]
    async fn queue_complete(
        &self,
        id: String,
        hash: String,
    ) -> impl Stream<Item = ChangelogEvent> + Send + 'static {
        let storage = self.storage.clone();

        stream! {
            match storage.complete_queue_entry(&id, &hash).await {
                Ok(Some(entry)) => {
                    yield ChangelogEvent::QueueUpdated { entry };
                }
                Ok(None) => {
                    tracing::warn!("Queue entry not found: {}", id);
                }
                Err(e) => {
                    tracing::error!("Failed to complete queue entry: {}", e);
                }
            }
        }
    }
}
