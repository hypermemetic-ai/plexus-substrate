//! Session File Management Module
//!
//! Provides CRUD operations for Claude Code session files stored as JSONL
//! in `~/.claude/projects/<project>/<session-id>.jsonl`
//!
//! Also provides integration with arbor for importing/exporting sessions.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Component, Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::activations::arbor::{ArborStorage, TreeId};
use crate::activations::claudecode::types::NodeEvent;

// ═══════════════════════════════════════════════════════════════════════════
// SESSION FILE TYPES
// ═══════════════════════════════════════════════════════════════════════════

/// Event in a session JSONL file
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum SessionEvent {
    /// User message
    #[serde(rename = "user")]
    User {
        #[serde(flatten)]
        data: UserEvent,
    },
    /// Assistant message
    #[serde(rename = "assistant")]
    Assistant {
        #[serde(flatten)]
        data: AssistantEvent,
    },
    /// System message
    #[serde(rename = "system")]
    System {
        #[serde(flatten)]
        data: SystemEvent,
    },
    /// File history snapshot
    #[serde(rename = "file-history-snapshot")]
    FileHistorySnapshot {
        timestamp: Option<String>,
    },
    /// Queue operation
    #[serde(rename = "queue-operation")]
    QueueOperation {
        operation: String,
        timestamp: String,
        #[serde(rename = "sessionId")]
        session_id: String,
    },
    /// Unknown event type
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserEvent {
    pub uuid: String,
    #[serde(rename = "parentUuid")]
    pub parent_uuid: Option<String>,
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub timestamp: String,
    pub cwd: String,
    pub message: UserMessage,
    #[serde(rename = "isSidechain")]
    pub is_sidechain: Option<bool>,
    #[serde(rename = "gitBranch")]
    pub git_branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssistantEvent {
    pub uuid: String,
    #[serde(rename = "parentUuid")]
    pub parent_uuid: Option<String>,
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub timestamp: String,
    pub cwd: Option<String>,
    pub message: AssistantMessage,
    #[serde(rename = "requestId")]
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AssistantMessage {
    /// Full message with content array
    Full {
        role: String,
        content: Vec<ContentBlock>,
        model: Option<String>,
        id: Option<String>,
        #[serde(rename = "stop_reason")]
        stop_reason: Option<String>,
        usage: Option<Value>,
    },
    /// Simple string (for streaming events)
    Simple(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text { text: String },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        #[serde(rename = "tool_use_id")]
        tool_use_id: String,
        content: String,
        #[serde(rename = "is_error")]
        is_error: Option<bool>,
    },
    Thinking {
        thinking: String,
        signature: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemEvent {
    pub uuid: String,
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub timestamp: String,
    pub message: String,
}

// ═══════════════════════════════════════════════════════════════════════════
// SESSION FILE OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════

/// Get the base directory for Claude sessions
///
/// **The host default only.** A tenant's base comes from
/// [`StorageScope::claude_sessions_root`](crate::activations::storage::StorageScope::claude_sessions_root);
/// see [`SessionRoot`].
pub fn get_sessions_base_dir() -> PathBuf {
    dirs::home_dir()
        .expect("Could not determine home directory")
        .join(".claude")
        .join("projects")
}

/// **PLX-129 — the base directory `claudecode.sessions_*` joins onto, and the
/// check that join was missing.**
///
/// # The hole this closes
///
/// `sessions_list`, `sessions_get`, `sessions_import`, `sessions_export` and
/// `sessions_delete` all take a `project_path` **straight off the wire** and,
/// until now, joined it onto a process-global `~/.claude/projects` with no
/// validation at all. `project_path = "../../.ssh"` read the operator's keys;
/// `project_path = "../<other tenant>"` read another tenant's transcripts.
/// PLX-130 named `claudecode.sessions_*` as a row and PLX-127 could not
/// exclude `claudecode`, because it is the product.
///
/// Per-tenant sqlite files do nothing about this: it is not a database.
/// PLX-129's requirement 2 is exactly this case — *"every filesystem path a
/// per-tenant activation touches is tenant-prefixed. Storage is not only the
/// database."*
///
/// # The rule, which is PLX-130's measured one
///
/// `canonicalize` **resolves** a path; it does not **constrain** one, and a
/// rule compared against a non-canonical path silently no-ops. So:
///
/// 1. `project_path` must be **exactly one normal path component** — this
///    rejects `..`, `a/b`, `/etc` and the empty string before any filesystem
///    call, the same
///    step 2 `TenantRoot::resolve` keeps for the same reason;
/// 2. join, then **canonicalize the join and compare it to the canonicalized
///    base** — which is what catches a symlink planted at
///    `<base>/<project>` by whoever can write inside the base.
///
/// Step 2 is not redundant with step 1. Step 1 cannot see a symlink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRoot {
    base: PathBuf,
}

impl SessionRoot {
    /// Build a root over `base`. The base is an operator/composition choice —
    /// `StorageScope` produces it — and is never caller input.
    #[must_use]
    pub fn new(base: impl Into<PathBuf>) -> Self {
        Self { base: base.into() }
    }

    /// The host default, `~/.claude/projects`.
    #[must_use]
    pub fn host_default() -> Self {
        Self::new(get_sessions_base_dir())
    }

    /// The unresolved base directory.
    #[must_use]
    pub fn base(&self) -> &Path {
        &self.base
    }

    /// Step 1: is `project_path` usable as a single directory name?
    ///
    /// Kept separate and total so it can be tested on strings that never touch
    /// a filesystem. Note it deliberately allows `.` *inside* a name —
    /// real Claude Code project directories are slugs like
    /// `-Users-x-.config-thing` — while refusing the components `.` and `..`.
    #[must_use]
    pub fn segment_is_safe(project_path: &str) -> bool {
        if project_path.is_empty() || project_path.len() > 255 {
            return false;
        }
        if project_path.bytes().any(|b| b == 0 || b < 0x20 || b == 0x7f) {
            return false;
        }
        let p = Path::new(project_path);
        p.components().count() == 1
            && matches!(p.components().next(), Some(Component::Normal(_)))
    }

    /// `<base>/<project_path>`, proven to be inside `<base>`.
    ///
    /// `create` decides whether a missing directory is made (write paths) or
    /// refused (read paths). Either way the containment check runs against a
    /// canonical path.
    ///
    /// # Errors
    ///
    /// A message naming the rule that fired, never the resolved host path — a
    /// caller who guessed wrong should not learn the layout by guessing.
    pub fn project_dir(&self, project_path: &str, create: bool) -> Result<PathBuf, String> {
        if !Self::segment_is_safe(project_path) {
            return Err(format!(
                "project_path {project_path:?} is not a single directory name; \
                 `..`, `/` and absolute paths are refused"
            ));
        }
        if create {
            std::fs::create_dir_all(&self.base)
                .map_err(|e| format!("Failed to create session base: {e}"))?;
        }
        // Canonicalize the BASE first — trap 2. On macOS the base can easily
        // sit under /tmp, which is a symlink to /private/tmp, and a comparison
        // against the uncanonicalized spelling is a check that is not one.
        let base = std::fs::canonicalize(&self.base)
            .map_err(|e| format!("Session base is unresolvable: {e}"))?;
        let joined = base.join(project_path);
        if create {
            std::fs::create_dir_all(&joined)
                .map_err(|e| format!("Failed to create session directory: {e}"))?;
        }
        if !joined.exists() {
            // Nothing to constrain and nothing to read. Report the lexical
            // path, which is inside the base by construction of step 1.
            return Ok(joined);
        }
        // Resolve … then constrain — trap 1.
        let canonical = std::fs::canonicalize(&joined)
            .map_err(|e| format!("Session directory is unresolvable: {e}"))?;
        if canonical.starts_with(&base) {
            Ok(canonical)
        } else {
            Err(format!(
                "project_path {project_path:?} resolves outside the session root"
            ))
        }
    }

    /// Get the path to a session file, contained.
    ///
    /// # Errors
    ///
    /// As [`SessionRoot::project_dir`], plus a refusal if `session_id` is not
    /// a single safe component — it is caller input too, and a `session_id` of
    /// `../../x` would otherwise climb straight back out of the directory the
    /// step above just proved.
    pub fn session_path(
        &self,
        project_path: &str,
        session_id: &str,
        create: bool,
    ) -> Result<PathBuf, String> {
        if !Self::segment_is_safe(session_id) {
            return Err(format!(
                "session_id {session_id:?} is not a single path component"
            ));
        }
        Ok(self
            .project_dir(project_path, create)?
            .join(format!("{session_id}.jsonl")))
    }
}

impl Default for SessionRoot {
    fn default() -> Self {
        Self::host_default()
    }
}

/// List all sessions for a project
pub async fn list_sessions(root: &SessionRoot, project_path: &str) -> Result<Vec<String>, String> {
    let dir = root.project_dir(project_path, false)?;

    if !dir.exists() {
        return Ok(vec![]);
    }

    let mut sessions = vec![];
    let mut entries = fs::read_dir(&dir)
        .await
        .map_err(|e| format!("Failed to read directory: {e}"))?;

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| format!("Failed to read entry: {e}"))?
    {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                sessions.push(stem.to_string());
            }
        }
    }

    Ok(sessions)
}

/// Read all events from a session file
pub async fn read_session(
    root: &SessionRoot,
    project_path: &str,
    session_id: &str,
) -> Result<Vec<SessionEvent>, String> {
    let path = root.session_path(project_path, session_id, false)?;

    if !path.exists() {
        return Err(format!("Session not found: {session_id}"));
    }

    let file = fs::File::open(&path)
        .await
        .map_err(|e| format!("Failed to open session file: {e}"))?;

    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut events = vec![];

    while let Some(line) = lines
        .next_line()
        .await
        .map_err(|e| format!("Failed to read line: {e}"))?
    {
        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<SessionEvent>(&line) {
            Ok(event) => events.push(event),
            Err(e) => {
                eprintln!("Warning: Failed to parse event: {} - {}", e, &line[..line.len().min(100)]);
                // Continue reading despite parse errors
            }
        }
    }

    Ok(events)
}

/// Append an event to a session file
pub async fn append_to_session(
    root: &SessionRoot,
    project_path: &str,
    session_id: &str,
    event: &SessionEvent,
) -> Result<(), String> {
    let path = root.session_path(project_path, session_id, true)?;

    // Ensure directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| format!("Failed to create directory: {e}"))?;
    }

    let json = serde_json::to_string(event).map_err(|e| format!("Failed to serialize event: {e}"))?;

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .map_err(|e| format!("Failed to open session file: {e}"))?;

    file.write_all(json.as_bytes())
        .await
        .map_err(|e| format!("Failed to write to session: {e}"))?;
    file.write_all(b"\n")
        .await
        .map_err(|e| format!("Failed to write newline: {e}"))?;

    Ok(())
}

/// Delete a session file
pub async fn delete_session(
    root: &SessionRoot,
    project_path: &str,
    session_id: &str,
) -> Result<(), String> {
    let path = root.session_path(project_path, session_id, false)?;

    if !path.exists() {
        return Err(format!("Session not found: {session_id}"));
    }

    fs::remove_file(&path)
        .await
        .map_err(|e| format!("Failed to delete session: {e}"))?;

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// ARBOR INTEGRATION
// ═══════════════════════════════════════════════════════════════════════════

/// Import a session file into an arbor tree
///
/// Creates a tree structure matching the session conversation flow
pub async fn import_to_arbor(
    arbor: &ArborStorage,
    root: &SessionRoot,
    project_path: &str,
    session_id: &str,
    owner_id: &str,
) -> Result<TreeId, String> {
    let events = read_session(root, project_path, session_id).await?;

    // Create new tree
    let metadata = serde_json::json!({
        "source": "claude_session_import",
        "session_id": session_id,
        "project_path": project_path,
    });

    let tree_id = arbor
        .tree_create(Some(metadata), owner_id)
        .await
        .map_err(|e| e.to_string())?;

    let tree = arbor.tree_get(&tree_id).await.map_err(|e| e.to_string())?;
    let mut current_parent = tree.root;

    // Process each event
    for event in events {
        match event {
            SessionEvent::User { data } => {
                // Create user message node
                let node_event = NodeEvent::UserMessage {
                    content: data.message.content.clone(),
                };
                let json =
                    serde_json::to_string(&node_event).map_err(|e| format!("Serialize error: {e}"))?;

                let node_id = arbor
                    .node_create_text(&tree_id, Some(current_parent), json, None)
                    .await
                    .map_err(|e| e.to_string())?;

                current_parent = node_id;
            }
            SessionEvent::Assistant { data } => {
                // Create assistant start node
                let start_event = NodeEvent::AssistantStart;
                let json = serde_json::to_string(&start_event)
                    .map_err(|e| format!("Serialize error: {e}"))?;

                let start_node = arbor
                    .node_create_text(&tree_id, Some(current_parent), json, None)
                    .await
                    .map_err(|e| e.to_string())?;

                current_parent = start_node;

                // Process assistant message content
                if let AssistantMessage::Full { content, .. } = data.message {
                    for block in content {
                        let node_event = match block {
                            ContentBlock::Text { text } => NodeEvent::ContentText { text },
                            ContentBlock::ToolUse { id, name, input } => {
                                NodeEvent::ContentToolUse { id, name, input }
                            }
                            ContentBlock::ToolResult {
                                tool_use_id,
                                content,
                                is_error,
                            } => NodeEvent::UserToolResult {
                                tool_use_id,
                                content,
                                is_error: is_error.unwrap_or(false),
                            },
                            ContentBlock::Thinking { thinking, .. } => {
                                NodeEvent::ContentThinking { thinking }
                            }
                        };

                        let json = serde_json::to_string(&node_event)
                            .map_err(|e| format!("Serialize error: {e}"))?;

                        let node_id = arbor
                            .node_create_text(&tree_id, Some(current_parent), json, None)
                            .await
                            .map_err(|e| e.to_string())?;

                        current_parent = node_id;
                    }
                }

                // Create assistant complete node
                let complete_event = NodeEvent::AssistantComplete { usage: None };
                let json = serde_json::to_string(&complete_event)
                    .map_err(|e| format!("Serialize error: {e}"))?;

                let complete_node = arbor
                    .node_create_text(&tree_id, Some(current_parent), json, None)
                    .await
                    .map_err(|e| e.to_string())?;

                current_parent = complete_node;
            }
            _ => {
                // Skip other event types for now
            }
        }
    }

    Ok(tree_id)
}

/// Export an arbor tree to a session JSONL file
///
/// Converts arbor node structure back to claude session format
pub async fn export_from_arbor(
    arbor: &ArborStorage,
    root: &SessionRoot,
    tree_id: &TreeId,
    project_path: &str,
    session_id: &str,
) -> Result<(), String> {
    use crate::activations::arbor::NodeType;
    use crate::activations::claudecode::types::NodeEvent;

    let tree = arbor.tree_get(tree_id).await.map_err(|e| e.to_string())?;

    // Helper to traverse tree in DFS order
    let traverse_dfs = |tree: &crate::activations::arbor::Tree| -> Vec<TreeId> {
        use std::collections::HashMap;

        // Build child map
        let mut children: HashMap<TreeId, Vec<TreeId>> = HashMap::new();
        for (node_id, node) in &tree.nodes {
            if let Some(parent_id) = &node.parent {
                children.entry(*parent_id)
                    .or_default()
                    .push(*node_id);
            }
        }

        // DFS traversal
        let mut visited = Vec::new();
        let mut stack = vec![tree.root];

        while let Some(current) = stack.pop() {
            visited.push(current);
            if let Some(child_ids) = children.get(&current) {
                // Reverse to maintain left-to-right order
                for child_id in child_ids.iter().rev() {
                    stack.push(*child_id);
                }
            }
        }

        visited
    };

    let node_ids = traverse_dfs(&tree);

    // Parse NodeEvents and aggregate into SessionEvents
    let mut session_events = Vec::new();
    let mut current_assistant_blocks: Vec<ContentBlock> = Vec::new();
    let mut in_assistant = false;

    for node_id in node_ids {
        let node = tree.nodes.get(&node_id).unwrap();

        if let NodeType::Text { content } = &node.data {
            // Skip empty content (like root node)
            if content.is_empty() {
                continue;
            }

            // Try to parse as NodeEvent
            let node_event: NodeEvent = match serde_json::from_str(content) {
                Ok(e) => e,
                Err(_) => continue, // Skip nodes that aren't NodeEvents
            };

            match node_event {
                NodeEvent::UserMessage { content } => {
                    // Complete any pending assistant message
                    if in_assistant && !current_assistant_blocks.is_empty() {
                        session_events.push(build_assistant_event(
                            std::mem::take(&mut current_assistant_blocks),
                            session_id,
                        ));
                        in_assistant = false;
                    }

                    // Create user event
                    session_events.push(SessionEvent::User {
                        data: UserEvent {
                            uuid: uuid::Uuid::new_v4().to_string(),
                            parent_uuid: None,
                            session_id: session_id.to_string(),
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            cwd: std::env::current_dir()
                                .ok()
                                .and_then(|p| p.to_str().map(String::from))
                                .unwrap_or_default(),
                            message: UserMessage {
                                role: "user".to_string(),
                                content,
                            },
                            is_sidechain: None,
                            git_branch: None,
                        },
                    });
                }

                NodeEvent::AssistantStart => {
                    in_assistant = true;
                    current_assistant_blocks.clear();
                }

                NodeEvent::ContentText { text } => {
                    if in_assistant {
                        current_assistant_blocks.push(ContentBlock::Text { text });
                    }
                }

                NodeEvent::ContentToolUse { id, name, input } => {
                    if in_assistant {
                        current_assistant_blocks.push(ContentBlock::ToolUse { id, name, input });
                    }
                }

                NodeEvent::ContentThinking { thinking } => {
                    if in_assistant {
                        current_assistant_blocks.push(ContentBlock::Thinking {
                            thinking,
                            signature: None,
                        });
                    }
                }

                NodeEvent::UserToolResult {
                    tool_use_id,
                    content,
                    is_error,
                } => {
                    // Complete any pending assistant message
                    if in_assistant && !current_assistant_blocks.is_empty() {
                        session_events.push(build_assistant_event(
                            std::mem::take(&mut current_assistant_blocks),
                            session_id,
                        ));
                        in_assistant = false;
                    }

                    // Tool results become user messages in Claude API
                    let content_str = serde_json::to_string(&vec![ContentBlock::ToolResult {
                        tool_use_id,
                        content,
                        is_error: Some(is_error),
                    }])
                    .unwrap_or_default();

                    session_events.push(SessionEvent::User {
                        data: UserEvent {
                            uuid: uuid::Uuid::new_v4().to_string(),
                            parent_uuid: None,
                            session_id: session_id.to_string(),
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            cwd: std::env::current_dir()
                                .ok()
                                .and_then(|p| p.to_str().map(String::from))
                                .unwrap_or_default(),
                            message: UserMessage {
                                role: "user".to_string(),
                                content: content_str,
                            },
                            is_sidechain: None,
                            git_branch: None,
                        },
                    });
                }

                NodeEvent::AssistantComplete { .. } => {
                    if in_assistant && !current_assistant_blocks.is_empty() {
                        session_events.push(build_assistant_event(
                            std::mem::take(&mut current_assistant_blocks),
                            session_id,
                        ));
                        in_assistant = false;
                    }
                }

                // Debug/observability nodes — not part of conversation history
                NodeEvent::LaunchCommand { .. } | NodeEvent::ClaudeStderr { .. } => {}
            }
        }
    }

    // Complete any pending assistant message
    if in_assistant && !current_assistant_blocks.is_empty() {
        session_events.push(build_assistant_event(current_assistant_blocks, session_id));
    }

    // Write to session file
    for event in session_events {
        append_to_session(root, project_path, session_id, &event).await?;
    }

    Ok(())
}

/// Helper to build `AssistantEvent` from content blocks
fn build_assistant_event(blocks: Vec<ContentBlock>, session_id: &str) -> SessionEvent {
    SessionEvent::Assistant {
        data: AssistantEvent {
            uuid: uuid::Uuid::new_v4().to_string(),
            parent_uuid: None,
            session_id: session_id.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            cwd: std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(String::from)),
            message: AssistantMessage::Full {
                role: "assistant".to_string(),
                content: blocks,
                model: None,
                id: None,
                stop_reason: None,
                usage: None,
            },
            request_id: None,
        },
    }
}
