use super::storage::{OrchaStorage, OrchaStorageConfig};
use super::types::*;
use std::path::PathBuf;
use std::sync::Arc;

/// Helper to create a test storage instance with a unique database
async fn create_test_storage() -> Arc<OrchaStorage> {
    // Use /tmp for test databases to avoid permission issues.
    //
    // PLX-133/PLX-145: a nanosecond timestamp is NOT unique under test
    // parallelism — two tests starting in the same nanosecond shared one file
    // and raced each other's `ALTER TABLE`, failing with "duplicate column
    // name: agent_mode". It cost a spurious red board twice. A v4 uuid cannot
    // collide, so the flake is unrepresentable rather than unlikely.
    let test_db = format!("/tmp/orcha_test_{}.db", uuid::Uuid::new_v4());
    let config = OrchaStorageConfig {
        db_path: PathBuf::from(&test_db),
    };
    Arc::new(OrchaStorage::new(config).await.expect("Failed to create test storage"))
}

// ═══════════════════════════════════════════════════════════════════════════
// Session Management Tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_create_session() {
    let storage = create_test_storage().await;

    let session_id = format!("test-{}", uuid::Uuid::new_v4());
    let session = storage
        .create_session(
            session_id.clone(),
            "sonnet".to_string(),
            "/workspace".to_string(),
            None,
            3,
            AgentMode::Single,
            None,
        )
        .await
        .expect("Failed to create session");

    assert_eq!(session.session_id, session_id);
    assert_eq!(session.model, "sonnet");
    assert_eq!(session.max_retries, 3);
    assert_eq!(session.retry_count, 0);
    assert_eq!(session.agent_mode, AgentMode::Single);
    assert!(matches!(session.state, SessionState::Idle));

    // Verify session can be retrieved
    let retrieved = storage
        .get_session(&session_id)
        .await
        .expect("Failed to get session");
    assert_eq!(retrieved.session_id, session_id);
}

#[tokio::test]
async fn test_create_multi_agent_session() {
    let storage = create_test_storage().await;

    let session_id = format!("test-multi-{}", uuid::Uuid::new_v4());
    let session = storage
        .create_session(
            session_id.clone(),
            "sonnet".to_string(),
            "/workspace".to_string(),
            None,
            3,
            AgentMode::Multi,
            None,
        )
        .await
        .expect("Failed to create session");

    assert_eq!(session.agent_mode, AgentMode::Multi);
    assert!(session.primary_agent_id.is_none());
}

#[tokio::test]
async fn test_list_sessions() {
    let storage = create_test_storage().await;

    // Create multiple sessions
    let id1 = format!("test-1-{}", uuid::Uuid::new_v4());
    let id2 = format!("test-2-{}", uuid::Uuid::new_v4());

    storage
        .create_session(id1.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Single, None)
        .await
        .expect("Failed to create session 1");
    storage
        .create_session(id2.clone(), "opus".to_string(), "/workspace".to_string(), None, 3, AgentMode::Multi, None)
        .await
        .expect("Failed to create session 2");

    // List sessions
    let sessions = storage.list_sessions().await;

    assert_eq!(sessions.len(), 2);
    assert!(sessions.iter().any(|s| s.session_id == id1));
    assert!(sessions.iter().any(|s| s.session_id == id2));
}

#[tokio::test]
async fn test_get_nonexistent_session() {
    let storage = create_test_storage().await;

    let result = storage.get_session(&"nonexistent".to_string()).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not found"));
}

#[tokio::test]
async fn test_delete_session() {
    let storage = create_test_storage().await;

    let session_id = format!("test-delete-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Single, None)
        .await
        .expect("Failed to create session");

    // Verify session exists
    assert!(storage.get_session(&session_id).await.is_ok());

    // Delete session
    storage.delete_session(&session_id).await.expect("Failed to delete session");

    // Verify session no longer exists
    assert!(storage.get_session(&session_id).await.is_err());
}

// ═══════════════════════════════════════════════════════════════════════════
// Session State Management Tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_update_session_state() {
    let storage = create_test_storage().await;

    let session_id = format!("test-state-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Single, None)
        .await
        .expect("Failed to create session");

    // Update to Running state
    let running_state = SessionState::Running {
        stream_id: "stream-123".to_string(),
        sequence: 0,
        active_agents: 0,
        completed_agents: 0,
        failed_agents: 0,
    };
    storage
        .update_state(&session_id, running_state.clone())
        .await
        .expect("Failed to update state");

    // Verify state was updated
    let session = storage.get_session(&session_id).await.expect("Failed to get session");
    assert_eq!(session.state, running_state);

    // Update to Complete state
    storage
        .update_state(&session_id, SessionState::Complete)
        .await
        .expect("Failed to update to complete");

    let session = storage.get_session(&session_id).await.expect("Failed to get session");
    assert!(matches!(session.state, SessionState::Complete));
}

#[tokio::test]
async fn test_update_state_failure() {
    let storage = create_test_storage().await;

    let session_id = format!("test-fail-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Single, None)
        .await
        .expect("Failed to create session");

    // Update to Failed state
    let failed_state = SessionState::Failed {
        error: "Test error".to_string(),
    };
    storage
        .update_state(&session_id, failed_state.clone())
        .await
        .expect("Failed to update state");

    let session = storage.get_session(&session_id).await.expect("Failed to get session");
    assert_eq!(session.state, failed_state);
}

#[tokio::test]
async fn test_update_state_nonexistent_session() {
    let storage = create_test_storage().await;

    let result = storage
        .update_state(&"nonexistent".to_string(), SessionState::Complete)
        .await;

    assert!(result.is_ok()); // Update doesn't fail, it just doesn't update anything
}

// ═══════════════════════════════════════════════════════════════════════════
// Retry Logic Tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_increment_retry_count() {
    let storage = create_test_storage().await;

    let session_id = format!("test-retry-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Single, None)
        .await
        .expect("Failed to create session");

    // Increment retry count
    let retry_count = storage
        .increment_retry(&session_id)
        .await
        .expect("Failed to increment retry");

    assert_eq!(retry_count, 1);

    // Verify session retry count was updated
    let session = storage.get_session(&session_id).await.expect("Failed to get session");
    assert_eq!(session.retry_count, 1);
    assert_eq!(session.max_retries, 3);
}

#[tokio::test]
async fn test_retry_count_multiple_increments() {
    let storage = create_test_storage().await;

    let session_id = format!("test-retries-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Single, None)
        .await
        .expect("Failed to create session");

    // Increment multiple times
    for expected in 1..=5 {
        let retry_count = storage
            .increment_retry(&session_id)
            .await
            .expect("Failed to increment retry");
        assert_eq!(retry_count, expected);
    }

    // Verify final count
    let session = storage.get_session(&session_id).await.expect("Failed to get session");
    assert_eq!(session.retry_count, 5);
}

#[tokio::test]
async fn test_increment_retry_nonexistent_session() {
    let storage = create_test_storage().await;

    let result = storage.increment_retry(&"nonexistent".to_string()).await;
    assert!(result.is_err());
}

// ═══════════════════════════════════════════════════════════════════════════
// Multi-Agent Tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_create_agent() {
    let storage = create_test_storage().await;

    let session_id = format!("test-agent-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Multi, None)
        .await
        .expect("Failed to create session");

    // Create an agent
    let subtask = "Write a function to calculate fibonacci numbers";
    let cc_session_id = format!("cc-{}", uuid::Uuid::new_v4());
    let agent = storage
        .create_agent(&session_id, cc_session_id.clone(), subtask.to_string(), true, None)
        .await
        .expect("Failed to create agent");

    assert!(!agent.agent_id.is_empty());
    assert_eq!(agent.session_id, session_id);
    assert_eq!(agent.claudecode_session_id, cc_session_id);
    assert_eq!(agent.subtask, subtask);
    assert!(agent.is_primary);
    assert!(agent.parent_agent_id.is_none());
    assert!(matches!(agent.state, AgentState::Idle));

    // Verify agent can be retrieved
    let retrieved = storage.get_agent(&agent.agent_id).await.expect("Failed to get agent");
    assert_eq!(retrieved.agent_id, agent.agent_id);
}

#[tokio::test]
async fn test_create_multiple_agents() {
    let storage = create_test_storage().await;

    let session_id = format!("test-agents-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Multi, None)
        .await
        .expect("Failed to create session");

    // Create primary agent
    let agent1 = storage
        .create_agent(&session_id, format!("cc-1-{}", uuid::Uuid::new_v4()), "Task 1".to_string(), true, None)
        .await
        .expect("Failed to create agent 1");

    // Create child agent
    let agent2 = storage
        .create_agent(&session_id, format!("cc-2-{}", uuid::Uuid::new_v4()), "Task 2".to_string(), false, Some(agent1.agent_id.clone()))
        .await
        .expect("Failed to create agent 2");

    // Verify both agents exist
    assert!(agent1.is_primary);
    assert!(!agent2.is_primary);
    assert_eq!(agent2.parent_agent_id, Some(agent1.agent_id.clone()));
}

#[tokio::test]
async fn test_list_agents() {
    let storage = create_test_storage().await;

    let session_id = format!("test-list-agents-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Multi, None)
        .await
        .expect("Failed to create session");

    // Create multiple agents
    for i in 0..3 {
        let is_primary = i == 0;
        storage
            .create_agent(
                &session_id,
                format!("cc-{}-{}", i, uuid::Uuid::new_v4()),
                format!("Task {i}"),
                is_primary,
                None,
            )
            .await
            .unwrap_or_else(|_| panic!("Failed to create agent {i}"));
    }

    // List agents
    let agents = storage
        .list_agents(&session_id)
        .await
        .expect("Failed to list agents");

    assert_eq!(agents.len(), 3);

    // Verify primary agent is only the first one
    let primary_count = agents.iter().filter(|a| a.is_primary).count();
    assert_eq!(primary_count, 1);
}

#[tokio::test]
async fn test_update_agent_state() {
    let storage = create_test_storage().await;

    let session_id = format!("test-agent-state-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Multi, None)
        .await
        .expect("Failed to create session");

    let agent = storage
        .create_agent(&session_id, format!("cc-{}", uuid::Uuid::new_v4()), "Test task".to_string(), true, None)
        .await
        .expect("Failed to create agent");

    // Update agent state to Running
    let running_state = AgentState::Running { sequence: 0 };
    storage
        .update_agent_state(&agent.agent_id, running_state.clone())
        .await
        .expect("Failed to update agent state");

    let updated_agent = storage.get_agent(&agent.agent_id).await.expect("Failed to get agent");
    assert_eq!(updated_agent.state, running_state);

    // Update to Complete
    storage
        .update_agent_state(&agent.agent_id, AgentState::Complete)
        .await
        .expect("Failed to update to complete");

    let completed_agent = storage.get_agent(&agent.agent_id).await.expect("Failed to get agent");
    assert!(matches!(completed_agent.state, AgentState::Complete));
    assert!(completed_agent.completed_at.is_some());
}

#[tokio::test]
async fn test_update_agent_state_failure() {
    let storage = create_test_storage().await;

    let session_id = format!("test-agent-fail-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Multi, None)
        .await
        .expect("Failed to create session");

    let agent = storage
        .create_agent(&session_id, format!("cc-{}", uuid::Uuid::new_v4()), "Test task".to_string(), true, None)
        .await
        .expect("Failed to create agent");

    // Update to Failed state
    let error_msg = "Agent encountered an error";
    let failed_state = AgentState::Failed {
        error: error_msg.to_string(),
    };
    storage
        .update_agent_state(&agent.agent_id, failed_state.clone())
        .await
        .expect("Failed to update agent state");

    let failed_agent = storage.get_agent(&agent.agent_id).await.expect("Failed to get agent");
    assert_eq!(failed_agent.state, failed_state);
    assert_eq!(failed_agent.error_message, Some(error_msg.to_string()));
}

#[tokio::test]
async fn test_agent_isolation_between_sessions() {
    let storage = create_test_storage().await;

    // Create two separate sessions
    let session1_id = format!("test-iso-1-{}", uuid::Uuid::new_v4());
    let session2_id = format!("test-iso-2-{}", uuid::Uuid::new_v4());

    storage
        .create_session(session1_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Multi, None)
        .await
        .expect("Failed to create session 1");
    storage
        .create_session(session2_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Multi, None)
        .await
        .expect("Failed to create session 2");

    // Create agents in each session
    storage
        .create_agent(&session1_id, format!("cc-1-{}", uuid::Uuid::new_v4()), "Session 1 Task".to_string(), true, None)
        .await
        .expect("Failed to create agent in session 1");
    storage
        .create_agent(&session2_id, format!("cc-2-{}", uuid::Uuid::new_v4()), "Session 2 Task".to_string(), true, None)
        .await
        .expect("Failed to create agent in session 2");

    // Verify each session only sees its own agents
    let agents1 = storage
        .list_agents(&session1_id)
        .await
        .expect("Failed to list agents for session 1");
    let agents2 = storage
        .list_agents(&session2_id)
        .await
        .expect("Failed to list agents for session 2");

    assert_eq!(agents1.len(), 1);
    assert_eq!(agents2.len(), 1);
    assert_ne!(agents1[0].agent_id, agents2[0].agent_id);
}

#[tokio::test]
async fn test_delete_session_cascades_to_agents() {
    let storage = create_test_storage().await;

    let session_id = format!("test-cascade-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Multi, None)
        .await
        .expect("Failed to create session");

    // Create agent
    let agent = storage
        .create_agent(&session_id, format!("cc-{}", uuid::Uuid::new_v4()), "Task".to_string(), true, None)
        .await
        .expect("Failed to create agent");

    // Verify agent exists
    assert!(storage.get_agent(&agent.agent_id).await.is_ok());

    // Delete session
    storage
        .delete_session(&session_id)
        .await
        .expect("Failed to delete session");

    // Verify agent was also deleted
    assert!(storage.get_agent(&agent.agent_id).await.is_err());
}

#[tokio::test]
async fn test_get_agent_counts() {
    let storage = create_test_storage().await;

    let session_id = format!("test-counts-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Multi, None)
        .await
        .expect("Failed to create session");

    // Create agents with different states
    let agent1 = storage
        .create_agent(&session_id, format!("cc-1-{}", uuid::Uuid::new_v4()), "Task 1".to_string(), true, None)
        .await
        .expect("Failed to create agent 1");

    let agent2 = storage
        .create_agent(&session_id, format!("cc-2-{}", uuid::Uuid::new_v4()), "Task 2".to_string(), false, None)
        .await
        .expect("Failed to create agent 2");

    let agent3 = storage
        .create_agent(&session_id, format!("cc-3-{}", uuid::Uuid::new_v4()), "Task 3".to_string(), false, None)
        .await
        .expect("Failed to create agent 3");

    // Update states
    storage.update_agent_state(&agent1.agent_id, AgentState::Running { sequence: 0 }).await.ok();
    storage.update_agent_state(&agent2.agent_id, AgentState::Complete).await.ok();
    storage.update_agent_state(&agent3.agent_id, AgentState::Failed { error: "test".to_string() }).await.ok();

    // Get counts
    let (active, completed, failed) = storage
        .get_agent_counts(&session_id)
        .await
        .expect("Failed to get agent counts");

    assert_eq!(active, 1); // Running counts as active
    assert_eq!(completed, 1);
    assert_eq!(failed, 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// Type Tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_session_state_serialization() {
    let states = vec![
        SessionState::Idle,
        SessionState::Running {
            stream_id: "test".to_string(),
            sequence: 42,
            active_agents: 1,
            completed_agents: 0,
            failed_agents: 0,
        },
        SessionState::WaitingApproval {
            approval_id: "approval-123".to_string(),
        },
        SessionState::Complete,
        SessionState::Failed {
            error: "Test error".to_string(),
        },
    ];

    for state in states {
        let json = serde_json::to_string(&state).expect("Failed to serialize");
        let deserialized: SessionState =
            serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(state, deserialized);
    }
}

#[test]
fn test_agent_state_serialization() {
    let states = vec![
        AgentState::Idle,
        AgentState::Running { sequence: 42 },
        AgentState::WaitingApproval {
            approval_id: "approval-123".to_string(),
        },
        AgentState::Complete,
        AgentState::Failed {
            error: "Test error".to_string(),
        },
    ];

    for state in states {
        let json = serde_json::to_string(&state).expect("Failed to serialize");
        let deserialized: AgentState =
            serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(state, deserialized);
    }
}

#[test]
fn test_agent_mode_default() {
    let mode = AgentMode::default();
    assert_eq!(mode, AgentMode::Single);
}

#[test]
fn test_create_session_request_defaults() {
    let json = r#"{"model": "sonnet"}"#;
    let req: CreateSessionRequest = serde_json::from_str(json).expect("Failed to deserialize");

    assert_eq!(req.model, "sonnet");
    assert_eq!(req.working_directory, "/workspace");
    assert_eq!(req.max_retries, 3);
    assert!(!req.multi_agent);
}

// ═══════════════════════════════════════════════════════════════════════════
// Concurrency Tests
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_concurrent_session_creation() {
    let storage = create_test_storage().await;

    // Create multiple sessions concurrently
    let mut handles = vec![];
    for i in 0..10 {
        let storage_clone = storage.clone();
        let handle = tokio::spawn(async move {
            let session_id = format!("test-concurrent-{}-{}", i, uuid::Uuid::new_v4());
            storage_clone
                .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Single, None)
                .await
                .map(|s| s.session_id)
        });
        handles.push(handle);
    }

    // Wait for all to complete
    let mut session_ids = vec![];
    for handle in handles {
        let result = handle.await.expect("Task panicked");
        let session_id = result.expect("Failed to create session");
        session_ids.push(session_id);
    }

    // Verify all sessions were created with unique IDs
    session_ids.sort();
    let len_before = session_ids.len();
    session_ids.dedup();
    assert_eq!(session_ids.len(), len_before); // No duplicates

    assert_eq!(session_ids.len(), 10);
}

#[tokio::test]
async fn test_concurrent_agent_creation() {
    let storage = create_test_storage().await;

    let session_id = format!("test-concurrent-agents-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Multi, None)
        .await
        .expect("Failed to create session");

    // Create multiple agents concurrently
    let mut handles = vec![];
    for i in 0..5 {
        let storage_clone = storage.clone();
        let session_id_clone = session_id.clone();
        let handle = tokio::spawn(async move {
            let subtask = format!("Task {i}");
            let cc_session_id = format!("cc-{}-{}", i, uuid::Uuid::new_v4());
            storage_clone
                .create_agent(&session_id_clone, cc_session_id, subtask, i == 0, None)
                .await
                .map(|a| a.agent_id)
        });
        handles.push(handle);
    }

    // Wait for all to complete
    let mut agent_ids = vec![];
    for handle in handles {
        let result = handle.await.expect("Task panicked");
        let agent_id = result.expect("Failed to create agent");
        agent_ids.push(agent_id);
    }

    // Verify all agents were created with unique IDs
    agent_ids.sort();
    let len_before = agent_ids.len();
    agent_ids.dedup();
    assert_eq!(agent_ids.len(), len_before); // No duplicates

    assert_eq!(agent_ids.len(), 5);

    // Verify we can list all agents
    let agents = storage
        .list_agents(&session_id)
        .await
        .expect("Failed to list agents");
    assert_eq!(agents.len(), 5);
}

#[tokio::test]
async fn test_concurrent_state_updates() {
    let storage = create_test_storage().await;

    let session_id = format!("test-concurrent-states-{}", uuid::Uuid::new_v4());
    storage
        .create_session(session_id.clone(), "sonnet".to_string(), "/workspace".to_string(), None, 3, AgentMode::Single, None)
        .await
        .expect("Failed to create session");

    // Update state multiple times concurrently
    let mut handles = vec![];
    for i in 0..10 {
        let storage_clone = storage.clone();
        let session_id_clone = session_id.clone();
        let handle = tokio::spawn(async move {
            let state = SessionState::Running {
                stream_id: format!("stream-{i}"),
                sequence: i as u64,
                active_agents: 0,
                completed_agents: 0,
                failed_agents: 0,
            };
            storage_clone
                .update_state(&session_id_clone, state)
                .await
        });
        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        handle
            .await
            .expect("Task panicked")
            .expect("Failed to update state");
    }

    // Verify session is in a valid Running state
    let session = storage
        .get_session(&session_id)
        .await
        .expect("Failed to get session");
    assert!(matches!(session.state, SessionState::Running { .. }));
}
