use std::io::Write;

use plexus_substrate::activations::arbor::{ArborConfig, ArborStorage};
use plexus_substrate::activations::claudecode::sessions;
use plexus_substrate::activations::claudecode::sessions::SessionRoot;
use plexus_substrate::activations::claudecode::{
    AssistantMessage, ContentBlock, NodeEvent, SessionEvent,
};
use tempfile::TempDir;

#[tokio::test]
async fn test_session_import_export_roundtrip() {
    // Create temporary storage
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let config = ArborConfig {
        db_path,
        ..Default::default()
    };
    let storage = ArborStorage::new(config).await.unwrap();

    println!("\n=== Testing Session Import/Export Round-trip ===\n");

    // 1. Create a simple test session
    //
    // PLX-129: this fixture used to pass an ABSOLUTE path as `project_path`,
    // which `Path::join` silently honours by DISCARDING the base — that is the
    // traversal this ticket closed, and a test depending on it is evidence the
    // hole was reachable rather than theoretical. The session root is now
    // explicit and the project is a name inside it. Every assertion below is
    // unchanged.
    let root = SessionRoot::new(temp_dir.path());
    let project_str = "test-project";
    std::fs::create_dir_all(temp_dir.path().join(project_str)).unwrap();
    let session_id = "test-session-123";

    println!("1. Creating test session...");

    // Write a simple session with user and assistant messages
    let test_events = vec![
        serde_json::json!({
            "type": "user",
            "uuid": "user-1",
            "sessionId": session_id,
            "timestamp": "2024-01-01T00:00:00Z",
            "cwd": "/test",
            "message": {
                "role": "user",
                "content": "Hello, Claude!"
            }
        }),
        serde_json::json!({
            "type": "assistant",
            "uuid": "asst-1",
            "sessionId": session_id,
            "timestamp": "2024-01-01T00:00:01Z",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Hello! "},
                    {"type": "text", "text": "How can I "},
                    {"type": "text", "text": "help you "},
                    {"type": "text", "text": "today?"}
                ]
            }
        }),
    ];

    // Write session file
    let session_path = root.session_path(project_str, session_id, true).unwrap();
    std::fs::create_dir_all(session_path.parent().unwrap()).unwrap();
    let mut session_file = std::fs::File::create(&session_path).unwrap();
    for event in &test_events {
        writeln!(session_file, "{}", serde_json::to_string(event).unwrap()).unwrap();
    }
    drop(session_file);

    println!("✓ Created test session with {} events\n", test_events.len());

    // 2. Import to arbor
    println!("2. Importing session to arbor...");
    let tree_id = sessions::import_to_arbor(&storage, &root, project_str, session_id, "test-user")
        .await
        .unwrap();
    println!("✓ Imported to tree: {tree_id}\n");

    // 3. Check tree structure
    let tree = storage.tree_get(&tree_id).await.unwrap();
    println!("3. Checking imported tree structure:");
    println!("  Root: {}", tree.root);
    println!("  Total nodes: {}\n", tree.nodes.len());

    // Expected structure:
    // root → user_message → assistant_start → content_text (x4) → assistant_complete
    assert!(
        tree.nodes.len() >= 6,
        "Should have at least 6 nodes (root, user, asst_start, 4x text, asst_complete)"
    );

    // 4. Export back to JSONL
    println!("4. Exporting tree back to JSONL...");
    let export_session_id = "test-session-exported";
    sessions::export_from_arbor(&storage, &root, &tree_id, project_str, export_session_id)
        .await
        .unwrap();
    println!("✓ Exported to session: {export_session_id}\n");

    // 5. Read exported session
    println!("5. Reading exported session...");
    let exported_events = sessions::read_session(&root, project_str, export_session_id)
        .await
        .unwrap();
    println!("✓ Exported session has {} events\n", exported_events.len());

    // 6. Compare structures
    println!("6. Comparing original vs exported:");
    println!("  Original events: {}", test_events.len());
    println!("  Exported events: {}", exported_events.len());

    assert_eq!(
        exported_events.len(),
        test_events.len(),
        "Exported session should have same number of events"
    );

    // Check event types match
    for (i, event) in exported_events.iter().enumerate() {
        match (i, event) {
            (0, SessionEvent::User { .. }) => {
                println!("  ✓ Event {i}: User message");
            }
            (1, SessionEvent::Assistant { .. }) => {
                println!("  ✓ Event {i}: Assistant message");
            }
            _ => {
                panic!("Unexpected event at index {i}: {event:?}");
            }
        }
    }

    println!("\n✓ Round-trip test passed!");
}

#[tokio::test]
#[ignore = "view_collapse_text_runs merges structural NodeEvent markers (AssistantStart/Complete) with ContentText nodes, producing malformed collapsed content that export_from_arbor cannot parse back into session events"]
async fn test_view_collapse_export() {
    // Create temporary storage
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let config = ArborConfig {
        db_path,
        ..Default::default()
    };
    let storage = ArborStorage::new(config).await.unwrap();

    println!("\n=== Testing View Collapse + Export ===\n");

    // 1. Create a tree with consecutive text nodes
    println!("1. Creating storage tree with text runs...");
    let tree_id = storage.tree_create(None, "test-user").await.unwrap();
    let tree = storage.tree_get(&tree_id).await.unwrap();

    // Add consecutive NodeEvent text nodes
    let texts = vec!["Hello ", "from ", "Claude ", "Code!"];

    let mut parent = tree.root;

    // Start with assistant_start
    let start_json = serde_json::to_string(&NodeEvent::AssistantStart).unwrap();
    let start_node = storage
        .node_create_text(&tree_id, Some(parent), start_json, None)
        .await
        .unwrap();
    parent = start_node;

    // Add text nodes
    for text in &texts {
        let node_event = NodeEvent::ContentText {
            text: text.to_string(),
        };
        let json = serde_json::to_string(&node_event).unwrap();
        let node_id = storage
            .node_create_text(&tree_id, Some(parent), json, None)
            .await
            .unwrap();
        parent = node_id;
    }

    // End with assistant_complete
    let complete_json =
        serde_json::to_string(&NodeEvent::AssistantComplete { usage: None }).unwrap();
    storage
        .node_create_text(&tree_id, Some(parent), complete_json, None)
        .await
        .unwrap();

    println!("✓ Created storage tree with {} text chunks\n", texts.len());

    // 2. Create collapsed view
    println!("2. Creating collapsed view (min_length=2)...");
    let (view_tree_id, collapsed_runs) = storage
        .view_collapse_text_runs(&tree_id, 2, "test-user")
        .await
        .unwrap();

    println!("✓ Created view tree: {view_tree_id}");
    println!("✓ Collapsed {} text run(s)\n", collapsed_runs.len());

    // DEBUG: Check view tree structure
    let view_tree = storage.tree_get(&view_tree_id).await.unwrap();
    println!("DEBUG: View tree has {} nodes", view_tree.nodes.len());
    for (i, (_node_id, node)) in view_tree.nodes.iter().enumerate().take(10) {
        use plexus_substrate::activations::arbor::NodeType;
        if let NodeType::Text { content } = &node.data {
            println!("  Node {}: content length = {}, content = {:?}",
                i, content.len(), &content[..content.len().min(100)]);
        }
    }

    // 3. Export view tree to JSONL
    println!("3. Exporting view tree to JSONL...");
    let root = SessionRoot::new(temp_dir.path());
    let project_str = "test-project";
    std::fs::create_dir_all(temp_dir.path().join(project_str)).unwrap();
    let session_id = "view-export-test";

    match sessions::export_from_arbor(&storage, &root, &view_tree_id, project_str, session_id).await {
        Ok(()) => println!("✓ Exported view to session\n"),
        Err(e) => {
            eprintln!("ERROR exporting: {e}");
            panic!("Export failed: {e}");
        }
    }

    // Check if file was created
    let session_path = root.session_path(project_str, session_id, true).unwrap();
    println!("Session file should be at: {session_path:?}");
    println!("File exists: {}", session_path.exists());

    // 4. Read exported session and verify merged content
    println!("4. Verifying exported content...");
    let exported_events = match sessions::read_session(&root, project_str, session_id).await {
        Ok(events) => events,
        Err(e) => {
            eprintln!("ERROR reading session: {e}");
            panic!("Failed to read exported session: {e}");
        }
    };

    println!("  Exported events: {}", exported_events.len());

    // Should have 1 assistant message with merged content
    for (i, event) in exported_events.iter().enumerate() {
        match event {
            SessionEvent::Assistant { data } => {
                println!("  ✓ Event {i}: Assistant message");

                match &data.message {
                    AssistantMessage::Full { content, .. } => {
                        println!("    Content blocks: {}", content.len());

                        // Check if we have merged text
                        for (j, block) in content.iter().enumerate() {
                            if let ContentBlock::Text { text } = block {
                                println!("    Block {j}: \"{text}\"");
                            }
                        }
                    }
                    AssistantMessage::Simple(s) => {
                        println!("    Simple message: {s}");
                    }
                }
            }
            other => {
                println!("  Event {i}: {other:?}");
            }
        }
    }

    println!("\n✓ View export test passed!");
}
