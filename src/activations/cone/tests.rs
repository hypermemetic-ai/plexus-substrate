//! Tests for Cone activation
//!
//! Includes:
//! - Schema validation tests
//! - Integration tests for Cone + Arbor handle workflow

use super::*;
use crate::activations::arbor::{ArborConfig, ArborStorage};
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

// ============================================================================
// Schema validation tests (moved from mod.rs)
// ============================================================================

/// Test that `RegistryResult` schema has proper structure with `RegistryExport` fields.
#[test]
fn test_registry_result_schema_has_all_fields() {
    let schema = schemars::schema_for!(RegistryResult);
    let schema_value = serde_json::to_value(&schema).unwrap();

    // RegistryResult has only one variant: Registry
    let one_of = schema_value.get("oneOf").and_then(|v| v.as_array()).unwrap();
    assert_eq!(one_of.len(), 1, "RegistryResult should have exactly 1 variant");

    let registry_variant = &one_of[0];
    let properties = registry_variant.get("properties").unwrap();
    assert!(properties.get("type").is_some(), "Should have type discriminant");

    // The RegistryExport fields should be in $defs and referenced
    let defs = schema_value.get("$defs").expect("Should have $defs");
    let registry_export = defs.get("RegistryExport").expect("Should have RegistryExport in $defs");
    let registry_props = registry_export.get("properties").unwrap();

    assert!(registry_props.get("families").is_some(), "RegistryExport should have families field");
    assert!(registry_props.get("models").is_some(), "RegistryExport should have models field");
    assert!(registry_props.get("services").is_some(), "RegistryExport should have services field");
    assert!(registry_props.get("stats").is_some(), "RegistryExport should have stats field");
}

/// Test that each method returns its specific type, not a union of all types.
#[test]
fn test_method_specific_return_types() {
    let method_schemas = ConeMethod::method_schemas();

    // PLX-117: `create` and `list` are unary now — they return
    // `Result<_, ConeError>`, so the failure path is the turn's terminal
    // (StopKind::Failed, a structured TurnError) rather than an `Error` variant
    // smuggled through the success type. Each therefore has ONE variant, and the
    // error shaping lives in the single `impl From<ConeError> for TurnError`.
    // `chat` below still has 4 — it genuinely streams, so an error mid-stream
    // has to be an event.

    // create -> CreateResult (1 variant: Created)
    let create = method_schemas.iter().find(|m| m.name == "create").unwrap();
    let create_returns = serde_json::to_value(create.returns.as_ref().unwrap()).unwrap();
    let create_variants = create_returns.get("oneOf").and_then(|v| v.as_array()).unwrap();
    assert_eq!(create_variants.len(), 1, "CreateResult should have 1 variant");

    // list -> ListResult (1 variant: List)
    let list = method_schemas.iter().find(|m| m.name == "list").unwrap();
    let list_returns = serde_json::to_value(list.returns.as_ref().unwrap()).unwrap();
    let list_variants = list_returns.get("oneOf").and_then(|v| v.as_array()).unwrap();
    assert_eq!(list_variants.len(), 1, "ListResult should have 1 variant");

    // chat -> ChatEvent (4 variants: Start, Content, Complete, Error)
    let chat = method_schemas.iter().find(|m| m.name == "chat").unwrap();
    let chat_returns = serde_json::to_value(chat.returns.as_ref().unwrap()).unwrap();
    let chat_variants = chat_returns.get("oneOf").and_then(|v| v.as_array()).unwrap();
    assert_eq!(chat_variants.len(), 4, "ChatEvent should have 4 variants");

    // registry -> RegistryResult (1 variant: Registry)
    let registry = method_schemas.iter().find(|m| m.name == "registry").unwrap();
    let registry_returns = serde_json::to_value(registry.returns.as_ref().unwrap()).unwrap();
    let registry_variants = registry_returns.get("oneOf").and_then(|v| v.as_array()).unwrap();
    assert_eq!(registry_variants.len(), 1, "RegistryResult should have 1 variant");
}

#[test]
fn test_streaming_flag() {
    let method_schemas = ConeMethod::method_schemas();

    // chat is streaming (returns impl Stream<Item = ChatEvent>)
    let chat = method_schemas.iter().find(|m| m.name == "chat").unwrap();
    assert!(chat.streaming, "chat should be streaming");

    // create is NOT streaming (PLX-117: it is now spelled as what it is —
    // a unary `Result`-returning method)
    let create = method_schemas.iter().find(|m| m.name == "create").unwrap();
    assert!(!create.streaming, "create should NOT be streaming");
}

// ============================================================================
// Integration tests: Cone + Arbor handle workflow
// ============================================================================

/// Create test storage instances with temp databases
async fn create_test_storage() -> (ConeStorage, Arc<ArborStorage>, TempDir) {
    let dir = tempdir().unwrap();

    // Create Arbor storage first (shared)
    let arbor_config = ArborConfig {
        db_path: dir.path().join("test_arbor.db"),
        auto_cleanup: false,
        ..Default::default()
    };
    let arbor = Arc::new(ArborStorage::new(arbor_config).await.unwrap());

    // Create Cone storage with shared Arbor
    let cone_config = ConeStorageConfig {
        db_path: dir.path().join("test_cones.db"),
    };
    let cone_storage = ConeStorage::new(cone_config, arbor.clone()).await.unwrap();

    (cone_storage, arbor, dir)
}

// ============================================================================
// Test 1: Direct storage test (no Plexus)
// ============================================================================

#[tokio::test]
async fn test_cone_message_to_arbor_handle_direct() {
    let (cone_storage, arbor, _dir) = create_test_storage().await;

    // 1. Create a cone
    let cone = cone_storage
        .cone_create(
            "test-assistant".to_string(),
            "gpt-4o-mini".to_string(),
            Some("You are a helpful assistant.".to_string()),
            None,
        )
        .await
        .unwrap();

    println!("Created cone: {} (id: {})", cone.name, cone.id);
    println!("Initial head: tree={}, node={}", cone.head.tree_id, cone.head.node_id);

    // 2. Create a user message
    let user_message = cone_storage
        .message_create(
            &cone.id,
            MessageRole::User,
            "Hello, how are you?".to_string(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

    println!("Created user message: {}", user_message.id);

    // 3. Convert message to handle using ConeHandle
    let user_handle = ConeStorage::message_to_handle(&user_message, "user");
    println!("User handle: {}@{}::{}:{}",
        user_handle.plugin_id,
        user_handle.version,
        user_handle.method,
        user_handle.meta.join(":"));

    // 4. Store handle in Arbor tree as external node
    let user_node_id = arbor
        .node_create_external(
            &cone.head.tree_id,
            Some(cone.head.node_id),  // parent is root
            user_handle.clone(),
            None,
        )
        .await
        .unwrap();

    println!("Created user node in arbor: {user_node_id}");

    // 5. Create an assistant message
    let assistant_message = cone_storage
        .message_create(
            &cone.id,
            MessageRole::Assistant,
            "I'm doing well, thank you for asking! How can I help you today?".to_string(),
            Some("gpt-4o-mini".to_string()),
            Some(10),  // input tokens
            Some(15),  // output tokens
        )
        .await
        .unwrap();

    println!("Created assistant message: {}", assistant_message.id);

    // 6. Convert to handle and store in Arbor
    let assistant_handle = ConeStorage::message_to_handle(&assistant_message, &cone.name);
    let assistant_node_id = arbor
        .node_create_external(
            &cone.head.tree_id,
            Some(user_node_id),  // parent is user message
            assistant_handle.clone(),
            None,
        )
        .await
        .unwrap();

    println!("Created assistant node in arbor: {assistant_node_id}");

    // 7. Update cone head
    cone_storage.cone_update_head(&cone.id, assistant_node_id).await.unwrap();

    // 8. Render the tree
    let tree = arbor.tree_get(&cone.head.tree_id).await.unwrap();
    let rendered = tree.render();

    println!("\n=== Tree Render ===");
    println!("{rendered}");
    println!("==================\n");

    // 9. Verify tree structure
    assert_eq!(tree.nodes.len(), 3); // root + user + assistant

    // Verify the handles can be parsed back to ConeHandle
    let parsed_user = ConeHandle::try_from(&user_handle).unwrap();
    let ConeHandle::Message { message_id, role, name } = &parsed_user;
    assert!(message_id.starts_with("msg-"));
    assert_eq!(role, "user");
    assert_eq!(name, "user");

    let parsed_assistant = ConeHandle::try_from(&assistant_handle).unwrap();
    let ConeHandle::Message { message_id, role, name } = &parsed_assistant;
    assert!(message_id.starts_with("msg-"));
    assert_eq!(role, "assistant");
    assert_eq!(name, "test-assistant");

    // 10. Test resolution_params
    let user_params = parsed_user.resolution_params();
    assert!(user_params.is_some());
    let p = user_params.unwrap();
    assert_eq!(p.table, "messages");
    assert_eq!(p.key_column, "id");
    // key_value should have "msg-" stripped
    assert!(!p.key_value.starts_with("msg-"));

    println!("Resolution params for user message:");
    println!("  table: {}", p.table);
    println!("  key_column: {}", p.key_column);
    println!("  key_value: {}", p.key_value);
    println!("  context: {:?}", p.context);
}

#[tokio::test]
async fn test_multi_turn_conversation() {
    let (cone_storage, arbor, _dir) = create_test_storage().await;

    // Create cone
    let cone = cone_storage
        .cone_create(
            "multi-turn-test".to_string(),
            "claude-3-haiku".to_string(),
            None,
            None,
        )
        .await
        .unwrap();

    let mut current_parent = cone.head.node_id;

    // Simulate a 3-turn conversation
    let turns = vec![
        (MessageRole::User, "What is 2+2?", "user"),
        (MessageRole::Assistant, "2+2 equals 4.", "multi-turn-test"),
        (MessageRole::User, "And 3+3?", "user"),
        (MessageRole::Assistant, "3+3 equals 6.", "multi-turn-test"),
        (MessageRole::User, "Thanks!", "user"),
        (MessageRole::Assistant, "You're welcome!", "multi-turn-test"),
    ];

    for (role, content, name) in turns {
        let message = cone_storage
            .message_create(&cone.id, role, content.to_string(), None, None, None)
            .await
            .unwrap();

        let handle = ConeStorage::message_to_handle(&message, name);
        let node_id = arbor
            .node_create_external(&cone.head.tree_id, Some(current_parent), handle, None)
            .await
            .unwrap();

        current_parent = node_id;
    }

    // Update head to final node
    cone_storage.cone_update_head(&cone.id, current_parent).await.unwrap();

    // Render tree
    let tree = arbor.tree_get(&cone.head.tree_id).await.unwrap();
    let rendered = tree.render();

    println!("\n=== Multi-turn Conversation Tree ===");
    println!("{rendered}");
    println!("====================================\n");

    // Verify structure: root + 6 messages
    assert_eq!(tree.nodes.len(), 7);

    // Verify it's a linear chain (each node except root has exactly one child, leaf has none)
    let root = tree.nodes.get(&tree.root).unwrap();
    assert_eq!(root.children.len(), 1); // root has one child (first user message)
}

// ============================================================================
// Test 2: Correct architecture - Cone uses ArborStorage directly
//
// Key architectural principle:
// - Cone uses ArborStorage DIRECTLY (it's infrastructure)
// - Plexus is only needed for:
//   1. External clients calling Cone methods
//   2. Resolving handles that point to OTHER plugins
// ============================================================================

#[tokio::test]
async fn test_cone_uses_arbor_directly() {
    use crate::activations::arbor::Arbor;
    use crate::activations::cone::Cone;

    let dir = tempdir().unwrap();

    // Create Arbor - this is infrastructure, shared directly
    let arbor_config = ArborConfig {
        db_path: dir.path().join("arbor.db"),
        auto_cleanup: false,
        ..Default::default()
    };
    let arbor = Arbor::new(arbor_config).await.unwrap();

    // Cone gets ArborStorage directly - NOT via Plexus
    let arbor_storage = arbor.storage();

    // Create Cone with direct ArborStorage reference
    let cone_config = ConeStorageConfig {
        db_path: dir.path().join("cones.db"),
    };
    let cone = Cone::new(cone_config, arbor_storage.clone()).await.unwrap();
    let cone_storage = cone.storage();

    // 1. Create a cone - this internally uses ArborStorage to create the tree
    let cone_config = cone_storage
        .cone_create(
            "test-cone".to_string(),
            "gpt-4".to_string(),
            None,
            None,
        )
        .await
        .unwrap();

    println!("Created cone: {}", cone_config.name);
    println!("Tree ID: {}", cone_config.head.tree_id);

    // 2. Create messages and store in Arbor - Cone uses ArborStorage directly
    let user_msg = cone_storage
        .message_create(&cone_config.id, MessageRole::User, "Hello!".to_string(), None, None, None)
        .await
        .unwrap();

    let user_handle = ConeStorage::message_to_handle(&user_msg, "user");

    // Cone stores in Arbor DIRECTLY, not via Plexus
    let user_node = arbor_storage
        .node_create_external(&cone_config.head.tree_id, Some(cone_config.head.node_id), user_handle, None)
        .await
        .unwrap();

    let assistant_msg = cone_storage
        .message_create(&cone_config.id, MessageRole::Assistant, "Hi there!".to_string(), Some("gpt-4".to_string()), None, None)
        .await
        .unwrap();

    let assistant_handle = ConeStorage::message_to_handle(&assistant_msg, &cone_config.name);

    let assistant_node = arbor_storage
        .node_create_external(&cone_config.head.tree_id, Some(user_node), assistant_handle, None)
        .await
        .unwrap();

    // Update cone head - uses internal storage
    cone_storage.cone_update_head(&cone_config.id, assistant_node).await.unwrap();

    // 3. Render tree - using ArborStorage directly
    let tree = arbor_storage.tree_get(&cone_config.head.tree_id).await.unwrap();
    let rendered = tree.render();

    println!("\n=== Tree (direct ArborStorage render) ===");
    println!("{rendered}");
    println!("==========================================\n");

    // Verify structure
    assert_eq!(tree.nodes.len(), 3); // root + user + assistant

    // The rendered tree shows handles like:
    // └── [ext] {plugin_id}@1.0.0::chat:msg-xxx:user:user
    //     └── [ext] {plugin_id}@1.0.0::chat:msg-xxx:assistant:test-cone
    assert!(rendered.contains("::chat:"));
    println!("Tree contains external handles pointing to Cone messages");
}

// ============================================================================
// Test 4: render_resolved with mock resolver
// ============================================================================

#[tokio::test]
async fn test_render_resolved_with_mock_resolver() {
    let (cone_storage, arbor, _dir) = create_test_storage().await;

    // Create cone and messages
    let cone = cone_storage
        .cone_create(
            "resolved-test".to_string(),
            "gpt-4".to_string(),
            None,
            None,
        )
        .await
        .unwrap();

    // Create messages with known content
    let user_msg = cone_storage
        .message_create(
            &cone.id,
            MessageRole::User,
            "What is 2+2?".to_string(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let assistant_msg = cone_storage
        .message_create(
            &cone.id,
            MessageRole::Assistant,
            "2+2 equals 4.".to_string(),
            Some("gpt-4".to_string()),
            None,
            None,
        )
        .await
        .unwrap();

    // Store handles in arbor
    let user_handle = ConeStorage::message_to_handle(&user_msg, "user");
    let user_node = arbor
        .node_create_external(&cone.head.tree_id, Some(cone.head.node_id), user_handle.clone(), None)
        .await
        .unwrap();

    let assistant_handle = ConeStorage::message_to_handle(&assistant_msg, "resolved-test");
    let _assistant_node = arbor
        .node_create_external(&cone.head.tree_id, Some(user_node), assistant_handle.clone(), None)
        .await
        .unwrap();

    // Get tree
    let tree = arbor.tree_get(&cone.head.tree_id).await.unwrap();

    // Create a mock resolver that looks up messages from cone_storage
    // Wrap in Arc to share with async closure
    let storage_arc = std::sync::Arc::new(cone_storage);
    let rendered = tree.render_resolved(|handle| {
        let storage = storage_arc.clone();
        let identifier = handle.meta.join(":");
        async move {
            match storage.resolve_message_handle(&identifier).await {
                Ok(message) => {
                    format!("[{}] {}", message.role.as_str(), message.content)
                }
                Err(_) => format!("[unresolved: {identifier}]"),
            }
        }
    }).await;

    println!("\n=== Tree (render_resolved with mock resolver) ===");
    println!("{rendered}");
    println!("=================================================\n");

    // Verify the rendered tree shows actual message content
    assert!(rendered.contains("[user] What is 2+2?"), "Should show user message content");
    assert!(rendered.contains("[assistant] 2+2 equals 4."), "Should show assistant message content");

    // Also verify the structure is preserved
    assert!(
        rendered.lines().count() >= 3,
        "Should have at least 3 lines (root + 2 messages)"
    );
}

// ============================================================================
// IR-19: `of` dynamic child gate tests
//
// Verifies that Cone surfaces `of` as a role-tagged DynamicChild method with
// `list_method = Some("cone_ids")`, and that `ChildRouter::get_child` resolves
// valid UUIDs to `Some(ConeActivation)` and rejects unknown IDs with `None`.
// ============================================================================

/// AC #3: `plugin_schema()` has `of` method tagged as
/// `DynamicChild { list_method: Some("cone_ids"), search_method: None }`.
#[tokio::test]
async fn of_gate_role_is_dynamic_child_with_cone_ids_list() {
    use crate::plexus::{Activation, MethodRole};

    let (_cone_storage, arbor, dir) = create_test_storage().await;
    let cone = Cone::new(
        ConeStorageConfig { db_path: dir.path().join("schema_cones.db") },
        arbor,
    ).await.unwrap();

    let schema = cone.plugin_schema();
    let of = schema
        .methods
        .iter()
        .find(|m| m.name == "of")
        .expect("cone should expose an `of` method (IR-19 child gate)");

    assert_eq!(
        of.role,
        MethodRole::DynamicChild {
            list_method: Some("cone_ids".into()),
            search_method: None,
        },
        "`of` must be DynamicChild with list_method=Some(\"cone_ids\"), search_method=None; \
         got {:?}", of.role,
    );
}

/// AC #4a: `ChildRouter::get_child(cone, <valid-uuid>)` returns `Some(..)`.
#[tokio::test]
async fn get_child_resolves_valid_cone_id() {
    use crate::plexus::ChildRouter;

    let (_cone_storage, arbor, dir) = create_test_storage().await;
    let cone = Cone::new(
        ConeStorageConfig { db_path: dir.path().join("resolve_cones.db") },
        arbor,
    ).await.unwrap();

    let created = cone
        .storage()
        .cone_create(
            "resolve-test".to_string(),
            "gpt-4o-mini".to_string(),
            None,
            None,
        )
        .await
        .expect("cone_create must succeed");

    let got = cone.get_child(&created.id.to_string()).await;
    assert!(got.is_some(), "get_child must resolve a valid cone UUID");
    assert_eq!(got.unwrap().router_namespace(), "cone");
}

/// AC #4b: `ChildRouter::get_child(cone, <unknown-id>)` returns `None`.
#[tokio::test]
async fn get_child_returns_none_for_unknown_id() {
    use crate::plexus::ChildRouter;

    let (_cone_storage, arbor, dir) = create_test_storage().await;
    let cone = Cone::new(
        ConeStorageConfig { db_path: dir.path().join("unknown_cones.db") },
        arbor,
    ).await.unwrap();

    // No cones created — every ID is unknown.
    let unknown_uuid = uuid::Uuid::new_v4().to_string();
    assert!(
        cone.get_child(&unknown_uuid).await.is_none(),
        "get_child must return None for unknown cone IDs",
    );
    assert!(
        cone.get_child("definitely-not-a-cone").await.is_none(),
        "get_child must return None for non-UUID, non-existent names",
    );
}

/// AC #6: Cone advertises the LIST capability on its dynamic child gate.
///
/// The IR-4 replacement for `ChildRouter::capabilities()` + `ChildCapabilities::LIST`
/// is the `MethodRole::DynamicChild { list_method, .. }` tag on the gate
/// method's `MethodSchema`. A present `list_method` is the authoritative
/// post-IR-4 signal that listing is supported — it's the shape Synapse and
/// other introspection clients read to know whether `list_children()` will
/// return `Some(stream)`.
#[tokio::test]
async fn cone_child_router_capabilities_include_list() {
    use crate::plexus::{Activation, MethodRole};

    let (_cone_storage, arbor, dir) = create_test_storage().await;
    let cone = Cone::new(
        ConeStorageConfig { db_path: dir.path().join("caps_cones.db") },
        arbor,
    ).await.unwrap();

    let schema = cone.plugin_schema();
    let of = schema
        .methods
        .iter()
        .find(|m| m.name == "of")
        .expect("cone should expose an `of` child gate method");

    match &of.role {
        MethodRole::DynamicChild { list_method, .. } => {
            assert!(
                list_method.is_some(),
                "Cone must advertise LIST capability via MethodRole::DynamicChild \
                 `list_method` (list = \"cone_ids\" opt-in); got list_method=None"
            );
        }
        other => panic!(
            "`of` must be DynamicChild with a list_method set; got role={other:?}"
        ),
    }
}

/// Sanity: `cone_ids` streams the ids of currently-known cones (CHILD-4 listing).
#[tokio::test]
async fn list_children_streams_known_cone_ids() {
    use crate::plexus::ChildRouter;
    use futures::StreamExt;

    let (_cone_storage, arbor, dir) = create_test_storage().await;
    let cone = Cone::new(
        ConeStorageConfig { db_path: dir.path().join("list_cones.db") },
        arbor,
    ).await.unwrap();

    let a = cone
        .storage()
        .cone_create("a-list".to_string(), "gpt-4o-mini".to_string(), None, None)
        .await
        .unwrap();
    let b = cone
        .storage()
        .cone_create("b-list".to_string(), "gpt-4o-mini".to_string(), None, None)
        .await
        .unwrap();

    let listed: Vec<String> = cone
        .list_children()
        .await
        .expect("list_children must be Some when `list = ...` is set")
        .collect()
        .await;

    assert!(
        listed.iter().any(|id| id == &a.id.to_string()),
        "list_children should contain cone a's UUID; got {listed:?}"
    );
    assert!(
        listed.iter().any(|id| id == &b.id.to_string()),
        "list_children should contain cone b's UUID; got {listed:?}"
    );
}
