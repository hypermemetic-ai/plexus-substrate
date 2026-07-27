//! Tests for the Arbor activation.
//!
//! PLX-117 / PLX-111: Arbor was the ONE real reader of the injected hub handle
//! in all of substrate, and `tree_render` was the only site that read it. The
//! handle has been replaced by a [`HandleResolvers`] data field. This module
//! holds the test that proves the substitution renders — the regression gate
//! (`cone::tests::test_render_resolved_with_mock_resolver`, which drove
//! `Tree::render_resolved` with a storage-built resolver and no hub at all)
//! proved the *mechanism* already; this drives it through the real
//! `arbor.tree_render` method and the real `Cone::resolve_handle`.

use super::{Arbor, ArborConfig, ArborStorage, Handle, HandleResolvers};
use crate::activations::cone::{Cone, ConeStorageConfig, MessageRole};
use crate::plexus::{Activation, PlexusStreamItem};
use futures::StreamExt;
use std::sync::Arc;

/// `tree_render` resolves through the INJECTED map, and falls back to
/// `[unresolved: …]` when a node's `plugin_id` is not in it.
///
/// Both halves in one tree, because the point is that the two branches coexist
/// exactly as they did when the resolution went through `DynamicHub`'s registry:
/// a hit renders content, a miss renders the same `[unresolved: {method} - {e}]`
/// string the hub's `ActivationNotFound` produced.
#[tokio::test]
async fn tree_render_resolves_through_injected_map_and_falls_back_on_plugin_id_miss() {
    let dir = tempfile::tempdir().unwrap();

    let arbor_storage = Arc::new(
        ArborStorage::new(ArborConfig {
            db_path: dir.path().join("arbor.db"),
            auto_cleanup: false,
            ..Default::default()
        })
        .await
        .unwrap(),
    );

    let cone = Cone::new(
        ConeStorageConfig {
            db_path: dir.path().join("cones.db"),
        },
        arbor_storage.clone(),
    )
    .await
    .unwrap();

    // A real cone message, reachable only through Cone's own storage — the
    // handles are runtime SQLite rows, which is why the resolver had to be data
    // and could never have come from the Connectome (PLX-111).
    let cone_config = cone
        .storage()
        .cone_create("render-test".to_string(), "gpt-4".to_string(), None, None)
        .await
        .unwrap();
    let msg = cone
        .storage()
        .message_create(
            &cone_config.id,
            MessageRole::User,
            "What is 2+2?".to_string(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let tree_id = cone_config.head.tree_id;
    let resolvable = crate::activations::cone::ConeStorage::message_to_handle(&msg, "user");
    let hit_node = arbor_storage
        .node_create_external(&tree_id, Some(cone_config.head.node_id), resolvable, None)
        .await
        .unwrap();

    // A handle whose plugin_id names nothing in the map. Note this is exactly
    // the shape an untrusted caller can write: `arbor.node_create_external` is a
    // public RPC method taking a caller-supplied `plugin_id`.
    let orphan = Handle::new(uuid::Uuid::new_v4(), "1.0.0", "chat")
        .with_meta(vec!["msg-nonexistent".to_string(), "user".to_string()]);
    arbor_storage
        .node_create_external(&tree_id, Some(hit_node), orphan, None)
        .await
        .unwrap();

    // The substitution under test: no hub, no parent, just the map.
    let arbor = Arbor::with_storage(arbor_storage)
        .with_resolvers(HandleResolvers::new().with(Arc::new(cone)));

    // `call_arc`, never `call` — a turn-native method reached through `call`
    // returns a deliberate ExecutionError naming `call_arc`.
    let mut stream = Arc::new(arbor)
        .call_arc(
            "tree_render",
            serde_json::json!({ "tree_id": tree_id.to_string() }),
            None,
            None,
        )
        .await
        .expect("tree_render must dispatch");

    // `tree_render` is unary now (PLX-110), so this is zero updates and ONE
    // terminal carrying the serialized `ArborEvent::TreeRender`.
    let mut render = None;
    let mut data_items = 0;
    while let Some(item) = stream.next().await {
        if let PlexusStreamItem::Data { content, .. } = item {
            data_items += 1;
            render = content
                .get("value")
                .and_then(|v| v.get("render"))
                .and_then(|v| v.as_str())
                .map(std::string::ToString::to_string);
        }
    }
    assert_eq!(data_items, 1, "a unary method emits exactly one terminal");
    let render = render.expect("tree_render must yield a TreeRender event");

    assert!(
        render.contains("[user:user] What is 2+2?"),
        "the injected resolver must render the message content; got:\n{render}"
    );
    assert!(
        render.contains("[unresolved: chat - "),
        "a plugin_id miss must fall back to the pre-migration `[unresolved: …]` \
         string; got:\n{render}"
    );
}
