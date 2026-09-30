use std::sync::Arc;

use crate::app::http::app_state::AppState;
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::schema::CreateCollectionRequest;

fn collection_request() -> CreateCollectionRequest {
    serde_json::from_value(serde_json::json!({
        "fields": { "value": { "type": "keyword" } }
    }))
    .expect("valid collection request")
}

#[tokio::test]
async fn default_sink_atomically_replaces_live_state() {
    let live = Arc::new(Engine::new());
    live.create_collection("old", collection_request())
        .expect("old collection");
    let state = AppState::open(live.clone());

    let source = Engine::new();
    source
        .create_collection("new", collection_request())
        .expect("new collection");
    let snapshot = source.snapshot().expect("snapshot");

    state
        .restore_sink
        .restore(snapshot)
        .await
        .expect("restore succeeds");
    let restored = live.snapshot().expect("restored snapshot");
    assert!(!restored.collections.contains_key("old"));
    assert!(restored.collections.contains_key("new"));
}
