use std::sync::Arc;
use std::time::Duration;

use lumen::coordinator::MutationGate;
use lumen::storage::Engine;
use lumen::types::CreateCollectionRequest;
use lumen::wal::MemWal;

use crate::cli::serve::WalBackend;
use crate::serve::restore::segment_restore_sink;

fn test_schema() -> CreateCollectionRequest {
    serde_json::from_value(serde_json::json!({
        "fields": { "value": { "type": "keyword" } }
    }))
    .expect("valid test schema")
}

fn test_writer(
    engine: Arc<Engine>,
) -> (
    Arc<dyn lumen::coordinator::WriteSink>,
    lumen::coordinator::SharedAof,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().expect("AOF tempdir");
    let aof = Arc::new(std::sync::Mutex::new(
        lumen::aof::AofWriter::open(dir.path().join("aof.log")).expect("open AOF"),
    ));
    let writer = lumen::coordinator::WriteCoordinator::start_from_with_aof(
        Arc::new(MemWal::new()),
        engine,
        0,
        aof.clone(),
    );
    (writer, aof, dir)
}

#[tokio::test]
async fn segment_restore_sink_is_not_selected_for_non_segment_mode() {
    let engine = Arc::new(Engine::new());
    let (writer, aof, _dir) = test_writer(engine.clone());
    assert!(
        segment_restore_sink(false, WalBackend::Embedded, engine, None, writer, Some(aof))
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn segment_restore_sink_replaces_current_and_live_state() {
    let live = Arc::new(Engine::new());
    live.create_collection("old", test_schema()).unwrap();
    let source = Engine::new();
    source.create_collection("new", test_schema()).unwrap();
    let snapshot = source.snapshot().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(lumen::segment_rdb::SegmentRdbStore::new(dir.path()).unwrap());
    let aof = Arc::new(std::sync::Mutex::new(
        lumen::aof::AofWriter::open(dir.path().join("aof.log")).unwrap(),
    ));
    let writer = lumen::coordinator::WriteCoordinator::start_from_with_aof(
        Arc::new(MemWal::new()),
        live.clone(),
        0,
        aof.clone(),
    );
    let sink = segment_restore_sink(
        true,
        WalBackend::Embedded,
        live.clone(),
        Some(store.clone()),
        writer,
        Some(aof),
    )
    .unwrap()
    .expect("embedded segment sink");

    sink.restore(snapshot).await.unwrap();
    assert_eq!(live.list_collections().unwrap(), vec!["new".to_string()]);
    let loaded = store.load_current_generation().unwrap().unwrap();
    assert_eq!(
        loaded.engine.list_collections().unwrap(),
        vec!["new".to_string()]
    );
}

#[tokio::test]
async fn segment_restore_sink_is_unavailable_without_store_or_aof() {
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    let engine = Arc::new(Engine::new());
    let writer = lumen::coordinator::WriteCoordinator::start_from(
        Arc::new(MemWal::new()),
        engine.clone(),
        0,
    );
    let restore_sink = segment_restore_sink(
        true,
        WalBackend::Embedded,
        engine.clone(),
        None,
        writer.clone(),
        None,
    )
    .unwrap()
    .expect("missing durable resources must install a fail-closed sink");
    let state = lumen::api::AppState::with_components(
        engine,
        Arc::new(lumen::auth::AuthConfig::open()),
        writer,
    )
    .with_restore_sink(restore_sink);
    let response = lumen::api::router(state)
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/admin/restore")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"version": 1, "collections": {}}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn checkpoint_shared_permit_blocks_exclusive_restore_until_checkpoint_finishes() {
    let gate = MutationGate::default();
    let checkpoint = gate.shared().await.unwrap();
    let mut restore = Box::pin(gate.exclusive());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut restore)
            .await
            .is_err()
    );
    drop(checkpoint);
    tokio::time::timeout(Duration::from_secs(1), restore)
        .await
        .expect("exclusive restore must proceed after checkpoint permit is released")
        .unwrap();
}

#[tokio::test]
async fn manual_checkpoint_can_join_before_queued_exclusive_restore_without_deadlock() {
    let gate = MutationGate::default();
    let checkpoint = gate.shared().await.unwrap();
    let mut restore = Box::pin(gate.exclusive());
    tokio::task::yield_now().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut restore)
            .await
            .is_err()
    );
    let mut next_checkpoint = Box::pin(gate.shared());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut next_checkpoint)
            .await
            .is_err()
    );
    drop(checkpoint);
    // Once the active checkpoint releases, the fair gate lets the queued
    // exclusive restore run. A handler must not hold a second read guard.
    let _restore = tokio::time::timeout(Duration::from_secs(1), restore)
        .await
        .expect("queued restore must not deadlock")
        .unwrap();
    drop(_restore);
    tokio::time::timeout(Duration::from_secs(1), next_checkpoint)
        .await
        .expect("checkpoint must finish after restore")
        .unwrap();
}
