use std::sync::Arc;

use crate::index::application::engine::Engine;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::persistence::application::capacity::worker::Owner;
use crate::persistence::application::segment_checkpoint_sink::tests::{
    admitted_keyword, spill_keyword_schema,
};
use crate::persistence::application::segment_checkpoint_sink::{
    CheckpointSelection, CheckpointTraceOrigin, EngineWatermarkSink, SegmentCheckpointSink,
};
use crate::persistence::infrastructure::segment_rdb_store::diagnostic::{
    CheckpointDiagnosticContext, DiagnosticCapture,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coalesced_checkpoint_does_not_fabricate_an_attempt_or_diagnostic() {
    let budget = ChangeBudget::new();
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine
        .create_collection("captured", spill_keyword_schema())
        .unwrap();
    admitted_keyword(&engine, "kept", "kept");
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store: store.clone(),
        writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
        aof: None,
    });
    let mut owner = Owner::start(sink.clone(), true).unwrap().unwrap();
    let fence = owner.fence();
    let revision = engine.request_pending_checkpoint_revision().unwrap();
    sink.checkpoint_sync(&store.as_ref().clone().with_publication_fence(fence.clone()))
        .unwrap();
    let completed = engine.metrics().segment_checkpoint_completed_total.get();
    let capture = DiagnosticCapture::new();
    let context =
        CheckpointDiagnosticContext::new("periodic", Some(1)).with_capture(capture.token());
    let result = sink
        .checkpoint_requested_with_fence(
            Some(fence),
            CheckpointTraceOrigin::Periodic,
            Some(context),
            Some(revision),
            Some(CheckpointSelection {
                reason: "capacity_request",
                source: "budget_notice",
                pending: budget.snapshot(),
            }),
        )
        .await
        .unwrap();
    assert!(
        !result,
        "completed relief must not write another checkpoint"
    );
    assert_eq!(
        engine.metrics().segment_checkpoint_completed_total.get(),
        completed
    );
    assert!(
        capture.drain().is_empty(),
        "coalescing must not claim a new selected/frozen/published attempt"
    );
    assert_eq!(
        engine
            .capacity_owner_state()
            .unwrap()
            .checkpoint_request_revision,
        Some(revision)
    );
    engine.consume_checkpoint_request(revision);
    owner.join().unwrap();
}
