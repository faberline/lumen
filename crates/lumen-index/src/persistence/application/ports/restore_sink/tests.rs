use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::index::application::engine::Engine;
use crate::index::infrastructure::snapshot_v1::SnapshotV1;
use crate::ingest::application::write_coordinator::mutation_gate::MutationGate;
use crate::persistence::application::ports::restore_sink::{InMemoryRestoreSink, RestoreSink};

#[tokio::test]
async fn invalid_snapshot_is_rejected_before_exclusive_gate() {
    let engine = Arc::new(Engine::new());
    let gate = MutationGate::default();
    let shared = gate.shared().await.expect("shared permit");
    let sink = InMemoryRestoreSink::new(engine, Some(gate));
    let invalid = SnapshotV1 {
        version: 999,
        collections: BTreeMap::new(),
    };

    let result = tokio::time::timeout(Duration::from_millis(100), sink.restore(invalid))
        .await
        .expect("invalid snapshot must not wait for the exclusive gate");
    assert!(result.is_err());
    drop(shared);
}
