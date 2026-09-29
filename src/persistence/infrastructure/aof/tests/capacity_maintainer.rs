use std::sync::{mpsc, Arc};
use std::time::Duration;

use crate::ingest::domain::change_budget::ChangeBudget;
use crate::persistence::infrastructure::aof::aof_writer::AofWriter;
use crate::persistence::infrastructure::aof::frame::encode_payload;
use crate::persistence::infrastructure::aof::replay::replay_aof_into;
use crate::persistence::infrastructure::aof::tests::{create_entry, index_entry, rec, term_query};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::storage::Engine;

#[test]
fn replay_full_waits_for_real_checkpoint_then_replays_and_cold_recovers() {
    // Deliberately below CHECKPOINT_TRIGGER: replay itself must request a
    // checkpoint before its blocking reprice/wait, rather than depending on
    // the soft trigger.
    const HARD: usize = 64 * 1024;
    let budget = ChangeBudget::with_hard_limit(HARD);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    let RaftLogEntry::CreateCollection { req, .. } = create_entry("u") else {
        unreachable!()
    };
    engine.create_collection("u", req).unwrap();
    engine
        .index(
            "u",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: "filler".into(),
                    field: "email".into(),
                    value: FieldValue::String("x".repeat(12 * 1024)),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
    let entry = index_entry("u", "replayed", "after-checkpoint");
    let record = rec(entry);
    let payload = encode_payload(&record).unwrap();
    let raw = Engine::record_owned_bytes(&record.entry).unwrap();
    let active = budget.snapshot().total;
    let needed = raw.checked_add(payload.len()).unwrap();
    assert!(
        active + needed < HARD,
        "fixture must leave a reservable record"
    );
    let held = budget
        .owner()
        .try_reserve(HARD - active - needed + 1)
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let aof = dir.path().join("replay.aof");
    let mut writer = AofWriter::open(&aof).unwrap();
    writer.append(1, &record).unwrap();
    writer.sync().unwrap();
    let before = std::fs::read(&aof).unwrap();
    let wake = budget.checkpoint_wake();
    let observed = wake.epoch();
    let (done_tx, done_rx) = mpsc::channel();
    let replay_engine = engine.clone();
    let replay_path = aof.clone();
    std::thread::spawn(move || {
        done_tx
            .send(replay_aof_into(&replay_engine, replay_path, 0))
            .unwrap();
    });

    assert!(
        wake.wait_for_change_timeout(observed, Duration::from_secs(2)),
        "a full replay record must request a checkpoint before waiting"
    );
    assert!(
        done_rx.try_recv().is_err(),
        "replay must not apply before capacity releases"
    );
    assert_eq!(
        engine
            .search("u", term_query("email", "after-checkpoint"))
            .unwrap()
            .total,
        0
    );

    let store = SegmentRdbStore::new(dir.path().join("segments")).unwrap();
    store.save(&engine, 0).unwrap();
    assert_eq!(
        store.load_latest().unwrap().unwrap().1,
        0,
        "the blocked frame must not advance the checkpoint watermark"
    );
    assert_eq!(
        std::fs::read(&aof).unwrap(),
        before,
        "waiting never rewrites the AOF"
    );
    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        1
    );
    assert_eq!(
        engine
            .search("u", term_query("email", "after-checkpoint"))
            .unwrap()
            .total,
        1
    );

    store.save(&engine, 1).unwrap();
    let (cold, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 1);
    assert_eq!(
        cold.search("u", term_query("email", "after-checkpoint"))
            .unwrap()
            .total,
        1
    );
    drop(held);
}

// Insert inside the existing `#[cfg(test)] mod tests` in src/aof.rs,
// after `replay_full_waits_for_real_checkpoint_then_replays_and_cold_recovers`.
// It uses that module's existing imports and helpers: `ChangeBudget`, `Engine`,
// `AofWriter`, `SegmentRdbStore`, `create_entry`, `index_entry`, `rec`, and
// `term_query`.

#[test]
fn public_replay_full_checkpointable_work_starts_its_own_capacity_maintainer() {
    const HARD: usize = 64 * 1024;
    let budget = ChangeBudget::with_hard_limit(HARD);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    let RaftLogEntry::CreateCollection { req, .. } = create_entry("u") else {
        unreachable!()
    };
    engine.create_collection("u", req).unwrap();
    engine
        .index(
            "u",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: "checkpointable".into(),
                    field: "email".into(),
                    value: FieldValue::String("x".repeat(12 * 1024)),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
    let record = rec(index_entry("u", "replayed", "after-capacity-release"));
    let payload = encode_payload(&record).unwrap();
    let workspace =
        crate::ingest::infrastructure::wire_cost::scan_workspace_bound(&payload).unwrap();
    let raw = Engine::record_owned_bytes(&record.entry).unwrap();
    let crate::ingest::domain::change_record_cost::RecordEstimate::Ready(cost) =
        engine.estimate_record_cost(&record.entry)
    else {
        panic!("known Keyword fixture must have a normalized cost")
    };
    let normalized = raw + cost.active + cost.frozen + cost.prepublish;
    let active = budget.snapshot().total;
    assert!(
        active + workspace < HARD,
        "fixture scanner must fit after publication"
    );
    let held_bytes = HARD - active - workspace + 1;
    let held = budget.owner().try_reserve(held_bytes).unwrap();
    let available = HARD - budget.snapshot().total;
    assert!(
        available < workspace,
        "fixture must block initial scanner admission"
    );
    assert!(
        held_bytes + workspace <= HARD,
        "held charge and scanner workspace must fit after publication"
    );
    assert!(
        held_bytes + normalized <= HARD,
        "held charge and completed normalized record must fit after publication"
    );

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("standalone-progress.aof");
    let mut writer = AofWriter::open(&path).unwrap();
    writer.append(1, &record).unwrap();
    writer.sync().unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let replay_engine = engine.clone();
    std::thread::spawn(move || {
        done_tx
            .send(replay_aof_into(&replay_engine, path, 0))
            .unwrap();
    });

    match done_rx.recv_timeout(Duration::from_secs(2)) {
        Ok(result) => assert_eq!(result.unwrap(), 1),
        Err(timeout) => {
            // Cleanup only. The public replay is required to have arranged this
            // publication itself before the deadline above.
            let store = SegmentRdbStore::new(dir.path().join("cleanup-segments")).unwrap();
            store.save(&engine, 0).unwrap();
            assert_eq!(
                done_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap(),
                1
            );
            panic!("public replay did not start independent capacity maintenance: {timeout}");
        }
    }
    assert_eq!(
        engine
            .search("u", term_query("email", "after-capacity-release"))
            .unwrap()
            .total,
        1
    );
    drop(held);
}

#[test]
fn public_replay_growth_wait_starts_its_own_capacity_maintainer() {
    const HARD: usize = 256 * 1024;
    let budget = ChangeBudget::with_hard_limit(HARD);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    let RaftLogEntry::CreateCollection { req, .. } = create_entry("u") else {
        unreachable!()
    };
    engine.create_collection("u", req).unwrap();
    engine
        .index(
            "u",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: "checkpointable".into(),
                    field: "email".into(),
                    value: FieldValue::String("x".repeat(12 * 1024)),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
    let keyword = FieldSpec {
        field_type: FieldType::Keyword,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    };
    let fields = (0..8)
        .map(|ordinal| (format!("field-{ordinal:03}"), keyword.clone()))
        .collect();
    let record = rec(RaftLogEntry::CreateCollection {
        collection_id: "growth-created".into(),
        req: CreateCollectionRequest { fields },
    });
    let payload = encode_payload(&record).unwrap();
    let workspace =
        crate::ingest::infrastructure::wire_cost::scan_workspace_bound(&payload).unwrap();
    let decoded = crate::ingest::infrastructure::wire_cost::decoded_peak_bound(&payload).unwrap();
    assert!(
        workspace < decoded,
        "fixture must pass scanner reserve before decoded growth"
    );
    let raw = Engine::record_owned_bytes(&record.entry).unwrap();
    let crate::ingest::domain::change_record_cost::RecordEstimate::Ready(cost) =
        engine.estimate_record_cost(&record.entry)
    else {
        panic!("CreateCollection fixture must have a normalized cost")
    };
    let normalized = raw + cost.active + cost.frozen + cost.prepublish;
    let active = budget.snapshot().total;
    assert!(
        active + decoded < HARD,
        "fixture decoded record must fit after publication"
    );
    let held_bytes = HARD - active - decoded + 1;
    let held = budget.owner().try_reserve(held_bytes).unwrap();
    let available = HARD - budget.snapshot().total;
    assert!(workspace <= available, "fixture scanner reserve must fit");
    assert!(
        available < decoded,
        "fixture must block only decoded growth"
    );
    assert!(
        held_bytes + normalized <= HARD,
        "held charge and completed normalized record must fit after publication"
    );

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("standalone-growth.aof");
    let mut writer = AofWriter::open(&path).unwrap();
    writer.append(1, &record).unwrap();
    writer.sync().unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let replay_engine = engine.clone();
    std::thread::spawn(move || {
        done_tx
            .send(replay_aof_into(&replay_engine, path, 0))
            .unwrap();
    });

    match done_rx.recv_timeout(Duration::from_secs(2)) {
        Ok(result) => assert_eq!(result.unwrap(), 1),
        Err(timeout) => {
            let store = SegmentRdbStore::new(dir.path().join("cleanup-segments")).unwrap();
            store.save(&engine, 0).unwrap();
            assert_eq!(
                done_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap(),
                1
            );
            panic!("public replay decoded-growth wait had no capacity maintainer: {timeout}");
        }
    }
    assert!(engine
        .list_collections()
        .unwrap()
        .contains(&"growth-created".to_owned()));
    drop(held);
}
