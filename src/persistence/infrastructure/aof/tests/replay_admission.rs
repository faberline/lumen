use std::cell::Cell;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use storage_durable::LogFrame;

use crate::ingest::domain::change_budget::ChangeBudget;
use crate::persistence::infrastructure::aof::aof_writer::AofWriter;
use crate::persistence::infrastructure::aof::frame::encode_payload;
use crate::persistence::infrastructure::aof::replay::{
    replay_aof_into, replay_aof_into_observed, replay_frames, AofReader,
};
use crate::persistence::infrastructure::aof::tests::{
    create_entry, index_entry, rec, term_query, text_match_query,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::storage::Engine;

#[test]
fn replay_reserves_capacity_before_decoding_a_complete_frame() {
    use crate::ingest::domain::change_budget::ChangeBudget;
    use crate::storage::Engine;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::{Duration, Instant};

    const LIMIT: usize = 4 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("aof.log");
    let budget = ChangeBudget::with_hard_limit(LIMIT);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.apply_raft_entry(create_entry("u")).unwrap();
    {
        let mut writer = AofWriter::open(&path).unwrap();
        writer
            .append(1, &rec(index_entry("u", "id", &"x".repeat(512 * 1024))))
            .unwrap();
        writer.sync().unwrap();
    }
    let owner = budget.owner();
    let filler = owner.try_reserve(LIMIT - budget.snapshot().total).unwrap();
    let decodes = Arc::new(AtomicUsize::new(0));
    let replay_engine = engine.clone();
    let replay_decodes = decodes.clone();
    let replay = std::thread::spawn(move || {
        replay_aof_into_observed(&replay_engine, &path, 0, || {
            replay_decodes.fetch_add(1, Ordering::SeqCst);
        })
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !budget.has_capacity_waiters() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    let waited = budget.has_capacity_waiters();
    let before_capacity = decodes.load(Ordering::SeqCst);
    // Always release the real budget owner and join replay before assertions.
    drop(filler);
    let completed = replay.join().unwrap().unwrap();
    assert!(waited, "replay must reach a real capacity wait");
    assert_eq!(
        before_capacity, 0,
        "AOF decoded a frame before pending capacity was reserved"
    );
    assert_eq!(completed, 1);
    assert_eq!(decodes.load(Ordering::SeqCst), 1);
    assert_eq!(engine.stats("u").unwrap().documents_indexed, 1);
}

#[test]
fn replay_does_not_fetch_frame_two_before_applying_frame_one() {
    let frames = std::cell::RefCell::new(VecDeque::from([
        LogFrame {
            seq: 1,
            payload: encode_payload(&rec(create_entry("u"))).unwrap(),
        },
        LogFrame {
            seq: 2,
            payload: encode_payload(&rec(index_entry("u", "u1", "a@x"))).unwrap(),
        },
    ]));
    let polls = Cell::new(0usize);
    let mut applied = Vec::new();

    let max = replay_frames(
        0,
        || {
            polls.set(polls.get() + 1);
            Ok(frames.borrow_mut().pop_front())
        },
        |seq, _| {
            if seq == 1 {
                assert_eq!(
                    polls.get(),
                    1,
                    "frame two must stay unread until frame one applies"
                );
            }
            applied.push(seq);
        },
    )
    .unwrap();

    assert_eq!(applied, vec![1, 2]);
    assert_eq!(max, 2);
}

#[test]
fn public_replay_does_not_checkpoint_reserved_only_capacity_pressure() {
    const HARD: usize = 64 * 1024;
    let budget = ChangeBudget::with_hard_limit(HARD);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    let RaftLogEntry::CreateCollection { req, .. } = create_entry("u") else {
        unreachable!()
    };
    engine.create_collection("u", req).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let baseline = SegmentRdbStore::new(dir.path().join("baseline-segments")).unwrap();
    baseline.save(&engine, 0).unwrap();
    assert_eq!(
        budget.snapshot().total,
        0,
        "baseline checkpoint must release schema setup work"
    );
    let record = rec(index_entry("u", "replayed", "after-reservation-release"));
    let payload = encode_payload(&record).unwrap();
    let workspace =
        crate::ingest::infrastructure::wire_cost::scan_workspace_bound(&payload).unwrap();
    let decoded = crate::ingest::infrastructure::wire_cost::decoded_peak_bound(&payload).unwrap();
    assert!(workspace > 0, "fixture must have scanner admission work");
    assert!(
        decoded < HARD,
        "fixture must be fitting, rather than oversized"
    );
    let held = budget.owner().try_reserve(HARD - workspace + 1).unwrap();
    let checkpoints_before = engine.metrics().segment_checkpoint_completed_total.get();

    let path = dir.path().join("reserved-only.aof");
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

    let waiter_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !budget.has_capacity_waiters() && std::time::Instant::now() < waiter_deadline {
        std::thread::yield_now();
    }
    assert!(
        budget.has_capacity_waiters(),
        "replay must reach the real initial reservation wait"
    );
    assert!(
        done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "reserved-only capacity must not be treated as checkpointable progress"
    );
    assert_eq!(
        engine.metrics().segment_checkpoint_completed_total.get(),
        checkpoints_before,
        "reserved-only pressure must not publish a pointless checkpoint"
    );
    drop(held);
    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        1
    );
    assert_eq!(
        engine
            .search("u", term_query("email", "after-reservation-release"))
            .unwrap()
            .total,
        1
    );
}

#[test]
fn replay_reprice_with_free_capacity_does_not_request_checkpoint() {
    let budget = ChangeBudget::with_hard_limit(64 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.apply_raft_entry(create_entry("u")).unwrap();
    let RaftLogEntry::Index { req, .. } = index_entry("u", "existing", "kept") else {
        unreachable!()
    };
    engine.index("u", req).unwrap();
    assert!(
        budget.snapshot().active > 0,
        "fixture needs checkpointable preceding work"
    );
    let value = "a".repeat(2048);
    let record = rec(index_entry("u", "replayed", &value));
    let raw = Engine::record_owned_bytes(&record.entry).unwrap();
    let crate::ingest::domain::change_record_cost::RecordEstimate::Ready(cost) =
        engine.estimate_record_cost(&record.entry)
    else {
        panic!("Keyword fixture must have a known normalized cost")
    };
    let normalized = raw + cost.active + cost.frozen + cost.prepublish;
    assert!(normalized > raw, "fixture must need normalized repricing");
    assert!(budget.snapshot().total + normalized < 64 * 1024);
    let before = budget.snapshot().checkpoint_request_revision;
    assert_eq!(before, None);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reprice-with-room.aof");
    let mut writer = AofWriter::open(&path).unwrap();
    writer.append(1, &record).unwrap();
    writer.sync().unwrap();
    assert_eq!(replay_aof_into(&engine, &path, 0).unwrap(), 1);
    assert_eq!(
        engine
            .search("u", term_query("email", &value))
            .unwrap()
            .total,
        1
    );
    assert_eq!(
        budget.snapshot().checkpoint_request_revision,
        before,
        "repricing that fits must not force a checkpoint of the preceding record"
    );
}

#[test]
fn replay_reprice_error_keeps_failed_frame_and_watermark() {
    // This is deliberately an unresolved oversized prepared-Text record.
    // The ordinary replay route must return Err before it mutates or
    // advances, rather than using the old uncharged apply fallback.
    let budget = ChangeBudget::with_hard_limit(64 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget));
    engine
        .create_collection(
            "text",
            CreateCollectionRequest {
                fields: BTreeMap::from([(
                    "body".into(),
                    FieldSpec {
                        field_type: FieldType::Text,
                        analyzer: Some(crate::shared_kernel::types::schema::Analyzer::Ngram),
                        multi: None,
                        dim: None,
                        metric: None,
                        backend: None,
                        quantize: None,
                    },
                )]),
            },
        )
        .unwrap();
    // Repetitions now admit their distinct terms. Keep this refusal
    // fixture truly oversized even when its terms are priced exactly.
    let value = format!(
        "ab{}",
        (0x4e00..0x4e00 + 512)
            .map(|scalar| char::from_u32(scalar).unwrap())
            .collect::<String>()
    );
    let mut distinct = std::collections::BTreeSet::new();
    crate::ngram_stream::stream_default_ngrams(&value, |token| {
        distinct.insert(token.to_owned());
        Ok::<_, ()>(())
    })
    .unwrap();
    let normalized = crate::ingest::domain::change_memory_cost::estimate_change(
        &crate::ingest::domain::change_memory_cost::Change::Index {
            external_id_bytes: "not-applied".len(),
            new_document: true,
            field: crate::ingest::domain::change_memory_cost::FieldCost::Text {
                distinct_terms: distinct.len(),
                total_term_bytes: distinct.iter().map(String::len).sum(),
            },
            volatile_metadata_bytes: 0,
        },
    )
    .unwrap();
    assert!(
        normalized.total() > 64 * 1024,
        "distinct normalized data must exceed this test's budget before raw transport is added"
    );
    let record = rec(RaftLogEntry::Index {
        collection_id: "text".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "not-applied".into(),
                field: "body".into(),
                value: FieldValue::String(value),
                version: None,
            }],
            request_id: None,
        },
    });
    let dir = tempfile::tempdir().unwrap();
    let aof = dir.path().join("reprice.aof");
    let mut writer = AofWriter::open(&aof).unwrap();
    writer.append(1, &record).unwrap();
    writer.sync().unwrap();
    let before = std::fs::read(&aof).unwrap();

    assert!(replay_aof_into(&engine, &aof, 0).is_err());
    assert_eq!(
        std::fs::read(&aof).unwrap(),
        before,
        "failed replay preserves its source"
    );
    assert_eq!(
        engine
            .search("text", text_match_query("body", "ab"))
            .unwrap()
            .total,
        0
    );
    let store = SegmentRdbStore::new(dir.path().join("segments")).unwrap();
    store.save(&engine, 0).unwrap();
    assert_eq!(store.load_latest().unwrap().unwrap().1, 0);
    assert_eq!(
        AofReader::replay(&aof, 0, |seq, _| assert_eq!(seq, 1)).unwrap(),
        1
    );
}
