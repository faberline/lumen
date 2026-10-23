use crate::index::application::admission::tests::{engine, entry};
use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};

#[test]
fn admission_direct_engine_rejects_before_mutation_when_capacity_is_reserved() {
    let budget = ChangeBudget::with_hard_limit(1 << 20);
    let engine = engine(&budget);
    let remaining = (1 << 20) - budget.snapshot().total;
    let competing = budget.owner();
    let held = competing.try_reserve(remaining).unwrap();
    let RaftLogEntry::Index { req, .. } = entry() else {
        unreachable!()
    };
    let result = engine.index("c", req.clone());
    assert!(
        result.is_err(),
        "direct Engine mutation must reserve capacity before changing state"
    );
    assert!(result
        .unwrap_err()
        .downcast_ref::<crate::ingest::domain::change_admission::PendingChangeCapacity>()
        .is_some());
    assert!(engine.state.read().unwrap().collections["c"]
        .interner
        .id("one")
        .is_none());
    drop(held);
    let before = budget.snapshot().active;
    engine.index("c", req).unwrap();
    assert!(
        budget.snapshot().active > before,
        "direct Engine changes must retain their actual pending charge"
    );
    let root = tempfile::tempdir().unwrap();
    let store =
        crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore::new(root.path())
            .unwrap();
    store.save(&std::sync::Arc::new(engine), 0).unwrap();
    assert_eq!(budget.snapshot().total, 0);
    let (cold, _) = store.load_latest().unwrap().unwrap();
    assert!(cold.state.read().unwrap().collections["c"]
        .interner
        .id("one")
        .is_some());
}

#[test]
fn local_record_split_keeps_transport_out_of_apply_reprice() {
    let budget = ChangeBudget::with_hard_limit(1 << 20);
    let engine = engine(&budget);
    let raw = Engine::record_owned_bytes(&entry()).unwrap();
    let accepted = engine.try_reserve_record(&entry(), raw * 2).unwrap();
    let total = accepted.bytes();

    let (apply, transient) = accepted.split_transport();
    let transient = transient.expect("transport-owned copies must split");
    assert_eq!(apply.extra_owned, 0);
    assert_eq!(transient.bytes(), raw * 2);
    assert_eq!(
        apply.bytes() + transient.bytes(),
        total,
        "split keeps the one atomic admission total"
    );
    assert_eq!(budget.snapshot().reserved, total);
    drop(apply);
    assert_eq!(budget.snapshot().reserved, transient.bytes());
    drop(transient);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn admission_stale_prepared_text_releases_the_discarded_reader_charge() {
    let budget = ChangeBudget::with_hard_limit(64 * 1024 * 1024);
    let engine = engine(&budget);
    engine
        .add_field(
            "c",
            "body",
            serde_json::from_str(r#"{"type":"text","analyzer":"ngram"}"#).unwrap(),
        )
        .unwrap();
    let entry = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "text".into(),
                field: "body".into(),
                value: FieldValue::String("ababa".into()),
                version: None,
            }],
            request_id: None,
        },
    };
    let mut reserved = engine.try_reserve_record(&entry, 0).unwrap();
    reserved.staged_text = true;
    reserved
        .try_grow_to(engine.record_memory_bound(&entry, 0, true).unwrap())
        .unwrap();
    reserved.prepared_text = Some(engine.prepare_text_rows(&entry, &mut reserved).unwrap());
    assert!(reserved.preparation_bytes > 0);
    let once = reserved.preparation_bytes;
    engine
        .add_field(
            "c",
            "later",
            serde_json::from_str(r#"{"type":"keyword"}"#).unwrap(),
        )
        .unwrap();
    // Price one prepared row after the schema change. The cost table is
    // temporary; its reservation must not become retained data. A stale
    // preparation retry must retain exactly one reader charge.
    let expected = engine.record_memory_bound(&entry, 0, true).unwrap() + once;
    let mut apply = engine.begin_admitted_record(entry, reserved).ok().unwrap();
    assert_eq!(
        apply.charge.bytes(),
        expected,
        "discarded prepared Text rows must not accumulate reserved bytes on retry"
    );
    engine.apply_prepared_raft_entry(&mut apply).unwrap();
}

#[test]
fn prepared_text_dispatch_keeps_payload_mapped_and_uses_sparse_document_rows() {
    let budget = ChangeBudget::with_hard_limit(64 * 1024 * 1024);
    let engine = engine(&budget);
    engine
        .add_field(
            "c",
            "body",
            serde_json::from_str(r#"{"type":"text","analyzer":"ngram"}"#).unwrap(),
        )
        .unwrap();
    // An unrelated field can establish a large runtime ID before Text is written.
    {
        let mut state = engine.state.write().unwrap();
        for id in 0..10_000 {
            state
                .collections
                .get_mut("c")
                .unwrap()
                .interner
                .intern(&format!("unused-{id}"));
        }
    }
    let entry = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "last".into(),
                field: "body".into(),
                value: FieldValue::String("ababa".into()),
                version: None,
            }],
            request_id: None,
        },
    };
    let mut reserved = engine.try_reserve_record(&entry, 0).unwrap();
    reserved.staged_text = true;
    let required = engine.record_memory_bound(&entry, 0, true).unwrap();
    reserved.wait_grow_to(required).unwrap();
    let mut guard = engine.begin_admitted_record(entry, reserved).ok().unwrap();
    engine.apply_prepared_raft_entry(&mut guard).unwrap();
    drop(guard);
    {
        let state = engine.state.read().unwrap();
        let FieldIndex::Text { idx, .. } = &state.collections["c"].fields["body"] else {
            unreachable!()
        };
        assert!(
            idx.tokens.is_empty(),
            "prepared Text must not rebuild normalized postings in RAM"
        );
        assert!(
            idx.lens.is_empty() && idx.distinct.is_empty(),
            "sparse Text must not allocate the global document ID prefix"
        );
        assert_eq!(idx.staged_rows.len(), 1);
        assert_eq!(idx.tok_postings("ab").unwrap().tfs(), &[2]);
        assert_eq!(idx.doc_len(10_000), 7);
    }
    engine
        .index(
            "c",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: "last".into(),
                    field: "body".into(),
                    value: FieldValue::String("bcdef".into()),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
    let state = engine.state.read().unwrap();
    let FieldIndex::Text { idx, .. } = &state.collections["c"].fields["body"] else {
        unreachable!()
    };
    assert!(idx.staged_rows.is_empty());
    assert!(idx.lens.is_empty() && idx.distinct.is_empty());
    assert_eq!(idx.delta_docs.len(), 1);
    assert!(idx.tok_postings("ab").is_none());
}
