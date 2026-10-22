use std::sync::Arc;

use crate::index::application::apply::committed_index_apply::tests::{
    compare_apply, engine, item, request,
};
use crate::index::application::engine::raft_dispatch::ApplyOutcome;
use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::schema::CreateCollectionRequest;

fn hash_engine() -> Arc<Engine> {
    let engine = engine();
    engine
        .create_collection_inner(
            "docs",
            CreateCollectionRequest {
                fields: serde_json::from_value(serde_json::json!({"hash":{"type":"hash"}}))
                    .unwrap(),
            },
        )
        .unwrap();
    engine
}

fn hash_fingerprint(engine: &Engine) -> serde_json::Value {
    let state = engine.state.read().unwrap();
    let coll = &state.collections["docs"];
    let FieldIndex::Hash(hash) = &coll.fields["hash"] else {
        unreachable!()
    };
    let rows: Vec<_> = coll
        .interner
        .to_eid
        .iter()
        .enumerate()
        .map(|(id, eid)| {
            serde_json::json!([
                eid,
                hash.hash_at(id as u32),
                coll.cell_versions.get(&(id as u32)),
                coll.eid_fields
                    .get(&(id as u32))
                    .is_some_and(|fields| fields.contains("hash"))
            ])
        })
        .collect();
    serde_json::json!({"rows": rows, "bytes":hash.bytes})
}

#[test]
fn borrowed_hash_matches_owned_prefix_errors_versions_and_duplicates() {
    let actual = hash_engine();
    let reference = hash_engine();
    let requests = [
        request(
            vec![
                item("one", "kw", FieldValue::String("prefix".into()), Some(1)),
                item(
                    "one",
                    "hash",
                    FieldValue::String(" 0X000042 ".into()),
                    Some(1),
                ),
                item("one", "hash", FieldValue::String("+ff".into()), Some(2)),
                item(
                    "one",
                    "hash",
                    FieldValue::String("wrong but stale".into()),
                    Some(1),
                ),
            ],
            Some("once"),
        ),
        request(
            vec![item(
                "one",
                "hash",
                FieldValue::String("01".into()),
                Some(3),
            )],
            Some("once"),
        ),
        request(
            vec![
                item("two", "hash", FieldValue::String("ff".into()), None),
                item("one", "hash", FieldValue::Number(2.0), Some(3)),
            ],
            None,
        ),
        request(
            vec![
                item("two", "hash", FieldValue::String("10".into()), None),
                item("two", "hash", FieldValue::String("invalid".into()), None),
                item("never", "kw", FieldValue::String("suffix".into()), None),
            ],
            Some("failed"),
        ),
        request(
            vec![
                item("one", "hash", FieldValue::String("17".into()), Some(4)),
                item("one", "missing", FieldValue::String("unknown".into()), None),
            ],
            None,
        ),
    ];
    for (offset, req) in requests.iter().enumerate() {
        compare_apply(&actual, &reference, req, 100 + offset as u64);
        assert_eq!(hash_fingerprint(&actual), hash_fingerprint(&reference));
    }
}

#[test]
fn borrowed_hash_large_leading_zero_source_retains_only_small_changes() {
    // The encoded value is larger than this Engine's entire change budget.
    let actual = hash_engine();
    let root = tempfile::tempdir().unwrap();
    let store =
        crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore::new(root.path())
            .unwrap();
    {
        let apply = actual.capture_barrier.apply();
        actual
            .index_inner(
                "docs",
                request(
                    vec![item("one", "hash", FieldValue::String("01".into()), None)],
                    None,
                ),
                None,
                None,
            )
            .unwrap();
        apply.advance_sequence(22);
    }
    store.save(&actual, 22).unwrap();
    {
        let state = actual.state.read().unwrap();
        let FieldIndex::Hash(hash) = &state.collections["docs"].fields["hash"] else {
            unreachable!()
        };
        assert!(
            hash.segment.is_some(),
            "the next checkpoint must update an existing Hash base"
        );
    }
    let value = format!("0x{}42", "0".repeat(33 * 1024 * 1024));
    let bytes = WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: request(
            vec![
                item("one", "kw", FieldValue::String("prefix".into()), None),
                item("one", "hash", FieldValue::String(value), None),
            ],
            None,
        ),
    })
    .encode()
    .unwrap();
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let mut completed = false;
    assert!(
        actual
            .try_apply_committed_index(&scanner, 23, |apply, result| {
                let ApplyOutcome::Indexed(response) = result.unwrap() else {
                    panic!("Index outcome")
                };
                assert_eq!(response.indexed, 2);
                assert_eq!(response.bytes_written["hash"], 12);
                apply.advance_sequence(23);
                completed = true;
            })
            .unwrap(),
        "valid Hash must use borrowed apply without owning the source string"
    );
    assert!(completed);
    assert_eq!(hash_fingerprint(&actual)["rows"][0][1], 66);
    let pending = actual.changes.budget.snapshot();
    assert!(
        pending.active + pending.frozen + pending.reserved < 1024 * 1024,
        "the retained journal owns the parsed u64, not the source string: {pending:?}"
    );
    store.save(&actual, 23).unwrap();
    let (cold, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 23);
    assert_eq!(hash_fingerprint(&actual)["rows"][0][1], 66);
    assert_eq!(
        hash_fingerprint(&cold)["rows"][0][1],
        66,
        "incremental checkpoint must persist the parsed Hash value"
    );
    // A cold mmap reader has a different resident-byte footprint. Logical
    // rows, versions and coverage must still match the live view exactly.
    assert_eq!(
        hash_fingerprint(&cold)["rows"],
        hash_fingerprint(&actual)["rows"]
    );
}
