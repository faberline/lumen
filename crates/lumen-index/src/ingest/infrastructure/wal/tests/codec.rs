use anyhow::Result;

use crate::ingest::domain::wal_record::{
    WalRecord, WAL_CONTROL_FORMAT_VERSION, WAL_FORMAT_VERSION,
};
use crate::ingest::infrastructure::wal::encode::{put_str, put_u32};
use crate::ingest::infrastructure::wal::tests::create_entry;
use crate::ingest::infrastructure::wal::{
    WAL_FAST_INDEX, WAL_FAST_INDEX_VERSIONED, WAL_FAST_MAGIC, WAL_FAST_TRUNCATE_DOCS,
    WAL_FAST_UNINDEX_DOCS, WAL_VALUE_STRING,
};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{
    BatchUnindexDocsRequest, FieldValue, IndexItem, IndexRequest, MAX_BATCH_UNINDEX_DOCS_SIZE,
};

#[test]
fn record_round_trips() {
    let rec = WalRecord::new(create_entry("users"));
    let bytes = rec.encode().unwrap();
    let back = WalRecord::decode(&bytes).unwrap();
    assert!(matches!(back.entry, RaftLogEntry::CreateCollection { .. }));
    assert_eq!(back.version, WAL_FORMAT_VERSION);
}

#[test]
fn truncate_control_record_round_trips_and_fails_a_v1_reader_at_version_gate() {
    let bytes = WalRecord::new(RaftLogEntry::TruncateDocs {
        collection_id: "users".into(),
    })
    .encode()
    .unwrap();
    assert!(bytes.starts_with(WAL_FAST_MAGIC));
    assert_eq!(bytes[WAL_FAST_MAGIC.len()], WAL_CONTROL_FORMAT_VERSION);
    assert_eq!(bytes[WAL_FAST_MAGIC.len() + 1], WAL_FAST_TRUNCATE_DOCS);

    let back = WalRecord::decode(&bytes).expect("0.4.31 must read a v2 control record");
    assert!(matches!(
        back.entry,
        RaftLogEntry::TruncateDocs { collection_id } if collection_id == "users"
    ));

    // This is the pre-0.4.31 fast-record entrance check.  It reads and
    // rejects the version byte before looking at the command tag, so a
    // direct downgrade fails closed with the intended compatibility
    // boundary rather than attempting to decode `TruncateDocs`.
    let v1_reader = || -> Result<()> {
        anyhow::ensure!(bytes.starts_with(WAL_FAST_MAGIC), "invalid WAL fast magic");
        anyhow::ensure!(
            bytes[WAL_FAST_MAGIC.len()] == WAL_FORMAT_VERSION,
            "unsupported WAL fast record version {} (expected {})",
            bytes[WAL_FAST_MAGIC.len()],
            WAL_FORMAT_VERSION
        );
        Ok(())
    };
    assert!(
        v1_reader().is_err(),
        "a v1 reader must refuse the v2 byte first"
    );
}

#[test]
fn generic_wal_envelope_cannot_smuggle_control_versions_or_commands() {
    let truncate = RaftLogEntry::TruncateDocs {
        collection_id: "users".into(),
    };
    let unindex = RaftLogEntry::UnindexDocs {
        collection_id: "users".into(),
        req: BatchUnindexDocsRequest {
            external_ids: vec!["d1".into()],
        },
    };
    assert!(
        WalRecord {
            version: WAL_FORMAT_VERSION,
            entry: truncate.clone(),
        }
        .encode()
        .is_err(),
        "a v1 envelope must never emit TruncateDocs"
    );
    assert!(
        WalRecord {
            version: WAL_FORMAT_VERSION,
            entry: unindex.clone(),
        }
        .encode()
        .is_err(),
        "a v1 envelope must never emit UnindexDocs"
    );
    assert!(
        WalRecord {
            version: WAL_CONTROL_FORMAT_VERSION,
            entry: create_entry("users"),
        }
        .encode()
        .is_err(),
        "v2 is reserved for the fast control tag"
    );

    // Decode must enforce the same boundary even for bytes that bypassed
    // this build's encoder (for example, a malformed external WAL frame).
    for record in [
        WalRecord {
            version: WAL_FORMAT_VERSION,
            entry: truncate,
        },
        WalRecord {
            version: WAL_CONTROL_FORMAT_VERSION,
            entry: create_entry("users"),
        },
        WalRecord {
            version: WAL_FORMAT_VERSION,
            entry: unindex,
        },
    ] {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&record, &mut bytes).unwrap();
        assert!(WalRecord::decode(&bytes).is_err());
    }
}

#[test]
fn unindex_control_record_round_trips_and_rejects_invalid_fast_shapes() {
    let entry = RaftLogEntry::UnindexDocs {
        collection_id: "users".into(),
        req: BatchUnindexDocsRequest {
            external_ids: vec!["one".into(), "two".into()],
        },
    };
    let bytes = WalRecord::new(entry).encode().unwrap();
    assert!(bytes.starts_with(WAL_FAST_MAGIC));
    assert_eq!(bytes[WAL_FAST_MAGIC.len()], WAL_CONTROL_FORMAT_VERSION);
    assert_eq!(bytes[WAL_FAST_MAGIC.len() + 1], WAL_FAST_UNINDEX_DOCS);
    assert!(matches!(
        WalRecord::decode(&bytes).unwrap().entry,
        RaftLogEntry::UnindexDocs { collection_id, req }
            if collection_id == "users" && req.external_ids == ["one", "two"]
    ));

    // This is the legacy entrance guard: an older reader refuses the v2
    // byte before it can observe either control tag.
    assert_ne!(bytes[WAL_FAST_MAGIC.len()], WAL_FORMAT_VERSION);

    let mut over_limit = Vec::new();
    over_limit.extend_from_slice(WAL_FAST_MAGIC);
    over_limit.push(WAL_CONTROL_FORMAT_VERSION);
    over_limit.push(WAL_FAST_UNINDEX_DOCS);
    put_str(&mut over_limit, "users").unwrap();
    put_u32(&mut over_limit, MAX_BATCH_UNINDEX_DOCS_SIZE + 1).unwrap();
    let err = WalRecord::decode(&over_limit).unwrap_err();
    assert!(
        err.to_string().contains("item count"),
        "count must fail before allocation/read, got: {err}"
    );

    let mut duplicate = Vec::new();
    duplicate.extend_from_slice(WAL_FAST_MAGIC);
    duplicate.push(WAL_CONTROL_FORMAT_VERSION);
    duplicate.push(WAL_FAST_UNINDEX_DOCS);
    put_str(&mut duplicate, "users").unwrap();
    put_u32(&mut duplicate, 2).unwrap();
    put_str(&mut duplicate, "same").unwrap();
    put_str(&mut duplicate, "same").unwrap();
    let err = WalRecord::decode(&duplicate).unwrap_err();
    assert!(err.to_string().contains("duplicate"), "got: {err}");
}

#[test]
fn fast_index_record_round_trips_all_value_shapes() {
    let rec = WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: IndexRequest {
            request_id: Some("req-1".into()),
            items: vec![
                IndexItem {
                    external_id: "doc-1".into(),
                    field: "title".into(),
                    value: FieldValue::String("lumen".into()),
                    version: None,
                },
                IndexItem {
                    external_id: "doc-1".into(),
                    field: "score".into(),
                    value: FieldValue::Number(42.5),
                    version: None,
                },
                IndexItem {
                    external_id: "doc-1".into(),
                    field: "embedding".into(),
                    value: FieldValue::Vector(vec![0.25, 0.5, 0.75]),
                    version: None,
                },
                IndexItem {
                    external_id: "doc-1".into(),
                    field: "tags".into(),
                    value: FieldValue::StringList(vec!["rust".into(), "search".into()]),
                    version: None,
                },
            ],
        },
    });
    let bytes = rec.encode().unwrap();
    assert!(bytes.starts_with(WAL_FAST_MAGIC));

    let back = WalRecord::decode(&bytes).unwrap();
    assert_eq!(back.version, WAL_FORMAT_VERSION);
    let RaftLogEntry::Index { collection_id, req } = back.entry else {
        panic!("expected index record");
    };
    assert_eq!(collection_id, "docs");
    assert_eq!(req.request_id.as_deref(), Some("req-1"));
    assert_eq!(req.items.len(), 4);
    assert!(matches!(
        &req.items[0].value,
        FieldValue::String(s) if s == "lumen"
    ));
    assert!(matches!(
        req.items[1].value,
        FieldValue::Number(n) if (n - 42.5).abs() < f64::EPSILON
    ));
    assert!(matches!(
        &req.items[2].value,
        FieldValue::Vector(v) if v == &[0.25, 0.5, 0.75]
    ));
    assert!(matches!(
        &req.items[3].value,
        FieldValue::StringList(values) if values == &["rust".to_string(), "search".to_string()]
    ));
    assert!(
        req.items.iter().all(|item| item.version.is_none()),
        "no item in this record carried a version; decode must not invent one"
    );
}

/// #3952: `IndexItem.version` (#184 external LWW) must survive the fast
/// codec round trip — this is the exact gap the AOF replay bug (#3952)
/// came from: the wire never carried it, so every replayed item looked
/// unversioned and LWW silently degraded to arrival order.
#[test]
fn fast_index_record_round_trips_versioned_items() {
    let rec = WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: IndexRequest {
            request_id: None,
            items: vec![
                IndexItem {
                    external_id: "d1".into(),
                    field: "kw".into(),
                    value: FieldValue::String("v5".into()),
                    version: Some(5),
                },
                IndexItem {
                    external_id: "d1".into(),
                    field: "kw".into(),
                    value: FieldValue::String("v3".into()),
                    version: Some(3),
                },
                IndexItem {
                    external_id: "d2".into(),
                    field: "kw".into(),
                    value: FieldValue::String("unversioned".into()),
                    version: None,
                },
            ],
        },
    });
    let bytes = rec.encode().unwrap();
    assert!(bytes.starts_with(WAL_FAST_MAGIC));
    assert_eq!(
        bytes[5], WAL_FAST_INDEX_VERSIONED,
        "a record carrying versions must be written with the versioned tag \
             (byte layout: 4-byte magic, then a 1-byte format version, then this tag)"
    );

    let back = WalRecord::decode(&bytes).unwrap();
    let RaftLogEntry::Index { req, .. } = back.entry else {
        panic!("expected index record");
    };
    assert_eq!(req.items[0].version, Some(5));
    assert_eq!(req.items[1].version, Some(3));
    assert_eq!(
        req.items[2].version, None,
        "an item with no version must decode back to None, not 0 or Some(anything)"
    );
}

/// #3952 negative control: an AOF/WAL segment written by a binary before
/// this fix used tag `WAL_FAST_INDEX` (1) and never wrote a version byte
/// per item at all. That decode branch must stay byte-for-byte readable.
///
/// Today's encoder still emits that tag — it picks the tag from the content,
/// and a batch where no item carries a version is written unversioned, which
/// is what keeps a segment readable by a peer that has not been upgraded
/// yet. So this test does not bypass the encoder to reach an unreachable
/// shape; it hand-builds the bytes so the branch is measured against a
/// literal layout rather than against whatever the encoder currently emits
/// — the encoder is the thing under suspicion in a compatibility case.
/// It confirms decode still succeeds with every item's version
/// reconstructed as `None`, matching the pre-fix behavior exactly.
#[test]
fn decode_fast_record_still_reads_legacy_unversioned_tag() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(WAL_FAST_MAGIC);
    bytes.push(WAL_FORMAT_VERSION);
    bytes.push(WAL_FAST_INDEX); // legacy tag — no per-item version byte
    put_str(&mut bytes, "docs").unwrap();
    bytes.push(0); // no request_id
    put_u32(&mut bytes, 1).unwrap(); // one item
    put_str(&mut bytes, "d1").unwrap();
    put_str(&mut bytes, "kw").unwrap();
    bytes.push(WAL_VALUE_STRING);
    put_str(&mut bytes, "v3").unwrap();

    let back = WalRecord::decode(&bytes).expect("legacy fast-Index record must still decode");
    let RaftLogEntry::Index { collection_id, req } = back.entry else {
        panic!("expected index record");
    };
    assert_eq!(collection_id, "docs");
    assert_eq!(req.items.len(), 1);
    assert_eq!(req.items[0].external_id, "d1");
    assert!(matches!(&req.items[0].value, FieldValue::String(s) if s == "v3"));
    assert_eq!(
        req.items[0].version, None,
        "a pre-#3952 record never had a version on the wire; it must decode as None, \
             exactly as it did before this fix — never fabricated from thin air"
    );
}

#[test]
fn decode_rejects_bad_version() {
    let bytes = WalRecord {
        version: 9,
        entry: create_entry("u"),
    }
    .encode()
    .unwrap();
    assert!(WalRecord::decode(&bytes).is_err());
}

#[test]
fn decode_accepts_legacy_json_payload() {
    let rec = WalRecord::new(create_entry("legacy-json"));
    let bytes = serde_json::to_vec(&rec).unwrap();
    let back = WalRecord::decode(&bytes).unwrap();
    assert!(matches!(back.entry, RaftLogEntry::CreateCollection { .. }));
    assert_eq!(back.version, WAL_FORMAT_VERSION);
}
