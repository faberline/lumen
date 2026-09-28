use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use serde::Deserialize;

use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::bounded_generic::decode;
use crate::ingest::infrastructure::wal::bounded_generic::field_value::WireFieldValue;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{
    FieldValue, IndexItem, IndexRequest, ReplaceDocItem, ReplaceDocsRequest,
};
use crate::shared_kernel::types::schema::{
    Analyzer, CreateCollectionRequest, FieldSpec, FieldType,
};

fn legacy_decode(bytes: &[u8]) -> Result<WalRecord> {
    match ciborium::de::from_reader(bytes) {
        Ok(record) => Ok(record),
        Err(cbor_err) => serde_json::from_slice(bytes).map_err(|json_err| {
            anyhow!("decode WAL record as cbor ({cbor_err}) or legacy json ({json_err})")
        }),
    }
}

fn assert_same_wire_result(bytes: &[u8]) {
    let old = legacy_decode(bytes);
    let new = decode(bytes);
    match (old, new) {
        (Ok(old), Ok(new)) => {
            let mut old_bytes = Vec::new();
            let mut new_bytes = Vec::new();
            ciborium::ser::into_writer(&old, &mut old_bytes).unwrap();
            ciborium::ser::into_writer(&new, &mut new_bytes).unwrap();
            assert_eq!(old_bytes, new_bytes, "generic decode changed the record");
        }
        (Err(_), Err(_)) => {}
        (old, new) => panic!("old/new decode disagree: old={old:?}, new={new:?}"),
    }
}

fn cbor(record: &WalRecord) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(record, &mut bytes).unwrap();
    bytes
}

#[test]
fn generic_cbor_and_legacy_json_preserve_every_entry_shape_and_defaults() {
    let spec = FieldSpec {
        field_type: FieldType::Text,
        analyzer: Some(Analyzer::WhitespaceLower),
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    };
    let records = vec![
        WalRecord {
            version: 1,
            entry: RaftLogEntry::CreateCollection {
                collection_id: "集合".into(),
                req: CreateCollectionRequest {
                    fields: BTreeMap::from([("title".into(), spec.clone())]),
                },
            },
        },
        WalRecord {
            version: 1,
            entry: RaftLogEntry::Index {
                collection_id: "docs".into(),
                req: IndexRequest {
                    request_id: None,
                    items: vec![
                        IndexItem {
                            external_id: "id".into(),
                            field: "text".into(),
                            value: FieldValue::String("escaped \\ \" 雪".into()),
                            version: None,
                        },
                        IndexItem {
                            external_id: "id".into(),
                            field: "number".into(),
                            value: FieldValue::Number(4.5),
                            version: Some(9),
                        },
                        IndexItem {
                            external_id: "id".into(),
                            field: "vector".into(),
                            value: FieldValue::Vector(vec![0.25, 0.5]),
                            version: None,
                        },
                        IndexItem {
                            external_id: "id".into(),
                            field: "set".into(),
                            value: FieldValue::StringList(vec!["é".into(), "雪".into()]),
                            version: None,
                        },
                        IndexItem {
                            external_id: "id".into(),
                            field: "empty".into(),
                            value: FieldValue::Vector(Vec::new()),
                            version: None,
                        },
                    ],
                },
            },
        },
        WalRecord {
            version: 1,
            entry: RaftLogEntry::ReplaceDocs {
                collection_id: "docs".into(),
                req: ReplaceDocsRequest {
                    docs: vec![ReplaceDocItem {
                        external_id: "id".into(),
                        version: None,
                        fields: BTreeMap::from([
                            ("title".into(), FieldValue::String("多字節".into())),
                            ("embedding".into(), FieldValue::Vector(vec![1.0, 2.0])),
                        ]),
                    }],
                },
            },
        },
        WalRecord {
            version: 1,
            entry: RaftLogEntry::Delete {
                collection_id: "docs".into(),
                external_id: "id".into(),
                field: None,
            },
        },
        WalRecord {
            version: 1,
            entry: RaftLogEntry::DropCollection {
                collection_id: "docs".into(),
                force: true,
            },
        },
        WalRecord {
            version: 1,
            entry: RaftLogEntry::AddField {
                collection_id: "docs".into(),
                field_name: "title".into(),
                spec: spec.clone(),
            },
        },
        WalRecord {
            version: 1,
            entry: RaftLogEntry::DropField {
                collection_id: "docs".into(),
                field_name: "title".into(),
            },
        },
        WalRecord {
            version: 2,
            entry: RaftLogEntry::TruncateDocs {
                collection_id: "docs".into(),
            },
        },
        WalRecord {
            version: 2,
            entry: RaftLogEntry::UnindexDocs {
                collection_id: "docs".into(),
                req: crate::shared_kernel::types::document::BatchUnindexDocsRequest {
                    external_ids: vec!["id".into()],
                },
            },
        },
    ];
    for record in records {
        let bytes = cbor(&record);
        assert_same_wire_result(&bytes);
        assert_same_wire_result(&serde_json::to_vec(&record).unwrap());
    }

    let nonfinite = WalRecord {
        version: 1,
        entry: RaftLogEntry::Index {
            collection_id: "docs".into(),
            req: IndexRequest {
                request_id: Some("cbor-only-nonfinite".into()),
                items: vec![IndexItem {
                    external_id: "id".into(),
                    field: "embedding".into(),
                    value: FieldValue::Vector(vec![f32::NAN, f32::INFINITY]),
                    version: None,
                }],
            },
        },
    };
    assert_same_wire_result(&cbor(&nonfinite));
}

#[test]
fn generic_field_value_rejects_mixed_and_non_value_shapes_like_untagged_field_value() {
    for json in [
        br#"[1,"two"]"#.as_slice(),
        br#"null"#.as_slice(),
        br#"true"#.as_slice(),
        br#"{}"#.as_slice(),
    ] {
        assert!(
            serde_json::from_slice::<WireFieldValue>(json).is_err(),
            "{json:?}"
        );
        assert!(
            serde_json::from_slice::<FieldValue>(json).is_err(),
            "{json:?}"
        );
    }
    let empty: WireFieldValue = serde_json::from_slice(br#"[]"#).unwrap();
    assert!(matches!(empty.0, FieldValue::Vector(values) if values.is_empty()));
}

#[test]
fn generic_unknown_unindex_keys_keep_legacy_refusal() {
    let value = serde_json::json!({
        "version": 1,
        "entry": {"UnindexDocs": {"collection_id": "c", "req": {
            "external_ids": ["id"], "unexpected": true
        }}}
    });
    let mut cbor = Vec::new();
    ciborium::ser::into_writer(&value, &mut cbor).unwrap();
    for bytes in [serde_json::to_vec(&value).unwrap(), cbor] {
        assert!(legacy_decode(&bytes).is_err());
        assert!(
            decode(&bytes).is_err(),
            "generic decoder accepted an unknown control field"
        );
    }
}

#[test]
fn generic_integer_vector_tokens_keep_direct_f32_rounding() {
    let value = serde_json::json!({
        "version": 1,
        "entry": {"Index": {"collection_id": "c", "req": {"items": [{
            "external_id": "id", "field": "v",
            "value": [(1u64 << 63) + (1u64 << 39) + 1]
        }]}}}
    });
    let mut cbor = Vec::new();
    ciborium::ser::into_writer(&value, &mut cbor).unwrap();
    for bytes in [serde_json::to_vec(&value).unwrap(), cbor] {
        for record in [legacy_decode(&bytes).unwrap(), decode(&bytes).unwrap()] {
            let RaftLogEntry::Index { req, .. } = record.entry else {
                panic!("expected Index")
            };
            let FieldValue::Vector(vector) = &req.items[0].value else {
                panic!("expected Vector")
            };
            assert_eq!(
                vector[0].to_bits(),
                0x5f000001,
                "integer vector token was double-rounded"
            );
        }
    }
}

#[test]
fn owned_string_token_moves_into_final_field_value_without_content_copy() {
    let owned = "x".repeat(256 * 1024);
    let allocation = owned.as_ptr();
    let value = WireFieldValue::deserialize(serde::de::value::StringDeserializer::<
        serde::de::value::Error,
    >::new(owned))
    .unwrap();
    let FieldValue::String(final_string) = value.0 else {
        panic!("string token must stay a string");
    };
    assert_eq!(
        final_string.as_ptr(),
        allocation,
        "decoder copied an owned string token"
    );
}
