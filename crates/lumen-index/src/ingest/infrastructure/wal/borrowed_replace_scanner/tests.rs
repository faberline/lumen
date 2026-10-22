use std::collections::BTreeMap;

use anyhow::{bail, Result};

use super::{BorrowedReplaceScanner, BorrowedReplaceValue};
use crate::ingest::domain::wal_record::WalRecord;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, ReplaceDocItem, ReplaceDocsRequest};

fn cbor(record: &WalRecord) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(record, &mut bytes).unwrap();
    bytes
}

fn record() -> WalRecord {
    WalRecord {
        version: 1,
        entry: RaftLogEntry::ReplaceDocs {
            collection_id: "docs".into(),
            req: ReplaceDocsRequest {
                docs: vec![
                    ReplaceDocItem {
                        external_id: "first".into(),
                        version: Some(7),
                        fields: BTreeMap::from([
                            ("empty".into(), FieldValue::Vector(vec![])),
                            (
                                "list".into(),
                                FieldValue::StringList(vec!["one".into(), "雪".into()]),
                            ),
                            ("number".into(), FieldValue::Number(-2.5)),
                            ("text".into(), FieldValue::String("borrowed-value".into())),
                            (
                                "vector".into(),
                                FieldValue::Vector(vec![1.5, -0.0, f32::INFINITY]),
                            ),
                        ]),
                    },
                    ReplaceDocItem {
                        external_id: "second".into(),
                        version: None,
                        fields: BTreeMap::from([(
                            "text".into(),
                            FieldValue::String("later".into()),
                        )]),
                    },
                ],
            },
        },
    }
}

#[test]
fn borrows_every_replace_value_and_preserves_document_and_btreemap_order() {
    let bytes = cbor(&record());
    let owned = WalRecord::decode(&bytes).unwrap();
    let RaftLogEntry::ReplaceDocs { collection_id, req } = owned.entry else {
        panic!("fixture must decode as ReplaceDocs")
    };
    let source_start = bytes.as_ptr() as usize;
    let source_end = source_start + bytes.len();
    let scanner = BorrowedReplaceScanner::scan(&bytes, |_| Ok(()))
        .unwrap()
        .unwrap();
    assert_eq!(scanner.collection_id(), "docs");
    assert_eq!(scanner.collection_id(), collection_id);
    assert_eq!(scanner.consumed_bytes(), bytes.len());
    let docs: Vec<_> = scanner.docs().collect();
    assert_eq!(docs.len(), 2);
    assert_eq!(docs[0].external_id(), "first");
    assert_eq!(docs[0].version(), Some(7));
    assert_eq!(docs[0].external_id(), req.docs[0].external_id);
    assert_eq!(docs[0].version(), req.docs[0].version);
    assert_eq!(docs[1].external_id(), "second");
    assert_eq!(docs[1].version(), None);

    let fields: Vec<_> = docs[0].fields().collect();
    assert_eq!(
        fields.iter().map(|field| field.name()).collect::<Vec<_>>(),
        ["empty", "list", "number", "text", "vector"]
    );
    match fields[3].value() {
        BorrowedReplaceValue::String(value) => {
            assert_eq!(value, "borrowed-value");
            assert!(
                matches!(req.docs[0].fields.get("text"), Some(FieldValue::String(owned)) if owned == value)
            );
            assert!(
                (value.as_ptr() as usize) >= source_start && (value.as_ptr() as usize) < source_end
            );
        }
        _ => panic!("text value changed shape"),
    }
    match fields[0].value() {
        BorrowedReplaceValue::Vector(values) => assert_eq!(
            values.collect::<Result<Vec<_>>>().unwrap(),
            Vec::<f32>::new()
        ),
        _ => panic!("empty array must remain Vector"),
    }
    match fields[1].value() {
        BorrowedReplaceValue::StringList(values) => {
            assert_eq!(values.len(), 2);
            assert_eq!(
                values.clone().collect::<Result<Vec<_>>>().unwrap(),
                ["one", "雪"]
            );
            assert_eq!(values.collect::<Result<Vec<_>>>().unwrap(), ["one", "雪"]);
        }
        _ => panic!("string list changed shape"),
    }
    match fields[2].value() {
        BorrowedReplaceValue::Number(value) => {
            assert_eq!(value, -2.5);
            assert!(
                matches!(req.docs[0].fields.get("number"), Some(FieldValue::Number(owned)) if *owned == value)
            );
        }
        _ => panic!("number changed shape"),
    }
    match fields[4].value() {
        BorrowedReplaceValue::Vector(values) => {
            assert_eq!(values.len(), 3);
            let values = values.collect::<Result<Vec<_>>>().unwrap();
            assert_eq!(values[0].to_bits(), 1.5f32.to_bits());
            assert_eq!(values[1].to_bits(), (-0.0f32).to_bits());
            assert!(values[2].is_infinite());
            assert!(
                matches!(req.docs[0].fields.get("vector"), Some(FieldValue::Vector(owned)) if owned.iter().map(|value| value.to_bits()).eq(values.iter().map(|value| value.to_bits())))
            );
        }
        _ => panic!("vector changed shape"),
    }
}

#[test]
fn validates_the_entire_cbor_record_but_ignores_trailing_bytes_like_from_reader() {
    let mut bytes = cbor(&record());
    bytes.extend_from_slice(&[0xf6, 0xf6]);
    let scanner = BorrowedReplaceScanner::scan(&bytes, |_| Ok(()))
        .unwrap()
        .unwrap();
    assert_eq!(scanner.consumed_bytes() + 2, bytes.len());

    let mut invalid = cbor(&record());
    let offset = invalid
        .windows("borrowed-value".len())
        .position(|window| window == b"borrowed-value")
        .unwrap();
    invalid[offset] = 0xff;
    assert!(BorrowedReplaceScanner::scan(&invalid, |_| Ok(())).is_err());
}

#[test]
fn reservation_refusal_happens_before_doc_descriptor_growth() {
    let bytes = cbor(&record());
    let mut requested = 0;
    let error = BorrowedReplaceScanner::scan(&bytes, |bytes| {
        requested += 1;
        bail!("reservation refuses {bytes} descriptor bytes")
    })
    .unwrap_err();
    assert_eq!(requested, 1);
    assert!(error.to_string().contains("reservation refuses"));
}

#[test]
fn large_definite_text_is_a_borrowed_range_and_vector_iteration_is_constant_state() {
    let giant = "x".repeat(65 * 1024);
    let record = WalRecord {
        version: 1,
        entry: RaftLogEntry::ReplaceDocs {
            collection_id: "docs".into(),
            req: ReplaceDocsRequest {
                docs: vec![ReplaceDocItem {
                    external_id: "id".into(),
                    version: None,
                    fields: BTreeMap::from([("giant".into(), FieldValue::String(giant))]),
                }],
            },
        },
    };
    let bytes = cbor(&record);
    let scanner = BorrowedReplaceScanner::scan(&bytes, |_| Ok(()))
        .unwrap()
        .unwrap();
    let field = scanner.docs().next().unwrap().fields().next().unwrap();
    match field.value() {
        BorrowedReplaceValue::String(value) => {
            assert_eq!(value.len(), 65 * 1024);
            let start = bytes.as_ptr() as usize;
            let end = start + bytes.len();
            assert!((value.as_ptr() as usize) >= start && (value.as_ptr() as usize) < end);
        }
        _ => panic!("giant text changed shape"),
    }
}

#[test]
fn accepts_well_formed_more_than_32_docs_for_storage_to_apply_its_bulk_limit() {
    let docs = (0..33)
        .map(|number| ReplaceDocItem {
            external_id: format!("id-{number}"),
            version: None,
            fields: BTreeMap::from([("text".into(), FieldValue::String("x".into()))]),
        })
        .collect();
    let bytes = cbor(&WalRecord {
        version: 1,
        entry: RaftLogEntry::ReplaceDocs {
            collection_id: "docs".into(),
            req: ReplaceDocsRequest { docs },
        },
    });
    assert_eq!(
        BorrowedReplaceScanner::scan(&bytes, |_| Ok(()))
            .unwrap()
            .unwrap()
            .docs()
            .count(),
        33
    );
}

#[test]
fn reserve_requests_complete_aggregate_metadata_peak_and_refuses_before_second_field_growth() {
    let bytes = cbor(&record());
    let mut requests = Vec::new();
    let scanner = BorrowedReplaceScanner::scan(&bytes, |need| {
        requests.push(need);
        Ok(())
    })
    .unwrap()
    .unwrap();
    assert!(requests.iter().copied().max().unwrap() >= scanner.retained_metadata_bytes());
    assert!(
        requests.windows(2).any(|pair| pair[1] > pair[0]),
        "field vectors must include earlier retained vectors"
    );

    let mut calls = 0;
    let error = BorrowedReplaceScanner::scan(&bytes, |_| {
        calls += 1;
        if calls == 3 {
            bail!("refuse before second field vector growth");
        }
        Ok(())
    })
    .unwrap_err();
    assert_eq!(calls, 3);
    assert!(error.to_string().contains("second field vector"));
}

#[test]
fn indefinite_containers_finish_before_following_map_members() {
    fn text(out: &mut Vec<u8>, value: &str) {
        out.push(0x60 | value.len() as u8);
        out.extend_from_slice(value.as_bytes());
    }
    let mut bytes = vec![0xbf]; // top-level indefinite map
    text(&mut bytes, "version");
    bytes.push(1);
    text(&mut bytes, "entry");
    bytes.push(0xbf);
    text(&mut bytes, "ReplaceDocs");
    bytes.push(0xbf);
    text(&mut bytes, "collection_id");
    text(&mut bytes, "docs");
    text(&mut bytes, "req");
    bytes.push(0xbf);
    text(&mut bytes, "docs");
    bytes.push(0x9f);
    bytes.push(0xbf);
    text(&mut bytes, "external_id");
    text(&mut bytes, "one");
    text(&mut bytes, "fields");
    bytes.push(0xbf);
    text(&mut bytes, "set");
    bytes.push(0x9f);
    text(&mut bytes, "a");
    text(&mut bytes, "b");
    bytes.push(0xff);
    bytes.push(0xff);
    bytes.push(0xff);
    bytes.push(0xff); // fields, doc, docs
    text(&mut bytes, "ignored");
    bytes.push(0xf6);
    bytes.push(0xff); // req
    bytes.push(0xff);
    bytes.push(0xff);
    bytes.push(0xff); // body, entry, top
    let scanner = BorrowedReplaceScanner::scan(&bytes, |_| Ok(()))
        .unwrap()
        .unwrap();
    let field = scanner.docs().next().unwrap().fields().next().unwrap();
    match field.value() {
        BorrowedReplaceValue::StringList(values) => {
            assert_eq!(values.collect::<Result<Vec<_>>>().unwrap(), ["a", "b"])
        }
        _ => panic!("indefinite string list changed shape"),
    }
    assert_eq!(scanner.consumed_bytes(), bytes.len());
}

#[test]
fn rejects_multi_variant_entry_and_reuses_validated_string_list_without_a_second_utf8_scan() {
    use ciborium::value::Value;
    let text = |value: &str| Value::Text(value.into());
    let doc = Value::Map(vec![
        (text("external_id"), text("id")),
        (text("fields"), Value::Map(vec![(text("text"), text("x"))])),
    ]);
    let replace = Value::Map(vec![
        (text("collection_id"), text("docs")),
        (
            text("req"),
            Value::Map(vec![(text("docs"), Value::Array(vec![doc]))]),
        ),
    ]);
    let multi_variant_record = Value::Map(vec![
        (text("version"), Value::Integer(1.into())),
        (
            text("entry"),
            Value::Map(vec![
                (text("ReplaceDocs"), replace),
                (text("Index"), Value::Null),
            ]),
        ),
    ]);
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(&multi_variant_record, &mut bytes).unwrap();
    assert!(BorrowedReplaceScanner::scan(&bytes, |_| Ok(())).is_err());

    let bytes = cbor(&record());
    super::reset_utf8_validation_bytes_for_test();
    let scanner = BorrowedReplaceScanner::scan(&bytes, |_| Ok(()))
        .unwrap()
        .unwrap();
    let validated = super::utf8_validation_bytes_for_test();
    let list = scanner
        .docs()
        .next()
        .unwrap()
        .fields()
        .find(|field| field.name() == "list")
        .unwrap();
    for _ in 0..3 {
        match list.value() {
            BorrowedReplaceValue::StringList(values) => {
                assert_eq!(values.collect::<Result<Vec<_>>>().unwrap(), ["one", "雪"]);
            }
            _ => unreachable!(),
        }
    }
    assert_eq!(super::utf8_validation_bytes_for_test(), validated);
}

#[test]
fn bounded_preflight_rejects_break_bad_chunks_and_the_257th_container() {
    assert!(BorrowedReplaceScanner::scan(&[0xff], |_| Ok(())).is_err());
    assert!(BorrowedReplaceScanner::scan(&[0x7f, 0x41, b'x', 0xff], |_| Ok(())).is_err());
    let mut nested = vec![0x81; 257];
    nested.push(0xf6);
    assert!(BorrowedReplaceScanner::scan(&nested, |_| Ok(())).is_err());
}
