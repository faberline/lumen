use std::collections::BTreeMap;

use crate::index::application::admission;
use crate::index::application::engine::Engine;
use crate::index::application::text_preparation::{
    borrowed_field_metadata_bound, borrowed_text_metadata_bound, TEXT_SCRATCH_BYTES,
};
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::{
    Analyzer, CreateCollectionRequest, FieldSpec, FieldType,
};

fn spec(field_type: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
    FieldSpec {
        field_type,
        analyzer,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}

fn engine(fields: &[(&str, FieldType)]) -> Engine {
    let mut schema = BTreeMap::new();
    for (name, kind) in fields {
        schema.insert(
            (*name).to_owned(),
            spec(
                *kind,
                (*kind == FieldType::Text).then_some(Analyzer::WhitespaceLower),
            ),
        );
    }
    let engine = Engine::new();
    engine
        .create_collection("docs", CreateCollectionRequest { fields: schema })
        .unwrap();
    engine
}

fn record_reservation(
    engine: &Engine,
    scanner: &FastIndexScanner<'_>,
) -> admission::record_reservation::RecordReservation {
    let bytes = borrowed_text_metadata_bound(scanner)
        .unwrap()
        .checked_mul(2)
        .and_then(|metadata| metadata.checked_add(TEXT_SCRATCH_BYTES))
        .unwrap();
    engine
        .wait_reserve_record_ram(&engine.record_ram_request_from_bound(bytes, 0))
        .unwrap()
}

fn encode(items: Vec<IndexItem>, request_id: Option<&str>) -> Vec<u8> {
    WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: IndexRequest {
            items,
            request_id: request_id.map(str::to_owned),
        },
    })
    .encode()
    .unwrap()
}

#[test]
fn borrowed_text_rows_match_owned_rows_and_keep_duplicate_ordinals_metadata() {
    let entry = RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: IndexRequest {
            request_id: Some("request-7".into()),
            items: vec![
                IndexItem {
                    external_id: "same".into(),
                    field: "body".into(),
                    value: FieldValue::String("alpha beta alpha".into()),
                    version: Some(4),
                },
                IndexItem {
                    external_id: "same".into(),
                    field: "body".into(),
                    value: FieldValue::String("beta gamma".into()),
                    version: Some(5),
                },
            ],
        },
    };
    let bytes = WalRecord::new(entry.clone()).encode().unwrap();
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let engine = engine(&[("body", FieldType::Text)]);

    let mut owned_reservation = engine
        .wait_reserve_record_ram(
            &engine.record_ram_request_from_bound(engine.prepared_text_bound(&entry).unwrap(), 0),
        )
        .unwrap();
    let owned = engine
        .prepare_text_rows(&entry, &mut owned_reservation)
        .unwrap();
    let mut borrowed_reservation = record_reservation(&engine, &scanner);
    let Some((RaftLogEntry::Index { req, .. }, borrowed)) = engine
        .prepare_borrowed_text_rows(&scanner, &mut borrowed_reservation)
        .unwrap()
    else {
        panic!("Text-only command must use the borrowed preparation path")
    };

    assert_eq!(req.request_id.as_deref(), Some("request-7"));
    assert_eq!(req.items.len(), 2);
    assert_eq!(req.items[0].external_id, "same");
    assert_eq!(req.items[0].version, Some(4));
    assert_eq!(req.items[1].version, Some(5));
    assert!(matches!(&req.items[0].value, FieldValue::String(value) if value.is_empty()));
    assert_eq!(
        borrowed.retained_reader_bytes(),
        owned.retained_reader_bytes(),
    );
    assert_eq!(
        borrowed.scratch_growth_bytes(),
        owned.scratch_growth_bytes(),
    );
    for ordinal in 0..2 {
        let owned = owned.get(ordinal, "body").unwrap();
        let borrowed = borrowed.get(ordinal, "body").unwrap();
        assert_eq!(borrowed.doc_len(), owned.doc_len());
        assert_eq!(borrowed.indexed_bytes("same"), owned.indexed_bytes("same"));
        assert_eq!(
            borrowed.reader().text_dictionary_stats(),
            owned.reader().text_dictionary_stats(),
        );
    }
}

#[test]
fn borrowed_text_wrong_value_type_keeps_the_type_and_has_no_staged_row() {
    let bytes = encode(
        vec![IndexItem {
            external_id: "one".into(),
            field: "body".into(),
            value: FieldValue::Vector(vec![1.0, 2.0]),
            version: None,
        }],
        None,
    );
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let engine = engine(&[("body", FieldType::Text)]);
    let mut reservation = record_reservation(&engine, &scanner);
    let Some((RaftLogEntry::Index { req, .. }, rows)) = engine
        .prepare_borrowed_text_rows(&scanner, &mut reservation)
        .unwrap()
    else {
        panic!("a Text field with a wrong value type still needs live validation")
    };
    assert!(matches!(&req.items[0].value, FieldValue::Vector(values) if values.is_empty()));
    assert!(rows.get(0, "body").is_none());
}

#[test]
fn borrowed_text_leaves_unknown_fields_for_live_valid_prefix_handling() {
    let bytes = encode(
        vec![
            IndexItem {
                external_id: "one".into(),
                field: "body".into(),
                value: FieldValue::String("alpha".into()),
                version: None,
            },
            IndexItem {
                external_id: "one".into(),
                field: "later".into(),
                value: FieldValue::String("not validated here".into()),
                version: None,
            },
        ],
        None,
    );
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let engine = engine(&[("body", FieldType::Text)]);
    let mut reservation = record_reservation(&engine, &scanner);
    let Some((RaftLogEntry::Index { req, .. }, rows)) = engine
        .prepare_borrowed_text_rows(&scanner, &mut reservation)
        .unwrap()
    else {
        panic!("unknown field must remain for live validation")
    };
    assert_eq!(req.items[1].field, "later");
    assert!(matches!(&req.items[1].value, FieldValue::String(value) if value.is_empty()));
    assert!(rows.get(0, "body").is_some());
    assert!(rows.get(1, "later").is_none());
}

#[test]
fn borrowed_text_falls_back_when_an_existing_field_is_not_text() {
    let bytes = encode(
        vec![IndexItem {
            external_id: "one".into(),
            field: "tag".into(),
            value: FieldValue::String("value".into()),
            version: None,
        }],
        None,
    );
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let engine = engine(&[("tag", FieldType::Keyword)]);
    let mut reservation = record_reservation(&engine, &scanner);
    assert!(engine
        .prepare_borrowed_text_rows(&scanner, &mut reservation)
        .unwrap()
        .is_none());
}

#[test]
fn validated_replace_field_metadata_allows_32_by_33_flattened_fields() {
    const REPLACE_DOCUMENTS: usize = 32;
    const FIELDS_PER_DOCUMENT: usize = 33;
    let count = REPLACE_DOCUMENTS * FIELDS_PER_DOCUMENT;
    let bytes = encode(
        (0..count)
            .map(|ordinal| IndexItem {
                external_id: format!("doc-{}", ordinal / FIELDS_PER_DOCUMENT),
                field: format!("field-{}", ordinal % FIELDS_PER_DOCUMENT),
                value: FieldValue::String("small".into()),
                version: None,
            })
            .collect(),
        None,
    );
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    assert!(borrowed_text_metadata_bound(&scanner).is_err());
    assert!(borrowed_field_metadata_bound(&scanner).unwrap() > 0);
}

mod pre_stage;
