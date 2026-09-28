use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::fast_index_scanner::{FastIndexScanner, FastIndexValue};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};

fn all_shapes(versioned: bool) -> Vec<u8> {
    WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: IndexRequest {
            request_id: Some("request".into()),
            items: vec![
                IndexItem {
                    external_id: "one".into(),
                    field: "kw".into(),
                    value: FieldValue::String("value".into()),
                    version: versioned.then_some(7),
                },
                IndexItem {
                    external_id: "two".into(),
                    field: "number".into(),
                    value: FieldValue::Number(2.5),
                    version: None,
                },
                IndexItem {
                    external_id: "three".into(),
                    field: "vector".into(),
                    value: FieldValue::Vector(vec![1.0, 2.0]),
                    version: None,
                },
                IndexItem {
                    external_id: "four".into(),
                    field: "set".into(),
                    value: FieldValue::StringList(vec!["a".into(), "b".into()]),
                    version: None,
                },
            ],
        },
    })
    .encode()
    .unwrap()
}

#[test]
fn scanner_matches_existing_fast_encoder_for_every_value_shape_and_both_tags() {
    for versioned in [false, true] {
        let bytes = all_shapes(versioned);
        let scanner = FastIndexScanner::parse(&bytes).unwrap();
        assert_eq!(scanner.collection_id(), "docs");
        assert_eq!(scanner.request_id(), Some("request"));
        let items: Vec<_> = scanner.items().collect();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0].external_id, "one");
        assert_eq!(items[0].version, versioned.then_some(7));
        assert!(matches!(items[0].value, FastIndexValue::String("value")));
        assert!(matches!(items[1].value, FastIndexValue::Number(value) if value == 2.5));
        assert!(matches!(
            items[2].value,
            FastIndexValue::Vector { len: 2, .. }
        ));
        let FastIndexValue::StringList(values) = items[3].value else {
            panic!("set")
        };
        assert_eq!(values.values().collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(
            scanner.items().count(),
            4,
            "iterator must rewind without a descriptor Vec"
        );
    }
}

#[test]
fn scanner_rejects_malformed_fast_index_framing_before_iteration() {
    let good = all_shapes(false);
    for mut bad in [good[..good.len() - 1].to_vec(), good.clone()] {
        if bad.len() == good.len() {
            bad[5] = 99;
        }
        assert!(FastIndexScanner::parse(&bad).is_err());
    }
    let mut bad_utf8 = all_shapes(false);
    // Collection payload starts after magic, version, tag, and u32 length.
    bad_utf8[10] = 0xff;
    assert!(FastIndexScanner::parse(&bad_utf8).is_err());
    let mut trailing = all_shapes(false);
    trailing.push(0);
    assert!(
        FastIndexScanner::parse(&trailing).is_err(),
        "scanner must reject bytes after the final item"
    );
}

#[test]
fn scanner_borrows_the_real_1000_by_270_kib_values() {
    const ITEMS: usize = 1_000;
    const VALUE_BYTES: usize = 270 * 1024;
    let value = "x".repeat(VALUE_BYTES);
    let bytes = WalRecord::new(RaftLogEntry::Index {
        collection_id: "large".into(),
        req: IndexRequest {
            request_id: None,
            items: (0..ITEMS)
                .map(|ordinal| IndexItem {
                    external_id: format!("id-{ordinal}"),
                    field: "kw".into(),
                    value: FieldValue::String(value.clone()),
                    version: None,
                })
                .collect(),
        },
    })
    .encode()
    .unwrap();
    assert!(bytes.len() > 256 * 1024 * 1024);
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    assert_eq!(scanner.cost().item_count, ITEMS);
    // The wire span includes the value tag and u32 string length.
    assert_eq!(scanner.cost().max_value_wire_bytes, VALUE_BYTES + 5);
    let first = scanner.items().next().unwrap();
    let FastIndexValue::String(first_value) = first.value else {
        panic!("keyword")
    };
    let base = bytes.as_ptr() as usize;
    let end = base + bytes.len();
    let value_ptr = first_value.as_ptr() as usize;
    assert!(
        base <= value_ptr && value_ptr + first_value.len() <= end,
        "scanner value must borrow command bytes"
    );
    assert_eq!(scanner.items().count(), ITEMS);
}
