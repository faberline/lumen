use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::fast_index_scanner::{FastIndexScanner, FastIndexValue};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};

#[cfg(test)]
use crate::ingest::infrastructure::wal::fast_index_scanner::{
    reset_utf8_validation_bytes_for_test, utf8_validation_bytes_for_test,
};

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

fn corrupt_first(bytes: &mut [u8], needle: &[u8]) {
    let mut encoded = (needle.len() as u32).to_le_bytes().to_vec();
    encoded.extend_from_slice(needle);
    let offset = bytes
        .windows(encoded.len())
        .position(|window| window == encoded)
        .expect("fixture string must occur once");
    bytes[offset + 4] = 0xff;
}

#[test]
fn parse_refuses_invalid_utf8_in_every_fast_string_position() {
    for needle in [
        b"docs".as_slice(),
        b"request".as_slice(),
        b"one".as_slice(),
        b"kw".as_slice(),
        b"value".as_slice(),
        b"a".as_slice(),
        b"b".as_slice(),
    ] {
        let mut bytes = all_shapes(true);
        corrupt_first(&mut bytes, needle);
        assert!(
            FastIndexScanner::parse(&bytes).is_err(),
            "parse must reject malformed UTF-8 in {needle:?}"
        );
    }
}

#[test]
fn validated_iteration_rewinds_lists_for_both_fast_tags() {
    for versioned in [false, true] {
        let bytes = all_shapes(versioned);
        let scanner = FastIndexScanner::parse(&bytes).unwrap();
        let first: Vec<_> = scanner
            .items()
            .map(|item| match item.value {
                FastIndexValue::StringList(values) => values.values().collect::<Vec<_>>(),
                _ => Vec::new(),
            })
            .collect();
        let second: Vec<_> = scanner
            .items()
            .map(|item| match item.value {
                FastIndexValue::StringList(values) => values.values().collect::<Vec<_>>(),
                _ => Vec::new(),
            })
            .collect();
        assert_eq!(first, second);
        assert_eq!(first[3], ["a", "b"]);
    }
}

#[test]
fn iteration_does_not_repeat_parse_utf8_validation() {
    reset_utf8_validation_bytes_for_test();
    let bytes = all_shapes(true);
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let validated_at_parse = utf8_validation_bytes_for_test();
    const FIXTURE_UTF8_BYTES: usize = "docs".len()
        + "request".len()
        + "one".len()
        + "kw".len()
        + "value".len()
        + "two".len()
        + "number".len()
        + "three".len()
        + "vector".len()
        + "four".len()
        + "set".len()
        + "a".len()
        + "b".len();
    assert_eq!(
        validated_at_parse, FIXTURE_UTF8_BYTES,
        "parse must validate every fixture string exactly once",
    );
    for _ in 0..3 {
        assert_eq!(scanner.collection_id(), "docs");
        assert_eq!(scanner.request_id(), Some("request"));
        for item in scanner.items() {
            if let FastIndexValue::StringList(values) = item.value {
                assert_eq!(values.values().collect::<Vec<_>>(), ["a", "b"]);
            }
        }
    }
    assert_eq!(
        utf8_validation_bytes_for_test(),
        validated_at_parse,
        "later scanner passes must use only parse-validated ranges"
    );
}
