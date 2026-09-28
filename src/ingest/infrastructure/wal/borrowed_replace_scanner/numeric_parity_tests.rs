use ciborium::Value;

use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::borrowed_replace_scanner::{
    BorrowedReplaceScanner, BorrowedReplaceValue,
};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::FieldValue;

#[test]
fn integer_vector_components_keep_the_owned_decoders_single_rounding() {
    let text = |s: &str| Value::Text(s.into());
    let values = [
        (1u64 << 54) + (1 << 30) + 1,
        (1u64 << 54) + (1 << 30) - 1,
        u64::MAX,
        1u64 << 63,
    ];
    let wire = Value::Map(vec![
        (text("version"), Value::Integer(1.into())),
        (
            text("entry"),
            Value::Map(vec![(
                text("ReplaceDocs"),
                Value::Map(vec![
                    (text("collection_id"), text("docs")),
                    (
                        text("req"),
                        Value::Map(vec![(
                            text("docs"),
                            Value::Array(vec![Value::Map(vec![
                                (text("external_id"), text("id")),
                                (
                                    text("fields"),
                                    Value::Map(vec![(
                                        text("vector"),
                                        Value::Array(
                                            values
                                                .into_iter()
                                                .map(|v| Value::Integer(v.into()))
                                                .collect(),
                                        ),
                                    )]),
                                ),
                            ])]),
                        )]),
                    ),
                ]),
            )]),
        ),
    ]);
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(&wire, &mut bytes).unwrap();
    let RaftLogEntry::ReplaceDocs { req, .. } = WalRecord::decode(&bytes).unwrap().entry else {
        panic!("fixture is replacement")
    };
    let FieldValue::Vector(expected) = &req.docs[0].fields["vector"] else {
        panic!("owned decoder returns vector")
    };
    let scanned = BorrowedReplaceScanner::scan(&bytes, |_| Ok(()))
        .unwrap()
        .unwrap();
    let field = scanned.docs().next().unwrap().fields().next().unwrap();
    let BorrowedReplaceValue::Vector(actual) = field.value() else {
        panic!("borrowed vector")
    };
    assert_eq!(
        actual.map(|v| v.unwrap().to_bits()).collect::<Vec<_>>(),
        expected.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
    );
}
