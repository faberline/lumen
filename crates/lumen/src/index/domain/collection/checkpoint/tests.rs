//! A collection's dirty field rows: kept per field at the latest mark, a field
//! the capture format does not handle forces a full checkpoint, and an
//! acknowledgement leaves the rows marked after its snapshot.

use std::collections::BTreeMap;

use crate::index::domain::collection::Collection;
use crate::shared_kernel::types::schema::{FieldSpec, FieldType};

#[test]
fn field_dirty_rows_are_per_field_latest_and_revision_safe() {
    let mut schema = BTreeMap::new();
    for name in ["left", "right"] {
        schema.insert(
            name.to_owned(),
            FieldSpec {
                field_type: FieldType::Keyword,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
    }
    schema.insert(
        "number".to_owned(),
        FieldSpec {
            field_type: FieldType::Number,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    let mut collection = Collection::new(schema).unwrap();
    collection.mark_field_dirty("left", "same").unwrap();
    let captured = collection.field_dirty_snapshot();
    collection.mark_field_dirty("left", "same").unwrap();
    collection.mark_field_dirty("right", "same").unwrap();
    collection.mark_field_dirty("left", "other").unwrap();

    assert_eq!(collection.field_dirty_len("left"), 2);
    assert_eq!(collection.field_dirty_len("right"), 1);
    collection.mark_field_dirty("number", "same").unwrap();
    assert_eq!(collection.field_dirty_len("number"), 1);
    assert!(!collection.requires_full_checkpoint());
    // A field not handled by this capture format must take a full checkpoint.
    collection
        .mark_field_dirty("unknown-field", "same")
        .unwrap();
    assert!(collection.requires_full_checkpoint());
    collection.acknowledge_field_dirty(&captured);
    assert_eq!(collection.field_dirty_len("left"), 2);
    assert_eq!(collection.field_dirty_len("right"), 1);
}
