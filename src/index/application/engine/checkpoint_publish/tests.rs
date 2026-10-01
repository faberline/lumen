//! A checkpoint's scalar cuts: the capture gives each unsealed scalar field an
//! empty cut, and a publication prepared from the cuts keeps a write made after
//! the preparation while it retires the unchanged captured overlays.

use std::collections::BTreeMap;

use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::sortable_f64::MISSING_SORTABLE_F64_BITS;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};

#[test]
fn checkpoint_capture_marks_unsealed_scalar_fields_with_empty_cuts() {
    let mut fields = BTreeMap::new();
    for (name, field_type) in [
        ("keyword", FieldType::Keyword),
        ("number", FieldType::Number),
        ("set", FieldType::Set),
    ] {
        fields.insert(
            name.into(),
            FieldSpec {
                field_type,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
    }
    let engine = Engine::new();
    engine
        .create_collection("c", CreateCollectionRequest { fields })
        .unwrap();
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    let cuts = frozen.capture.scalar_cuts.get("c").unwrap();
    assert_eq!(cuts.len(), 3);
    for field in ["keyword", "number", "set"] {
        assert!(cuts.contains_key(field), "{field} must get an empty cut");
    }
}

#[test]
fn scalar_checkpoint_retirement_keeps_a_mutation_after_preparation() {
    let engine = Engine::with_change_budget(
        crate::ingest::domain::change_budget::ChangeBudget::with_hard_limit(32 * 1024 * 1024),
    );
    engine
        .create_collection_inner(
            "c",
            CreateCollectionRequest {
                fields: serde_json::from_value(serde_json::json!({
                    "keyword":{"type":"keyword"}, "number":{"type":"number"}
                }))
                .unwrap(),
            },
        )
        .unwrap();
    let write = |value: &str, include_number: bool| {
        let mut items = vec![crate::shared_kernel::types::document::IndexItem {
            external_id: "e".into(),
            field: "keyword".into(),
            value: FieldValue::String(value.into()),
            version: None,
        }];
        if include_number {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: "e".into(),
                field: "number".into(),
                value: FieldValue::Number(3.0),
                version: None,
            });
        }
        engine
            .index_inner(
                "c",
                IndexRequest {
                    items,
                    request_id: None,
                },
                None,
                None,
            )
            .unwrap();
    };
    write("captured", true);
    // Select the full-base branch so both ordinary overlay retirement and
    // a newer ordinary overlay cross the real Engine publication seam.
    engine
        .state
        .write()
        .unwrap()
        .collections
        .get_mut("c")
        .unwrap()
        .requires_full_checkpoint = true;
    let root = tempfile::tempdir().unwrap();
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    let mut capture = frozen.write(root.path(), 0).unwrap();
    engine
        .prepare_scalar_checkpoint_publications(&mut capture)
        .unwrap();
    write("later", false);
    engine
        .bind_checkpoint_origins(root.path(), &mut capture)
        .unwrap();
    let state = engine.state.read().unwrap();
    let coll = &state.collections["c"];
    let id = coll.interner.id("e").unwrap();
    let FieldIndex::Keyword(keyword) = &coll.fields["keyword"] else {
        unreachable!()
    };
    assert_eq!(
        keyword.keyword_at(id).as_deref(),
        Some("later"),
        "publication must preserve the ordinary write made after preparation"
    );
    assert!(
        keyword
            .dense_forward
            .get(id as usize)
            .and_then(Option::as_ref)
            .is_some()
            || keyword.forward.contains_key(&id)
    );
    let FieldIndex::Number(number) = &coll.fields["number"] else {
        unreachable!()
    };
    assert_eq!(number.number_at(id).unwrap().to_f64(), 3.0);
    assert!(
        number.forward.is_empty(),
        "unchanged captured overlays must be retired"
    );
    assert!(number
        .dense_forward
        .get(id as usize)
        .is_none_or(|value| *value == MISSING_SORTABLE_F64_BITS));
    assert!(number.segment.is_some());
}

mod retained_charge;
