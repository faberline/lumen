use crate::index::application::apply::committed_index_plan::tests::{
    encode, item, planned, scalar_view,
};
use crate::index::application::apply::committed_index_plan::ScalarAction;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::schema::FieldType;

#[test]
fn text_is_planned_in_the_unified_ledger_and_keeps_text_drop_kind() {
    let mut view = scalar_view();
    view.fields.insert("body".into(), FieldType::Text);
    view.ids.insert("id".into(), 2);
    view.cells.insert((2, "body".into()));
    let plan = planned(
        &encode(
            vec![
                item(
                    "id",
                    "body",
                    FieldValue::String("valid prefix".into()),
                    Some(1),
                ),
                item("id", "body", FieldValue::Number(3.0), Some(2)),
            ],
            None,
        ),
        &view,
    );
    assert!(plan.has_text());
    assert_eq!(
        plan.text_actions()
            .map(|(_, ordinal)| ordinal)
            .collect::<Vec<_>>(),
        vec![0]
    );
    assert!(matches!(
        plan.actions.last(),
        Some(ScalarAction::Drop {
            kind: FieldType::Text,
            ordinal: 1,
            ..
        })
    ));
    assert!(plan.business_error.is_some());
}

#[test]
fn vector_is_planned_without_decoding_its_borrowed_values() {
    let mut view = scalar_view();
    view.fields.insert("vector".into(), FieldType::Vector);
    view.vector_dimensions.insert("vector".into(), 3);
    let plan = planned(
        &encode(
            vec![item(
                "id",
                "vector",
                FieldValue::Vector(vec![1.0, -2.0, 3.0]),
                None,
            )],
            None,
        ),
        &view,
    );
    assert_eq!(plan.applied, 1);
    assert_eq!(plan.winners.values().copied().collect::<Vec<_>>(), vec![0]);
    assert!(matches!(
        plan.actions.as_slice(),
        [ScalarAction::Apply {
            kind: FieldType::Vector,
            ordinal: 0,
            bytes,
            ..
        }] if *bytes == 3 * std::mem::size_of::<f32>() as u64 + 2
    ));
}

#[test]
fn vector_dimension_error_keeps_valid_prefix_then_drops_the_bad_cell() {
    let mut view = scalar_view();
    view.fields.insert("vector".into(), FieldType::Vector);
    view.vector_dimensions.insert("vector".into(), 3);
    view.ids.insert("old".into(), 2);
    view.cells.insert((2, "vector".into()));
    let plan = planned(
        &encode(
            vec![
                item(
                    "old",
                    "vector",
                    FieldValue::Vector(vec![1.0, 2.0, 3.0]),
                    Some(1),
                ),
                item("old", "vector", FieldValue::Vector(vec![4.0]), Some(2)),
            ],
            None,
        ),
        &view,
    );
    assert_eq!(plan.applied, 1);
    assert!(matches!(
        plan.actions.last(),
        Some(ScalarAction::Drop {
            kind: FieldType::Vector,
            ordinal: 1,
            ..
        })
    ));
    let error = plan
        .business_error
        .expect("wrong vector dimension is a business error")
        .into_error(
            "docs",
            &FastIndexScanner::parse(&encode(
                vec![item(
                    "old",
                    "vector",
                    FieldValue::Vector(vec![4.0]),
                    Some(2),
                )],
                None,
            ))
            .unwrap(),
        );
    assert_eq!(
        error.to_string(),
        "vector field `vector` declared dim=3 but got vector of length 1"
    );
}

#[test]
fn stale_vector_dimension_is_skipped_before_validation() {
    let mut view = scalar_view();
    view.fields.insert("vector".into(), FieldType::Vector);
    view.vector_dimensions.insert("vector".into(), 3);
    view.ids.insert("old".into(), 2);
    view.versions.insert((2, "vector".into()), 5);
    let plan = planned(
        &encode(
            vec![item(
                "old",
                "vector",
                FieldValue::Vector(vec![1.0]),
                Some(5),
            )],
            None,
        ),
        &view,
    );
    assert_eq!(plan.applied, 0);
    assert!(plan.actions.is_empty());
    assert!(plan.business_error.is_none());
}
