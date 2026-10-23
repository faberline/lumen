use crate::index::application::apply::committed_index_apply::tests::{
    borrowed, compare_apply, engine, item, request, state_fingerprint,
};
use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::document::FieldValue;

#[cfg(test)]
use crate::index::application::apply::committed_index_apply::{
    reset_text_workspace_retries_for_test, text_workspace_retries_for_test,
};

fn text_search(engine: &Engine, text: &str) -> serde_json::Value {
    let request = serde_json::from_value(serde_json::json!({
        "query": {"match": {"field":"body", "text": text}},
        "limit": 10
    }))
    .unwrap();
    let mut value = serde_json::to_value(engine.search("docs", request).unwrap()).unwrap();
    value.as_object_mut().unwrap().remove("took_ms");
    value.as_object_mut().unwrap().remove("took_us");
    value
}

fn text_stats(engine: &Engine) -> (u64, u64, u64) {
    let state = engine.state.read().unwrap();
    let FieldIndex::Text { idx, .. } = &state.collections["docs"].fields["body"] else {
        unreachable!()
    };
    (idx.doc_count, idx.total_doc_len, idx.bytes)
}

#[test]
fn borrowed_mixed_text_and_scalars_match_owned_bytes_stats_duplicates_and_workspace_retry() {
    let actual = engine();
    let reference = engine();
    // The single Unicode token needs more than the initial metadata spare
    // space. The real committed wrapper must grow outside apply, then retry.
    let unicode = "İ".repeat(1_500);
    let req = request(
        vec![
            item("same", "body", FieldValue::String(unicode), Some(1)),
            item("same", "kw", FieldValue::String("first".into()), Some(1)),
            item("same", "n", FieldValue::Number(3.5), Some(1)),
            item(
                "same",
                "tags",
                FieldValue::StringList(vec!["a".into(), "b".into()]),
                Some(1),
            ),
            item(
                "same",
                "body",
                FieldValue::String("final body term".into()),
                Some(2),
            ),
            item("same", "kw", FieldValue::String("final".into()), Some(2)),
        ],
        Some("mixed-once"),
    );
    reset_text_workspace_retries_for_test();
    let actual_response = borrowed(&actual, &req, 17).unwrap();
    assert!(
        text_workspace_retries_for_test() >= 2,
        "the Unicode token must require a second retry beyond the fixed Text scratch"
    );
    let expected_response = reference.index_inner("docs", req, None, None).unwrap();
    assert_eq!(
        serde_json::to_value(actual_response).unwrap(),
        serde_json::to_value(expected_response).unwrap()
    );
    assert_eq!(
        text_search(&actual, "final body"),
        text_search(&reference, "final body")
    );
    assert_eq!(text_stats(&actual), text_stats(&reference));
    assert_eq!(state_fingerprint(&actual), state_fingerprint(&reference));
}

#[test]
fn borrowed_mixed_text_wrong_type_and_unknown_field_keep_owned_valid_prefix() {
    let actual = engine();
    let reference = engine();
    let wrong_type = request(
        vec![
            item(
                "id",
                "body",
                FieldValue::String("kept text".into()),
                Some(1),
            ),
            item(
                "id",
                "kw",
                FieldValue::String("kept keyword".into()),
                Some(1),
            ),
            item("id", "body", FieldValue::Number(9.0), Some(2)),
        ],
        None,
    );
    compare_apply(&actual, &reference, &wrong_type, 18);
    assert_eq!(
        text_search(&actual, "kept text"),
        text_search(&reference, "kept text")
    );
    assert_eq!(text_stats(&actual), text_stats(&reference));
    // Seed a live staged Text row, then make a wrong-type-only replacement.
    // This exercises the Text Drop ledger branch with no successful Text Apply.
    let seed = request(
        vec![item(
            "drop",
            "body",
            FieldValue::String("seeded text".into()),
            Some(1),
        )],
        None,
    );
    compare_apply(&actual, &reference, &seed, 19);
    // Warm the rank cache before the Drop-only invalidation.
    assert_eq!(
        text_search(&actual, "seeded text"),
        text_search(&reference, "seeded text")
    );
    let drop_only = request(
        vec![item("drop", "body", FieldValue::Number(1.0), Some(2))],
        None,
    );
    compare_apply(&actual, &reference, &drop_only, 20);
    assert_eq!(
        text_search(&actual, "seeded text"),
        text_search(&reference, "seeded text")
    );
    assert_eq!(text_stats(&actual), text_stats(&reference));
    let unknown = request(
        vec![
            item("id", "body", FieldValue::String("new text".into()), Some(3)),
            item("id", "missing", FieldValue::String("bad".into()), None),
        ],
        None,
    );
    compare_apply(&actual, &reference, &unknown, 21);
    assert_eq!(
        text_search(&actual, "new text"),
        text_search(&reference, "new text")
    );
    assert_eq!(text_stats(&actual), text_stats(&reference));
}
