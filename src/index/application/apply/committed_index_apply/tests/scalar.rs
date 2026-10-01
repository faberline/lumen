use crate::index::application::apply::committed_index_apply::tests::{
    borrowed, compare_apply, engine, item, request,
};
use crate::index::application::apply::committed_index_apply::{composed, BEFORE_ATTACH};
use crate::index::domain::field_index::FieldIndex;
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::FieldValue;

#[test]
fn borrowed_apply_matches_owned_versions_duplicates_and_error_prefixes() {
    let actual = engine();
    let reference = engine();
    let requests = [
        request(
            vec![
                item("old", "kw", FieldValue::String("base".into()), Some(7)),
                item("old", "n", FieldValue::Number(3.5), None),
                item(
                    "old",
                    "tags",
                    FieldValue::StringList(vec!["a".into(), "b".into()]),
                    None,
                ),
            ],
            None,
        ),
        request(
            vec![
                item("old", "kw", FieldValue::String("arrival".into()), None),
                item("old", "kw", FieldValue::String("stale".into()), Some(7)),
                item(
                    "new",
                    "tags",
                    FieldValue::StringList(vec!["z".into(), "a".into(), "z".into()]),
                    None,
                ),
                item("new", "kw", FieldValue::String("first".into()), Some(1)),
                item("new", "kw", FieldValue::String("last".into()), Some(2)),
                item("new", "n", FieldValue::Number(-0.0), None),
            ],
            Some("once"),
        ),
        request(
            vec![
                item("old", "n", FieldValue::Number(9.0), None),
                item(
                    "unknown-id",
                    "missing",
                    FieldValue::String("bad".into()),
                    None,
                ),
            ],
            None,
        ),
        request(
            vec![
                item(
                    "old",
                    "kw",
                    FieldValue::String("valid-prefix".into()),
                    Some(8),
                ),
                item("old", "kw", FieldValue::Number(99.0), Some(9)),
            ],
            None,
        ),
        request(
            vec![item(
                "new",
                "kw",
                FieldValue::String("must-not-apply".into()),
                Some(10),
            )],
            Some("once"),
        ),
        request(Vec::new(), Some("empty")),
    ];
    for (sequence, req) in requests.iter().enumerate() {
        compare_apply(&actual, &reference, req, sequence as u64 + 1);
    }
    let state = actual.state.read().unwrap();
    let FieldIndex::Set(tags) = &state.collections["docs"].fields["tags"] else {
        unreachable!()
    };
    assert!(
        tags.forward.is_empty(),
        "staged values must not remain in owned overlay"
    );
    assert!(
        tags.segment.is_some(),
        "queries must use attached immutable layers"
    );
    assert!(
        actual.changes.budget.snapshot().total > 0,
        "journal retains pending metadata charge"
    );
}

#[test]
fn concurrent_apply_invalidates_detached_scalar_plan_before_install() {
    let actual = engine();
    let initial = request(
        vec![item("id", "kw", FieldValue::String("base".into()), Some(1))],
        None,
    );
    borrowed(&actual, &initial, 1).unwrap();
    let newer = actual.clone();
    BEFORE_ATTACH.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            newer
                .index_inner(
                    "docs",
                    request(
                        vec![item(
                            "id",
                            "kw",
                            FieldValue::String("newer".into()),
                            Some(3),
                        )],
                        None,
                    ),
                    None,
                    None,
                )
                .unwrap();
        }))
    });
    let response = borrowed(
        &actual,
        &request(
            vec![item(
                "id",
                "kw",
                FieldValue::String("staged-old".into()),
                Some(2),
            )],
            None,
        ),
        2,
    )
    .unwrap();
    assert_eq!(
        response.indexed, 0,
        "a stale detached plan must be re-evaluated against the newer version"
    );
    let state = actual.state.read().unwrap();
    let coll = &state.collections["docs"];
    let FieldIndex::Keyword(k) = &coll.fields["kw"] else {
        unreachable!()
    };
    assert_eq!(
        k.keyword_at(coll.interner.id("id").unwrap()).as_deref(),
        Some("newer")
    );
}

#[test]
fn vector_type_error_keeps_the_valid_scalar_prefix_on_borrowed_route() {
    let actual = engine();
    let reference = engine();
    for engine in [&actual, &reference] {
        engine.add_field_inner("docs", "vector", serde_json::from_value(
            serde_json::json!({"type":"vector", "dim":2, "metric":"l2", "backend":"flat-cpu"})
        ).unwrap()).unwrap();
    }
    let req = request(
        vec![
            item("id", "kw", FieldValue::String("prefix".into()), None),
            item("id", "vector", FieldValue::Number(7.0), None),
        ],
        None,
    );
    compare_apply(&actual, &reference, &req, 1);
    let state = actual.state.read().unwrap();
    let coll = &state.collections["docs"];
    let FieldIndex::Keyword(keyword) = &coll.fields["kw"] else {
        unreachable!()
    };
    assert_eq!(
        keyword
            .keyword_at(coll.interner.id("id").unwrap())
            .as_deref(),
        Some("prefix")
    );
}

#[test]
fn frozen_checkpoint_slot_forces_real_maintenance_before_sixteenth_private_append() {
    let engine = engine();
    let mut window = Some(engine.layer_maintenance.freeze());
    for sequence in 1..=15 {
        borrowed(
            &engine,
            &request(
                vec![item(
                    "same",
                    "kw",
                    FieldValue::String(format!("value-{sequence}")),
                    None,
                )],
                None,
            ),
            sequence,
        )
        .unwrap();
    }
    let pinned = {
        let state = engine.state.read().unwrap();
        let view = composed(&state.collections["docs"].fields["kw"]).unwrap();
        assert_eq!(view.incremental_layer_count(), 15);
        view.clone()
    };
    let raw = WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: request(
            vec![item(
                "same",
                "kw",
                FieldValue::String("value-16".into()),
                None,
            )],
            None,
        ),
    })
    .encode()
    .unwrap();
    let scanner = FastIndexScanner::parse(&raw).unwrap();
    let mut fallback = None;
    let mut requested = false;
    let mut completions = 0;
    assert!(engine
        .try_apply_committed_index_with_capacity_owner(
            &scanner,
            16,
            || {
                requested = true;
                assert!(
                    engine.state.try_write().is_ok(),
                    "capacity wait must release state ownership"
                );
                assert!(
                    engine.capture_barrier.capture(0).is_ok(),
                    "capacity bootstrap must be outside apply"
                );
                assert!(
                    engine.changes.budget.snapshot().reserved > 0,
                    "committed input keeps its reservation while asking for maintenance"
                );
                // Complete the simulated older cut. The actual capacity worker still
                // needs to checkpoint the 15 private readers before the append resumes.
                drop(window.take());
                crate::persistence::application::capacity::Fallback::ensure(
                    &mut fallback,
                    &engine,
                    None,
                )
            },
            |apply, outcome| {
                outcome.unwrap();
                completions += 1;
                apply.advance_sequence(16);
            }
        )
        .unwrap());
    assert!(
        requested,
        "frozen publication owns the last slot, so the sixteenth append must ask for maintenance"
    );
    assert_eq!(completions, 1);
    assert!(engine.metrics().segment_checkpoint_completed_total.get() > 0);
    assert_eq!(pinned.keyword_at(0).as_deref(), Some("value-15"));
    let state = engine.state.read().unwrap();
    let view = composed(&state.collections["docs"].fields["kw"]).unwrap();
    assert!(view.incremental_layer_count() <= 16);
    assert_eq!(view.keyword_at(0).as_deref(), Some("value-16"));
}
