use std::time::Instant;

use crate::index::application::apply::committed_index_plan::ScalarAction;
use crate::index::application::apply::committed_replace_plan::pass::plan;
use crate::index::application::apply::committed_replace_plan::tests::{item, parsed, view, wire};
use crate::index::application::apply::committed_replace_plan::{
    ReplaceItemError, ReplaceItemOutcome,
};
use crate::ingest::infrastructure::wal::borrowed_replace_spool::BorrowedReplaceSpoolDoc as ReplaceDocDescriptor;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::schema::FieldType;

#[test]
fn stale_is_dropped_before_bad_schema_and_id_is_still_interned() {
    let raw = wire(vec![item(
        "new",
        "missing",
        FieldValue::String("bad".into()),
    )]);
    let scan = FastIndexScanner::parse(&raw).unwrap();
    let mut v = view();
    v.versions.insert(10, 9);
    // `new` is allocated at watermark 10 before stale/schema work.
    let p = plan(
        &scan,
        &[ReplaceDocDescriptor {
            external_id: "new",
            version: Some(8),
            fields_start: 0,
            fields_end: 1,
        }],
        &parsed(),
        &v,
        Instant::now(),
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(p.scalar.new_external_ids, ["new"]);
    assert!(matches!(
        p.outcomes.as_slice(),
        [ReplaceItemOutcome::Dropped { current_version: 9 }]
    ));
}

#[test]
fn invalid_doc_keeps_old_coverage_and_later_sibling_runs() {
    let raw = wire(vec![
        item("old", "kw", FieldValue::Number(3.)),
        item("new", "kw", FieldValue::String("ok".into())),
    ]);
    let scan = FastIndexScanner::parse(&raw).unwrap();
    let p = plan(
        &scan,
        &[
            ReplaceDocDescriptor {
                external_id: "old",
                version: None,
                fields_start: 0,
                fields_end: 1,
            },
            ReplaceDocDescriptor {
                external_id: "new",
                version: None,
                fields_start: 1,
                fields_end: 2,
            },
        ],
        &parsed(),
        &view(),
        Instant::now(),
        |_| Ok(()),
    )
    .unwrap();
    assert!(matches!(&p.outcomes[0], ReplaceItemOutcome::Error { .. }));
    assert!(matches!(
        &p.outcomes[1],
        ReplaceItemOutcome::Ok {
            fields_written: 1,
            ..
        }
    ));
    assert!(p.final_docs.iter().all(|doc| doc.id != 2));
}

#[test]
fn duplicate_unversioned_docs_follow_source_order_and_omit_old_fields() {
    let raw = wire(vec![
        item("old", "kw", FieldValue::String("first".into())),
        item("old", "text", FieldValue::String("second".into())),
    ]);
    let scan = FastIndexScanner::parse(&raw).unwrap();
    let p = plan(
        &scan,
        &[
            ReplaceDocDescriptor {
                external_id: "old",
                version: None,
                fields_start: 0,
                fields_end: 1,
            },
            ReplaceDocDescriptor {
                external_id: "old",
                version: None,
                fields_start: 1,
                fields_end: 2,
            },
        ],
        &parsed(),
        &view(),
        Instant::now(),
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(p.final_docs[0].fields.as_slice(), ["text"]);
    assert!(p
        .scalar
        .actions
        .iter()
        .any(|a| matches!(a,ScalarAction::Drop{ordinal:usize::MAX,cell,..}if cell.field=="kw")));
}

#[test]
fn text_and_vector_need_a_prior_replace_checksum_before_equal_skip() {
    let raw = wire(vec![item("old", "text", FieldValue::String("same".into()))]);
    let scan = FastIndexScanner::parse(&raw).unwrap();
    let mut v = view(); // false: a merge write or first replace has no checksum
    let first = plan(
        &scan,
        &[ReplaceDocDescriptor {
            external_id: "old",
            version: None,
            fields_start: 0,
            fields_end: 1,
        }],
        &parsed(),
        &v,
        Instant::now(),
        |_| Ok(()),
    )
    .unwrap();
    assert!(matches!(
        &first.outcomes[0],
        ReplaceItemOutcome::Ok {
            fields_written: 1,
            ..
        }
    ));
    v.unchanged.insert((2, "text".into()));
    let second = plan(
        &scan,
        &[ReplaceDocDescriptor {
            external_id: "old",
            version: None,
            fields_start: 0,
            fields_end: 1,
        }],
        &parsed(),
        &v,
        Instant::now(),
        |_| Ok(()),
    )
    .unwrap();
    assert!(matches!(
        &second.outcomes[0],
        ReplaceItemOutcome::Ok {
            fields_skipped: 1,
            ..
        }
    ));
}

#[test]
fn replace_does_not_consult_index_cell_versions() {
    let raw = wire(vec![item("old", "kw", FieldValue::String("v".into()))]);
    let scan = FastIndexScanner::parse(&raw).unwrap();
    let p = plan(
        &scan,
        &[ReplaceDocDescriptor {
            external_id: "old",
            version: None,
            fields_start: 0,
            fields_end: 1,
        }],
        &parsed(),
        &view(),
        Instant::now(),
        |_| Ok(()),
    )
    .unwrap();
    assert!(matches!(&p.outcomes[0], ReplaceItemOutcome::Ok { .. }));
}

#[test]
fn versioned_then_unversioned_keeps_effective_version_for_later_stale_item() {
    let raw = wire(vec![
        item("old", "kw", FieldValue::String("a".into())),
        item("old", "text", FieldValue::String("b".into())),
        item("old", "kw", FieldValue::String("c".into())),
    ]);
    let scan = FastIndexScanner::parse(&raw).unwrap();
    let mut v = view();
    v.versions.insert(2, 10);
    let p = plan(
        &scan,
        &[
            ReplaceDocDescriptor {
                external_id: "old",
                version: Some(30),
                fields_start: 0,
                fields_end: 1,
            },
            ReplaceDocDescriptor {
                external_id: "old",
                version: None,
                fields_start: 1,
                fields_end: 2,
            },
            ReplaceDocDescriptor {
                external_id: "old",
                version: Some(20),
                fields_start: 2,
                fields_end: 3,
            },
        ],
        &parsed(),
        &v,
        Instant::now(),
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(p.final_docs[0].version, Some(30));
    assert!(matches!(
        &p.outcomes[2],
        ReplaceItemOutcome::Dropped {
            current_version: 30
        }
    ));
}

#[test]
fn invalid_hash_keeps_source_ordinal_for_exact_late_rendering() {
    let raw = wire(vec![item(
        "new",
        "hash",
        FieldValue::String("not-a-hash".into()),
    )]);
    let scan = FastIndexScanner::parse(&raw).unwrap();
    let mut v = view();
    v.fields.insert("hash".into(), FieldType::Hash);
    let p = plan(
        &scan,
        &[ReplaceDocDescriptor {
            external_id: "new",
            version: None,
            fields_start: 0,
            fields_end: 1,
        }],
        &parsed(),
        &v,
        Instant::now(),
        |_| Ok(()),
    )
    .unwrap();
    assert!(matches!(
        &p.outcomes[0],
        ReplaceItemOutcome::Error {
            error: ReplaceItemError::InvalidHash { ordinal: 0 }
        }
    ));
}
