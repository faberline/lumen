//! Exists and Duplicated read from sealed segments answer as the live tail
//! does.

use crate::index::application::engine::search::tests::exists_duplicated::search_ids;
use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::QueryNode;
use crate::shared_kernel::types::query::{DuplicatedQuery, ExistsQuery};

#[test]
fn exists_duplicated_segment_paths_equal_tail_paths() {
    // Guards the eval_field_doc_union asymmetry: segment OFF answers from the
    // in-RAM map (+ dup_values candidates for min>=2), segment ON from the
    // segment-aware live_* accessors. Seal must not change any answer, a
    // checkpoint reopen must agree, and a post-seal delete must obey the
    // group-size semantics on the sealed path (a 2-group losing a member
    // drops BOTH docs from `duplicated`).
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item("u1", "email", FieldValue::String("a@x.com".into())),
                item("u2", "email", FieldValue::String("a@x.com".into())),
                item("u3", "email", FieldValue::String("a@x.com".into())),
                item("u4", "email", FieldValue::String("b@y.com".into())),
                item("u5", "email", FieldValue::String("b@y.com".into())),
                item("u6", "email", FieldValue::String("solo@z.com".into())),
                item("u1", "age", FieldValue::Number(30.0)),
                item("u2", "age", FieldValue::Number(30.0)),
                item(
                    "u7",
                    "tags",
                    FieldValue::StringList(vec!["x".into(), "y".into()]),
                ),
                item("u8", "tags", FieldValue::StringList(vec!["x".into()])),
            ],
            request_id: None,
        },
    )
    .unwrap();

    let queries: Vec<(&str, QueryNode)> = vec![
        (
            "exists email",
            QueryNode::Exists(ExistsQuery {
                field: "email".into(),
            }),
        ),
        (
            "exists age",
            QueryNode::Exists(ExistsQuery {
                field: "age".into(),
            }),
        ),
        (
            "exists tags",
            QueryNode::Exists(ExistsQuery {
                field: "tags".into(),
            }),
        ),
        (
            "dup email >=2",
            QueryNode::Duplicated(DuplicatedQuery {
                field: "email".into(),
                min_group_size: 2,
            }),
        ),
        (
            "dup email >=3",
            QueryNode::Duplicated(DuplicatedQuery {
                field: "email".into(),
                min_group_size: 3,
            }),
        ),
        (
            "dup age >=2",
            QueryNode::Duplicated(DuplicatedQuery {
                field: "age".into(),
                min_group_size: 2,
            }),
        ),
        (
            "dup tags >=2",
            QueryNode::Duplicated(DuplicatedQuery {
                field: "tags".into(),
                min_group_size: 2,
            }),
        ),
    ];
    let tail: Vec<_> = queries
        .iter()
        .map(|(_, q)| search_ids(&e, "users", q.clone()))
        .collect();

    // Seal in place → the same queries now answer off the segment path.
    let dir = tempfile::tempdir().unwrap();
    e.flush_to_segments(dir.path(), 1).unwrap();
    for ((label, q), want) in queries.iter().zip(&tail) {
        let got = search_ids(&e, "users", q.clone());
        assert_eq!(&got, want, "sealed path diverged from tail path: {label}");
    }

    // Checkpoint reopen must agree too.
    let reopened = Engine::new();
    reopened.reopen_from_segment_dir(dir.path()).unwrap();
    for ((label, q), want) in queries.iter().zip(&tail) {
        let got = search_ids(&reopened, "users", q.clone());
        assert_eq!(&got, want, "reopened path diverged from tail path: {label}");
    }

    // Post-seal delete: u5 leaves → b@y.com group shrinks 2→1, so u4 must
    // ALSO leave `duplicated`; exists drops u5 only.
    e.delete("users", "u5", None).unwrap();
    let (_, ids) = search_ids(
        &e,
        "users",
        QueryNode::Duplicated(DuplicatedQuery {
            field: "email".into(),
            min_group_size: 2,
        }),
    );
    assert_eq!(
        ids,
        vec!["u1", "u2", "u3"],
        "2-group survivor must exit duplicated"
    );
    let (_, ids) = search_ids(
        &e,
        "users",
        QueryNode::Exists(ExistsQuery {
            field: "email".into(),
        }),
    );
    assert_eq!(
        ids,
        vec!["u1", "u2", "u3", "u4", "u6"],
        "exists must drop only the deleted doc"
    );
}
