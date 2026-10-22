//! Duplicate groups: the side index follows inserts and deletes, a keyword
//! field groups its values, and a text field is rejected.

use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::search::DuplicatesRequest;

#[test]
fn duplicates_side_index_tracks_inserts_and_deletes() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item("u1", "email", FieldValue::String("a@x.com".into())),
                item("u2", "email", FieldValue::String("a@x.com".into())),
                item("u3", "email", FieldValue::String("b@y.com".into())),
                item("u4", "email", FieldValue::String("b@y.com".into())),
                item("u5", "email", FieldValue::String("solo@z.com".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let groups = |min: u32| {
        e.duplicates(
            "users",
            DuplicatesRequest {
                field: "email".into(),
                min_group_size: min,
                limit: 10,
                offset: 0,
            },
        )
        .unwrap()
        .groups
    };
    let g = groups(2);
    assert_eq!(g.len(), 2);
    // Delete one of the `a@x.com` pair — the group must drop out, and the
    // side-index must not leak a stale candidate.
    e.delete("users", "u2", None).unwrap();
    let g = groups(2);
    assert_eq!(g.len(), 1);
    assert_eq!(g[0].value, serde_json::Value::String("b@y.com".into()));
    // Re-index the deleted doc back into the pair — group returns.
    e.index(
        "users",
        IndexRequest {
            items: vec![item("u2", "email", FieldValue::String("a@x.com".into()))],
            request_id: None,
        },
    )
    .unwrap();
    assert_eq!(groups(2).len(), 2);
}

#[test]
fn duplicates_keyword() {
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
                item("u6", "email", FieldValue::String("c@z.com".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let resp = e
        .duplicates(
            "users",
            DuplicatesRequest {
                field: "email".into(),
                min_group_size: 2,
                limit: 100,
                offset: 0,
            },
        )
        .unwrap();
    assert_eq!(resp.groups.len(), 2);
    // Largest first.
    assert_eq!(resp.groups[0].external_ids.len(), 3);
    assert_eq!(resp.groups[1].external_ids.len(), 2);
}

#[test]
fn duplicates_text_rejected() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let err = e
        .duplicates(
            "users",
            DuplicatesRequest {
                field: "bio".into(),
                min_group_size: 2,
                limit: 10,
                offset: 0,
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("duplicates not supported on text"));
}
