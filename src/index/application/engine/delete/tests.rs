//! Deleting documents: a delete by external id removes every field, and a
//! truncate keeps the schema and its version without walking the documents.

use std::collections::BTreeMap;

use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::index::domain::field_index::delta::DROP_EID_CALLS;
use crate::index::infrastructure::collection_retirement::CollectionRetirementWorker;
use crate::shared_kernel::types::document::{
    FieldValue, IndexRequest, ReplaceDocItem, ReplaceDocsRequest,
};
use crate::shared_kernel::types::query::ExistsQuery;
use crate::shared_kernel::types::query::{QueryNode, TermQuery};
use crate::shared_kernel::types::search::SearchRequest;

#[test]
fn delete_external_id_removes_all_fields() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item("u1", "email", FieldValue::String("a@x.com".into())),
                item("u1", "bio", FieldValue::String("rust engineer".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();
    e.delete("users", "u1", None).unwrap();
    let r = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "email".into(),
                    value: FieldValue::String("a@x.com".into()),
                }),
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap();
    assert_eq!(r.total, 0);
}

/// #3992: a truncate is a document-state swap, not a disguised batch of
/// per-document deletes. This checks the precise state that has to reset
/// and uses an unavailable worker to prove the caller thread does not
/// reach the per-document removal primitive.
#[test]
fn truncate_docs_preserves_schema_version_and_never_walks_documents() {
    let e = Engine::new();
    let created = e.create_collection("users", build_users_schema()).unwrap();
    let schema = {
        let state = e.state.read().unwrap();
        state.collections["users"].schema.clone()
    };
    e.index(
        "users",
        IndexRequest {
            items: vec![
                crate::shared_kernel::types::document::IndexItem {
                    external_id: "u1".into(),
                    field: "email".into(),
                    value: FieldValue::String("u1@example.com".into()),
                    version: Some(7),
                },
                item("u1", "bio", FieldValue::String("rust engineer".into())),
                item(
                    "u1",
                    "tags",
                    FieldValue::StringList(vec!["systems".into(), "search".into()]),
                ),
                item("u1", "age", FieldValue::Number(42.0)),
            ],
            request_id: Some("before-truncate".into()),
        },
    )
    .unwrap();
    let mut replacement = BTreeMap::new();
    replacement.insert("bio".into(), FieldValue::String("new bio".into()));
    e.replace_docs(
        "users",
        ReplaceDocsRequest {
            docs: vec![ReplaceDocItem {
                external_id: "u1".into(),
                version: Some(9),
                fields: replacement,
            }],
        },
    )
    .unwrap();
    let _ = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Exists(ExistsQuery {
                    field: "email".into(),
                }),
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap();

    DROP_EID_CALLS.with(|calls| calls.set(0));
    e.truncate_docs_with_retirement("users", &CollectionRetirementWorker::Unavailable)
        .unwrap();
    DROP_EID_CALLS.with(|calls| {
        assert_eq!(
            calls.get(),
            0,
            "truncate must not call the per-document drop primitive on its caller thread"
        );
    });

    let state = e.state.read().unwrap();
    let coll = &state.collections["users"];
    assert_eq!(coll.version, created.version);
    assert_eq!(coll.schema, schema);
    assert!(coll.interner.to_eid.is_empty());
    assert!(coll.eid_fields.is_empty());
    assert!(coll.seen_requests.is_empty());
    assert!(coll.cell_versions.is_empty());
    assert!(coll.doc_versions.is_empty());
    assert!(coll.field_checksums.is_empty());
    assert!(coll.search_cache.read().unwrap().is_empty());
    assert!(coll.last_indexed_at.is_none());
    for index in coll.fields.values() {
        assert_eq!(
            index.bytes(),
            0,
            "fresh schema field must hold no documents"
        );
    }
    drop(state);

    e.index(
        "users",
        IndexRequest {
            items: vec![item(
                "u2",
                "email",
                FieldValue::String("u2@example.com".into()),
            )],
            request_id: None,
        },
    )
    .unwrap();
    assert_eq!(
        e.stats("users").unwrap().documents_indexed,
        1,
        "the retained schema must accept a new document immediately"
    );
}

mod batch_unindex_docs;
