//! Batch unindex: it removes a known document, clears the LWW and replace side
//! state before a rewrite, and revalidates the request before it takes any
//! mutation path.

use std::collections::BTreeMap;

use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{BatchUnindexDocsRequest, IndexItem};
use crate::shared_kernel::types::document::{
    FieldValue, IndexRequest, ReplaceDocItem, ReplaceDocsRequest,
};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};

#[test]
fn batch_unindex_removes_a_known_document() {
    let engine = Engine::new();
    let mut fields = BTreeMap::new();
    fields.insert(
        "email".to_string(),
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
    engine
        .create_collection("docs", CreateCollectionRequest { fields })
        .unwrap();
    engine
        .index(
            "docs",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: "old".to_string(),
                    field: "email".to_string(),
                    value: FieldValue::String("old@example.com".to_string()),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();

    engine
        .unindex_docs(
            "docs",
            BatchUnindexDocsRequest {
                external_ids: vec!["old".to_string()],
            },
        )
        .unwrap();
    assert_eq!(engine.stats("docs").unwrap().documents_indexed, 0);
}

#[test]
fn batch_unindex_clears_lww_and_replace_side_state_before_rewrite() {
    let engine = Engine::new();
    let mut fields = BTreeMap::new();
    fields.insert(
        "email".to_string(),
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
    engine
        .create_collection("docs", CreateCollectionRequest { fields })
        .unwrap();
    engine
        .index(
            "docs",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: "old".to_string(),
                    field: "email".to_string(),
                    value: FieldValue::String("old@example.com".to_string()),
                    version: Some(4),
                }],
                request_id: None,
            },
        )
        .unwrap();
    engine
        .replace_docs(
            "docs",
            ReplaceDocsRequest {
                docs: vec![ReplaceDocItem {
                    external_id: "old".to_string(),
                    version: Some(9),
                    fields: BTreeMap::from([(
                        "email".to_string(),
                        FieldValue::String("replace@example.com".to_string()),
                    )]),
                }],
            },
        )
        .unwrap();

    engine
        .unindex_docs(
            "docs",
            BatchUnindexDocsRequest {
                external_ids: vec!["old".to_string()],
            },
        )
        .unwrap();
    let state = engine.state.read().unwrap();
    let coll = state.collections.get("docs").unwrap();
    let id = coll.interner.id("old").expect("append-only interner entry");
    assert!(!coll.eid_fields.contains_key(&id));
    assert!(!coll.cell_versions.contains_key(&id));
    assert!(!coll.doc_versions.contains_key(&id));
    assert!(!coll.field_checksums.contains_key(&id));
    drop(state);

    // No unindex tombstone is retained.  An older external version can
    // become the first version of the rewritten row.
    let rewritten = engine
        .index(
            "docs",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: "old".to_string(),
                    field: "email".to_string(),
                    value: FieldValue::String("rewritten@example.com".to_string()),
                    version: Some(1),
                }],
                request_id: None,
            },
        )
        .unwrap();
    assert_eq!(rewritten.indexed, 1);
}

#[test]
fn batch_unindex_revalidates_before_taking_any_mutation_path() {
    let engine = Engine::new();
    engine
        .create_collection(
            "docs",
            CreateCollectionRequest {
                fields: BTreeMap::new(),
            },
        )
        .unwrap();
    let err = engine
        .unindex_docs(
            "docs",
            BatchUnindexDocsRequest {
                external_ids: Vec::new(),
            },
        )
        .unwrap_err();
    assert!(err.to_string().contains("at least one"), "got: {err}");
    assert_eq!(engine.stats("docs").unwrap().documents_indexed, 0);
}
