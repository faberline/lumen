use std::collections::BTreeMap;

use lumen::storage::Engine;
use lumen::types::{
    CreateCollectionRequest, FieldSpec, FieldType, FieldValue, IndexItem, IndexRequest, QueryNode,
    SearchRequest, TermQuery,
};

use crate::serve::bootstrap::apply_bootstrap_seed;

#[test]
fn bootstrap_seed_file_restores_snapshot_before_catchup() {
    let source = Engine::new();
    let mut fields = BTreeMap::new();
    fields.insert(
        "tag".to_string(),
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
    source
        .create_collection("c", CreateCollectionRequest { fields })
        .unwrap();
    source
        .index(
            "c",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: "doc-1".into(),
                    field: "tag".into(),
                    value: FieldValue::String("seeded".into()),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();

    let path = std::env::temp_dir().join(format!(
        "lumen-bootstrap-seed-{}-{}.json",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ));
    std::fs::write(
        &path,
        serde_json::to_vec(&source.snapshot().unwrap()).unwrap(),
    )
    .unwrap();

    let target = Engine::new();
    let uri = format!("file://{}", path.display());
    assert!(apply_bootstrap_seed(&target, Some(&uri)).unwrap());
    let result = target
        .search(
            "c",
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "tag".into(),
                    value: FieldValue::String("seeded".into()),
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
    assert_eq!(result.total, 1);
    assert_eq!(result.hits[0].external_id, "doc-1");

    let _ = std::fs::remove_file(path);
}
