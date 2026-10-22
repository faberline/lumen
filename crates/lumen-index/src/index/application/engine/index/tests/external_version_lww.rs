//! #184: external-version last-write-wins. An IndexItem may carry an optional
//! `version`; lumen keeps the highest version per (external_id, field) and drops
//! strictly-older writes. Absent version = arrival order (today's behavior).

use std::collections::BTreeMap;

use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::IndexItem;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{QueryNode, TermQuery};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::shared_kernel::types::search::SearchRequest;

fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert(
        "price".into(),
        FieldSpec {
            field_type: FieldType::Number,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    CreateCollectionRequest { fields }
}

fn setup() -> Engine {
    let e = Engine::new();
    e.create_collection("c", schema()).unwrap();
    e
}

fn write(e: &Engine, eid: &str, price: f64, version: Option<u64>) {
    e.index(
        "c",
        IndexRequest {
            items: vec![IndexItem {
                external_id: eid.into(),
                field: "price".into(),
                value: FieldValue::Number(price),
                version,
            }],
            request_id: None,
        },
    )
    .unwrap();
}

/// external_ids whose `price` equals `price`.
fn matches_price(e: &Engine, price: f64) -> Vec<String> {
    let req = SearchRequest {
        query: QueryNode::Term(TermQuery {
            field: "price".into(),
            value: FieldValue::Number(price),
        }),
        limit: 100,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    };
    e.search("c", req)
        .unwrap()
        .hits
        .into_iter()
        .map(|h| h.external_id)
        .collect()
}

/// R1: a versioned write older than the stored version is dropped.
#[test]
fn stale_versioned_write_is_dropped() {
    let e = setup();
    write(&e, "d0", 10.0, Some(5));
    write(&e, "d0", 20.0, Some(3)); // stale: 3 < stored 5
    assert_eq!(
        matches_price(&e, 10.0),
        vec!["d0".to_string()],
        "value must remain at the v5 write"
    );
    assert!(
        matches_price(&e, 20.0).is_empty(),
        "the stale v3 write must not apply"
    );
}

/// R2: a newer versioned write advances the cell.
#[test]
fn newer_versioned_write_wins() {
    let e = setup();
    write(&e, "d0", 10.0, Some(5));
    write(&e, "d0", 20.0, Some(6)); // newer: 6 > stored 5
    assert_eq!(matches_price(&e, 20.0), vec!["d0".to_string()]);
    assert!(matches_price(&e, 10.0).is_empty());
}

/// R3: writes without a version apply in arrival order (last wins) —
/// unchanged from today.
#[test]
fn unversioned_writes_keep_arrival_order() {
    let e = setup();
    write(&e, "d0", 10.0, None);
    write(&e, "d0", 20.0, None);
    assert_eq!(matches_price(&e, 20.0), vec!["d0".to_string()]);
    assert!(matches_price(&e, 10.0).is_empty());
}
