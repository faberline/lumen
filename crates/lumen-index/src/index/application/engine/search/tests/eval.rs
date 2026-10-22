//! Query evaluation's exact scores: AND and OR sum their children's scores, NOT
//! excludes exactly the matched set, and validation rejects pathological trees
//! but allows normal ones.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;

use crate::index::application::engine::search::tests::score_of;
use crate::index::application::engine::tests::item;
use crate::index::application::engine::Engine;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{QueryNode, TermQuery, TermsQuery};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::shared_kernel::types::search::{SearchRequest, SearchResponse};

fn two_keyword_schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    for name in ["tag", "region"] {
        fields.insert(
            name.to_string(),
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
    }
    CreateCollectionRequest { fields }
}

#[test]
fn eval_query_and_sums_child_scores_exactly() {
    // QueryNode::And of two term queries. Each term contributes a
    // constant score of 1.0, so a doc matching both must score
    // exactly 2.0. Kills the eval_query AND `score + s` → `-`/`*`.
    let e = Engine::new();
    e.create_collection("c", two_keyword_schema()).unwrap();
    e.index(
        "c",
        IndexRequest {
            items: vec![
                item("d", "tag", FieldValue::String("rust".into())),
                item("d", "region", FieldValue::String("apac".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let resp = e
        .search(
            "c",
            SearchRequest {
                query: QueryNode::And(vec![
                    QueryNode::Term(TermQuery {
                        field: "tag".into(),
                        value: FieldValue::String("rust".into()),
                    }),
                    QueryNode::Term(TermQuery {
                        field: "region".into(),
                        value: FieldValue::String("apac".into()),
                    }),
                ]),
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
    assert_eq!(resp.hits.len(), 1);
    assert!(
        (resp.hits[0].score - 2.0).abs() < 1e-6,
        "AND of two terms must sum to exactly 2.0, got {}",
        resp.hits[0].score
    );
}

#[test]
fn eval_query_or_sums_and_ranks_multi_match_first() {
    // QueryNode::Or of two terms. A doc matching both scores 2.0 and
    // must rank above a doc matching one (1.0). Kills eval_query OR
    // `+= score` → `-=`/`*=` (which would flip or zero the ranking).
    let e = Engine::new();
    e.create_collection("c", two_keyword_schema()).unwrap();
    e.index(
        "c",
        IndexRequest {
            items: vec![
                item("both", "tag", FieldValue::String("rust".into())),
                item("both", "region", FieldValue::String("apac".into())),
                item("one", "tag", FieldValue::String("rust".into())),
                item("one", "region", FieldValue::String("emea".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let resp = e
        .search(
            "c",
            SearchRequest {
                query: QueryNode::Or(vec![
                    QueryNode::Term(TermQuery {
                        field: "tag".into(),
                        value: FieldValue::String("rust".into()),
                    }),
                    QueryNode::Term(TermQuery {
                        field: "region".into(),
                        value: FieldValue::String("apac".into()),
                    }),
                ]),
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
    assert_eq!(resp.hits[0].external_id, "both");
    assert!((score_of(&resp.hits, "both") - 2.0).abs() < 1e-6);
    assert!((score_of(&resp.hits, "one") - 1.0).abs() < 1e-6);
}

#[test]
fn eval_query_not_excludes_exactly_the_matched_set() {
    // NOT over a term: universe minus the matched eids, each scored
    // 1.0. Kills the `delete !` mutant on the Not branch.
    let e = Engine::new();
    e.create_collection("c", two_keyword_schema()).unwrap();
    e.index(
        "c",
        IndexRequest {
            items: vec![
                item("a", "tag", FieldValue::String("rust".into())),
                item("b", "tag", FieldValue::String("go".into())),
                item("c", "tag", FieldValue::String("python".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let resp = e
        .search(
            "c",
            SearchRequest {
                query: QueryNode::Not(Box::new(QueryNode::Term(TermQuery {
                    field: "tag".into(),
                    value: FieldValue::String("rust".into()),
                }))),
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
    assert_eq!(resp.total, 2);
    let ids: BTreeSet<&str> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert!(ids.contains("b") && ids.contains("c") && !ids.contains("a"));
}

#[test]
fn validate_query_rejects_pathological_trees_but_allows_normal() {
    let e = Engine::new();
    e.create_collection("c", two_keyword_schema()).unwrap();
    let term = || {
        QueryNode::Term(TermQuery {
            field: "tag".into(),
            value: FieldValue::String("rust".into()),
        })
    };
    let is_too_complex = |r: Result<SearchResponse>| {
        matches!(
            r.unwrap_err().downcast_ref::<StorageError>(),
            Some(StorageError::QueryTooComplex(_))
        )
    };
    let search = |q: QueryNode| {
        e.search(
            "c",
            SearchRequest {
                query: q,
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
            },
        )
    };

    // Deeply nested (would stack-overflow eval without the guard). Build
    // iteratively so the test itself doesn't recurse.
    let mut deep = term();
    for _ in 0..1000 {
        deep = QueryNode::And(vec![deep]);
    }
    assert!(is_too_complex(search(deep)), "deep query must be rejected");

    // Very wide (node-count DoS).
    let wide = QueryNode::And((0..100_000).map(|_| term()).collect());
    assert!(is_too_complex(search(wide)), "wide query must be rejected");

    // Huge terms fan-out.
    let huge_terms = QueryNode::Terms(TermsQuery {
        field: "tag".into(),
        values: (0..2000)
            .map(|i| FieldValue::String(format!("v{i}")))
            .collect(),
    });
    assert!(
        is_too_complex(search(huge_terms)),
        "huge terms must be rejected"
    );

    // A normal shallow query still works.
    assert!(
        search(QueryNode::And(vec![term()])).is_ok(),
        "normal query must pass"
    );
}
