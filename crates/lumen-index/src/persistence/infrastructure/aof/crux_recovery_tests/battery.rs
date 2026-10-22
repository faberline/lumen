use std::collections::{BTreeMap, BTreeSet};

use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::{
    KnnQuery, MatchOp, MatchQuery, QueryNode, RangeBound, RangeQuery, TermQuery, TermsQuery,
};
use crate::shared_kernel::types::search::SearchRequest;

fn req(query: QueryNode, limit: u32) -> SearchRequest {
    SearchRequest {
        query,
        limit,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    }
}

fn run(e: &Engine, coll: &str, query: QueryNode, limit: u32) -> Vec<(String, u32)> {
    e.search(coll, req(query, limit))
        .unwrap()
        .hits
        .into_iter()
        .map(|h| (h.external_id, h.score.to_bits()))
        .collect()
}

fn set_of(rows: &[(String, u32)]) -> BTreeSet<String> {
    rows.iter().map(|(e, _)| e.clone()).collect()
}

fn scores_of(rows: &[(String, u32)]) -> BTreeMap<String, u32> {
    rows.iter().map(|(e, s)| (e.clone(), *s)).collect()
}

fn driven(extra: QueryNode) -> QueryNode {
    QueryNode::And(vec![
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: "tok".into(),
            op: MatchOp::And,
        }),
        extra,
    ])
}

/// Full query battery: predicate legs (range/term/setmem/point/bm25/hamming)
/// as (set, byte-scores), kNN as the ordered ranked vec, and a doc count.
pub(super) fn battery(e: &Engine, coll: &str) -> Vec<(BTreeSet<String>, BTreeMap<String, u32>)> {
    let legs = vec![
        driven(QueryNode::Range(RangeQuery {
            field: "num".into(),
            gt: None,
            gte: Some(RangeBound::Number(2.0)),
            lt: Some(RangeBound::Number(9.0)),
            lte: None,
        })),
        driven(QueryNode::Term(TermQuery {
            field: "kw".into(),
            value: FieldValue::String("a".into()),
        })),
        driven(QueryNode::Terms(TermsQuery {
            field: "tags".into(),
            values: vec![FieldValue::String("red".into())],
        })),
        QueryNode::Term(TermQuery {
            field: "kw".into(),
            value: FieldValue::String("b".into()),
        }),
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: "tok".into(),
            op: MatchOp::And,
        }),
        QueryNode::Hamming(crate::shared_kernel::types::query::HammingQuery {
            field: "sig".into(),
            hash: format!("{:016x}", 0u64),
            max_distance: 8,
        }),
    ];
    legs.into_iter()
        .map(|q| {
            let r = run(e, coll, q, 100_000);
            (set_of(&r), scores_of(&r))
        })
        .collect()
}

pub(super) fn knn(e: &Engine, coll: &str, q: &[f32]) -> Vec<(String, u32)> {
    run(
        e,
        coll,
        QueryNode::Knn(KnnQuery {
            field: "emb".into(),
            vector: q.to_vec(),
            k: 8,
        }),
        8,
    )
}
