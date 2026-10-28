use crate::sharding::application::search_fanout::{
    make_cursor, merge_shard_search_responses, search_shards_parallel,
};
use crate::shared_kernel::types::{
    document::FieldValue,
    query::{KnnQuery, QueryNode, RrfQuery, SortMissing, SortOrder, SortSpec, TermQuery},
    search::{SearchHit, SearchRequest, SearchResponse},
};

#[test]
fn merge_shard_search_responses_ranks_score_desc_then_external_id() {
    let req = search_req(None);
    let resp = merge_shard_search_responses(
        &req,
        [
            search_resp([hit("b", 2.0), hit("d", 1.0)], 2),
            search_resp([hit("a", 2.0), hit("c", 3.0)], 2),
        ],
        42,
        |_, _| None,
    );

    let ids: Vec<_> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert_eq!(ids, ["c", "a", "b"]);
    assert_eq!(resp.total, 4);
    assert!(resp.cursor.is_some());
    assert_eq!(resp.took_us, 42);
}

#[test]
fn merge_shard_search_responses_applies_global_cursor_offset() {
    let mut req = search_req(None);
    req.cursor = Some(make_cursor(2));
    let resp = merge_shard_search_responses(
        &req,
        [
            search_resp([hit("a", 4.0), hit("b", 3.0)], 2),
            search_resp([hit("c", 2.0), hit("d", 1.0)], 2),
        ],
        1000,
        |_, _| None,
    );

    let ids: Vec<_> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert_eq!(ids, ["c", "d"]);
    assert_eq!(resp.cursor, None);
    assert_eq!(resp.took_ms, 1);
}

#[test]
fn merge_shard_search_responses_applies_native_offset_after_global_rank() {
    let mut req = search_req(None);
    req.offset = 2;
    req.limit = 2;
    let resp = merge_shard_search_responses(
        &req,
        [
            search_resp([hit("a", 4.0), hit("d", 1.0)], 2),
            search_resp([hit("b", 3.0), hit("c", 2.0)], 2),
        ],
        1000,
        |_, _| None,
    );

    let ids: Vec<_> = resp
        .hits
        .iter()
        .map(|hit| hit.external_id.as_str())
        .collect();
    assert_eq!(ids, ["c", "d"]);
    assert_eq!(resp.total, 4);
}

#[test]
fn merge_shard_search_responses_applies_native_offset_after_global_sort() {
    let mut req = search_req(Some(vec![SortSpec {
        field: "age".into(),
        order: SortOrder::Asc,
        missing: SortMissing::Exclude,
    }]));
    req.offset = 1;
    req.limit = 2;
    let resp = merge_shard_search_responses(
        &req,
        [
            search_resp([hit("older", 1.0), hit("middle", 1.0)], 2),
            search_resp([hit("young", 1.0)], 1),
        ],
        0,
        |hit, field| match (hit.external_id.as_str(), field) {
            ("young", "age") => Some(20.0),
            ("middle", "age") => Some(35.0),
            ("older", "age") => Some(70.0),
            _ => None,
        },
    );
    let ids: Vec<_> = resp
        .hits
        .iter()
        .map(|hit| hit.external_id.as_str())
        .collect();
    assert_eq!(ids, ["middle", "older"]);
}

#[test]
fn merge_shard_search_responses_sorts_by_resolved_number_key() {
    let mut req = search_req(None);
    req.sort = Some(vec![SortSpec {
        field: "age".into(),
        order: SortOrder::Asc,
        missing: SortMissing::Exclude,
    }]);
    let resp = merge_shard_search_responses(
        &req,
        [
            search_resp([hit("older", 1.0), hit("middle", 1.0)], 2),
            search_resp([hit("young", 1.0)], 1),
        ],
        0,
        |hit, field| match (hit.external_id.as_str(), field) {
            ("young", "age") => Some(20.0),
            ("middle", "age") => Some(35.0),
            ("older", "age") => Some(70.0),
            _ => None,
        },
    );

    let ids: Vec<_> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert_eq!(ids, ["young", "middle", "older"]);
}

fn search_req(sort: Option<Vec<SortSpec>>) -> SearchRequest {
    SearchRequest {
        query: QueryNode::Term(TermQuery {
            field: "city".into(),
            value: FieldValue::String("taipei".into()),
        }),
        limit: 3,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort,
        track_total: true,
        collapse: None,
    }
}

fn search_resp<const N: usize>(hits: [SearchHit; N], total: u64) -> SearchResponse {
    SearchResponse {
        hits: hits.into(),
        total,
        cursor: None,
        took_ms: 0,
        took_us: 0,
    }
}

fn hit(external_id: &str, score: f32) -> SearchHit {
    SearchHit {
        external_id: external_id.into(),
        score,
    }
}

#[test]
fn merge_shard_search_responses_equal_scores_independent_of_shard_order() {
    let req = search_req(None);
    let s1 = search_resp([hit("beta", 1.0), hit("delta", 1.0)], 2);
    let s2 = search_resp([hit("alpha", 1.0), hit("gamma", 1.0)], 2);

    let resp_order1 = merge_shard_search_responses(&req, [s1.clone(), s2.clone()], 0, |_, _| None);
    let resp_order2 = merge_shard_search_responses(&req, [s2, s1], 0, |_, _| None);

    let ids1: Vec<_> = resp_order1
        .hits
        .iter()
        .map(|h| h.external_id.as_str())
        .collect();
    let ids2: Vec<_> = resp_order2
        .hits
        .iter()
        .map(|h| h.external_id.as_str())
        .collect();

    assert_eq!(ids1, ["alpha", "beta", "delta"]);
    assert_eq!(ids2, ["alpha", "beta", "delta"]);
    assert_eq!(resp_order1.hits.len(), resp_order2.hits.len());
    for (h1, h2) in resp_order1.hits.iter().zip(&resp_order2.hits) {
        assert_eq!(h1.external_id, h2.external_id);
        assert_eq!(h1.score, h2.score);
    }
}

#[test]
fn merge_shard_search_responses_duplicate_external_id_retains_higher_score() {
    let req = search_req(None);
    // Same external_id "dup" on both shards: shard 1 has score 1.5, shard 2 has score 3.5.
    let s1 = search_resp([hit("dup", 1.5)], 1);
    let s2 = search_resp([hit("dup", 3.5), hit("unique", 2.0)], 2);

    let resp = merge_shard_search_responses(&req, [s1, s2], 0, |_, _| None);

    let ids: Vec<_> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert_eq!(ids, ["dup", "unique"]);
    assert_eq!(resp.hits[0].score, 3.5);
    // Total documents shard totals sum (1 + 2 = 3).
    assert_eq!(resp.total, 3);
}

#[test]
fn merge_shard_search_responses_with_empty_shard_page() {
    let req = search_req(None);
    let s1 = search_resp([hit("a", 2.0)], 1);
    let s2 = search_resp([], 0);
    let s3 = search_resp([hit("b", 1.0)], 1);

    let resp = merge_shard_search_responses(&req, [s1, s2, s3], 0, |_, _| None);

    let ids: Vec<_> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert_eq!(ids, ["a", "b"]);
    assert_eq!(resp.total, 2);
}

#[test]
fn merge_shard_search_responses_page_boundary_inside_equal_score_run() {
    let mut req1 = search_req(None);
    req1.limit = 2;
    let s1 = search_resp([hit("doc-3", 1.0), hit("doc-1", 1.0)], 2);
    let s2 = search_resp([hit("doc-4", 1.0), hit("doc-2", 1.0)], 2);

    // Page 1
    let resp1 = merge_shard_search_responses(&req1, [s1.clone(), s2.clone()], 0, |_, _| None);
    let ids1: Vec<_> = resp1.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert_eq!(ids1, ["doc-1", "doc-2"]);
    let cursor = resp1.cursor.expect("cursor must be present for next page");

    // Page 2 continues with cursor
    let mut req2 = search_req(None);
    req2.limit = 2;
    req2.cursor = Some(cursor);
    let resp2 = merge_shard_search_responses(&req2, [s1, s2], 0, |_, _| None);
    let ids2: Vec<_> = resp2.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert_eq!(ids2, ["doc-3", "doc-4"]);
    assert_eq!(resp2.cursor, None);
}

#[test]
fn merge_shard_search_responses_knn_shaped_request_ranks_score_desc_then_external_id() {
    let mut req = search_req(None);
    req.query = QueryNode::Knn(KnnQuery {
        field: "embedding".into(),
        vector: vec![0.1, 0.2, 0.3],
        k: 10,
    });
    req.limit = 10;

    let s1 = search_resp([hit("k2", 0.9), hit("k4", 0.5)], 2);
    let s2 = search_resp([hit("k1", 0.9), hit("k3", 0.7)], 2);

    let resp = merge_shard_search_responses(&req, [s1, s2], 0, |_, _| None);

    let ids: Vec<_> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert_eq!(ids, ["k1", "k2", "k3", "k4"]);
    assert_eq!(resp.total, 4);
}

#[test]
fn merge_shard_search_responses_rrf_shaped_request_pins_score_desc_then_external_id() {
    // Pins rule: cross-shard RRF rankings merge by per-shard fused score descending,
    // tie-broken by external_id ascending.
    let mut req = search_req(None);
    req.query = QueryNode::Rrf(RrfQuery {
        queries: vec![
            QueryNode::Term(TermQuery {
                field: "city".into(),
                value: FieldValue::String("taipei".into()),
            }),
            QueryNode::Knn(KnnQuery {
                field: "embedding".into(),
                vector: vec![0.1, 0.2, 0.3],
                k: 10,
            }),
        ],
        k: 60,
    });
    req.limit = 10;

    let s1 = search_resp([hit("r2", 0.03), hit("r4", 0.01)], 2);
    let s2 = search_resp([hit("r1", 0.03), hit("r3", 0.02)], 2);

    let resp = merge_shard_search_responses(&req, [s1, s2], 0, |_, _| None);

    let ids: Vec<_> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert_eq!(ids, ["r1", "r2", "r3", "r4"]);
    assert_eq!(resp.total, 4);
}

#[test]
fn search_shards_parallel_returns_error_and_no_partial_page_on_shard_failure() {
    let req = search_req(None);
    let shards = vec![1, 2, 3];
    let result = search_shards_parallel(
        "coll",
        req,
        &shards,
        |shard, _coll, _req| {
            if *shard == 2 {
                anyhow::bail!("shard 2 connection lost")
            } else {
                Ok(search_resp([hit("x", 1.0)], 1))
            }
        },
        |_, _| None,
    );
    assert!(result.is_err(), "expected shard failure to return Err");
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("shard 2 connection lost"),
        "expected error message from failing shard, got {err_str}"
    );
}

#[test]
fn search_shards_parallel_succeeds_when_all_shards_succeed() {
    let req = search_req(None);
    let shards = vec![1, 2];
    let resp = search_shards_parallel(
        "coll",
        req,
        &shards,
        |shard, _coll, _req| {
            if *shard == 1 {
                Ok(search_resp([hit("a", 2.0)], 1))
            } else {
                Ok(search_resp([hit("b", 1.0)], 1))
            }
        },
        |_, _| None,
    )
    .expect("parallel search should succeed");
    let ids: Vec<_> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert_eq!(ids, ["a", "b"]);
    assert_eq!(resp.total, 2);
}

#[test]
fn search_shards_parallel_filter_before_candidate_selection_preserves_filter_and_expands_limit() {
    let mut req = search_req(None);
    req.offset = 2;
    req.limit = 3;
    let expected_query = req.query.clone();

    let shards = vec![1, 2];
    let resp = search_shards_parallel(
        "coll",
        req,
        &shards,
        |shard, _coll, shard_req| {
            // Per-shard request keeps the original filter, offset=0, cursor=None, and limit = offset + limit
            assert_eq!(shard_req.limit, 5);
            assert_eq!(shard_req.offset, 0);
            assert_eq!(shard_req.cursor, None);
            assert_eq!(
                format!("{:?}", shard_req.query),
                format!("{:?}", expected_query)
            );

            // Shard applies filter: candidate "filtered_out" does not match filter and is excluded.
            // Only matching candidates are returned.
            if *shard == 1 {
                Ok(search_resp(
                    [hit("s1_1", 6.0), hit("s1_2", 5.0), hit("s1_3", 4.0)],
                    3,
                ))
            } else {
                Ok(search_resp(
                    [hit("s2_1", 3.0), hit("s2_2", 2.0), hit("s2_3", 1.0)],
                    3,
                ))
            }
        },
        |_, _| None,
    )
    .expect("search should succeed");

    // Verify filtered-out hit never reaches merged page, and offset=2, limit=3 pagination works
    assert_eq!(resp.hits.len(), 3);
    let ids: Vec<_> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert_eq!(ids, ["s1_3", "s2_1", "s2_2"]);
    assert!(!resp.hits.iter().any(|h| h.external_id == "filtered_out"));
}
