use crate::sharding::application::search_fanout::{make_cursor, merge_shard_search_responses};
use crate::shared_kernel::types::{
    document::FieldValue,
    query::{QueryNode, SortMissing, SortOrder, SortSpec, TermQuery},
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
