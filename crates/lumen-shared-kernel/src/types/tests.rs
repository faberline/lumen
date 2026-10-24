use crate::types::document::FieldValue;
use crate::types::query::{MatchOp, MatchQuery, QueryNode};
use crate::types::schema::{
    Analyzer, FieldSpec, FieldType, VectorBackend, VectorMetric, VectorQuantize,
};
use crate::types::search::{BatchSearchItem, BatchSearchRequest, SearchRequest};

fn fs(t: FieldType) -> FieldSpec {
    FieldSpec {
        field_type: t,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}

#[test]
fn field_spec_keyword_multi_becomes_set() {
    let spec = FieldSpec {
        multi: Some(true),
        ..fs(FieldType::Keyword)
    }
    .normalize();
    assert_eq!(spec.field_type, FieldType::Set);
    assert!(spec.multi.is_none());
}

#[test]
fn field_spec_text_gets_default_analyzer() {
    let spec = fs(FieldType::Text).normalize();
    assert_eq!(spec.analyzer, Some(Analyzer::WhitespaceLower));
}

#[test]
fn field_spec_vector_defaults_backend_to_hnsw_cpu() {
    let spec = FieldSpec {
        dim: Some(128),
        metric: Some(VectorMetric::Cosine),
        ..fs(FieldType::Vector)
    }
    .normalize();
    assert_eq!(spec.backend, Some(VectorBackend::HnswCpu));
}

#[test]
fn field_spec_vector_spec_wire_round_trip() {
    let j = r#"{"type":"vector","dim":768,"metric":"cosine","backend":"hnsw-cpu","quantize":"sq"}"#;
    let s: FieldSpec = serde_json::from_str(j).unwrap();
    assert!(matches!(s.field_type, FieldType::Vector));
    let vs = s.vector_spec().unwrap().unwrap();
    assert_eq!(vs.dim, 768);
    assert!(matches!(vs.metric, VectorMetric::Cosine));
    assert!(matches!(vs.backend, VectorBackend::HnswCpu));
    assert!(matches!(vs.quantize, Some(VectorQuantize::Sq)));
}

#[test]
fn field_spec_vector_missing_dim_rejected() {
    let bad = FieldSpec {
        metric: Some(VectorMetric::L2),
        ..fs(FieldType::Vector)
    };
    assert!(bad.vector_spec().is_err());
}

#[test]
fn vector_value_round_trip_as_float_array() {
    let v: FieldValue = serde_json::from_str("[0.1, 0.2, 0.3]").unwrap();
    match v {
        FieldValue::Vector(xs) => {
            assert_eq!(xs.len(), 3);
            assert!((xs[0] - 0.1).abs() < 1e-6);
        }
        _ => panic!("expected Vector, got {v:?}"),
    }
}

#[test]
fn query_node_knn_round_trip() {
    let j = r#"{"knn":{"field":"e","vector":[0.1,0.2,0.3],"k":5}}"#;
    let q: QueryNode = serde_json::from_str(j).unwrap();
    match q {
        QueryNode::Knn(k) => {
            assert_eq!(k.field, "e");
            assert_eq!(k.k, 5);
            assert_eq!(k.vector.len(), 3);
        }
        _ => panic!("expected Knn, got {q:?}"),
    }
}

#[test]
fn query_node_match_serializes_externally_tagged() {
    let q = QueryNode::Match(MatchQuery {
        field: "bio".into(),
        text: "rust".into(),
        op: MatchOp::And,
    });
    let j = serde_json::to_string(&q).unwrap();
    assert!(j.contains("\"match\""), "got: {j}");
    assert!(j.contains("\"field\":\"bio\""));
}

#[test]
fn query_node_and_round_trip() {
    let j = r#"{"and":[{"term":{"field":"tags","value":"rust"}},{"range":{"field":"age","gte":25,"lt":40}}]}"#;
    let q: QueryNode = serde_json::from_str(j).unwrap();
    match q {
        QueryNode::And(ref children) => assert_eq!(children.len(), 2),
        _ => panic!("expected And, got {q:?}"),
    }
}

#[test]
fn field_value_polymorphic() {
    let s: FieldValue = serde_json::from_str(r#""hello""#).unwrap();
    assert!(matches!(s, FieldValue::String(_)));
    let n: FieldValue = serde_json::from_str("42").unwrap();
    assert!(matches!(n, FieldValue::Number(_)));
    let l: FieldValue = serde_json::from_str(r#"["a","b"]"#).unwrap();
    assert!(matches!(l, FieldValue::StringList(_)));
}

#[test]
fn search_request_rejects_unknown_top_level_field() {
    let j = r#"{"query":{"match":{"field":"bio","text":"rust"}},"frobnicate":1}"#;
    let res: Result<SearchRequest, _> = serde_json::from_str(j);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("frobnicate"), "error must name unknown field: {err}");
}

#[test]
fn search_request_negative_control_hypothetical_v2_field_mode_rejected() {
    // Negative control: before this change, unknown fields like "mode":"v2" were silently
    // ignored during deserialization. Under the new behavior, deserialization must fail and
    // name the unknown field "mode". Removing deny_unknown_fields causes this test to fail.
    let j = r#"{"query":{"match":{"field":"bio","text":"rust"}},"mode":"v2"}"#;
    let res: Result<SearchRequest, _> = serde_json::from_str(j);
    assert!(res.is_err(), "hypothetical v2 field 'mode' must be rejected");
    let err = res.unwrap_err().to_string();
    assert!(err.contains("mode"), "error must name the unknown field: {err}");
}

#[test]
fn search_request_round_trips_known_old_requests_and_defaults() {
    // Minimal request: verify default values
    let min_json = r#"{"query":{"match":{"field":"title","text":"rust"}}}"#;
    let req: SearchRequest = serde_json::from_str(min_json).unwrap();
    assert_eq!(req.limit, 20);
    assert_eq!(req.offset, 0);
    assert!(req.track_total);
    assert!(req.cursor.is_none());
    assert!(req.routing_key.is_none());
    assert!(req.sort.is_none());
    assert!(req.collapse.is_none());

    // Full request: all known fields
    let full_json = r#"{
        "query":{"term":{"field":"tags","value":"db"}},
        "limit":50,
        "offset":10,
        "cursor":"cur123",
        "routing_key":"shard-a",
        "sort":[{"field":"created_at","order":"desc","missing":"last"}],
        "track_total":false,
        "collapse":"user_id"
    }"#;
    let full_req: SearchRequest = serde_json::from_str(full_json).unwrap();
    assert_eq!(full_req.limit, 50);
    assert_eq!(full_req.offset, 10);
    assert_eq!(full_req.cursor.as_deref(), Some("cur123"));
    assert_eq!(full_req.routing_key.as_deref(), Some("shard-a"));
    assert!(!full_req.track_total);
    assert_eq!(full_req.collapse.as_deref(), Some("user_id"));
    let sort = full_req.sort.as_ref().unwrap();
    assert_eq!(sort.len(), 1);
    assert_eq!(sort[0].field, "created_at");

    // Round-trip serialize & deserialize
    let serialized = serde_json::to_string(&full_req).unwrap();
    let round_tripped: SearchRequest = serde_json::from_str(&serialized).unwrap();
    assert_eq!(round_tripped.limit, full_req.limit);
    assert_eq!(round_tripped.offset, full_req.offset);
    assert_eq!(round_tripped.cursor, full_req.cursor);
}

#[test]
fn search_request_nested_unknown_fields_in_query_nodes_rejected() {
    // SortSpec unknown field
    let j = r#"{"query":{"match":{"field":"f","text":"t"}},"sort":[{"field":"f","unexpected":1}]}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("unexpected"), "SortSpec err: {err}");

    // MatchQuery unknown field
    let j = r#"{"query":{"match":{"field":"f","text":"t","unknown":1}}}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("unknown"), "MatchQuery err: {err}");

    // TermQuery unknown field
    let j = r#"{"query":{"term":{"field":"f","value":"v","unknown":1}}}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("unknown"), "TermQuery err: {err}");

    // TermsQuery unknown field
    let j = r#"{"query":{"terms":{"field":"f","values":["v"],"unknown":1}}}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("unknown"), "TermsQuery err: {err}");

    // PrefixQuery unknown field
    let j = r#"{"query":{"prefix":{"field":"f","value":"v","unknown":1}}}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("unknown"), "PrefixQuery err: {err}");

    // IdsQuery unknown field
    let j = r#"{"query":{"ids":{"values":["id1"],"unknown":1}}}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("unknown"), "IdsQuery err: {err}");

    // RangeQuery unknown field
    let j = r#"{"query":{"range":{"field":"f","gte":10,"unknown":1}}}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("unknown"), "RangeQuery err: {err}");

    // KnnQuery unknown field
    let j = r#"{"query":{"knn":{"field":"f","vector":[0.1],"k":5,"unknown":1}}}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("unknown"), "KnnQuery err: {err}");

    // HasChildQuery unknown field
    let j = r#"{"query":{"has_child":{"collection":"c","field":"f","query":{"exists":{"field":"e"}},"unknown":1}}}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("unknown"), "HasChildQuery err: {err}");

    // HammingQuery unknown field
    let j = r#"{"query":{"hamming":{"field":"f","hash":"0x12","max_distance":2,"unknown":1}}}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("unknown"), "HammingQuery err: {err}");

    // RrfQuery unknown field
    let j = r#"{"query":{"rrf":{"queries":[{"exists":{"field":"e"}}],"unknown":1}}}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("unknown"), "RrfQuery err: {err}");

    // ExistsQuery unknown field
    let j = r#"{"query":{"exists":{"field":"f","unknown":1}}}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("unknown"), "ExistsQuery err: {err}");

    // DuplicatedQuery unknown field
    let j = r#"{"query":{"duplicated":{"field":"f","unknown":1}}}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("unknown"), "DuplicatedQuery err: {err}");

    // Unknown QueryNode variant
    let j = r#"{"query":{"bogus":{"field":"f"}}}"#;
    let err = serde_json::from_str::<SearchRequest>(j).unwrap_err().to_string();
    assert!(err.contains("bogus"), "QueryNode variant err: {err}");
}

#[test]
fn batch_search_item_rejects_unknown_fields() {
    // Unknown field next to collection and SearchRequest fields
    let j = r#"{"collection":"coll1","query":{"match":{"field":"f","text":"t"}},"extra_field":123}"#;
    let res: Result<BatchSearchItem, _> = serde_json::from_str(j);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("extra_field"), "error must name unknown field: {err}");

    // Unknown field inside nested query
    let j_nested = r#"{"collection":"coll1","query":{"match":{"field":"f","text":"t","nested_bad":true}}}"#;
    let res_nested: Result<BatchSearchItem, _> = serde_json::from_str(j_nested);
    assert!(res_nested.is_err());
    let err_nested = res_nested.unwrap_err().to_string();
    assert!(err_nested.contains("nested_bad"), "error must name nested field: {err_nested}");

    // Valid item deserializes and defaults are set correctly
    let valid_j = r#"{"collection":"coll1","query":{"match":{"field":"f","text":"t"}}}"#;
    let item: BatchSearchItem = serde_json::from_str(valid_j).unwrap();
    assert_eq!(item.collection, "coll1");
    assert_eq!(item.request.limit, 20);
    assert_eq!(item.request.offset, 0);
    assert!(item.request.track_total);

    // Serialization keeps the flattened wire shape: {"collection":..., "query":..., "limit":...}
    let serialized = serde_json::to_string(&item).unwrap();
    let val: serde_json::Value = serde_json::from_str(&serialized).unwrap();
    assert_eq!(val["collection"], "coll1");
    assert!(val.get("query").is_some());
    assert_eq!(val["limit"], 20);
    assert_eq!(val["offset"], 0);
    assert_eq!(val["track_total"], true);
    // And round-trips back
    let round_tripped: BatchSearchItem = serde_json::from_str(&serialized).unwrap();
    assert_eq!(round_tripped.collection, "coll1");
    assert_eq!(round_tripped.request.limit, 20);
}

#[test]
fn batch_search_request_rejects_unknown_top_level_field() {
    let j = r#"{"searches":[{"collection":"c","query":{"match":{"field":"f","text":"t"}}}],"unknown_top":1}"#;
    let res: Result<BatchSearchRequest, _> = serde_json::from_str(j);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("unknown_top"), "error must name unknown field: {err}");
}
