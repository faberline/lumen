use crate::types::document::FieldValue;
use crate::types::query::{MatchOp, MatchQuery, QueryNode};
use crate::types::schema::{
    Analyzer, FieldSpec, FieldType, VectorBackend, VectorMetric, VectorQuantize,
};

fn fs(t: FieldType) -> FieldSpec {
    FieldSpec {
        field_type: t,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
        facetable: false,
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
fn float64_parses_as_number() {
    let spec: FieldSpec = serde_json::from_str(r#"{"type":"float64"}"#).unwrap();
    assert_eq!(spec.field_type, FieldType::Number);
    let value = serde_json::to_value(&spec).unwrap();
    assert_eq!(value["type"], serde_json::json!("number"));
    let caps = FieldType::Number.capabilities();
    assert!(caps.exact && caps.range && caps.sort);
    assert!(!caps.set_membership && !caps.prefix && !caps.bm25);
}

#[test]
fn number_type_still_parses() {
    let spec: FieldSpec = serde_json::from_str(r#"{"type":"number"}"#).unwrap();
    assert_eq!(spec.field_type, FieldType::Number);
}

#[test]
fn float65_is_refused() {
    assert!(serde_json::from_str::<FieldSpec>(r#"{"type":"float65"}"#).is_err());
}

#[test]
fn facetable_defaults_to_false_and_is_omitted_on_the_wire() {
    let spec: FieldSpec = serde_json::from_str(r#"{"type":"keyword"}"#).unwrap();
    assert!(!spec.facetable);
    let wire = serde_json::to_string(&spec).unwrap();
    assert!(!wire.contains("facetable"), "wire form changed: {wire}");
}

#[test]
fn facetable_true_round_trips() {
    let spec: FieldSpec = serde_json::from_str(r#"{"type":"keyword","facetable":true}"#).unwrap();
    assert!(spec.facetable);
    let value = serde_json::to_value(&spec).unwrap();
    assert_eq!(value["facetable"], serde_json::json!(true));
    let back: FieldSpec = serde_json::from_value(value).unwrap();
    assert_eq!(back, spec);
}
