use lumen::types::{FieldValue, QueryNode, TermQuery};

use crate::cli::client::{QueryDuplicatesArgs, QuerySearchArgs, QueryTarget};

#[cfg(any(test, feature = "backup"))]
use crate::query::{
    build_duplicates_body, build_index_body, build_search_body, build_search_query_node,
    parse_field_value, parse_index_item, resolve_base_url,
};

// -----------------------------------------------------------------
// `lumen connect` / `lumen query` (#1321)
// -----------------------------------------------------------------

fn test_query_target() -> QueryTarget {
    QueryTarget {
        url: None,
        context: None,
        namespace: None,
        client_sa: None,
    }
}

#[test]
fn resolve_base_url_requires_explicit_url() {
    let mut target = test_query_target();
    assert!(resolve_base_url(&target).is_err());
    target.url = Some("http://127.0.0.1:7373".to_string());
    assert_eq!(resolve_base_url(&target).unwrap(), "http://127.0.0.1:7373");
}

#[test]
fn parse_field_value_prefers_json_then_falls_back_to_string() {
    assert!(matches!(parse_field_value("79"), FieldValue::Number(n) if n == 79.0));
    assert!(matches!(parse_field_value("acme"), FieldValue::String(s) if s == "acme"));
    assert!(matches!(parse_field_value("[0.1,0.2,0.9]"), FieldValue::Vector(v) if v.len() == 3));
    assert!(matches!(
        parse_field_value(r#"["a","b"]"#),
        FieldValue::StringList(v) if v == vec!["a".to_string(), "b".to_string()]
    ));
}

#[test]
fn parse_index_item_splits_external_id_field_value() {
    let item = parse_index_item("row-42:email=person@example.com").unwrap();
    assert_eq!(item.external_id, "row-42");
    assert_eq!(item.field, "email");
    assert!(matches!(item.value, FieldValue::String(ref s) if s == "person@example.com"));

    assert!(parse_index_item("missing-colon").is_err());
    assert!(parse_index_item("row-42:missing-equals").is_err());
}

/// AC3: `lumen query index`'s assembled body must match the FLAT shape
/// `lumen spec --shapes` publishes for "index" — the reporter's bug was
/// assuming a nested `{id, fields:{...}}` shape.
#[test]
fn build_index_body_matches_published_index_shape() {
    let (path, body) = build_index_body(
        "products",
        &[
            "row-42:email=person@example.com".to_string(),
            "row-42:price=79".to_string(),
        ],
    )
    .unwrap();
    assert_eq!(path, "/collections/products/index");

    let shapes = lumen::spec::query_shapes();
    let index_shape = shapes["shapes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "index")
        .expect("query_shapes() publishes an `index` shape");
    let published = &index_shape["request"];

    assert!(body["items"].is_array());
    assert!(published["items"].is_array());
    assert_eq!(
        body["items"][0]
            .as_object()
            .unwrap()
            .keys()
            .collect::<std::collections::BTreeSet<_>>(),
        published["items"][0]
            .as_object()
            .unwrap()
            .keys()
            .collect::<std::collections::BTreeSet<_>>(),
        "assembled item keys must match the published flat {{external_id,field,value}} shape"
    );
    assert_eq!(body["items"][0]["external_id"], "row-42");
    assert_eq!(body["items"][0]["field"], "email");
    assert_eq!(body["items"][0]["value"], "person@example.com");
    assert_eq!(body["items"][1]["value"], 79.0);
}

#[test]
fn build_search_query_node_requires_exactly_one_of_term_match_query_json() {
    let mut args = QuerySearchArgs {
        target: test_query_target(),
        collection: "products".into(),
        term: None,
        match_: None,
        query_json: None,
        limit: 20,
    };
    assert!(
        build_search_query_node(&args).is_err(),
        "none set should be rejected"
    );

    args.term = Some("status=active".to_string());
    let node = build_search_query_node(&args).unwrap();
    assert!(matches!(node, QueryNode::Term(TermQuery { ref field, .. }) if field == "status"));

    args.term = None;
    args.match_ = Some("title=earbuds".to_string());
    let node = build_search_query_node(&args).unwrap();
    assert!(matches!(node, QueryNode::Match(_)));

    args.term = Some("status=active".to_string());
    assert!(
        build_search_query_node(&args).is_err(),
        "both --term and --match set should be rejected"
    );
}

#[test]
fn build_search_body_assembles_search_request_wire_shape() {
    let args = QuerySearchArgs {
        target: test_query_target(),
        collection: "products".into(),
        term: None,
        match_: Some("title=earbuds".to_string()),
        query_json: None,
        limit: 10,
    };
    let (path, body) = build_search_body(&args).unwrap();
    assert_eq!(path, "/collections/products/search");
    assert_eq!(body["query"]["match"]["field"], "title");
    assert_eq!(body["query"]["match"]["text"], "earbuds");
    assert_eq!(body["limit"], 10);
}

#[test]
fn build_duplicates_body_matches_duplicates_request_shape() {
    let args = QueryDuplicatesArgs {
        target: test_query_target(),
        collection: "products".into(),
        field: "email".into(),
        min_group_size: 2,
        limit: 100,
        offset: 0,
    };
    let (path, body) = build_duplicates_body(&args).unwrap();
    assert_eq!(path, "/collections/products/duplicates");
    assert_eq!(body["field"], "email");
    assert_eq!(body["min_group_size"], 2);
    assert_eq!(body["limit"], 100);
    assert_eq!(body["offset"], 0);
}

// `wait_for_local_port_ready`/`ChildGuard` unit tests moved to
// `libs/cli-std/src/connect.rs` (#1376) along with the primitives
// themselves; lumen's own coverage is the thin-adapter tests above
// (`resolve_base_url_requires_explicit_url`, `build_*_body_*`) plus
// `cargo test -p cli-std --features k8s`. The credential half of that
// shared module (`select_token`, `cr_tokens_secret`, `secret_data_bytes`)
// keeps its own tests there and is simply no longer called from here
// (#2873) — the integration gate for that is
// `tests/it/cli_credential_paths_retired.rs`.
