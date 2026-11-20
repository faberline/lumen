//! `lumen migrate search-request`: a pure JSON-to-JSON transform from a 0.4
//! search request to the 0.5 request. It does not use the engine's types.

use serde_json::{json, Map, Value};

/// One converted value, as the report lists it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Change {
    pub(crate) path: String,
    pub(crate) from: Value,
    pub(crate) to: Value,
}

const KNOWN_KEYS: [&str; 8] = [
    "query",
    "limit",
    "offset",
    "routing_key",
    "sort",
    "track_total",
    "cursor",
    "collapse",
];
const SCORING_ROOTS: [&str; 4] = ["match", "knn", "rrf", "hamming"];
const SORT_KEYS: [&str; 3] = ["field", "order", "missing"];

/// Converts one 0.4 request. `Err` holds every refusal, in rule order.
pub(crate) fn convert_search_request(input: &Value) -> Result<(Value, Vec<Change>), Vec<String>> {
    let Some(top) = input.as_object() else {
        return Err(vec!["request must be a JSON object".to_string()]);
    };
    let mut refusals = Vec::new();
    let mut changes = Vec::new();

    // Rule 2: unknown keys, in ascending name order.
    let mut names: Vec<&String> = top.keys().collect();
    names.sort();
    for name in &names {
        if !KNOWN_KEYS.contains(&name.as_str()) {
            refusals.push(format!("unknown field \"{name}\""));
        }
    }

    // Rule 3: the query becomes `scoring`; only scoring-only roots convert.
    let mut out = Map::new();
    match top.get("query") {
        None => refusals.push("query is required".to_string()),
        Some(query) => match convert_query_root(query) {
            Ok(node) => {
                out.insert("scoring".to_string(), node);
            }
            Err(message) => refusals.push(message),
        },
    }

    // Rule 4: copied unchanged.
    for key in ["limit", "offset", "routing_key"] {
        if let Some(value) = top.get(key) {
            out.insert(key.to_string(), value.clone());
        }
    }

    // Rule 5: slice #349.
    for key in ["cursor", "collapse"] {
        if top.contains_key(key) {
            refusals.push(format!(
                "{key} is not supported by this version of the tool"
            ));
        }
    }

    // Rule 7: sort. Inserted here so the output key order is fixed.
    if let Some(sort) = top.get("sort") {
        if let Some(items) = convert_sort(sort, &mut refusals) {
            out.insert("sort".to_string(), Value::Array(items));
        }
    }

    // Rule 6: track_total (decision A).
    if let Some(value) = top.get("track_total") {
        match value.as_bool() {
            Some(flag) => {
                let to = if flag { "exact" } else { "none" };
                changes.push(Change {
                    path: "track_total".to_string(),
                    from: value.clone(),
                    to: json!(to),
                });
                out.insert("track_total".to_string(), json!(to));
            }
            None => refusals.push("track_total: must be true or false (a 0.4 value)".to_string()),
        }
    }

    if refusals.is_empty() {
        Ok((Value::Object(out), changes))
    } else {
        Err(refusals)
    }
}

/// Rule 3: only a scoring-only root (`match`, `knn`, `rrf`, `hamming`) converts.
fn convert_query_root(query: &Value) -> Result<Value, String> {
    let unsupported = || {
        "query: splitting this query tree is not supported by this version of the tool".to_string()
    };
    let Some(obj) = query.as_object() else {
        return Err(unsupported());
    };
    if obj.len() == 1
        && SCORING_ROOTS.contains(&obj.keys().next().map(String::as_str).unwrap_or(""))
    {
        Ok(query.clone())
    } else {
        Err(unsupported())
    }
}

/// Rule 7: every sort key needs an explicit `first` or `last`. Returns the
/// converted items, or `None` when the array was refused (messages pushed).
fn convert_sort(sort: &Value, refusals: &mut Vec<String>) -> Option<Vec<Value>> {
    let Some(items) = sort.as_array() else {
        refusals.push("sort: must be an array".to_string());
        return None;
    };
    let mut out = Vec::new();
    let mut ok = true;
    for (i, item) in items.iter().enumerate() {
        let shape_ok = item
            .as_object()
            .is_some_and(|o| o.keys().all(|k| SORT_KEYS.contains(&k.as_str())));
        if !shape_ok {
            refusals.push(format!(
                "sort[{i}]: must be an object with field, order and missing only"
            ));
            ok = false;
            continue;
        }
        let obj = item.as_object().expect("checked above");
        let Some(field) = obj.get("field").and_then(Value::as_str) else {
            refusals.push(format!("sort[{i}]: field is missing or not a string"));
            ok = false;
            continue;
        };
        match obj.get("missing").map(Value::as_str) {
            Some(Some("first")) | Some(Some("last")) => {}
            None | Some(Some("exclude")) => {
                refusals.push(format!(
                    "sort[{i}] field \"{field}\": missing is \"exclude\" (the 0.4 default); 0.5 requires an explicit \"first\" or \"last\", the caller must choose one"
                ));
                ok = false;
                continue;
            }
            _ => {
                refusals.push(format!(
                    "sort[{i}] field \"{field}\": missing must be \"first\" or \"last\""
                ));
                ok = false;
                continue;
            }
        }
        // Output key order: field, order (when present), missing.
        let mut item_out = Map::new();
        item_out.insert("field".to_string(), json!(field));
        if let Some(order) = obj.get("order") {
            item_out.insert("order".to_string(), order.clone());
        }
        item_out.insert("missing".to_string(), obj["missing"].clone());
        out.push(Value::Object(item_out));
    }
    if items.len() > 4 {
        refusals.push("sort: at most 4 keys".to_string());
    }
    if ok {
        Some(out)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::convert_search_request;
    use serde_json::{json, Value};

    fn q() -> Value {
        json!({"match": {"field": "bio", "text": "rust"}})
    }

    fn ok(input: Value) -> (Value, Vec<super::Change>) {
        convert_search_request(&input).expect("conversion should succeed")
    }

    fn refused(input: Value) -> Vec<String> {
        convert_search_request(&input).expect_err("conversion should be refused")
    }

    #[test]
    fn track_total_true_becomes_exact() {
        let (out, changes) = ok(json!({"query": q(), "track_total": true}));
        assert_eq!(out["track_total"], "exact");
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].path, "track_total");
        assert_eq!(changes[0].from, json!(true));
        assert_eq!(changes[0].to, json!("exact"));
    }

    #[test]
    fn track_total_false_becomes_none() {
        let (out, changes) = ok(json!({"query": q(), "track_total": false}));
        assert_eq!(out["track_total"], "none");
        assert_eq!(changes[0].to, json!("none"));
    }

    #[test]
    fn absent_track_total_stays_absent() {
        let (out, changes) = ok(json!({"query": q()}));
        assert!(out.get("track_total").is_none());
        assert!(changes.is_empty());
    }

    #[test]
    fn track_total_bool_is_never_left_as_a_bool() {
        // Negative control: fails if the conversion is removed.
        for value in [json!(true), json!(false)] {
            let (out, _) = ok(json!({"query": q(), "track_total": value}));
            assert!(out["track_total"].is_string());
            assert!(!out["track_total"].is_boolean());
        }
    }

    #[test]
    fn non_bool_track_total_is_refused() {
        for value in [json!("exact"), json!(1), json!(null)] {
            let errs = refused(json!({"query": q(), "track_total": value}));
            assert!(
                errs.contains(&"track_total: must be true or false (a 0.4 value)".to_string()),
                "{errs:?}"
            );
        }
    }

    #[test]
    fn sort_missing_default_exclude_is_refused() {
        let expected = r#"sort[0] field "age": missing is "exclude" (the 0.4 default); 0.5 requires an explicit "first" or "last", the caller must choose one"#;
        let errs = refused(json!({"query": q(), "sort": [{"field": "age"}]}));
        assert!(errs.contains(&expected.to_string()), "{errs:?}");
        let errs = refused(json!({"query": q(), "sort": [{"field": "age", "missing": "exclude"}]}));
        assert!(errs.contains(&expected.to_string()), "{errs:?}");
    }

    #[test]
    fn sort_missing_first_and_last_are_kept() {
        let sort = json!([{"field": "age", "order": "desc", "missing": "last"}]);
        let (out, changes) = ok(json!({"query": q(), "sort": sort.clone()}));
        assert_eq!(out["sort"], sort);
        assert!(changes.is_empty());
    }

    #[test]
    fn sort_bad_shapes_are_refused() {
        let cases = [
            (json!(5), "sort: must be an array"),
            (
                json!([5]),
                "sort[0]: must be an object with field, order and missing only",
            ),
            (
                json!([{"field": 3, "missing": "first"}]),
                "sort[0]: field is missing or not a string",
            ),
            (
                json!([{"field": "a", "missing": "middle"}]),
                r#"sort[0] field "a": missing must be "first" or "last""#,
            ),
            (
                json!([{"field": "a", "missing": "first", "x": 1}]),
                "sort[0]: must be an object with field, order and missing only",
            ),
        ];
        for (sort, message) in cases {
            let errs = refused(json!({"query": q(), "sort": sort}));
            assert!(errs.contains(&message.to_string()), "{message}: {errs:?}");
        }
    }

    #[test]
    fn sort_with_more_than_four_keys_is_refused() {
        let keys: Vec<Value> = (0..5)
            .map(|i| json!({"field": format!("f{i}"), "missing": "first"}))
            .collect();
        let errs = refused(json!({"query": q(), "sort": keys}));
        assert!(
            errs.contains(&"sort: at most 4 keys".to_string()),
            "{errs:?}"
        );
    }

    #[test]
    fn query_is_written_as_scoring_and_never_as_query() {
        let roots = [
            q(),
            json!({"knn": {"field": "embedding", "vector": [0.1, 0.2], "k": 3}}),
            json!({"rrf": {"queries": [q()], "k": 60}}),
            json!({"hamming": {"field": "avatar_phash", "hash": "ff", "max_distance": 4}}),
        ];
        for root in roots {
            let (out, _) = ok(json!({"query": root.clone()}));
            assert_eq!(out["scoring"], root);
            assert!(out.get("query").is_none());
            assert!(out.get("filter").is_none());
        }
    }

    #[test]
    fn unsplit_query_trees_are_refused() {
        let msg = "query: splitting this query tree is not supported by this version of the tool";
        for query in [
            json!({"term": {"field": "a", "value": "x"}}),
            json!({"and": [q()]}),
            json!({}),
        ] {
            let errs = refused(json!({"query": query}));
            assert!(errs.contains(&msg.to_string()), "{errs:?}");
        }
        let errs = refused(json!({"limit": 5}));
        assert!(errs.contains(&"query is required".to_string()), "{errs:?}");
    }

    #[test]
    fn cursor_collapse_and_unknown_keys_are_refused_together() {
        let errs = refused(json!({"query": q(), "cursor": "x", "collapse": "f", "zzz": 1}));
        assert!(errs.contains(&"cursor is not supported by this version of the tool".to_string()));
        assert!(errs.contains(&"collapse is not supported by this version of the tool".to_string()));
        assert!(errs.contains(&r#"unknown field "zzz""#.to_string()));
    }

    #[test]
    fn limit_offset_routing_key_are_copied() {
        let (out, _) = ok(json!({"query": q(), "limit": 7, "offset": 100, "routing_key": "r"}));
        assert_eq!(out["limit"], 7);
        assert_eq!(out["offset"], 100);
        assert_eq!(out["routing_key"], "r");
    }

    #[test]
    fn output_key_order_is_fixed() {
        let a: Value = serde_json::from_str(
            r#"{"track_total":true,"sort":[{"field":"a","missing":"first"}],"limit":2,"query":{"match":{"field":"bio","text":"rust"}}}"#,
        )
        .unwrap();
        let b: Value = serde_json::from_str(
            r#"{"query":{"match":{"field":"bio","text":"rust"}},"limit":2,"sort":[{"field":"a","missing":"first"}],"track_total":true}"#,
        )
        .unwrap();
        let (out_a, _) = ok(a);
        let (out_b, _) = ok(b);
        let text_a = serde_json::to_string_pretty(&out_a).unwrap();
        assert_eq!(text_a, serde_json::to_string_pretty(&out_b).unwrap());
        assert!(text_a.find("\"scoring\"") < text_a.find("\"limit\""));
    }

    #[test]
    fn top_level_not_an_object_is_refused() {
        assert_eq!(
            refused(json!([])),
            vec!["request must be a JSON object".to_string()]
        );
    }
}
