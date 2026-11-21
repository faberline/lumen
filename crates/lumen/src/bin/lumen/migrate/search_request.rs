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
const FILTER_ROOTS: [&str; 8] = [
    "term",
    "terms",
    "prefix",
    "ids",
    "range",
    "has_child",
    "exists",
    "duplicated",
];
const SORT_KEYS: [&str; 3] = ["field", "order", "missing"];
const DUPLICATES_KEYS: [&str; 4] = ["field", "min_group_size", "limit", "offset"];
/// Same limit as `MAX_BATCH_SEARCH_SIZE` in `lumen-shared-kernel`.
const MAX_BATCH_ITEMS: usize = 32;
const CURSOR_REFUSAL: &str = "cursor: a 0.4 cursor is not portable to 0.5 and cannot continue a 0.5 query; drop it and start from the first page of the 0.5 request";
const COLLAPSE_REFUSAL: &str = "collapse: must be a non-empty field name string (a 0.4 value)";
const DUPLICATES_REFUSAL: &str = "duplicates: the 0.4 /duplicates request has no Search v2 form; Search v2 removes /duplicates, so use a terms facet and then a term filter for each selected value (terms facets are a later milestone item, see docs/migration-search-v2.md)";

/// Converts one input of the command: a batch, a single request, or a refused
/// legacy duplicates request. `Err` holds every refusal.
pub(crate) fn convert_input(input: &Value) -> Result<(Value, Vec<Change>), Vec<String>> {
    let Some(top) = input.as_object() else {
        return Err(vec!["request must be a JSON object".to_string()]);
    };
    if top.contains_key("searches") {
        return convert_batch(top);
    }
    let is_duplicates = top.contains_key("field")
        && !top.contains_key("query")
        && top
            .keys()
            .all(|key| DUPLICATES_KEYS.contains(&key.as_str()));
    if is_duplicates {
        return Err(vec![DUPLICATES_REFUSAL.to_string()]);
    }
    convert_search_request(input)
}

/// A batch of `collections:search` items. Each item converts with the single
/// request rules. One refused item refuses the batch, and nothing is written.
fn convert_batch(top: &Map<String, Value>) -> Result<(Value, Vec<Change>), Vec<String>> {
    let mut refusals = Vec::new();
    let mut names: Vec<&String> = top.keys().filter(|key| *key != "searches").collect();
    names.sort();
    for name in names {
        refusals.push(format!("unknown field \"{name}\""));
    }

    let Some(items) = top["searches"].as_array() else {
        refusals.push("searches: must be an array".to_string());
        return Err(refusals);
    };
    if items.len() > MAX_BATCH_ITEMS {
        refusals.push(format!("searches: at most {MAX_BATCH_ITEMS} items"));
        return Err(refusals);
    }

    let mut converted = Vec::new();
    let mut changes = Vec::new();
    for (index, item) in items.iter().enumerate() {
        let prefix = format!("searches[{index}].");
        let Some(obj) = item.as_object() else {
            refusals.push(format!("searches[{index}]: not an object"));
            continue;
        };
        let Some(collection) = obj.get("collection").filter(|v| v.is_string()) else {
            refusals.push(format!(
                "searches[{index}]: collection is missing or not a string"
            ));
            continue;
        };
        // The item is a single request: its `collection` is not a request key.
        let mut rest = obj.clone();
        rest.remove("collection");
        match convert_search_request(&Value::Object(rest)) {
            Ok((request, rows)) => {
                let mut out = Map::new();
                out.insert("collection".to_string(), collection.clone());
                if let Value::Object(fields) = request {
                    out.extend(fields);
                }
                converted.push(Value::Object(out));
                changes.extend(rows.into_iter().map(|row| Change {
                    path: format!("{prefix}{}", row.path),
                    from: row.from,
                    to: row.to,
                }));
            }
            Err(messages) => {
                refusals.extend(messages.into_iter().map(|m| format!("{prefix}{m}")));
            }
        }
    }

    if !refusals.is_empty() {
        return Err(refusals);
    }
    let mut root = Map::new();
    root.insert("searches".to_string(), Value::Array(converted));
    Ok((Value::Object(root), changes))
}

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

    // Rule 3: the query is split by node kind into `scoring` and `filter`.
    let mut out = Map::new();
    match top.get("query") {
        None => refusals.push("query is required".to_string()),
        Some(query) => match split_query(query) {
            Ok(split) => {
                let to = match (&split.scoring, &split.filter) {
                    (Some(_), Some(_)) => "scoring+filter",
                    (Some(_), None) => "scoring",
                    (None, _) => "filter",
                };
                if let Some(node) = split.scoring {
                    out.insert("scoring".to_string(), node);
                }
                if let Some(node) = split.filter {
                    out.insert("filter".to_string(), node);
                }
                changes.push(Change {
                    path: "query".to_string(),
                    from: json!("query"),
                    to: json!(to),
                });
            }
            Err(messages) => refusals.extend(messages),
        },
    }

    // Rule 4: copied unchanged.
    for key in ["limit", "offset", "routing_key"] {
        if let Some(value) = top.get(key) {
            out.insert(key.to_string(), value.clone());
        }
    }

    // Rule 5: cursor. A JSON null is the same as absent; any other value is refused.
    if top.get("cursor").is_some_and(|cursor| !cursor.is_null()) {
        refusals.push(CURSOR_REFUSAL.to_string());
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

    // Rule 8: collapse. A non-empty string s becomes {"field": s}; null is absent.
    match top.get("collapse") {
        None | Some(Value::Null) => {}
        Some(Value::String(field)) if !field.is_empty() => {
            let to = json!({"field": field});
            changes.push(Change {
                path: "collapse".to_string(),
                from: top["collapse"].clone(),
                to: to.clone(),
            });
            out.insert("collapse".to_string(), to);
        }
        Some(_) => refusals.push(COLLAPSE_REFUSAL.to_string()),
    }

    if refusals.is_empty() {
        Ok((Value::Object(out), changes))
    } else {
        Err(refusals)
    }
}

/// The kind of a query subtree. `Mixed` holds both scoring and filter nodes.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Scoring,
    Filter,
    Mixed,
}

/// The two halves of a split query. `None` means that half is absent.
#[derive(Debug, PartialEq)]
struct QuerySplit {
    scoring: Option<Value>,
    filter: Option<Value>,
}

/// Rule 3: splits a query tree by node kind. `Err` holds every refusal.
fn split_query(query: &Value) -> Result<QuerySplit, Vec<String>> {
    let mut refusals = Vec::new();
    let Some(kind) = classify(query, "query", &mut refusals) else {
        return Err(refusals);
    };
    match kind {
        Kind::Scoring => Ok(QuerySplit {
            scoring: Some(query.clone()),
            filter: None,
        }),
        Kind::Filter => Ok(QuerySplit {
            scoring: None,
            filter: Some(query.clone()),
        }),
        Kind::Mixed => {
            let Some((op, value)) = op_and_value(query) else {
                return Err(vec!["query: not a query node".to_string()]);
            };
            if op != "and" {
                return Err(vec![refusal_mixed("query", op)]);
            }
            let Some(children) = value.as_array() else {
                return Err(vec!["query: not a query node".to_string()]);
            };
            let mut scoring: Vec<Value> = Vec::new();
            let mut filter: Vec<Value> = Vec::new();
            split_and(children, "query", &mut scoring, &mut filter, &mut refusals);
            if !refusals.is_empty() {
                return Err(refusals);
            }
            Ok(QuerySplit {
                scoring: combine(scoring),
                filter: combine(filter),
            })
        }
    }
}

/// The single key and its value of a node that has exactly one key.
fn op_and_value(node: &Value) -> Option<(&str, &Value)> {
    let obj = node.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    let (key, value) = obj.iter().next()?;
    Some((key.as_str(), value))
}

/// Classifies a subtree. Every invalid node in it adds one refusal, in
/// document order. `None` means the subtree has a refusal.
fn classify(node: &Value, path: &str, refusals: &mut Vec<String>) -> Option<Kind> {
    let Some((key, value)) = op_and_value(node) else {
        refusals.push(format!("{path}: not a query node"));
        return None;
    };
    match key {
        _ if SCORING_ROOTS.contains(&key) => Some(Kind::Scoring),
        _ if FILTER_ROOTS.contains(&key) => Some(Kind::Filter),
        "not" => classify(value, &format!("{path}.not"), refusals),
        "and" | "or" => {
            let Some(children) = value.as_array() else {
                refusals.push(format!("{path}: not a query node"));
                return None;
            };
            if children.is_empty() {
                refusals.push(format!(
                    r#"{path}: empty "and" or "or" cannot be classified"#
                ));
                return None;
            }
            // Every child is classified, so every refusal is reported. The first
            // collect runs every `classify`; only the second may short-circuit.
            let kinds: Vec<Option<Kind>> = children
                .iter()
                .enumerate()
                .map(|(i, child)| classify(child, &format!("{path}.{key}[{i}]"), refusals))
                .collect();
            let kinds: Option<Vec<Kind>> = kinds.into_iter().collect();
            Some(combine_kinds(&kinds?))
        }
        _ => {
            refusals.push(format!("{path}: not a query node"));
            None
        }
    }
}

/// The kind of an `and` or `or` from the kinds of its children.
fn combine_kinds(kinds: &[Kind]) -> Kind {
    if kinds.iter().all(|kind| *kind == Kind::Scoring) {
        Kind::Scoring
    } else if kinds.iter().all(|kind| *kind == Kind::Filter) {
        Kind::Filter
    } else {
        Kind::Mixed
    }
}

/// Splits the children of an `and` by kind. A mixed `and` child is flattened.
/// A mixed `or` or `not` child is refused. Every such child is reported.
fn split_and(
    children: &[Value],
    base: &str,
    scoring: &mut Vec<Value>,
    filter: &mut Vec<Value>,
    refusals: &mut Vec<String>,
) {
    for (i, child) in children.iter().enumerate() {
        let path = format!("{base}.and[{i}]");
        let Some(kind) = classify(child, &path, refusals) else {
            continue;
        };
        match kind {
            Kind::Scoring => scoring.push(child.clone()),
            Kind::Filter => filter.push(child.clone()),
            Kind::Mixed => match op_and_value(child) {
                Some(("and", value)) => {
                    if let Some(inner) = value.as_array() {
                        split_and(inner, &path, scoring, filter, refusals);
                    }
                }
                Some((op, _)) => refusals.push(refusal_mixed(&path, op)),
                None => {}
            },
        }
    }
}

fn refusal_mixed(path: &str, op: &str) -> String {
    format!(
        r#"{path}: "{op}" mixes scoring and filter nodes and cannot be split without changing the result set or the score"#
    )
}

/// One node stays as it is. More nodes form an `and` in their order.
fn combine(mut nodes: Vec<Value>) -> Option<Value> {
    match nodes.len() {
        0 => None,
        1 => nodes.pop(),
        _ => Some(json!({"and": nodes})),
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
    use super::{convert_input, convert_search_request};
    use serde_json::{json, Value};

    const CURSOR_MSG: &str = "cursor: a 0.4 cursor is not portable to 0.5 and cannot continue a 0.5 query; drop it and start from the first page of the 0.5 request";
    const COLLAPSE_MSG: &str = "collapse: must be a non-empty field name string (a 0.4 value)";
    const DUP_MSG: &str = "duplicates: the 0.4 /duplicates request has no Search v2 form; Search v2 removes /duplicates, so use a terms facet and then a term filter for each selected value (terms facets are a later milestone item, see docs/migration-search-v2.md)";

    fn q() -> Value {
        json!({"match": {"field": "bio", "text": "rust"}})
    }

    fn m() -> Value {
        q()
    }

    fn k() -> Value {
        json!({"knn": {"field": "embedding", "vector": [0.1, 0.2], "k": 10}})
    }

    fn t() -> Value {
        json!({"term": {"field": "status", "value": "active"}})
    }

    fn r() -> Value {
        json!({"range": {"field": "age", "gte": 18}})
    }

    fn refusal_mix(path: &str, op: &str) -> String {
        format!(
            r#"{path}: "{op}" mixes scoring and filter nodes and cannot be split without changing the result set or the score"#
        )
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
        assert_eq!(changes.len(), 2);
        let row = changes.iter().find(|c| c.path == "track_total").unwrap();
        assert_eq!(row.from, json!(true));
        assert_eq!(row.to, json!("exact"));
    }

    #[test]
    fn track_total_false_becomes_none() {
        let (out, changes) = ok(json!({"query": q(), "track_total": false}));
        assert_eq!(out["track_total"], "none");
        let row = changes.iter().find(|c| c.path == "track_total").unwrap();
        assert_eq!(row.to, json!("none"));
    }

    #[test]
    fn absent_track_total_stays_absent() {
        let (out, changes) = ok(json!({"query": q()}));
        assert!(out.get("track_total").is_none());
        assert!(changes.iter().all(|c| c.path != "track_total"));
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
        assert!(changes.iter().all(|c| c.path == "query"));
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
    fn missing_query_is_refused() {
        let errs = refused(json!({"limit": 5}));
        assert!(errs.contains(&"query is required".to_string()), "{errs:?}");
    }

    #[test]
    fn scoring_only_tree_goes_to_scoring() {
        for query in [m(), json!({"and": [m(), k()]}), json!({"not": m()})] {
            let (out, changes) = ok(json!({"query": query.clone()}));
            assert_eq!(out["scoring"], query);
            assert!(out.get("filter").is_none());
            assert!(out.get("query").is_none());
            assert_eq!(changes[0].path, "query");
            assert_eq!(changes[0].to, json!("scoring"));
        }
    }

    #[test]
    fn filter_only_tree_goes_to_filter() {
        for query in [
            t(),
            json!({"and": [t(), r()]}),
            json!({"or": [t(), r()]}),
            json!({"not": t()}),
        ] {
            let (out, changes) = ok(json!({"query": query.clone()}));
            assert_eq!(out["filter"], query);
            assert!(out.get("scoring").is_none());
            assert!(out.get("query").is_none());
            assert_eq!(changes[0].path, "query");
            assert_eq!(changes[0].to, json!("filter"));
        }
    }

    #[test]
    fn and_of_a_mix_is_split() {
        let (out, changes) = ok(json!({"query": {"and": [m(), t(), r()]}}));
        assert_eq!(out["scoring"], m());
        assert_eq!(out["filter"], json!({"and": [t(), r()]}));
        assert!(out.get("query").is_none());
        assert_eq!(changes[0].to, json!("scoring+filter"));
    }

    #[test]
    fn and_of_a_mix_keeps_the_order_of_each_side() {
        let (out, _) = ok(json!({"query": {"and": [t(), m(), r(), k()]}}));
        assert_eq!(out["scoring"], json!({"and": [m(), k()]}));
        assert_eq!(out["filter"], json!({"and": [t(), r()]}));
    }

    #[test]
    fn nested_and_is_flattened() {
        let (out, _) = ok(json!({"query": {"and": [m(), {"and": [t(), r()]}]}}));
        assert_eq!(out["scoring"], m());
        assert_eq!(out["filter"], json!({"and": [t(), r()]}));
    }

    #[test]
    fn or_of_a_mix_is_refused_with_its_path() {
        // Negative control: fails if an `or` of a mix is silently split.
        let result = convert_search_request(&json!({"query": {"or": [m(), t()]}}));
        assert!(result.is_err());
        let errs = refused(json!({"query": {"or": [m(), t()]}}));
        assert_eq!(errs, vec![refusal_mix("query", "or")]);
    }

    #[test]
    fn not_of_a_mix_is_refused_with_its_path() {
        let errs = refused(json!({"query": {"not": {"and": [m(), t()]}}}));
        assert_eq!(errs, vec![refusal_mix("query", "not")]);
    }

    #[test]
    fn refused_node_path_is_exact_in_a_nested_tree() {
        let errs = refused(json!({"query": {"and": [m(), {"and": [t(), {"or": [m(), t()]}]}]}}));
        assert_eq!(errs, vec![refusal_mix("query.and[1].and[1]", "or")]);
    }

    #[test]
    fn every_refused_node_is_reported() {
        let query = json!({"and": [{"or": [m(), t()]}, {"not": {"and": [k(), r()]}}, t()]});
        let errs = refused(json!({"query": query}));
        assert_eq!(
            errs,
            vec![
                refusal_mix("query.and[0]", "or"),
                refusal_mix("query.and[1]", "not"),
            ]
        );
    }

    #[test]
    fn every_invalid_node_is_reported() {
        let query = json!({"and": [{"foo": 1}, m(), {"or": [t(), 5]}]});
        let errs = refused(json!({"query": query}));
        assert_eq!(
            errs,
            vec![
                "query.and[0]: not a query node".to_string(),
                "query.and[2].or[1]: not a query node".to_string(),
            ]
        );
    }

    #[test]
    fn an_or_that_holds_a_mixed_and_is_refused() {
        let errs = refused(json!({"query": {"or": [t(), {"and": [m(), t()]}]}}));
        assert_eq!(errs, vec![refusal_mix("query", "or")]);
    }

    #[test]
    fn invalid_nodes_are_refused() {
        let cases = [
            (json!({}), "query: not a query node"),
            (json!({"foo": 1}), "query: not a query node"),
            (json!({"match": {}, "term": {}}), "query: not a query node"),
            (json!(5), "query: not a query node"),
            (
                json!({"and": []}),
                r#"query: empty "and" or "or" cannot be classified"#,
            ),
            (json!({"and": {"match": 1}}), "query: not a query node"),
        ];
        for (query, message) in cases {
            let errs = refused(json!({"query": query}));
            assert!(errs.contains(&message.to_string()), "{message}: {errs:?}");
        }
    }

    #[test]
    fn leaf_insides_are_never_split() {
        let rrf = json!({"rrf": {"queries": [{"and": [k(), t()]}, m()], "k": 60}});
        let (out, _) = ok(json!({"query": rrf.clone()}));
        assert_eq!(out["scoring"], rrf);
        let has_child =
            json!({"has_child": {"collection": "c", "field": "f", "query": {"or": [m(), t()]}}});
        let (out, _) = ok(json!({"query": has_child.clone()}));
        assert_eq!(out["filter"], has_child);
    }

    #[test]
    fn query_key_is_never_written() {
        let cases = [
            m(),
            json!({"and": [m(), t(), r()]}),
            json!({"and": [t(), r()]}),
            json!({"rrf": {"queries": [{"and": [k(), t()]}, m()], "k": 60}}),
        ];
        for query in cases {
            let (out, _) = ok(json!({"query": query}));
            assert!(out.get("query").is_none());
        }
    }

    #[test]
    fn cursor_and_unknown_keys_are_refused_together() {
        let errs = refused(json!({"query": q(), "cursor": "x", "zzz": 1}));
        assert_eq!(errs.len(), 2);
        assert!(errs.contains(&CURSOR_MSG.to_string()));
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
        let (out_mix, _) = ok(json!({
            "track_total": true,
            "limit": 2,
            "query": {"and": [m(), t()]}
        }));
        let keys: Vec<&str> = out_mix
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, ["scoring", "filter", "limit", "track_total"]);
    }

    #[test]
    fn top_level_not_an_object_is_refused() {
        assert_eq!(
            refused(json!([])),
            vec!["request must be a JSON object".to_string()]
        );
    }

    fn input_ok(input: Value) -> (Value, Vec<super::Change>) {
        convert_input(&input).expect("conversion should succeed")
    }

    fn input_refused(input: Value) -> Vec<String> {
        convert_input(&input).expect_err("conversion should be refused")
    }

    fn or_mix() -> Value {
        json!({"or": [m(), t()]})
    }

    fn batch(items: Vec<Value>) -> Value {
        json!({"searches": items})
    }

    fn three_items() -> Vec<Value> {
        vec![
            json!({"collection": "a", "query": m(), "track_total": true}),
            json!({"collection": "b", "query": t(), "limit": 5}),
            json!({"collection": "c", "query": {"and": [m(), t()]}, "collapse": "k"}),
        ]
    }

    #[test]
    fn legacy_cursor_is_refused_with_the_reason() {
        assert_eq!(
            input_refused(json!({"query": q(), "cursor": "abc"})),
            vec![CURSOR_MSG.to_string()]
        );
    }

    #[test]
    fn null_cursor_and_null_collapse_are_ignored() {
        let (out, _) = input_ok(json!({"query": q(), "cursor": null, "collapse": null}));
        assert!(out.get("cursor").is_none());
        assert!(out.get("collapse").is_none());
    }

    #[test]
    fn collapse_string_becomes_an_object() {
        let (out, changes) = input_ok(json!({"query": q(), "collapse": "category"}));
        assert_eq!(out["collapse"], json!({"field": "category"}));
        let row = changes
            .iter()
            .find(|c| c.path == "collapse")
            .expect("a collapse change row");
        assert_eq!(row.from, json!("category"));
        assert_eq!(row.to, json!({"field": "category"}));
        let last = out.as_object().unwrap().keys().next_back().cloned();
        assert_eq!(last.as_deref(), Some("collapse"));
    }

    #[test]
    fn collapse_bad_values_are_refused() {
        for bad in [json!(""), json!(5), json!({"field": "x"}), json!(true)] {
            assert_eq!(
                input_refused(json!({"query": q(), "collapse": bad})),
                vec![COLLAPSE_MSG.to_string()]
            );
        }
    }

    #[test]
    fn duplicates_request_is_refused_with_the_note() {
        assert_eq!(
            input_refused(
                json!({"field": "email", "min_group_size": 2, "limit": 100, "offset": 0})
            ),
            vec![DUP_MSG.to_string()]
        );
        assert_eq!(
            input_refused(json!({"field": "email"})),
            vec![DUP_MSG.to_string()]
        );
    }

    #[test]
    fn a_search_request_with_a_field_key_is_not_a_duplicates_request() {
        assert_eq!(
            input_refused(json!({"field": "email", "query": m()})),
            vec![r#"unknown field "field""#.to_string()]
        );
    }

    #[test]
    fn batch_of_three_all_convert() {
        let (out, changes) = input_ok(batch(three_items()));
        let items = out["searches"].as_array().expect("searches is an array");
        assert_eq!(items.len(), 3);
        assert_eq!(
            items[0],
            json!({"collection": "a", "scoring": m(), "track_total": "exact"})
        );
        assert_eq!(
            items[1],
            json!({"collection": "b", "filter": t(), "limit": 5})
        );
        assert_eq!(items[2]["scoring"], m());
        assert_eq!(items[2]["filter"], t());
        assert_eq!(items[2]["collapse"], json!({"field": "k"}));
        for item in items {
            let first = item.as_object().unwrap().keys().next().cloned();
            assert_eq!(first.as_deref(), Some("collection"));
        }
        let paths: Vec<&str> = changes.iter().map(|c| c.path.as_str()).collect();
        assert!(paths.contains(&"searches[0].track_total"));
        assert!(paths.contains(&"searches[2].collapse"));
    }

    #[test]
    fn batch_with_one_refused_item_refuses_the_whole_batch() {
        let mut items = three_items();
        items[1] = json!({"collection": "b", "query": or_mix()});
        let result = convert_input(&batch(items));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            vec![refusal_mix("searches[1].query", "or")]
        );
    }

    #[test]
    fn batch_reports_every_refused_item() {
        let items = vec![
            json!({"collection": "a", "query": or_mix()}),
            json!({"collection": "b", "query": t()}),
            json!({"collection": "c", "query": m(), "cursor": "x"}),
        ];
        let errs = input_refused(batch(items));
        assert_eq!(errs.len(), 2);
        assert!(errs[0].starts_with("searches[0].query:"), "{errs:?}");
        assert!(errs[1].starts_with("searches[2].cursor:"), "{errs:?}");
    }

    #[test]
    fn batch_bad_shapes_are_refused() {
        assert_eq!(
            input_refused(json!({"searches": 5})),
            vec!["searches: must be an array".to_string()]
        );
        assert_eq!(
            input_refused(json!({"searches": [5]})),
            vec!["searches[0]: not an object".to_string()]
        );
        assert_eq!(
            input_refused(json!({"searches": [{"query": m()}]})),
            vec!["searches[0]: collection is missing or not a string".to_string()]
        );
        assert_eq!(
            input_refused(json!({"searches": [], "x": 1})),
            vec![r#"unknown field "x""#.to_string()]
        );
    }

    #[test]
    fn batch_of_more_than_32_items_is_refused() {
        let items = |n: usize| -> Vec<Value> {
            (0..n)
                .map(|i| json!({"collection": format!("c{i}"), "query": m()}))
                .collect()
        };
        assert_eq!(
            input_refused(batch(items(33))),
            vec!["searches: at most 32 items".to_string()]
        );
        let (out, _) = input_ok(batch(items(32)));
        assert_eq!(out["searches"].as_array().unwrap().len(), 32);
    }

    #[test]
    fn batch_item_order_is_kept() {
        let items: Vec<Value> = ["c", "a", "b"]
            .iter()
            .map(|c| json!({"collection": c, "query": m()}))
            .collect();
        let (out, _) = input_ok(batch(items));
        let names: Vec<&str> = out["searches"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["collection"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["c", "a", "b"]);
    }
}
