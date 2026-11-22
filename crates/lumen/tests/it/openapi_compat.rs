//! 1.0.0 compatibility check of the HTTP surface. The committed baseline
//! lists every operation of `lumen spec --format openapi`, with its status
//! codes. An operation or a status code that the baseline lists must still be
//! in the live spec. Added operations are allowed.

use lumen::spec::openapi_json;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const BASELINE: &str = include_str!("../openapi-operations-baseline.txt");
const METHODS: [&str; 9] = [
    "get", "put", "post", "delete", "patch", "head", "options", "trace", "query",
];

/// Key: `METHOD /path operationId`. Value: the status codes of the operation.
type Operations = BTreeMap<String, BTreeSet<String>>;

fn operations(spec: &Value) -> Operations {
    let mut out = Operations::new();
    let paths = spec["paths"].as_object().expect("paths is an object");
    for (path, item) in paths {
        for method in METHODS {
            let Some(op) = item[method].as_object() else {
                continue;
            };
            let operation_id = op.get("operationId").and_then(Value::as_str).unwrap_or("");
            let key = format!("{} {} {}", method.to_uppercase(), path, operation_id);
            let statuses: BTreeSet<String> = op
                .get("responses")
                .and_then(Value::as_object)
                .map(|responses| responses.keys().cloned().collect())
                .unwrap_or_default();
            out.insert(key, statuses);
        }
    }
    out
}

fn parse_baseline(text: &str) -> Operations {
    let mut out = Operations::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let (key, statuses) = line
            .rsplit_once(' ')
            .expect("baseline line ends with a status list");
        out.insert(
            key.to_string(),
            statuses.split(',').map(str::to_string).collect(),
        );
    }
    out
}

/// Breaking changes of `live` against `baseline`. An empty vector means compatible.
fn breaking(baseline: &Operations, live: &Operations) -> Vec<String> {
    let mut out = Vec::new();
    for (key, statuses) in baseline {
        match live.get(key) {
            None => out.push(format!("removed or renamed operation: {key}")),
            Some(live_statuses) => {
                for status in statuses.difference(live_statuses) {
                    out.push(format!("removed status {status} on {key}"));
                }
            }
        }
    }
    out
}

fn live_operations() -> Operations {
    operations(&serde_json::from_str(&openapi_json()).expect("openapi is valid JSON"))
}

#[test]
fn baseline_operations_survive() {
    let baseline = parse_baseline(BASELINE);
    assert!(
        baseline.len() >= 30,
        "baseline has {} operations, expected at least 30",
        baseline.len()
    );
    let found = breaking(&baseline, &live_operations());
    assert!(
        found.is_empty(),
        "breaking changes against the 1.0.0 baseline:\n{}",
        found.join("\n")
    );
}

#[test]
fn removing_an_operation_is_detected() {
    let baseline = parse_baseline(BASELINE);
    let mut live = live_operations();
    assert!(live.remove("GET /admin/backup backup").is_some());
    assert_eq!(
        breaking(&baseline, &live),
        vec!["removed or renamed operation: GET /admin/backup backup".to_string()]
    );
}

#[test]
fn removing_a_status_code_is_detected() {
    let baseline = parse_baseline(BASELINE);
    let mut live = live_operations();
    let statuses = live
        .get_mut("GET /admin/backup backup")
        .expect("operation is in the live spec");
    assert!(statuses.remove("403"));
    assert_eq!(
        breaking(&baseline, &live),
        vec!["removed status 403 on GET /admin/backup backup".to_string()]
    );
}

#[test]
fn adding_an_operation_is_allowed() {
    let baseline = parse_baseline(BASELINE);
    let mut live = live_operations();
    live.insert(
        "GET /new-thing new_thing".to_string(),
        BTreeSet::from(["200".to_string()]),
    );
    assert!(breaking(&baseline, &live).is_empty());
}

#[test]
fn baseline_is_sorted_and_unique() {
    let lines: Vec<&str> = BASELINE.lines().filter(|line| !line.is_empty()).collect();
    assert!(
        lines.windows(2).all(|pair| pair[0] < pair[1]),
        "baseline lines must be strictly increasing"
    );
}
