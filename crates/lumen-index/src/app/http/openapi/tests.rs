use super::*;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FactKind {
    Status,
    Header,
    MediaType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Fact {
    pub path: &'static str,
    pub method: &'static str,
    pub kind: FactKind,
    pub value: &'static str,
}

impl Fact {
    pub const fn status(path: &'static str, method: &'static str, value: &'static str) -> Self {
        Self {
            path,
            method,
            kind: FactKind::Status,
            value,
        }
    }

    pub const fn header(path: &'static str, method: &'static str, value: &'static str) -> Self {
        Self {
            path,
            method,
            kind: FactKind::Header,
            value,
        }
    }

    pub const fn media_type(path: &'static str, method: &'static str, value: &'static str) -> Self {
        Self {
            path,
            method,
            kind: FactKind::MediaType,
            value,
        }
    }
}

pub static REACHABLE_FACTS: &[Fact] = &[
    // X-Read-Consistency request headers
    Fact::header("/collections/{collection_id}/search", "post", "X-Read-Consistency"),
    Fact::header("/collections/{collection_id}/search:all", "post", "X-Read-Consistency"),
    Fact::header("/collections:search", "post", "X-Read-Consistency"),
    Fact::header("/collections/{collection_id}/duplicates", "post", "X-Read-Consistency"),
    Fact::header("/collections/{collection_id}", "query", "X-Read-Consistency"),
    Fact::header("/collections", "query", "X-Read-Consistency"),

    // Shared responses under components.responses
    Fact::status("#/components/responses/401", "", "401"),
    Fact::status("#/components/responses/413", "", "413"),
    Fact::status("#/components/responses/429", "", "429"),
    Fact::status("#/components/responses/500", "", "500"),
    Fact::header("#/components/responses/429", "", "Retry-After"),

    // Reindex stream media types
    Fact::media_type("/collections/{collection_id}/reindex/stream", "post", "application/x-ndjson"),

    // Write operations mapping BulkLimit (413) and/or PendingChangeCapacity (429)
    Fact::status("/collections/{collection_id}/index", "post", "413"),
    Fact::status("/collections/{collection_id}/index", "post", "429"),
    Fact::status("/collections/{collection_id}/docs:replace", "put", "413"),
    Fact::status("/collections/{collection_id}/docs:replace", "put", "429"),
    Fact::status("/collections/{collection_id}/docs/{external_id}", "put", "429"),
    Fact::status("/collections/{collection_id}/index/{external_id}", "delete", "429"),
    Fact::status("/collections/{collection_id}/docs/{external_id}", "delete", "429"),
    Fact::status("/collections/{collection_id}/docs:truncate", "post", "429"),
    Fact::status("/collections/{collection_id}/docs:unindex", "post", "429"),

    // BearerAuth operations referencing shared 401 and 500
    Fact::status("/admin/backup", "get", "401"),
    Fact::status("/admin/backup", "get", "500"),
    Fact::status("/admin/backup/local", "post", "401"),
    Fact::status("/admin/backup/local", "post", "500"),
    Fact::status("/admin/backup:scoped", "post", "401"),
    Fact::status("/admin/backup:scoped", "post", "500"),
    Fact::status("/admin/checkpoint", "post", "401"),
    Fact::status("/admin/checkpoint", "post", "500"),
    Fact::status("/admin/reshard:apply", "post", "401"),
    Fact::status("/admin/reshard:apply", "post", "500"),
    Fact::status("/admin/reshard:evict", "post", "401"),
    Fact::status("/admin/reshard:evict", "post", "500"),
    Fact::status("/admin/reshard:fence", "post", "401"),
    Fact::status("/admin/reshard:fence", "post", "500"),
    Fact::status("/admin/reshard:prune", "post", "401"),
    Fact::status("/admin/reshard:prune", "post", "500"),
    Fact::status("/admin/restart:seal-hnsw-cache", "post", "401"),
    Fact::status("/admin/restart:seal-hnsw-cache", "post", "500"),
    Fact::status("/admin/restore", "post", "401"),
    Fact::status("/admin/restore", "post", "500"),
    Fact::status("/collections", "get", "401"),
    Fact::status("/collections", "get", "500"),
    Fact::status("/collections", "query", "401"),
    Fact::status("/collections", "query", "500"),
    Fact::status("/collections/{collection_id}", "put", "401"),
    Fact::status("/collections/{collection_id}", "put", "500"),
    Fact::status("/collections/{collection_id}", "delete", "401"),
    Fact::status("/collections/{collection_id}", "delete", "500"),
    Fact::status("/collections/{collection_id}", "query", "401"),
    Fact::status("/collections/{collection_id}", "query", "500"),
    Fact::status("/collections/{collection_id}/docs/{external_id}", "put", "401"),
    Fact::status("/collections/{collection_id}/docs/{external_id}", "put", "500"),
    Fact::status("/collections/{collection_id}/docs/{external_id}", "delete", "401"),
    Fact::status("/collections/{collection_id}/docs/{external_id}", "delete", "500"),
    Fact::status("/collections/{collection_id}/docs:replace", "put", "401"),
    Fact::status("/collections/{collection_id}/docs:replace", "put", "500"),
    Fact::status("/collections/{collection_id}/docs:truncate", "post", "401"),
    Fact::status("/collections/{collection_id}/docs:truncate", "post", "500"),
    Fact::status("/collections/{collection_id}/docs:unindex", "post", "401"),
    Fact::status("/collections/{collection_id}/docs:unindex", "post", "500"),
    Fact::status("/collections/{collection_id}/duplicates", "post", "401"),
    Fact::status("/collections/{collection_id}/duplicates", "post", "500"),
    Fact::status("/collections/{collection_id}/fields/{field_name}", "delete", "401"),
    Fact::status("/collections/{collection_id}/fields/{field_name}", "delete", "500"),
    Fact::status("/collections/{collection_id}/index", "post", "401"),
    Fact::status("/collections/{collection_id}/index", "post", "500"),
    Fact::status("/collections/{collection_id}/index/{external_id}", "delete", "401"),
    Fact::status("/collections/{collection_id}/index/{external_id}", "delete", "500"),
    Fact::status("/collections/{collection_id}/reindex/stream", "post", "401"),
    Fact::status("/collections/{collection_id}/reindex/stream", "post", "500"),
    Fact::status("/collections/{collection_id}/search", "post", "401"),
    Fact::status("/collections/{collection_id}/search", "post", "500"),
    Fact::status("/collections/{collection_id}/search:all", "post", "401"),
    Fact::status("/collections/{collection_id}/search:all", "post", "500"),
    Fact::status("/collections/{collection_id}/stats", "get", "401"),
    Fact::status("/collections/{collection_id}/stats", "get", "500"),
    Fact::status("/collections:search", "post", "401"),
    Fact::status("/collections:search", "post", "500"),
];

fn fact_present(doc_val: &Value, fact: &Fact) -> bool {
    if fact.path.starts_with("#/components/responses/") || fact.path.is_empty() {
        let code = if fact.path.is_empty() {
            fact.method
        } else {
            fact.path.rsplit('/').next().unwrap_or(fact.method)
        };
        let resp = &doc_val["components"]["responses"][code];
        if resp.is_null() {
            return false;
        }
        return match fact.kind {
            FactKind::Status => true,
            FactKind::Header => !resp["headers"][fact.value].is_null(),
            FactKind::MediaType => !resp["content"][fact.value].is_null(),
        };
    }

    let op = &doc_val["paths"][fact.path][fact.method.to_ascii_lowercase()];
    if op.is_null() {
        return false;
    }

    match fact.kind {
        FactKind::Status => !op["responses"][fact.value].is_null(),
        FactKind::Header => {
            if let Some(params) = op["parameters"].as_array() {
                params.iter().any(|p| {
                    p["in"].as_str() == Some("header")
                        && p["name"]
                            .as_str()
                            .map(|n| n.eq_ignore_ascii_case(fact.value))
                            == Some(true)
                })
            } else {
                false
            }
        }
        FactKind::MediaType => {
            let in_req = op["requestBody"]["content"][fact.value].is_object();
            let in_res = op["responses"]["200"]["content"][fact.value].is_object();
            in_req && in_res
        }
    }
}

pub fn missing_facts(doc: &utoipa::openapi::OpenApi) -> Vec<Fact> {
    let doc_val = serde_json::to_value(doc).expect("doc serializes to JSON");
    REACHABLE_FACTS
        .iter()
        .copied()
        .filter(|fact| !fact_present(&doc_val, fact))
        .collect()
}

#[test]
fn openapi_declares_all_reachable_facts() {
    let doc = openapi();
    let missing = missing_facts(&doc);
    assert!(
        missing.is_empty(),
        "openapi() is missing reachable facts: {missing:#?}"
    );
}

#[test]
fn parity_negative_control_reports_missing_retry_after() {
    let mut doc = openapi();
    if let Some(utoipa::openapi::RefOr::T(resp)) = doc
        .components
        .as_mut()
        .and_then(|c| c.responses.get_mut("429"))
    {
        resp.headers.remove("Retry-After");
    } else {
        panic!("shared 429 response is not present or not an inline response");
    }

    let missing = missing_facts(&doc);
    assert_eq!(
        missing,
        vec![Fact::header("#/components/responses/429", "", "Retry-After")],
        "negative control did not report exactly the missing Retry-After header on shared 429"
    );
}
