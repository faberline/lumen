//! The `collection_lifecycle` functional phase (#259): create, extend, index every field
//! kind, clear, drop and recreate a collection over HTTP. A recreated collection must never
//! show an earlier generation.

use std::path::Path;

use anyhow::{bail, Result};
use serde_json::{json, Value};

use crate::http::HttpApi;
use crate::run::Phase;

pub(super) struct CollectionLifecycle;

impl Phase for CollectionLifecycle {
    fn name(&self) -> &str {
        "collection_lifecycle"
    }

    fn run(&self, endpoint: &str, _artifact_dir: &Path) -> Result<()> {
        let api = HttpApi::new(endpoint)?;
        run_lifecycle(
            &api,
            &format!("acceptance-lifecycle-{}", std::process::id()),
        )
    }
}

fn hit_ids(hits: &Value) -> Vec<&str> {
    hits["hits"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|hit| hit["external_id"].as_str())
                .collect()
        })
        .unwrap_or_default()
}

/// Fails when the search response contains any hit.
pub(super) fn assert_no_hits(hits: &Value, what: &str) -> Result<()> {
    let ids = hit_ids(hits);
    if !ids.is_empty() {
        bail!("{what}: expected no hits, found {ids:?}");
    }
    Ok(())
}

/// Fails when the search response does not contain a hit with `external_id == id`.
pub(super) fn assert_hit(hits: &Value, id: &str, what: &str) -> Result<()> {
    if !hit_ids(hits).contains(&id) {
        bail!("{what}: expected a hit for `{id}`, found {hits}");
    }
    Ok(())
}

fn search(api: &HttpApi, base: &str, query: Value) -> Result<Value> {
    api.expect(
        "search",
        &[200],
        "POST",
        &format!("{base}/search"),
        Some(&json!({"query": query, "limit": 10})),
    )
}

fn index(api: &HttpApi, base: &str, what: &str, items: Value) -> Result<()> {
    api.expect(
        what,
        &[200],
        "POST",
        &format!("{base}/index"),
        Some(&json!({"items": items})),
    )?;
    Ok(())
}

fn documents_indexed(api: &HttpApi, base: &str) -> Result<Value> {
    let stats = api.expect("stats", &[200], "GET", &format!("{base}/stats"), None)?;
    Ok(stats["documents_indexed"].clone())
}

/// The lifecycle steps of #259 section 3, in order.
pub(super) fn run_lifecycle(api: &HttpApi, collection: &str) -> Result<()> {
    let base = format!("/collections/{collection}");
    let drop_path = format!("{base}?force=true");

    // 1. Best effort: a leftover from an earlier run is removed; 204 or 404 both mean it is gone.
    let _ = api.call("DELETE", &drop_path, None);

    // 2. Create with the first schema.
    api.expect("create", &[200], "PUT", &base, Some(&schema_v1()))?;

    // 3. Extend with a superset that adds text, flat vector and HNSW vector fields.
    api.expect("extend", &[200], "PUT", &base, Some(&schema_v2()))?;

    // 4. Every field kind is accepted and stored; each searchable field finds doc-1.
    index(
        api,
        &base,
        "index every field kind",
        json!([
            {"external_id": "doc-1", "field": "kw", "value": "red"},
            {"external_id": "doc-1", "field": "num", "value": 7.0},
            {"external_id": "doc-1", "field": "tags", "value": ["a", "b"]},
            {"external_id": "doc-1", "field": "fp", "value": "0000000000000001"},
            {"external_id": "doc-1", "field": "body", "value": "hello lumen"},
            {"external_id": "doc-1", "field": "vec_flat", "value": [1.0, 0.0]},
            {"external_id": "doc-1", "field": "vec_hnsw", "value": [0.0, 1.0]}
        ]),
    )?;
    let count = documents_indexed(api, &base)?;
    anyhow::ensure!(count == json!(1), "documents_indexed after index: {count}");
    let probes = [
        ("kw", json!({"term": {"field": "kw", "value": "red"}})),
        (
            "num",
            json!({"range": {"field": "num", "gte": 7.0, "lte": 7.0}}),
        ),
        ("tags", json!({"term": {"field": "tags", "value": "a"}})),
        ("body", json!({"match": {"field": "body", "text": "hello"}})),
        (
            "vec_flat",
            json!({"knn": {"field": "vec_flat", "vector": [1.0, 0.0], "k": 1}}),
        ),
        (
            "vec_hnsw",
            json!({"knn": {"field": "vec_hnsw", "vector": [0.0, 1.0], "k": 1}}),
        ),
    ];
    for (field, query) in probes {
        let hits = search(api, &base, query)?;
        assert_hit(&hits, "doc-1", &format!("search on {field}"))?;
    }

    // 5. Clear: no document survives, and the term search for red finds nothing.
    api.expect(
        "clear",
        &[204],
        "POST",
        &format!("{base}/docs:truncate"),
        None,
    )?;
    let count = documents_indexed(api, &base)?;
    anyhow::ensure!(count == json!(0), "documents_indexed after clear: {count}");
    let hits = search(api, &base, json!({"term": {"field": "kw", "value": "red"}}))?;
    assert_no_hits(&hits, "kw red after clear")?;

    // 6. A document of the old generation that must not come back after recreate.
    index(
        api,
        &base,
        "index doc-2",
        json!([{"external_id": "doc-2", "field": "kw", "value": "blue"}]),
    )?;

    // 7. Drop; the collection is gone.
    api.expect("drop", &[204], "DELETE", &drop_path, None)?;
    api.expect(
        "stats after drop",
        &[404],
        "GET",
        &format!("{base}/stats"),
        None,
    )?;

    // 8. Recreate with the first schema: no earlier generation shows.
    api.expect("recreate", &[200], "PUT", &base, Some(&schema_v1()))?;
    let count = documents_indexed(api, &base)?;
    anyhow::ensure!(
        count == json!(0),
        "documents_indexed after recreate: {count}"
    );
    let hits = search(
        api,
        &base,
        json!({"term": {"field": "kw", "value": "blue"}}),
    )?;
    assert_no_hits(
        &hits,
        "recreated collection shows an earlier generation (blue)",
    )?;
    let hits = search(api, &base, json!({"term": {"field": "kw", "value": "red"}}))?;
    assert_no_hits(
        &hits,
        "recreated collection shows an earlier generation (red)",
    )?;

    // 9. Final cleanup.
    api.expect("cleanup", &[204], "DELETE", &drop_path, None)?;
    Ok(())
}

fn schema_v1() -> Value {
    json!({"fields": {
        "kw": {"type": "keyword"},
        "num": {"type": "number"},
        "tags": {"type": "set"},
        "fp": {"type": "hash"}
    }})
}

fn schema_v2() -> Value {
    json!({"fields": {
        "kw": {"type": "keyword"},
        "num": {"type": "number"},
        "tags": {"type": "set"},
        "fp": {"type": "hash"},
        "body": {"type": "text", "analyzer": "ngram"},
        "vec_flat": {"type": "vector", "dim": 2, "metric": "cosine", "backend": "flat-cpu"},
        "vec_hnsw": {"type": "vector", "dim": 2, "metric": "cosine", "backend": "hnsw-cpu"}
    }})
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use axum_test::{TestServer, TestServerConfig, Transport};
    use lumen::api::{router, AppState};
    use lumen::storage::Engine;
    use serde_json::json;

    use super::{assert_hit, assert_no_hits, run_lifecycle, CollectionLifecycle};
    use crate::http::HttpApi;
    use crate::run::Phase;

    fn open_server() -> TestServer {
        let config = TestServerConfig {
            transport: Some(Transport::HttpRandomPort),
            ..Default::default()
        };
        TestServer::new_with_config(router(AppState::open(Arc::new(Engine::new()))), config)
            .expect("server")
    }

    fn schema_v1() -> serde_json::Value {
        json!({"fields": {
            "kw": {"type": "keyword"},
            "num": {"type": "number"},
            "tags": {"type": "set"},
            "fp": {"type": "hash"}
        }})
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_passes_against_a_real_server() {
        let server = open_server();
        let url = server.server_address().expect("address").to_string();
        let result =
            tokio::task::spawn_blocking(move || CollectionLifecycle.run(&url, Path::new(".")))
                .await
                .expect("join");
        result.expect("collection lifecycle passes");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recreated_collection_never_shows_an_earlier_generation() {
        let server = open_server();
        let url = server.server_address().expect("address").to_string();
        let result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let api = HttpApi::new(&url)?;
            let id = "acceptance-negative";
            let base = format!("/collections/{id}");
            // The lifecycle itself leaves nothing behind.
            run_lifecycle(&api, "acceptance-negative-lifecycle")?;

            api.expect("create", &[200], "PUT", &base, Some(&schema_v1()))?;
            api.expect(
                "index ghost",
                &[200],
                "POST",
                &format!("{base}/index"),
                Some(
                    &json!({"items": [{"external_id": "doc-9", "field": "kw", "value": "ghost"}]}),
                ),
            )?;
            api.expect(
                "drop",
                &[204],
                "DELETE",
                &format!("{base}?force=true"),
                None,
            )?;
            api.expect("recreate", &[200], "PUT", &base, Some(&schema_v1()))?;

            let stats = api.expect("stats", &[200], "GET", &format!("{base}/stats"), None)?;
            anyhow::ensure!(
                stats["documents_indexed"] == json!(0),
                "stats after recreate: {stats}"
            );
            let hits = api.expect(
                "search ghost",
                &[200],
                "POST",
                &format!("{base}/search"),
                Some(&json!({"query": {"term": {"field": "kw", "value": "ghost"}}, "limit": 10})),
            )?;
            assert_no_hits(&hits, "ghost after recreate")?;
            api.expect(
                "cleanup",
                &[204],
                "DELETE",
                &format!("{base}?force=true"),
                None,
            )?;
            Ok(())
        })
        .await
        .expect("join");
        result.expect("no earlier generation shows");
    }

    #[test]
    fn check_fails_when_a_document_survives_clear() {
        let hits = json!({"hits": [{"external_id": "x", "score": 1.0}]});
        let err = assert_no_hits(&hits, "kw red after clear").expect_err("a hit survives");
        assert!(format!("{err:#}").contains("kw red after clear"));
        assert!(assert_no_hits(&json!({"hits": []}), "kw red after clear").is_ok());
    }

    #[test]
    fn assert_hit_requires_the_named_document() {
        let hits = json!({"hits": [{"external_id": "x", "score": 1.0}]});
        let err = assert_hit(&hits, "doc-1", "kw red").expect_err("doc-1 is missing");
        assert!(format!("{err:#}").contains("kw red"));
        assert!(assert_hit(&json!({"hits": []}), "doc-1", "kw red").is_err());
        let found = json!({"hits": [{"external_id": "doc-1", "score": 1.0}]});
        assert!(assert_hit(&found, "doc-1", "kw red").is_ok());
    }
}
