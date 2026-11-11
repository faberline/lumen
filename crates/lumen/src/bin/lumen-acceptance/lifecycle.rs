//! The `collection_lifecycle` functional phase: create, extend, clear, drop and
//! recreate a collection over HTTP. It proves that a recreated collection never shows
//! an earlier generation.

use std::path::Path;

use anyhow::{anyhow, bail, Result};
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
        let collection = format!("acceptance-lifecycle-{}", std::process::id());
        run_lifecycle(&api, &collection)
    }
}

/// The first schema: one field of each of the four simple kinds.
fn schema_v1() -> Value {
    json!({"fields": {
        "kw": {"type": "keyword"},
        "num": {"type": "number"},
        "tags": {"type": "set"},
        "fp": {"type": "hash"}
    }})
}

/// The extended schema: the first schema plus Text with ngram, Flat Vector and HNSW Vector.
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

/// Fails unless the search response has no hit at all.
pub(super) fn assert_no_hits(response: &Value, what: &str) -> Result<()> {
    let count = response["hits"].as_array().map_or(0, Vec::len);
    if count > 0 {
        bail!("{what}: found {count} hit(s), wanted none");
    }
    Ok(())
}

/// Fails unless the search response has a hit for the document `id`.
pub(super) fn assert_hit(response: &Value, id: &str, what: &str) -> Result<()> {
    let found = response["hits"]
        .as_array()
        .is_some_and(|hits| hits.iter().any(|hit| hit["external_id"] == id));
    if !found {
        bail!("{what}: no hit for {id}");
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

fn documents_indexed(api: &HttpApi, base: &str) -> Result<u64> {
    let stats = api.expect("stats", &[200], "GET", &format!("{base}/stats"), None)?;
    stats["documents_indexed"]
        .as_u64()
        .ok_or_else(|| anyhow!("stats has no documents_indexed: {stats}"))
}

/// Runs the lifecycle steps for one collection id. The steps are fixed by the spec.
pub(super) fn run_lifecycle(api: &HttpApi, collection: &str) -> Result<()> {
    let base = format!("/collections/{collection}");
    let index = format!("{base}/index");
    let delete = format!("{base}?force=true");

    // 1. Best effort: remove a collection left by an earlier run.
    let _ = api.call("DELETE", &delete, None);

    // 2. Create, 3. extend with the superset schema.
    api.expect("create", &[200], "PUT", &base, Some(&schema_v1()))?;
    api.expect("extend", &[200], "PUT", &base, Some(&schema_v2()))?;

    // 4. Every field kind is accepted and stored, and each searchable field finds doc-1.
    api.expect(
        "index every field kind",
        &[200],
        "POST",
        &index,
        Some(&json!({"items": [
            {"external_id": "doc-1", "field": "kw", "value": "red"},
            {"external_id": "doc-1", "field": "num", "value": 7.0},
            {"external_id": "doc-1", "field": "tags", "value": ["a", "b"]},
            {"external_id": "doc-1", "field": "fp", "value": "0000000000000001"},
            {"external_id": "doc-1", "field": "body", "value": "hello lumen"},
            {"external_id": "doc-1", "field": "vec_flat", "value": [1.0, 0.0]},
            {"external_id": "doc-1", "field": "vec_hnsw", "value": [0.0, 1.0]}
        ]})),
    )?;
    let count = documents_indexed(api, &base)?;
    if count != 1 {
        bail!("after index: documents_indexed is {count}, wanted 1");
    }
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

    // 5. Clear: the documents are gone and the keyword search finds nothing.
    api.expect(
        "clear",
        &[204],
        "POST",
        &format!("{base}/docs:truncate"),
        None,
    )?;
    let count = documents_indexed(api, &base)?;
    if count != 0 {
        bail!("after clear: documents_indexed is {count}, wanted 0");
    }
    let hits = search(api, &base, json!({"term": {"field": "kw", "value": "red"}}))?;
    assert_no_hits(&hits, "after clear, the kw search for red")?;

    // 6. A document of the old generation, which must never come back.
    api.expect(
        "index doc-2",
        &[200],
        "POST",
        &index,
        Some(&json!({"items": [{"external_id": "doc-2", "field": "kw", "value": "blue"}]})),
    )?;

    // 7. Drop: the collection is gone.
    api.expect("drop", &[204], "DELETE", &delete, None)?;
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
    if count != 0 {
        bail!("recreated collection shows an earlier generation: documents_indexed is {count}");
    }
    let blue = search(
        api,
        &base,
        json!({"term": {"field": "kw", "value": "blue"}}),
    )?;
    assert_no_hits(
        &blue,
        "recreated collection shows an earlier generation: kw blue",
    )?;
    let red = search(api, &base, json!({"term": {"field": "kw", "value": "red"}}))?;
    assert_no_hits(
        &red,
        "recreated collection shows an earlier generation: kw red",
    )?;

    // 9. Cleanup.
    api.expect("cleanup", &[204], "DELETE", &delete, None)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::tests::serve_for_test;

    #[test]
    fn check_fails_when_a_document_survives_clear() {
        let survivor = json!({"hits": [{"external_id": "x", "score": 1.0}]});
        let err = assert_no_hits(&survivor, "after clear").expect_err("a survivor is a failure");
        assert!(format!("{err:#}").contains("after clear"));
        assert_no_hits(&json!({"hits": []}), "after clear").expect("no hits passes");
    }

    #[test]
    fn assert_hit_requires_the_named_document() {
        let other = json!({"hits": [{"external_id": "x", "score": 1.0}]});
        let err = assert_hit(&other, "doc-1", "search").expect_err("doc-1 is missing");
        assert!(format!("{err:#}").contains("search"));
        let found = json!({"hits": [{"external_id": "doc-1", "score": 1.0}]});
        assert_hit(&found, "doc-1", "search").expect("doc-1 is present");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_passes_against_a_real_server() {
        let (_server, url) = serve_for_test();
        let result =
            tokio::task::spawn_blocking(move || CollectionLifecycle.run(&url, Path::new(".")))
                .await
                .expect("join");
        result.expect("every lifecycle step passes");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recreated_collection_never_shows_an_earlier_generation() {
        let (_server, url) = serve_for_test();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let api = HttpApi::new(&url)?;
            // A full run first, which must leave nothing behind.
            run_lifecycle(&api, "acceptance-ghost-first")?;

            let base = "/collections/acceptance-ghost";
            api.expect("create", &[200], "PUT", base, Some(&schema_v1()))?;
            api.expect(
                "index ghost",
                &[200],
                "POST",
                &format!("{base}/index"),
                Some(&json!({"items": [
                    {"external_id": "doc-9", "field": "kw", "value": "ghost"}
                ]})),
            )?;
            api.expect(
                "drop",
                &[204],
                "DELETE",
                &format!("{base}?force=true"),
                None,
            )?;
            api.expect("recreate", &[200], "PUT", base, Some(&schema_v1()))?;

            let count = documents_indexed(&api, base)?;
            if count != 0 {
                bail!("recreated collection shows an earlier generation: {count} document(s)");
            }
            let ghost = search(
                &api,
                base,
                json!({"term": {"field": "kw", "value": "ghost"}}),
            )?;
            assert_no_hits(
                &ghost,
                "recreated collection shows an earlier generation: ghost",
            )?;
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
        .expect("join")
        .expect("the recreated collection is empty");
    }

    #[test]
    fn schemas_name_every_field_kind() {
        let v2 = schema_v2();
        let fields = v2["fields"].as_object().expect("fields");
        assert_eq!(fields.len(), 7);
        assert_eq!(fields["body"]["analyzer"], "ngram");
        assert_eq!(fields["vec_flat"]["backend"], "flat-cpu");
        assert_eq!(fields["vec_hnsw"]["backend"], "hnsw-cpu");
        assert_eq!(schema_v1()["fields"].as_object().expect("fields").len(), 4);
    }
}
