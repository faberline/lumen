//! The `writes_and_masking` functional phase: partial merge, complete replacement,
//! single and batch unindex, and complete clear over HTTP. It proves that an update or
//! a delete masks older values, also those in an older (checkpointed) layer.

use std::path::Path;

use anyhow::{bail, Result};
use serde_json::{json, Value};

use crate::http::HttpApi;
use crate::lifecycle::{assert_hit, assert_no_hits, documents_indexed, search};
use crate::run::Phase;

pub(super) struct WritesAndMasking;

impl Phase for WritesAndMasking {
    fn name(&self) -> &str {
        "writes_and_masking"
    }

    fn run(&self, endpoint: &str, _artifact_dir: &Path) -> Result<()> {
        let api = HttpApi::new(endpoint)?;
        let collection = format!("acceptance-writes-{}", std::process::id());
        run_writes(&api, &collection)
    }
}

/// The schema for the writes phase: one keyword field pair and one number field.
fn schema() -> Value {
    json!({"fields": {"kw": {"type": "keyword"}, "num": {"type": "number"}, "tag": {"type": "keyword"}}})
}

/// Fails when the search response still has a hit for the document `id`.
pub(super) fn assert_absent(response: &Value, id: &str, what: &str) -> Result<()> {
    let found = response["hits"]
        .as_array()
        .is_some_and(|hits| hits.iter().any(|hit| hit["external_id"] == id));
    if found {
        bail!("{what}: {id} is still a hit");
    }
    Ok(())
}

/// Asks the server to checkpoint, so that earlier writes move to an older layer. The
/// result is ignored: a server without a data directory can refuse the request.
fn checkpoint(api: &HttpApi) {
    let _ = api.call("POST", "/admin/checkpoint", None);
}

/// A term query on one field.
fn term(field: &str, value: &str) -> Value {
    json!({"term": {"field": field, "value": value}})
}

/// Merges the items into the collection and returns the response body.
fn merge(api: &HttpApi, base: &str, items: Value) -> Result<Value> {
    api.expect(
        "merge",
        &[200],
        "POST",
        &format!("{base}/docs:merge"),
        Some(&json!({"items": items})),
    )
}

/// Runs the writes and masking steps for one collection id. The steps are fixed by the spec.
pub(super) fn run_writes(api: &HttpApi, collection: &str) -> Result<()> {
    let base = format!("/collections/{collection}");
    let delete = format!("{base}?force=true");

    // 1. Best effort: remove a collection left by an earlier run. Then create it.
    let _ = api.call("DELETE", &delete, None);
    api.expect("create", &[200], "PUT", &base, Some(&schema()))?;

    // 2. Merge: two fields of doc-1 and one field of doc-2.
    let merged = merge(
        api,
        &base,
        json!([
            {"external_id": "doc-1", "field": "kw", "value": "red"},
            {"external_id": "doc-1", "field": "num", "value": 1.0},
            {"external_id": "doc-2", "field": "kw", "value": "red"},
        ]),
    )?;
    if merged["indexed"].as_u64() != Some(3) {
        bail!("merge: indexed is {}, wanted 3", merged["indexed"]);
    }
    let indexed = documents_indexed(api, &base)?;
    if indexed != 2 {
        bail!("merge: documents_indexed is {indexed}, wanted 2");
    }
    let hits = search(api, &base, term("kw", "red"))?;
    assert_hit(&hits, "doc-1", "merge kw red")?;
    assert_hit(&hits, "doc-2", "merge kw red")?;

    // 3. Partial merge: doc-1 gains tag=a and keeps kw=red.
    merge(
        api,
        &base,
        json!([{"external_id": "doc-1", "field": "tag", "value": "a"}]),
    )?;
    assert_hit(
        &search(api, &base, term("tag", "a"))?,
        "doc-1",
        "partial merge tag a",
    )?;
    let hits = search(api, &base, term("kw", "red"))?;
    assert_hit(&hits, "doc-1", "partial merge keeps kw red")?;
    assert_hit(&hits, "doc-2", "partial merge keeps kw red")?;

    // 4. An update masks the old value of the same field.
    merge(
        api,
        &base,
        json!([{"external_id": "doc-1", "field": "kw", "value": "blue"}]),
    )?;
    let hits = search(api, &base, term("kw", "red"))?;
    assert_absent(&hits, "doc-1", "update masks kw red")?;
    assert_hit(&hits, "doc-2", "update keeps doc-2")?;
    assert_hit(
        &search(api, &base, term("kw", "blue"))?,
        "doc-1",
        "update kw blue",
    )?;
    assert_hit(
        &search(api, &base, term("tag", "a"))?,
        "doc-1",
        "update keeps tag a",
    )?;

    // 5. Replace: doc-1 keeps only kw=green, so the omitted tag is deleted.
    let replaced = api.expect(
        "replace",
        &[200],
        "PUT",
        &format!("{base}/docs:replace"),
        Some(&json!({"docs": [{"external_id": "doc-1", "fields": {"kw": "green"}}]})),
    )?;
    if replaced["results"][0]["status"] != "ok" {
        bail!(
            "replace: doc-1 result is {}, wanted ok",
            replaced["results"][0]
        );
    }
    assert_hit(
        &search(api, &base, term("kw", "green"))?,
        "doc-1",
        "replace kw green",
    )?;
    assert_absent(
        &search(api, &base, term("kw", "blue"))?,
        "doc-1",
        "replace masks kw blue",
    )?;
    assert_absent(
        &search(api, &base, term("tag", "a"))?,
        "doc-1",
        "replace masks tag a",
    )?;

    // 6. Older layer: doc-2 is updated after a checkpoint, so the old value lives in an
    // older layer and must still be masked.
    checkpoint(api);
    merge(
        api,
        &base,
        json!([{"external_id": "doc-2", "field": "kw", "value": "yellow"}]),
    )?;
    assert_no_hits(
        &search(api, &base, term("kw", "red"))?,
        "older layer masks kw red",
    )?;
    assert_hit(
        &search(api, &base, term("kw", "yellow"))?,
        "doc-2",
        "older layer kw yellow",
    )?;

    // 7. Single unindex: doc-2 disappears.
    api.expect(
        "unindex one",
        &[204],
        "DELETE",
        &format!("{base}/index/doc-2"),
        None,
    )?;
    assert_no_hits(
        &search(api, &base, term("kw", "yellow"))?,
        "single unindex kw yellow",
    )?;
    let indexed = documents_indexed(api, &base)?;
    if indexed != 1 {
        bail!("single unindex: documents_indexed is {indexed}, wanted 1");
    }

    // 8. Batch unindex: doc-3 and doc-4 go, doc-5 stays. doc-5 is in an older layer.
    merge(
        api,
        &base,
        json!([
            {"external_id": "doc-3", "field": "kw", "value": "purple"},
            {"external_id": "doc-4", "field": "kw", "value": "purple"},
            {"external_id": "doc-5", "field": "kw", "value": "purple"},
        ]),
    )?;
    checkpoint(api);
    api.expect(
        "unindex batch",
        &[204],
        "POST",
        &format!("{base}/docs:unindex"),
        Some(&json!({"external_ids": ["doc-3", "doc-4"]})),
    )?;
    let hits = search(api, &base, term("kw", "purple"))?;
    assert_hit(&hits, "doc-5", "batch unindex keeps doc-5")?;
    assert_absent(&hits, "doc-3", "batch unindex removes doc-3")?;
    assert_absent(&hits, "doc-4", "batch unindex removes doc-4")?;
    let indexed = documents_indexed(api, &base)?;
    if indexed != 2 {
        bail!("batch unindex: documents_indexed is {indexed}, wanted 2");
    }

    // 9. A delete masks all older layers: doc-5 is gone after a checkpoint.
    checkpoint(api);
    api.expect(
        "unindex doc-5",
        &[204],
        "POST",
        &format!("{base}/docs:unindex"),
        Some(&json!({"external_ids": ["doc-5"]})),
    )?;
    assert_no_hits(
        &search(api, &base, term("kw", "purple"))?,
        "delete masks kw purple",
    )?;

    // 10. Complete clear: nothing is indexed and no old value matches.
    api.expect(
        "truncate",
        &[204],
        "POST",
        &format!("{base}/docs:truncate"),
        None,
    )?;
    let indexed = documents_indexed(api, &base)?;
    if indexed != 0 {
        bail!("clear: documents_indexed is {indexed}, wanted 0");
    }
    assert_no_hits(&search(api, &base, term("kw", "green"))?, "clear kw green")?;

    // 11. Cleanup.
    api.expect("cleanup", &[204], "DELETE", &delete, None)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::tests::serve_for_test;

    #[test]
    fn assert_absent_fails_when_the_old_value_still_matches() {
        let survivor = json!({"hits": [{"external_id": "doc-1", "score": 1.0}]});
        let err =
            assert_absent(&survivor, "doc-1", "after delete").expect_err("doc-1 is still a hit");
        assert!(format!("{err:#}").contains("after delete"));
        assert_absent(&json!({"hits": []}), "doc-1", "after delete").expect("no hits passes");
        let other = json!({"hits": [{"external_id": "doc-2", "score": 1.0}]});
        assert_absent(&other, "doc-1", "after delete").expect("only doc-2 passes");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_phase_passes_against_a_real_server() {
        let (_server, url) = serve_for_test();
        let result =
            tokio::task::spawn_blocking(move || WritesAndMasking.run(&url, Path::new(".")))
                .await
                .expect("join");
        result.expect("every writes step passes");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deleted_document_is_not_visible_after_unindex() {
        let (_server, url) = serve_for_test();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let api = HttpApi::new(&url)?;
            let base = "/collections/acceptance-unindex-check";
            let delete = format!("{base}?force=true");
            let _ = api.call("DELETE", &delete, None);
            api.expect("create", &[200], "PUT", base, Some(&schema()))?;
            merge(
                &api,
                base,
                json!([{"external_id": "doc-1", "field": "kw", "value": "red"}]),
            )?;
            api.expect(
                "unindex",
                &[204],
                "DELETE",
                &format!("{base}/index/doc-1"),
                None,
            )?;
            let hits = search(&api, base, term("kw", "red"))?;
            assert_absent(&hits, "doc-1", "after unindex")?;
            assert_no_hits(&hits, "after unindex")?;
            // The same check must fail while doc-1 is indexed again.
            merge(
                &api,
                base,
                json!([{"external_id": "doc-1", "field": "kw", "value": "red"}]),
            )?;
            let hits = search(&api, base, term("kw", "red"))?;
            assert!(assert_no_hits(&hits, "indexed again").is_err());
            assert_hit(&hits, "doc-1", "indexed again")?;
            api.expect("cleanup", &[204], "DELETE", &delete, None)?;
            Ok(())
        })
        .await
        .expect("join")
        .expect("the unindex check passes");
    }
}
