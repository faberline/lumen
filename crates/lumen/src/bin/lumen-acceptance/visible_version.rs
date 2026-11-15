//! The `visible_version` functional phase: it updates, then deletes, documents over
//! HTTP and proves that every read path shows the same current version of each
//! document, never a stale one.

use std::path::Path;

use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value};

use crate::http::HttpApi;
use crate::run::Phase;

pub(super) struct VisibleVersion;

impl Phase for VisibleVersion {
    fn name(&self) -> &str {
        "visible_version"
    }

    fn run(&self, endpoint: &str, _artifact_dir: &Path) -> Result<()> {
        let api = HttpApi::new(endpoint)?;
        run_visible_version(&api, &format!("acceptance-visible-{}", std::process::id()))
    }
}

/// What every read path returns for one collection at one point in time.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ReadView {
    pub(super) red: Vec<String>,
    pub(super) blue: Vec<String>,
    pub(super) body_gamma: Vec<String>,
    pub(super) body_alpha: Vec<String>,
    pub(super) num_desc: Vec<String>,
    pub(super) dup_groups: Vec<(String, Vec<String>)>,
    pub(super) avg_doc_len: f64,
    pub(super) documents: u64,
    pub(super) flat_knn: Vec<String>,
    pub(super) hnsw_knn: Vec<String>,
}

/// Reads every path of one collection. `knn_probe` is the query vector of both kNN paths.
pub(super) fn read_view(api: &HttpApi, collection: &str, knn_probe: [f32; 2]) -> Result<ReadView> {
    let search = |what: &str, query: Value, sort: Option<Value>| -> Result<Vec<String>> {
        let mut body = json!({"query": query, "limit": 10});
        if let Some(sort) = sort {
            body["sort"] = sort;
        }
        let value = api.expect(
            what,
            &[200],
            "POST",
            &format!("/collections/{collection}/search"),
            Some(&body),
        )?;
        hit_ids(&value, what)
    };
    let ordered = json!([{"field": "num", "order": "desc"}]);

    let red = sorted(search(
        "filter_red",
        json!({"term": {"field": "kw", "value": "red"}}),
        None,
    )?);
    let blue = sorted(search(
        "filter_blue",
        json!({"term": {"field": "kw", "value": "blue"}}),
        None,
    )?);
    let body_gamma = sorted(search(
        "text_hits_gamma",
        json!({"match": {"field": "body", "text": "gamma"}}),
        None,
    )?);
    let body_alpha = sorted(search(
        "text_hits_alpha",
        json!({"match": {"field": "body", "text": "alpha"}}),
        None,
    )?);
    let num_desc = search(
        "ordering",
        json!({"range": {"field": "num", "gte": -1000.0}}),
        Some(ordered),
    )?;
    let flat_knn = search(
        "flat_knn",
        json!({"knn": {"field": "vec_flat", "vector": knn_probe, "k": 3}}),
        None,
    )?;
    let hnsw_knn = search(
        "hnsw_knn",
        json!({"knn": {"field": "vec_hnsw", "vector": knn_probe, "k": 3}}),
        None,
    )?;

    let dup = api.expect(
        "duplicates",
        &[200],
        "POST",
        &format!("/collections/{collection}/duplicates"),
        Some(&json!({"field": "kw", "min_group_size": 2, "limit": 100})),
    )?;
    let mut dup_groups = Vec::new();
    for group in dup["groups"]
        .as_array()
        .ok_or_else(|| anyhow!("duplicates: no groups array"))?
    {
        let value = group["value"]
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| group["value"].to_string());
        let external_ids = str_list(&group["external_ids"], "duplicates")?;
        dup_groups.push((value, sorted(external_ids)));
    }
    dup_groups.sort();

    let stats = api.expect(
        "bm25_doc_length",
        &[200],
        "GET",
        &format!("/collections/{collection}/stats"),
        None,
    )?;
    let avg_doc_len = stats["fields"]["body"]["avg_doc_len"]
        .as_f64()
        .ok_or_else(|| anyhow!("bm25_doc_length: no fields.body.avg_doc_len"))?;
    let documents = stats["documents_indexed"]
        .as_u64()
        .ok_or_else(|| anyhow!("document_count: no documents_indexed"))?;

    Ok(ReadView {
        red,
        blue,
        body_gamma,
        body_alpha,
        num_desc,
        dup_groups,
        avg_doc_len,
        documents,
        flat_knn,
        hnsw_knn,
    })
}

/// Compares the read paths one by one, in a fixed order. Fails on the first stale path.
pub(super) fn expect_view(got: &ReadView, want: &ReadView, step: &str) -> Result<()> {
    let stale = |path: &str| -> Result<()> {
        bail!("{step}: read path `{path}` is stale: got {got:?}, want {want:?}")
    };
    if got.red != want.red {
        return stale("filter_red");
    }
    if got.blue != want.blue {
        return stale("filter_blue");
    }
    if got.body_gamma != want.body_gamma {
        return stale("text_hits_gamma");
    }
    if got.body_alpha != want.body_alpha {
        return stale("text_hits_alpha");
    }
    if got.num_desc != want.num_desc {
        return stale("ordering");
    }
    if got.dup_groups != want.dup_groups {
        return stale("duplicates");
    }
    if (got.avg_doc_len - want.avg_doc_len).abs() >= 1e-3 {
        return stale("bm25_doc_length");
    }
    if got.documents != want.documents {
        return stale("document_count");
    }
    if got.flat_knn != want.flat_knn {
        return stale("flat_knn");
    }
    if got.hnsw_knn != want.hnsw_knn {
        return stale("hnsw_knn");
    }
    Ok(())
}

/// The expected view after the initial load (version 1).
fn view_v1() -> ReadView {
    ReadView {
        red: ids(&["d1", "d2"]),
        blue: ids(&["d3"]),
        body_gamma: ids(&["d1"]),
        body_alpha: ids(&["d1", "d2"]),
        num_desc: ids(&["d3", "d2", "d1"]),
        dup_groups: vec![("red".into(), ids(&["d1", "d2"]))],
        avg_doc_len: 2.0,
        documents: 3,
        flat_knn: ids(&["d1", "d2", "d3"]),
        hnsw_knn: ids(&["d1", "d2", "d3"]),
    }
}

/// The expected view after d1 is updated to blue, `[9, 9]`.
fn view_v2() -> ReadView {
    ReadView {
        red: ids(&["d2"]),
        blue: ids(&["d1", "d3"]),
        body_gamma: vec![],
        body_alpha: ids(&["d1", "d2"]),
        num_desc: ids(&["d1", "d3", "d2"]),
        dup_groups: vec![("blue".into(), ids(&["d1", "d3"]))],
        avg_doc_len: 4.0 / 3.0,
        documents: 3,
        flat_knn: ids(&["d2", "d3", "d1"]),
        hnsw_knn: ids(&["d2", "d3", "d1"]),
    }
}

/// The expected view after d3 is deleted.
fn view_v3() -> ReadView {
    ReadView {
        red: ids(&["d2"]),
        blue: ids(&["d1"]),
        body_gamma: vec![],
        body_alpha: ids(&["d1", "d2"]),
        num_desc: ids(&["d1", "d2"]),
        dup_groups: vec![],
        avg_doc_len: 1.0,
        documents: 2,
        flat_knn: ids(&["d2", "d1"]),
        hnsw_knn: ids(&["d2", "d1"]),
    }
}

/// The expected view after d2 is unindexed.
fn view_v4() -> ReadView {
    ReadView {
        red: vec![],
        blue: ids(&["d1"]),
        body_gamma: vec![],
        body_alpha: ids(&["d1"]),
        num_desc: ids(&["d1"]),
        dup_groups: vec![],
        avg_doc_len: 1.0,
        documents: 1,
        flat_knn: ids(&["d1"]),
        hnsw_knn: ids(&["d1"]),
    }
}

/// The expected view after d3 is re-added as red, `zeta`, `[1, 0]`.
fn view_v5() -> ReadView {
    ReadView {
        red: ids(&["d3"]),
        blue: ids(&["d1"]),
        body_gamma: vec![],
        body_alpha: ids(&["d1"]),
        num_desc: ids(&["d1", "d3"]),
        dup_groups: vec![],
        avg_doc_len: 1.0,
        documents: 2,
        flat_knn: ids(&["d3", "d1"]),
        hnsw_knn: ids(&["d3", "d1"]),
    }
}

/// Runs the update and delete steps of the phase against `collection`.
pub(super) fn run_visible_version(api: &HttpApi, collection: &str) -> Result<()> {
    let probe = [1.0_f32, 0.0];
    let path = format!("/collections/{collection}");

    // Step 1: a leftover from an earlier run goes first; its result does not matter.
    let _ = api.call("DELETE", &format!("{path}?force=true"), None);

    // Step 2: the schema.
    api.expect(
        "create collection",
        &[200],
        "PUT",
        &path,
        Some(&json!({"fields": {
            "kw": {"type": "keyword"},
            "num": {"type": "number"},
            "body": {"type": "text", "analyzer": "whitespace_lower"},
            "vec_flat": {"type": "vector", "dim": 2, "metric": "l2", "backend": "flat-cpu"},
            "vec_hnsw": {"type": "vector", "dim": 2, "metric": "l2", "backend": "hnsw-cpu"},
        }})),
    )?;

    // Step 3: version 1 of d1, d2 and d3 in one batch.
    let batch = api.expect(
        "replace batch",
        &[200],
        "PUT",
        &format!("{path}/docs:replace"),
        Some(&json!({"docs": [
            {"external_id": "d1", "fields": {"kw": "red", "num": 1.0, "body": "alpha beta gamma",
                "vec_flat": [1.0, 0.0], "vec_hnsw": [1.0, 0.0]}},
            {"external_id": "d2", "fields": {"kw": "red", "num": 2.0, "body": "alpha",
                "vec_flat": [0.0, 1.0], "vec_hnsw": [0.0, 1.0]}},
            {"external_id": "d3", "fields": {"kw": "blue", "num": 3.0, "body": "delta epsilon",
                "vec_flat": [5.0, 5.0], "vec_hnsw": [5.0, 5.0]}},
        ]})),
    )?;
    for result in batch["results"]
        .as_array()
        .ok_or_else(|| anyhow!("replace batch: no results array"))?
    {
        check_status_ok(result, "replace batch item")?;
    }
    expect_view(&read_view(api, collection, probe)?, &view_v1(), "version 1")?;

    // Step 4: d1 is replaced in full, as blue, with `alpha` and `[9, 9]`.
    let update = api.expect(
        "update d1",
        &[200],
        "PUT",
        &format!("{path}/docs/d1"),
        Some(
            &json!({"fields": {"kw": "blue", "num": 10.0, "body": "alpha",
            "vec_flat": [9.0, 9.0], "vec_hnsw": [9.0, 9.0]}}),
        ),
    )?;
    check_status_ok(&update, "update d1")?;
    expect_view(&read_view(api, collection, probe)?, &view_v2(), "update d1")?;

    // Step 5: d3 is deleted.
    api.expect(
        "delete d3",
        &[204],
        "DELETE",
        &format!("{path}/docs/d3"),
        None,
    )?;
    expect_view(&read_view(api, collection, probe)?, &view_v3(), "delete d3")?;

    // Step 6: d2 is unindexed.
    api.expect(
        "unindex d2",
        &[204],
        "POST",
        &format!("{path}/docs:unindex"),
        Some(&json!({"external_ids": ["d2"]})),
    )?;
    expect_view(
        &read_view(api, collection, probe)?,
        &view_v4(),
        "unindex d2",
    )?;

    // Step 7: d3 comes back with new values; its old `blue` and `delta epsilon` must not return.
    let readd = api.expect(
        "re-add d3",
        &[200],
        "PUT",
        &format!("{path}/docs/d3"),
        Some(&json!({"fields": {"kw": "red", "num": 3.0, "body": "zeta",
            "vec_flat": [1.0, 0.0], "vec_hnsw": [1.0, 0.0]}})),
    )?;
    check_status_ok(&readd, "re-add d3")?;
    expect_view(&read_view(api, collection, probe)?, &view_v5(), "re-add d3")?;

    // Step 8: cleanup.
    api.expect(
        "drop collection",
        &[204],
        "DELETE",
        &format!("{path}?force=true"),
        None,
    )?;
    Ok(())
}

/// Fails unless a write reply has `status == "ok"`. `what` names the step.
fn check_status_ok(reply: &Value, what: &str) -> Result<()> {
    if reply["status"] != "ok" {
        bail!("{what}: not ok: {reply}");
    }
    Ok(())
}

fn ids(xs: &[&str]) -> Vec<String> {
    xs.iter().map(|x| x.to_string()).collect()
}

fn sorted(mut xs: Vec<String>) -> Vec<String> {
    xs.sort();
    xs
}

/// The `external_id` of each hit, in server order.
fn hit_ids(value: &Value, what: &str) -> Result<Vec<String>> {
    let hits = value["hits"]
        .as_array()
        .ok_or_else(|| anyhow!("{what}: no hits array"))?;
    hits.iter()
        .map(|hit| {
            hit["external_id"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| anyhow!("{what}: a hit has no external_id"))
        })
        .collect()
}

fn str_list(value: &Value, what: &str) -> Result<Vec<String>> {
    value
        .as_array()
        .ok_or_else(|| anyhow!("{what}: expected an array of ids"))?
        .iter()
        .map(|x| {
            x.as_str()
                .map(str::to_string)
                .ok_or_else(|| anyhow!("{what}: an id is not a string"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::http::tests::serve_for_test;
    use crate::run::Phase;

    /// The expected view after step 4 (d1 updated to blue, `[9, 9]`).
    fn v2() -> ReadView {
        ReadView {
            red: vec!["d2".into()],
            blue: vec!["d1".into(), "d3".into()],
            body_gamma: vec![],
            body_alpha: vec!["d1".into(), "d2".into()],
            num_desc: vec!["d1".into(), "d3".into(), "d2".into()],
            dup_groups: vec![("blue".into(), vec!["d1".into(), "d3".into()])],
            avg_doc_len: 4.0 / 3.0,
            documents: 3,
            flat_knn: vec!["d2".into(), "d3".into(), "d1".into()],
            hnsw_knn: vec!["d2".into(), "d3".into(), "d1".into()],
        }
    }

    /// One copy of `base` per read path, with only that path changed to a stale value.
    fn stale_variants(base: &ReadView) -> Vec<(&'static str, ReadView)> {
        let mut out = Vec::new();
        let mut v = base.clone();
        v.red = vec!["d1".into(), "d2".into()];
        out.push(("filter_red", v));
        let mut v = base.clone();
        v.blue = vec!["d3".into()];
        out.push(("filter_blue", v));
        let mut v = base.clone();
        v.body_gamma = vec!["d1".into()];
        out.push(("text_hits_gamma", v));
        let mut v = base.clone();
        v.body_alpha = vec!["d1".into()];
        out.push(("text_hits_alpha", v));
        let mut v = base.clone();
        v.num_desc = vec!["d2".into(), "d3".into(), "d1".into()];
        out.push(("ordering", v));
        let mut v = base.clone();
        v.dup_groups = vec![("red".into(), vec!["d1".into(), "d2".into()])];
        out.push(("duplicates", v));
        let mut v = base.clone();
        v.avg_doc_len = 2.0;
        out.push(("bm25_doc_length", v));
        let mut v = base.clone();
        v.documents = 4;
        out.push(("document_count", v));
        let mut v = base.clone();
        v.flat_knn = vec!["d1".into(), "d2".into(), "d3".into()];
        out.push(("flat_knn", v));
        let mut v = base.clone();
        v.hnsw_knn = vec!["d1".into(), "d2".into(), "d3".into()];
        out.push(("hnsw_knn", v));
        out
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visible_version_passes_against_a_real_server() {
        let (_server, url) = serve_for_test();
        let result = tokio::task::spawn_blocking(move || VisibleVersion.run(&url, Path::new(".")))
            .await
            .expect("join");
        result.expect("every read path shows the visible version");
    }

    #[test]
    fn expect_view_fails_when_any_one_read_path_is_stale() {
        let base = v2();
        expect_view(&base, &base, "t").expect("an equal view passes");
        let variants = stale_variants(&base);
        assert_eq!(variants.len(), 10, "one case per read path");
        for (path, stale) in &variants {
            let err =
                expect_view(stale, &base, "t").expect_err("a stale read path must fail the check");
            let text = format!("{err:#}");
            assert!(
                text.contains(&format!("read path `{path}` is stale")),
                "{path}: {text}"
            );
        }
    }

    #[test]
    fn expect_view_accepts_tiny_float_noise() {
        let base = v2();
        let mut noisy = base.clone();
        noisy.avg_doc_len = 1.3333;
        expect_view(&noisy, &base, "t").expect("1.3333 is 4/3 within noise");
        let mut off = base.clone();
        off.avg_doc_len = 1.5;
        assert!(expect_view(&off, &base, "t").is_err(), "1.5 is not 4/3");
    }

    #[test]
    fn stale_variants_name_a_real_path() {
        // The helper's own labels must match the paths the check reports.
        let names: Vec<&str> = stale_variants(&v2()).iter().map(|(p, _)| *p).collect();
        assert!(names.contains(&"ordering") && names.contains(&"hnsw_knn"));
    }

    /// One canned reply: method, path prefix, status, body.
    type Route = (&'static str, &'static str, u16, &'static str);

    /// A canned HTTP server for the error paths. The first route whose method matches and
    /// whose path starts with its prefix answers with its status and body; others get 404.
    fn serve_canned(routes: Vec<Route>) -> String {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("address");
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut reader = BufReader::new(stream.try_clone().expect("clone"));
                let mut request_line = String::new();
                reader.read_line(&mut request_line).expect("request line");
                let mut parts = request_line.split_whitespace();
                let method = parts.next().unwrap_or_default().to_string();
                let path = parts.next().unwrap_or_default().to_string();
                let mut length: usize = 0;
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).expect("header");
                    if header.trim().is_empty() {
                        break;
                    }
                    if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:")
                    {
                        length = value.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; length];
                reader.read_exact(&mut body).expect("body");
                let (status, reply) = routes
                    .iter()
                    .find(|(m, prefix, _, _)| *m == method && path.starts_with(*prefix))
                    .map_or((404, ""), |(_, _, status, reply)| (*status, *reply));
                let response = format!(
                    "HTTP/1.1 {status} Canned\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        format!("http://{addr}")
    }

    /// Replies that let `read_view` finish on collection `c`, after the `first` overrides.
    fn read_routes(first: Vec<Route>) -> Vec<Route> {
        let mut routes = first;
        routes.push((
            "POST",
            "/collections/c/search",
            200,
            r#"{"hits":[{"external_id":"d1"}]}"#,
        ));
        routes.push(("POST", "/collections/c/duplicates", 200, r#"{"groups":[]}"#));
        routes.push((
            "GET",
            "/collections/c/stats",
            200,
            r#"{"fields":{"body":{"avg_doc_len":2.0}},"documents_indexed":3}"#,
        ));
        routes
    }

    fn read_ok(first: Vec<Route>) -> ReadView {
        let api = HttpApi::new(&serve_canned(read_routes(first))).expect("client");
        read_view(&api, "c", [1.0, 0.0]).expect("the read must finish")
    }

    fn read_err(first: Vec<Route>) -> String {
        let api = HttpApi::new(&serve_canned(read_routes(first))).expect("client");
        let err = read_view(&api, "c", [1.0, 0.0]).expect_err("the read must fail");
        format!("{err:#}")
    }

    /// Runs the phase on collection `c`; the replace batch answers with `status` and `reply`.
    fn run_err(replace_status: u16, replace_reply: &'static str) -> String {
        run_err_with(vec![(
            "PUT",
            "/collections/c/docs:replace",
            replace_status,
            replace_reply,
        )])
    }

    /// Runs the phase on collection `c` with `first` replies before the defaults, which are
    /// a dropped leftover, a created collection and a replace batch that is not used.
    fn run_err_with(first: Vec<Route>) -> String {
        let mut routes = first;
        routes.push(("DELETE", "/collections/c", 204, ""));
        routes.push(("PUT", "/collections/c", 200, "{}"));
        let api = HttpApi::new(&serve_canned(routes)).expect("client");
        let err = run_visible_version(&api, "c").expect_err("the phase must stop");
        format!("{err:#}")
    }

    #[test]
    fn visible_version_has_its_name() {
        assert_eq!(VisibleVersion.name(), "visible_version");
    }

    #[test]
    fn run_visible_version_stops_when_the_collection_cannot_be_created() {
        let mut routes = vec![("PUT", "/collections/c", 500, "{}")];
        routes.insert(0, ("DELETE", "/collections/c", 204, ""));
        let api = HttpApi::new(&serve_canned(routes)).expect("client");
        let err = run_visible_version(&api, "c").expect_err("the create must fail");
        assert!(format!("{err:#}").contains("create collection"), "{err:#}");
    }

    #[test]
    fn run_visible_version_stops_when_the_batch_is_refused() {
        let err = run_err(500, "{}");
        assert!(
            err.contains("replace batch: PUT /collections/c/docs:replace returned 500"),
            "{err}"
        );
    }

    #[test]
    fn run_visible_version_stops_when_the_first_read_is_refused() {
        let err = run_err_with(vec![
            (
                "PUT",
                "/collections/c/docs:replace",
                200,
                r#"{"results":[{"status":"ok"}]}"#,
            ),
            ("POST", "/collections/c/search", 500, "{}"),
        ]);
        assert!(
            err.contains("filter_red") && err.contains("returned 500"),
            "{err}"
        );
    }

    #[test]
    fn check_status_ok_accepts_only_ok() {
        check_status_ok(&json!({"status": "ok"}), "step").expect("ok passes");
        let err = check_status_ok(&json!({"status": "error"}), "step").expect_err("not ok fails");
        assert!(format!("{err:#}").contains("step: not ok"), "{err:#}");
    }

    #[test]
    fn read_view_reads_a_complete_canned_view() {
        let view = read_ok(vec![]);
        assert_eq!(view.red, vec!["d1".to_string()]);
        assert_eq!(view.flat_knn, vec!["d1".to_string()]);
        assert_eq!(view.documents, 3);
        assert!((view.avg_doc_len - 2.0).abs() < 1e-9);
    }

    #[test]
    fn read_view_fails_when_a_search_has_no_hits() {
        let err = read_err(vec![("POST", "/collections/c/search", 200, "{}")]);
        assert!(err.contains("filter_red: no hits array"), "{err}");
    }

    #[test]
    fn read_view_fails_when_a_hit_has_no_external_id() {
        let err = read_err(vec![(
            "POST",
            "/collections/c/search",
            200,
            r#"{"hits":[{"score":1.0}]}"#,
        )]);
        assert!(err.contains("a hit has no external_id"), "{err}");
    }

    #[test]
    fn read_view_fails_when_a_search_is_refused() {
        let err = read_err(vec![("POST", "/collections/c/search", 500, "{}")]);
        assert!(
            err.contains("filter_red") && err.contains("returned 500"),
            "{err}"
        );
    }

    #[test]
    fn read_view_fails_when_duplicates_has_no_groups() {
        let err = read_err(vec![("POST", "/collections/c/duplicates", 200, "{}")]);
        assert!(err.contains("duplicates: no groups array"), "{err}");
    }

    #[test]
    fn read_view_names_a_numeric_duplicate_value_by_its_json() {
        let view = read_ok(vec![(
            "POST",
            "/collections/c/duplicates",
            200,
            r#"{"groups":[{"value":7,"external_ids":["d2","d1"]}]}"#,
        )]);
        assert_eq!(
            view.dup_groups,
            vec![("7".to_string(), vec!["d1".to_string(), "d2".to_string()])]
        );
    }

    #[test]
    fn read_view_fails_when_a_duplicate_id_is_not_a_string() {
        let err = read_err(vec![(
            "POST",
            "/collections/c/duplicates",
            200,
            r#"{"groups":[{"value":"red","external_ids":[1]}]}"#,
        )]);
        assert!(err.contains("duplicates: an id is not a string"), "{err}");
    }

    #[test]
    fn read_view_fails_when_duplicate_ids_are_not_a_list() {
        let err = read_err(vec![(
            "POST",
            "/collections/c/duplicates",
            200,
            r#"{"groups":[{"value":"red","external_ids":"d1"}]}"#,
        )]);
        assert!(
            err.contains("duplicates: expected an array of ids"),
            "{err}"
        );
    }

    #[test]
    fn read_view_fails_when_stats_have_no_average_length() {
        let err = read_err(vec![(
            "GET",
            "/collections/c/stats",
            200,
            r#"{"documents_indexed":3}"#,
        )]);
        assert!(
            err.contains("bm25_doc_length: no fields.body.avg_doc_len"),
            "{err}"
        );
    }

    #[test]
    fn read_view_fails_when_stats_have_no_document_count() {
        let err = read_err(vec![(
            "GET",
            "/collections/c/stats",
            200,
            r#"{"fields":{"body":{"avg_doc_len":2.0}}}"#,
        )]);
        assert!(
            err.contains("document_count: no documents_indexed"),
            "{err}"
        );
    }

    #[test]
    fn run_visible_version_stops_when_the_batch_has_no_results() {
        let err = run_err(200, "{}");
        assert!(err.contains("replace batch: no results array"), "{err}");
    }

    #[test]
    fn run_visible_version_stops_when_a_batch_item_is_not_ok() {
        let err = run_err(200, r#"{"results":[{"status":"error"}]}"#);
        assert!(err.contains("replace batch item: not ok"), "{err}");
    }
}
