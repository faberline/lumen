//! The `restart_persistence` functional phase. With `--spawn` it writes documents, one of
//! them through a request that the client cancels, restarts the owned `lumen serve` child
//! on the same data dir and port, and proves that the same search returns the same hits.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value};

use crate::http::HttpApi;
use crate::lifecycle::search;
use crate::run::Phase;
use crate::spawn::SharedChild;

pub(super) struct RestartPersistence {
    pub(super) child: SharedChild,
}

impl Phase for RestartPersistence {
    fn name(&self) -> &str {
        "restart_persistence"
    }

    fn run(&self, endpoint: &str, _artifact_dir: &Path) -> Result<()> {
        let api = HttpApi::new(endpoint)?;
        let collection = format!("acceptance-restart-{}", std::process::id());
        let mut restart = || -> Result<()> {
            let mut child = self
                .child
                .lock()
                .map_err(|_| anyhow!("child lock poisoned"))?;
            child.stop()?;
            child.start()
        };
        run_restart(&api, &collection, &mut restart)
    }
}

/// Runs the restart steps for one collection id. The steps are fixed by the spec.
pub(super) fn run_restart(
    api: &HttpApi,
    collection: &str,
    restart: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    let base = format!("/collections/{collection}");
    let index = format!("{base}/index");
    let delete = format!("{base}?force=true");

    // 1. Best effort: remove a collection left by an earlier run, then create it.
    let _ = api.call("DELETE", &delete, None);
    api.expect(
        "create",
        &[200],
        "PUT",
        &base,
        Some(&json!({"fields": {"kw": {"type": "keyword"}}})),
    )?;

    // 2. Two acknowledged writes.
    api.expect(
        "index doc-1 and doc-2",
        &[200],
        "POST",
        &index,
        Some(&json!({"items": [
            {"external_id": "doc-1", "field": "kw", "value": "red"},
            {"external_id": "doc-2", "field": "kw", "value": "red"}
        ]})),
    )?;

    // 3. A write whose request the client cancels. It must still become searchable.
    api.fire_and_cancel(
        "POST",
        &index,
        Some(&json!({"items": [
            {"external_id": "doc-3", "field": "kw", "value": "red"}
        ]})),
    )?;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if term_red(api, &base)?.iter().any(|id| id == "doc-3") {
            break;
        }
        if Instant::now() >= deadline {
            bail!("cancelled write doc-3 was not applied");
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // 4. Record the hits before the restart.
    let before = term_red(api, &base)?;
    if before != ["doc-1", "doc-2", "doc-3"] {
        bail!("before restart the hits are {before:?}, wanted doc-1, doc-2, doc-3");
    }

    // 5. Restart the owned child on the same data dir and port.
    restart()?;

    // 6. The same search, with a short retry for a connection that the restart broke.
    let after = term_red_retrying(api, &base)?;
    if after != before {
        bail!("after restart the hits are {after:?}, wanted {before:?}");
    }

    // 7. Best effort cleanup.
    let _ = api.call("DELETE", &delete, None);
    Ok(())
}

/// The sorted `external_id`s of the hits of a search response.
fn hit_ids(response: &Value) -> Vec<String> {
    let mut ids: Vec<String> = response["hits"]
        .as_array()
        .map(|hits| {
            hits.iter()
                .filter_map(|hit| hit["external_id"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    ids
}

/// The sorted ids of the documents whose keyword field `kw` is `red`.
fn term_red(api: &HttpApi, base: &str) -> Result<Vec<String>> {
    let response = search(api, base, json!({"term": {"field": "kw", "value": "red"}}))?;
    Ok(hit_ids(&response))
}

/// `term_red` with up to three attempts, 200 ms apart.
fn term_red_retrying(api: &HttpApi, base: &str) -> Result<Vec<String>> {
    let mut last = None;
    for attempt in 0..3 {
        match term_red(api, base) {
            Ok(ids) => return Ok(ids),
            Err(err) => last = Some(err),
        }
        if attempt < 2 {
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    Err(last.unwrap_or_else(|| anyhow!("no attempt ran")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::tests::serve_for_test;

    #[test]
    fn hit_ids_sorts_the_external_ids() {
        let response = json!({"hits": [{"external_id": "b"}, {"external_id": "a"}]});
        assert_eq!(hit_ids(&response), ["a", "b"]);
        assert!(hit_ids(&json!({})).is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_restart_passes_when_restart_keeps_the_data() {
        let (_server, url) = serve_for_test();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let api = HttpApi::new(&url)?;
            let mut restart = || Ok(());
            run_restart(&api, "acceptance-restart-keeps", &mut restart)
        })
        .await
        .expect("join")
        .expect("a restart that keeps the data passes");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_restart_fails_when_restart_loses_the_data() {
        let (_server, url) = serve_for_test();
        let err = tokio::task::spawn_blocking(move || -> Result<()> {
            let api = HttpApi::new(&url)?;
            let other_url = url.clone();
            let base = "/collections/acceptance-restart-loses";
            // The restart drops the collection and recreates it empty: the data is lost.
            let mut restart = || -> Result<()> {
                let api = HttpApi::new(&other_url)?;
                api.expect(
                    "drop",
                    &[204],
                    "DELETE",
                    &format!("{base}?force=true"),
                    None,
                )?;
                api.expect(
                    "recreate",
                    &[200],
                    "PUT",
                    base,
                    Some(&json!({"fields": {"kw": {"type": "keyword"}}})),
                )?;
                Ok(())
            };
            run_restart(&api, "acceptance-restart-loses", &mut restart)
        })
        .await
        .expect("join")
        .expect_err("a restart that loses the data must fail");
        assert!(
            format!("{err:#}").contains("after restart the hits are"),
            "{err:#}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_restart_reports_a_failed_restart() {
        let (_server, url) = serve_for_test();
        let err = tokio::task::spawn_blocking(move || -> Result<()> {
            let api = HttpApi::new(&url)?;
            let mut restart = || -> Result<()> { bail!("restart boom") };
            run_restart(&api, "acceptance-restart-boom", &mut restart)
        })
        .await
        .expect("join")
        .expect_err("a failed restart fails the phase");
        assert!(format!("{err:#}").contains("restart boom"), "{err:#}");
    }
}
