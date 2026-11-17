//! The `checkpoint_and_merge` functional phase. With `--spawn` it forces one checkpoint on
//! the owned `lumen serve` child, reads the checkpoint and merge counters from `/metrics`
//! before and after, restarts the child and proves that the same search returns the same
//! hits. Merge gets no trigger: the phase only records the merge counter it observes.

use std::path::Path;
use std::sync::Mutex;

use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value};

use crate::http::HttpApi;
use crate::lifecycle::search;
use crate::metrics::observe;
use crate::run::{Observed, Phase};
use crate::spawn::SharedChild;

pub(super) struct CheckpointAndMerge {
    child: SharedChild,
    observed: Mutex<Observed>,
}

impl CheckpointAndMerge {
    pub(super) fn new(child: SharedChild) -> Self {
        Self {
            child,
            observed: Mutex::new(Observed::default()),
        }
    }
}

impl Phase for CheckpointAndMerge {
    fn name(&self) -> &str {
        "checkpoint_and_merge"
    }

    fn run(&self, endpoint: &str, _artifact_dir: &Path) -> Result<()> {
        let api = HttpApi::new(endpoint)?;
        let collection = format!("acceptance-checkpoint-{}", std::process::id());
        let mut restart = || -> Result<()> {
            let mut child = self
                .child
                .lock()
                .map_err(|_| anyhow!("child lock poisoned"))?;
            child.stop()?;
            child.start()
        };
        let observed = run_checkpoint(&api, &collection, &mut restart)?;
        *self
            .observed
            .lock()
            .map_err(|_| anyhow!("observed lock poisoned"))? = observed;
        Ok(())
    }

    fn observed(&self) -> Observed {
        match self.observed.lock() {
            Ok(guard) => *guard,
            Err(poisoned) => *poisoned.into_inner(),
        }
    }
}

/// Runs the checkpoint steps for one collection id. The steps are fixed by the spec.
/// Returns the counts observed from `/metrics` only when every step passed.
pub(super) fn run_checkpoint(
    api: &HttpApi,
    collection: &str,
    restart: &mut dyn FnMut() -> Result<()>,
) -> Result<Observed> {
    let base = format!("/collections/{collection}");
    let index = format!("{base}/index");
    let delete = format!("{base}?force=true");

    // 1. Best effort: remove a collection left by an earlier run, then create it and index
    //    two documents.
    let _ = api.call("DELETE", &delete, None);
    api.expect(
        "create",
        &[200],
        "PUT",
        &base,
        Some(&json!({"fields": {"kw": {"type": "keyword"}}})),
    )?;
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

    // 2. The hits before the checkpoint.
    let hits_before = sorted_ids(api, &base)?;
    if hits_before != ["doc-1", "doc-2"] {
        bail!("before the checkpoint the hits are {hits_before:?}, wanted doc-1, doc-2");
    }

    // 3. Read the counters, force one checkpoint, read them again.
    let before = api.text("/metrics")?;
    api.expect(
        "force checkpoint",
        &[200],
        "POST",
        "/admin/checkpoint",
        Some(&json!({})),
    )?;
    let after = api.text("/metrics")?;
    let observed = observe(&before, &after)?;

    // 4. A checkpoint that the counter does not show is a failure.
    if observed.checkpoint_count == 0 {
        bail!("checkpoint counter did not increase after POST /admin/checkpoint");
    }

    // 5. Restart the owned child on the same data dir and port, then the same search. The
    //    search retries like `restart_persistence`: a pooled connection to the old child is
    //    closed by the restart.
    restart()?;
    let hits_after = sorted_ids_retrying(api, &base)?;
    if hits_after != hits_before {
        bail!("after restart the hits are {hits_after:?}, wanted {hits_before:?}");
    }

    // 6. Best effort cleanup.
    let _ = api.call("DELETE", &delete, None);
    Ok(observed)
}

/// `sorted_ids` with up to three attempts, 200 ms apart.
fn sorted_ids_retrying(api: &HttpApi, base: &str) -> Result<Vec<String>> {
    let mut last = None;
    for attempt in 0..3 {
        match sorted_ids(api, base) {
            Ok(ids) => return Ok(ids),
            Err(err) => last = Some(err),
        }
        if attempt < 2 {
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }
    Err(last.unwrap_or_else(|| anyhow!("no attempt ran")))
}

/// The sorted `external_id`s of the documents whose keyword field `kw` is `red`.
fn sorted_ids(api: &HttpApi, base: &str) -> Result<Vec<String>> {
    let response: Value = search(api, base, json!({"term": {"field": "kw", "value": "red"}}))?;
    let mut ids: Vec<String> = response["hits"]
        .as_array()
        .map(|hits| {
            hits.iter()
                .filter_map(|hit| hit["external_id"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    Ok(ids)
}
