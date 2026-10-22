//! The bounded bridge that runs synchronous Engine searches on blocking
//! threads, with the one process-wide permit pool every HTTP and routed search
//! leg shares.

use std::sync::{Arc, OnceLock};

use anyhow::Result;
use tokio::sync::Semaphore;

/// One process-wide permit pool for all HTTP and routed synchronous search
/// legs. `RoutedRouter` is constructed separately from `AppState`, so keeping
/// this at the bridge seam prevents each router from silently multiplying the
/// configured blocking-work budget.
static SEARCH_EXECUTOR_PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();

/// Bounded bridge for the synchronous search engine. HTTP handlers must not
/// run CPU-bound Engine work on Tokio's reactor workers: one long sort would
/// otherwise prevent unrelated searches and readiness probes from being
/// polled. The bound limits blocking work per serving component without
/// adding a public configuration knob.
#[derive(Clone)]
pub(crate) struct BlockingSearchExecutor {
    permits: Arc<Semaphore>,
}

impl BlockingSearchExecutor {
    pub(crate) fn new() -> Self {
        Self {
            permits: SEARCH_EXECUTOR_PERMITS
                .get_or_init(|| {
                    let permits = std::thread::available_parallelism()
                        .map(|parallelism| parallelism.get())
                        .unwrap_or(1)
                        .clamp(1, 8);
                    Arc::new(Semaphore::new(permits))
                })
                .clone(),
        }
    }

    pub(crate) async fn run<T, F>(&self, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("search executor is closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work()
        })
        .await
        .map_err(|error| anyhow::anyhow!("search worker failed: {error}"))?
    }
}
