//! The process-wide background merge statics: one shared scheduler per
//! canonical checkpoint root, and the single merge worker thread with its job
//! queue.

use crate::persistence::application::background_merge::{Job, MergeOutcome, RootWork};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex, OnceLock};

pub(in crate::persistence) fn shared(root: &Path) -> Result<Arc<RootWork>> {
    // A loaded Engine may outlive all store handles. Keep its weak reader pins
    // discoverable by a later handle for the same canonical root.
    static ROOTS: OnceLock<Mutex<HashMap<PathBuf, Arc<RootWork>>>> = OnceLock::new();
    let root = std::fs::canonicalize(root)?;
    let mut roots = ROOTS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    roots.retain(|_, state| Arc::strong_count(state) > 1 || state.has_live_ownership());
    if let Some(state) = roots.get(&root) {
        return Ok(state.clone());
    }
    let state = Arc::new(RootWork::default());
    roots.insert(root, state.clone());
    Ok(state)
}

pub(in crate::persistence) fn queue_sender() -> Result<&'static mpsc::Sender<Job>> {
    static WORKER: OnceLock<std::result::Result<mpsc::Sender<Job>, String>> = OnceLock::new();
    WORKER
        .get_or_init(|| {
            let (sender, receiver) = mpsc::channel::<Job>();
            std::thread::Builder::new()
                .name("lumen-segment-merge".into())
                .spawn(move || {
                    while let Ok(mut job) = receiver.recv() {
                        let work = job.store.background.clone();
                        {
                            let mut state = work.state.lock().unwrap_or_else(|p| p.into_inner());
                            if let Some((engine, fence)) = state.next_owner.take() {
                                job.engine = engine;
                                job.store.publication_fence = fence;
                            }
                            state.queued = false;
                            state.running = true;
                            state.requested = false;
                        }
                        let engine = job.engine.upgrade();
                        let mut result =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                engine
                                    .as_ref()
                                    .map_or(Ok(MergeOutcome::NoEligibleWork), |engine| {
                                        job.store.merge_one(engine)
                                    })
                            }))
                            .unwrap_or_else(|_| {
                                Err(anyhow!("background merge worker panicked; files retained"))
                            });
                        let again = work.finish_job(&mut result, engine.is_some());
                        if let Err(error) = &result {
                            tracing::warn!(%error, "background segment merge failed");
                        }
                        if again {
                            if let Some(engine) = engine {
                                if let Ok(queue) = queue_sender() {
                                    if queue
                                        .send(Job {
                                            store: job.store,
                                            engine: Arc::downgrade(&engine),
                                        })
                                        .is_ok()
                                    {
                                        continue;
                                    }
                                }
                                let mut state =
                                    work.state.lock().unwrap_or_else(|p| p.into_inner());
                                state.queued = false;
                                state.error = Some("background merge queue stopped".into());
                                work.changed.notify_all();
                            }
                        }
                    }
                })
                .map_err(|error| error.to_string())?;
            Ok(sender)
        })
        .as_ref()
        .map_err(|error| anyhow!("start background merge worker: {error}"))
}
