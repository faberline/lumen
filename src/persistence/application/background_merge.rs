//! One process worker compacts immutable fields outside the checkpoint lock.
//! A root token owns its source/staging pins until publication or refusal.
//! Publication rebases exact inputs onto the newest complete CURRENT catalog.

pub(crate) mod link;
pub(crate) mod publish;
pub(crate) mod root_work;

use crate::persistence::domain::generation_manifest::{
    SegmentGenerationManifest, SegmentKind, SegmentRole,
};
use crate::persistence::infrastructure::merge_worker::queue_sender;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::storage::Engine;
use anyhow::{bail, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Condvar;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use storage_durable::StagedGeneration;

#[derive(Default)]
pub(in crate::persistence) struct WorkState {
    pub(in crate::persistence) queued: bool,
    pub(in crate::persistence) running: bool,
    pub(in crate::persistence) requested: bool,
    ordinary_retry_permits: u8,
    retryable_stale: bool,
    published_revision: u64,
    publication_revision_overflowed: bool,
    #[cfg(test)]
    wait_entries: u64,
    pub(in crate::persistence) next_owner: Option<(
        Weak<Engine>,
        Option<crate::segment_capacity::PublicationFence>,
    )>,
    pub(in crate::persistence) error: Option<String>,
    protected: BTreeMap<String, usize>,
    // A proved predecessor deferred only because a reader still owned it.
    // Later pruning may have shortened the visible chain in the meantime.
    retired: BTreeSet<String>,
    readers: Vec<(String, Vec<Weak<SegmentReader>>)>,
}

#[derive(Default)]
pub(in crate::persistence) struct RootWork {
    pub(in crate::persistence) state: Mutex<WorkState>,
    pub(in crate::persistence) changed: Condvar,
}

/// One point-in-time view of the root-local merge scheduler. It is copied
/// into the checkpoint diagnostic trace after a durable publication.
#[derive(Clone, Copy)]
pub(in crate::persistence) struct TraceState {
    pub(in crate::persistence) queued: bool,
    pub(in crate::persistence) running: bool,
    pub(in crate::persistence) requested: bool,
    pub(in crate::persistence) published_revision: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(in crate::persistence) enum CapacityWait {
    Published,
    Idle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::persistence) enum MergeOutcome {
    Published,
    RetryableStale,
    NoEligibleWork,
}

pub(in crate::persistence) struct Job {
    pub(in crate::persistence) store: SegmentRdbStore,
    pub(in crate::persistence) engine: Weak<Engine>,
}

fn identities(
    manifest: &SegmentGenerationManifest,
) -> BTreeMap<String, crate::storage::CheckpointCollectionIdentity> {
    manifest
        .collections
        .iter()
        .map(|collection| {
            (
                collection.collection_id.clone(),
                crate::storage::CheckpointCollectionIdentity {
                    generation: collection.collection_generation,
                    data_version: collection.data_version,
                    schema_version: collection.schema_version,
                },
            )
        })
        .collect()
}

pub(in crate::persistence) fn needs_merge(manifest: &SegmentGenerationManifest) -> bool {
    manifest.collections.iter().any(|collection| {
        let mut counts = BTreeMap::new();
        collection.segments.iter().any(|segment| {
            if segment.role != SegmentRole::Field || segment.kind != SegmentKind::Delta {
                return false;
            }
            let count = counts.entry(segment.field.as_deref()).or_insert(0);
            *count += 1;
            *count >= 4
        })
    })
}

impl SegmentRdbStore {
    /// Remove only this merge's owned scratch under the root save gate. On a
    /// removal error, leave both pins in place so no later sweep can treat the
    /// incomplete merge as abandoned work.
    fn cleanup_owned_merge_staging(
        &self,
        scratch_path: &Path,
        scratch_name: &str,
        source_name: &str,
        scratch: StagedGeneration,
    ) -> Result<()> {
        self.cleanup_owned_merge_staging_with(
            scratch_path,
            scratch_name,
            source_name,
            scratch,
            |path| std::fs::remove_dir_all(path),
        )
    }

    fn cleanup_owned_merge_staging_with(
        &self,
        scratch_path: &Path,
        scratch_name: &str,
        source_name: &str,
        scratch: StagedGeneration,
        remove: impl FnOnce(&Path) -> std::io::Result<()>,
    ) -> Result<()> {
        let _cleanup_guard = self.save_gate.lock_owned();
        match std::fs::symlink_metadata(scratch_path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    bail!(
                        "owned background merge staging must be a real directory: {}",
                        scratch_path.display()
                    );
                }
                if let Err(error) = remove(scratch_path) {
                    return Err(error).context("remove owned background merge staging");
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "inspect owned background merge staging {}",
                        scratch_path.display()
                    )
                });
            }
        }
        self.background.unpin(scratch_name);
        self.background.unpin(source_name);
        drop(scratch);
        Ok(())
    }

    pub(in crate::persistence) fn request_merge(&self, engine: &Arc<Engine>) -> Result<()> {
        self.request_merge_with_revision(engine).map(|_| ())
    }

    pub(in crate::persistence) fn request_merge_with_revision(
        &self,
        engine: &Arc<Engine>,
    ) -> Result<u64> {
        let queue = queue_sender()?;
        let mut state = self
            .background
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let revision = state.published_revision;
        if state.publication_revision_overflowed {
            bail!("background segment merge publication revision overflowed");
        }
        if state.running || state.queued {
            state.ordinary_retry_permits = state.ordinary_retry_permits.saturating_add(1);
            return Ok(revision);
        }
        state.retryable_stale = false;
        state.error = None;
        state.next_owner = Some((Arc::downgrade(engine), self.publication_fence.clone()));
        state.queued = true;
        if queue
            .send(Job {
                store: self.clone(),
                engine: Arc::downgrade(engine),
            })
            .is_err()
        {
            state.queued = false;
            state.error = Some("background merge queue stopped".into());
            self.background.changed.notify_all();
            bail!("background merge queue stopped");
        }
        Ok(revision)
    }

    pub(in crate::persistence) fn request_merge_for_capacity_retry(
        &self,
        engine: &Arc<Engine>,
        idle_revision: Option<u64>,
        deadline: Option<Instant>,
    ) -> Result<u64> {
        let queue = queue_sender()?;
        let mut state = self
            .background
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        state.validate_capacity_retry(idle_revision, deadline)?;
        state.retryable_stale = false;
        let revision = state.published_revision;
        state.next_owner = Some((Arc::downgrade(engine), self.publication_fence.clone()));
        if state.running {
            state.requested = true;
            return Ok(revision);
        }
        if state.queued {
            return Ok(revision);
        }
        state.error = None;
        state.queued = true;
        if queue
            .send(Job {
                store: self.clone(),
                engine: Arc::downgrade(engine),
            })
            .is_err()
        {
            state.queued = false;
            state.error = Some("background merge queue stopped".into());
            self.background.changed.notify_all();
            bail!("background merge queue stopped");
        }
        Ok(revision)
    }

    /// Wait for all currently requested merges on this root. Checkpoint save
    /// completion alone does not imply background compaction completion.
    #[doc(hidden)]
    pub fn wait_for_merges(&self, timeout: Duration) -> Result<()> {
        self.background.wait(timeout)
    }

    pub(in crate::persistence) fn needs_delta_capacity(
        &self,
        engine: &Arc<Engine>,
        manifest: &SegmentGenerationManifest,
        current_root: &Path,
    ) -> Result<bool> {
        let dirty = engine.checkpoint_reusable_dirty_fields(current_root)?;
        for collection in &manifest.collections {
            let Some((generation, schema, fields)) = dirty.get(&collection.collection_id) else {
                continue;
            };
            if *generation != collection.collection_generation
                || *schema != collection.schema_version
            {
                continue;
            }
            for field in fields {
                if collection
                    .segments
                    .iter()
                    .filter(|segment| {
                        segment.role == SegmentRole::Field
                            && segment.kind == SegmentKind::Delta
                            && segment.field.as_ref() == Some(field)
                    })
                    .count()
                    >= 16
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests;
