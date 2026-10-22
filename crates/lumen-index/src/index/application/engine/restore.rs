//! Snapshot and restore: every collection's state as one self-contained
//! snapshot, restore from one, activation of a fully prepared replacement
//! engine, and the fields a reopen could not restore.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use anyhow::{anyhow, bail, Result};

use crate::index::application::engine::Engine;
use crate::index::domain::collection::coverage::{FieldNotAudited, ReindexNeeded};
use crate::index::domain::collection::Collection;
use crate::index::infrastructure::snapshot_v1::{SnapshotV1, SNAPSHOT_VERSION};

impl Engine {
    /// Snapshot every collection's in-memory state. The result is a
    /// fully self-contained, deterministically-orderable JSON document
    /// that `restore` can replay into a fresh `Engine`.
    pub fn snapshot(&self) -> Result<SnapshotV1> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let mut collections = BTreeMap::new();
        for (id, coll) in &state.collections {
            collections.insert(id.clone(), coll.to_snapshot()?);
        }
        Ok(SnapshotV1 {
            version: SNAPSHOT_VERSION,
            collections,
        })
    }

    /// Every field, across every collection, whose contents did not survive
    /// into the state this engine is holding. See [`ReindexNeeded`].
    ///
    /// Ordered by collection then field. Empty is the healthy answer, and it is
    /// a real answer: a caller can use it to clear a volume, which is why the
    /// probe reads live state instead of a flag set at reopen — a flag would
    /// keep reporting a field after the re-index that repaired it.
    pub fn reindex_needed(&self) -> Result<Vec<ReindexNeeded>> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let mut out: Vec<ReindexNeeded> = state
            .collections
            .iter()
            .flat_map(|(id, coll)| coll.reindex_needed(id))
            .collect();
        out.sort_by(|a, b| (&a.collection, &a.field).cmp(&(&b.collection, &b.field)));
        Ok(out)
    }

    /// Every field [`Engine::reindex_needed`] did not examine, across every
    /// collection. See [`FieldNotAudited`].
    ///
    /// A caller reporting the empty verdict as "nothing needs re-indexing" has
    /// to report this beside it, or it is describing a partial audit as a
    /// complete one.
    pub fn fields_not_audited(&self) -> Result<Vec<FieldNotAudited>> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let mut out: Vec<FieldNotAudited> = state
            .collections
            .iter()
            .flat_map(|(id, coll)| coll.fields_not_audited(id))
            .collect();
        out.sort_by(|a, b| (&a.collection, &a.field).cmp(&(&b.collection, &b.field)));
        Ok(out)
    }

    /// Say out loud what a reopen could not restore.
    ///
    /// Called at the end of every path that populates state from durable
    /// bytes. The reopen itself is left alone — refusing it would strand the
    /// fields that ARE intact, and the operator needs the node up to re-index
    /// at all — so this log is the entire difference between a damaged field
    /// and a field nobody wrote.
    pub(super) fn report_reindex_needed(&self, source: &str) {
        match self.reindex_needed() {
            Ok(needed) => {
                for row in &needed {
                    tracing::error!(
                        collection = %row.collection,
                        field = %row.field,
                        documents_covered = row.documents_covered,
                        source = %source,
                        "field restored empty while the document census covers it; \
                         its contents did not survive and re-indexing is the only \
                         repair (`lumen inspect` reports the same list offline)"
                    );
                }
            }
            // A poisoned lock is the caller's problem to surface, not this
            // probe's to escalate: it is narration for a reopen that already
            // succeeded.
            Err(error) => tracing::warn!(%error, source, "could not audit the reopened state"),
        }
    }

    /// Atomically replace the engine's state with the given snapshot.
    /// Idempotency keys are not part of the snapshot — restored
    /// collections start with an empty deduplication window.
    pub fn restore(&self, snap: SnapshotV1) -> Result<()> {
        let _apply = self.capture_barrier.apply();
        // Every format up to the one this build writes is readable: the only
        // change so far REMOVED a field, and serde ignores what it does not
        // know. Refusing an older snapshot here would strand data a running
        // cluster wrote yesterday.
        if !(1..=SNAPSHOT_VERSION).contains(&snap.version) {
            bail!(
                "snapshot version mismatch: got {}, supported 1..={}",
                snap.version,
                SNAPSHOT_VERSION
            );
        }
        let mut collections: BTreeMap<String, Collection> = snap
            .collections
            .into_iter()
            .map(|(id, snap)| Collection::from_snapshot(snap).map(|c| (id, c)))
            .collect::<Result<_>>()?;
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        for coll in collections.values_mut() {
            coll.collection_generation = state.allocate_collection_generation()?;
        }
        let budget_cut = self
            .changes
            .owner
            .freeze()
            .map_err(|error| anyhow!("restore pending budget: {error:?}"))?;
        _apply.replace_epoch();
        state.collections = collections;
        let retired_charges = self.changes.records.discard_for_restore();
        self.changes
            .owner
            .publish_through(&budget_cut)
            .map_err(|error| anyhow!("restore pending budget cut: {error:?}"))?;
        drop(retired_charges);
        self.publish_storage_bytes(&state);
        drop(state);
        self.report_reindex_needed("restore");
        Ok(())
    }

    /// Atomically replace all active collection state with a fully prepared
    /// disposable engine.
    ///
    /// Durable restore builds and checkpoint-validates `replacement` before
    /// the on-disk commit point. This method acquires every fallible live-state
    /// lock before the swap, then moves the complete map in one step. An empty
    /// replacement therefore removes every old collection. Old reshard-prune
    /// accumulators are also cleared because they name the replaced dataset.
    pub fn activate_replacement(&self, replacement: Engine) -> Result<()> {
        let _apply = self.capture_barrier.apply();
        let Engine {
            state: replacement_state,
            changes: replacement_changes,
            checkpoint_root_guards: replacement_root_guards,
            ..
        } = replacement;
        let replacement_state = replacement_state
            .into_inner()
            .map_err(|_| anyhow!("replacement state poisoned"))?;
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let mut prune_accumulator = self
            .prune_accumulator
            .lock()
            .map_err(|_| anyhow!("prune accumulator poisoned"))?;

        let next_generation = state
            .next_collection_generation
            .max(replacement_state.next_collection_generation);
        let previous_cut = self
            .changes
            .owner
            .freeze()
            .map_err(|error| anyhow!("replace pending budget: {error:?}"))?;
        let candidate_cut = replacement_changes
            .owner
            .freeze()
            .map_err(|error| anyhow!("candidate pending budget: {error:?}"))?;
        for guard in replacement_root_guards
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            self.retain_checkpoint_root(guard);
        }
        _apply.replace_epoch();
        *state = replacement_state;
        let retired_charges = self.changes.records.discard_for_restore();
        self.changes
            .records
            .adopt_for_restore(replacement_changes.records.discard_for_restore());
        self.changes
            .owner
            .publish_through(&previous_cut)
            .map_err(|error| anyhow!("replace pending budget cut: {error:?}"))?;
        replacement_changes
            .owner
            .publish_through(&candidate_cut)
            .map_err(|error| anyhow!("candidate pending budget cut: {error:?}"))?;
        drop(retired_charges);
        state.next_collection_generation = next_generation;
        prune_accumulator.clear();
        self.prune_accum_tick.store(0, Ordering::Release);
        self.publish_storage_bytes(&state);
        Ok(())
    }
}
