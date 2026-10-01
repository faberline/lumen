//! The active generation chain: the current record and its history, pruning
//! what the chain no longer names, and staging the next generation.

use crate::persistence::infrastructure::segment_rdb_store::manifest_io::sync_directory;
use crate::persistence::infrastructure::segment_rdb_store::records::{
    definitely_unpointed_after, parse_revision_name,
};
use crate::persistence::infrastructure::segment_rdb_store::{
    GenerationRecord, GenerationStaging, SegmentRdbStore, StagingSelection,
};
use anyhow::{anyhow, bail, Context, Result};
use std::collections::BTreeSet;
use storage_durable::{CurrentTarget, GenerationName, StagedGeneration};

impl SegmentRdbStore {
    /// Retain the active generation and up to `keep - 1` prior generations.
    ///
    /// `keep=0` still retains the active generation. Complete revisions newer
    /// than `CURRENT` are failed pre-commit attempts and are removed. The method
    /// never removes the directory named by `CURRENT`. If a prior prune already
    /// removed the predecessor named by an immutable manifest, this call keeps
    /// any unlinked older directories whose lineage it can no longer prove.
    pub fn prune(&self, keep: usize) -> Result<usize> {
        let _guard = self.save_gate.lock_owned();
        self.inventory_root()?;
        self.sweep_abandoned_staging()?;
        let (history, truncated) = self.active_history()?;
        let all = self.generation_entries()?;

        let retain: BTreeSet<_> = history
            .iter()
            .take(keep.max(1))
            .map(|record| record.name.as_str().to_owned())
            .collect();
        let active_chain: BTreeSet<_> = history
            .iter()
            .map(|record| record.name.as_str().to_owned())
            .collect();
        let current = history.first();

        let mut removed = 0usize;
        for (name, path) in all {
            if retain.contains(&name) {
                continue;
            }
            if self.background.protects(&name) {
                if active_chain.contains(&name) {
                    self.background.defer_reclaim(&name);
                }
                continue;
            }
            if truncated
                && !active_chain.contains(&name)
                && !self.background.known_retired(&name)
                && !current.is_some_and(|current| definitely_unpointed_after(&name, current))
            {
                continue;
            }
            std::fs::remove_dir_all(&path)
                .with_context(|| format!("remove segment generation {}", path.display()))?;
            self.background.reclaimed(&name);
            removed += 1;
        }
        if removed > 0 {
            sync_directory(&self.root).context("fsync checkpoint root after prune")?;
        }
        Ok(removed)
    }

    /// Activated predecessor-chain sequences, ascending and de-duplicated.
    pub fn generation_seqs(&self) -> Result<Vec<u64>> {
        Ok(self
            .active_history()?
            .0
            .into_iter()
            .map(|record| record.sequence)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    pub(in crate::persistence) fn current_record(&self) -> Result<Option<GenerationRecord>> {
        match self.generations.read_current() {
            Ok(CurrentTarget::Empty) => Ok(None),
            Ok(CurrentTarget::Generation(name)) => self.record_for_name(name).map(Some),
            Err(error) => Err(anyhow::Error::new(error).context("read CURRENT")),
        }
    }

    /// Return the exact activated predecessor chain, newest first. A missing
    /// predecessor is an allowed retention boundary because prune never mutates
    /// an immutable manifest merely to truncate its link.
    fn active_history(&self) -> Result<(Vec<GenerationRecord>, bool)> {
        let Some(mut record) = self.current_record()? else {
            return Ok((Vec::new(), false));
        };
        let mut visited = BTreeSet::new();
        let mut history = Vec::new();
        let mut truncated = false;

        loop {
            let name = record.name.as_str().to_owned();
            if !visited.insert(name.clone()) {
                bail!("segment generation predecessor cycle at `{name}`");
            }
            let next = if let Some(previous) = &record.previous {
                if let Some(previous_record) = self.record_if_present(previous.clone())? {
                    if previous_record.sequence > record.sequence
                        || previous_record.order_key() >= record.order_key()
                    {
                        bail!(
                            "generation {} has non-predecessor link {}",
                            record.name,
                            previous
                        );
                    }
                    Some(previous_record)
                } else {
                    truncated = true;
                    None
                }
            } else if record.legacy {
                self.legacy_records()?
                    .into_iter()
                    .filter(|candidate| candidate.legacy && candidate.sequence < record.sequence)
                    .max_by_key(|candidate| candidate.sequence)
            } else {
                None
            };
            history.push(record);
            let Some(next) = next else {
                break;
            };
            record = next;
        }
        Ok((history, truncated))
    }

    pub(in crate::persistence) fn begin_next_generation(
        &self,
        sequence: u64,
    ) -> Result<(u64, StagedGeneration)> {
        let (revision, staged) =
            self.begin_next_generation_selected(sequence, StagingSelection::Generic)?;
        match staged {
            GenerationStaging::Generic(staged) => Ok((revision, staged)),
            GenerationStaging::Current(_) => {
                unreachable!("generic staging selection returned current-derived stage")
            }
        }
    }

    pub(in crate::persistence) fn begin_background_merge_stage(
        &self,
        sequence: u64,
    ) -> Result<StagedGeneration> {
        let (_, staged) =
            self.begin_next_generation_selected(sequence, StagingSelection::BackgroundScratch)?;
        match staged {
            GenerationStaging::Generic(staged) => Ok(staged),
            GenerationStaging::Current(_) => {
                unreachable!("background scratch selection returned current-derived stage")
            }
        }
    }

    pub(in crate::persistence) fn begin_next_generation_selected(
        &self,
        sequence: u64,
        selection: StagingSelection,
    ) -> Result<(u64, GenerationStaging)> {
        let mut revision = match selection {
            // A background merge scratch tree is private and never becomes a
            // published generation. Keep it outside the published revision
            // sequence so the later atomic publication can use the next
            // revision without colliding with its own scratch directory.
            StagingSelection::BackgroundScratch => 0,
            StagingSelection::Generic | StagingSelection::CurrentIfDurable => self
                .generation_entries()?
                .into_iter()
                .filter_map(|(name, _)| parse_revision_name(&name).map(|(_, revision)| revision))
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| anyhow!("segment generation revision exhausted"))?,
        };

        loop {
            let name = GenerationName::parse(format!("gen-{sequence}-rev-{revision}"))
                .map_err(anyhow::Error::new)
                .context("build segment generation name")?;
            let staged = match selection {
                StagingSelection::Generic => self
                    .generations
                    .begin(name.clone())
                    .map(GenerationStaging::Generic),
                StagingSelection::CurrentIfDurable => {
                    #[cfg(unix)]
                    {
                        match self.generations.begin_from_current_if_durable(name.clone()) {
                            Ok(Some(staged)) => Ok(GenerationStaging::Current(staged)),
                            Ok(None) => self
                                .generations
                                .begin(name.clone())
                                .map(GenerationStaging::Generic),
                            Err(error) => Err(error),
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        self.generations
                            .begin(name.clone())
                            .map(GenerationStaging::Generic)
                    }
                }
                StagingSelection::BackgroundScratch => self
                    .generations
                    .begin(name.clone())
                    .map(GenerationStaging::Generic),
            };
            match staged {
                Ok(staged) => return Ok((revision, staged)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    revision = revision
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("segment generation revision exhausted"))?;
                }
                Err(error) => return Err(error).context("create segment generation staging"),
            }
        }
    }
}
