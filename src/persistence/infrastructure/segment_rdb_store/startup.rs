//! Cold start: classifying every root entry before anything is written,
//! adopting an exact 0.4.28 generation once, sweeping abandoned staging and
//! reporting the decision the binary logs.

use crate::persistence::infrastructure::segment_rdb_store::manifest_io::sync_directory;
use crate::persistence::infrastructure::segment_rdb_store::records::{
    is_known_staging_name, is_legacy_aside_name, parse_legacy_aside_name, parse_legacy_name,
    parse_revision_name, root_entry_kind,
};
use crate::persistence::infrastructure::segment_rdb_store::{
    SegmentRdbStore, AOF_COMPACT_TEMP_FILE, AOF_FILE, CONTAINER_VOLUME_SEED_FILE, CURRENT_FILE,
    CURRENT_TEMP_FILE, EXT_FILESYSTEM_METADATA_DIR, HNSW_GRAPH_CACHE_DIR,
};
use anyhow::{bail, Context, Result};
use storage_durable::{CurrentReadErrorKind, GenerationName};

/// The durable checkpoint decision made before Lumen starts accepting work.
///
/// This is intentionally about the checkpoint root only. The binary logs the
/// separate AOF replay decision after it applies the checkpoint baseline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentStartupDecision {
    InitializedEmptyRoot,
    RecoveredUncommittedEmpty,
    RestoredCurrentEmpty,
    RestoredCurrentGeneration,
    AdoptedLegacy0428,
}

impl SegmentStartupDecision {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InitializedEmptyRoot => "initialized_empty_root",
            Self::RecoveredUncommittedEmpty => "recovered_uncommitted_empty",
            Self::RestoredCurrentEmpty => "restored_current_empty",
            Self::RestoredCurrentGeneration => "restored_current_generation",
            Self::AdoptedLegacy0428 => "adopted_legacy_0428",
        }
    }
}

/// The exact successful checkpoint-root state selected during cold start.
#[derive(Clone, Debug)]
pub struct SegmentStartupOutcome {
    pub decision: SegmentStartupDecision,
    pub checkpoint_sequence: Option<u64>,
    pub generation: Option<GenerationName>,
    pub recovered_legacy_aside: bool,
    pub staging_cleaned: usize,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum StartupBootstrap {
    ExistingCurrent {
        staging_cleaned: usize,
    },
    InitializedEmpty {
        recovered_uncommitted: bool,
        staging_cleaned: usize,
    },
    Legacy {
        recovered_legacy_aside: bool,
        staging_cleaned: usize,
    },
}

#[derive(Debug, Default)]
pub(super) struct RootInventory {
    non_seed_entries: usize,
    pub(super) revision_generations: Vec<String>,
    has_aof_log: bool,
    has_aof_compact_temp: bool,
    has_graph_cache: bool,
}

impl SegmentRdbStore {
    /// Inspect every direct child before any root mutation. The segment root is
    /// Lumen-owned, but a wrong mount or an older unsupported layout must still
    /// fail loudly instead of being converted into a fresh empty store.
    pub(super) fn inventory_root(&self) -> Result<RootInventory> {
        let mut inventory = RootInventory::default();
        let mut children = Vec::new();
        let mut violations = Vec::new();
        let mut entries = std::fs::read_dir(&self.root)
            .with_context(|| format!("read checkpoint root {}", self.root.display()))?
            .collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.file_name());

        for entry in entries {
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path)
                .with_context(|| format!("inspect checkpoint root entry {}", path.display()))?;
            let kind = root_entry_kind(&metadata);
            let raw = match entry.file_name().into_string() {
                Ok(raw) => raw,
                Err(name) => {
                    let display = format!("{name:?}");
                    children.push(format!("{display} ({kind})"));
                    violations.push(format!("checkpoint root has non-UTF-8 entry {display}"));
                    continue;
                }
            };
            children.push(format!("{raw} ({kind})"));
            // This optional cache cannot authorize empty initialization. With
            // CURRENT present, a malformed cache remains ignorable. Readers
            // and writers independently refuse cache symlinks.
            if raw == HNSW_GRAPH_CACHE_DIR {
                inventory.has_graph_cache = true;
                continue;
            }
            let regular_file = !metadata.file_type().is_symlink() && metadata.is_file();
            let real_directory = !metadata.file_type().is_symlink() && metadata.is_dir();

            if raw != CONTAINER_VOLUME_SEED_FILE && raw != EXT_FILESYSTEM_METADATA_DIR {
                inventory.non_seed_entries += 1;
            }

            if raw == EXT_FILESYSTEM_METADATA_DIR {
                if !real_directory {
                    violations.push(format!(
                        "{EXT_FILESYSTEM_METADATA_DIR} must be a real empty directory: {}",
                        path.display()
                    ));
                } else {
                    match std::fs::read_dir(&path) {
                        Ok(mut contents) => match contents.next() {
                            None => {}
                            Some(Ok(_)) => violations.push(format!(
                                "{EXT_FILESYSTEM_METADATA_DIR} must be empty: {}",
                                path.display()
                            )),
                            Some(Err(error)) => violations.push(format!(
                                "cannot verify {EXT_FILESYSTEM_METADATA_DIR} is empty at {}: {error}",
                                path.display()
                            )),
                        },
                        Err(error) => violations.push(format!(
                            "cannot inspect {EXT_FILESYSTEM_METADATA_DIR} at {}: {error}",
                            path.display()
                        )),
                    }
                }
                continue;
            }

            if matches!(
                raw.as_str(),
                CURRENT_FILE
                    | CURRENT_TEMP_FILE
                    | AOF_FILE
                    | AOF_COMPACT_TEMP_FILE
                    | CONTAINER_VOLUME_SEED_FILE
            ) {
                if !regular_file {
                    violations.push(format!(
                        "checkpoint root entry must be a regular file: {}",
                        path.display()
                    ));
                } else if raw == AOF_FILE {
                    inventory.has_aof_log = true;
                } else if raw == AOF_COMPACT_TEMP_FILE {
                    inventory.has_aof_compact_temp = true;
                }
                continue;
            }
            if parse_legacy_name(&raw).is_some() {
                if !real_directory {
                    violations.push(format!(
                        "legacy checkpoint must be a real directory: {}",
                        path.display()
                    ));
                }
                continue;
            }
            if parse_revision_name(&raw).is_some() {
                if !real_directory {
                    violations.push(format!(
                        "segment generation must be a real directory: {}",
                        path.display()
                    ));
                } else {
                    inventory.revision_generations.push(raw);
                }
                continue;
            }
            if is_legacy_aside_name(&raw) {
                if !real_directory {
                    violations.push(format!(
                        "legacy aside must be a real directory: {}",
                        path.display()
                    ));
                }
                continue;
            }
            if is_known_staging_name(&raw) {
                if !real_directory {
                    violations.push(format!(
                        "checkpoint staging must be a real directory: {}",
                        path.display()
                    ));
                }
                continue;
            }
            violations.push(format!(
                "unrecognized non-empty segment checkpoint root entry `{raw}` at {}",
                path.display()
            ));
        }

        if inventory.has_aof_compact_temp && !inventory.has_aof_log {
            violations.push(format!(
                "{AOF_COMPACT_TEMP_FILE} requires regular {AOF_FILE} beside it"
            ));
        }
        if !violations.is_empty() {
            bail!(
                "invalid segment checkpoint root inventory [{}]; refusing to initialize CURRENT: {}",
                children.join(", "),
                violations.join("; ")
            );
        }
        Ok(inventory)
    }

    /// Establish the one permitted missing-`CURRENT` state before a caller can
    /// save or reopen. This runs under `save_gate` and mutates only after the
    /// full direct-child inventory has accepted the root.
    pub(super) fn prepare_startup_root(&self) -> Result<StartupBootstrap> {
        let inventory = self.inventory_root()?;
        match self.generations.read_current() {
            Ok(_) => Ok(StartupBootstrap::ExistingCurrent {
                staging_cleaned: self.sweep_abandoned_staging()?,
            }),
            Err(error) if error.kind == CurrentReadErrorKind::Missing => {
                if let Some(name) = inventory.revision_generations.first() {
                    bail!(
                        "CURRENT is missing but root contains unpointed revision generation `{name}`; refusing to select or initialize it"
                    );
                }
                if inventory.has_graph_cache {
                    bail!(
                        "CURRENT is missing beside an optional HNSW graph cache; refusing initialization or cleanup without durable authority"
                    );
                }
                let recovered_legacy_aside = self.reconcile_legacy_asides()?;
                let staging_cleaned = self.sweep_abandoned_staging()?;
                if !self.legacy_records()?.is_empty() {
                    return Ok(StartupBootstrap::Legacy {
                        recovered_legacy_aside,
                        staging_cleaned,
                    });
                }
                self.generations
                    .initialize_empty()
                    .map_err(anyhow::Error::new)
                    .context("initialize empty segment generation store")?;
                Ok(StartupBootstrap::InitializedEmpty {
                    recovered_uncommitted: inventory.non_seed_entries > 0,
                    staging_cleaned,
                })
            }
            Err(error) => Err(anyhow::Error::new(error).context("read CURRENT")),
        }
    }

    pub(super) fn sweep_abandoned_staging(&self) -> Result<usize> {
        let mut removed = 0usize;
        for entry in std::fs::read_dir(&self.root)
            .with_context(|| format!("read checkpoint root {}", self.root.display()))?
        {
            let entry = entry?;
            let Some(raw) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !is_known_staging_name(&raw) {
                continue;
            }
            if self.background.protects(&raw) {
                continue;
            }
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path)
                .with_context(|| format!("inspect checkpoint staging {}", path.display()))?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                bail!(
                    "checkpoint staging must be a real directory: {}",
                    path.display()
                );
            }
            std::fs::remove_dir_all(&path)
                .with_context(|| format!("remove abandoned staging {}", path.display()))?;
            removed += 1;
        }
        if removed > 0 {
            sync_directory(&self.root).context("fsync root after staging cleanup")?;
        }
        Ok(removed)
    }

    fn reconcile_legacy_asides(&self) -> Result<bool> {
        let mut asides = Vec::new();
        for entry in std::fs::read_dir(&self.root)
            .with_context(|| format!("read checkpoint root {}", self.root.display()))?
        {
            let entry = entry?;
            let Some(raw) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(sequence) = parse_legacy_aside_name(&raw) else {
                continue;
            };
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path)
                .with_context(|| format!("inspect legacy aside {}", path.display()))?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                bail!("legacy aside must be a real directory: {}", path.display());
            }
            asides.push((sequence, path));
        }
        asides.sort_by_key(|(sequence, _)| *sequence);
        let mut changed = false;
        for (sequence, aside) in asides {
            let committed = self.root.join(format!("gen-{sequence}"));
            match std::fs::symlink_metadata(&committed) {
                Ok(metadata) if !metadata.file_type().is_symlink() && metadata.is_dir() => {
                    std::fs::remove_dir_all(&aside).with_context(|| {
                        format!("remove stale legacy aside {}", aside.display())
                    })?;
                }
                Ok(_) => {
                    bail!(
                        "legacy checkpoint target must be a real directory: {}",
                        committed.display()
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::rename(&aside, &committed).with_context(|| {
                        format!(
                            "restore legacy checkpoint {} -> {}",
                            aside.display(),
                            committed.display()
                        )
                    })?;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("inspect legacy checkpoint {}", committed.display())
                    });
                }
            }
            changed = true;
        }
        if changed {
            sync_directory(&self.root).context("fsync root after legacy aside recovery")?;
        }
        Ok(changed)
    }
}
