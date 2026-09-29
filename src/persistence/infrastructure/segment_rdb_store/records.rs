//! Generation records: parsing generation, revision, legacy and staging names,
//! resolving a name to its record, and validating a record before a reopen.

use crate::persistence::infrastructure::segment_rdb_store::generation_validation::validate_generation_layout;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::read_generation_manifest;
use crate::persistence::infrastructure::segment_rdb_store::{
    GenerationRecord, SegmentRdbStore, GENERATION_MANIFEST_FILE, GENERATION_MANIFEST_V2,
    GENERATION_MANIFEST_V3,
};
use crate::storage::{Engine, RecoveryPhase};
use anyhow::{anyhow, bail, Context, Result};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use storage_durable::GenerationName;

impl SegmentRdbStore {
    pub(in crate::persistence) fn record_for_name(
        &self,
        name: GenerationName,
    ) -> Result<GenerationRecord> {
        let path = self.generations.generation_path(&name);
        let metadata = std::fs::symlink_metadata(&path)
            .with_context(|| format!("inspect segment generation {}", path.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!(
                "segment generation must be a real directory: {}",
                path.display()
            );
        }
        if let Some(sequence) = parse_legacy_name(name.as_str()) {
            if path.join(GENERATION_MANIFEST_FILE).exists() {
                bail!(
                    "legacy generation {} unexpectedly contains {}",
                    name,
                    GENERATION_MANIFEST_FILE
                );
            }
            return Ok(GenerationRecord {
                name,
                path,
                sequence,
                revision: 0,
                legacy: true,
                previous: None,
            });
        }

        let (sequence, revision) = parse_revision_name(name.as_str())
            .ok_or_else(|| anyhow!("CURRENT names an unsupported generation `{name}`"))?;
        let manifest = read_generation_manifest(&path)?;
        if manifest.schema_version != 1
            && !matches!(
                manifest.schema_version,
                GENERATION_MANIFEST_V2 | GENERATION_MANIFEST_V3
            )
        {
            bail!(
                "generation {} has unsupported manifest schema {}",
                name,
                manifest.schema_version
            );
        }
        if manifest.checkpoint_sequence != sequence || manifest.revision != revision {
            bail!(
                "generation {} manifest does not match its directory name",
                name
            );
        }
        let previous = match manifest.previous {
            Some(raw) => {
                if !is_supported_generation_name(&raw) {
                    bail!("generation {name} has unsupported predecessor `{raw}`");
                }
                if !is_older_predecessor(&raw, sequence, revision) {
                    bail!("generation {name} has non-predecessor link `{raw}`");
                }
                Some(
                    GenerationName::parse(raw)
                        .map_err(anyhow::Error::new)
                        .context("parse previous segment generation")?,
                )
            }
            None => None,
        };
        if previous.as_ref() == Some(&name) {
            bail!("generation {name} points to itself as predecessor");
        }
        Ok(GenerationRecord {
            name,
            path,
            sequence,
            revision,
            legacy: false,
            previous,
        })
    }

    pub(super) fn record_if_present(
        &self,
        name: GenerationName,
    ) -> Result<Option<GenerationRecord>> {
        let path = self.generations.generation_path(&name);
        match std::fs::symlink_metadata(&path) {
            Ok(_) => self.record_for_name(name).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => {
                Err(error).with_context(|| format!("inspect segment generation {}", path.display()))
            }
        }
    }

    pub(super) fn generation_entries(&self) -> Result<Vec<(String, PathBuf)>> {
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(&self.root)
            .with_context(|| format!("read checkpoint root {}", self.root.display()))?
        {
            let entry = entry?;
            let Some(raw) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if parse_legacy_name(&raw).is_none() && parse_revision_name(&raw).is_none() {
                continue;
            }
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path)
                .with_context(|| format!("inspect segment generation {}", path.display()))?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                bail!(
                    "segment generation must be a real directory: {}",
                    path.display()
                );
            }
            entries.push((raw, path));
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(entries)
    }

    pub(super) fn legacy_records(&self) -> Result<Vec<GenerationRecord>> {
        let mut records = Vec::new();
        for (raw, _) in self.generation_entries()? {
            if parse_legacy_name(&raw).is_none() {
                continue;
            }
            let name = GenerationName::parse(raw)
                .map_err(anyhow::Error::new)
                .context("parse legacy segment generation")?;
            records.push(self.record_for_name(name)?);
        }
        records.sort_by_key(|record| record.sequence);
        Ok(records)
    }

    pub(super) fn reopen_record(
        &self,
        engine: &Arc<Engine>,
        record: &GenerationRecord,
    ) -> Result<u64> {
        self.reopen_record_with_graph_policy(engine, record, false)
    }

    pub(super) fn reopen_record_with_graph_policy(
        &self,
        engine: &Arc<Engine>,
        record: &GenerationRecord,
        defer_until_aof: bool,
    ) -> Result<u64> {
        self.reset_recovery_profile();
        let layout_started = self.recovery_profile.enabled().then(Instant::now);
        let collections = self
            .recovery_profile
            .phase_start(RecoveryPhase::LayoutValidation, || {
                validate_generation_layout(record)
            })?;
        if let Some(started) = layout_started {
            self.add_recovery_timing(|timings| {
                timings.layout_validation_ms = started.elapsed().as_millis() as u64;
            });
        }
        let replacement = Engine::new();
        self.reopen_once_with_graph_policy(&replacement, record, collections, defer_until_aof)?;
        self.retain_root_for(&replacement);
        // Decode and build backends once, before touching the caller. Activation
        // is one apply interval and invalidates captures from its former epoch.
        self.background
            .pin_loaded_engine(record.name.as_str().to_owned(), &replacement)?;
        engine.activate_replacement(replacement)?;
        if !defer_until_aof {
            self.emit_recovery_profile();
        }
        Ok(record.sequence)
    }

    /// Validate a candidate without installing it, for legacy adoption.
    pub(super) fn validate_record(&self, record: &GenerationRecord) -> Result<usize> {
        let collections = validate_generation_layout(record)?;
        let verifier = Engine::new();
        self.reopen_once(&verifier, record, collections)?;
        Ok(collections)
    }

    pub(super) fn reopen_once(
        &self,
        engine: &Engine,
        record: &GenerationRecord,
        collections: usize,
    ) -> Result<()> {
        self.reopen_once_with_graph_policy(engine, record, collections, false)
    }
}

fn parse_canonical_u64(raw: &str) -> Option<u64> {
    let value = raw.parse::<u64>().ok()?;
    (value.to_string() == raw).then_some(value)
}

pub(super) fn parse_legacy_name(raw: &str) -> Option<u64> {
    parse_canonical_u64(raw.strip_prefix("gen-")?)
}

pub(super) fn parse_legacy_aside_name(raw: &str) -> Option<u64> {
    raw.strip_prefix("gen-")?
        .strip_suffix(".old")
        .and_then(parse_canonical_u64)
}

pub(super) fn root_entry_kind(metadata: &std::fs::Metadata) -> &'static str {
    if metadata.file_type().is_symlink() {
        "symlink"
    } else if metadata.is_file() {
        "regular file"
    } else if metadata.is_dir() {
        "directory"
    } else {
        "special file"
    }
}

pub(super) fn is_legacy_aside_name(raw: &str) -> bool {
    parse_legacy_aside_name(raw).is_some()
}

pub(super) fn is_known_staging_name(raw: &str) -> bool {
    raw.strip_prefix(".gen-")
        .and_then(|name| name.strip_suffix(".tmp"))
        .and_then(parse_canonical_u64)
        .is_some()
        || raw
            .strip_prefix(".stage-")
            .and_then(parse_revision_name)
            .is_some()
}

pub(super) fn parse_revision_name(raw: &str) -> Option<(u64, u64)> {
    let rest = raw.strip_prefix("gen-")?;
    let (sequence, revision) = rest.rsplit_once("-rev-")?;
    Some((
        parse_canonical_u64(sequence)?,
        parse_canonical_u64(revision)?,
    ))
}

fn is_supported_generation_name(raw: &str) -> bool {
    parse_legacy_name(raw).is_some() || parse_revision_name(raw).is_some()
}

pub(super) fn is_older_predecessor(
    raw: &str,
    current_sequence: u64,
    current_revision: u64,
) -> bool {
    if let Some(sequence) = parse_legacy_name(raw) {
        sequence <= current_sequence
    } else if let Some((sequence, revision)) = parse_revision_name(raw) {
        sequence <= current_sequence && revision < current_revision
    } else {
        false
    }
}

pub(super) fn definitely_unpointed_after(raw: &str, current: &GenerationRecord) -> bool {
    if let Some(sequence) = parse_legacy_name(raw) {
        sequence > current.sequence
    } else if let Some((sequence, revision)) = parse_revision_name(raw) {
        current.legacy || sequence > current.sequence || revision >= current.revision
    } else {
        false
    }
}
