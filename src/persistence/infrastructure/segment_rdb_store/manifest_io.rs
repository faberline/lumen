//! Reading and writing `_generation.json`, including the legacy v1 form, and
//! registering the files a checkpoint inherits from its predecessor.

use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, SegmentGenerationManifest, SegmentReference,
};
use crate::persistence::infrastructure::segment_rdb_store::flat_layout::collection_output_relative;
use crate::persistence::infrastructure::segment_rdb_store::{
    CHECKPOINT_SCHEMA_FILE, GENERATION_MANIFEST_FILE,
};
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;
use storage_durable::CurrentGenerationStaging;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyV1GenerationManifest {
    pub(super) schema_version: u32,
    pub(super) sequence: u64,
    pub(super) revision: u64,
    pub(super) previous: Option<String>,
}

pub(in crate::persistence) fn write_generation_manifest(
    path: &Path,
    manifest: &SegmentGenerationManifest,
) -> Result<()> {
    let file = std::fs::File::create(path.join(GENERATION_MANIFEST_FILE))
        .with_context(|| format!("create generation manifest under {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, manifest).context("encode generation manifest")?;
    writer.write_all(b"\n")?;
    writer
        .flush()
        .with_context(|| format!("write generation manifest under {}", path.display()))
}

pub(in crate::persistence) fn read_generation_manifest(
    path: &Path,
) -> Result<SegmentGenerationManifest> {
    let manifest_path = path.join(GENERATION_MANIFEST_FILE);
    let metadata = std::fs::symlink_metadata(&manifest_path)
        .with_context(|| format!("inspect generation manifest {}", manifest_path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(
            "generation manifest must be a regular file: {}",
            manifest_path.display()
        );
    }
    // Probe only the discriminator. Serde skips the collection tree in this
    // pass; the second pass decodes directly into the strict typed catalog.
    // A complete v2 catalog scales with collections and layers, unlike v1's
    // fixed-size envelope. Do not impose v1's byte limit on that catalog or
    // keep both a raw file buffer and an untyped JSON tree in memory.
    #[derive(Deserialize)]
    struct VersionProbe {
        schema_version: Option<u32>,
    }
    let file = std::fs::File::open(&manifest_path)
        .with_context(|| format!("open generation manifest {}", manifest_path.display()))?;
    let mut reader = BufReader::new(file);
    let probe: VersionProbe = serde_json::from_reader(&mut reader)
        .with_context(|| format!("decode generation manifest {}", manifest_path.display()))?;
    reader.seek(SeekFrom::Start(0))?;
    match probe.schema_version {
        Some(1) => {
            if metadata.len() > 4096 {
                bail!(
                    "generation manifest is too large: {}",
                    manifest_path.display()
                );
            }
            let legacy: LegacyV1GenerationManifest =
                serde_json::from_reader(reader).with_context(|| {
                    format!("decode v1 generation manifest {}", manifest_path.display())
                })?;
            if legacy.schema_version != 1 {
                bail!("generation manifest version changed while reading");
            }
            Ok(SegmentGenerationManifest {
                schema_version: legacy.schema_version,
                checkpoint_sequence: legacy.sequence,
                revision: legacy.revision,
                previous: legacy.previous,
                next_collection_generation: 1,
                collections: Vec::new(),
            })
        }
        Some(2) => {
            let manifest: SegmentGenerationManifest = serde_json::from_reader(reader)
                .with_context(|| {
                    format!("decode v2 generation manifest {}", manifest_path.display())
                })?;
            if manifest.schema_version != 2 {
                bail!("generation manifest version changed while reading");
            }
            Ok(manifest)
        }
        Some(3) => {
            let manifest: SegmentGenerationManifest = serde_json::from_reader(reader)
                .with_context(|| {
                    format!("decode v3 generation manifest {}", manifest_path.display())
                })?;
            if manifest.schema_version != 3 {
                bail!("unknown generation layout for schema version 3");
            }
            Ok(manifest)
        }
        Some(version) => bail!("unknown manifest schema version {version}"),
        None => bail!("generation manifest has no schema_version"),
    }
}

pub(super) type PriorCatalog<'a> = Option<(&'a Path, &'a SegmentGenerationManifest)>;

fn exact_prior_segment_reference(
    collection: &CollectionCatalog,
    segment: &SegmentReference,
    prior: Option<&SegmentGenerationManifest>,
) -> bool {
    prior
        .and_then(|prior| {
            prior.collections.iter().find(|old| {
                old.collection_id == collection.collection_id
                    && old.collection_generation == collection.collection_generation
                    && old.schema_version == collection.schema_version
                    && old.schema == collection.schema
            })
        })
        .is_some_and(|old| {
            old.segments
                .iter()
                .any(|old_segment| old_segment == segment)
        })
}

pub(super) fn register_checkpoint_inherited_files(
    staged: &mut CurrentGenerationStaging,
    collections: &[CollectionCatalog],
    prior: Option<&SegmentGenerationManifest>,
    capture: &crate::storage::CheckpointCapture,
) -> Result<()> {
    for collection in collections {
        let same_collection = prior.is_some_and(|prior| {
            prior.collections.iter().any(|old| {
                old.collection_id == collection.collection_id
                    && old.collection_generation == collection.collection_generation
                    && old.schema_version == collection.schema_version
                    && old.schema == collection.schema
            })
        });
        if same_collection && capture.reused.contains(&collection.collection_id) {
            let relative = collection_output_relative(collection, CHECKPOINT_SCHEMA_FILE);
            staged
                .inherit_current_file(&relative)
                .with_context(|| format!("inherit CURRENT file {}", relative))?;
        }
        for segment in &collection.segments {
            if exact_prior_segment_reference(collection, segment, prior) {
                staged
                    .inherit_current_file(&segment.path)
                    .with_context(|| format!("inherit CURRENT file {}", segment.path))?;
                if let Some(rows) = &segment.local_rows {
                    staged
                        .inherit_current_file(&rows.path)
                        .with_context(|| format!("inherit CURRENT file {}", rows.path))?;
                }
            }
        }
    }
    Ok(())
}

pub(super) fn sync_directory(path: &Path) -> std::io::Result<()> {
    OpenOptions::new().read(true).open(path)?.sync_all()
}
