//! Building a merge's scratch generation by hard-linking the unchanged files of
//! the source generation, and the per-step cost a published job records.

use crate::persistence::domain::generation_manifest::SegmentGenerationManifest;
use crate::persistence::infrastructure::segment_rdb_store::flat_layout::{
    collection_checkpoint_dir_name, flat_payload_name,
};
use crate::persistence::infrastructure::segment_rdb_store::generation_validation::{
    map_generation_pass, GENERATION_LINK_WORKERS,
};
use crate::persistence::infrastructure::segment_rdb_store::{
    FLAT_PAYLOAD_DIR, GENERATION_MANIFEST_V2, GENERATION_MANIFEST_V3,
};
use anyhow::{anyhow, bail, Result};
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

/// One merge job's accumulated per-[`MergeStep`] cost.
///
/// Steps are buffered rather than published as they finish because a merge
/// attempt may abandon at any of its refusal points (a frozen checkpoint, a
/// moved capture epoch, a rebase whose inputs no longer occur). Publishing a
/// half-finished attempt's phases would leave the `phase` rows with
/// different observation counts, and the breakdown would no longer describe
/// any single job. [`MergeStepCosts::publish`] therefore runs only on a job
/// that actually published, so every `phase` row shares one denominator.
#[derive(Default)]
pub(super) struct MergeStepCosts {
    entries: [(Duration, u64); crate::metrics::MERGE_STEP_COUNT],
}

impl MergeStepCosts {
    pub(super) fn record(
        &mut self,
        step: crate::metrics::MergeStep,
        elapsed: Duration,
        files: u64,
    ) {
        let entry = &mut self.entries[step.index()];
        entry.0 += elapsed;
        entry.1 += files;
    }

    pub(super) fn publish(
        &self,
        metrics: &crate::metrics::Metrics,
        total: Duration,
        save_gate_held: Duration,
        fields: u64,
    ) {
        for step in crate::metrics::MergeStep::ALL {
            if step == crate::metrics::MergeStep::Total {
                continue;
            }
            let (elapsed, files) = self.entries[step.index()];
            metrics.observe_segment_merge_step(step, elapsed, files);
        }
        metrics.observe_segment_merge_step(crate::metrics::MergeStep::Total, total, 0);
        metrics.observe_segment_merge_save_gate(save_gate_held);
        metrics.incr_segment_merge_fields(fields);
    }
}

/// Hard-link one collection's directory from `source` into `destination`,
/// returning the number of files linked.
///
/// A merge job's scratch stage is job-private: it carries no generation
/// manifest, is never published, and is removed by
/// [`SegmentRdbStore::cleanup_owned_merge_staging`] when the job ends, so no
/// reader ever opens it as a generation. The compaction reads and writes only
/// the compacted collection's own directory, so that is all the stage needs —
/// and the count
/// `lumen_segment_merge_phase_files_total{phase="link_scratch"}` publishes is
/// proportional to the job's payload rather than to the whole root.
///
/// [`SegmentRdbStore::cleanup_owned_merge_staging`]: crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore::cleanup_owned_merge_staging
pub(super) fn link_collection(
    source: &Path,
    destination: &Path,
    collection_id: &str,
) -> Result<usize> {
    let name = collection_checkpoint_dir_name(collection_id);
    if source.join(FLAT_PAYLOAD_DIR).is_dir() {
        // v3 flat names keep the collection ID as their leading byte string,
        // then hex-encode only the former collection-relative path. The old
        // directory name is hexadecimal and cannot select those files.
        let prefix = flat_payload_name(collection_id, Path::new(""));
        return link_flat_collection(
            &source.join(FLAT_PAYLOAD_DIR),
            &destination.join(FLAT_PAYLOAD_DIR),
            &prefix,
        );
    }
    link_tree(&source.join(&name), &destination.join(&name))
}

fn link_flat_collection(source: &Path, destination: &Path, prefix: &str) -> Result<usize> {
    std::fs::create_dir_all(destination)?;
    let mut count = 0;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !format!("payload/{name}").starts_with(prefix) {
            continue;
        }
        let metadata = std::fs::symlink_metadata(entry.path())?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            bail!("flat merge source contains a nonregular file");
        }
        std::fs::hard_link(entry.path(), destination.join(entry.file_name()))?;
        count += 1;
    }
    Ok(count)
}

/// Hard-link every collection of `manifest` from `source` into `destination`,
/// returning every linked path relative to the generation root.
///
/// This is the publication-side pass: the generation being staged inherits
/// every file of the one it replaces, so its cost is proportional to the whole
/// root and it is paid while the job holds the save gate. Collections are
/// disjoint subtrees, so they are linked on a bounded worker set
/// ([`map_generation_pass`]) and folded back in manifest order, which keeps
/// both the reported path set and the first reported error identical to a
/// sequential pass.
pub(super) fn link_collections_with_paths(
    source: &Path,
    destination: &Path,
    manifest: &SegmentGenerationManifest,
) -> Result<BTreeSet<String>> {
    if uses_flat_generation_layout(manifest.schema_version) {
        let mut linked = BTreeSet::new();
        link_tree_with_paths(
            source.join(FLAT_PAYLOAD_DIR).as_path(),
            destination.join(FLAT_PAYLOAD_DIR).as_path(),
            Path::new(FLAT_PAYLOAD_DIR),
            &mut linked,
        )?;
        return Ok(linked);
    }
    let names: Vec<String> = manifest
        .collections
        .iter()
        .map(|collection| collection_checkpoint_dir_name(&collection.collection_id))
        .collect();
    let per_collection = map_generation_pass(&names, GENERATION_LINK_WORKERS, |name| {
        let mut linked = BTreeSet::new();
        link_tree_with_paths(
            &source.join(name),
            &destination.join(name),
            Path::new(name),
            &mut linked,
        )?;
        Ok(linked)
    });
    let mut linked = BTreeSet::new();
    for collection in per_collection {
        linked.extend(collection?);
    }
    Ok(linked)
}

#[inline]
pub(super) fn uses_flat_generation_layout(schema_version: u32) -> bool {
    schema_version == GENERATION_MANIFEST_V3
}

pub(super) fn background_merge_supports_manifest(schema_version: u32) -> bool {
    schema_version == GENERATION_MANIFEST_V2
}

fn link_tree_with_paths(
    source: &Path,
    destination: &Path,
    relative: &Path,
    linked: &mut BTreeSet<String>,
) -> Result<()> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let metadata = std::fs::symlink_metadata(entry.path())?;
        let target = destination.join(entry.file_name());
        let relative = relative.join(entry.file_name());
        if metadata.file_type().is_symlink() {
            bail!("merge source contains a symlink");
        }
        if metadata.is_dir() {
            link_tree_with_paths(&entry.path(), &target, &relative, linked)?;
        } else if metadata.is_file() {
            std::fs::hard_link(entry.path(), target)?;
            linked.insert(
                relative
                    .to_str()
                    .ok_or_else(|| anyhow!("merge source path is not UTF-8"))?
                    .to_owned(),
            );
        } else {
            bail!("merge source contains a nonregular file");
        }
    }
    Ok(())
}

/// Returns the number of files hard-linked, including nested directories.
fn link_tree(source: &Path, destination: &Path) -> Result<usize> {
    let mut linked = 0;
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let metadata = std::fs::symlink_metadata(entry.path())?;
        let target = destination.join(entry.file_name());
        if metadata.file_type().is_symlink() {
            bail!("merge source contains a symlink");
        }
        if metadata.is_dir() {
            linked += link_tree(&entry.path(), &target)?;
        } else if metadata.is_file() {
            std::fs::hard_link(entry.path(), target)?;
            linked += 1;
        } else {
            bail!("merge source contains a nonregular file");
        }
    }
    Ok(linked)
}
