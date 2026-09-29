//! Validating a staged or published generation: every entry and the layout of
//! its tree, in parallel passes over the root.

use crate::persistence::domain::generation_manifest::SegmentReference;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment_rdb_store::catalog::validate_catalog_references_with_prior;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::{
    read_generation_manifest, PriorCatalog,
};
use crate::persistence::infrastructure::segment_rdb_store::records::is_older_predecessor;
use crate::persistence::infrastructure::segment_rdb_store::{
    GenerationRecord, CHECKPOINT_SCHEMA_FILE, FLAT_PAYLOAD_DIR, GENERATION_MANIFEST_FILE,
    GENERATION_MANIFEST_V2, GENERATION_MANIFEST_V3,
};
use anyhow::{anyhow, bail, Context, Result};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;
use storage_durable::GenerationName;

/// Worker bound for the whole-generation filesystem passes.
///
/// The hard-link and stat passes over a generation are filesystem-metadata
/// bound, not CPU bound: measured on this project's storage, link throughput
/// over a 182-collection / 8k-file generation peaks near four concurrent
/// workers (3.3s serial, 1.7s at four) and degrades again past it (2.5s at
/// six), so an unbounded pool is slower than this bound, not faster.
pub(in crate::persistence) const GENERATION_LINK_WORKERS: usize = 4;

/// Worker bound for the validation pass over a staged generation. It is not
/// the link bound: validating a collection reads and decodes its checkpoint
/// schema and opens every base segment it declares, so it keeps more than four
/// workers busy where a pure link pass does not.
const GENERATION_VALIDATE_WORKERS: usize = 8;

/// Apply `task` to every item on a bounded worker set, returning the results in
/// input order.
///
/// Order is the contract: callers fold the results in input order, so the
/// first error reported is the same one a sequential pass would have reported,
/// whichever worker happened to observe it first.
pub(in crate::persistence) fn map_generation_pass<T: Sync, R: Send>(
    items: &[T],
    workers: usize,
    task: impl Fn(&T) -> Result<R> + Sync,
) -> Vec<Result<R>> {
    if items.len() < 2 {
        return items.iter().map(&task).collect();
    }
    let next = std::sync::atomic::AtomicUsize::new(0);
    let slots: Vec<Mutex<Option<Result<R>>>> = items.iter().map(|_| Mutex::new(None)).collect();
    let workers = workers.max(1).min(items.len());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(item) = items.get(index) else {
                    return;
                };
                let result = task(item);
                *slots[index].lock().unwrap_or_else(|p| p.into_inner()) = Some(result);
            });
        }
    });
    slots
        .into_iter()
        .map(|slot| {
            slot.into_inner()
                .unwrap_or_else(|p| p.into_inner())
                .expect("every item is assigned to exactly one worker")
        })
        .collect()
}

/// Validate one direct entry of a generation directory: either its manifest
/// file, or one collection subtree with its checkpoint schema and every base
/// segment the schema declares.
///
/// Returns whether the entry was a collection. Entries are disjoint subtrees,
/// which is what lets [`validate_generation_layout_with_prior`] run this pass
/// on a bounded worker set without weakening any check it makes.
fn validate_generation_entry(
    path: &Path,
    record: &GenerationRecord,
    references: &BTreeMap<&str, &SegmentReference>,
    v2: bool,
    flat: bool,
) -> Result<bool> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        bail!("generation contains a symlink: {}", path.display());
    }
    if path.file_name().and_then(|name| name.to_str()) == Some(GENERATION_MANIFEST_FILE) {
        if record.legacy || !metadata.is_file() {
            bail!("invalid generation manifest entry: {}", path.display());
        }
        return Ok(false);
    }
    if !metadata.is_dir() {
        bail!("unexpected generation entry: {}", path.display());
    }
    validate_real_tree(path)?;
    if flat && path.file_name().and_then(|name| name.to_str()) == Some(FLAT_PAYLOAD_DIR) {
        return Ok(false);
    }

    let schema_path = path.join(CHECKPOINT_SCHEMA_FILE);
    let schema_metadata = std::fs::symlink_metadata(&schema_path)
        .with_context(|| format!("inspect checkpoint schema {}", schema_path.display()))?;
    if schema_metadata.file_type().is_symlink() || !schema_metadata.is_file() {
        bail!(
            "checkpoint schema must be a regular file: {}",
            schema_path.display()
        );
    }
    let schema: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&schema_path)
            .with_context(|| format!("read checkpoint schema {}", schema_path.display()))?,
    )
    .with_context(|| format!("decode checkpoint schema {}", schema_path.display()))?;
    let applied_seq = schema
        .get("applied_seq")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            anyhow!(
                "checkpoint schema has no applied_seq: {}",
                schema_path.display()
            )
        })?;
    if (v2 && applied_seq > record.sequence) || (!v2 && applied_seq != record.sequence) {
        bail!(
            "checkpoint schema {} has sequence {applied_seq}, expected {}",
            schema_path.display(),
            record.sequence
        );
    }
    if v2 {
        let layout = crate::storage::CheckpointLayout::from_sidecar(&schema)?;
        let fields: BTreeMap<String, crate::shared_kernel::types::schema::FieldSpec> =
            serde_json::from_value(
                schema
                    .get("fields")
                    .cloned()
                    .ok_or_else(|| anyhow!("checkpoint schema has no fields"))?,
            )?;
        let mut bases = vec![(path.join("_collection.lmeta.lseg"), false)];
        for (name, spec) in fields {
            let stem = layout.field_stem(&name);
            let vector = spec.field_type == crate::shared_kernel::types::schema::FieldType::Vector;
            bases.push((path.join(format!("{stem}.lseg")), vector));
            if vector {
                bases.push((path.join(format!("{stem}.eids.lseg")), false));
            }
        }
        for (file, vector) in bases {
            let segment = SegmentReader::open(&file)
                .with_context(|| format!("catalogued segment is missing: {}", file.display()))?;
            // Only shipped raw-layout vectors used row count as watermark.
            let legacy_vector_header = layout == crate::storage::CheckpointLayout::Legacy
                && vector
                && segment.applied_seq() == segment.n_docs() as u64;
            let relative = file
                .strip_prefix(&record.path)?
                .to_str()
                .ok_or_else(|| anyhow!("non-UTF8 base path"))?;
            let reference = *references
                .get(relative)
                .ok_or_else(|| anyhow!("base segment has no catalog reference"))?;
            // Original v2 bases inherit their collection sidecar cut.
            // Compacted bases record their own, possibly newer, cut.
            let catalog_sequence = reference.applied_seq.unwrap_or(applied_seq);
            if !legacy_vector_header
                && (segment.applied_seq() != catalog_sequence || catalog_sequence > record.sequence)
            {
                bail!("segment sequence does not match checkpoint catalog");
            }
        }
    }
    Ok(true)
}

pub(in crate::persistence) fn validate_generation_layout(
    record: &GenerationRecord,
) -> Result<usize> {
    validate_generation_layout_with_prior(record, None)
}

pub(in crate::persistence) fn validate_generation_layout_with_prior(
    record: &GenerationRecord,
    prior: PriorCatalog<'_>,
) -> Result<usize> {
    let manifest = if record.legacy {
        None
    } else {
        Some(read_generation_manifest(&record.path)?)
    };
    let v2 = manifest.as_ref().is_some_and(|manifest| {
        matches!(
            manifest.schema_version,
            GENERATION_MANIFEST_V2 | GENERATION_MANIFEST_V3
        )
    });
    let flat = manifest
        .as_ref()
        .is_some_and(|manifest| manifest.schema_version == GENERATION_MANIFEST_V3);
    // Validate names before using the catalog to resolve a physical base.
    // A malformed reference must not be mistaken for an omitted base.
    if v2 {
        for reference in manifest
            .as_ref()
            .into_iter()
            .flat_map(|manifest| &manifest.collections)
            .flat_map(|collection| &collection.segments)
        {
            if Path::new(&reference.path).is_absolute()
                || reference
                    .path
                    .split('/')
                    .any(|part| matches!(part, "" | "." | ".."))
            {
                bail!(
                    "segment reference path escapes generation: {}",
                    reference.path
                );
            }
        }
    }
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(&record.path)
        .with_context(|| format!("read generation {}", record.path.display()))?
    {
        entries.push(entry?.path());
    }
    entries.sort();

    // One index over the whole catalog: resolving each declared base by a
    // linear scan of every collection's segments made this pass quadratic in
    // the number of collections.
    let mut references: BTreeMap<&str, &SegmentReference> = BTreeMap::new();
    for reference in manifest
        .as_ref()
        .into_iter()
        .flat_map(|manifest| &manifest.collections)
        .flat_map(|collection| &collection.segments)
    {
        references
            .entry(reference.path.as_str())
            .or_insert(reference);
    }
    let mut collections = 0usize;
    for outcome in map_generation_pass(&entries, GENERATION_VALIDATE_WORKERS, |path| {
        validate_generation_entry(path, record, &references, v2, flat)
    }) {
        if outcome? {
            collections += 1;
        }
    }

    if !record.legacy {
        if let Some(previous) = &record.previous {
            if !is_older_predecessor(previous.as_str(), record.sequence, record.revision) {
                bail!(
                    "generation {} has non-predecessor link {}",
                    record.name,
                    previous
                );
            }
        }
        let manifest = read_generation_manifest(&record.path)?;
        let expected_previous = record.previous.as_ref().map(GenerationName::as_str);
        if (manifest.schema_version != 1
            && !matches!(
                manifest.schema_version,
                GENERATION_MANIFEST_V2 | GENERATION_MANIFEST_V3
            ))
            || manifest.checkpoint_sequence != record.sequence
            || manifest.revision != record.revision
            || manifest.previous.as_deref() != expected_previous
        {
            bail!(
                "generation {} manifest does not match its validated record",
                record.name
            );
        }
        if matches!(
            manifest.schema_version,
            GENERATION_MANIFEST_V2 | GENERATION_MANIFEST_V3
        ) {
            validate_catalog_references_with_prior(&record.path, &manifest, prior)?;
        }
    }
    Ok(collections)
}

fn validate_real_tree(root: &Path) -> Result<()> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let metadata = std::fs::symlink_metadata(&directory)
            .with_context(|| format!("inspect checkpoint path {}", directory.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!(
                "checkpoint directory must be a real directory: {}",
                directory.display()
            );
        }

        let mut entries = std::fs::read_dir(&directory)
            .with_context(|| format!("read checkpoint directory {}", directory.display()))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        entries.sort();
        for path in entries {
            let metadata = std::fs::symlink_metadata(&path)
                .with_context(|| format!("inspect checkpoint path {}", path.display()))?;
            if metadata.file_type().is_symlink() {
                bail!("checkpoint contains a symlink: {}", path.display());
            }
            if metadata.is_dir() {
                pending.push(path);
            } else if !metadata.is_file() {
                bail!(
                    "checkpoint contains a special filesystem entry: {}",
                    path.display()
                );
            }
        }
    }
    Ok(())
}
