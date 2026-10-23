//! Checkpoint measurements use real file identities and completed lock intervals.
//! Byte counts are logical file bytes, not filesystem block/device traffic.

use crate::persistence::domain::generation_manifest::{CollectionCatalog, SegmentKind};
use crate::shared_kernel::capture_barrier::CaptureLease;
use anyhow::{bail, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static INSPECTED_FILE_ENTRIES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, feature = "test-support"))]
pub fn reset_inspected_file_entries() {
    INSPECTED_FILE_ENTRIES.with(|c| c.set(0));
}

#[cfg(any(test, feature = "test-support"))]
pub fn inspected_file_entries() -> u64 {
    INSPECTED_FILE_ENTRIES.with(std::cell::Cell::get)
}

#[inline]
pub(super) fn note_inspected_file_entry() {
    #[cfg(any(test, feature = "test-support"))]
    INSPECTED_FILE_ENTRIES.with(|c| c.set(c.get().saturating_add(1)));
}

pub(super) struct MeasuredCapture<'a> {
    lease: CaptureLease<'a>,
    total: &'a AtomicU64,
    started: Instant,
}
impl<'a> MeasuredCapture<'a> {
    pub(super) fn new(lease: CaptureLease<'a>, total: &'a AtomicU64) -> Self {
        Self {
            lease,
            total,
            started: Instant::now(),
        }
    }
}
impl<'a> std::ops::Deref for MeasuredCapture<'a> {
    type Target = CaptureLease<'a>;
    fn deref(&self) -> &Self::Target {
        &self.lease
    }
}
impl Drop for MeasuredCapture<'_> {
    fn drop(&mut self) {
        let nanos = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let _ = self
            .total
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
                Some(total.saturating_add(nanos))
            });
    }
}

#[derive(Debug, Default, Clone)]
pub(in crate::persistence) struct RetainedInventory {
    files: BTreeMap<(u64, u64), IdentityEntry>,
    generations: BTreeMap<String, GenerationFiles>,
    total_bytes: u64,
}

#[derive(Debug, Clone)]
struct IdentityEntry {
    bytes: u64,
    generations: BTreeSet<String>,
}

#[derive(Debug, Default, Clone)]
struct GenerationFiles {
    all: BTreeSet<(u64, u64)>,
    collections: BTreeMap<String, BTreeSet<(u64, u64)>>,
}

impl RetainedInventory {
    pub(super) fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub(super) fn contains_identity(&self, id: &(u64, u64)) -> bool {
        self.files.contains_key(id)
    }

    pub(super) fn remove_generation(&mut self, generation: &str) {
        if let Some(gen_files) = self.generations.remove(generation) {
            for id in gen_files.all {
                if let Some(entry) = self.files.get_mut(&id) {
                    entry.generations.remove(generation);
                    if entry.generations.is_empty() {
                        self.total_bytes = self.total_bytes.saturating_sub(entry.bytes);
                        self.files.remove(&id);
                    }
                }
            }
        }
    }

    #[cfg(unix)]
    pub(super) fn full_walk(root: &Path) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;

        let mut files = BTreeMap::new();
        let mut generations = BTreeMap::new();

        if !root.exists() {
            return Ok(Self::default());
        }

        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = match name.to_str() {
                Some(s) if s.starts_with("gen-") => s.to_string(),
                _ => continue,
            };
            let gen_path = entry.path();
            let mut gen_files = GenerationFiles::default();

            for child in std::fs::read_dir(&gen_path)? {
                let child = child?;
                let child_path = child.path();
                let file_name = child.file_name();
                let file_name_str = file_name.to_string_lossy();

                if file_name_str == super::GENERATION_MANIFEST_FILE {
                    note_inspected_file_entry();
                    let meta = std::fs::symlink_metadata(&child_path)?;
                    if meta.file_type().is_symlink() || !meta.is_file() {
                        bail!("manifest must be a regular file");
                    }
                    let id = (meta.dev(), meta.ino());
                    gen_files.all.insert(id);
                    let entry = files.entry(id).or_insert_with(|| IdentityEntry {
                        bytes: meta.len(),
                        generations: BTreeSet::new(),
                    });
                    entry.generations.insert(name_str.clone());
                } else if child_path.is_dir() {
                    let collection_id =
                        crate::index::infrastructure::checkpoint_fs::collection_name_from_dir(
                            &child_path,
                        )
                        .unwrap_or_else(|| {
                            file_name_str
                                .strip_prefix("c-")
                                .unwrap_or(&file_name_str)
                                .to_string()
                        });
                    let mut coll_files = BTreeSet::new();
                    scan_directory_identities(&child_path, &mut coll_files, &mut files, &name_str)?;
                    gen_files.all.extend(coll_files.iter().copied());
                    gen_files.collections.insert(collection_id, coll_files);
                }
            }
            generations.insert(name_str, gen_files);
        }

        let total_bytes = files.values().try_fold(0u64, |sum, e| {
            sum.checked_add(e.bytes)
                .ok_or_else(|| anyhow::anyhow!("segment byte count overflow"))
        })?;

        Ok(Self {
            files,
            generations,
            total_bytes,
        })
    }

    #[cfg(not(unix))]
    pub(super) fn full_walk(_root: &Path) -> Result<Self> {
        bail!("segment file identity measurements require Unix")
    }

    pub(super) fn record_published_generation(
        &mut self,
        generation_name: &str,
        manifest_id: ((u64, u64), u64),
        changed_files: BTreeMap<String, Vec<((u64, u64), u64)>>,
        reused_collections: &BTreeSet<String>,
        previous_generation: Option<&str>,
    ) -> bool {
        let mut gen_files = GenerationFiles::default();

        // 1. Manifest
        let (m_id, m_bytes) = manifest_id;
        gen_files.all.insert(m_id);
        let m_entry = self.files.entry(m_id).or_insert_with(|| {
            self.total_bytes = self.total_bytes.saturating_add(m_bytes);
            IdentityEntry {
                bytes: m_bytes,
                generations: BTreeSet::new(),
            }
        });
        m_entry.generations.insert(generation_name.to_string());

        // 2. Reused collections (inherit file identities from previous generation)
        if let Some(prev) = previous_generation {
            if let Some(prev_gen) = self.generations.get(prev) {
                for reused_id in reused_collections {
                    if let Some(prev_coll_files) = prev_gen.collections.get(reused_id) {
                        gen_files
                            .collections
                            .entry(reused_id.clone())
                            .or_default()
                            .extend(prev_coll_files.iter().copied());
                        for &id in prev_coll_files {
                            gen_files.all.insert(id);
                            if let Some(entry) = self.files.get_mut(&id) {
                                entry.generations.insert(generation_name.to_string());
                            }
                        }
                    }
                }
            } else if !reused_collections.is_empty() {
                return false;
            }
        } else if !reused_collections.is_empty() {
            return false;
        }

        // 3. Changed files
        for (coll_id, files) in changed_files {
            let coll_set = gen_files.collections.entry(coll_id).or_default();
            for (id, bytes) in files {
                coll_set.insert(id);
                gen_files.all.insert(id);
                let entry = self.files.entry(id).or_insert_with(|| {
                    self.total_bytes = self.total_bytes.saturating_add(bytes);
                    IdentityEntry {
                        bytes,
                        generations: BTreeSet::new(),
                    }
                });
                entry.generations.insert(generation_name.to_string());
            }
        }

        self.generations
            .insert(generation_name.to_string(), gen_files);
        true
    }
}

#[cfg(unix)]
fn scan_directory_identities(
    dir: &Path,
    coll_files: &mut BTreeSet<(u64, u64)>,
    files: &mut BTreeMap<(u64, u64), IdentityEntry>,
    gen_name: &str,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        note_inspected_file_entry();
        let meta = std::fs::symlink_metadata(&path)?;
        if meta.file_type().is_symlink() {
            bail!("segment metric refuses symlink");
        }
        if meta.is_dir() {
            scan_directory_identities(&path, coll_files, files, gen_name)?;
        } else if meta.is_file() {
            let id = (meta.dev(), meta.ino());
            coll_files.insert(id);
            let entry = files.entry(id).or_insert_with(|| IdentityEntry {
                bytes: meta.len(),
                generations: BTreeSet::new(),
            });
            entry.generations.insert(gen_name.to_string());
        }
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn collect_staged_changed_files(
    staging_path: &Path,
    collections: &[CollectionCatalog],
    idle: &BTreeSet<String>,
) -> Result<BTreeMap<String, Vec<((u64, u64), u64)>>> {
    use crate::persistence::infrastructure::segment_rdb_store::flat_layout::collection_checkpoint_dir_name;

    let mut changed = BTreeMap::new();
    for coll in collections {
        if idle.contains(&coll.collection_id) {
            continue;
        }
        let dir = staging_path.join(collection_checkpoint_dir_name(&coll.collection_id));
        if !dir.exists() {
            continue;
        }
        let mut coll_files = Vec::new();
        scan_staged_files(&dir, &mut coll_files)?;
        changed.insert(coll.collection_id.clone(), coll_files);
    }
    Ok(changed)
}

#[cfg(not(unix))]
pub(super) fn collect_staged_changed_files(
    _staging_path: &Path,
    _collections: &[CollectionCatalog],
    _idle: &BTreeSet<String>,
) -> Result<BTreeMap<String, Vec<((u64, u64), u64)>>> {
    bail!("segment file identity measurements require Unix")
}

#[cfg(unix)]
fn scan_staged_files(dir: &Path, out: &mut Vec<((u64, u64), u64)>) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        note_inspected_file_entry();
        let meta = std::fs::symlink_metadata(&path)?;
        if meta.file_type().is_symlink() {
            bail!("segment metric refuses symlink");
        }
        if meta.is_dir() {
            scan_staged_files(&path, out)?;
        } else if meta.is_file() {
            out.push(((meta.dev(), meta.ino()), meta.len()));
        }
    }
    Ok(())
}

#[cfg(unix)]
#[allow(dead_code)]
fn inventory(path: &Path, files: &mut BTreeMap<(u64, u64), u64>) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    note_inspected_file_entry();
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        bail!("segment metric refuses symlink");
    }
    if metadata.is_dir() {
        for entry in std::fs::read_dir(path)? {
            inventory(&entry?.path(), files)?;
        }
    } else if metadata.is_file() {
        files.insert((metadata.dev(), metadata.ino()), metadata.len());
    } else {
        bail!("segment metric requires a regular file or directory");
    }
    Ok(())
}
#[cfg(not(unix))]
#[allow(dead_code)]
fn inventory(_path: &Path, _files: &mut BTreeMap<(u64, u64), u64>) -> Result<()> {
    bail!("segment file identity measurements require Unix")
}
#[allow(dead_code)]
fn sum(mut files: impl Iterator<Item = u64>) -> Result<u64> {
    files.try_fold(0u64, |total, bytes| {
        total
            .checked_add(bytes)
            .ok_or_else(|| anyhow::anyhow!("segment byte count overflow"))
    })
}
#[allow(dead_code)]
pub(super) fn new_file_bytes(staging: &Path, previous: Option<&Path>) -> Result<u64> {
    let mut old = BTreeMap::new();
    if let Some(previous) = previous {
        inventory(previous, &mut old)?;
    }
    let mut new = BTreeMap::new();
    inventory(staging, &mut new)?;
    sum(new
        .into_iter()
        .filter(|(id, _)| !old.contains_key(id))
        .map(|(_, bytes)| bytes))
}

/// `_generation.json` is written after the checkpoint payload snapshot.
#[allow(dead_code)]
pub(super) fn manifest_bytes(staging: &Path) -> Result<u64> {
    note_inspected_file_entry();
    let metadata = std::fs::symlink_metadata(staging.join(super::GENERATION_MANIFEST_FILE))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("segment generation manifest must be a regular file");
    }
    Ok(metadata.len())
}

#[cfg(unix)]
pub(super) fn manifest_identity(staging: &Path) -> Result<((u64, u64), u64)> {
    use std::os::unix::fs::MetadataExt;
    note_inspected_file_entry();
    let metadata = std::fs::symlink_metadata(staging.join(super::GENERATION_MANIFEST_FILE))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("segment generation manifest must be a regular file");
    }
    Ok(((metadata.dev(), metadata.ino()), metadata.len()))
}

#[cfg(not(unix))]
pub(super) fn manifest_identity(_staging: &Path) -> Result<((u64, u64), u64)> {
    bail!("segment file identity measurements require Unix")
}

#[allow(dead_code)]
pub(in crate::persistence) fn generation_disk_bytes(root: &Path) -> Result<u64> {
    let mut files = BTreeMap::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_str().is_some_and(|name| name.starts_with("gen-")) {
            inventory(&entry.path(), &mut files)?;
        }
    }
    sum(files.into_values())
}
pub(in crate::persistence) fn pending_deltas(
    root: &Path,
    collections: &[CollectionCatalog],
) -> Result<(u64, u64)> {
    let mut bytes = 0u64;
    let mut layers = 0u64;
    for segment in collections
        .iter()
        .flat_map(|collection| &collection.segments)
    {
        if matches!(segment.kind, SegmentKind::Delta) {
            layers += 1;
            for path in std::iter::once(&segment.path)
                .chain(segment.local_rows.iter().map(|rows| &rows.path))
            {
                bytes = bytes
                    .checked_add(std::fs::metadata(root.join(path))?.len())
                    .ok_or_else(|| anyhow::anyhow!("pending delta bytes overflow"))?;
            }
        }
    }
    Ok((bytes, layers))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn linked_generations_count_payload_only_once() {
        let dir = tempfile::tempdir().unwrap();
        let one = dir.path().join("gen-1");
        let two = dir.path().join("gen-2");
        std::fs::create_dir(&one).unwrap();
        std::fs::create_dir(&two).unwrap();
        std::fs::write(one.join("data"), b"12345").unwrap();
        std::fs::hard_link(one.join("data"), two.join("data")).unwrap();
        std::fs::write(two.join("new"), b"67").unwrap();
        assert_eq!(new_file_bytes(&two, Some(&one)).unwrap(), 2);
        assert_eq!(generation_disk_bytes(dir.path()).unwrap(), 7);
    }

    #[test]
    fn premerge_snapshot_keeps_fresh_delta_out_of_merge_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let prior = dir.path().join("gen-1");
        let staging = dir.path().join("staging");
        std::fs::create_dir(&prior).unwrap();
        std::fs::create_dir(&staging).unwrap();
        std::fs::write(prior.join("base"), b"base").unwrap();
        std::fs::hard_link(prior.join("base"), staging.join("base")).unwrap();
        std::fs::write(staging.join("delta"), b"delta!!").unwrap();
        let checkpoint_payload = new_file_bytes(&staging, Some(&prior)).unwrap();
        assert_eq!(checkpoint_payload, 7);
        std::fs::write(staging.join("delta"), b"merged-output").unwrap();
        std::fs::write(staging.join("_generation.json"), b"meta!").unwrap();
        assert_eq!(manifest_bytes(&staging).unwrap(), 5);
        assert_eq!(checkpoint_payload + manifest_bytes(&staging).unwrap(), 12);
        assert_eq!(std::fs::metadata(staging.join("delta")).unwrap().len(), 13);
    }
}
