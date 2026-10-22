//! Byte accounting and crash-safe writes for a compaction's output files: each
//! local-row or EID sidecar is written under a unique temporary name, fsynced,
//! and renamed into place.

use crate::persistence::infrastructure::segment::eid_writer::write_eid_segment;
use crate::persistence::infrastructure::segment::sparse_rows::encode_sparse_local_rows;
use anyhow::{anyhow, bail, Context, Result};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static COMPACTION_TEMP_NONCE: AtomicU64 = AtomicU64::new(0);

pub(super) fn checked_bytes(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| anyhow!("compaction byte counter overflow"))
}

pub(super) fn file_len(path: &Path) -> Result<u64> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(
            "compaction input must be a regular file: {}",
            path.display()
        );
    }
    Ok(metadata.len())
}

fn unique_aux_temp(path: &Path) -> Result<PathBuf> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("output has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("output has no UTF-8 name"))?;
    for _ in 0..32 {
        let nonce = COMPACTION_TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            ".{name}.compact-{}-{nonce}.tmp",
            std::process::id()
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => {
                drop(file);
                return Ok(candidate);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("create {}", candidate.display()))
            }
        }
    }
    bail!("could not allocate compaction temporary output")
}

pub(super) fn write_sparse_rows_atomic(path: &Path, ids: &[String]) -> Result<()> {
    let temp = unique_aux_temp(path)?;
    let result = encode_sparse_local_rows(&temp, ids).and_then(|_| sync_and_rename(&temp, path));
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

pub(super) fn write_eids_atomic(path: &Path, sequence: u64, ids: &[String]) -> Result<()> {
    let temp = unique_aux_temp(path)?;
    let refs: Vec<_> = ids.iter().map(String::as_str).collect();
    let result =
        write_eid_segment(&temp, sequence, &refs).and_then(|_| sync_and_rename(&temp, path));
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn sync_and_rename(temp: &Path, path: &Path) -> Result<()> {
    File::open(temp)?.sync_all()?;
    std::fs::rename(temp, path)?;
    File::open(
        path.parent()
            .ok_or_else(|| anyhow!("output has no parent"))?,
    )?
    .sync_all()?;
    Ok(())
}
