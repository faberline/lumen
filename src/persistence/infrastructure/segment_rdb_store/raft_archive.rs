//! Immutable `LSEGRAFT` v1 generation archive framing.
//!
//! This module only reads a caller-pinned generation directory.  It never
//! resolves `CURRENT` and never builds an Engine while writing.

pub(crate) mod store;

use crate::persistence::infrastructure::segment_rdb_store::flat_layout::collection_checkpoint_dir_name;
use crate::persistence::infrastructure::segment_rdb_store::generation_validation::validate_generation_layout;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::read_generation_manifest;
use crate::persistence::infrastructure::segment_rdb_store::{
    GenerationRecord, CHECKPOINT_SCHEMA_FILE, GENERATION_MANIFEST_FILE,
};
use anyhow::{anyhow, ensure, Context, Result};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use storage_durable::GenerationName;

pub(crate) const MAGIC: &[u8; 8] = b"LSEGRAFT";
const VERSION: u8 = 1;
const COPY_BUF: usize = 64 * 1024;

struct ArchiveCandidate {
    record: GenerationRecord,
    collections: usize,
}

pub(crate) fn write_archive<W: Write + ?Sized>(
    root: &Path,
    sequence: u64,
    out: &mut W,
) -> Result<()> {
    let files = inventory(root)?;
    out.write_all(MAGIC)?;
    out.write_all(&[VERSION])?;
    out.write_all(&sequence.to_le_bytes())?;
    out.write_all(&(files.len() as u64).to_le_bytes())?;
    for (relative, path) in files {
        let encoded = relative.as_bytes();
        out.write_all(&(encoded.len() as u64).to_le_bytes())?;
        out.write_all(encoded)?;
        let len = std::fs::metadata(&path)?.len();
        out.write_all(&len.to_le_bytes())?;
        let digest = digest_file(&path)?;
        out.write_all(&digest)?;
        let mut file = File::open(&path)?;
        copy_exact(&mut file, out, len, None)?;
    }
    Ok(())
}

/// Decode into a caller-created staging directory. The directory must be empty
/// and below the destination root; callers validate/cold-open then publish it.
fn read_header<R: Read + ?Sized>(input: &mut R) -> Result<(u64, u64)> {
    let mut magic = [0; 8];
    input.read_exact(&mut magic)?;
    ensure!(magic == *MAGIC, "unsupported segment Raft archive magic");
    let mut version = [0; 1];
    input.read_exact(&mut version)?;
    ensure!(
        version[0] == VERSION,
        "unsupported segment Raft archive version {}",
        version[0]
    );
    let sequence = read_u64(input)?;
    let count = read_u64(input)?;
    Ok((sequence, count))
}

fn stage_archive<R: Read + ?Sized>(
    input: &mut R,
    staging: &Path,
    sequence: u64,
    count: u64,
) -> Result<ArchiveCandidate> {
    ensure_empty_directory(staging)?;
    let path_limit = filesystem_path_limit(staging)?;
    let mut seen = BTreeSet::new();
    for _ in 0..count {
        let path_len = read_u64(input)?;
        let path_len =
            usize::try_from(path_len).map_err(|_| anyhow!("archive path length overflow"))?;
        ensure!(
            path_len > 0 && path_len < path_limit,
            "archive path cannot fit the destination filesystem"
        );
        let mut raw = vec![0; path_len];
        input.read_exact(&mut raw)?;
        let relative = std::str::from_utf8(&raw).context("archive path is not UTF-8")?;
        validate_relative(relative)?;
        ensure!(
            seen.insert(relative.to_owned()),
            "duplicate archive path {relative}"
        );
        let length = read_u64(input)?;
        let mut expected = [0; 32];
        input.read_exact(&mut expected)?;
        let target = staging.join(relative);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)?;
        let actual = copy_exact(input, &mut output, length, Some(&expected))?;
        output.flush()?;
        ensure!(actual == expected, "archive digest mismatch for {relative}");
    }
    let mut trailing = [0; 1];
    ensure!(
        input.read(&mut trailing)? == 0,
        "trailing bytes after segment Raft archive"
    );
    let manifest = read_generation_manifest(staging)?;
    ensure!(
        manifest.schema_version == 2,
        "archive requires a v2 generation manifest"
    );
    ensure!(
        manifest.checkpoint_sequence == sequence,
        "archive sequence does not match manifest"
    );
    let mut expected = BTreeSet::from([GENERATION_MANIFEST_FILE.to_owned()]);
    for collection in &manifest.collections {
        expected.insert(format!(
            "{}/{}",
            collection_checkpoint_dir_name(&collection.collection_id),
            CHECKPOINT_SCHEMA_FILE
        ));
        for segment in &collection.segments {
            expected.insert(segment.path.clone());
            if let Some(rows) = &segment.local_rows {
                expected.insert(rows.path.clone());
            }
        }
    }
    ensure!(
        seen == expected,
        "archive file inventory differs from the complete generation manifest"
    );
    let record = GenerationRecord {
        name: GenerationName::parse(format!("gen-{sequence}-rev-{}", manifest.revision))?,
        path: staging.to_path_buf(),
        sequence,
        revision: manifest.revision,
        legacy: false,
        previous: manifest.previous.map(GenerationName::parse).transpose()?,
    };
    let collections =
        validate_generation_layout(&record).context("validate staged archive generation")?;
    Ok(ArchiveCandidate {
        record,
        collections,
    })
}

/// A path is metadata, never an allocation sized by an unchecked peer field.
/// The limit comes from the receiving filesystem, not the document API.
#[cfg(unix)]
fn filesystem_path_limit(staging: &Path) -> Result<usize> {
    use std::os::fd::AsRawFd;
    let directory = File::open(staging)?;
    // SAFETY: directory owns an open descriptor for the duration of fpathconf.
    let limit = unsafe { libc::fpathconf(directory.as_raw_fd(), libc::_PC_PATH_MAX) };
    ensure!(limit > 0, "destination filesystem has no finite path limit");
    Ok(usize::try_from(limit)?)
}

#[cfg(not(unix))]
fn filesystem_path_limit(_staging: &Path) -> Result<usize> {
    bail!("segment Raft archive requires the supported Unix filesystem backend")
}

fn inventory(root: &Path) -> Result<Vec<(String, PathBuf)>> {
    fn visit(root: &Path, here: &Path, out: &mut Vec<(String, PathBuf)>) -> Result<()> {
        for entry in std::fs::read_dir(here)? {
            let entry = entry?;
            let ty = entry.file_type()?;
            let path = entry.path();
            ensure!(
                !ty.is_symlink(),
                "generation archive refuses symlink {}",
                path.display()
            );
            if ty.is_dir() {
                visit(root, &path, out)?;
            } else {
                ensure!(
                    ty.is_file(),
                    "generation archive refuses non-regular file {}",
                    path.display()
                );
                let rel = path
                    .strip_prefix(root)?
                    .to_str()
                    .ok_or_else(|| anyhow!("non-UTF8 generation path"))?
                    .to_owned();
                validate_relative(&rel)?;
                out.push((rel, path));
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    visit(root, root, &mut out)?;
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}
fn validate_relative(path: &str) -> Result<()> {
    ensure!(
        !path.is_empty() && !path.starts_with('/') && !path.contains(['\\', '\0']),
        "invalid archive path"
    );
    ensure!(
        path.split('/')
            .all(|p| !p.is_empty() && p != "." && p != ".."),
        "invalid archive path {path}"
    );
    Ok(())
}
fn digest_file(path: &Path) -> Result<[u8; 32]> {
    let mut f = File::open(path)?;
    let mut h = Sha256::new();
    let mut b = [0; COPY_BUF];
    loop {
        let n = f.read(&mut b)?;
        if n == 0 {
            break;
        }
        h.update(&b[..n]);
    }
    Ok(h.finalize().into())
}
fn copy_exact<R: Read + ?Sized, W: Write + ?Sized>(
    input: &mut R,
    out: &mut W,
    len: u64,
    expected: Option<&[u8; 32]>,
) -> Result<[u8; 32]> {
    let mut left = len;
    let mut h = Sha256::new();
    let mut b = [0; COPY_BUF];
    while left > 0 {
        let want = usize::try_from(left.min(COPY_BUF as u64)).unwrap();
        input.read_exact(&mut b[..want])?;
        out.write_all(&b[..want])?;
        h.update(&b[..want]);
        left -= want as u64;
    }
    let got: [u8; 32] = h.finalize().into();
    if let Some(expected) = expected {
        ensure!(&got == expected, "archive payload digest mismatch");
    }
    Ok(got)
}
fn read_u64<R: Read + ?Sized>(r: &mut R) -> Result<u64> {
    let mut b = [0; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}
fn ensure_empty_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    ensure!(
        std::fs::read_dir(path)?.next().is_none(),
        "archive staging directory is not empty"
    );
    Ok(())
}
/// Cleanup only the private path this invocation created. A committed directory
/// has moved away from this name and is never removed by this guard.
struct StageCleanup(PathBuf);
impl Drop for StageCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests;
