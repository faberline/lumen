//! Bounded staging for long `WhitespaceLower` tokens.
//!
//! A caller supplies the final row path inside its private stage directory.
//! This helper owns a child directory and removes all of its temporary files
//! before returning. The caller owns the final row file.

mod one_row;

use crate::index::infrastructure::staging::large_text_row::one_row::{Group, OneRow};

use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(test)]
use std::cell::Cell;

use anyhow::{anyhow, bail, Context, Result};
use memmap2::{Mmap, MmapOptions};

use crate::index::infrastructure::analysis::unicode_lower_stream::{
    lowercase_stream_workspace_bytes, write_streaming_lowercase,
};
use crate::persistence::infrastructure::segment::stream::text_projection::write_text_projection;
use crate::persistence::infrastructure::segment::text_row_stage::{
    stage_text_row, TextRowStageOptions,
};
use crate::persistence::infrastructure::segment::SegmentReader;

pub(crate) const LARGE_TOKEN_SOURCE_BYTES: usize = 64 * 1024;
const MAPPED_TOKEN_BYTES: usize = 256;
const IO_BUFFER_BYTES: usize = 8 * 1024;
const RAW_OFFSET_COPY_BYTES: usize = 64 * 1024;
const VAR_BLOCK_BYTES: usize = 64 * 1024;
static NONCE: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum TokenFailure {
    Normalize,
    Map,
}
#[cfg(test)]
thread_local! { static TOKEN_FAILURE: Cell<Option<TokenFailure>> = const { Cell::new(None) }; }
#[cfg(test)]
pub(super) fn set_token_failure_for_test(failure: Option<TokenFailure>) {
    TOKEN_FAILURE.with(|current| current.set(failure));
}
fn token_failure(_stage: &'static str) -> Result<()> {
    #[cfg(test)]
    if TOKEN_FAILURE
        .with(|current| current.get())
        .is_some_and(|failure| {
            matches!(
                (_stage, failure),
                ("normalize", TokenFailure::Normalize) | ("map", TokenFailure::Map)
            )
        })
    {
        bail!("injected large Text token {_stage} failure");
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct LargeTextRowReceipt {
    pub(super) doc_len: u32,
    pub(crate) final_reader_metadata_bytes: usize,
}

/// Stage one exact `for_whitespace_lower_cow` row.  `reserve` is called before
/// every helper-owned allocation. It is additive: `options.scratch_bytes` and
/// the caller's `3 * threshold + 64` short-lower workspace were already
/// reserved by the enclosing staged-row owner and are not charged again.
pub(crate) fn stage_large_whitespace_row(
    input: &str,
    final_path: &Path,
    scratch_dir: &Path,
    options: TextRowStageOptions,
    threshold: usize,
    mut reserve: impl FnMut(usize) -> Result<()>,
) -> Result<LargeTextRowReceipt> {
    if threshold == 0 {
        bail!("large Text token threshold must be nonzero")
    }
    reserve(4096)?; // Private directory, transient paths, and row-view metadata.
    let cleanup = PrivateDirectory::create(scratch_dir)?;
    let mut long = Vec::new();
    let mut doc_len = 0u32;
    let mut small_len = 0u32;
    visit_tokens(input, |token| {
        doc_len = doc_len
            .checked_add(1)
            .ok_or_else(|| anyhow!("Text document length exceeds u32"))?;
        if token.len() > threshold {
            if long.is_empty() {
                reserve(lowercase_stream_workspace_bytes())?;
            }
            reserve_vector_growth(&mut long, &mut reserve)?;
            reserve(MAPPED_TOKEN_BYTES)?;
            long.push(MappedToken::write(token, cleanup.path())?);
        } else {
            small_len = small_len
                .checked_add(1)
                .ok_or_else(|| anyhow!("Text document length exceeds u32"))?;
        }
        Ok(())
    })?;

    let small_path = cleanup.path().join("small-row.lseg");
    stage_text_row(
        &small_path,
        0,
        small_len,
        cleanup.path(),
        options,
        Box::new(|emit| {
            visit_tokens(input, |token| {
                if token.len() <= threshold {
                    emit_small_lower(token, emit)?;
                }
                Ok(())
            })
        }),
    )?;
    reserve(SegmentReader::staged_metadata_bound(&small_path)?)?;
    let small = Arc::new(SegmentReader::open(&small_path)?);

    long.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
    let mut groups = Vec::new();
    let mut first = 0usize;
    while first < long.len() {
        reserve_vector_growth(&mut groups, &mut reserve)?;
        let term = long[first].as_str();
        let mut end = first + 1;
        let mut tf = 1u32;
        while end < long.len() && long[end].as_str() == term {
            tf = tf
                .checked_add(1)
                .ok_or_else(|| anyhow!("Text term frequency exceeds u32"))?;
            end += 1;
        }
        groups.push(Group { first, tf });
        first = end;
    }

    reserve(raw_text_projection_workspace_bytes()?)?;
    let view = OneRow {
        small: &small,
        long: &long,
        groups: &groups,
        doc_len,
    };
    write_text_projection(final_path, 0, &view)?;
    let final_reader_metadata_bytes = SegmentReader::staged_metadata_bound(final_path)?;
    Ok(LargeTextRowReceipt {
        doc_len,
        final_reader_metadata_bytes,
    })
}

/// The exact shared scanner: Unicode whitespace separates, then leading and
/// trailing non-alphanumeric scalars are removed before threshold selection.
fn visit_tokens(input: &str, mut emit: impl FnMut(&str) -> Result<()>) -> Result<()> {
    let mut rest = input;
    while !rest.is_empty() {
        rest = rest.trim_start();
        if rest.is_empty() {
            break;
        }
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let raw = &rest[..end];
        rest = &rest[end..];
        let token = raw.trim_matches(|c: char| !c.is_alphanumeric());
        if !token.is_empty() {
            emit(token)?;
        }
    }
    Ok(())
}

fn emit_small_lower(token: &str, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<()> {
    if token
        .as_bytes()
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        return emit(token);
    }
    // Token source bytes are bounded by `threshold`; the existing row writer
    // immediately copies this short temporary into its priced run storage.
    emit(&token.to_lowercase())
}

struct MappedToken {
    mmap: Mmap,
}
impl MappedToken {
    fn write(token: &str, dir: &Path) -> Result<Self> {
        let path = fresh(dir, "large-token", "utf8");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| format!("create normalized Text token {}", path.display()))?;
        // The enclosing call reserved both handle and fixed lowercase workspace
        // before creating this file; the lower primitive's confirming callback
        // therefore performs no new allocation admission.
        write_streaming_lowercase(token, &mut file, |_| Ok(()))?;
        token_failure("normalize")?;
        file.sync_all()?;
        token_failure("map")?;
        let mmap =
            unsafe { MmapOptions::new().map(&file) }.context("mmap normalized Text token")?;
        if mmap.is_empty() {
            bail!("trimmed Text token normalized to empty")
        }
        // SAFETY: the only writer is the lowercase primitive, which emits UTF-8;
        // sync completed before mapping and this private file is never reopened.
        let _ = unsafe { std::str::from_utf8_unchecked(&mmap) };
        Ok(Self { mmap })
    }
    fn as_str(&self) -> &str {
        // SAFETY: established in `write`; immutable mmap lifetime is `self`.
        unsafe { std::str::from_utf8_unchecked(&self.mmap) }
    }
}

fn reserve_vector_growth<T>(
    values: &mut Vec<T>,
    reserve: &mut impl FnMut(usize) -> Result<()>,
) -> Result<()> {
    if values.len() != values.capacity() {
        return Ok(());
    }
    let next = values
        .capacity()
        .checked_mul(2)
        .map(|n| n.max(4))
        .ok_or_else(|| anyhow!("large Text metadata capacity overflow"))?;
    // Price the full new allocation while the previous allocation is still
    // live. Explicit reserve avoids Vec::push's implicit minimum capacity.
    reserve(
        next.checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| anyhow!("large Text metadata capacity overflow"))?,
    )?;
    values
        .try_reserve_exact(next - values.len())
        .map_err(|error| anyhow!("reserve large Text metadata: {error}"))?;
    Ok(())
}

/// The raw dictionary writer has an outer and offsets `BufWriter` (8KiB each),
/// a 64KiB offsets-copy buffer, and one one-row posting block. One posting is
/// only count/docid/TF, so three 64KiB blocks cover suffixes, CBOR, and LZ4.
fn raw_text_projection_workspace_bytes() -> Result<usize> {
    IO_BUFFER_BYTES
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(RAW_OFFSET_COPY_BYTES))
        .and_then(|bytes| bytes.checked_add(VAR_BLOCK_BYTES.checked_mul(3)?))
        .ok_or_else(|| anyhow!("raw Text projection workspace overflow"))
}

struct PrivateDirectory {
    path: PathBuf,
}
impl PrivateDirectory {
    fn create(parent: &Path) -> Result<Self> {
        fs::create_dir_all(parent)?;
        for _ in 0..128 {
            let path = parent.join(format!(
                ".large-text-row-{}-{}",
                std::process::id(),
                NONCE.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        bail!("could not create private large Text staging directory")
    }
    fn path(&self) -> &Path {
        &self.path
    }
}
impl Drop for PrivateDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn fresh(dir: &Path, kind: &str, extension: &str) -> PathBuf {
    dir.join(format!(
        ".{kind}-{}-{}.{}",
        std::process::id(),
        NONCE.fetch_add(1, Ordering::Relaxed),
        extension
    ))
}

#[cfg(test)]
mod tests;
