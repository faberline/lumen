//! Bounded external-sort seal for one local Text row.
//!
//! This module is a child of `segment`, so it uses the established private
//! LSEG codec.  It stores sorted, deduplicated term runs on the caller's
//! staging filesystem and merges at fan-in two.  It never builds the field
//! dictionary, a postings map, or a token list in memory.
//!
//! Scratch memory uses one fixed codec/IO area and sixteen payload slots.
//! Sorted-run storage, merge heads, prefix buffers and serialized/compressed
//! blocks use those slots. Skip metadata streams through disk files, so it
//! does not grow the writer heap. An oversized individual term returns an
//! internal workspace requirement for the caller to reserve before retrying.

pub(crate) mod lseg_run;
pub(crate) mod row_var_writer;
pub(crate) mod sorted_run;

use crate::persistence::infrastructure::segment::text_row_stage::lseg_run::write_lseg_from_run;
use crate::persistence::infrastructure::segment::text_row_stage::sorted_run::{
    flush_run, merge_runs,
};
use anyhow::{anyhow, bail, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

static ROW_STAGE_NONCE: AtomicU64 = AtomicU64::new(0);

/// The current LSEG codec needs a raw CBOR block and its compressed block.
/// This reservation is deliberately part of, rather than outside, the caller
/// supplied scratch budget.
const MIN_CODEC_BYTES: usize = 2 * 1024 * 1024;
const PAYLOAD_SLOTS: usize = 16;
const MAX_BLOCK_ENTRIES: usize = 256;
const RUN_RECORD_OVERHEAD: usize = 2 * std::mem::size_of::<String>() + 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RequiredTextRowWorkspace {
    pub(crate) required_bytes: usize,
}

impl std::fmt::Display for RequiredTextRowWorkspace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "text row staging requires {} scratch bytes",
            self.required_bytes
        )
    }
}
impl std::error::Error for RequiredTextRowWorkspace {}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TextRowStageOptions {
    /// Total writer-owned RAM.  It excludes the borrowed token supplied by the
    /// caller, but includes every cloned run term and codec buffer.
    pub scratch_bytes: usize,
}

impl TextRowStageOptions {
    pub(crate) fn minimum_scratch_bytes() -> usize {
        MIN_CODEC_BYTES + PAYLOAD_SLOTS * RUN_RECORD_OVERHEAD
    }

    fn run_bytes(self) -> Result<usize> {
        self.scratch_bytes
            .checked_sub(MIN_CODEC_BYTES)
            .map(|bytes| bytes / PAYLOAD_SLOTS)
            .filter(|bytes| *bytes >= RUN_RECORD_OVERHEAD)
            .ok_or_else(|| {
                anyhow!(
                    "text row staging needs at least {} scratch bytes for the current LSEG codec",
                    Self::minimum_scratch_bytes()
                )
            })
    }
}

/// Feed borrowed normalized tokens to `emit`.  The stream must preserve token
/// repetitions; this writer derives each term frequency while it sorts runs.
pub(crate) type TextTokenStream<'a> =
    dyn FnOnce(&mut dyn FnMut(&str) -> Result<()>) -> Result<()> + 'a;

/// Build a current-format one-row Text segment at `path`.
///
/// `document_len` is the tokenizer's checked total token count.  An empty
/// token stream still writes an explicitly present row with document length
/// zero.  `scratch_dir` must be on the same filesystem as `path`, because the
/// final artifact is atomically renamed into place.
pub(crate) fn stage_text_row(
    path: &Path,
    applied_seq: u64,
    document_len: u32,
    scratch_dir: &Path,
    options: TextRowStageOptions,
    stream: Box<TextTokenStream<'_>>,
) -> Result<()> {
    let run_bytes = options.run_bytes()?;
    let mut workspace = RowWorkspace::new(scratch_dir, run_bytes)?;
    let mut terms = Vec::<String>::new();
    let mut used = 0usize;
    let mut feed_error = None;
    let mut seen = 0u64;
    let mut emit = |term: &str| -> Result<()> {
        seen = seen
            .checked_add(1)
            .ok_or_else(|| anyhow!("text token count overflow"))?;
        if term.is_empty() {
            bail!("normalized Text stream emitted an empty token");
        }
        let record = term
            .len()
            .checked_add(RUN_RECORD_OVERHEAD)
            .ok_or_else(|| anyhow!("text term length overflow"))?;
        if record > run_bytes {
            // Reprice the bounded workspace before any output. The same
            // on-disk format handles the term after the caller reserves it.
            let required_bytes = record
                .checked_mul(PAYLOAD_SLOTS)
                .and_then(|bytes| MIN_CODEC_BYTES.checked_add(bytes))
                .ok_or_else(|| anyhow!("required text workspace overflows usize"))?;
            return Err(RequiredTextRowWorkspace { required_bytes }.into());
        }
        if used
            .checked_add(record)
            .ok_or_else(|| anyhow!("text run byte count overflow"))?
            > run_bytes
        {
            flush_run(&mut workspace, &mut terms, &mut used)?;
        }
        terms.push(term.to_owned());
        used = used
            .checked_add(record)
            .ok_or_else(|| anyhow!("text run byte count overflow"))?;
        Ok(())
    };
    if let Err(error) = stream(&mut emit) {
        feed_error = Some(error);
    }
    drop(emit);
    if let Some(error) = feed_error {
        return Err(error);
    }
    if seen != u64::from(document_len) {
        bail!("text row document length mismatch: declared {document_len}, streamed {seen}");
    }
    flush_run(&mut workspace, &mut terms, &mut used)?;
    let run = workspace.finish_runs()?;
    drop(terms);
    write_lseg_from_run(
        path,
        applied_seq,
        document_len,
        run.as_deref(),
        options,
        workspace.dir(),
    )?;
    // The final LSEG is outside the private workspace.  Drop removes every
    // intermediate run after the atomic final rename succeeds.
    Ok(())
}

struct RowWorkspace {
    dir: PathBuf,
    levels: Vec<Option<PathBuf>>,
    armed: bool,
    max_term: usize,
}

impl RowWorkspace {
    fn new(parent: &Path, max_term: usize) -> Result<Self> {
        fs::create_dir_all(parent)
            .with_context(|| format!("create text row staging directory {}", parent.display()))?;
        let nonce = ROW_STAGE_NONCE.fetch_add(1, AtomicOrdering::Relaxed);
        let dir = parent.join(format!(".text-row-stage-{}-{nonce}", std::process::id()));
        fs::create_dir(&dir)
            .with_context(|| format!("create text row workspace {}", dir.display()))?;
        Ok(Self {
            dir,
            levels: Vec::new(),
            armed: true,
            max_term,
        })
    }

    fn dir(&self) -> &Path {
        &self.dir
    }

    fn push_run(&mut self, mut run: PathBuf) -> Result<()> {
        let mut level = 0usize;
        loop {
            if self.levels.len() == level {
                self.levels.push(Some(run));
                return Ok(());
            }
            match self.levels[level].take() {
                None => {
                    self.levels[level] = Some(run);
                    return Ok(());
                }
                Some(left) => {
                    // Merge immediately like a binary carry.  This retains
                    // O(log runs) paths and opens exactly two inputs.
                    run = merge_runs(self.dir(), &left, &run, self.max_term)?;
                    level += 1;
                }
            }
        }
    }

    fn finish_runs(&mut self) -> Result<Option<PathBuf>> {
        let mut runs: Vec<PathBuf> = self.levels.iter_mut().filter_map(Option::take).collect();
        if runs.is_empty() {
            return Ok(None);
        }
        // Pairwise merging keeps at most two input files open.  The vector
        // stores only O(log runs) paths after normal ingestion; final folding
        // is intentionally sequential and does not open all paths.
        let mut result = runs.remove(0);
        for next in runs {
            result = merge_runs(self.dir(), &result, &next, self.max_term)?;
        }
        Ok(Some(result))
    }
}

impl Drop for RowWorkspace {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }
}

fn fresh_path(dir: &Path, kind: &str) -> PathBuf {
    let nonce = ROW_STAGE_NONCE.fetch_add(1, AtomicOrdering::Relaxed);
    dir.join(format!("{kind}-{}-{nonce}.run", std::process::id()))
}

#[cfg(test)]
mod tests;
