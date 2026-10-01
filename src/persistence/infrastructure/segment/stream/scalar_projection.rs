//! What a scalar projection needs from its source and its owner: the rewindable
//! Keyword, Set and Number source traits, the scratch charge and its peak
//! bound, and the error that asks the owner for more scratch.

use anyhow::{anyhow, Result};

use crate::persistence::infrastructure::segment::reader_cache::DEFAULT_VAR_CACHE_BYTES;
use crate::persistence::infrastructure::segment::var_column::{VarBlockMeta, VarEntry};
use crate::persistence::infrastructure::segment::*;

/// Apply-time temporary storage charged by the staging owner.  This is an
/// internal retry charge, never a public request limit.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ScalarProjectionScratch {
    max_entry_bytes: usize,
    pub(super) raw_dictionary: bool,
}
impl ScalarProjectionScratch {
    pub(crate) const fn new(max_entry_bytes: usize) -> Self {
        Self {
            max_entry_bytes,
            raw_dictionary: false,
        }
    }
    /// Select the mmap-readable raw dictionary codec for a scalar projection.
    /// It is private to the projection path; existing callers retain the
    /// bounded prefix/LZ4 codec by default.
    pub(crate) const fn raw_scalar_dictionary(mut self) -> Self {
        self.raw_dictionary = true;
        self
    }
    pub(super) fn require(&self, kind: &'static str, required: usize) -> Result<()> {
        if required <= self.max_entry_bytes {
            Ok(())
        } else {
            Err(ScalarProjectionScratchRequired { kind, required }.into())
        }
    }
}

#[derive(Debug)]
pub(crate) struct ScalarProjectionScratchRequired {
    pub(crate) kind: &'static str,
    pub(crate) required: usize,
}
impl std::fmt::Display for ScalarProjectionScratchRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "scalar projection {} needs {} bytes of scratch",
            self.kind, self.required
        )
    }
}
impl std::error::Error for ScalarProjectionScratchRequired {}

/// Peak ownership for a projection whose VAR columns use full-entry block
/// boundaries. Counts include duplicate source terms, which safely overprice
/// dictionaries after deduplication. No term content is needed for this pass.
pub(crate) fn scalar_projection_peak_bound(
    max_entry: usize,
    term_bytes: usize,
    terms: usize,
    memberships: usize,
) -> Result<usize> {
    let overflow = || anyhow!("scalar projection memory bound overflow");
    let add = |a: usize, b: usize| a.checked_add(b).ok_or_else(overflow);
    let mul = |a: usize, b: usize| a.checked_mul(b).ok_or_else(overflow);
    // Every flushed block except the last has at least 64 KiB of full entry
    // bytes + four bytes per entry. One large entry may exceed that target.
    let block = add(VAR_BLOCK_BYTES, add(max_entry, 4)?)?;
    let entries = terms.min(VAR_BLOCK_BYTES / 4 + 1);
    // CBOR: <= 2 bytes/u8, <= 64 bytes per fixed VarEntry map, plus outer
    // framing. Vec growth is charged at twice the encoded bound. LZ4's output
    // maximum is below 2*raw+64, including the prepended length.
    let raw = add(add(mul(2, block)?, mul(64, entries)?)?, 256)?;
    let entry_vectors = mul(
        mul(2, entries)?,
        std::mem::size_of::<VarEntry>() + std::mem::size_of::<Vec<u8>>(),
    )?;
    let codec_peak = add(add(mul(8, block)?, mul(6, raw)?)?, entry_vectors)?;
    let dictionary_total = add(term_bytes, mul(4, terms)?)?;
    // Each count and each u32 delta needs at most five LEB128 bytes.
    let posting_total = add(mul(9, terms)?, mul(5, memberships)?)?;
    let dict_blocks = add(dictionary_total / VAR_BLOCK_BYTES, 1)?;
    let post_blocks = add(posting_total / VAR_BLOCK_BYTES, 1)?;
    let blocks = add(dict_blocks, post_blocks)?;
    // Each skip entry has four bounded integer fields. 128 bytes/entry plus
    // 4096 for all fixed column names, maps, arrays and final directory framing
    // overprices the existing CBOR format. The spool has a subset of this.
    let directory = add(4096, mul(128, blocks)?)?;
    let reader = add(
        std::mem::size_of::<SegmentReader>(),
        mul(
            directory,
            std::mem::size_of::<ColumnRef>() + std::mem::size_of::<VarBlockMeta>() + 8,
        )?,
    )?;
    let writer_metadata = add(
        mul(mul(2, blocks)?, std::mem::size_of::<VarBlockMeta>())?,
        mul(8, directory)?,
    )?;
    // A spool cache can retain 16 MiB while a miss decodes a new block and a
    // target writer retains its current block. Include both codec workspaces.
    add(
        add(
            add(DEFAULT_VAR_CACHE_BYTES as usize, mul(2, codec_peak)?)?,
            add(reader, writer_metadata)?,
        )?,
        128 * 1024,
    )
}

/// A scalar source supplies fresh, rewindable callback passes.  The fast WAL
/// source can therefore lend UTF-8 values directly from retained command bytes.
pub(crate) trait KeywordStreamProjection {
    fn n_docs(&self) -> u32;
    fn keyword_row(&self, row: u32, emit: &mut dyn FnMut(Option<&str>) -> Result<()>)
        -> Result<()>;
    fn keyword_terms(&self, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<()>;
    fn keyword_posting(&self, term: &str, emit: &mut dyn FnMut(u32) -> Result<()>) -> Result<bool>;
}
pub(crate) trait SetStreamProjection {
    fn n_docs(&self) -> u32;
    /// Returns false for an absent row; a present empty row calls no emit.
    fn set_row(&self, row: u32, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<bool>;
    fn set_terms(&self, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<()>;
    fn set_posting(&self, term: &str, emit: &mut dyn FnMut(u32) -> Result<()>) -> Result<bool>;
}
pub(crate) trait NumberStreamProjection {
    fn n_docs(&self) -> u32;
    fn number_row(&self, row: u32, emit: &mut dyn FnMut(Option<f64>) -> Result<()>) -> Result<()>;
    fn number_keys(&self, emit: &mut dyn FnMut(u64) -> Result<()>) -> Result<()>;
    fn number_posting(&self, key: u64, emit: &mut dyn FnMut(u32) -> Result<()>) -> Result<bool>;
}
