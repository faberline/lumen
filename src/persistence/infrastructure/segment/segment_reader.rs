//! Opening a segment: the checks that fail closed before a reader exists, and
//! the private staging directories a reader can own.

pub(crate) mod columns;
pub(crate) mod eid;
pub(crate) mod hash;
pub(crate) mod keyword;
pub(crate) mod number;
pub(crate) mod set;
pub(crate) mod text;
pub(crate) mod vector;

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};

use crate::persistence::infrastructure::segment::reader_cache::{
    cached_posting_weight, cached_text_posting_weight, decoded_block_weight, posting_cache_bytes,
    CachedPosting, CachedTextPosting, DecodedBlock, DEFAULT_VAR_CACHE_BYTES,
};
use crate::persistence::infrastructure::segment::var_column::{
    decode_var_skip_index, VarBlockMeta,
};
use crate::persistence::infrastructure::segment::{
    ColumnRef, Footer, Header, OwnedStageDirectory, ScalarPayloadKind, SegmentReader, CODEC_FIXED,
    CODEC_LZ4_VAR, CODEC_RAW_VAR, FOOTER_LEN, FORMAT_VER, HEADER_LEN, HOST_ENDIAN_MARKER, MAGIC1,
    MAGIC2, PAGE, ROLE_DICT, ROLE_DICT_OFFSETS, ROLE_KEYWORD_DICTID, ROLE_KEYWORD_POSTINGS,
    ROLE_NUMBER, ROLE_NUMBER_POSTINGS, ROLE_NUMBER_SORTED, ROLE_SET_OFFSETS, ROLE_SET_PACKED,
    ROLE_SET_POSTINGS,
};

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
use crate::persistence::infrastructure::segment::OWNED_STAGE_OPEN_CALLS;

/// Fail closed before constructing a reader.  Raw dictionary entry boundaries
/// must stay in the mmap; decoding them into a `Vec` would reintroduce an
/// O(dictionary) heap allocation during staging.
fn validate_column_codecs(
    mmap: &memmap2::Mmap,
    dir_offset: usize,
    dir: &[ColumnRef],
) -> Result<()> {
    for column in dir {
        match column.codec {
            CODEC_FIXED | CODEC_LZ4_VAR => {}
            CODEC_RAW_VAR if column.role == ROLE_DICT => {}
            codec => bail!(
                "unsupported segment column codec {codec} for role {}",
                column.role
            ),
        }
    }
    let mut dicts = dir.iter().filter(|column| column.role == ROLE_DICT);
    let dict = dicts.next();
    if dicts.next().is_some() {
        bail!("segment has duplicate dictionary columns");
    }
    let mut offset_columns = dir.iter().filter(|column| column.role == ROLE_DICT_OFFSETS);
    let offsets = offset_columns.next();
    if offset_columns.next().is_some() {
        bail!("segment has duplicate raw dictionary offsets columns");
    }
    let Some(dict) = dict else {
        if offsets.is_some() {
            bail!("segment has orphan raw dictionary offsets column");
        }
        return Ok(());
    };
    if dict.codec != CODEC_RAW_VAR {
        if offsets.is_some() {
            bail!("segment has orphan raw dictionary offsets column");
        }
        return Ok(());
    }
    if !dict.skip_index.is_empty() || dict.width != 0 {
        bail!("raw dictionary has variable metadata")
    }
    if dict.elem_count > u64::from(u32::MAX) {
        bail!("raw dictionary exceeds u32 ordinal capacity")
    }
    let offsets = offsets.ok_or_else(|| anyhow!("raw dictionary is missing offsets column"))?;
    if offsets.codec != CODEC_FIXED
        || offsets.width != 8
        || !offsets.skip_index.is_empty()
        || offsets.byte_offset % 8 != 0
        || offsets.byte_len % 8 != 0
    {
        bail!("raw dictionary offsets must be fixed u64")
    }
    let expected_count = dict
        .elem_count
        .checked_add(1)
        .ok_or_else(|| anyhow!("raw dictionary offset count overflow"))?;
    if offsets.elem_count != expected_count || offsets.byte_len != expected_count.saturating_mul(8)
    {
        bail!("raw dictionary offset count does not match dictionary")
    }
    let range = |column: &ColumnRef| -> Result<&[u8]> {
        let start = usize::try_from(column.byte_offset).context("segment column offset")?;
        let len = usize::try_from(column.byte_len).context("segment column length")?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| anyhow!("segment column range overflow"))?;
        if start < HEADER_LEN || end > dir_offset {
            bail!("segment column exceeds directory boundary")
        }
        mmap.get(start..end)
            .ok_or_else(|| anyhow!("segment column is out of range"))
    };
    let data = range(dict)?;
    let offset_bytes = range(offsets)?;
    let ranges_overlap = |left: &ColumnRef, right: &ColumnRef| {
        let left_start = left.byte_offset;
        let left_end = left_start.saturating_add(left.byte_len);
        let right_start = right.byte_offset;
        let right_end = right_start.saturating_add(right.byte_len);
        left_start < right_end && right_start < left_end
    };
    if ranges_overlap(dict, offsets) {
        bail!("raw dictionary bytes overlap offsets column")
    }
    let values = bytemuck::try_cast_slice::<u8, u64>(offset_bytes)
        .map_err(|_| anyhow!("raw dictionary offsets are misaligned"))?;
    if values.first() != Some(&0) || values.last() != Some(&(data.len() as u64)) {
        bail!("raw dictionary offsets must start at zero and end at dictionary length")
    }
    if values.windows(2).any(|pair| pair[0] > pair[1]) {
        bail!("raw dictionary offsets are not monotone")
    }
    for (index, pair) in values.windows(2).enumerate() {
        let start = usize::try_from(pair[0]).context("raw dictionary offset")?;
        let end = usize::try_from(pair[1]).context("raw dictionary offset")?;
        let entry = data
            .get(start..end)
            .ok_or_else(|| anyhow!("raw dictionary offset is out of range"))?;
        std::str::from_utf8(entry).context("raw dictionary entry is not UTF-8")?;
        if index != 0 {
            // Compare adjacent mmap slices directly; do not retain a `Vec` or
            // `String` for the previous (possibly huge) dictionary term.
            let prior_start =
                usize::try_from(values[index - 1]).context("raw dictionary offset")?;
            let prior_end = usize::try_from(values[index]).context("raw dictionary offset")?;
            let prior = data
                .get(prior_start..prior_end)
                .ok_or_else(|| anyhow!("raw dictionary offset is out of range"))?;
            if prior >= entry {
                bail!("raw dictionary entries are not strictly sorted")
            }
        }
    }
    Ok(())
}

impl SegmentReader {
    /// Identify a complete scalar column family without interpreting a row as
    /// absent when a staged file was created for another field kind.
    pub(crate) fn scalar_payload_kind(&self) -> Option<ScalarPayloadKind> {
        let keyword = self.column(ROLE_KEYWORD_DICTID).is_some()
            && self.column(ROLE_DICT).is_some()
            && self.column(ROLE_KEYWORD_POSTINGS).is_some();
        let set = self.column(ROLE_SET_OFFSETS).is_some()
            && self.column(ROLE_SET_PACKED).is_some()
            && self.column(ROLE_DICT).is_some()
            && self.column(ROLE_SET_POSTINGS).is_some();
        let number = self.column(ROLE_NUMBER).is_some()
            && self.column(ROLE_NUMBER_SORTED).is_some()
            && self.column(ROLE_NUMBER_POSTINGS).is_some();
        match (keyword, number, set) {
            (true, false, false) => Some(ScalarPayloadKind::Keyword),
            (false, true, false) => Some(ScalarPayloadKind::Number),
            (false, false, true) => Some(ScalarPayloadKind::Set),
            _ => None,
        }
    }

    /// Bound heap metadata needed to open a locally authored staged segment.
    /// This deliberately reads only the fixed footer and validates its directory
    /// range. It does not mmap or decode CBOR before the caller reserves this
    /// reader-owned allocation.
    pub(crate) fn staged_metadata_bound(path: &Path) -> Result<usize> {
        let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let len = usize::try_from(file.seek(SeekFrom::End(0))?)
            .context("staged segment exceeds platform size")?;
        if len < HEADER_LEN + FOOTER_LEN {
            bail!("segment too small: {len} bytes");
        }
        file.seek(SeekFrom::End(-(FOOTER_LEN as i64)))?;
        let mut footer_bytes = [0u8; FOOTER_LEN];
        file.read_exact(&mut footer_bytes)?;
        let footer = Footer::from_bytes(&footer_bytes)?;
        if footer.magic2 != MAGIC2 {
            bail!("bad footer magic2: {:#x}", footer.magic2);
        }
        let dir_offset = usize::try_from(footer.dir_offset).context("directory offset")?;
        let dir_len = usize::try_from(footer.dir_len).context("directory length")?;
        let footer_offset = len - FOOTER_LEN;
        let dir_end = dir_offset
            .checked_add(dir_len)
            .ok_or_else(|| anyhow!("directory length overflow"))?;
        if dir_offset < HEADER_LEN || dir_end > footer_offset {
            bail!("directory out of range: [{dir_offset}..{dir_end}) vs footer at {footer_offset}");
        }

        // CBOR directory data is decoded into `Vec<ColumnRef>`, strings and
        // skip-index Vecs. A locally authored directory cannot need more
        // element payload than its encoded bytes, but charge one ColumnRef per
        // encoded byte as a conservative sparse-entry bound. The fixed reader
        // and three cache handles are reader-owned allocations as well.
        let directory = dir_len
            .checked_mul(std::mem::size_of::<ColumnRef>())
            .and_then(|n| n.checked_add(dir_len))
            .ok_or_else(|| anyhow!("staged directory metadata overflows usize"))?;
        // Each encoded skip byte may describe one sparse block entry in the
        // most pessimistic valid layout. The reader retains both encoded
        // directory bytes and this parsed immutable index.
        let parsed_skip = dir_len
            .checked_mul(std::mem::size_of::<VarBlockMeta>())
            .and_then(|n| n.checked_add(dir_len))
            .ok_or_else(|| anyhow!("staged parsed skip metadata overflows usize"))?;
        std::mem::size_of::<SegmentReader>()
            .checked_add(directory)
            .and_then(|n| n.checked_add(parsed_skip))
            .and_then(|n| n.checked_add(3 * std::mem::size_of::<usize>()))
            .ok_or_else(|| anyhow!("staged reader metadata overflows usize"))
    }

    /// Open a staged segment after its metadata bound has been reserved. The
    /// private directory is retained by every `Arc<SegmentReader>` clone.
    pub(crate) fn open_owned_stage(path: &Path, stage_dir: PathBuf) -> Result<SegmentReader> {
        #[cfg(test)]
        OWNED_STAGE_OPEN_CALLS.with(|count| count.set(count.get() + 1));
        let mut reader = Self::open(path)?;
        reader.owned_stage = Some(Arc::new(OwnedStageDirectory { path: stage_dir }));
        Ok(reader)
    }

    #[cfg(test)]
    pub(crate) fn owned_stage_dir(&self) -> Option<&Path> {
        self.owned_stage.as_ref().map(|stage| stage.path.as_path())
    }

    #[cfg(test)]
    pub(crate) fn reset_owned_stage_open_calls() {
        OWNED_STAGE_OPEN_CALLS.with(|count| count.set(0));
    }

    #[cfg(test)]
    pub(crate) fn owned_stage_open_calls() -> usize {
        OWNED_STAGE_OPEN_CALLS.with(Cell::get)
    }

    /// Open and validate a segment file. Reads the footer first, validates
    /// `magic2`, the directory crc32, then the header magic / version /
    /// endianness. Any mismatch (bad magic, bad crc, wrong endian, directory
    /// pointer out of range) returns `Err`; the caller discards and replays.
    pub fn open(path: &Path) -> Result<SegmentReader> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        // SAFETY: read-only mapping; we never mutate it and treat all bytes as
        // untrusted (bounds-checked, crc-checked before use).
        let mmap = unsafe { memmap2::Mmap::map(&file) }
            .with_context(|| format!("mmap {}", path.display()))?;
        let len = mmap.len();

        // A valid file is at least header + footer.
        if len < HEADER_LEN + FOOTER_LEN {
            bail!("segment too small: {len} bytes");
        }

        // The kernel page-aligns the mapping base; a page-aligned column
        // offset therefore yields an 8-byte-aligned slice for try_cast_slice.
        debug_assert_eq!(
            mmap.as_ptr() as usize % PAGE,
            0,
            "mmap base must be page-aligned"
        );

        // --- FOOTER (tail-first) ---
        let footer_off = len - FOOTER_LEN;
        let footer = Footer::from_bytes(&mmap[footer_off..len])?;
        if footer.magic2 != MAGIC2 {
            bail!("bad footer magic2: {:#x}", footer.magic2);
        }

        // Directory must lie strictly inside [HEADER_LEN, footer_off].
        let dir_offset = footer.dir_offset as usize;
        let dir_len = footer.dir_len as usize;
        let dir_end = dir_offset
            .checked_add(dir_len)
            .ok_or_else(|| anyhow!("directory length overflow"))?;
        if dir_offset < HEADER_LEN || dir_end > footer_off {
            bail!("directory out of range: [{dir_offset}..{dir_end}) vs footer at {footer_off}");
        }

        // --- CRC over the directory bytes ---
        let dir_bytes = &mmap[dir_offset..dir_end];
        let crc = crc32fast::hash(dir_bytes);
        if crc != footer.crc32 {
            bail!(
                "directory crc mismatch: computed {:#x} != footer {:#x}",
                crc,
                footer.crc32
            );
        }

        // --- HEADER ---
        let header = Header::from_bytes(&mmap[..HEADER_LEN])?;
        if header.magic1 != MAGIC1 {
            bail!("bad header magic1: {:#x}", header.magic1);
        }
        if header.format_ver != FORMAT_VER {
            bail!("unsupported format_ver: {}", header.format_ver);
        }
        if header.host_endian_marker != HOST_ENDIAN_MARKER {
            bail!(
                "host endian mismatch: {:#x} != {:#x}",
                header.host_endian_marker,
                HOST_ENDIAN_MARKER
            );
        }

        // --- DIRECTORY (CBOR) ---
        let dir: Vec<ColumnRef> = ciborium::from_reader(dir_bytes)
            .map_err(|e| anyhow!("cbor decode segment directory: {e}"))?;
        validate_column_codecs(&mmap, dir_offset, &dir)?;
        let var_skip_indices = dir.iter().map(decode_var_skip_index).collect();

        let block_cache: moka::sync::Cache<u64, DecodedBlock> = moka::sync::Cache::builder()
            .weigher(|_k: &u64, v: &DecodedBlock| decoded_block_weight(v))
            .max_capacity(DEFAULT_VAR_CACHE_BYTES)
            .build();

        // BOUNDED decoded-posting caches (Phase 2m), mirroring `block_cache`'s
        // byte-weighted moka build. Sized off `LUMEN_SEG_POSTING_CACHE_MB`.
        let posting_budget = posting_cache_bytes();
        let posting_cache: moka::sync::Cache<u64, CachedPosting> = moka::sync::Cache::builder()
            .weigher(|_k: &u64, v: &CachedPosting| cached_posting_weight(v))
            .max_capacity(posting_budget)
            .build();
        let text_posting_cache: moka::sync::Cache<u64, CachedTextPosting> =
            moka::sync::Cache::builder()
                .weigher(|_k: &u64, v: &CachedTextPosting| cached_text_posting_weight(v))
                .max_capacity(posting_budget)
                .build();

        Ok(SegmentReader {
            mmap: Arc::new(mmap),
            dir,
            var_skip_indices,
            applied_seq: header.applied_seq,
            n_docs: header.n_docs,
            doc_count: header.doc_count,
            total_doc_len: header.total_doc_len,
            block_cache,
            posting_cache,
            text_posting_cache,
            owned_stage: None,
        })
    }
}
