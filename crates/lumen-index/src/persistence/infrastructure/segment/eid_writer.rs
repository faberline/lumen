//! Writes an external-ID segment.

use std::path::Path;

use anyhow::{Context, Result};

use crate::persistence::infrastructure::segment::format::{finalize_and_write, header_block};
use crate::persistence::infrastructure::segment::var_column::append_var_blob_column;
use crate::persistence::infrastructure::segment::{page_align, ROLE_EID};

/// Write the collection-level EID meta segment (Phase 2f-1). `eids[i]` is the
/// external_id string of docid `i` in dense `[0..n_docs)` order — exactly the
/// interner's `to_eid` Vec. Stored as ONE var-width LZ4 column (role
/// [`ROLE_EID`]) by position, so a reopen rebuilds the whole `Interner` from
/// this file alone. No present bitset / fixed region is needed: every docid in
/// `[0..n_docs)` has an external_id (the interner is append-only and dense), so
/// the column's entry count IS `n_docs`. `n_docs` is also stamped in the header
/// for cross-checking. Makes the sealed collection self-describing + reopenable
/// without a CBOR snapshot.
pub fn write_eid_segment(path: &Path, applied_seq: u64, eids: &[&str]) -> Result<()> {
    let n_docs: u32 = eids
        .len()
        .try_into()
        .context("eid segment exceeds u32 doc capacity")?;

    let mut buf = header_block(applied_seq, n_docs, 0, 0);

    // --- VAR REGION (the eid-by-position dictionary). Page-align the start so
    // the var region begins on a clean boundary, mirroring the other writers. ---
    let region_end = page_align(buf.len());
    buf.resize(region_end, 0);
    let entries: Vec<Vec<u8>> = eids.iter().map(|s| s.as_bytes().to_vec()).collect();
    let eid_ref = append_var_blob_column(&mut buf, &entries, ROLE_EID, "eid")?;

    let dir = vec![eid_ref];
    finalize_and_write(path, buf, dir)
}
