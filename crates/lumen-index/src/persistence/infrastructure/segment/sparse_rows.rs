//! The stable external IDs of an immutable sparse delta's local rows, stored as
//! a CBOR string array and decoded with a bound on the row count.

use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};

/// Stable external IDs for the local rows of an immutable sparse delta.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SparseLocalRows(Vec<String>);

impl SparseLocalRows {
    /// Returns the validated IDs without copying their strings.
    pub(crate) fn into_external_ids(self) -> Vec<String> {
        self.0
    }

    pub fn external_id(&self, local_row: u32) -> Option<&str> {
        self.0.get(local_row as usize).map(String::as_str)
    }
    pub fn len(&self) -> u32 {
        self.0.len() as u32
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub fn encode_sparse_local_rows(path: &Path, external_ids: &[String]) -> Result<()> {
    let count: u32 = external_ids
        .len()
        .try_into()
        .context("sparse local rows exceed u32 row capacity")?;
    validate_sparse_rows(external_ids, count)?;
    let mut bytes = Vec::new();
    ciborium::into_writer(external_ids, &mut bytes)
        .map_err(|error| anyhow!("encode sparse local rows: {error}"))?;
    std::fs::write(path, bytes)
        .with_context(|| format!("write sparse local rows {}", path.display()))
}

pub fn decode_sparse_local_rows(path: &Path, expected_count: u32) -> Result<SparseLocalRows> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("read sparse local rows {}", path.display()))?;
    if expected_count as usize > bytes.len() {
        bail!("sparse local rows count mismatch: declared {expected_count}, input is too short");
    }
    let rows = decode_cbor_string_array(&bytes, expected_count as usize)?;
    Ok(SparseLocalRows(rows))
}

fn decode_cbor_string_array(bytes: &[u8], expected: usize) -> Result<Vec<String>> {
    let mut at = 0usize;
    let Some(head) = bytes.get(at).copied() else {
        bail!("decode sparse local rows: truncated CBOR");
    };
    at += 1;
    if head >> 5 != 4 {
        bail!("decode sparse local rows: expected CBOR array");
    }
    let count = cbor_len(head & 31, bytes, &mut at)?;
    if count != expected {
        bail!("sparse local rows count mismatch");
    }
    let mut rows = Vec::with_capacity(expected);
    for _ in 0..count {
        let head = *bytes
            .get(at)
            .ok_or_else(|| anyhow!("decode sparse local rows: truncated CBOR"))?;
        at += 1;
        if head >> 5 != 3 {
            bail!("decode sparse local rows: expected text string");
        }
        let len = cbor_len(head & 31, bytes, &mut at)?;
        let end = at
            .checked_add(len)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| anyhow!("decode sparse local rows: truncated CBOR"))?;
        rows.push(
            std::str::from_utf8(&bytes[at..end])
                .context("decode sparse local rows: invalid UTF-8")?
                .to_owned(),
        );
        at = end;
    }
    if at != bytes.len() {
        bail!("decode sparse local rows: trailing bytes");
    }
    validate_sparse_rows(&rows, expected as u32)?;
    Ok(rows)
}
fn cbor_len(ai: u8, bytes: &[u8], at: &mut usize) -> Result<usize> {
    let n = match ai {
        0..=23 => ai as u64,
        24 => take(bytes, at, 1)?,
        25 => take(bytes, at, 2)?,
        26 => take(bytes, at, 4)?,
        27 => take(bytes, at, 8)?,
        _ => bail!("decode sparse local rows: indefinite or reserved CBOR length"),
    };
    usize::try_from(n).context("decode sparse local rows: length exceeds platform")
}
fn take(bytes: &[u8], at: &mut usize, width: usize) -> Result<u64> {
    let end = at
        .checked_add(width)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| anyhow!("decode sparse local rows: truncated CBOR"))?;
    let mut n = 0;
    for b in &bytes[*at..end] {
        n = (n << 8) | u64::from(*b);
    }
    *at = end;
    Ok(n)
}

fn validate_sparse_rows(rows: &[String], expected: u32) -> Result<()> {
    if rows.len() != expected as usize {
        bail!("sparse local rows count mismatch");
    }
    let mut seen = std::collections::BTreeSet::new();
    for id in rows {
        if !seen.insert(id) {
            bail!("sparse local rows contain duplicate external id `{id}`");
        }
    }
    Ok(())
}
