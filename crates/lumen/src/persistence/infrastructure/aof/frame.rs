//! The AOF frame payload: a [`WalRecord`] encoded into, and decoded from, the
//! payload of one storage-durable log frame.

use anyhow::{Context, Result};

use crate::ingest::domain::wal_record::WalRecord;

/// Fixed per-frame header width: `seq(8) + len(4) + crc(4)`.
#[cfg(test)]
pub(super) const HEADER_LEN: usize = 16;

/// Encode a [`WalRecord`] payload. Common high-QPS index records use Lumen's
/// fast binary WAL codec; uncommon records fall back to compact CBOR.
pub(super) fn encode_payload(rec: &WalRecord) -> Result<Vec<u8>> {
    rec.encode().context("encode AOF record")
}

/// Decode an AOF payload back into a [`WalRecord`].
pub(super) fn decode_payload(bytes: &[u8]) -> Result<WalRecord> {
    WalRecord::decode(bytes).context("decode AOF record")
}
