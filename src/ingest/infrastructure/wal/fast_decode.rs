//! Decoding a `WalRecord`: the fast binary format, or bounded CBOR for a
//! generic record.

use anyhow::{anyhow, Result};

use crate::ingest::domain::wal_record::{
    WalRecord, WAL_CONTROL_FORMAT_VERSION, WAL_FORMAT_VERSION,
};
use crate::ingest::infrastructure::wal::fast_cursor::FastCursor;
use crate::ingest::infrastructure::wal::{
    WAL_FAST_INDEX, WAL_FAST_INDEX_VERSIONED, WAL_FAST_MAGIC, WAL_FAST_TRUNCATE_DOCS,
    WAL_FAST_UNINDEX_DOCS,
};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{
    validate_batch_unindex_docs_request, BatchUnindexDocsRequest, IndexItem, IndexRequest,
    MAX_BATCH_UNINDEX_DOCS_SIZE,
};

impl WalRecord {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.starts_with(WAL_FAST_MAGIC) {
            return decode_fast_record(bytes);
        }
        let rec = crate::ingest::infrastructure::wal::bounded_generic::decode(bytes)?;
        anyhow::ensure!(
            rec.version == WAL_FORMAT_VERSION,
            "unsupported generic WAL record version {} (expected {})",
            rec.version,
            WAL_FORMAT_VERSION
        );
        anyhow::ensure!(
            !matches!(
                &rec.entry,
                RaftLogEntry::TruncateDocs { .. } | RaftLogEntry::UnindexDocs { .. }
            ),
            "control commands must use WAL v{} fast control encoding",
            WAL_CONTROL_FORMAT_VERSION
        );
        Ok(rec)
    }
}

fn decode_fast_record(bytes: &[u8]) -> Result<WalRecord> {
    let mut cur = FastCursor::new(bytes);
    cur.expect_magic(WAL_FAST_MAGIC)?;
    let version = cur.read_u8()?;
    anyhow::ensure!(
        matches!(version, WAL_FORMAT_VERSION | WAL_CONTROL_FORMAT_VERSION),
        "unsupported WAL fast record version {} (expected {} or {})",
        version,
        WAL_FORMAT_VERSION,
        WAL_CONTROL_FORMAT_VERSION
    );
    let tag = cur.read_u8()?;
    if version == WAL_CONTROL_FORMAT_VERSION {
        return match tag {
            WAL_FAST_TRUNCATE_DOCS => {
                let collection_id = cur.read_string()?;
                cur.expect_eof()?;
                Ok(WalRecord {
                    version,
                    entry: RaftLogEntry::TruncateDocs { collection_id },
                })
            }
            WAL_FAST_UNINDEX_DOCS => {
                let collection_id = cur.read_string()?;
                let item_count = cur.read_u32()? as usize;
                // Validate the untrusted count before `Vec::with_capacity`.
                // A corrupt WAL frame must not force a replica to reserve an
                // attacker-sized allocation merely to reject it later.
                anyhow::ensure!(
                    (1..=MAX_BATCH_UNINDEX_DOCS_SIZE).contains(&item_count),
                    "invalid WAL UnindexDocs item count {item_count} (must be 1..={MAX_BATCH_UNINDEX_DOCS_SIZE})"
                );
                let mut external_ids = Vec::with_capacity(item_count);
                let mut seen = std::collections::BTreeSet::new();
                for _ in 0..item_count {
                    let external_id = cur.read_string()?;
                    anyhow::ensure!(
                        seen.insert(external_id.clone()),
                        "duplicate external_id in WAL UnindexDocs record"
                    );
                    external_ids.push(external_id);
                }
                cur.expect_eof()?;
                let req = BatchUnindexDocsRequest { external_ids };
                validate_batch_unindex_docs_request(&req)?;
                Ok(WalRecord {
                    version,
                    entry: RaftLogEntry::UnindexDocs { collection_id, req },
                })
            }
            _ => Err(anyhow!("unsupported WAL v2 control tag {tag}")),
        };
    }
    anyhow::ensure!(
        tag == WAL_FAST_INDEX || tag == WAL_FAST_INDEX_VERSIONED,
        "unsupported WAL fast record tag {tag}"
    );
    let collection_id = cur.read_string()?;
    let request_id = match cur.read_u8()? {
        0 => None,
        1 => Some(cur.read_string()?),
        other => return Err(anyhow!("invalid WAL fast request_id tag {other}")),
    };
    let item_count = cur.read_u32()? as usize;
    let mut items = Vec::with_capacity(item_count);
    for _ in 0..item_count {
        let external_id = cur.read_string()?;
        let field = cur.read_string()?;
        // `WAL_FAST_INDEX` (legacy, pre-#3952) never wrote a version on the
        // wire at all — every item it produced reconstructs as `None`,
        // unchanged from before this fix, so an AOF/WAL segment written by an
        // older binary keeps decoding exactly as it always did.
        // `WAL_FAST_INDEX_VERSIONED` carries an explicit presence byte per
        // item.
        let version = if tag == WAL_FAST_INDEX_VERSIONED {
            match cur.read_u8()? {
                0 => None,
                1 => Some(cur.read_u64()?),
                other => return Err(anyhow!("invalid WAL fast item version tag {other}")),
            }
        } else {
            None
        };
        let value = cur.read_field_value()?;
        items.push(IndexItem {
            external_id,
            field,
            value,
            version,
        });
    }
    cur.expect_eof()?;
    Ok(WalRecord {
        version,
        entry: RaftLogEntry::Index {
            collection_id,
            req: IndexRequest { items, request_id },
        },
    })
}
