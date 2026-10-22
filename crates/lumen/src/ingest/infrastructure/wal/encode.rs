//! Encoding a `WalRecord`: the fast binary format for Index and the control
//! records, CBOR for everything else.

use std::io::Write;

use anyhow::{anyhow, Result};

use crate::ingest::domain::wal_record::{
    WalRecord, WAL_CONTROL_FORMAT_VERSION, WAL_FORMAT_VERSION,
};
use crate::ingest::infrastructure::wal::{
    WAL_FAST_INDEX, WAL_FAST_INDEX_VERSIONED, WAL_FAST_MAGIC, WAL_FAST_TRUNCATE_DOCS,
    WAL_FAST_UNINDEX_DOCS, WAL_VALUE_NUMBER, WAL_VALUE_STRING, WAL_VALUE_STRING_LIST,
    WAL_VALUE_VECTOR,
};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{
    validate_batch_unindex_docs_request, FieldValue, IndexRequest,
};

impl WalRecord {
    pub(crate) fn is_fast_index_wire(&self) -> bool {
        self.version == WAL_FORMAT_VERSION && matches!(&self.entry, RaftLogEntry::Index { .. })
    }

    /// Stream the frozen fast Index wire format. This writes no aggregate
    /// payload buffer and rejects every length that cannot fit the public u32
    /// frame fields before it writes that field.
    pub(crate) fn write_fast_index_wire(&self, out: &mut dyn Write) -> std::io::Result<()> {
        let RaftLogEntry::Index { collection_id, req } = &self.entry else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "not a fast Index record",
            ));
        };
        if self.version != WAL_FORMAT_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "unsupported fast Index version",
            ));
        }
        let string = |out: &mut dyn Write, value: &str| -> std::io::Result<()> {
            let len = u32::try_from(value.len()).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "fast WAL string exceeds u32",
                )
            })?;
            out.write_all(&len.to_le_bytes())?;
            out.write_all(value.as_bytes())
        };
        let versioned = req.items.iter().any(|item| item.version.is_some());
        out.write_all(WAL_FAST_MAGIC)?;
        out.write_all(&[
            self.version,
            if versioned {
                WAL_FAST_INDEX_VERSIONED
            } else {
                WAL_FAST_INDEX
            },
        ])?;
        string(out, collection_id)?;
        match &req.request_id {
            Some(value) => {
                out.write_all(&[1])?;
                string(out, value)?;
            }
            None => out.write_all(&[0])?,
        }
        let items = u32::try_from(req.items.len()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "fast WAL item count exceeds u32",
            )
        })?;
        out.write_all(&items.to_le_bytes())?;
        for item in &req.items {
            string(out, &item.external_id)?;
            string(out, &item.field)?;
            if versioned {
                match item.version {
                    Some(value) => {
                        out.write_all(&[1])?;
                        out.write_all(&value.to_le_bytes())?;
                    }
                    None => out.write_all(&[0])?,
                }
            }
            match &item.value {
                FieldValue::String(value) => {
                    out.write_all(&[WAL_VALUE_STRING])?;
                    string(out, value)?;
                }
                FieldValue::Number(value) => {
                    out.write_all(&[WAL_VALUE_NUMBER])?;
                    out.write_all(&value.to_le_bytes())?;
                }
                FieldValue::Vector(values) => {
                    out.write_all(&[WAL_VALUE_VECTOR])?;
                    let len = u32::try_from(values.len()).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "fast WAL vector count exceeds u32",
                        )
                    })?;
                    out.write_all(&len.to_le_bytes())?;
                    for value in values {
                        out.write_all(&value.to_le_bytes())?;
                    }
                }
                FieldValue::StringList(values) => {
                    out.write_all(&[WAL_VALUE_STRING_LIST])?;
                    let len = u32::try_from(values.len()).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "fast WAL set count exceeds u32",
                        )
                    })?;
                    out.write_all(&len.to_le_bytes())?;
                    for value in values {
                        string(out, value)?;
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub fn encode(&self) -> Result<Vec<u8>> {
        match &self.entry {
            RaftLogEntry::TruncateDocs { .. } => {
                return self.encode_fast_truncate_docs().ok_or_else(|| {
                    anyhow!(
                        "TruncateDocs must use WAL v{} fast control encoding",
                        WAL_CONTROL_FORMAT_VERSION
                    )
                });
            }
            RaftLogEntry::UnindexDocs { .. } => {
                return self.encode_fast_unindex_docs().ok_or_else(|| {
                    anyhow!(
                        "UnindexDocs must use a valid WAL v{} fast control encoding",
                        WAL_CONTROL_FORMAT_VERSION
                    )
                });
            }
            _ => {}
        }
        anyhow::ensure!(
            self.version != WAL_CONTROL_FORMAT_VERSION,
            "WAL v{} is reserved for fast control records",
            WAL_CONTROL_FORMAT_VERSION
        );
        if let Some(bytes) = self.encode_fast_index() {
            return Ok(bytes);
        }
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(self, &mut bytes)
            .map_err(|e| anyhow!("cbor encode WAL record: {e}"))?;
        Ok(bytes)
    }

    fn encode_fast_index(&self) -> Option<Vec<u8>> {
        if self.version != WAL_FORMAT_VERSION {
            return None;
        }
        let RaftLogEntry::Index { collection_id, req } = &self.entry else {
            return None;
        };
        // Choose the tag by CONTENT, not unconditionally: a record none of
        // whose items carries a `version` is expressible on the legacy wire,
        // and writing it there is what keeps a rollback to a pre-#3952 binary
        // readable (see `WAL_FAST_INDEX_VERSIONED`).
        let versioned = req.items.iter().any(|item| item.version.is_some());
        let mut bytes = Vec::with_capacity(estimate_fast_index_len(collection_id, req, versioned));
        bytes.extend_from_slice(WAL_FAST_MAGIC);
        bytes.push(self.version);
        bytes.push(if versioned {
            WAL_FAST_INDEX_VERSIONED
        } else {
            WAL_FAST_INDEX
        });
        put_str(&mut bytes, collection_id)?;
        match &req.request_id {
            Some(request_id) => {
                bytes.push(1);
                put_str(&mut bytes, request_id)?;
            }
            None => bytes.push(0),
        }
        put_u32(&mut bytes, req.items.len())?;
        for item in &req.items {
            put_str(&mut bytes, &item.external_id)?;
            put_str(&mut bytes, &item.field)?;
            // #3952: carry the external LWW version (#184) on the wire so
            // replay reconstructs the same `cell_versions` ceiling the live
            // apply path enforced. Present only under the versioned tag — the
            // legacy layout has no per-item version byte at all, and writing
            // one under tag 1 would desynchronize every reader, old and new.
            if versioned {
                match item.version {
                    Some(v) => {
                        bytes.push(1);
                        bytes.extend_from_slice(&v.to_le_bytes());
                    }
                    None => bytes.push(0),
                }
            }
            match &item.value {
                FieldValue::String(s) => {
                    bytes.push(WAL_VALUE_STRING);
                    put_str(&mut bytes, s)?;
                }
                FieldValue::Number(n) => {
                    bytes.push(WAL_VALUE_NUMBER);
                    bytes.extend_from_slice(&n.to_le_bytes());
                }
                FieldValue::Vector(v) => {
                    bytes.push(WAL_VALUE_VECTOR);
                    put_u32(&mut bytes, v.len())?;
                    for x in v {
                        bytes.extend_from_slice(&x.to_le_bytes());
                    }
                }
                FieldValue::StringList(values) => {
                    bytes.push(WAL_VALUE_STRING_LIST);
                    put_u32(&mut bytes, values.len())?;
                    for value in values {
                        put_str(&mut bytes, value)?;
                    }
                }
            }
        }
        Some(bytes)
    }

    fn encode_fast_truncate_docs(&self) -> Option<Vec<u8>> {
        let RaftLogEntry::TruncateDocs { collection_id } = &self.entry else {
            return None;
        };
        if self.version != WAL_CONTROL_FORMAT_VERSION {
            return None;
        }
        let mut bytes = Vec::with_capacity(WAL_FAST_MAGIC.len() + 2 + collection_id.len() + 4);
        bytes.extend_from_slice(WAL_FAST_MAGIC);
        bytes.push(WAL_CONTROL_FORMAT_VERSION);
        bytes.push(WAL_FAST_TRUNCATE_DOCS);
        put_str(&mut bytes, collection_id)?;
        Some(bytes)
    }

    fn encode_fast_unindex_docs(&self) -> Option<Vec<u8>> {
        let RaftLogEntry::UnindexDocs { collection_id, req } = &self.entry else {
            return None;
        };
        if self.version != WAL_CONTROL_FORMAT_VERSION
            || validate_batch_unindex_docs_request(req).is_err()
        {
            return None;
        }
        let ids_len: usize = req.external_ids.iter().map(|id| 4 + id.len()).sum();
        let mut bytes =
            Vec::with_capacity(WAL_FAST_MAGIC.len() + 2 + 4 + collection_id.len() + 4 + ids_len);
        bytes.extend_from_slice(WAL_FAST_MAGIC);
        bytes.push(WAL_CONTROL_FORMAT_VERSION);
        bytes.push(WAL_FAST_UNINDEX_DOCS);
        put_str(&mut bytes, collection_id)?;
        put_u32(&mut bytes, req.external_ids.len())?;
        for external_id in &req.external_ids {
            put_str(&mut bytes, external_id)?;
        }
        Some(bytes)
    }
}

/// `versioned` must be the same predicate `encode_fast_index` used to pick the
/// tag — under the legacy tag there is no per-item version byte to reserve, and
/// over-reserving one byte per item is a silent per-record allocation tax on
/// every write in a deployment that never sets `version`.
fn estimate_fast_index_len(collection_id: &str, req: &IndexRequest, versioned: bool) -> usize {
    let mut len = WAL_FAST_MAGIC.len() + 2 + 4 + collection_id.len() + 1 + 4;
    if let Some(request_id) = &req.request_id {
        len += 4 + request_id.len();
    }
    for item in &req.items {
        len += 4 + item.external_id.len() + 4 + item.field.len() + 1;
        if versioned {
            len += if item.version.is_some() { 9 } else { 1 };
        }
        match &item.value {
            FieldValue::String(s) => len += 4 + s.len(),
            FieldValue::Number(_) => len += 8,
            FieldValue::Vector(v) => len += 4 + v.len() * 4,
            FieldValue::StringList(values) => {
                len += 4;
                for value in values {
                    len += 4 + value.len();
                }
            }
        }
    }
    len
}

pub(super) fn put_u32(bytes: &mut Vec<u8>, n: usize) -> Option<()> {
    let n = u32::try_from(n).ok()?;
    bytes.extend_from_slice(&n.to_le_bytes());
    Some(())
}

pub(super) fn put_str(bytes: &mut Vec<u8>, s: &str) -> Option<()> {
    put_u32(bytes, s.len())?;
    bytes.extend_from_slice(s.as_bytes());
    Some(())
}
