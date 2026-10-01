//! Bounded decoding of the generic wire representation.

// This module deliberately owns the generic wire representation.  The public
// RaftLogEntry and FieldValue types remain unchanged.  In particular, the
// FieldValue visitor receives owned strings from serde and moves them into the
// final record instead of asking untagged deserialization to retain Content
// and then copy it into a second String/Vec.

pub(crate) mod field_value;

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use serde::Deserialize;

use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::bounded_generic::field_value::{GrowVec, WireFieldValue};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::{
    document::{FieldValue, IndexItem, IndexRequest, ReplaceDocItem, ReplaceDocsRequest},
    schema::{CreateCollectionRequest, FieldSpec},
};

// Keep allocation pricing correct if wire or public layouts change.
pub(crate) const INDEX_ITEM_WIDTH: usize =
    if std::mem::size_of::<WireIndexItem>() > std::mem::size_of::<IndexItem>() {
        std::mem::size_of::<WireIndexItem>()
    } else {
        std::mem::size_of::<IndexItem>()
    };
pub(crate) const REPLACE_ITEM_WIDTH: usize =
    if std::mem::size_of::<WireReplaceDocItem>() > std::mem::size_of::<ReplaceDocItem>() {
        std::mem::size_of::<WireReplaceDocItem>()
    } else {
        std::mem::size_of::<ReplaceDocItem>()
    };
pub(crate) const FIELD_VALUE_WIDTH: usize =
    if std::mem::size_of::<WireFieldValue>() > std::mem::size_of::<FieldValue>() {
        std::mem::size_of::<WireFieldValue>()
    } else {
        std::mem::size_of::<FieldValue>()
    };

pub(super) fn decode(bytes: &[u8]) -> Result<WalRecord> {
    let wire: WireRecord = match ciborium::de::from_reader(bytes) {
        Ok(wire) => wire,
        Err(cbor_err) => serde_json::from_slice(bytes).map_err(|json_err| {
            anyhow!("decode WAL record as cbor ({cbor_err}) or legacy json ({json_err})")
        })?,
    };
    Ok(wire.into_record())
}

#[derive(Deserialize)]
struct WireRecord {
    version: u8,
    entry: WireEntry,
}

impl WireRecord {
    fn into_record(self) -> WalRecord {
        WalRecord {
            version: self.version,
            entry: self.entry.into_entry(),
        }
    }
}

// Keep serde's externally tagged enum layout.  This is the layout emitted by
// the old derived RaftLogEntry decoder for both CBOR and the legacy JSON form.
#[derive(Deserialize)]
enum WireEntry {
    CreateCollection {
        collection_id: String,
        req: WireCreateCollectionRequest,
    },
    Index {
        collection_id: String,
        req: WireIndexRequest,
    },
    ReplaceDocs {
        collection_id: String,
        req: WireReplaceDocsRequest,
    },
    TruncateDocs {
        collection_id: String,
    },
    UnindexDocs {
        collection_id: String,
        req: WireBatchUnindexDocsRequest,
    },
    Delete {
        collection_id: String,
        external_id: String,
        field: Option<String>,
    },
    DropCollection {
        collection_id: String,
        force: bool,
    },
    AddField {
        collection_id: String,
        field_name: String,
        spec: FieldSpec,
    },
    DropField {
        collection_id: String,
        field_name: String,
    },
}

impl WireEntry {
    fn into_entry(self) -> RaftLogEntry {
        match self {
            Self::CreateCollection { collection_id, req } => RaftLogEntry::CreateCollection {
                collection_id,
                req: req.into_request(),
            },
            Self::Index { collection_id, req } => RaftLogEntry::Index {
                collection_id,
                req: req.into_request(),
            },
            Self::ReplaceDocs { collection_id, req } => RaftLogEntry::ReplaceDocs {
                collection_id,
                req: req.into_request(),
            },
            Self::TruncateDocs { collection_id } => RaftLogEntry::TruncateDocs { collection_id },
            Self::UnindexDocs { collection_id, req } => RaftLogEntry::UnindexDocs {
                collection_id,
                req: req.into_request(),
            },
            Self::Delete {
                collection_id,
                external_id,
                field,
            } => RaftLogEntry::Delete {
                collection_id,
                external_id,
                field,
            },
            Self::DropCollection {
                collection_id,
                force,
            } => RaftLogEntry::DropCollection {
                collection_id,
                force,
            },
            Self::AddField {
                collection_id,
                field_name,
                spec,
            } => RaftLogEntry::AddField {
                collection_id,
                field_name,
                spec,
            },
            Self::DropField {
                collection_id,
                field_name,
            } => RaftLogEntry::DropField {
                collection_id,
                field_name,
            },
        }
    }
}

#[derive(Deserialize)]
struct WireCreateCollectionRequest {
    fields: BTreeMap<String, FieldSpec>,
}

impl WireCreateCollectionRequest {
    fn into_request(self) -> CreateCollectionRequest {
        CreateCollectionRequest {
            fields: self.fields,
        }
    }
}

#[derive(Deserialize)]
struct WireIndexRequest {
    items: GrowVec<WireIndexItem>,
    #[serde(default)]
    request_id: Option<String>,
}

impl WireIndexRequest {
    fn into_request(self) -> IndexRequest {
        IndexRequest {
            items: self
                .items
                .0
                .into_iter()
                .map(WireIndexItem::into_item)
                .collect(),
            request_id: self.request_id,
        }
    }
}

#[derive(Deserialize)]
struct WireIndexItem {
    external_id: String,
    field: String,
    value: WireFieldValue,
    #[serde(default)]
    version: Option<u64>,
}

impl WireIndexItem {
    fn into_item(self) -> IndexItem {
        IndexItem {
            external_id: self.external_id,
            field: self.field,
            value: self.value.0,
            version: self.version,
        }
    }
}

#[derive(Deserialize)]
struct WireReplaceDocsRequest {
    docs: GrowVec<WireReplaceDocItem>,
}

impl WireReplaceDocsRequest {
    fn into_request(self) -> ReplaceDocsRequest {
        ReplaceDocsRequest {
            docs: self
                .docs
                .0
                .into_iter()
                .map(WireReplaceDocItem::into_item)
                .collect(),
        }
    }
}

#[derive(Deserialize)]
struct WireReplaceDocItem {
    external_id: String,
    #[serde(default)]
    version: Option<u64>,
    fields: BTreeMap<String, WireFieldValue>,
}

impl WireReplaceDocItem {
    fn into_item(self) -> ReplaceDocItem {
        ReplaceDocItem {
            external_id: self.external_id,
            version: self.version,
            fields: self
                .fields
                .into_iter()
                .map(|(key, value)| (key, value.0))
                .collect(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireBatchUnindexDocsRequest {
    external_ids: GrowVec<String>,
}

impl WireBatchUnindexDocsRequest {
    fn into_request(self) -> crate::shared_kernel::types::document::BatchUnindexDocsRequest {
        crate::shared_kernel::types::document::BatchUnindexDocsRequest {
            external_ids: self.external_ids.0,
        }
    }
}

#[cfg(test)]
mod tests;
