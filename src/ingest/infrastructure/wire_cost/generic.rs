//! Cost-only serde grammar for the private, moving WAL decoder. Container
//! storage is counted here; token storage comes from the borrowing preflight.

#![allow(dead_code)]

pub(crate) mod value;

use super::bounded_serde::Quiet;
use super::{add, allocation, mul, token_stats};
use crate::ingest::infrastructure::wal::bounded_generic::{
    FIELD_VALUE_WIDTH, INDEX_ITEM_WIDTH, REPLACE_ITEM_WIDTH,
};
use crate::ingest::infrastructure::wire_cost::generic::value::{Fields, List, Text, Value};
use crate::shared_kernel::types::schema::FieldSpec;
use anyhow::{ensure, Result};
use serde::de;
use serde::Deserialize;
use std::mem::size_of;

trait HeapCost {
    fn heap(&self) -> Result<usize>;
}
impl<T: HeapCost> HeapCost for Option<T> {
    fn heap(&self) -> Result<usize> {
        self.as_ref().map_or(Ok(0), HeapCost::heap)
    }
}
impl HeapCost for FieldSpec {
    fn heap(&self) -> Result<usize> {
        Ok(0)
    }
}
fn grow_array(count: usize, width: usize) -> Result<usize> {
    if count == 0 {
        Ok(0)
    } else {
        allocation(mul(mul(count.max(4), width)?, 3)?)
    }
}
fn as_de<E: de::Error>(error: anyhow::Error) -> E {
    E::custom(error.to_string())
}

#[derive(Deserialize)]
struct Create {
    fields: Fields<FieldSpec, { size_of::<FieldSpec>() }>,
}
impl HeapCost for Create {
    fn heap(&self) -> Result<usize> {
        self.fields.heap()
    }
}
#[derive(Deserialize)]
struct Item {
    external_id: Text,
    field: Text,
    value: Value,
    #[serde(default)]
    version: Option<u64>,
}
impl HeapCost for Item {
    fn heap(&self) -> Result<usize> {
        add(
            add(self.external_id.heap()?, self.field.heap()?)?,
            self.value.heap()?,
        )
    }
}
#[derive(Deserialize)]
struct Index {
    items: List<Item, INDEX_ITEM_WIDTH>,
    #[serde(default)]
    request_id: Option<Text>,
}
impl HeapCost for Index {
    fn heap(&self) -> Result<usize> {
        add(self.items.heap()?, self.request_id.heap()?)
    }
}
#[derive(Deserialize)]
struct Doc {
    external_id: Text,
    #[serde(default)]
    version: Option<u64>,
    fields: Fields<Value, FIELD_VALUE_WIDTH>,
}
impl HeapCost for Doc {
    fn heap(&self) -> Result<usize> {
        add(self.external_id.heap()?, self.fields.heap()?)
    }
}
#[derive(Deserialize)]
struct Replace {
    docs: List<Doc, REPLACE_ITEM_WIDTH>,
}
impl HeapCost for Replace {
    fn heap(&self) -> Result<usize> {
        self.docs.heap()
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Unindex {
    external_ids: List<Text, { size_of::<String>() }>,
}
#[derive(Deserialize)]
enum Entry {
    CreateCollection {
        collection_id: Text,
        req: Create,
    },
    Index {
        collection_id: Text,
        req: Index,
    },
    ReplaceDocs {
        collection_id: Text,
        req: Replace,
    },
    TruncateDocs {
        collection_id: Text,
    },
    UnindexDocs {
        collection_id: Text,
        req: Unindex,
    },
    Delete {
        collection_id: Text,
        external_id: Text,
        field: Option<Text>,
    },
    DropCollection {
        collection_id: Text,
        force: bool,
    },
    AddField {
        collection_id: Text,
        field_name: Text,
        spec: FieldSpec,
    },
    DropField {
        collection_id: Text,
        field_name: Text,
    },
}
impl HeapCost for Entry {
    fn heap(&self) -> Result<usize> {
        let nested = match self {
            Self::CreateCollection { collection_id, req } => {
                add(collection_id.heap()?, req.heap()?)?
            }
            Self::Index { collection_id, req } => add(collection_id.heap()?, req.heap()?)?,
            Self::ReplaceDocs { collection_id, req } => add(collection_id.heap()?, req.heap()?)?,
            Self::Delete {
                collection_id,
                external_id,
                field,
            } => add(
                add(collection_id.heap()?, external_id.heap()?)?,
                field.heap()?,
            )?,
            Self::DropCollection { collection_id, .. } => collection_id.heap()?,
            Self::AddField {
                collection_id,
                field_name,
                ..
            }
            | Self::DropField {
                collection_id,
                field_name,
            } => add(collection_id.heap()?, field_name.heap()?)?,
            Self::TruncateDocs { .. } | Self::UnindexDocs { .. } => {
                anyhow::bail!("control commands must use WAL v2 fast control encoding")
            }
        };
        add(
            size_of::<crate::shared_kernel::log_entry::RaftLogEntry>(),
            nested,
        )
    }
}
#[derive(Deserialize)]
struct Record {
    version: u8,
    entry: Entry,
}

pub(super) fn decoded_peak_bound(bytes: &[u8]) -> Result<usize> {
    let Quiet(record): Quiet<Record> = match ciborium::de::from_reader(bytes) {
        Ok(record) => record,
        Err(_) => serde_json::from_slice(bytes)?,
    };
    ensure!(
        record.version == 1,
        "unsupported generic WAL record version {}",
        record.version
    );
    // Strings move from wire to public records. Three aggregate token lengths
    // cover final storage and geometric token growth, including ignored fields
    // and JSON's retained escape scratch. Per-token slack covers small buffers
    // and allocator headers. This is separate from the container bound above.
    let stats = token_stats::scan(bytes)?;
    let tokens = add(stats.text_bytes, stats.byte_string_bytes)?;
    let token_heap = add(mul(tokens, 3)?, mul(stats.token_count, 40)?)?;
    add(add(record.entry.heap()?, token_heap)?, 8192)
}
