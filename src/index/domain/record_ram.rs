//! Allocation-free upper bound for one decoded owned RaftLogEntry.
//!
//! It counts allocations reachable from this one record. It excludes a separate
//! AOF buffer, transport copies, normalized index state, and vector graphs.

use std::collections::BTreeMap;
use std::mem::size_of;

use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::{
    document::{
        BatchUnindexDocsRequest, FieldValue, IndexItem, IndexRequest, ReplaceDocItem,
        ReplaceDocsRequest,
    },
    schema::{CreateCollectionRequest, FieldSpec},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordRamError {
    Overflow,
}

// Rust 1.96 alloc::collections::btree uses B=6 and CAPACITY=11. A one-entry
// map owns a full leaf node. We charge a full worst-case node per entry:
// 11 key slots, 11 value slots, 12 edge pointers, plus a conservative two-word
// allocator header. This intentionally overcounts dense maps but bounds sparse
// nodes, partial nodes, bookkeeping, alignment, and internal-node edges.
const BTREE_CAPACITY: usize = 11;
const BTREE_EDGES: usize = BTREE_CAPACITY + 1;
const ALLOCATION_HEADER: usize = 2 * size_of::<usize>();

fn add(a: usize, b: usize) -> Result<usize, RecordRamError> {
    a.checked_add(b).ok_or(RecordRamError::Overflow)
}
fn mul(a: usize, b: usize) -> Result<usize, RecordRamError> {
    a.checked_mul(b).ok_or(RecordRamError::Overflow)
}
fn allocation(payload: usize) -> Result<usize, RecordRamError> {
    if payload == 0 {
        Ok(0)
    } else {
        add(payload, ALLOCATION_HEADER)
    }
}
fn string(value: &String) -> Result<usize, RecordRamError> {
    allocation(value.capacity())
}
fn vec<T>(value: &Vec<T>) -> Result<usize, RecordRamError> {
    allocation(mul(value.capacity(), size_of::<T>())?)
}

pub fn estimate_record_ram(entry: &RaftLogEntry) -> Result<usize, RecordRamError> {
    let nested = match entry {
        RaftLogEntry::CreateCollection { collection_id, req } => {
            add(string(collection_id)?, create(req)?)?
        }
        RaftLogEntry::Index { collection_id, req } => add(string(collection_id)?, index(req)?)?,
        RaftLogEntry::ReplaceDocs { collection_id, req } => {
            add(string(collection_id)?, replace(req)?)?
        }
        RaftLogEntry::TruncateDocs { collection_id }
        | RaftLogEntry::DropCollection { collection_id, .. } => string(collection_id)?,
        RaftLogEntry::UnindexDocs { collection_id, req } => {
            add(string(collection_id)?, unindex(req)?)?
        }
        RaftLogEntry::Delete {
            collection_id,
            external_id,
            field,
        } => {
            let mut total = add(string(collection_id)?, string(external_id)?)?;
            if let Some(field) = field {
                total = add(total, string(field)?)?;
            }
            total
        }
        RaftLogEntry::AddField {
            collection_id,
            field_name,
            spec,
        } => add(
            add(string(collection_id)?, string(field_name)?)?,
            spec_ram(spec)?,
        )?,
        RaftLogEntry::DropField {
            collection_id,
            field_name,
        } => add(string(collection_id)?, string(field_name)?)?,
    };
    add(size_of::<RaftLogEntry>(), nested)
}

/// Peak for the private CBOR decoder, including its untagged value buffering.
pub fn estimate_record_decode_peak(entry: &RaftLogEntry) -> Result<usize, RecordRamError> {
    let mut total = mul(estimate_record_ram(entry)?, 3)?;
    let content = |value: &FieldValue| -> Result<usize, RecordRamError> {
        let count = match value {
            FieldValue::Vector(values) => values.len(),
            FieldValue::StringList(values) => values.len(),
            _ => return Ok(0),
        };
        if count == 0 {
            return Ok(0);
        }
        // serde1.0.228 first builds Vec<Content>, then tries the owned
        // untagged alternatives. Eight words bound Content's three-word
        // payload, discriminant and alignment. Its cautious size hint can
        // force geometric Vec growth even for a definite CBOR array. Include
        // both allocations during reallocation, independent of final values.
        allocation(mul(mul(count.max(4), 8 * size_of::<usize>())?, 3)?)
    };
    match entry {
        RaftLogEntry::Index { req, .. } => {
            for item in &req.items {
                total = add(total, content(&item.value)?)?;
            }
        }
        RaftLogEntry::ReplaceDocs { req, .. } => {
            for doc in &req.docs {
                for value in doc.fields.values() {
                    total = add(total, content(value)?)?;
                }
            }
        }
        _ => (),
    }
    Ok(total)
}

fn create(req: &CreateCollectionRequest) -> Result<usize, RecordRamError> {
    map(&req.fields, |key, spec| add(string(key)?, spec_ram(spec)?))
}
fn index(req: &IndexRequest) -> Result<usize, RecordRamError> {
    let mut total = vec(&req.items)?;
    if let Some(id) = &req.request_id {
        total = add(total, string(id)?)?;
    }
    for item in &req.items {
        total = add(total, index_item(item)?)?;
    }
    Ok(total)
}
fn index_item(item: &IndexItem) -> Result<usize, RecordRamError> {
    add(
        add(string(&item.external_id)?, string(&item.field)?)?,
        value(&item.value)?,
    )
}
fn replace(req: &ReplaceDocsRequest) -> Result<usize, RecordRamError> {
    let mut total = vec(&req.docs)?;
    for doc in &req.docs {
        total = add(total, replace_doc(doc)?)?;
    }
    Ok(total)
}
fn replace_doc(doc: &ReplaceDocItem) -> Result<usize, RecordRamError> {
    add(
        string(&doc.external_id)?,
        map(&doc.fields, |key, field_value| {
            add(string(key)?, value(field_value)?)
        })?,
    )
}
fn unindex(req: &BatchUnindexDocsRequest) -> Result<usize, RecordRamError> {
    let mut total = vec(&req.external_ids)?;
    for id in &req.external_ids {
        total = add(total, string(id)?)?;
    }
    Ok(total)
}
fn spec_ram(_spec: &FieldSpec) -> Result<usize, RecordRamError> {
    Ok(0)
}
fn value(field_value: &FieldValue) -> Result<usize, RecordRamError> {
    match field_value {
        FieldValue::String(value) => string(value),
        FieldValue::Number(_) => Ok(0),
        FieldValue::Vector(values) => vec(values),
        FieldValue::StringList(values) => {
            let mut total = vec(values)?;
            for value in values {
                total = add(total, string(value)?)?;
            }
            Ok(total)
        }
    }
}
fn full_node_bytes<K, V>() -> Result<usize, RecordRamError> {
    allocation(add(
        mul(BTREE_CAPACITY, add(size_of::<K>(), size_of::<V>())?)?,
        mul(BTREE_EDGES, size_of::<usize>())?,
    )?)
}
fn map<K, V>(
    value: &BTreeMap<K, V>,
    mut entry: impl FnMut(&K, &V) -> Result<usize, RecordRamError>,
) -> Result<usize, RecordRamError> {
    let mut total = mul(value.len(), full_node_bytes::<K, V>()?)?;
    for (key, value) in value {
        total = add(total, entry(key, value)?)?;
    }
    Ok(total)
}

#[cfg(test)]
mod tests;
