//! The part of a snapshot that falls in a set of virtual buckets.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Result};

use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;
use crate::storage::{CollectionSnapshot, FieldIndexSnapshot, SnapshotV1};

/// Restrict a snapshot to only the external_ids routed to one of `buckets`,
/// computed with the exact same `route_document` hash
/// `snapshot_reshard_batches` uses (`route_hash(collection_id, external_id) %
/// virtual_bucket_count`). Backs the bucket-scoped export admin verb (`POST
/// /admin/backup:scoped`, #1380 R2) so an export and the batches later
/// computed against the same map can never disagree about bucket
/// membership. `physical_shard_count` is irrelevant to bucket selection, so
/// callers only need to agree on `virtual_bucket_count`.
///
/// #2496: every collection's *schema* is retained even when it contributes
/// no in-scope external_ids — a collection with zero documents anywhere (or
/// whose documents all route elsewhere) never appears in the doc-driven
/// subset above, but the reshard pipeline still needs its schema to reach a
/// shard gaining buckets, or a later write to that collection 404s on the
/// new shard with `CollectionNotFound` even though the collection genuinely
/// exists. `collection_subset(collection, &BTreeSet::new())` reproduces
/// exactly the "freshly created, zero documents" shape (schema + per-field
/// index structure, no postings) that a real empty collection's own
/// snapshot already has.
pub fn snapshot_bucket_subset(
    snapshot: &SnapshotV1,
    virtual_bucket_count: u32,
    buckets: &BTreeSet<u32>,
) -> Result<SnapshotV1> {
    let map = VirtualBucketShardMap::balanced(0, virtual_bucket_count, 1)?;
    let mut external_ids: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (collection_id, collection) in &snapshot.collections {
        for external_id in collection.eid_fields.keys() {
            let bucket = map.route_document(collection_id, None, external_id).bucket;
            if buckets.contains(&bucket) {
                external_ids
                    .entry(collection_id.clone())
                    .or_default()
                    .insert(external_id.clone());
            }
        }
    }
    let mut scoped = snapshot_subset(snapshot, &external_ids)?;
    for (collection_id, collection) in &snapshot.collections {
        scoped
            .collections
            .entry(collection_id.clone())
            .or_insert_with(|| collection_subset(collection, &BTreeSet::new()));
    }
    Ok(scoped)
}

pub(super) fn ids_map_from_pairs(
    pairs: impl IntoIterator<Item = (String, String)>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (collection_id, external_id) in pairs {
        out.entry(collection_id).or_default().insert(external_id);
    }
    out
}

pub(super) fn snapshot_subset(
    snapshot: &SnapshotV1,
    external_ids: &BTreeMap<String, BTreeSet<String>>,
) -> Result<SnapshotV1> {
    let mut collections = BTreeMap::new();
    for (collection_id, wanted) in external_ids {
        let Some(collection) = snapshot.collections.get(collection_id) else {
            bail!("snapshot missing collection `{collection_id}`");
        };
        let subset = collection_subset(collection, wanted);
        if !subset.eid_fields.is_empty() {
            collections.insert(collection_id.clone(), subset);
        }
    }
    Ok(SnapshotV1 {
        version: snapshot.version,
        collections,
    })
}

pub(super) fn collection_subset(
    collection: &CollectionSnapshot,
    wanted: &BTreeSet<String>,
) -> CollectionSnapshot {
    CollectionSnapshot {
        schema: collection.schema.clone(),
        version: collection.version,
        eid_fields: collection
            .eid_fields
            .iter()
            .filter(|(external_id, _)| wanted.contains(*external_id))
            .map(|(external_id, fields)| (external_id.clone(), fields.clone()))
            .collect(),
        fields: collection
            .fields
            .iter()
            .map(|(field, index)| (field.clone(), field_index_subset(index, wanted)))
            .collect(),
    }
}

fn field_index_subset(index: &FieldIndexSnapshot, wanted: &BTreeSet<String>) -> FieldIndexSnapshot {
    match index {
        FieldIndexSnapshot::Text {
            analyzer,
            tokens,
            forward,
            bytes,
            ..
        } => {
            let forward: BTreeMap<String, (BTreeSet<String>, u32)> = forward
                .iter()
                .filter(|(external_id, _)| wanted.contains(*external_id))
                .map(|(external_id, value)| (external_id.clone(), value.clone()))
                .collect();
            let doc_count = forward.len() as u64;
            let total_doc_len = forward.values().map(|(_, len)| u64::from(*len)).sum();
            let tokens = tokens
                .iter()
                .filter_map(|(token, postings)| {
                    let postings: BTreeMap<String, u32> = postings
                        .iter()
                        .filter(|(external_id, _)| wanted.contains(*external_id))
                        .map(|(external_id, tf)| (external_id.clone(), *tf))
                        .collect();
                    (!postings.is_empty()).then(|| (token.clone(), postings))
                })
                .collect();
            FieldIndexSnapshot::Text {
                analyzer: *analyzer,
                tokens,
                forward: forward.into_iter().collect(),
                doc_count,
                total_doc_len,
                bytes: *bytes,
            }
        }
        FieldIndexSnapshot::Keyword { forward, bytes, .. } => FieldIndexSnapshot::Keyword {
            terms: crate::storage::LegacyInvertedIndex,
            forward: forward
                .iter()
                .filter(|(external_id, _)| wanted.contains(*external_id))
                .map(|(external_id, value)| (external_id.clone(), value.clone()))
                .collect(),
            bytes: *bytes,
        },
        FieldIndexSnapshot::Number { forward, bytes } => FieldIndexSnapshot::Number {
            forward: forward
                .iter()
                .filter(|(external_id, _)| wanted.contains(*external_id))
                .map(|(external_id, value)| (external_id.clone(), *value))
                .collect(),
            bytes: *bytes,
        },
        FieldIndexSnapshot::Set { forward, bytes, .. } => FieldIndexSnapshot::Set {
            elements: crate::storage::LegacyInvertedIndex,
            forward: forward
                .iter()
                .filter(|(external_id, _)| wanted.contains(*external_id))
                .map(|(external_id, value)| (external_id.clone(), value.clone()))
                .collect(),
            bytes: *bytes,
        },
        FieldIndexSnapshot::Vector {
            spec,
            vectors,
            codebook,
            bytes,
        } => FieldIndexSnapshot::Vector {
            spec: *spec,
            vectors: vectors
                .iter()
                .filter(|(external_id, _)| wanted.contains(external_id))
                .cloned()
                .collect(),
            codebook: *codebook,
            bytes: *bytes,
        },
        FieldIndexSnapshot::Hash { forward, bytes } => FieldIndexSnapshot::Hash {
            forward: forward
                .iter()
                .filter(|(external_id, _)| wanted.contains(*external_id))
                .map(|(external_id, value)| (external_id.clone(), *value))
                .collect(),
            bytes: *bytes,
        },
    }
}
