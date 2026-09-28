//! Merging a reshard batch's snapshot delta into a base snapshot.

use std::collections::BTreeSet;

use anyhow::{bail, Result};

use crate::storage::{CollectionSnapshot, FieldIndexSnapshot, SnapshotV1};

/// Merge a reshard delta snapshot into an existing target snapshot. This is
/// the wire-level primitive an operator can use between batches: fetch target
/// snapshot, merge one moved-bucket batch, restore the merged snapshot, then
/// checkpoint the batch as complete.
pub fn merge_snapshot_delta(mut base: SnapshotV1, delta: SnapshotV1) -> Result<SnapshotV1> {
    if base.version != delta.version {
        bail!(
            "snapshot version mismatch: base={} delta={}",
            base.version,
            delta.version
        );
    }
    for (collection_id, delta_collection) in delta.collections {
        match base.collections.get_mut(&collection_id) {
            Some(base_collection) => merge_collection_delta(base_collection, delta_collection)?,
            None => {
                base.collections.insert(collection_id, delta_collection);
            }
        }
    }
    Ok(base)
}

fn merge_collection_delta(base: &mut CollectionSnapshot, delta: CollectionSnapshot) -> Result<()> {
    if base.schema != delta.schema {
        bail!("cannot merge reshard snapshots with different collection schemas");
    }
    base.version = base.version.max(delta.version);
    base.eid_fields.extend(delta.eid_fields);
    for (field, delta_index) in delta.fields {
        match base.fields.get_mut(&field) {
            Some(base_index) => merge_field_index_delta(base_index, delta_index)?,
            None => {
                base.fields.insert(field, delta_index);
            }
        }
    }
    Ok(())
}

fn merge_field_index_delta(base: &mut FieldIndexSnapshot, delta: FieldIndexSnapshot) -> Result<()> {
    match (base, delta) {
        (
            FieldIndexSnapshot::Text {
                tokens,
                forward,
                doc_count,
                total_doc_len,
                bytes,
                ..
            },
            FieldIndexSnapshot::Text {
                tokens: delta_tokens,
                forward: delta_forward,
                bytes: delta_bytes,
                ..
            },
        ) => {
            for (token, postings) in delta_tokens {
                tokens.entry(token).or_default().extend(postings);
            }
            forward.extend(delta_forward);
            *doc_count = forward.len() as u64;
            *total_doc_len = forward.values().map(|(_, len)| u64::from(*len)).sum();
            *bytes = bytes.saturating_add(delta_bytes);
        }
        // Keyword and Set carry only their forward column, so the merge is
        // `extend` and nothing else. The inverted maps used to be unioned here
        // as well, and that union could not be right: `forward` resolves an id
        // present in both shards last-writer-wins, while a union keeps BOTH
        // shards' postings, so a doc whose keyword differed between them left a
        // posting its own forward entry no longer backed. It restored into an
        // index answering a term query with a document that does not hold the
        // term — except that `from_snapshot` rebuilds from `forward` and threw
        // the union away, so the bug was latent rather than live.
        (
            FieldIndexSnapshot::Keyword { forward, bytes, .. },
            FieldIndexSnapshot::Keyword {
                forward: delta_forward,
                bytes: delta_bytes,
                ..
            },
        ) => {
            forward.extend(delta_forward);
            *bytes = bytes.saturating_add(delta_bytes);
        }
        (
            FieldIndexSnapshot::Number { forward, bytes },
            FieldIndexSnapshot::Number {
                forward: delta_forward,
                bytes: delta_bytes,
            },
        ) => {
            forward.extend(delta_forward);
            *bytes = bytes.saturating_add(delta_bytes);
        }
        (
            FieldIndexSnapshot::Set { forward, bytes, .. },
            FieldIndexSnapshot::Set {
                forward: delta_forward,
                bytes: delta_bytes,
                ..
            },
        ) => {
            forward.extend(delta_forward);
            *bytes = bytes.saturating_add(delta_bytes);
        }
        (
            FieldIndexSnapshot::Vector {
                spec,
                vectors,
                codebook,
                bytes,
            },
            FieldIndexSnapshot::Vector {
                spec: delta_spec,
                vectors: delta_vectors,
                codebook: delta_codebook,
                bytes: delta_bytes,
            },
        ) => {
            if *spec != delta_spec {
                bail!("cannot merge vector snapshots with different specs");
            }
            let delta_ids: BTreeSet<String> = delta_vectors
                .iter()
                .map(|(external_id, _)| external_id.clone())
                .collect();
            vectors.retain(|(external_id, _)| !delta_ids.contains(external_id));
            vectors.extend(delta_vectors);
            if codebook.is_none() {
                *codebook = delta_codebook;
            }
            *bytes = bytes.saturating_add(delta_bytes);
        }
        (
            FieldIndexSnapshot::Hash { forward, bytes },
            FieldIndexSnapshot::Hash {
                forward: delta_forward,
                bytes: delta_bytes,
            },
        ) => {
            forward.extend(delta_forward);
            *bytes = bytes.saturating_add(delta_bytes);
        }
        _ => bail!("cannot merge snapshots with different field index types"),
    }
    Ok(())
}
