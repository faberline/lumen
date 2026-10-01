//! What one field's value adds to pending-change memory, by field type.

use crate::ingest::domain::change_memory_cost::{
    add, mul, string_bytes, string_collection_bytes, CostError, FieldCost, BTREE_MEMBER_BYTES,
    DENSE_DOC_SLOT_BYTES, MAP_ENTRY_BYTES, POSTING_ROW_BYTES, ROARING_MEMBER_BYTES,
    SPARSE_TEXT_DOCUMENT_BUCKET_BYTES, TEXT_BTREE_FIRST_NODE_SLACK_BYTES, TEXT_BTREE_TERM_BYTES,
};

pub(super) fn field_cost(
    external_id_bytes: usize,
    field: &FieldCost,
) -> Result<(usize, usize, usize), CostError> {
    let active = match field {
        FieldCost::Keyword { value_bytes } => add(
            add(string_bytes(*value_bytes)?, MAP_ENTRY_BYTES)?,
            add(ROARING_MEMBER_BYTES, DENSE_DOC_SLOT_BYTES)?,
        )?,
        FieldCost::Number => add(
            MAP_ENTRY_BYTES,
            add(ROARING_MEMBER_BYTES, DENSE_DOC_SLOT_BYTES)?,
        )?,
        FieldCost::Set {
            members,
            member_bytes,
        } => {
            // `elements` owns one string + bitmap membership per member and
            // `forward` owns another string in its per-document BTreeSet.
            // `member_bytes` already sums all member lengths. Charge it once
            // for each owned String copy, and charge only metadata per member.
            let strings = mul(2, string_collection_bytes(*member_bytes, *members)?)?;
            let entries = mul(
                *members,
                add(
                    add(MAP_ENTRY_BYTES, ROARING_MEMBER_BYTES)?,
                    BTREE_MEMBER_BYTES,
                )?,
            )?;
            add(strings, entries)?
        }
        FieldCost::Hash => add(MAP_ENTRY_BYTES, add(ROARING_MEMBER_BYTES, 8)?)?,
        FieldCost::Text {
            distinct_terms,
            total_term_bytes,
        } => {
            // `tokens` is an ordered BTreeMap and `distinct` stores a second
            // token copy for delete/reseal. Both string payload copies remain
            // charged; the sparse changed-document bucket is independent of a
            // high stable ID and replaces the former dense Vec slot.
            let strings = mul(
                2,
                string_collection_bytes(*total_term_bytes, *distinct_terms)?,
            )?;
            let ordered_dictionary = if *distinct_terms == 0 {
                0
            } else {
                add(
                    TEXT_BTREE_FIRST_NODE_SLACK_BYTES,
                    mul(*distinct_terms, TEXT_BTREE_TERM_BYTES)?,
                )?
            };
            let term = add(strings, ordered_dictionary)?;
            add(
                term,
                add(
                    mul(*distinct_terms, POSTING_ROW_BYTES)?,
                    SPARSE_TEXT_DOCUMENT_BUCKET_BYTES,
                )?,
            )?
        }
        FieldCost::Vector {
            dim,
            backend: _,
            quantized_sq,
        } => {
            let stored = mul(*dim, if *quantized_sq { 1 } else { 4 })?;
            // The 256 MiB change budget owns pending VectorStore payloads and
            // checkpoint rows. HNSW graph and query-side flat materialization
            // belong to separate RSS policy and are intentionally excluded.
            add(
                stored,
                add(string_bytes(external_id_bytes)?, MAP_ENTRY_BYTES)?,
            )?
        }
    };
    let frozen = match field {
        FieldCost::Vector { dim, .. } => {
            add(add(mul(*dim, 4)?, string_bytes(external_id_bytes)?)?, 48)?
        }
        _ => active,
    };
    let prepublish = match field {
        FieldCost::Vector { .. } => 16, // `Vec<Option<&[f32]>>` capture row
        _ => 0,
    };
    Ok((active, frozen, prepublish))
}
