//! Sort keys: the sort field's kind, a doc's sort values, their order, the
//! keyset cursor position they resume after, and the checks a sort request must
//! pass.

use std::cmp::Ordering as CmpOrdering;

use anyhow::Result;

use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::query::{QueryNode, SortOrder, SortSpec};
use crate::shared_kernel::types::search::SearchRequest;

/// #183: max keys in a multi-key `sort`. The generic plan and keyset cursor carry
/// a full `Vec<SortValue>` and compare every key in order, so this is a guard
/// against pathological requests, not a structural limit.
pub const MAX_SORT_KEYS: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SortValue {
    Number(u64),
    Keyword(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SortAfter {
    pub(super) values: Vec<SortValue>,
    pub(super) docid: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SortFieldKind {
    Number,
    Keyword,
}

pub(super) fn sort_field_kind(
    coll: &Collection,
    collection_id: &str,
    spec: &SortSpec,
) -> Result<SortFieldKind> {
    let Some(field) = coll.fields.get(&spec.field) else {
        return Err(StorageError::UnknownField {
            collection: collection_id.to_string(),
            field: spec.field.clone(),
        }
        .into());
    };
    if !field.field_type().capabilities().sort {
        return Err(StorageError::UnsupportedSort(format!(
            "field `{}` is not sortable; supported sort fields are number and keyword",
            spec.field
        ))
        .into());
    }
    match field {
        FieldIndex::Number(_) => Ok(SortFieldKind::Number),
        FieldIndex::Keyword(_) => Ok(SortFieldKind::Keyword),
        _ => unreachable!("FieldType capability mapping only permits number and keyword sort"),
    }
}

fn query_can_be_sort_predicate(node: &QueryNode) -> bool {
    match node {
        // #181: has_child IS sortable — a query containing it routes to the
        // materialized sort path, which resolves it via eval_query rather than a
        // per-doc walk. knn/rrf/hamming remain non-sortable (relevance/fuzzy).
        QueryNode::Knn(_) | QueryNode::Rrf(_) | QueryNode::Hamming(_) => false,
        QueryNode::And(cs) | QueryNode::Or(cs) => cs.iter().all(query_can_be_sort_predicate),
        QueryNode::Not(c) => query_can_be_sort_predicate(c),
        _ => true,
    }
}

pub(crate) fn validate_sort_request(
    coll: &Collection,
    collection_id: &str,
    req: &SearchRequest,
) -> Result<()> {
    let Some(sort) = req.sort.as_deref() else {
        return Ok(());
    };
    if sort.is_empty() {
        return Err(
            StorageError::UnsupportedSort("sort must contain at least one key".into()).into(),
        );
    }
    if sort.len() > MAX_SORT_KEYS {
        return Err(StorageError::UnsupportedSort(format!(
            "multi-key sort supports at most {MAX_SORT_KEYS} keys, got {}",
            sort.len()
        ))
        .into());
    }
    if req.collapse.is_some() {
        return Err(StorageError::UnsupportedSort(
            "sort cannot be combined with collapse/group-by results".into(),
        )
        .into());
    }
    if !query_can_be_sort_predicate(&req.query) {
        return Err(StorageError::UnsupportedSort(
            "sort cannot be combined with knn, rrf, or hamming queries".into(),
        )
        .into());
    }
    for spec in sort {
        sort_field_kind(coll, collection_id, spec)?;
    }
    Ok(())
}

pub(crate) fn sort_value_at(
    coll: &Collection,
    spec: &SortSpec,
    id: u32,
) -> Result<Option<SortValue>> {
    match coll.fields.get(&spec.field) {
        Some(FieldIndex::Number(n)) => Ok(n.number_bits_at(id).map(SortValue::Number)),
        Some(FieldIndex::Keyword(k)) => Ok(k.keyword_at(id).map(SortValue::Keyword)),
        Some(_) => Err(StorageError::UnsupportedSort(format!(
            "field `{}` is not sortable; supported sort fields are number and keyword",
            spec.field
        ))
        .into()),
        None => Err(StorageError::UnknownField {
            collection: "<>".into(),
            field: spec.field.clone(),
        }
        .into()),
    }
}

pub(crate) fn sort_values_for_doc(
    coll: &Collection,
    sort: &[SortSpec],
    id: u32,
) -> Result<Option<Vec<SortValue>>> {
    let mut values = Vec::with_capacity(sort.len());
    for spec in sort {
        let Some(value) = sort_value_at(coll, spec, id)? else {
            return Ok(None);
        };
        values.push(value);
    }
    Ok(Some(values))
}

pub(super) fn compare_sort_value(a: &SortValue, b: &SortValue) -> CmpOrdering {
    match (a, b) {
        (SortValue::Number(a), SortValue::Number(b)) => a.cmp(b),
        (SortValue::Keyword(a), SortValue::Keyword(b)) => a.cmp(b),
        _ => CmpOrdering::Equal,
    }
}

pub(super) fn compare_sort_tuples(
    a_values: &[SortValue],
    a_docid: u32,
    b_values: &[SortValue],
    b_docid: u32,
    sort: &[SortSpec],
) -> CmpOrdering {
    for ((a, b), spec) in a_values.iter().zip(b_values).zip(sort) {
        let ord = compare_sort_value(a, b);
        if ord != CmpOrdering::Equal {
            return match spec.order {
                SortOrder::Asc => ord,
                SortOrder::Desc => ord.reverse(),
            };
        }
    }
    a_docid.cmp(&b_docid)
}

pub(super) fn is_after_sort_cursor(
    values: &[SortValue],
    docid: u32,
    after: Option<&SortAfter>,
    sort: &[SortSpec],
) -> bool {
    let Some(after) = after else {
        return true;
    };
    if values.len() != after.values.len() || values.len() != sort.len() {
        return true;
    }
    compare_sort_tuples(values, docid, &after.values, after.docid, sort) == CmpOrdering::Greater
}

pub(crate) fn sort_score(value: &SortValue) -> f32 {
    match value {
        SortValue::Number(bits) => SortableF64::from_bits(*bits).to_f64() as f32,
        SortValue::Keyword(_) => 1.0,
    }
}

#[cfg(test)]
mod tests;
