//! The exact leaves: term, terms, prefix and ids to their doc sets, and exists
//! and duplicated over the union of a field's postings.

use std::collections::BTreeMap;

use anyhow::{bail, Result};
use roaring::RoaringBitmap;

use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::{IdsQuery, PrefixQuery, TermQuery, TermsQuery};

/// Authoritative field-presence bitmap. `eid_fields` is updated on every
/// write/delete and rebuilt from segment presence on restore, so this answers
/// `Exists` without walking a high-cardinality term/value dictionary.
pub(super) fn field_presence_bitmap(coll: &Collection, field: &str) -> RoaringBitmap {
    coll.eid_fields
        .iter()
        .filter_map(|(id, coverage)| coverage.contains(field).then_some(*id))
        .collect()
}

/// Shared engine for `Exists` and `Duplicated`: the union of doc-ids that have a
/// value in `field` whose posting size ≥ `min_count`.
///   - `min_count = 1` → `Exists` (doc has any value in the field).
///   - `min_count = N` → `Duplicated` (the value is shared by ≥ N docs).
/// Segment ON: drive from the segment-aware `live_*` accessors (segment
/// dict/column minus tombstones + live tail), identical to `duplicates`.
/// Segment OFF: borrow the in-RAM map directly (the `live_*` accessors clone the
/// whole map in that case), and for `min_count ≥ 2` visit only the `dup_values`
/// side-index candidates instead of every distinct value — the high-cardinality
/// (email/phone) duplicated path stays O(|colliding values|). text / vector /
/// hash are not supported here (declare a keyword companion field for a text
/// "is empty" filter; use knn / hamming for vector / hash).
pub(super) fn eval_field_doc_union(
    coll: &Collection,
    field: &str,
    min_count: u64,
) -> Result<RoaringBitmap> {
    let fi = coll
        .fields
        .get(field)
        .ok_or_else(|| StorageError::UnknownField {
            collection: "<>".into(),
            field: field.to_string(),
        })?;
    // `Exists` is field coverage, not an inverted-dictionary aggregation. This
    // is both the authoritative semantic source (including empty values where
    // supported) and bounds high-cardinality fields to one census scan rather
    // than decoding every sealed term/value posting.
    if min_count == 1
        && matches!(
            fi,
            FieldIndex::Keyword(_) | FieldIndex::Number(_) | FieldIndex::Set(_)
        )
    {
        return Ok(field_presence_bitmap(coll, field));
    }
    // Union postings of size >= min_count: owned sets (segment path) or borrowed
    // in-RAM sets (tail-only path) without cloning the map.
    fn union_owned<K>(map: BTreeMap<K, RoaringBitmap>, min_count: u64) -> RoaringBitmap {
        let mut acc = RoaringBitmap::new();
        for set in map.values() {
            if set.len() >= min_count {
                acc |= set;
            }
        }
        acc
    }
    let mut acc = RoaringBitmap::new();
    match fi {
        FieldIndex::Keyword(k) => {
            if k.segment.is_some() {
                acc = union_owned(k.live_terms(), min_count);
            } else if min_count >= 2 {
                for set in k.dup_values.iter().filter_map(|v| k.terms.get(v)) {
                    if set.len() >= min_count {
                        acc |= set;
                    }
                }
            } else {
                for set in k.terms.values() {
                    acc |= set;
                }
            }
        }
        FieldIndex::Number(n) => {
            if n.segment.is_some() {
                acc = union_owned(n.live_values(), min_count);
            } else if min_count >= 2 {
                for set in n.dup_values.iter().filter_map(|v| n.values.get(v)) {
                    if set.len() >= min_count {
                        acc |= set;
                    }
                }
            } else {
                for set in n.values.values() {
                    acc |= set;
                }
            }
        }
        FieldIndex::Set(s) => {
            if s.segment.is_some() {
                acc = union_owned(s.live_elements(), min_count);
            } else if min_count >= 2 {
                for set in s.dup_values.iter().filter_map(|v| s.elements.get(v)) {
                    if set.len() >= min_count {
                        acc |= set;
                    }
                }
            } else {
                for set in s.elements.values() {
                    acc |= set;
                }
            }
        }
        FieldIndex::Text { .. } => bail!(
            "exists/duplicated is not supported on text field `{}` — declare a \
             keyword companion field for an \"is empty\"/duplicate filter",
            field
        ),
        FieldIndex::Vector { .. } => bail!(
            "exists/duplicated is not supported on vector field `{}` — use knn",
            field
        ),
        FieldIndex::Hash(_) => bail!(
            "exists/duplicated is not supported on hash field `{}` — use hamming",
            field
        ),
    }
    Ok(acc)
}

pub(super) fn eval_term(coll: &Collection, t: &TermQuery) -> Result<RoaringBitmap> {
    let fi = coll
        .fields
        .get(&t.field)
        .ok_or_else(|| StorageError::UnknownField {
            collection: "<>".into(),
            field: t.field.clone(),
        })?;
    Ok(match (fi, &t.value) {
        (FieldIndex::Keyword(k), FieldValue::String(s)) => {
            // Phase 2h-1: read the inverted index through the unified
            // accessor so a sealed field (RAM `terms` dropped) serves from
            // the mmap segment + live tail, identical to the in-RAM path.
            k.term_postings(s)
                .map(|c| c.into_owned())
                .unwrap_or_default()
        }
        (FieldIndex::Number(n), FieldValue::Number(x)) => {
            let key = SortableF64::new(*x)?;
            // Phase 2h-3: read the exact-match posting through the unified
            // accessor so a sealed field (RAM `values` dropped) serves from
            // the mmap sorted-value index + live tail, identical to the in-RAM
            // path.
            n.value_postings(key)
                .map(|c| c.into_owned())
                .unwrap_or_default()
        }
        (FieldIndex::Set(s), FieldValue::String(el)) => {
            // Phase 2h-2: read the inverted index through the unified accessor so
            // a sealed field (RAM `elements` dropped) serves from the mmap segment
            // + live tail, identical to the in-RAM path.
            s.element_postings(el)
                .map(|c| c.into_owned())
                .unwrap_or_default()
        }
        _ => bail!("term query type mismatch on field `{}`", t.field),
    })
}

pub(super) fn eval_prefix(coll: &Collection, q: &PrefixQuery) -> Result<RoaringBitmap> {
    if q.value.is_empty() {
        return Err(StorageError::QueryTooComplex("prefix value must not be empty".into()).into());
    }
    let fi = coll
        .fields
        .get(&q.field)
        .ok_or_else(|| StorageError::UnknownField {
            collection: "<>".into(),
            field: q.field.clone(),
        })?;
    if !fi.field_type().capabilities().prefix {
        bail!(
            "prefix query is only valid on keyword fields (field `{}`)",
            q.field
        );
    }
    let FieldIndex::Keyword(index) = fi else {
        unreachable!("only keyword fields advertise prefix capability")
    };
    let mut matches = RoaringBitmap::new();
    let terms = index.live_terms();
    for (term, postings) in terms.range(q.value.clone()..) {
        if !term.starts_with(&q.value) {
            break;
        }
        matches |= postings;
    }
    Ok(matches)
}

/// #182: resolve an `ids` query to the docid bitmap of the named external_ids.
/// Unknown ids are skipped (they simply contribute nothing).
///
/// #1487: liveness gate — `coll.interner.id(eid)` alone only proves the
/// external_id was *ever* interned, not that it is still live (the interner
/// itself is never GC'd; see `Collection::delete`, which removes the doc's
/// entry from `eid_fields` — not from the interner — on full delete). The
/// authoritative liveness fact used everywhere else in this module
/// (`Collection::delete`, the reseal-gather liveness predicate, the cold-load
/// invariant check) is `eid_fields.get(&id)` being present and non-empty:
/// a doc is live iff it still has at least one field written. Partial-field
/// delete leaves `eid_fields[id]` non-empty (matches `term`/`terms` still
/// hitting on the surviving field), and full delete either removes the entry
/// or leaves it empty — both read as dead here, consistent with `term`.
pub(super) fn eval_ids(coll: &Collection, q: &IdsQuery) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    for eid in &q.values {
        if let Some(id) = coll.interner.id(eid) {
            if coll.eid_fields.get(&id).is_some_and(|fs| !fs.is_empty()) {
                out.insert(id);
            }
        }
    }
    Ok(out)
}

pub(super) fn eval_terms(coll: &Collection, t: &TermsQuery) -> Result<RoaringBitmap> {
    let mut acc = RoaringBitmap::new();
    // Fast path: union the in-memory posting bitmaps by reference (word-wise
    // OR, no per-value clone).
    if let Some(fi) = coll.fields.get(&t.field) {
        for v in &t.values {
            match (fi, v) {
                (FieldIndex::Keyword(k), FieldValue::String(s)) => {
                    // Phase 2h-1: unified accessor — Borrowed (segment OFF)
                    // is the same by-reference word-wise OR as before; Owned
                    // (segment ON) ORs the decoded segment+tail union in.
                    if let Some(set) = k.term_postings(s) {
                        acc |= set.as_ref();
                    }
                }
                (FieldIndex::Set(se), FieldValue::String(el)) => {
                    // Phase 2h-2: unified accessor — Borrowed (segment OFF) is
                    // the same by-reference word-wise OR as before; Owned
                    // (segment ON) ORs the decoded segment+tail union in.
                    if let Some(set) = se.element_postings(el) {
                        acc |= set.as_ref();
                    }
                }
                (FieldIndex::Number(nx), FieldValue::Number(x)) => {
                    // Phase 2h-3: unified accessor — Borrowed (segment OFF) is
                    // the same by-reference word-wise OR as before; Owned
                    // (segment ON) ORs the decoded sorted-value+tail union in.
                    let key = SortableF64::new(*x)?;
                    if let Some(set) = nx.value_postings(key) {
                        acc |= set.as_ref();
                    }
                }
                _ => bail!("terms query type mismatch on field `{}`", t.field),
            }
        }
        return Ok(acc);
    }
    for v in &t.values {
        let one = TermQuery {
            field: t.field.clone(),
            value: v.clone(),
        };
        acc |= eval_term(coll, &one)?;
    }
    Ok(acc)
}
