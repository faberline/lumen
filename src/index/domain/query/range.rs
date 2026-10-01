//! The range clause: a number or keyword range lowered to bounds and evaluated
//! to its doc set, with the per-value checks the planner's predicates and the
//! number index's sorted-bits window use.

use anyhow::{bail, Result};
use roaring::RoaringBitmap;

use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::keyword_index::KeywordIndex;
use crate::index::domain::sortable_f64::{bound_to_bits, SortableF64};
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::query::{RangeBound, RangeQuery};

pub(super) fn eval_range(coll: &Collection, r: &RangeQuery) -> Result<RoaringBitmap> {
    let fi = coll
        .fields
        .get(&r.field)
        .ok_or_else(|| StorageError::UnknownField {
            collection: "<>".into(),
            field: r.field.clone(),
        })?;
    if !fi.field_type().capabilities().range {
        bail!(
            "range query is only valid on number or keyword fields (field `{}`)",
            r.field
        );
    }
    match fi {
        FieldIndex::Number(n) => {
            // Phase 2h-3: range walk through the unified accessor. Segment OFF: walks
            // the in-RAM `values.range((low, high))` exactly as before. Segment ON:
            // binary-searches the on-disk sorted-value index to the lo/hi bounds
            // (SELECTIVE — no forward scan), subtracts tombstones, and unions the live
            // tail — byte-identical result set to the in-RAM range walk over the same
            // data.
            let (low, high) = range_bounds(r)?;
            Ok(n.range_postings(low, high))
        }
        // #1307: `keyword` range — byte/lexicographic comparison over the same
        // `BTreeMap<String, RoaringBitmap>` ordering exact `term`/`terms` match
        // already uses. Not yet segment-accelerated (walks `live_terms()`, the
        // segment+live-tail composed enumeration `duplicates`/`unique_terms`
        // already drive from); correct on every corpus, just not the on-disk
        // binary-search fast path `NumberIndex::range_postings` has.
        FieldIndex::Keyword(k) => {
            let (low, high) = keyword_range_bounds(r)?;
            Ok(keyword_range_postings(k, low, high))
        }
        _ => unreachable!("FieldType capability mapping only permits number and keyword range"),
    }
}

/// Extract this bound's numeric value, or a clear error if the caller sent a
/// string bound against what turned out to be a `number` field (#1307 AC2 —
/// 400, not a silent misparse or panic).
fn numeric_bound(field: &str, b: &RangeBound) -> Result<f64> {
    match b {
        RangeBound::Number(v) => Ok(*v),
        RangeBound::Keyword(_) => bail!(
            "range query on field `{field}` expects a numeric bound (the field is `number`-typed), got a string"
        ),
    }
}

pub(super) fn range_bounds(
    r: &RangeQuery,
) -> Result<(std::ops::Bound<SortableF64>, std::ops::Bound<SortableF64>)> {
    use std::ops::Bound;
    let low = if let Some(v) = &r.gte {
        Bound::Included(SortableF64::new(numeric_bound(&r.field, v)?)?)
    } else if let Some(v) = &r.gt {
        Bound::Excluded(SortableF64::new(numeric_bound(&r.field, v)?)?)
    } else {
        Bound::Unbounded
    };
    let high = if let Some(v) = &r.lte {
        Bound::Included(SortableF64::new(numeric_bound(&r.field, v)?)?)
    } else if let Some(v) = &r.lt {
        Bound::Excluded(SortableF64::new(numeric_bound(&r.field, v)?)?)
    } else {
        Bound::Unbounded
    };
    Ok((low, high))
}

pub(super) fn in_range(v: SortableF64, r: &RangeQuery) -> Result<bool> {
    let (lo, hi) = range_bounds(r)?;
    Ok(in_sortable_range(v, &lo, &hi))
}

/// Extract this bound's string value, or a clear error if the caller sent a
/// numeric bound against what turned out to be a `keyword` field (#1307 AC2 —
/// 400, not a silent misparse or panic).
fn keyword_bound(field: &str, b: &RangeBound) -> Result<String> {
    match b {
        RangeBound::Keyword(v) => Ok(v.clone()),
        RangeBound::Number(_) => bail!(
            "range query on field `{field}` expects a string bound (the field is `keyword`-typed), got a number"
        ),
    }
}

/// #1307: the `keyword`-field analogue of [`range_bounds`] — same
/// gt/gte/lt/lte → `Bound` lowering, but over `String` bounds compared
/// byte/lexicographically (`String`'s `Ord` is a byte-wise UTF-8 comparison),
/// the ordering exact `term`/`terms` match already relies on via
/// `KeywordIndex::terms: BTreeMap<String, RoaringBitmap>`.
fn keyword_range_bounds(
    r: &RangeQuery,
) -> Result<(std::ops::Bound<String>, std::ops::Bound<String>)> {
    use std::ops::Bound;
    let low = if let Some(v) = &r.gte {
        Bound::Included(keyword_bound(&r.field, v)?)
    } else if let Some(v) = &r.gt {
        Bound::Excluded(keyword_bound(&r.field, v)?)
    } else {
        Bound::Unbounded
    };
    let high = if let Some(v) = &r.lte {
        Bound::Included(keyword_bound(&r.field, v)?)
    } else if let Some(v) = &r.lt {
        Bound::Excluded(keyword_bound(&r.field, v)?)
    } else {
        Bound::Unbounded
    };
    Ok((low, high))
}

/// `true` when a `(low, high)` `String` range is EMPTY by construction — the
/// `keyword`-bound analogue of [`range_is_empty`]: same inverted /
/// degenerate-exclusive guard (`BTreeMap::range` panics on the same shapes
/// for a `String` key as it does for `SortableF64`), just over `String`.
///
/// [`range_is_empty`]: crate::index::domain::sortable_f64::range_is_empty
fn range_is_empty_str(low: &std::ops::Bound<String>, high: &std::ops::Bound<String>) -> bool {
    use std::ops::Bound::*;
    let (lo, lo_excl) = match low {
        Included(b) => (b, false),
        Excluded(b) => (b, true),
        Unbounded => return false,
    };
    let (hi, hi_excl) = match high {
        Included(b) => (b, false),
        Excluded(b) => (b, true),
        Unbounded => return false,
    };
    lo > hi || (lo == hi && (lo_excl || hi_excl))
}

/// #1307: byte/lexicographic range walk over a `keyword` field's LIVE terms —
/// segment + live-tail composed via [`KeywordIndex::live_terms`] (the same
/// unified enumeration `duplicates`/`unique_terms` drive from after a seal
/// drops the in-RAM `terms` driver), so a sealed collection answers a keyword
/// range identically to an unsealed one. `BTreeMap<String, _>::range` walks
/// the SAME byte-order `terms` already keeps for exact `term`/`terms` match —
/// no new sort machinery.
fn keyword_range_postings(
    k: &KeywordIndex,
    lo: std::ops::Bound<String>,
    hi: std::ops::Bound<String>,
) -> RoaringBitmap {
    if range_is_empty_str(&lo, &hi) {
        return RoaringBitmap::new();
    }
    let terms = k.live_terms();
    let mut out = RoaringBitmap::new();
    for (_, posting) in terms.range((lo, hi)) {
        out |= posting;
    }
    out
}

pub(super) fn in_keyword_range(v: &str, r: &RangeQuery) -> Result<bool> {
    let (lo, hi) = keyword_range_bounds(r)?;
    Ok(in_str_range(v, &lo, &hi))
}

#[inline]
fn in_str_range(v: &str, lo: &std::ops::Bound<String>, hi: &std::ops::Bound<String>) -> bool {
    use std::ops::Bound::*;
    let lo_ok = match lo {
        Included(b) => v >= b.as_str(),
        Excluded(b) => v > b.as_str(),
        Unbounded => true,
    };
    let hi_ok = match hi {
        Included(b) => v <= b.as_str(),
        Excluded(b) => v < b.as_str(),
        Unbounded => true,
    };
    lo_ok && hi_ok
}

#[inline]
fn in_sortable_range(
    v: SortableF64,
    lo: &std::ops::Bound<SortableF64>,
    hi: &std::ops::Bound<SortableF64>,
) -> bool {
    in_sortable_bits_range(v.bits(), lo, hi)
}

#[inline]
pub(in crate::index::domain) fn in_sortable_bits_range(
    bits: u64,
    lo: &std::ops::Bound<SortableF64>,
    hi: &std::ops::Bound<SortableF64>,
) -> bool {
    use std::ops::Bound::*;
    let lo_ok = match lo {
        Included(b) => bits >= b.bits(),
        Excluded(b) => bits > b.bits(),
        Unbounded => true,
    };
    let hi_ok = match hi {
        Included(b) => bits <= b.bits(),
        Excluded(b) => bits < b.bits(),
        Unbounded => true,
    };
    lo_ok && hi_ok
}

#[derive(Clone, Copy)]
pub(super) struct SortableBitsBounds {
    low: Option<(u64, bool)>,
    high: Option<(u64, bool)>,
}

impl SortableBitsBounds {
    #[inline]
    pub(super) fn new(
        lo: &std::ops::Bound<SortableF64>,
        hi: &std::ops::Bound<SortableF64>,
    ) -> Self {
        Self {
            low: bound_to_bits(*lo),
            high: bound_to_bits(*hi),
        }
    }

    #[inline(always)]
    pub(super) fn contains(self, bits: u64) -> bool {
        let low_ok = match self.low {
            Some((b, true)) => bits >= b,
            Some((b, false)) => bits > b,
            None => true,
        };
        let high_ok = match self.high {
            Some((b, true)) => bits <= b,
            Some((b, false)) => bits < b,
            None => true,
        };
        low_ok && high_ok
    }
}

#[inline]
fn lower_bound_sortable_bits(values: &[(u64, u32)], bits: u64) -> usize {
    values.partition_point(|(probe, _)| *probe < bits)
}

#[inline]
fn upper_bound_sortable_bits(values: &[(u64, u32)], bits: u64) -> usize {
    values.partition_point(|(probe, _)| *probe <= bits)
}

#[inline]
pub(in crate::index::domain) fn sorted_bits_window(
    values: &[(u64, u32)],
    lo: &std::ops::Bound<SortableF64>,
    hi: &std::ops::Bound<SortableF64>,
) -> std::ops::Range<usize> {
    use std::ops::Bound::*;
    let start = match lo {
        Included(b) => lower_bound_sortable_bits(values, b.bits()),
        Excluded(b) => upper_bound_sortable_bits(values, b.bits()),
        Unbounded => 0,
    };
    let end = match hi {
        Included(b) => upper_bound_sortable_bits(values, b.bits()),
        Excluded(b) => lower_bound_sortable_bits(values, b.bits()),
        Unbounded => values.len(),
    };
    start.min(end)..end
}
