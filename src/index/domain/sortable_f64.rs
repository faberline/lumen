//! The number field's key: an f64 as a total-ordered, bit-monotone u64, and the
//! range helpers that guard, lower and cache-key a range over it.

use std::hash::Hash;

use anyhow::{bail, Result};

// ---------------------------------------------------------------------------
// Sortable f64 key
// ---------------------------------------------------------------------------

/// Total-ordered, bit-monotone wrapper around `f64`. NaN is rejected at
/// construction (the API layer must validate before reaching here).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SortableF64(u64);

pub(crate) const MISSING_SORTABLE_F64_BITS: u64 = 0xfff8_0000_0000_0000;

impl SortableF64 {
    pub fn new(x: f64) -> Result<Self> {
        if x.is_nan() {
            bail!("NaN is not a valid number value");
        }
        let bits = x.to_bits();
        // Non-negatives → flip top bit (places them above negatives).
        // Negatives → flip all bits (reverses their natural order so
        // larger magnitudes sort earlier among negatives).
        let key = if x.is_sign_negative() {
            !bits
        } else {
            bits ^ (1u64 << 63)
        };
        Ok(SortableF64(key))
    }

    pub fn to_f64(self) -> f64 {
        let bits = if self.0 & (1u64 << 63) != 0 {
            // Top bit set → originally non-negative.
            self.0 ^ (1u64 << 63)
        } else {
            !self.0
        };
        f64::from_bits(bits)
    }

    /// The raw order-preserving `u64` bit key. UNSIGNED `u64` order == numeric
    /// order (the whole point of the transform), so this is exactly the key the
    /// on-disk sorted-value range index
    /// ([`segment`](crate::persistence::infrastructure::segment) `ROLE_NUMBER_SORTED`)
    /// stores and binary-searches. Phase 2h-3.
    #[inline]
    pub(crate) fn bits(self) -> u64 {
        self.0
    }

    /// Reconstruct a `SortableF64` from its raw bit key (the inverse of
    /// [`Self::bits`]) — used to lift an on-disk sorted-value key back into the
    /// in-RAM key space. Phase 2h-3.
    #[inline]
    pub(crate) fn from_bits(bits: u64) -> Self {
        SortableF64(bits)
    }
}

/// `true` when a `(low, high)` `SortableF64` range is EMPTY by construction — an
/// inverted (`low > high`) or degenerate-exclusive (`low == high` with either
/// endpoint excluded) range. `BTreeMap::range` PANICS on such a pair (it asserts
/// `start <= end`, and rejects an equal pair where a bound is `Excluded`), so the
/// in-RAM walk MUST be guarded by this and short-circuit to empty — matching the
/// on-disk `number_range_window`, which already collapses these to an empty
/// window. A range with at least one `Unbounded` end is never empty by this rule.
/// Phase 2h-3 (defensive: an inverted range was previously an unguarded panic).
pub(crate) fn range_is_empty(
    low: std::ops::Bound<SortableF64>,
    high: std::ops::Bound<SortableF64>,
) -> bool {
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

/// Lower a range `Bound<SortableF64>` into the `(bits, inclusive)` shape the
/// on-disk sorted-value range index ([`crate::persistence::infrastructure::segment::SegmentReader::number_range`])
/// consumes: `Included(b) -> Some((b.bits(), true))`, `Excluded(b) -> Some((b.bits(),
/// false))`, `Unbounded -> None`. The disk reader's `number_range_window` applies
/// exactly the same inclusive/exclusive semantics `BTreeMap::range` does, so a
/// segment-driven range is byte-identical to the in-RAM `values.range`. Phase 2h-3.
#[inline]
pub(crate) fn bound_to_bits(b: std::ops::Bound<SortableF64>) -> Option<(u64, bool)> {
    use std::ops::Bound;
    match b {
        Bound::Included(v) => Some((v.bits(), true)),
        Bound::Excluded(v) => Some((v.bits(), false)),
        Bound::Unbounded => None,
    }
}

pub(crate) fn range_cache_key(
    lo: &std::ops::Bound<SortableF64>,
    hi: &std::ops::Bound<SortableF64>,
) -> String {
    use std::ops::Bound;
    let mut key = String::new();
    match lo {
        Bound::Included(v) => {
            key.push_str("i:");
            key.push_str(&v.bits().to_string());
        }
        Bound::Excluded(v) => {
            key.push_str("e:");
            key.push_str(&v.bits().to_string());
        }
        Bound::Unbounded => key.push_str("u"),
    }
    key.push('|');
    match hi {
        Bound::Included(v) => {
            key.push_str("i:");
            key.push_str(&v.bits().to_string());
        }
        Bound::Excluded(v) => {
            key.push_str("e:");
            key.push_str(&v.bits().to_string());
        }
        Bound::Unbounded => key.push_str("u"),
    }
    key
}

#[cfg(test)]
mod tests;
