//! A number field's range stats: they match a walk of the values for every
//! bound shape, and a cache clear invalidates them.

use crate::index::domain::number_index::{NumberIndex, NumberRangeStats};
use crate::index::domain::sortable_f64::SortableF64;

#[test]
fn number_range_stats_matches_walk_on_all_bound_shapes() {
    use std::ops::Bound;
    let mut idx = NumberIndex::default();
    // Values 0.0, 1.0, ..., 99.0; value k carries k+1 docs so df != distinct.
    let mut id = 0u32;
    for k in 0..100u32 {
        let key = SortableF64::new(k as f64).unwrap();
        for _ in 0..=k {
            idx.values.entry(key).or_default().insert(id);
            id += 1;
        }
    }
    let stats = NumberRangeStats::build(&idx.values);
    let s = |x: f64| SortableF64::new(x).unwrap();
    let cases: Vec<(Bound<SortableF64>, Bound<SortableF64>)> = vec![
        (Bound::Unbounded, Bound::Unbounded),
        (Bound::Included(s(10.0)), Bound::Excluded(s(20.0))),
        (Bound::Excluded(s(10.0)), Bound::Included(s(20.0))),
        (Bound::Included(s(10.5)), Bound::Excluded(s(10.6))), // empty window
        (Bound::Unbounded, Bound::Excluded(s(0.0))),          // before first
        (Bound::Excluded(s(99.0)), Bound::Unbounded),         // after last
        (Bound::Included(s(99.0)), Bound::Included(s(99.0))), // single key
    ];
    for (lo, hi) in cases {
        let walk_distinct = idx.values.range((lo, hi)).count() as u64;
        let walk_df: u64 = idx.values.range((lo, hi)).map(|(_, s)| s.len()).sum();
        assert_eq!(
            stats.range(lo, hi),
            (walk_distinct, walk_df),
            "bounds {lo:?}..{hi:?}"
        );
        // The public estimate entry points agree with the walk too.
        assert_eq!(idx.range_df(lo, hi), walk_df, "range_df {lo:?}..{hi:?}");
        assert_eq!(
            idx.range_distinct_count(lo, hi),
            walk_distinct,
            "distinct {lo:?}..{hi:?}"
        );
    }
}

#[test]
fn number_range_stats_invalidated_by_cache_clear() {
    use std::ops::Bound;
    let mut idx = NumberIndex::default();
    for k in 0..10u32 {
        let key = SortableF64::new(k as f64).unwrap();
        idx.values.entry(key).or_default().insert(k);
    }
    let all = (Bound::Unbounded, Bound::Unbounded);
    idx.build_range_stats();
    assert_eq!(idx.range_df(all.0, all.1), 10);
    // Mutate the tree the way the write path does, then clear caches —
    // the next estimate must see the new value, not the stale snapshot.
    idx.values
        .entry(SortableF64::new(100.0).unwrap())
        .or_default()
        .insert(10);
    idx.clear_keyword_range_cache();
    assert_eq!(idx.range_df(all.0, all.1), 11);
    assert_eq!(idx.range_distinct_count(all.0, all.1), 11);
}
