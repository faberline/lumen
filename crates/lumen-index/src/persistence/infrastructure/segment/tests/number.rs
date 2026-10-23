use crate::persistence::infrastructure::segment::number_writer::write_number_segment;
use crate::persistence::infrastructure::segment::tests::{bits, tmp_path};
use crate::persistence::infrastructure::segment::SegmentReader;

#[test]
fn round_trip_small() {
    let path = tmp_path("round-trip-small");
    let values = vec![Some(1.5), None, Some(-3.0), Some(0.0)];
    write_number_segment(&path, 42, &values).unwrap();

    let r = SegmentReader::open(&path).unwrap();
    assert_eq!(r.applied_seq(), 42);
    assert_eq!(r.n_docs(), 4);
    assert_eq!(r.number_at(0), Some(1.5));
    assert_eq!(r.number_at(1), None); // absent doc
    assert_eq!(r.number_at(2), Some(-3.0));
    assert_eq!(r.number_at(3), Some(0.0));
    assert_eq!(r.number_at(4), None); // id >= n_docs
    assert_eq!(r.number_at(u32::MAX), None);

    std::fs::remove_file(&path).ok();
}

/// Phase 2h-3: the SORTED-VALUE range index round-trips — distinct ascending
/// values, exact-match postings + df, range postings honoring every bound,
/// and the distinct-count. Values include negatives, ±0.0, and a duplicate.
#[test]
fn number_sorted_range_round_trip() {
    let path = tmp_path("number-sorted-range");
    // docids:        0     1      2     3    4     5
    let values = vec![
        Some(5.0),
        None,
        Some(-3.0),
        Some(0.0),
        Some(5.0),
        Some(-3.0),
    ];
    write_number_segment(&path, 1, &values).unwrap();
    let r = SegmentReader::open(&path).unwrap();

    // Distinct ascending values: -3.0, 0.0, 5.0.
    assert_eq!(r.number_distinct_count(), 3);

    // Exact-match postings (ascending docids) + df.
    assert_eq!(
        r.number_value_postings(bits(-3.0)),
        Some([2u32, 5].into_iter().collect())
    );
    assert_eq!(
        r.number_value_postings(bits(0.0)),
        Some([3u32].into_iter().collect())
    );
    assert_eq!(
        r.number_value_postings(bits(5.0)),
        Some([0u32, 4].into_iter().collect())
    );
    assert_eq!(r.number_value_postings(bits(99.0)), None); // absent value
    assert_eq!(r.number_value_df(bits(-3.0)), Some(2));
    assert_eq!(r.number_value_df(bits(0.0)), Some(1));
    assert_eq!(r.number_value_df(bits(5.0)), Some(2));
    assert_eq!(r.number_value_df(bits(99.0)), None);

    // RANGE bound semantics. all = {-3.0:[2,5], 0.0:[3], 5.0:[0,4]}.
    let all: roaring::RoaringBitmap = [0u32, 2, 3, 4, 5].into_iter().collect();
    // fully open
    assert_eq!(r.number_range(None, None), Some(all.clone()));
    // [-3.0, 5.0] inclusive both → all
    assert_eq!(
        r.number_range(Some((bits(-3.0), true)), Some((bits(5.0), true))),
        Some(all.clone())
    );
    // (-3.0, 5.0) exclusive both → only 0.0 → {3}
    assert_eq!(
        r.number_range(Some((bits(-3.0), false)), Some((bits(5.0), false))),
        Some([3u32].into_iter().collect())
    );
    // [-3.0, 5.0) → -3.0, 0.0 → {2,5,3}
    assert_eq!(
        r.number_range(Some((bits(-3.0), true)), Some((bits(5.0), false))),
        Some([2u32, 3, 5].into_iter().collect())
    );
    // (-3.0, 5.0] → 0.0, 5.0 → {3,0,4}
    assert_eq!(
        r.number_range(Some((bits(-3.0), false)), Some((bits(5.0), true))),
        Some([0u32, 3, 4].into_iter().collect())
    );
    // open-low (.. 0.0]  → -3.0, 0.0 → {2,5,3}
    assert_eq!(
        r.number_range(None, Some((bits(0.0), true))),
        Some([2u32, 3, 5].into_iter().collect())
    );
    // open-low (.. 0.0)  → -3.0 → {2,5}
    assert_eq!(
        r.number_range(None, Some((bits(0.0), false))),
        Some([2u32, 5].into_iter().collect())
    );
    // open-high [0.0 ..) → 0.0, 5.0 → {3,0,4}
    assert_eq!(
        r.number_range(Some((bits(0.0), true)), None),
        Some([0u32, 3, 4].into_iter().collect())
    );
    // open-high (0.0 ..) → 5.0 → {0,4}
    assert_eq!(
        r.number_range(Some((bits(0.0), false)), None),
        Some([0u32, 4].into_iter().collect())
    );
    // exact via inclusive lo==hi → single value 0.0
    assert_eq!(
        r.number_range(Some((bits(0.0), true)), Some((bits(0.0), true))),
        Some([3u32].into_iter().collect())
    );
    // empty: exclusive lo==hi
    assert_eq!(
        r.number_range(Some((bits(0.0), false)), Some((bits(0.0), false))),
        Some(roaring::RoaringBitmap::new())
    );
    // empty: inverted lo > hi
    assert_eq!(
        r.number_range(Some((bits(5.0), true)), Some((bits(-3.0), true))),
        Some(roaring::RoaringBitmap::new())
    );

    std::fs::remove_file(&path).ok();
}

/// The SORTED-VALUE range index must reproduce a BTreeMap range walk
/// byte-identically over a LARGE, multi-block corpus, across every bound
/// shape — the on-disk binary-search == the in-RAM `values.range`.
#[test]
fn number_range_matches_btreemap_oracle_multi_block() {
    let path = tmp_path("number-range-oracle");
    // 8k docs over ~2k distinct values → the posting var-column spans several
    // 64KB LZ4 blocks; the sorted-value column has ~2k entries.
    let n = 8_000usize;
    let values: Vec<Option<f64>> = (0..n)
        .map(|i| {
            if i % 37 == 0 {
                None
            } else {
                // Spread across negatives and positives with duplicates.
                Some(((i % 2000) as f64 - 1000.0) * 0.5)
            }
        })
        .collect();
    write_number_segment(&path, 1, &values).unwrap();
    let r = SegmentReader::open(&path).unwrap();

    // Build the in-RAM oracle: BTreeMap<bit-key, ascending docids>.
    use std::collections::BTreeMap;
    let mut oracle: BTreeMap<u64, roaring::RoaringBitmap> = BTreeMap::new();
    for (id, v) in values.iter().enumerate() {
        if let Some(x) = v {
            oracle.entry(bits(*x)).or_default().insert(id as u32);
        }
    }
    assert_eq!(r.number_distinct_count(), oracle.len() as u64);

    // A range of probe values, including endpoints sitting exactly on keys.
    let probes = [-1000.0, -500.0, -3.0, -0.5, 0.0, 0.5, 250.0, 499.5, 1000.0];
    for &lo in &probes {
        for &hi in &probes {
            for &lo_incl in &[true, false] {
                for &hi_incl in &[true, false] {
                    use std::ops::Bound;
                    let lo_b = bits(lo);
                    let hi_b = bits(hi);
                    // BTreeMap::range PANICS on an inverted / degenerate-exclusive
                    // pair; the on-disk window collapses it to empty. Compare the
                    // disk's empty result against the KNOWN-empty oracle without
                    // calling the panicking `oracle.range`.
                    let empty_pair = lo_b > hi_b || (lo_b == hi_b && (!lo_incl || !hi_incl));
                    let got = r
                        .number_range(Some((lo_b, lo_incl)), Some((hi_b, hi_incl)))
                        .unwrap();
                    if empty_pair {
                        assert!(
                            got.is_empty(),
                            "inverted/degenerate range [{lo} incl={lo_incl}, {hi} incl={hi_incl}) must be empty"
                        );
                        continue;
                    }
                    let low = if lo_incl {
                        Bound::Included(lo_b)
                    } else {
                        Bound::Excluded(lo_b)
                    };
                    let high = if hi_incl {
                        Bound::Included(hi_b)
                    } else {
                        Bound::Excluded(hi_b)
                    };
                    let mut want = roaring::RoaringBitmap::new();
                    for (_, set) in oracle.range((low, high)) {
                        want |= set;
                    }
                    assert_eq!(
                        got, want,
                        "range [{lo} incl={lo_incl}, {hi} incl={hi_incl}) diverged from BTreeMap oracle"
                    );
                }
            }
        }
    }
    // Open-ended probes vs the oracle's half-open / unbounded ranges.
    for &p in &probes {
        for &incl in &[true, false] {
            use std::ops::Bound;
            let b = if incl {
                Bound::Included(bits(p))
            } else {
                Bound::Excluded(bits(p))
            };
            // open-high [p.. / (p..
            let mut want_hi = roaring::RoaringBitmap::new();
            for (_, s) in oracle.range((b, Bound::Unbounded)) {
                want_hi |= s;
            }
            assert_eq!(
                r.number_range(Some((bits(p), incl)), None).unwrap(),
                want_hi,
                "open-high diverged at {p}"
            );
            // open-low ..p] / ..p)
            let mut want_lo = roaring::RoaringBitmap::new();
            for (_, s) in oracle.range((Bound::Unbounded, b)) {
                want_lo |= s;
            }
            assert_eq!(
                r.number_range(None, Some((bits(p), incl))).unwrap(),
                want_lo,
                "open-low diverged at {p}"
            );
        }
    }

    std::fs::remove_file(&path).ok();
}
