use std::collections::BTreeSet;
use std::sync::Arc;

use crate::index::application::engine::seal::tests::number_range::{
    assert_values_dropped, index_num, rangeq, run, run_sorted, schema, search_total, set_of,
    termkw, termnum, uniq,
};
use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::sortable_f64::SortableF64;
use crate::shared_kernel::types::query::{QueryNode, SortOrder};

/// RAM-BOUNDED + REOPEN-NO-REBUILD: seal the WHOLE collection to disk, reopen
/// from the segments alone (no CBOR snapshot), and assert the reopened Number
/// field's `values` map is EMPTY while range/exact queries still answer from
/// the mmap (a binary-search on the sorted-value column, NOT an O(n) forward
/// scan — the rebuild loop in `open_from_segment` was deleted in 2h-3).
#[test]
fn reopen_drives_from_segment_with_empty_values() {
    let dir = tempfile::tempdir().unwrap();
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_num(&e, "a", Some(-3.0), Some("red"));
    index_num(&e, "b", Some(0.0), Some("green"));
    index_num(&e, "c2", Some(3.0), None);
    index_num(&e, "d", Some(0.0), Some("red")); // dup value 0.0
    index_num(&e, "e", None, Some("green")); // present-but-no-price

    let want_range = set_of(&run(&e, rangeq(Some(-3.0), None, None, Some(3.0))));
    let want_exact0 = set_of(&run(&e, termnum(0.0)));
    let want_uniq = uniq(&e, "price");

    e.__seal_collection_to_segments("c", dir.path(), 1).unwrap();
    let schema = e.__collection_schema("c").unwrap();
    let e2 = Engine::__open_collection_from_segments("c", dir.path(), schema, 1).unwrap();

    // The reopened Number `values` map must be EMPTY (no RAM rebuild) ...
    {
        let state = e2.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Number(n) = coll.fields.get("price").unwrap() else {
            panic!("price must be a Number field");
        };
        assert!(n.segment.is_some(), "reopened segment attached");
        assert!(
            n.values.is_empty(),
            "reopen must NOT rebuild `values` in RAM (got {} entries) — the \
                 range read is a binary-search on the mmap sorted-value column, \
                 not an O(N) forward scan that rebuilds `values`",
            n.values.len()
        );
        // The on-disk sorted-value column carries the distinct values, so the
        // segment can answer a range WITHOUT the RAM map.
        let seg = n.segment.as_ref().unwrap();
        assert_eq!(
            seg.number_range_distinct_count(None, None).unwrap(),
            3,
            "distinct values {{-3.0, 0.0, 3.0}} live on the mmap sorted column"
        );
    }

    // ... yet range / exact queries still resolve, entirely from the mmap.
    assert_eq!(
        set_of(&run(&e2, rangeq(Some(-3.0), None, None, Some(3.0)))),
        want_range,
        "range post-reopen"
    );
    assert_eq!(
        set_of(&run(&e2, termnum(0.0))),
        want_exact0,
        "exact post-reopen"
    );
    assert_eq!(uniq(&e2, "price"), want_uniq, "unique_terms post-reopen");
}

#[test]
fn segment_keyword_range_skip_cache_matches_live_after_delete_and_tail() {
    fn build(e: &Engine, n: usize) {
        e.create_collection("c", schema()).unwrap();
        for i in 0..n {
            index_num(
                e,
                &format!("d{i}"),
                Some((i % 100) as f64),
                Some(if i % 2 == 0 { "red" } else { "blue" }),
            );
        }
    }

    let n = 20_000usize; // red df=10k, high enough to take the dense fast path.
    let query = QueryNode::And(vec![
        termkw("cat", "red"),
        rangeq(Some(10.0), None, None, Some(20.0)),
    ]);

    let oracle = Arc::new(Engine::new());
    build(&oracle, n);
    oracle.delete("c", "d10", None).unwrap();
    index_num(&oracle, "tail", Some(12.0), Some("red"));
    let want = set_of(&run(&oracle, query.clone()));
    let want_total = search_total(&oracle, query.clone());
    let want_sort = run_sorted(&oracle, termkw("cat", "red"), "price", SortOrder::Asc);

    let subject = Arc::new(Engine::new());
    build(&subject, n);
    let dir = tempfile::tempdir().unwrap();
    subject
        .__seal_collection_to_segments("c", dir.path(), 1)
        .unwrap();
    {
        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Keyword(k) = coll.fields.get("cat").unwrap() else {
            panic!("cat");
        };
        let FieldIndex::Number(num) = coll.fields.get("price").unwrap() else {
            panic!("price");
        };
        assert!(k.segment.is_some(), "keyword segment attached");
        assert!(k.terms.is_empty(), "keyword RAM driver dropped after seal");
        assert!(num.segment.is_some(), "number segment attached");
        assert!(
            num.values.is_empty(),
            "number RAM driver dropped after seal"
        );
    }

    subject.delete("c", "d10", None).unwrap();
    index_num(&subject, "tail", Some(12.0), Some("red"));

    let got = set_of(&run(&subject, query.clone()));
    let got_total = search_total(&subject, query);
    let got_sort = run_sorted(&subject, termkw("cat", "red"), "price", SortOrder::Asc);
    assert_eq!(got, want, "segment term+range fast path set diverged");
    assert_eq!(
        got_total, want_total,
        "segment term+range fast path total diverged"
    );
    assert_eq!(
        got_sort, want_sort,
        "segment keyword-filtered number sort page diverged"
    );

    let state = subject.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Number(num) = coll.fields.get("price").unwrap() else {
        panic!("price");
    };
    let cache = num
        .keyword_range_cache
        .read()
        .expect("number keyword-range cache poisoned");
    assert!(
        cache.contains_key("cat\0red"),
        "planner should have built the lazy segment keyword+range skip cache"
    );
}

/// LIVE-TAIL UNION: seal, then index more docs (tail into the live `values`),
/// and assert range/exact compose segment-base (sorted-value column) with the
/// live-tail `values.range`.
#[test]
fn live_tail_unions_with_segment_base() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_num(&e, "base0", Some(1.0), None); // id 0, sealed base
    index_num(&e, "base1", Some(5.0), None); // id 1, sealed base

    let dir = tempfile::tempdir().unwrap();
    let n = e
        .__seal_number_field_to_segment("c", "price", dir.path())
        .unwrap();
    assert_eq!(n, 2);
    assert_values_dropped(&e); // base index now on disk

    // Tail docs after seal → live `values` tail (ids >= n_docs).
    index_num(&e, "tail0", Some(1.0), None); // id 2, tail, SAME value as base0
    index_num(&e, "tail1", Some(9.0), None); // id 3, tail, NEW value

    // range [0, 10]: base {base0=1, base1=5} ∪ tail {tail0=1, tail1=9}.
    let got = set_of(&run(&e, rangeq(Some(0.0), None, Some(10.0), None)));
    let want: BTreeSet<String> = ["base0", "base1", "tail0", "tail1"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(got, want, "range must union segment base + live tail");

    // exact 1.0: base0 (segment) ∪ tail0 (tail).
    let got = set_of(&run(&e, termnum(1.0)));
    let want: BTreeSet<String> = ["base0", "tail0"].iter().map(|s| s.to_string()).collect();
    assert_eq!(got, want, "exact 1.0 must union segment base + live tail");

    // exact 9.0: ONLY the tail (not in the segment).
    let got = set_of(&run(&e, termnum(9.0)));
    let want: BTreeSet<String> = ["tail1".to_string()].into_iter().collect();
    assert_eq!(got, want, "9.0 must come from the live tail alone");

    // exact 5.0: ONLY the segment base.
    let got = set_of(&run(&e, termnum(5.0)));
    let want: BTreeSet<String> = ["base1".to_string()].into_iter().collect();
    assert_eq!(got, want, "5.0 must come from the segment base alone");

    // df composition: value_df(1.0) = 1 (base) + 1 (tail) = 2.
    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Number(nidx) = coll.fields.get("price").unwrap() else {
        panic!("price");
    };
    let k1 = SortableF64::new(1.0).unwrap();
    let k9 = SortableF64::new(9.0).unwrap();
    let k5 = SortableF64::new(5.0).unwrap();
    let k7 = SortableF64::new(7.0).unwrap();
    assert_eq!(
        nidx.value_df(k1),
        2,
        "df 1.0 must sum segment base + live tail"
    );
    assert_eq!(nidx.value_df(k5), 1, "df 5.0 segment-only");
    assert_eq!(nidx.value_df(k9), 1, "df 9.0 tail-only");
    assert_eq!(nidx.value_df(k7), 0, "df absent value");
}
