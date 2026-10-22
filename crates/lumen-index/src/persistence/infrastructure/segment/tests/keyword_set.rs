use crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment;
use crate::persistence::infrastructure::segment::set_writer::write_set_segment;
use crate::persistence::infrastructure::segment::tests::{kw_terms, set_elems, tmp_path};
use crate::persistence::infrastructure::segment::SegmentReader;

#[test]
fn keyword_round_trip_small() {
    let path = tmp_path("keyword-rt-small");
    // Distinct dict (sorted): "apple"(0) "banana"(1) "cherry"(2).
    let values: Vec<Option<&str>> = vec![
        Some("banana"),
        None,
        Some("apple"),
        Some("cherry"),
        Some("banana"),
    ];
    write_keyword_segment(&path, 5, &values, &kw_terms(&values)).unwrap();

    let r = SegmentReader::open(&path).unwrap();
    assert_eq!(r.applied_seq(), 5);
    assert_eq!(r.n_docs(), 5);
    assert_eq!(r.keyword_at(0).as_deref(), Some("banana"));
    assert_eq!(r.keyword_at(1), None); // absent doc
    assert_eq!(r.keyword_at(2).as_deref(), Some("apple"));
    assert_eq!(r.keyword_at(3).as_deref(), Some("cherry"));
    assert_eq!(r.keyword_at(4).as_deref(), Some("banana"));
    assert_eq!(r.keyword_at(5), None); // id >= n_docs
    assert_eq!(r.keyword_at(u32::MAX), None);

    // INVERTED postings column (Phase 2h-1): each term's docid set + df read
    // straight off the segment, byte-identical to the in-RAM `terms` fold.
    assert_eq!(
        r.keyword_postings("banana"),
        Some([0u32, 4].into_iter().collect())
    );
    assert_eq!(
        r.keyword_postings("apple"),
        Some([2u32].into_iter().collect())
    );
    assert_eq!(
        r.keyword_postings("cherry"),
        Some([3u32].into_iter().collect())
    );
    assert_eq!(r.keyword_postings("durian"), None); // absent term
    assert_eq!(r.keyword_df("banana"), Some(2));
    assert_eq!(r.keyword_df("apple"), Some(1));
    assert_eq!(r.keyword_df("cherry"), Some(1));
    assert_eq!(r.keyword_df("durian"), None);
    // #3997: the sort planner can stream sealed dictionary ordinals without
    // materializing the complete `(term, posting)` vector.
    assert_eq!(r.keyword_ordinal_count(), Some(3));
    assert_eq!(r.keyword_term_at_ordinal(0).as_deref(), Some("apple"));
    assert_eq!(r.keyword_term_at_ordinal(1).as_deref(), Some("banana"));
    assert_eq!(r.keyword_term_at_ordinal(3), None);
    assert_eq!(
        r.keyword_postings_at_ordinal(0),
        Some([2u32].into_iter().collect()) // apple
    );
    assert_eq!(
        r.keyword_postings_at_ordinal(1),
        Some([0u32, 4].into_iter().collect()) // banana
    );
    assert_eq!(r.keyword_postings_at_ordinal(3), None);
    std::fs::remove_file(&path).ok();
}

/// The keyword posting column must span MULTIPLE 64KB LZ4 blocks and every
/// term's posting list + df must still resolve through the skip-index.
#[test]
fn keyword_postings_multi_block() {
    let path = tmp_path("keyword-postings-multi-block");
    // ~12k distinct terms (one doc each) → several posting blocks, plus a few
    // hot terms shared by many docs to vary df.
    let n = 12_000usize;
    let owned: Vec<String> = (0..n).map(|i| format!("term-{i:08}")).collect();
    let mut values: Vec<Option<&str>> = owned.iter().map(|s| Some(s.as_str())).collect();
    // Append docs that all carry the very first term, so it has a fat df.
    let hot = owned[0].clone();
    for _ in 0..50 {
        values.push(Some(hot.as_str()));
    }
    let terms = kw_terms(&values);
    write_keyword_segment(&path, 1, &values, &terms).unwrap();

    let r = SegmentReader::open(&path).unwrap();
    // Spot-check terms across the dict-id space resolve to the right docs.
    for &i in &[0usize, 1, 1234, n / 2, n - 2, n - 1] {
        let want: roaring::RoaringBitmap = terms.get(owned[i].as_str()).unwrap().iter().collect();
        assert_eq!(
            r.keyword_postings(owned[i].as_str()),
            Some(want),
            "term {i}"
        );
        assert_eq!(
            r.keyword_df(owned[i].as_str()),
            Some(terms.get(owned[i].as_str()).unwrap().len()),
            "df {i}"
        );
    }
    // The hot term: docid 0 plus the 50 appended ids.
    assert_eq!(r.keyword_df(hot.as_str()), Some(51));
    std::fs::remove_file(&path).ok();
}

/// The dictionary must span MULTIPLE 64KB LZ4 blocks and every dict-id
/// must still resolve through the skip-index binary-search + per-block
/// prefix-delta reconstruction.
#[test]
fn keyword_multi_block_dict() {
    let path = tmp_path("keyword-multi-block");
    // ~20k distinct 30-byte keys with a long shared prefix (prefix-delta
    // makes a block dense) → forces several VAR_BLOCK_BYTES (64KB) blocks.
    let n = 20_000usize;
    let owned: Vec<String> = (0..n)
        .map(|i| format!("shared-prefix-key-{i:012}"))
        .collect();
    let values: Vec<Option<&str>> = owned.iter().map(|s| Some(s.as_str())).collect();
    write_keyword_segment(&path, 1, &values, &kw_terms(&values)).unwrap();

    let r = SegmentReader::open(&path).unwrap();
    assert_eq!(r.n_docs(), n as u32);
    // Probe across the whole id space (first, mid, last, and a stride) so a
    // skip-index block boundary cannot be skipped.
    for &id in &[0usize, 1, 1234, 9999, n / 2, n - 2, n - 1] {
        assert_eq!(
            r.keyword_at(id as u32).as_deref(),
            Some(owned[id].as_str()),
            "keyword id {id} diverged"
        );
    }
    for id in (0..n).step_by(517) {
        assert_eq!(r.keyword_at(id as u32).as_deref(), Some(owned[id].as_str()));
    }
    std::fs::remove_file(&path).ok();
}

/// The skip-index must locate the correct block for every dict-id even
/// when the dictionary's block boundaries fall mid-run.
#[test]
fn dict_skip_index_locate() {
    let path = tmp_path("dict-locate");
    // Keys long enough that ~3000 of them span >1 block, with NO shared
    // prefix so suffixes are the full key (stresses the byte budget).
    let n = 3000usize;
    let owned: Vec<String> = (0..n)
        .map(|i| format!("{i:08}-{}", "x".repeat(40)))
        .collect();
    let values: Vec<Option<&str>> = owned.iter().map(|s| Some(s.as_str())).collect();
    write_keyword_segment(&path, 1, &values, &kw_terms(&values)).unwrap();
    let r = SegmentReader::open(&path).unwrap();
    // Every dict-id resolves to its own key (the dict is the sorted
    // distinct set, which == the input here since all keys are distinct).
    let mut sorted = owned.clone();
    sorted.sort();
    for (dict_id, want) in sorted.iter().enumerate() {
        let got = r.dict_string(dict_id as u32);
        assert_eq!(got.as_deref(), Some(want.as_str()), "dict id {dict_id}");
    }
    assert_eq!(r.dict_string(n as u32), None); // out of range
    std::fs::remove_file(&path).ok();
}

#[test]
fn set_round_trip_small() {
    let path = tmp_path("set-rt-small");
    let d0: Vec<String> = vec!["red".into(), "green".into()];
    let d2: Vec<String> = vec!["blue".into()];
    let d3: Vec<String> = vec![]; // present-but-empty
    let values: Vec<Option<&[String]>> = vec![Some(&d0[..]), None, Some(&d2[..]), Some(&d3[..])];
    write_set_segment(&path, 7, &values, &set_elems(&values)).unwrap();

    let r = SegmentReader::open(&path).unwrap();
    assert_eq!(r.n_docs(), 4);
    // Members come back in the order they were stored.
    assert_eq!(
        r.set_at(0),
        Some(vec!["red".to_string(), "green".to_string()])
    );
    assert_eq!(r.set_at(1), None); // absent doc
    assert_eq!(r.set_at(2), Some(vec!["blue".to_string()]));
    assert_eq!(r.set_at(3), Some(vec![])); // present-but-empty
    assert_eq!(r.set_at(4), None); // id >= n_docs

    // INVERTED postings column (Phase 2h-2): each element's docid set + df
    // read straight off the segment, byte-identical to the in-RAM `elements`
    // fold. red→{0}, green→{0}, blue→{2}.
    assert_eq!(r.set_postings("red"), Some([0u32].into_iter().collect()));
    assert_eq!(r.set_postings("green"), Some([0u32].into_iter().collect()));
    assert_eq!(r.set_postings("blue"), Some([2u32].into_iter().collect()));
    assert_eq!(r.set_postings("absent"), None); // not in the dictionary
    assert_eq!(r.set_df("red"), Some(1));
    assert_eq!(r.set_df("blue"), Some(1));
    assert_eq!(r.set_df("absent"), None);
    // The full enumeration walks the dict in sorted order with postings.
    let all = r.set_elements_all().unwrap();
    assert_eq!(
        all,
        vec![
            ("blue".to_string(), [2u32].into_iter().collect()),
            ("green".to_string(), [0u32].into_iter().collect()),
            ("red".to_string(), [0u32].into_iter().collect()),
        ]
    );
    std::fs::remove_file(&path).ok();
}

/// CSR offsets round-trip: the packed member slice for each doc must be
/// exactly `packed[offsets[i]..offsets[i+1]]`. Build a multi-valued corpus
/// with varied cardinalities (incl 0) and assert the reconstructed member
/// sets, before any dual-path wiring.
#[test]
fn set_csr_offsets_round_trip() {
    let path = tmp_path("set-csr");
    // doc i gets i % 4 members drawn from a shared pool.
    let pool = ["a", "b", "c", "d", "e", "f", "g"];
    let n = 200usize;
    let docs: Vec<Vec<String>> = (0..n)
        .map(|i| {
            (0..(i % 4))
                .map(|j| pool[(i + j) % pool.len()].to_string())
                .collect::<std::collections::BTreeSet<_>>() // dedupe + sort
                .into_iter()
                .collect()
        })
        .collect();
    let values: Vec<Option<&[String]>> = docs.iter().map(|d| Some(d.as_slice())).collect();
    let elems = set_elems(&values);
    write_set_segment(&path, 1, &values, &elems).unwrap();

    let r = SegmentReader::open(&path).unwrap();
    for (i, want) in docs.iter().enumerate() {
        let got = r.set_at(i as u32).unwrap();
        assert_eq!(
            &got, want,
            "doc {i} member slice diverged (CSR off-by-one?)"
        );
    }
    // Every element's INVERTED postings (Phase 2h-2) must equal the in-RAM
    // fold, and df its length — proves the parallel column round-trips
    // through the skip-index over a multi-valued corpus.
    for (el, want) in &elems {
        assert_eq!(
            r.set_postings(el).as_ref(),
            Some(want),
            "set element `{el}` postings diverged"
        );
        assert_eq!(
            r.set_df(el),
            Some(want.len()),
            "set element `{el}` df diverged"
        );
    }
    std::fs::remove_file(&path).ok();
}
