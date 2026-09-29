use std::cell::Cell;

use crate::persistence::infrastructure::segment::reader_cache::posting_cache_key;
use crate::persistence::infrastructure::segment::tests::tmp_path;
use crate::persistence::infrastructure::segment::text_writer::write_text_segment;
use crate::persistence::infrastructure::segment::var_column::decode_var_skip_index;
use crate::persistence::infrastructure::segment::{
    ColumnRef, SegmentReader, CODEC_LZ4_VAR, DICTIONARY_SEARCHES, ROLE_DICT, ROLE_TEXT_POSTINGS,
    VAR_SKIP_INDEX_DECODES,
};

/// `write_text_segment` round-trip: a small text corpus's stored postings,
/// doc-len column, and header scalars all read back exactly.
#[test]
fn text_segment_round_trip() {
    use crate::storage::Postings;
    let path = tmp_path("text-rt");
    // Tokens (dict order is ascending — BTreeMap): "apple", "banana", "cherry".
    let mut tokens: std::collections::BTreeMap<String, Postings> =
        std::collections::BTreeMap::new();
    tokens.insert(
        "apple".into(),
        Postings::from_sorted(vec![0, 2, 3], vec![3, 1, 2]),
    );
    tokens.insert(
        "banana".into(),
        Postings::from_sorted(vec![1, 3], vec![5, 1]),
    );
    tokens.insert("cherry".into(), Postings::from_sorted(vec![0], vec![1]));
    let lens: Vec<u32> = vec![4, 5, 1, 3]; // doc lengths
    let present = vec![true; lens.len()];
    let doc_count: u64 = 4;
    let total_doc_len: u64 = 4 + 5 + 1 + 3;

    write_text_segment(
        &path,
        42,
        &tokens,
        &lens,
        &present,
        doc_count,
        total_doc_len,
    )
    .unwrap();
    let r = SegmentReader::open(&path).unwrap();

    assert_eq!(r.applied_seq(), 42);
    assert_eq!(r.n_docs(), 4);
    assert_eq!(r.text_doc_count(), doc_count);
    assert_eq!(r.text_total_doc_len(), total_doc_len);

    // Postings round-trip exactly, per token.
    assert_eq!(
        r.text_postings("apple"),
        Some((vec![0, 2, 3], vec![3, 1, 2]))
    );
    assert_eq!(r.text_postings("banana"), Some((vec![1, 3], vec![5, 1])));
    assert_eq!(r.text_postings("cherry"), Some((vec![0], vec![1])));
    assert_eq!(r.text_postings("durian"), None); // absent token

    // df == stored posting length.
    assert_eq!(r.text_token_df("apple"), 3);
    assert_eq!(r.text_token_df("banana"), 2);
    assert_eq!(r.text_token_df("cherry"), 1);
    assert_eq!(r.text_token_df("durian"), 0);

    // DocLen column read zero-copy.
    for (id, &l) in lens.iter().enumerate() {
        assert_eq!(r.text_doc_len(id as u32), l, "doclen id {id}");
        assert!(r.text_is_present(id as u32), "present id {id}");
    }
    assert_eq!(r.text_doc_len(99), 0); // out of range -> 0

    std::fs::remove_file(&path).ok();
}

#[test]
fn corrupt_var_skip_index_stays_a_lookup_refusal() {
    let column = ColumnRef {
        name: "dict".into(),
        role: ROLE_DICT,
        byte_offset: 0,
        byte_len: 0,
        elem_count: 0,
        width: 0,
        codec: CODEC_LZ4_VAR,
        skip_index: vec![0xff, 0x00],
    };
    assert!(decode_var_skip_index(&column).is_none());
}

#[test]
fn reader_parses_var_skip_indexes_once_and_reuses_them_for_text_lookups() {
    use crate::storage::Postings;
    let path = tmp_path("text-skip-index-once");
    let mut tokens = std::collections::BTreeMap::new();
    tokens.insert("alpha".into(), Postings::from_sorted(vec![0], vec![2]));
    tokens.insert("beta".into(), Postings::from_sorted(vec![0], vec![1]));
    write_text_segment(&path, 1, &tokens, &[3], &[true], 1, 3).unwrap();

    VAR_SKIP_INDEX_DECODES.with(|count| count.set(0));
    let reader = SegmentReader::open(&path).unwrap();
    let at_open = VAR_SKIP_INDEX_DECODES.with(Cell::get);
    assert_eq!(at_open, 2, "dict and text postings parse once at open");
    for _ in 0..4 {
        assert_eq!(reader.text_postings("alpha"), Some((vec![0], vec![2])));
        assert_eq!(reader.text_postings("beta"), Some((vec![0], vec![1])));
    }
    assert_eq!(
        VAR_SKIP_INDEX_DECODES.with(Cell::get),
        at_open,
        "lookups borrow immutable parsed skip indexes instead of decoding CBOR again"
    );
    assert_eq!(reader.text_dictionary_stats(), Some((2, 9)));
    std::fs::remove_file(&path).ok();
}

/// #4246: a liveness probe decodes a posting only as far as its first
/// accepted docid, through no cache and with no dictionary search.
#[test]
fn text_posting_scan_streams_the_stored_posting_without_filling_the_cache() {
    use crate::storage::Postings;
    let path = tmp_path("text-posting-scan");
    let mut tokens: std::collections::BTreeMap<String, Postings> =
        std::collections::BTreeMap::new();
    let ids: Vec<u32> = (0..5000u32).filter(|i| i % 3 != 1).collect();
    let tfs: Vec<u32> = ids.iter().map(|i| 1 + i % 7).collect();
    tokens.insert(
        "apple".into(),
        Postings::from_sorted(ids.clone(), tfs.clone()),
    );
    tokens.insert(
        "banana".into(),
        Postings::from_sorted(vec![1, 3], vec![5, 1]),
    );
    let lens: Vec<u32> = vec![3; 5000];
    let present = vec![true; lens.len()];
    write_text_segment(&path, 1, &tokens, &lens, &present, 5000, 15000).unwrap();
    let r = SegmentReader::open(&path).unwrap();

    assert!(
        r.text_posting_cached("apple").is_none(),
        "cold reader: nothing resident"
    );
    let mut seen = Vec::new();
    assert_eq!(
        r.text_posting_scan("apple", |id, tf| seen.push((id, tf))),
        Some(ids.len()),
        "the count prefix is the token's df"
    );
    let want: Vec<(u32, u32)> = ids.iter().copied().zip(tfs.iter().copied()).collect();
    assert_eq!(
        seen, want,
        "the scan visits exactly the encoded stream, ascending"
    );
    assert!(
        r.text_posting_cached("apple").is_none(),
        "a scan neither reads nor fills the posting cache"
    );
    assert_eq!(r.text_posting_scan("cherry", |_, _| unreachable!()), None);

    let resident = r.text_postings_arc("apple").unwrap();
    assert!(
        r.text_posting_cached("apple")
            .is_some_and(|hit| std::sync::Arc::ptr_eq(&hit, &resident)),
        "after a materializing read the same Arc is resident"
    );
    assert_eq!(resident.0, ids);
    assert_eq!(resident.1, tfs);
    assert!(r.text_posting_cached("banana").is_none());
}

#[test]
fn text_posting_any_at_stops_at_the_first_accepted_docid() {
    use crate::storage::Postings;
    let path = tmp_path("text-any-at");
    let mut tokens: std::collections::BTreeMap<String, Postings> =
        std::collections::BTreeMap::new();
    tokens.insert(
        "apple".into(),
        Postings::from_sorted(vec![0, 2, 3], vec![3, 1, 2]),
    );
    tokens.insert(
        "banana".into(),
        Postings::from_sorted(vec![1, 3], vec![5, 1]),
    );
    let lens: Vec<u32> = vec![3, 5, 1, 3];
    let present = vec![true; lens.len()];
    write_text_segment(&path, 1, &tokens, &lens, &present, 4, 12).unwrap();
    let r = SegmentReader::open(&path).unwrap();

    DICTIONARY_SEARCHES.with(|count| count.set(0));
    let mut seen = Vec::new();
    assert_eq!(
        r.text_posting_any_at(0, |id| {
            seen.push(id);
            id == 2
        }),
        Some(true)
    );
    assert_eq!(seen, [0, 2], "decoding stops at the first accepted docid");
    seen.clear();
    assert_eq!(
        r.text_posting_any_at(0, |id| {
            seen.push(id);
            false
        }),
        Some(false)
    );
    assert_eq!(seen, [0, 2, 3], "a rejected posting is decoded to its end");
    seen.clear();
    assert_eq!(
        r.text_posting_any_at(1, |id| {
            seen.push(id);
            id == 3
        }),
        Some(true)
    );
    assert_eq!(seen, [1, 3]);
    assert_eq!(
        r.text_posting_any_at(2, |_| true),
        None,
        "an ordinal past the dictionary is torn, not false"
    );
    assert_eq!(
        DICTIONARY_SEARCHES.with(|count| count.get()),
        0,
        "the ordinal is the posting address: no dictionary search"
    );
    assert!(
        r.text_posting_cache
            .get(&posting_cache_key(ROLE_TEXT_POSTINGS, 0))
            .is_none(),
        "the probe fills no posting cache"
    );
}

#[test]
fn text_segment_presence_is_independent_from_doclen() {
    use crate::storage::Postings;
    let tokens: std::collections::BTreeMap<String, Postings> = std::collections::BTreeMap::new();
    let lens = vec![0, 0, 1];
    let present = vec![false, true, true];
    let path = tmp_path("text-presence");
    write_text_segment(&path, 1, &tokens, &lens, &present, 2, 1).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert!(!reader.text_is_present(0));
    assert!(reader.text_is_present(1));
    assert!(reader.text_is_present(2));
    assert!(!reader.text_is_present(3));
    std::fs::remove_file(&path).ok();

    let legacy_present: Vec<bool> = lens.iter().map(|&len| len > 0).collect();
    let legacy_path = tmp_path("text-presence-legacy");
    write_text_segment(&legacy_path, 1, &tokens, &lens, &legacy_present, 1, 1).unwrap();
    let legacy = SegmentReader::open(&legacy_path).unwrap();
    assert!(!legacy.text_is_present(1));
    assert!(legacy.text_is_present(2));
    std::fs::remove_file(&legacy_path).ok();

    let mismatch_path = tmp_path("text-presence-mismatch");
    assert!(write_text_segment(&mismatch_path, 1, &tokens, &lens, &[true, false], 2, 1).is_err());
    assert!(!mismatch_path.exists());
}

/// The token dictionary must span MULTIPLE 64KB LZ4 blocks and every token's
/// posting block must still resolve through the skip-index binary-search.
#[test]
fn text_segment_multi_block() {
    use crate::storage::Postings;
    let path = tmp_path("text-multi-block");
    let n_tokens = 20_000usize;
    let n_docs = 200u32;
    let mut tokens: std::collections::BTreeMap<String, Postings> =
        std::collections::BTreeMap::new();
    for i in 0..n_tokens {
        // Each token posts to a couple of docs with deterministic tf.
        let d0 = (i as u32) % n_docs;
        let d1 = ((i as u32) * 7 + 3) % n_docs;
        let (docids, tfs) = if d0 == d1 {
            (vec![d0], vec![(i % 7 + 1) as u32])
        } else if d0 < d1 {
            (vec![d0, d1], vec![(i % 7 + 1) as u32, (i % 3 + 1) as u32])
        } else {
            (vec![d1, d0], vec![(i % 3 + 1) as u32, (i % 7 + 1) as u32])
        };
        tokens.insert(
            format!("shared-prefix-token-{i:012}"),
            Postings::from_sorted(docids, tfs),
        );
    }
    let lens: Vec<u32> = (0..n_docs).map(|d| (d % 9) + 1).collect();
    let present = vec![true; n_docs as usize];
    write_text_segment(
        &path,
        1,
        &tokens,
        &lens,
        &present,
        n_docs as u64,
        lens.iter().map(|&l| l as u64).sum(),
    )
    .unwrap();
    let r = SegmentReader::open(&path).unwrap();
    assert_eq!(r.n_docs(), n_docs);
    // Probe across the whole token space so no skip-index block is missed.
    for &i in &[
        0usize,
        1,
        1234,
        9999,
        n_tokens / 2,
        n_tokens - 2,
        n_tokens - 1,
    ] {
        let tok = format!("shared-prefix-token-{i:012}");
        let want = tokens.get(&tok).unwrap();
        assert_eq!(
            r.text_postings(&tok),
            Some((want.docids().to_vec(), want.tfs().to_vec())),
            "token {i} postings diverged"
        );
        assert_eq!(r.text_token_df(&tok), want.docids().len());
    }
    std::fs::remove_file(&path).ok();
}
