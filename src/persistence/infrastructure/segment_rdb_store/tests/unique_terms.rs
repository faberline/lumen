use crate::persistence::infrastructure::composed_segment::{
    reset_text_posting_clones, reset_text_term_probes, text_posting_clones, text_term_probes,
};
use crate::persistence::infrastructure::segment_rdb_store::tests::{
    committed_text, index_kw, text_schema,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::storage::Engine;
use std::sync::Arc;

/// #4246: `/stats` counts distinct Text terms. Counting must not
/// materialize a single posting — at 500k documents the owned form
/// allocated two vectors per term per request.
#[test]
fn unique_term_count_over_a_sealed_segment_copies_no_posting() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", text_schema()).unwrap();
    for n in 0..120u64 {
        committed_text(&engine, &format!("u{n}"), &format!("alpha term{n}"), n + 1);
    }
    store.save_required(&engine, 100_000).unwrap();

    reset_text_posting_clones();
    let stats = engine.stats("u").unwrap();
    assert_eq!(
        stats.fields["email"].unique_terms, 121,
        "alpha plus one private term per document"
    );
    assert_eq!(
        text_posting_clones(),
        0,
        "the distinct-term count must stream the dictionary, not copy postings"
    );
}

/// #4246: `unique_terms` must cost the same whether the sealed dictionary
/// holds 200 terms or 2000. Both engines carry the SAME staged-row work, so
/// any difference in probes is dictionary work — which must be zero.
#[test]
fn unique_term_count_cost_is_independent_of_the_sealed_dictionary_size() {
    let probes_for = |sealed: u64| -> u64 {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
        let engine = Arc::new(Engine::new());
        engine.create_collection("u", text_schema()).unwrap();
        // Seed the sealed corpus through the owned path: it needs no staged
        // row per document, so the dictionary can be large without holding
        // thousands of live change-budget reservations at once.
        for n in 0..sealed {
            index_kw(&engine, &format!("u{n}"), &format!("alpha term{n}"));
        }
        store.save_required(&engine, 900_000).unwrap();
        assert_eq!(engine.staged_text_row_count("u", "email"), 0);
        // The same eight staged rows in both runs.
        for n in 0..8u64 {
            committed_text(
                &engine,
                &format!("s{n}"),
                &format!("alpha staged{n}"),
                1_000_000 + n,
            );
        }
        reset_text_term_probes();
        let stats = engine.stats("u").unwrap();
        assert_eq!(
            stats.fields["email"].unique_terms,
            sealed + 8 + 1,
            "one private term per sealed doc, one per staged doc, plus alpha"
        );
        text_term_probes()
    };
    let small = probes_for(200);
    let large = probes_for(2_000);
    assert_eq!(
        small, large,
        "a ten-fold larger dictionary must not cost one extra term visit or posting decode"
    );
    assert_eq!(
        large, 0,
        "the count must not visit a dictionary term or decode a posting at all"
    );
}

/// #4246: the O(1) count must stay EXACT across every source a Text field
/// composes — sealed base, published delta, staged rows, live tail — and a
/// fully-deleted token must still drop out.
#[test]
fn unique_term_count_stays_exact_across_deletes_delta_and_staged_rows() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", text_schema()).unwrap();
    let terms = |engine: &Engine| engine.stats("u").unwrap().fields["email"].unique_terms;

    committed_text(&engine, "u0", "alpha one", 1);
    committed_text(&engine, "u1", "alpha two", 2);
    committed_text(&engine, "u2", "beta three", 3);
    committed_text(&engine, "u3", "beta four", 4);
    store.save_required(&engine, 100_000).unwrap();
    assert_eq!(terms(&engine), 6, "alpha beta one two three four");

    // `one` lived only in the deleted document and must drop out; `alpha`
    // survives in u1.
    engine.delete("u", "u0", Some("email")).unwrap();
    assert_eq!(terms(&engine), 5, "a fully deleted token drops out");
    assert_eq!(terms(&engine), 5, "the memoized answer is the same answer");

    // Staged rows on top of a tombstoned base.
    committed_text(&engine, "u4", "gamma five", 200_001);
    committed_text(&engine, "u5", "alpha six", 200_002);
    assert_eq!(terms(&engine), 8, "plus gamma five six");

    // Publish: the staged rows become a delta layer, the tombstone stays.
    store.save_required(&engine, 300_000).unwrap();
    assert_eq!(engine.staged_text_row_count("u", "email"), 0);
    assert_eq!(terms(&engine), 8, "publication does not change the count");

    // Delete a delta-layer document: `gamma` and `five` were only ever in it.
    engine.delete("u", "u4", Some("email")).unwrap();
    assert_eq!(terms(&engine), 6, "gamma and five drop out together");

    // A fresh staged row re-introduces one of the dropped tokens.
    committed_text(&engine, "u6", "gamma seven", 400_001);
    assert_eq!(terms(&engine), 8, "gamma is live again, plus seven");
}

/// #4246: under a pending delete the count walks the composed dictionary
/// once, decoding each posting only to its first live docid, and still
/// materializes no posting — the path every production reader takes once
/// the workload has deleted anything.
#[test]
fn unique_term_count_under_pending_deletes_copies_no_posting() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", text_schema()).unwrap();
    for n in 0..300u64 {
        committed_text(&engine, &format!("u{n}"), &format!("alpha term{n}"), n + 1);
    }
    store.save_required(&engine, 100_000).unwrap();
    assert_eq!(engine.staged_text_row_count("u", "email"), 0);
    engine.delete("u", "u7", Some("email")).unwrap();
    reset_text_posting_clones();
    let stats = engine.stats("u").unwrap();
    assert_eq!(
        stats.fields["email"].unique_terms, 300,
        "alpha plus one private term per surviving document"
    );
    assert_eq!(
        text_posting_clones(),
        0,
        "the tombstone walk must not copy a posting"
    );
}
