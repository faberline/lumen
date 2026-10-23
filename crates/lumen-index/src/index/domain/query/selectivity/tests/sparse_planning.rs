//! Planning over a sparse layered segment: an exact hamming filter plans its
//! match without materializing the posting (top-k and a general AND, whitespace
//! and n-gram analyzers), and the sparse candidate bound counts the actual hash
//! collisions.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::index::application::engine::Engine;
use crate::index::domain::analysis::tokenize;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::postings::Postings;
use crate::index::domain::query::selectivity::tests::{
    body, hamming, index, matchq, run, schema, seed, sig,
};
use crate::index::domain::query::selectivity::{plan_filter_candidates, SPARSE_CANDIDATE_MAX};
use crate::shared_kernel::types::query::QueryNode;
use crate::shared_kernel::types::schema::Analyzer;

fn sparse_layered_match_does_not_materialize_for_planning(general: bool, analyzer: Analyzer) {
    use crate::persistence::infrastructure::composed_segment::TextPostingAt;

    let e = Arc::new(Engine::new());
    let mut schema = schema();
    schema.fields.get_mut("body").unwrap().analyzer = Some(analyzer);
    e.create_collection("c", schema).unwrap();
    let text_for = |i| match analyzer {
        Analyzer::Ngram => format!(
            "ngram document {i} slot 0 {}",
            "durable search token ".repeat(12)
        ),
        _ => body(i),
    };
    for i in 0..128 {
        index(&e, &format!("d{i}"), sig(i), &text_for(i));
    }
    let text = text_for(7);
    let query = QueryNode::And(vec![hamming(7, 0), matchq(&text)]);
    let query = if general {
        QueryNode::Or(vec![query])
    } else {
        query
    };
    let expected = run(&e, query.clone());
    assert_eq!(expected.len(), 1);
    assert!(expected.contains_key("d7"));
    let wrong = matchq(&format!("{text} neverindexedpredicate"));
    let wrong_query = QueryNode::And(vec![hamming(7, 0), wrong.clone()]);
    let not_query = QueryNode::And(vec![hamming(7, 0), QueryNode::Not(Box::new(wrong))]);
    assert!(run(&e, wrong_query.clone()).is_empty());
    let expected_not = run(&e, not_query.clone());

    let dir = tempfile::tempdir().unwrap();
    e.__seal_text_field_to_segment("c", "body", dir.path())
        .unwrap();
    // A real replacement layer prevents the dense-base df shortcut.
    // It carries the same row, so the in-memory result remains the oracle.
    let tokens = tokenize::tokenize(&text, analyzer);
    let mut postings: BTreeMap<String, Postings> = BTreeMap::new();
    for token in &tokens {
        let posting = postings.entry(token.clone()).or_default();
        posting.upsert(0, posting.tf(0).unwrap_or(0) + 1);
    }
    let layer_path = dir.path().join("replacement.lseg");
    crate::persistence::infrastructure::segment::text_writer::write_text_segment(
        &layer_path,
        1,
        &postings,
        &[tokens.len() as u32],
        &[true],
        1,
        tokens.len() as u64,
    )
    .unwrap();
    let reader = Arc::new(
        crate::persistence::infrastructure::segment::SegmentReader::open(&layer_path).unwrap(),
    );
    let segment = {
        let mut state = e.state.write().unwrap();
        let coll = state.collections.get_mut("c").unwrap();
        // The fixture swaps the segment directly instead of publishing
        // through the normal write path. Do not reuse its live oracle.
        coll.clear_search_cache();
        let FieldIndex::Text { idx, .. } = coll.fields.get_mut("body").unwrap() else {
            unreachable!()
        };
        let segment = Arc::new(
            idx.segment
                .as_ref()
                .unwrap()
                .with_delta(reader, vec![7])
                .unwrap(),
        );
        idx.segment = Some(segment.clone());
        segment
    };
    assert!(matches!(
        segment.text_posting_at(&tokens[0], &[7], |_| false),
        Some(TextPostingAt::Sparse { .. })
    ));

    crate::persistence::infrastructure::composed_segment::reset_text_term_probes();
    assert_eq!(run(&e, query), expected, "layered BM25 score bits");
    let probes = crate::persistence::infrastructure::composed_segment::text_term_probes();
    let distinct = tokens.iter().collect::<BTreeSet<_>>().len() as u64;
    assert!(probes > 0, "the query must read the layered index");
    assert!(
        probes <= distinct,
        "planning probed {probes} postings for {distinct} distinct terms"
    );
    assert!(
        matches!(
            segment.text_posting_at(&tokens[0], &[7], |_| false),
            Some(TextPostingAt::Sparse { .. })
        ),
        "a one-document filter must not fill the whole-posting cache just to plan its match"
    );
    assert!(run(&e, wrong_query).is_empty());
    assert_eq!(run(&e, not_query), expected_not);
}

#[test]
fn sparse_layered_topk_skips_materializing_match_estimates() {
    sparse_layered_match_does_not_materialize_for_planning(false, Analyzer::WhitespaceLower);
}

#[test]
fn sparse_layered_general_and_skips_materializing_match_estimates() {
    sparse_layered_match_does_not_materialize_for_planning(true, Analyzer::WhitespaceLower);
}

#[test]
fn sparse_layered_ngram_topk_skips_materializing_match_estimates() {
    sparse_layered_match_does_not_materialize_for_planning(false, Analyzer::Ngram);
}

#[test]
fn sparse_layered_ngram_general_and_skips_materializing_match_estimates() {
    sparse_layered_match_does_not_materialize_for_planning(true, Analyzer::Ngram);
}

#[test]
fn sparse_filter_planning_checks_actual_hash_collision_count() {
    for n in [SPARSE_CANDIDATE_MAX as u32, SPARSE_CANDIDATE_MAX as u32 + 1] {
        let e = seed(n);
        for i in 0..n {
            index(&e, &format!("d{i}"), sig(0), &body(i));
        }
        let filter = hamming(0, 0);
        // The absent term has estimate zero. Only a truly bounded set may
        // skip this estimate; above the bound the original match driver wins.
        let absent = matchq("neverindexedpredicate");
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let plan = plan_filter_candidates(coll, &[&filter], &[], &[&absent]).unwrap();
        if u64::from(n) <= SPARSE_CANDIDATE_MAX {
            let ids = plan
                .expect("bounded actual candidates")
                .resolve(coll, &[&filter], &[])
                .unwrap();
            assert_eq!(ids.len(), u64::from(n));
        } else {
            assert!(
                plan.is_none(),
                "large collisions must retain the original text estimate"
            );
        }
        drop(state);
        assert!(run(&e, QueryNode::And(vec![filter.clone(), absent])).is_empty());
        let text = matchq("7");
        let exact = run(&e, QueryNode::And(vec![filter, text.clone()]));
        let fallback = run(&e, QueryNode::And(vec![hamming(0, 1), text]));
        assert_eq!(exact, fallback);
        assert_eq!(exact.len(), 1);
        assert!(exact.contains_key("d7"));
    }
}
