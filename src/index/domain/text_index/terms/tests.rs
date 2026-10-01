//! `/stats` cost with un-absorbed staged Text rows present (#4246). A
//! committed-WAL Text value stays in `TextIndex::staged_rows` until a
//! checkpoint publication absorbs it, so `unique_terms` must stay linear in
//! live terms plus staged tokens instead of scanning every staged row once
//! per term.

use std::sync::Arc;

use crate::index::domain::postings::Postings;
use crate::index::domain::text_index::TextIndex;
use crate::index::domain::text_index::{reset_staged_term_probes, staged_term_probes};
use crate::index::infrastructure::staging::staged_text_row;
use crate::persistence::infrastructure::segment::text_row_stage::TextRowStageOptions;
use crate::shared_kernel::types::schema::Analyzer;

fn staged_row(input: &str) -> Arc<staged_text_row::StagedTextRow> {
    Arc::new(
        staged_text_row::StagedTextRow::stage(
            input,
            Analyzer::WhitespaceLower,
            TextRowStageOptions::minimum_scratch_bytes() + 4096,
            |_| Ok(()),
        )
        .expect("stage one Text row"),
    )
}

/// `tail` distinct live-tail tokens on doc 0, then `rows` staged rows each
/// carrying one token shared with every other row plus one of its own.
fn index_with_staged_rows(tail: u32, rows: u32) -> TextIndex {
    let mut idx = TextIndex {
        doc_count: 1,
        total_doc_len: u64::from(tail),
        ..Default::default()
    };
    idx.lens.push(tail);
    for n in 0..tail {
        let mut posting = Postings::default();
        posting.upsert(0, 1);
        idx.tokens.insert(format!("tail{n}"), posting);
    }
    for row in 0..rows {
        idx.staged_rows
            .insert(row + 1, staged_row(&format!("shared only{row}")));
        idx.doc_count += 1;
    }
    idx
}

#[test]
fn live_unique_tokens_counts_every_staged_and_tail_token_exactly_once() {
    let idx = index_with_staged_rows(30, 50);
    // 30 tail tokens + the one token every staged row shares + 50
    // row-private tokens.
    assert_eq!(idx.live_unique_tokens(), 30 + 1 + 50);
}

#[test]
fn live_unique_tokens_cost_stays_linear_in_the_staged_row_count() {
    let small = index_with_staged_rows(30, 25);
    reset_staged_term_probes();
    assert_eq!(small.live_unique_tokens(), 30 + 1 + 25);
    let small_probes = staged_term_probes();

    let large = index_with_staged_rows(30, 100);
    reset_staged_term_probes();
    assert_eq!(large.live_unique_tokens(), 30 + 1 + 100);
    let large_probes = staged_term_probes();

    // Reading each staged row's dictionary once costs its 2 tokens and
    // nothing per live tail term: 2 x rows probes. Asking `tok_postings`
    // per union term instead costs (tail + 1 + rows) x rows, which is
    // 13_100 here and is what made `/stats` superlinear in document count.
    assert!(
        large_probes <= 4 * (30 + 2 * 100),
        "counting staged tokens must cost O(live terms + staged tokens), \
             observed {large_probes} probes"
    );
    // Four times the rows may cost at most four times the probes.
    assert!(
        large_probes <= 4 * small_probes + 16,
        "staged-row cost must grow linearly, not quadratically: \
             {small_probes} probes at 25 rows, {large_probes} at 100"
    );
}
