use std::collections::BTreeMap;

use crate::index::domain::postings::{Postings, TokPostings};
use crate::index::domain::text_index::query::count_common_sorted;
use crate::index::domain::text_index::TextIndex;
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::shared_kernel::types::schema::Analyzer;

#[test]
fn tok_probe_wiring_through_text_index_staged_overrides_live() {
    // Exercises `TextIndex::tok_probe` itself (not just the `TokProbe`
    // struct), so the staged-row gather + precedence wiring is covered
    // end-to-end for the live+staged combination.
    let mut idx = TextIndex {
        doc_count: 2,
        total_doc_len: 2,
        ..Default::default()
    };
    idx.lens.push(1);
    let mut live = Postings::default();
    live.upsert(0, 7);
    idx.tokens.insert("tok".to_string(), live);
    idx.staged_rows.insert(
        1,
        std::sync::Arc::new(
            crate::storage::staged_text_row::StagedTextRow::stage(
                "tok",
                Analyzer::WhitespaceLower,
                crate::persistence::infrastructure::segment::text_row_stage::TextRowStageOptions::minimum_scratch_bytes()
                    + 4096,
                |_| Ok(()),
            )
            .expect("stage one Text row"),
        ),
    );
    let p = idx.tok_probe("tok");
    assert!(!p.definitely_absent());
    assert_eq!(p.iter_active().collect::<Vec<_>>(), vec![(0, 7), (1, 1)]);
    let absent = idx.tok_probe("nowhere");
    assert!(absent.definitely_absent());
}

/// Randomized fixture for the sparse route (#4246): a real sealed Text
/// segment, a live overlay, staged rows and tombstones over one small
/// universe, so `tok_postings_at` is compared against `tok_postings`
/// itself — the same code the production BM25 scan reads — for the exact
/// df and every candidate's tf.
pub(crate) struct SparseFixture {
    pub(crate) idx: TextIndex,
    tokens: Vec<&'static str>,
    pub(crate) universe: u32,
}

pub(crate) fn sparse_fixture(dir: &std::path::Path, seed: u64, resident: bool) -> SparseFixture {
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u32) -> u32 {
            (self.next() % u64::from(n)) as u32
        }
    }
    let mut rng = Rng(seed | 1);
    let tokens = vec!["alpha", "beta", "gamma", "delta"];
    let n_base = 1 + rng.below(60);
    let universe = n_base + rng.below(20);
    // Sealed base: every base id carries a random subset of the tokens.
    let mut sealed: BTreeMap<String, Postings> = BTreeMap::new();
    let mut lens = vec![0u32; n_base as usize];
    let mut present = vec![false; n_base as usize];
    for id in 0..n_base {
        if rng.below(5) == 0 {
            continue;
        }
        present[id as usize] = true;
        for tok in &tokens {
            if rng.below(2) == 0 {
                let tf = 1 + rng.below(6);
                sealed.entry((*tok).to_string()).or_default().upsert(id, tf);
                lens[id as usize] += tf;
            }
        }
    }
    let doc_count = present.iter().filter(|p| **p).count() as u64;
    let total_len: u64 = lens.iter().map(|&l| u64::from(l)).sum();
    let path = dir.join(format!("f{seed}.lseg"));
    crate::persistence::infrastructure::segment::text_writer::write_text_segment(
        &path, 7, &sealed, &lens, &present, doc_count, total_len,
    )
    .expect("seal fixture");
    let reader = crate::persistence::infrastructure::segment::SegmentReader::open(&path)
        .expect("open fixture");
    if resident {
        for tok in &tokens {
            let _ = reader.text_postings_arc(tok);
        }
    }
    let mut idx = TextIndex {
        doc_count: u64::from(universe),
        total_doc_len: total_len + u64::from(universe),
        segment: Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
            std::sync::Arc::new(reader),
        ))),
        ..Default::default()
    };
    idx.lens = vec![3; universe as usize];
    // Live overlay over base AND tail ids.
    for tok in &tokens {
        let mut live = Postings::default();
        for id in 0..universe {
            if rng.below(4) == 0 {
                live.upsert(id, 1 + rng.below(6));
            }
        }
        if !live.docids.is_empty() && rng.below(5) != 0 {
            idx.tokens.insert((*tok).to_string(), live);
        }
    }
    // Staged rows: a text repeating each carried token tf times.
    for id in 0..universe {
        if rng.below(6) != 0 {
            continue;
        }
        let mut text = String::new();
        for tok in &tokens {
            if rng.below(2) == 0 {
                for _ in 0..(1 + rng.below(4)) {
                    text.push_str(tok);
                    text.push(' ');
                }
            }
        }
        if text.is_empty() {
            text.push_str("filler ");
        }
        let row = crate::storage::staged_text_row::StagedTextRow::stage(
            text.trim_end(),
            Analyzer::WhitespaceLower,
            crate::persistence::infrastructure::segment::text_row_stage::TextRowStageOptions::minimum_scratch_bytes() + 4096,
            |_| Ok(()),
        )
        .expect("stage row");
        idx.staged_rows.insert(id, std::sync::Arc::new(row));
    }
    for id in 0..n_base {
        if rng.below(4) == 0 {
            idx.tombstones.insert(id);
        }
    }
    SparseFixture {
        idx,
        tokens,
        universe,
    }
}

pub(crate) fn sparse_candidates(universe: u32, seed: u64) -> Vec<u32> {
    let mut x = seed | 1;
    (0..universe + 5)
        .filter(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % 3 == 0
        })
        .collect()
}

/// `tok_postings_at` returns the `Sparse` projection on cold AND resident
/// postings, with the exact df of `tok_postings` and the same tf for
/// every candidate, over random overlays; ids outside the candidate set
/// are absent; an absent token or an empty active posting is `None` on
/// both routes.
#[test]
fn tok_postings_at_matches_tok_postings_for_every_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let mut sparse_seen = 0usize;
    for trial in 0..40u64 {
        let resident = trial % 2 == 1;
        let fx = sparse_fixture(dir.path(), 0x4246_0000 + trial, resident);
        let candidates = sparse_candidates(fx.universe, trial * 7919 + 1);
        for tok in fx.tokens.iter().chain(["missing"].iter()) {
            let full = fx.idx.tok_postings(tok);
            let sparse = fx.idx.tok_postings_at(tok, &candidates);
            let label = format!("trial {trial} resident {resident} token {tok}");
            match (&full, &sparse) {
                (None, None) => {}
                (Some(full), Some(sparse)) => {
                    assert_eq!(sparse.df(), full.df(), "{label}: df");
                    if let TokPostings::Sparse(p) = sparse {
                        sparse_seen += 1;
                        assert!(
                            p.docids
                                .iter()
                                .all(|id| candidates.binary_search(id).is_ok()),
                            "{label}: projection ⊆ candidates"
                        );
                        assert!(
                            p.docids.windows(2).all(|w| w[0] < w[1]),
                            "{label}: projection ascending"
                        );
                    } else if fx
                        .idx
                        .segment
                        .as_ref()
                        .unwrap()
                        .text_postings_arc(tok)
                        .is_some()
                    {
                        panic!("{label}: expected the Sparse projection");
                    }
                    for &id in &candidates {
                        assert_eq!(sparse.tf(id), full.tf(id), "{label}: tf({id})");
                    }
                    let projected: Vec<u32> = candidates
                        .iter()
                        .copied()
                        .filter(|&id| full.tf(id).is_some())
                        .collect();
                    assert_eq!(
                        sparse
                            .docids()
                            .iter()
                            .copied()
                            .filter(|id| candidates.binary_search(id).is_ok())
                            .collect::<Vec<_>>(),
                        projected,
                        "{label}: docids ∩ candidates"
                    );
                }
                (full, sparse) => panic!(
                    "{label}: presence diverged: full {:?} sparse {:?}",
                    full.as_ref().map(|p| p.df()),
                    sparse.as_ref().map(|p| p.df())
                ),
            }
        }
    }
    assert!(
        sparse_seen > 60,
        "the fixture must exercise the sparse route ({sparse_seen})"
    );
}

#[test]
fn count_common_sorted_counts_the_intersection() {
    assert_eq!(count_common_sorted(&[], &[]), 0);
    assert_eq!(count_common_sorted(&[1, 2, 3], &[]), 0);
    assert_eq!(count_common_sorted(&[1, 3, 5, 7], &[2, 3, 4, 7, 9]), 2);
    assert_eq!(count_common_sorted(&[0, 1, 2], &[0, 1, 2]), 3);
    assert_eq!(count_common_sorted(&[10], &[1, 2, 10, 11]), 1);
}
