use std::collections::BTreeMap;

use roaring::RoaringBitmap;

use crate::index::domain::postings::Postings;
use crate::index::domain::query::rank::tests::{interner_with, Rng};
use crate::index::domain::query::rank::{bm25_contrib, ensure_sorted_prefix, MatchRankCache};
use crate::index::domain::query::zip_cursor::{build_and_ranked, should_use_zipper, AndRankInput};
use crate::index::domain::text_index::TextIndex;
use crate::index::domain::tok_probe::TokProbe;

fn postings_from(pairs: &[(u32, u32)]) -> Postings {
    Postings::from_sorted(
        pairs.iter().map(|&(id, _)| id).collect(),
        pairs.iter().map(|&(_, tf)| tf).collect(),
    )
}

/// Naive oracle: score every doc carrying EVERY token via a plain
/// nested-loop intersection (no probe/zipper strategy), using the exact
/// same `bm25_contrib` expression `build_and_ranked` does.
fn reference_and_scores(
    idx: &TextIndex,
    token_posts: &[Vec<(u32, u32)>],
    idfs: &[f32],
    avgdl: f32,
) -> BTreeMap<u32, f32> {
    let mut out = BTreeMap::new();
    if token_posts.is_empty() {
        return out;
    }
    'docs: for &(id, _) in &token_posts[0] {
        let doc_len = idx.doc_len(id) as f32;
        let mut score = 0.0f32;
        for (k, posts) in token_posts.iter().enumerate() {
            match posts.iter().find(|&&(pid, _)| pid == id) {
                Some(&(_, tf)) => {
                    score += bm25_contrib(idfs[k], tf as f32, doc_len, avgdl);
                }
                None => continue 'docs,
            }
        }
        out.insert(id, score);
    }
    out
}

/// Randomized equivalence: `build_and_ranked` must match the naive
/// intersection oracle whether the dense-zipper branch or the sparse
/// binary-search branch runs — trial parity biases the fixture toward
/// each so both strategies get exercised across the sweep.
#[test]
fn build_and_ranked_agrees_with_reference_dense_and_sparse() {
    let mut rng = Rng(0x243F_6A88_85A3_08D3);
    for trial in 0..80u32 {
        let universe = 5 + rng.below(120);
        let n_tokens = 2 + rng.below(3); // 2..=4 tokens
        let dense = trial % 2 == 0;
        let mk_ids = |rng: &mut Rng, dense: bool| -> Vec<u32> {
            let threshold = if dense { 1 } else { 6 };
            let mut ids: Vec<u32> = (0..universe)
                .filter(|_| rng.below(threshold) == 0)
                .collect();
            ids.sort_unstable();
            ids.dedup();
            ids
        };

        let mut idx = TextIndex {
            doc_count: universe as u64,
            total_doc_len: universe as u64,
            ..Default::default()
        };
        for _ in 0..universe {
            idx.lens.push(1 + rng.below(20));
        }

        let mut token_pairs: Vec<Vec<(u32, u32)>> = Vec::new();
        for _ in 0..n_tokens {
            let ids = mk_ids(&mut rng, dense);
            let pairs: Vec<(u32, u32)> = ids.iter().map(|&id| (id, 1 + rng.below(9))).collect();
            token_pairs.push(pairs);
        }
        // Always drive from the rarest (shortest) token, matching
        // `eval_match_topk`'s `min_by_key(dfs)` selection.
        token_pairs.sort_by_key(|p| p.len());

        let postings: Vec<Postings> = token_pairs.iter().map(|p| postings_from(p)).collect();
        let empty_tombstones = RoaringBitmap::new();
        let posts: Vec<TokProbe<'_>> = postings
            .iter()
            .map(|p| TokProbe {
                seg: None,
                live: Some(p),
                staged: Vec::new(),
                tombstones: &empty_tombstones,
            })
            .collect();

        let n = universe as f32;
        let idfs: Vec<f32> = token_pairs
            .iter()
            .map(|p| {
                let df = p.len() as f32;
                ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
            })
            .collect();
        let avgdl = idx.total_doc_len as f32 / idx.doc_count.max(1) as f32;
        let drive = 0usize;
        let drive_len = token_pairs[0].len();

        let entries = build_and_ranked(AndRankInput {
            idx: &idx,
            posts: &posts,
            idfs: &idfs,
            drive,
            drive_len,
            avgdl,
        });

        let want = reference_and_scores(&idx, &token_pairs, &idfs, avgdl);
        let got: BTreeMap<u32, f32> = entries.into_iter().collect();
        assert_eq!(
            got, want,
            "trial {trial} (dense={dense}): build_and_ranked diverged from reference"
        );
    }
}

/// The dense/sparse switch itself: a sparse driver against one huge dense
/// other posting must stay on the binary-search probe (a streaming pass
/// over the dense posting would be strictly more work), while two
/// postings comparably dense to the driver must pick the zipper.
#[test]
fn should_use_zipper_switches_on_density() {
    let empty = RoaringBitmap::new();
    let sparse_driver = postings_from(&[(1, 1)]);
    let dense_other_ids: Vec<(u32, u32)> = (0..2000).map(|i| (i, 1)).collect();
    let dense_other = postings_from(&dense_other_ids);
    let posts = vec![
        TokProbe {
            seg: None,
            live: Some(&sparse_driver),
            staged: Vec::new(),
            tombstones: &empty,
        },
        TokProbe {
            seg: None,
            live: Some(&dense_other),
            staged: Vec::new(),
            tombstones: &empty,
        },
    ];
    assert!(!should_use_zipper(&posts, 0, 1));

    let dense_a_ids: Vec<(u32, u32)> = (0..2000).map(|i| (i, 1)).collect();
    let dense_b_ids: Vec<(u32, u32)> = (0..2000).map(|i| (i, 1)).collect();
    let dense_a = postings_from(&dense_a_ids);
    let dense_b = postings_from(&dense_b_ids);
    let posts2 = vec![
        TokProbe {
            seg: None,
            live: Some(&dense_a),
            staged: Vec::new(),
            tombstones: &empty,
        },
        TokProbe {
            seg: None,
            live: Some(&dense_b),
            staged: Vec::new(),
            tombstones: &empty,
        },
    ];
    assert!(should_use_zipper(&posts2, 0, 2000));
}

/// The 20-token df≈500k `title_ngram` shape that collapsed the 500k cell:
/// every other token is as dense as the driver, so the zipper must win
/// regardless of token count (the old single-`log2` model flipped to the
/// binary-search probe at ≥ `log2(len)` tokens).
#[test]
fn should_use_zipper_holds_for_many_equally_dense_tokens() {
    let empty = RoaringBitmap::new();
    let ids: Vec<(u32, u32)> = (0..4096).map(|i| (i, 1)).collect();
    let postings: Vec<Postings> = (0..20).map(|_| postings_from(&ids)).collect();
    let posts: Vec<TokProbe<'_>> = postings
        .iter()
        .map(|p| TokProbe {
            seg: None,
            live: Some(p),
            staged: Vec::new(),
            tombstones: &empty,
        })
        .collect();
    assert!(should_use_zipper(&posts, 0, 4096));
}

/// Randomized equivalence for the GENERAL zipper lane (a probe whose
/// postings are spread over segment / live / staged sources with
/// tombstones — the real post-checkpoint shape) against the naive oracle
/// fed each probe's own `iter_active()` stream, which is the already
/// tested definition of a token's active postings. Even trials are dense
/// (zipper), odd trials sparse (binary-search probe); both must agree.
#[test]
fn build_and_ranked_general_lane_agrees_with_reference() {
    let mut rng = Rng(0x1319_8A2E_0370_7344);
    for trial in 0..120u32 {
        let universe = 8 + rng.below(160);
        let n_tokens = 2 + rng.below(4); // 2..=5 tokens
        let dense = trial % 2 == 0;
        let threshold = if dense { 2 } else { 7 };

        let mut idx = TextIndex {
            doc_count: universe as u64,
            total_doc_len: universe as u64,
            ..Default::default()
        };
        for _ in 0..universe {
            idx.lens.push(1 + rng.below(20));
        }
        let mut tomb = RoaringBitmap::new();
        for id in 0..universe {
            if rng.below(6) == 0 {
                tomb.insert(id);
            }
        }

        struct Src {
            seg: Option<std::sync::Arc<(Vec<u32>, Vec<u32>)>>,
            live: Option<Postings>,
            staged: Vec<(u32, u32)>,
        }
        let mut srcs: Vec<Src> = Vec::new();
        for _ in 0..n_tokens {
            let mut seg: Vec<(u32, u32)> = Vec::new();
            let mut live: Vec<(u32, u32)> = Vec::new();
            let mut staged: Vec<(u32, u32)> = Vec::new();
            for id in 0..universe {
                if rng.below(threshold) != 0 {
                    continue;
                }
                // Bit 0 → segment, bit 1 → live, bit 2 → staged; at
                // least one source, overlaps allowed (precedence case).
                let mask = 1 + rng.below(7);
                if mask & 1 != 0 {
                    seg.push((id, 1 + rng.below(9)));
                }
                if mask & 2 != 0 {
                    live.push((id, 1 + rng.below(9)));
                }
                if mask & 4 != 0 {
                    staged.push((id, 1 + rng.below(9)));
                }
            }
            let seg = (!seg.is_empty() || rng.below(3) == 0).then(|| {
                std::sync::Arc::new((
                    seg.iter().map(|&(id, _)| id).collect::<Vec<u32>>(),
                    seg.iter().map(|&(_, tf)| tf).collect::<Vec<u32>>(),
                ))
            });
            let live = (!live.is_empty()).then(|| postings_from(&live));
            srcs.push(Src { seg, live, staged });
        }
        let posts: Vec<TokProbe<'_>> = srcs
            .iter()
            .map(|s| TokProbe {
                seg: s.seg.clone(),
                live: s.live.as_ref(),
                staged: s.staged.clone(),
                tombstones: &tomb,
            })
            .collect();
        if posts.iter().any(|p| p.definitely_absent()) {
            continue;
        }

        let effective: Vec<Vec<(u32, u32)>> =
            posts.iter().map(|p| p.iter_active().collect()).collect();
        let n = universe as f32;
        let idfs: Vec<f32> = effective
            .iter()
            .map(|p| {
                let df = p.len() as f32;
                ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
            })
            .collect();
        let avgdl = idx.total_doc_len as f32 / idx.doc_count.max(1) as f32;
        let drive = (0..posts.len())
            .min_by_key(|&i| effective[i].len())
            .unwrap();
        let drive_len = effective[drive].len();

        let entries = build_and_ranked(AndRankInput {
            idx: &idx,
            posts: &posts,
            idfs: &idfs,
            drive,
            drive_len,
            avgdl,
        });
        let want = reference_and_scores(&idx, &effective, &idfs, avgdl);
        let got: BTreeMap<u32, f32> = entries.into_iter().collect();
        assert_eq!(
            got,
            want,
            "trial {trial} (dense={dense}, zipper={}): general lane diverged",
            should_use_zipper(&posts, drive, drive_len)
        );
    }
}

/// TIMING HARNESS (not a correctness gate) — measures the two cold-path
/// stages `build_and_ranked` used to spend ~550-800ms in: the probe/score
/// loop, and (via `ensure_sorted_prefix`) the top-k extraction that used
/// to be an eager full sort. 500k docs, ~20 tokens, all matching every
/// doc (df≈500k each) — the `title_ngram` "durable search" `op: and`
/// fixture from the perf regression. Run explicitly in release:
/// `cargo test --release -p lumen --lib -- --ignored \
///   timing_and_match_500k_all_docs_match --nocapture`.
#[test]
#[ignore = "timing harness, not a correctness gate — run explicitly in release"]
fn timing_and_match_500k_all_docs_match() {
    const N_DOCS: u32 = 500_000;
    const N_TOKENS: usize = 20;

    let mut idx = TextIndex {
        doc_count: N_DOCS as u64,
        ..Default::default()
    };
    let mut rng = Rng(0xA076_1D64_78BD_642F);
    let mut postings: Vec<Postings> = (0..N_TOKENS).map(|_| Postings::default()).collect();
    let mut total_len: u64 = 0;
    for id in 0..N_DOCS {
        // Every doc carries every token (df≈500k each), tf/doc_len varying
        // in a narrow band — mirrors the "durable search token " × N
        // title fixture, whose scores cluster heavily.
        let doc_len = 250 + rng.below(36);
        total_len += doc_len as u64;
        idx.lens.push(doc_len);
        for p in postings.iter_mut() {
            p.upsert(id, 1 + rng.below(3));
        }
    }
    idx.total_doc_len = total_len;
    for (i, p) in postings.into_iter().enumerate() {
        idx.tokens.insert(format!("tok{i}"), p);
    }

    let n = N_DOCS as f32;
    let avgdl = idx.total_doc_len as f32 / idx.doc_count as f32;
    let posts: Vec<TokProbe<'_>> = (0..N_TOKENS)
        .map(|i| idx.tok_probe(&format!("tok{i}")))
        .collect();
    let dfs: Vec<usize> = posts.iter().map(|p| p.iter_active().count()).collect();
    let idfs: Vec<f32> = dfs
        .iter()
        .map(|&df| {
            let df = df as f32;
            ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
        })
        .collect();
    let drive = (0..posts.len()).min_by_key(|&i| dfs[i]).unwrap_or(0);

    let t0 = std::time::Instant::now();
    let entries = build_and_ranked(AndRankInput {
        idx: &idx,
        posts: &posts,
        idfs: &idfs,
        drive,
        drive_len: dfs[drive],
        avgdl,
    });
    let probe_and_score = t0.elapsed();

    let interner = interner_with(N_DOCS);
    let mut state = MatchRankCache {
        entries,
        sorted_len: 0,
    };
    let t1 = std::time::Instant::now();
    ensure_sorted_prefix(&mut state, 10, &interner);
    let topk_extract = t1.elapsed();

    eprintln!(
        "timing_and_match_500k_all_docs_match: probe+score={:?} topk_extract={:?} total={:?}",
        probe_and_score,
        topk_extract,
        probe_and_score + topk_extract
    );
    assert!(
        probe_and_score + topk_extract < std::time::Duration::from_millis(50),
        "cold 500k AND match took {:?}, target < 50ms",
        probe_and_score + topk_extract
    );
}
