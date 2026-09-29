use crate::index::domain::fast_hash::FastHashMap;
use crate::index::domain::postings::{SparsePosting, TokPostings};
use crate::index::domain::query::prepared_match::PreparedMatch;
use crate::index::domain::text_index::tests::{sparse_candidates, sparse_fixture};
use crate::shared_kernel::types::query::MatchOp;

/// `resolve_at` yields bit-identical BM25 scores to `resolve` for every
/// candidate — repeated tokens, AND and OR — and memoizes per distinct
/// token (one `Sparse` `Arc` shared across the repeats).
#[test]
fn resolve_at_scores_are_bit_identical_to_resolve() {
    let dir = tempfile::tempdir().unwrap();
    for trial in 0..30u64 {
        let fx = sparse_fixture(dir.path(), 0x4246_1000 + trial, trial % 3 == 0);
        let candidates = sparse_candidates(fx.universe, trial * 104729 + 3);
        let tokens: Vec<String> = [
            "alpha", "beta", "alpha", "gamma", "missing", "beta", "delta", "alpha",
        ]
        .iter()
        .take(2 + (trial % 7) as usize)
        .map(|t| t.to_string())
        .collect();
        for op in [MatchOp::And, MatchOp::Or] {
            let full = PreparedMatch::resolve(&fx.idx, &tokens, op);
            let sparse = PreparedMatch::resolve_at(&fx.idx, &tokens, op, &candidates);
            let label = format!("trial {trial} op {op:?} tokens {tokens:?}");
            if candidates.is_empty() {
                assert!(sparse.is_none(), "{label}: no candidates ⇒ None");
                continue;
            }
            let (full, sparse) = match (full, sparse) {
                (Some(f), Some(s)) => (f, s),
                (f, s) => panic!("{label}: presence diverged {} {}", f.is_some(), s.is_some()),
            };
            assert_eq!(sparse.per_token.len(), tokens.len());
            for (i, (f, s)) in full.per_token.iter().zip(&sparse.per_token).enumerate() {
                assert_eq!(f.is_some(), s.is_some(), "{label}: token {i} presence");
                if let (Some((_, fi)), Some((_, si))) = (f, s) {
                    assert_eq!(fi.to_bits(), si.to_bits(), "{label}: token {i} idf");
                }
            }
            let mut arcs: FastHashMap<&str, *const SparsePosting> = FastHashMap::default();
            for (tok, entry) in tokens.iter().zip(&sparse.per_token) {
                if let Some((TokPostings::Sparse(p), _)) = entry {
                    let ptr = std::sync::Arc::as_ptr(p);
                    assert_eq!(
                        *arcs.entry(tok.as_str()).or_insert(ptr),
                        ptr,
                        "{label}: {tok} memoized"
                    );
                }
            }
            for &id in &candidates {
                let want = full.score(&fx.idx, id).map(f32::to_bits);
                let got = sparse.score(&fx.idx, id).map(f32::to_bits);
                assert_eq!(got, want, "{label}: score({id})");
            }
        }
    }
}
