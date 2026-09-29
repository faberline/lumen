use crate::index::domain::interner::Interner;
use crate::index::domain::query::rank::{
    cached_match_ranked_page, ensure_sorted_prefix, insert_match_rank_cache_and_page,
    match_rank_cmp, MatchRankCache,
};
use crate::index::domain::text_index::TextIndex;

// A small xorshift PRNG, dependency-free and deterministic (the same
// recipe `tok_probe_tests` uses).
pub(crate) struct Rng(pub(crate) u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    pub(crate) fn below(&mut self, n: u32) -> u32 {
        (self.next() % u64::from(n.max(1))) as u32
    }
}

pub(crate) fn interner_with(n: u32) -> Interner {
    let mut it = Interner::default();
    for i in 0..n {
        it.intern(&format!("eid-{i:06}"));
    }
    it
}

fn reference_full_sort(entries: &[(u32, f32)], interner: &Interner) -> Vec<(u32, f32)> {
    let mut v = entries.to_vec();
    v.sort_by(|a, b| match_rank_cmp(interner, a, b));
    v
}

/// `ensure_sorted_prefix`, called with a growing sequence of out-of-order
/// `k`s (including `k` beyond the total), must always agree with a plain
/// full sort on the requested prefix — this is the paging-cache
/// equivalence the perf fix depends on.
#[test]
fn ensure_sorted_prefix_matches_full_sort_for_every_k() {
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    for trial in 0..60u32 {
        let n_ids = 1 + rng.below(200);
        let interner = interner_with(n_ids);
        // Scores drawn from a small set so many entries tie and the
        // external-id tie-break actually gets exercised.
        let entries: Vec<(u32, f32)> = (0..n_ids)
            .map(|id| (id, rng.below(5) as f32 * 0.25))
            .collect();
        let reference = reference_full_sort(&entries, &interner);
        let mut state = MatchRankCache {
            entries: entries.clone(),
            sorted_len: 0,
        };
        let ks = [
            0usize,
            1,
            3,
            n_ids as usize / 2,
            n_ids as usize,
            n_ids as usize + 10,
        ];
        for &k in &ks {
            let want = k.min(state.entries.len());
            ensure_sorted_prefix(&mut state, want, &interner);
            assert_eq!(
                state.entries[..want],
                reference[..want],
                "trial {trial} k={k}: extended prefix diverged from full-sort reference"
            );
        }
    }
}

/// `insert_match_rank_cache_and_page` then `cached_match_ranked_page` for
/// a growing `k` must serve the SAME underlying cache entry (the extend-
/// in-place path), each page byte-identical to a full-sort reference.
#[test]
fn insert_and_cached_page_round_trip_serves_growing_k() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let n_ids = 500u32;
    let interner = interner_with(n_ids);
    let entries: Vec<(u32, f32)> = (0..n_ids).map(|id| (id, rng.below(7) as f32)).collect();
    let reference = reference_full_sort(&entries, &interner);

    let idx = TextIndex::default();
    let (page, total) =
        insert_match_rank_cache_and_page(&idx, "k".to_string(), entries.clone(), 5, &interner);
    assert_eq!(total, n_ids as u64);
    assert_eq!(page, reference[..5]);

    let (page2, total2) = cached_match_ranked_page(&idx, "k", 50, &interner).unwrap();
    assert_eq!(total2, n_ids as u64);
    assert_eq!(page2, reference[..50]);

    // k beyond total clamps to total and still matches the reference.
    let (page3, total3) =
        cached_match_ranked_page(&idx, "k", n_ids as usize + 25, &interner).unwrap();
    assert_eq!(total3, n_ids as u64);
    assert_eq!(page3, reference);
}
