use crate::persistence::infrastructure::composed_segment::text_reader::retain_uncovered_sorted;
use roaring::RoaringBitmap;

fn naive_uncovered(ids: &[u32], tfs: &[u32], covered: &RoaringBitmap) -> (Vec<u32>, Vec<u32>) {
    ids.iter()
        .zip(tfs)
        .filter(|(id, _)| !covered.contains(**id))
        .map(|(&id, &tf)| (id, tf))
        .unzip()
}

#[test]
fn retain_uncovered_sorted_matches_naive_filter_on_both_paths() {
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(0x4246);
    for round in 0..400 {
        let n = rng.gen_range(0..300usize);
        let mut ids: Vec<u32> = (0..n).map(|_| rng.gen_range(0..1200u32)).collect();
        ids.sort_unstable();
        ids.dedup();
        let tfs: Vec<u32> = ids.iter().map(|_| rng.gen_range(1..9u32)).collect();
        let mut covered = RoaringBitmap::new();
        // Sparse rounds exercise the galloping path, dense rounds the lockstep one.
        let count = if round % 2 == 0 {
            rng.gen_range(0..4u32)
        } else {
            rng.gen_range(0..400u32)
        };
        for _ in 0..count {
            let from_posting = !ids.is_empty() && rng.gen_bool(0.7);
            let id = if from_posting {
                ids[rng.gen_range(0..ids.len())]
            } else {
                rng.gen_range(0..1400u32)
            };
            covered.insert(id);
        }
        let (mut got_ids, mut got_tfs) = (Vec::new(), Vec::new());
        retain_uncovered_sorted(&ids, &tfs, &covered, &mut got_ids, &mut got_tfs);
        assert_eq!(
            (got_ids, got_tfs),
            naive_uncovered(&ids, &tfs, &covered),
            "round {round} n {n} covered {count}"
        );
    }
    let ids = vec![5, 9, 20, 21, 700];
    let tfs = vec![1, 2, 3, 4, 5];
    let (mut a, mut b) = (Vec::new(), Vec::new());
    retain_uncovered_sorted(
        &ids,
        &tfs,
        &RoaringBitmap::from_iter([5u32, 700, 9000]),
        &mut a,
        &mut b,
    );
    assert_eq!((a, b), (vec![9, 20, 21], vec![2, 3, 4]));
    let (mut a, mut b) = (Vec::new(), Vec::new());
    retain_uncovered_sorted(
        &ids,
        &tfs,
        &RoaringBitmap::from_iter(ids.iter().copied()),
        &mut a,
        &mut b,
    );
    assert!(a.is_empty() && b.is_empty());
}
