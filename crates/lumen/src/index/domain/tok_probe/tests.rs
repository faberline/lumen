use std::collections::BTreeMap;

use roaring::RoaringBitmap;

use crate::index::domain::postings::Postings;
use crate::index::domain::tok_probe::{gallop_to, TokProbe};

fn probe<'a>(
    seg: Option<(&[u32], &[u32])>,
    live: Option<&'a Postings>,
    staged: &[(u32, u32)],
    tombstones: &'a RoaringBitmap,
) -> TokProbe<'a> {
    TokProbe {
        seg: seg.map(|(ids, tfs)| std::sync::Arc::new((ids.to_vec(), tfs.to_vec()))),
        live,
        staged: staged.to_vec(),
        tombstones,
    }
}

fn live_postings(pairs: &[(u32, u32)]) -> Postings {
    Postings::from_sorted(
        pairs.iter().map(|&(id, _)| id).collect(),
        pairs.iter().map(|&(_, tf)| tf).collect(),
    )
}

#[test]
fn segment_only_streams_and_probes_every_entry() {
    let empty = RoaringBitmap::new();
    let p = probe(Some((&[1, 3, 5], &[10, 30, 50])), None, &[], &empty);
    assert!(!p.definitely_absent());
    assert_eq!(
        p.iter_active().collect::<Vec<_>>(),
        vec![(1, 10), (3, 30), (5, 50)]
    );
    assert_eq!(p.tf(3), Some(30));
    assert_eq!(p.tf(4), None);
}

#[test]
fn segment_plus_live_live_overrides_reused_id_and_adds_new_ids() {
    let empty = RoaringBitmap::new();
    // Segment has 1,3; live has 3 (override) and 7 (tail-only new id).
    let live = live_postings(&[(3, 300), (7, 700)]);
    let p = probe(Some((&[1, 3], &[10, 30])), Some(&live), &[], &empty);
    assert_eq!(
        p.iter_active().collect::<Vec<_>>(),
        vec![(1, 10), (3, 300), (7, 700)],
        "live tf must override the reused segment id"
    );
    assert_eq!(p.tf(1), Some(10));
    assert_eq!(p.tf(3), Some(300));
    assert_eq!(p.tf(7), Some(700));
}

#[test]
fn segment_plus_tombstones_drops_pure_segment_id_only() {
    let mut tombstones = RoaringBitmap::new();
    tombstones.insert(3);
    let p = probe(Some((&[1, 3, 5], &[10, 30, 50])), None, &[], &tombstones);
    assert_eq!(
        p.iter_active().collect::<Vec<_>>(),
        vec![(1, 10), (5, 50)],
        "tombstoned pure-segment id must be dropped"
    );
    assert_eq!(p.tf(3), None);
}

#[test]
fn segment_plus_live_plus_tombstones_live_wins_over_a_tombstoned_reused_id() {
    let mut tombstones = RoaringBitmap::new();
    tombstones.insert(3); // reused AND tombstoned base id
    tombstones.insert(5); // pure-segment tombstoned id
    let live = live_postings(&[(3, 999)]);
    let p = probe(
        Some((&[1, 3, 5], &[10, 30, 50])),
        Some(&live),
        &[],
        &tombstones,
    );
    assert_eq!(
        p.iter_active().collect::<Vec<_>>(),
        vec![(1, 10), (3, 999)],
        "a live posting must override a reused base id EVEN when tombstoned; \
             the pure-segment tombstoned id must still be dropped"
    );
    assert_eq!(p.tf(3), Some(999));
    assert_eq!(p.tf(5), None);
}

#[test]
fn staged_row_overrides_both_live_and_segment() {
    let empty = RoaringBitmap::new();
    let live = live_postings(&[(2, 20), (4, 40)]);
    // Staged overrides id 2 (was live) and id 1 (was segment-only); id 9 is
    // staged-only (neither segment nor live).
    let p = probe(
        Some((&[1, 4], &[100, 400])),
        Some(&live),
        &[(1, 111), (2, 222), (9, 999)],
        &empty,
    );
    assert_eq!(
        p.iter_active().collect::<Vec<_>>(),
        vec![(1, 111), (2, 222), (4, 40), (9, 999)],
        "staged must win over both live and segment for a shared id"
    );
    assert_eq!(p.tf(1), Some(111));
    assert_eq!(p.tf(2), Some(222));
    assert_eq!(p.tf(4), Some(40));
    assert_eq!(p.tf(9), Some(999));
}

#[test]
fn absent_token_has_no_source_and_no_matches() {
    let empty = RoaringBitmap::new();
    let p = probe(None, None, &[], &empty);
    assert!(p.definitely_absent());
    assert_eq!(p.iter_active().collect::<Vec<_>>(), Vec::new());
    assert_eq!(p.tf(0), None);
}

/// Reference (`#[cfg(test)]`-only) oracle: the OLD algorithm — materialize
/// a fully-merged `(docids, tfs)` pair via a naive three-way merge, tombstone
/// subtraction, and staged override, matching `tok_postings`'s SEMANTICS
/// without sharing its code — then look up by binary search. `TokProbe`
/// must produce the IDENTICAL `(docid, tf)` set for every randomized fixture.
fn reference_active(
    seg: &Option<(Vec<u32>, Vec<u32>)>,
    live: &Option<Postings>,
    staged: &[(u32, u32)],
    tombstones: &RoaringBitmap,
) -> BTreeMap<u32, u32> {
    let mut merged: BTreeMap<u32, u32> = BTreeMap::new();
    if let Some((ids, tfs)) = seg {
        for (&id, &tf) in ids.iter().zip(tfs.iter()) {
            if !tombstones.contains(id) {
                merged.insert(id, tf);
            }
        }
    }
    if let Some(live) = live {
        for (&id, &tf) in live.docids.iter().zip(live.tfs.iter()) {
            merged.insert(id, tf); // live always overrides, tombstoned or not
        }
    }
    for &(id, tf) in staged {
        merged.insert(id, tf); // staged always overrides
    }
    merged
}

#[test]
fn tok_probe_matches_reference_over_randomized_fixtures() {
    // A small xorshift PRNG so this stays dependency-free and deterministic.
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
    let mut rng = Rng(0x9E3779B97F4A7C15);
    for trial in 0..200u32 {
        let universe = 1 + rng.below(40);
        let mk_ids = |rng: &mut Rng, universe: u32| -> Vec<u32> {
            let mut ids: Vec<u32> = (0..universe).filter(|_| rng.below(3) == 0).collect();
            ids.sort_unstable();
            ids.dedup();
            ids
        };
        let seg_ids = mk_ids(&mut rng, universe);
        let seg: Option<(Vec<u32>, Vec<u32>)> = if rng.below(4) == 0 {
            None
        } else {
            let tfs: Vec<u32> = seg_ids.iter().map(|_| 1 + rng.below(9)).collect();
            Some((seg_ids.clone(), tfs))
        };
        let live_ids = mk_ids(&mut rng, universe);
        let live: Option<Postings> = if rng.below(4) == 0 {
            None
        } else {
            let tfs: Vec<u32> = live_ids.iter().map(|_| 1 + rng.below(9)).collect();
            Some(Postings::from_sorted(live_ids.clone(), tfs))
        };
        let staged_ids = mk_ids(&mut rng, universe);
        let staged: Vec<(u32, u32)> = staged_ids
            .iter()
            .map(|&id| (id, 1 + rng.below(9)))
            .collect();
        let mut tombstones = RoaringBitmap::new();
        for id in 0..universe {
            if rng.below(3) == 0 {
                tombstones.insert(id);
            }
        }

        let want = reference_active(&seg, &live, &staged, &tombstones);
        let p = probe(
            seg.as_ref().map(|(ids, tfs)| (&ids[..], &tfs[..])),
            live.as_ref(),
            &staged,
            &tombstones,
        );
        let got: BTreeMap<u32, u32> = p.iter_active().collect();
        assert_eq!(
            got, want,
            "trial {trial}: iter_active diverged from reference"
        );
        assert_eq!(
            p.active_len(),
            want.len(),
            "trial {trial}: active_len diverged from the exact df"
        );
        for id in 0..universe {
            assert_eq!(
                p.tf(id),
                want.get(&id).copied(),
                "trial {trial}: tf({id}) diverged from reference"
            );
        }
        assert_eq!(
            p.definitely_absent(),
            seg.is_none() && live.is_none() && staged.is_empty(),
            "trial {trial}: definitely_absent diverged"
        );
    }
}

/// The shape `active_len` is built for: a long, dense segment lane (the
/// stop-token at scale) with sparse live/staged overlays and either sparse
/// or heavy tombstones. The galloping count must equal the streaming one.
#[test]
fn active_len_matches_streaming_count_on_long_segments() {
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
    let mut rng = Rng(0xD1B54A32D192ED03);
    for trial in 0..40u32 {
        let universe = 2000 + rng.below(6001);
        let seg_ids: Vec<u32> = (0..universe).filter(|_| rng.below(10) != 0).collect();
        let seg_tfs: Vec<u32> = seg_ids.iter().map(|_| 1 + rng.below(9)).collect();
        let mut live_ids: Vec<u32> = (0..universe).filter(|_| rng.below(97) == 0).collect();
        live_ids.dedup();
        let live = Postings::from_sorted(
            live_ids.clone(),
            live_ids.iter().map(|_| 1 + rng.below(9)).collect(),
        );
        let live = if rng.below(3) == 0 { None } else { Some(live) };
        let staged_ids: Vec<u32> = (0..universe).filter(|_| rng.below(131) == 0).collect();
        let staged: Vec<(u32, u32)> = staged_ids
            .iter()
            .map(|&id| (id, 1 + rng.below(9)))
            .collect();
        let heavy = rng.below(2) == 0;
        let mut tombstones = RoaringBitmap::new();
        for id in 0..universe {
            let hit = if heavy {
                rng.below(2) == 0
            } else {
                rng.below(53) == 0
            };
            if hit {
                tombstones.insert(id);
            }
        }
        let seg = Some((seg_ids, seg_tfs));
        let p = probe(
            seg.as_ref().map(|(ids, tfs)| (&ids[..], &tfs[..])),
            live.as_ref(),
            &staged,
            &tombstones,
        );
        let want = reference_active(&seg, &live, &staged, &tombstones).len();
        assert_eq!(
            p.iter_active().count(),
            want,
            "trial {trial}: streaming count"
        );
        assert_eq!(
            p.active_len(),
            want,
            "trial {trial}: galloping count (heavy={heavy})"
        );
    }
    // Degenerate lanes: empty segment, and a segment fully tombstoned.
    let empty = RoaringBitmap::new();
    let p = probe(Some((&[], &[])), None, &[], &empty);
    assert_eq!(p.active_len(), 0);
    let all: RoaringBitmap = (0..100u32).collect();
    let ids: Vec<u32> = (0..100).collect();
    let tfs = vec![1u32; 100];
    let p = probe(Some((&ids, &tfs)), None, &[(5, 2), (200, 3)], &all);
    assert_eq!(p.active_len(), 2);
    assert_eq!(p.iter_active().count(), 2);
}

/// `gallop_to` must land on exactly the `partition_point` position for
/// every ascending target sequence, from any starting position.
#[test]
fn gallop_to_matches_partition_point() {
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
    let mut rng = Rng(0x2545F4914F6CDD1D);
    for trial in 0..300u32 {
        let n = rng.below(600);
        let mut ids: Vec<u32> = (0..n).map(|_| rng.below(5000)).collect();
        ids.sort_unstable();
        ids.dedup();
        let mut targets: Vec<u32> = (0..rng.below(80)).map(|_| rng.below(5200)).collect();
        targets.sort_unstable();
        let mut pos = 0usize;
        for &t in &targets {
            let hit = gallop_to(&ids, &mut pos, t);
            let want = ids.partition_point(|&x| x < t);
            assert_eq!(pos, want, "trial {trial}: position for target {t}");
            assert_eq!(hit, ids.get(want) == Some(&t), "trial {trial}: hit for {t}");
        }
    }
    let ids = [3u32, 8, 8, 20];
    let mut pos = 0;
    assert!(!gallop_to(&ids, &mut pos, 0));
    assert_eq!(pos, 0);
    assert!(gallop_to(&ids, &mut pos, 3));
    assert!(!gallop_to(&ids, &mut pos, 21));
    assert_eq!(pos, 4);
    assert!(!gallop_to(&ids, &mut pos, 99));
    assert_eq!(pos, 4);
}
