use crate::persistence::infrastructure::composed_segment::tests::text;
use crate::persistence::infrastructure::composed_segment::{
    ComposedSegmentReader, DeltaLayer, TextPostingAt,
};
use crate::persistence::infrastructure::segment::SegmentReader;
use roaring::RoaringBitmap;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

const MERGE_TOKENS: [&str; 3] = ["shared", "rare", "absent-from-base"];

fn random_text_rows(rng: &mut impl rand::Rng, n: usize) -> Vec<Option<Vec<(&'static str, u32)>>> {
    (0..n)
        .map(|_| {
            if rng.gen_bool(0.15) {
                return None;
            }
            let mut row = Vec::new();
            for (index, token) in MERGE_TOKENS.iter().enumerate() {
                let keep = match index {
                    0 => rng.gen_bool(0.8),
                    1 => rng.gen_bool(0.2),
                    _ => rng.gen_bool(0.3),
                };
                if keep {
                    row.push((*token, rng.gen_range(1..12u32)));
                }
            }
            Some(row)
        })
        .collect()
}

fn text_rows(path: &Path, rows: &[Option<Vec<(&str, u32)>>]) -> Arc<SegmentReader> {
    let refs: Vec<Option<&[(&str, u32)]>> = rows.iter().map(|r| r.as_deref()).collect();
    text(path, &refs)
}

fn assert_merge_matches_reference(view: &ComposedSegmentReader, label: &str) {
    for token in MERGE_TOKENS.iter().chain(["never-written"].iter()) {
        let fast = view.merge_text_postings(token).map(|p| (*p).clone());
        let reference = view
            .merge_text_postings_reference(token)
            .map(|p| (*p).clone());
        assert_eq!(fast, reference, "{label} token {token}");
        let cached = view.text_postings_arc(token).map(|p| (*p).clone());
        assert_eq!(
            cached, reference,
            "{label} token {token} via text_postings_arc"
        );
        assert_eq!(
            view.text_token_df(token),
            reference.as_ref().map_or(0, |p| p.0.len()),
            "{label} token {token} df"
        );
        if let Some((ids, tfs)) = &reference {
            assert!(
                ids.windows(2).all(|w| w[0] < w[1]),
                "{label} token {token} ids sorted unique"
            );
            assert_eq!(ids.len(), tfs.len());
        }
    }
}

/// Fixture-level model of the composition: the newest layer covering an
/// id owns its row; an id no layer covers falls back to the (mapped) base.
fn model_posting(
    token: &str,
    base_ids: &[u32],
    base_rows: &[Option<Vec<(&str, u32)>>],
    layers: &[(Vec<u32>, Vec<Option<Vec<(&str, u32)>>>)],
) -> Option<(Vec<u32>, Vec<u32>)> {
    let tf_in = |row: &Option<Vec<(&str, u32)>>| {
        row.as_ref()
            .and_then(|r| r.iter().find(|(t, _)| *t == token).map(|&(_, tf)| tf))
    };
    let mut out = Vec::new();
    for id in 0..200u32 {
        let owner = layers
            .iter()
            .rev()
            .find_map(|(ids, rows)| ids.iter().position(|&g| g == id).map(|local| &rows[local]));
        let tf = match owner {
            Some(row) => tf_in(row),
            None => base_ids
                .iter()
                .position(|&g| g == id)
                .and_then(|local| tf_in(&base_rows[local])),
        };
        if let Some(tf) = tf {
            out.push((id, tf));
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out.into_iter().unzip())
    }
}

#[test]
fn coverage_driven_text_merge_matches_the_layered_reference() {
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    let dir = tempfile::tempdir().unwrap();
    let mut rng = rand::rngs::StdRng::seed_from_u64(0x0424_6);
    for round in 0..60 {
        let round_dir = dir.path().join(format!("r{round}"));
        std::fs::create_dir_all(&round_dir).unwrap();
        let n_base = rng.gen_range(1..48usize);
        let base_rows = random_text_rows(&mut rng, n_base);
        let base = text_rows(&round_dir.join("base"), &base_rows);
        let mapped = round % 3 != 0;
        let base_ids: Vec<u32> = if mapped {
            let mut pool: Vec<u32> = (0..140).collect();
            pool.shuffle(&mut rng);
            let mut ids: Vec<u32> = pool[..n_base].to_vec();
            if round % 6 == 1 {
                ids.sort_unstable();
            }
            ids
        } else {
            (0..n_base as u32).collect()
        };
        let mut view = if mapped {
            ComposedSegmentReader::from_mapped_base(base, base_ids.clone()).unwrap()
        } else {
            ComposedSegmentReader::from_base(base)
        };
        let mut model_layers: Vec<(Vec<u32>, Vec<Option<Vec<(&str, u32)>>>)> = Vec::new();
        let check = |view: &ComposedSegmentReader,
                     model_layers: &[(Vec<u32>, Vec<Option<Vec<(&str, u32)>>>)],
                     label: &str| {
            assert_merge_matches_reference(view, label);
            for token in MERGE_TOKENS {
                assert_eq!(
                    view.text_postings_arc(token).map(|p| (*p).clone()),
                    model_posting(token, &base_ids, &base_rows, model_layers),
                    "{label} token {token} vs fixture model"
                );
            }
        };
        check(
            &view,
            &model_layers,
            &format!("round {round} layers 0 mapped {mapped}"),
        );
        let layers = rng.gen_range(0..=5usize);
        for layer in 0..layers {
            let m = rng.gen_range(1..=14usize);
            let mut pool: Vec<u32> = (0..140).collect();
            pool.shuffle(&mut rng);
            let mut ids: Vec<u32> = pool[..m].to_vec();
            if rng.gen_bool(0.5) {
                ids.sort_unstable();
            }
            let rows = random_text_rows(&mut rng, m);
            let reader = text_rows(&round_dir.join(format!("l{layer}")), &rows);
            view = view.with_delta(reader, ids.clone()).unwrap();
            model_layers.push((ids, rows));
            check(
                &view,
                &model_layers,
                &format!("round {round} layers {} mapped {mapped}", layer + 1),
            );
        }
    }
}

#[test]
fn coverage_driven_text_merge_gallops_over_a_wide_base() {
    // Coverage far sparser than the base posting (|C| · 16 < df) takes the
    // galloping path; the hidden rows and the layer rows must still land
    // exactly where the reference puts them.
    let dir = tempfile::tempdir().unwrap();
    let base_rows: Vec<Option<Vec<(&str, u32)>>> = (0..700u32)
        .map(|i| Some(vec![("shared", 1 + i % 5)]))
        .collect();
    let base = text_rows(&dir.path().join("base"), &base_rows);
    let older = text_rows(
        &dir.path().join("older"),
        &[Some(vec![("shared", 40)]), None, Some(vec![])],
    );
    let newer = text_rows(
        &dir.path().join("newer"),
        &[Some(vec![("shared", 50)]), Some(vec![("shared", 51)])],
    );
    let view = ComposedSegmentReader::from_base(base)
        .with_delta(older, vec![10, 699, 300])
        .unwrap()
        .with_delta(newer, vec![10, 900])
        .unwrap();
    assert_merge_matches_reference(&view, "wide base");
    let p = view.text_postings_arc("shared").unwrap();
    assert_eq!(p.0.len(), 700 - 3 + 2);
    let tf_of = |id: u32| p.0.binary_search(&id).ok().map(|i| p.1[i]);
    assert_eq!(tf_of(10), Some(50));
    assert_eq!(tf_of(900), Some(51));
    assert_eq!(tf_of(300), None);
    assert_eq!(tf_of(699), None);
    assert_eq!(tf_of(11), Some(1 + 11 % 5));
}

/// `text_posting_at` (#4246) against the materialized composition: on a
/// cold view every token resolves `Sparse` with the exact df of the
/// un-hidden ids and the tf of every un-hidden candidate, for dense,
/// mapped and layered compositions alike; once `text_postings_arc` has
/// made the posting resident the same call reports `Cached` and never
/// streams. A token no source carries is `None` on a dense base and an
/// empty `Sparse` on a composed one — both mean "no posting".
#[test]
fn sparse_text_posting_at_matches_the_materialized_composition() {
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    let dir = tempfile::tempdir().unwrap();
    let mut rng = rand::rngs::StdRng::seed_from_u64(0x4246_0003);
    let mut sparse_seen = 0usize;
    for round in 0..60 {
        let round_dir = dir.path().join(format!("r{round}"));
        std::fs::create_dir_all(&round_dir).unwrap();
        let n_base = rng.gen_range(1..48usize);
        let base_rows = random_text_rows(&mut rng, n_base);
        let base = text_rows(&round_dir.join("base"), &base_rows);
        let mapped = round % 3 != 0;
        let mut view = if mapped {
            let mut pool: Vec<u32> = (0..140).collect();
            pool.shuffle(&mut rng);
            let mut ids: Vec<u32> = pool[..n_base].to_vec();
            if round % 6 == 1 {
                ids.sort_unstable();
            }
            ComposedSegmentReader::from_mapped_base(base, ids).unwrap()
        } else {
            ComposedSegmentReader::from_base(base)
        };
        let layers = if round % 3 == 0 && round % 2 == 0 {
            0
        } else {
            rng.gen_range(0..=5usize)
        };
        for layer in 0..layers {
            let m = rng.gen_range(1..=14usize);
            let mut pool: Vec<u32> = (0..140).collect();
            pool.shuffle(&mut rng);
            let mut ids: Vec<u32> = pool[..m].to_vec();
            if rng.gen_bool(0.5) {
                ids.sort_unstable();
            }
            let rows = random_text_rows(&mut rng, m);
            let reader = text_rows(&round_dir.join(format!("l{layer}")), &rows);
            view = view.with_delta(reader, ids).unwrap();
        }
        let hidden_set: RoaringBitmap = (0..200u32).filter(|_| rng.gen_bool(0.25)).collect();
        let mut candidates: Vec<u32> = (0..200u32).filter(|_| rng.gen_bool(0.1)).collect();
        if rng.gen_bool(0.2) {
            candidates.clear();
        }
        let label = format!("round {round} mapped {mapped} layers {layers}");
        for token in MERGE_TOKENS.iter().chain(["never-indexed"].iter()) {
            // Cold: nothing resident yet, so the answer must be streamed.
            let cold = view.text_posting_at(token, &candidates, |id| hidden_set.contains(id));
            let full = view.text_postings_arc(token);
            match &full {
                None => match cold {
                    None => assert!(
                        view.dense_base_only(),
                        "{label} {token}: None only on a dense base"
                    ),
                    Some(TextPostingAt::Sparse { df, ref hits }) => {
                        assert_eq!((df, hits.len()), (0, 0), "{label} {token}: absent token");
                    }
                    Some(TextPostingAt::Cached(_)) => {
                        panic!("{label} {token}: cold view reported Cached")
                    }
                },
                Some(full) => {
                    let want_df = full
                        .0
                        .iter()
                        .filter(|id| !hidden_set.contains(**id))
                        .count();
                    let want_hits: Vec<(u32, u32)> = full
                        .0
                        .iter()
                        .zip(&full.1)
                        .filter(|(id, _)| {
                            !hidden_set.contains(**id) && candidates.binary_search(id).is_ok()
                        })
                        .map(|(&id, &tf)| (id, tf))
                        .collect();
                    match cold {
                        Some(TextPostingAt::Sparse { df, hits }) => {
                            assert_eq!(df, want_df, "{label} {token}: df");
                            assert_eq!(hits, want_hits, "{label} {token}: candidate hits");
                            sparse_seen += 1;
                        }
                        other => panic!("{label} {token}: expected Sparse, got {other:?}"),
                    }
                    // Warm: the resident posting is handed back untouched.
                    match view.text_posting_at(token, &candidates, |id| hidden_set.contains(id)) {
                        Some(TextPostingAt::Cached(hit)) => assert!(Arc::ptr_eq(&hit, full), "{label} {token}: cached arc"),
                        other => panic!("{label} {token}: expected Cached after text_postings_arc, got {other:?}"),
                    }
                }
            }
        }
    }
    assert!(
        sparse_seen > 100,
        "the fixture must exercise the sparse path ({sparse_seen})"
    );
}

#[test]
fn coverage_driven_text_merge_keeps_torn_map_as_none() {
    let dir = tempfile::tempdir().unwrap();
    let base = text(
        &dir.path().join("base"),
        &[Some(&[("shared", 1)]), Some(&[("shared", 2)])],
    );
    let delta = text(
        &dir.path().join("delta"),
        &[Some(&[("shared", 9)]), Some(&[("shared", 8)])],
    );
    let torn = Arc::new(DeltaLayer {
        reader: delta,
        ids: vec![1],
        local_by_global: BTreeMap::from([(1, 0)]),
        coverage: RoaringBitmap::from_iter([1u32]),
        private: false,
    });
    let view = ComposedSegmentReader {
        base,
        base_map: None,
        layers: vec![torn],
        n_docs: 2,
        has_catalog_base: true,
        distinct_terms: None,
        query_cache: Arc::default(),
    };
    assert!(view.merge_text_postings_reference("shared").is_none());
    assert!(view.merge_text_postings("shared").is_none());
    assert!(view.text_postings_arc("shared").is_none());
}
