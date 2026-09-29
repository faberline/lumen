use crate::index::domain::vector::distance::distance;
use crate::index::domain::vector::hnsw_cpu_index::HnswCpuIndex;
use crate::index::domain::vector::tests::{normalize, rand_vec, spec};
use crate::index::domain::vector::VectorIndex;
use crate::shared_kernel::types::schema::VectorMetric;

// -----------------------------------------------------------------
// Metric directionality (Contract 1). These assert the *sign* and
// *ordering* of each distance, so a mutated operator in distance() /
// l2_squared() / dot() / cosine_similarity() flips a comparison and
// fails. The score = -distance convention means "closer ⇒ larger
// score" must hold for every metric.
// -----------------------------------------------------------------

#[test]
fn l2_distance_grows_with_separation() {
    let q = [0.0_f32, 0.0, 0.0];
    let near = [1.0_f32, 0.0, 0.0];
    let far = [5.0_f32, 0.0, 0.0];
    let dn = distance(VectorMetric::L2, &q, &near);
    let df = distance(VectorMetric::L2, &q, &far);
    assert!(dn >= 0.0, "L2 distance is non-negative");
    assert!(df > dn, "farther vector must have larger L2 distance");
    // identical vectors → zero distance
    assert!(distance(VectorMetric::L2, &q, &q).abs() < 1e-6);
}

#[test]
fn cosine_distance_smaller_for_aligned_vectors() {
    let q = [1.0_f32, 0.0];
    let aligned = [2.0_f32, 0.0]; // same direction
    let orthogonal = [0.0_f32, 3.0];
    let opposite = [-1.0_f32, 0.0];
    let d_aligned = distance(VectorMetric::Cosine, &q, &aligned);
    let d_orth = distance(VectorMetric::Cosine, &q, &orthogonal);
    let d_opp = distance(VectorMetric::Cosine, &q, &opposite);
    // cosine distance = 1 - cos θ : aligned≈0, orthogonal≈1, opposite≈2
    assert!(d_aligned < d_orth, "aligned closer than orthogonal");
    assert!(d_orth < d_opp, "orthogonal closer than opposite");
    assert!(d_aligned.abs() < 1e-5, "aligned cosine distance ≈ 0");
}

#[test]
fn dot_distance_is_negative_dot_so_larger_dot_is_closer() {
    let q = [1.0_f32, 1.0];
    let high = [2.0_f32, 2.0]; // dot = 4
    let low = [0.5_f32, 0.5]; // dot = 1
    let d_high = distance(VectorMetric::Dot, &q, &high);
    let d_low = distance(VectorMetric::Dot, &q, &low);
    // distance = -dot ; higher dot ⇒ smaller (more negative) distance
    assert!(
        d_high < d_low,
        "higher dot product must be closer (smaller distance)"
    );
    assert!((d_high + 4.0).abs() < 1e-5, "dot distance == -dot");
}

#[test]
fn knn_orders_by_increasing_distance_for_each_metric() {
    // L2 + Cosine accept arbitrary vectors. (Dot's HNSW backend
    // requires unit-normalized input — its directionality is pinned
    // by `dot_distance_is_negative_dot_so_larger_dot_is_closer` at
    // the math level instead.)
    for metric in [VectorMetric::L2, VectorMetric::Cosine] {
        let idx = HnswCpuIndex::new(spec(3, metric, None));
        idx.add("near", &[1.0, 0.0, 0.0]).unwrap();
        idx.add("mid", &[1.0, 1.0, 0.0]).unwrap();
        idx.add("far", &[-1.0, 0.0, 0.0]).unwrap();
        let hits = idx.search_knn(&[1.0, 0.0, 0.0], 3).unwrap();
        // score = -distance ⇒ scores must be non-increasing down the list.
        for w in hits.windows(2) {
            assert!(
                w[0].1 >= w[1].1,
                "metric {metric:?}: scores must be sorted desc, got {hits:?}"
            );
        }
        // the exact-match query vector ("near") must be the top hit.
        assert_eq!(
            hits[0].0, "near",
            "metric {metric:?}: nearest is the query itself"
        );
    }
}

#[test]
fn filtered_knn_returns_nearest_within_allowlist_not_global_topk() {
    // 50 vectors along a 1-D ray: v{i} at distance i from the query.
    // Enough nodes that HNSW recall is reliable (a 3-node graph is
    // randomized enough to flake), and enough that we can deny a
    // prefix longer than the initial over-fetch pool to exercise the
    // widening loop.
    let idx = HnswCpuIndex::new(spec(3, VectorMetric::L2, None));
    let n = 50usize;
    for i in 0..n {
        idx.add(&format!("v{i:02}"), &[i as f32, 0.0, 0.0]).unwrap();
    }
    let query = [0.0_f32, 0.0, 0.0];
    let allowed_from =
        |eid: &str| -> bool { eid.trim_start_matches('v').parse::<usize>().unwrap() >= 20 };

    // Baseline: unfiltered nearest is v00.
    let all = idx.search_knn(&query, 3).unwrap();
    assert_eq!(all[0].0, "v00", "unfiltered nearest is the closest vector");

    // Deny v00..=v19 — a 20-wide prefix, wider than the k*4+k=15
    // initial pool, so a post-filter over the global top-k would
    // return nothing and the widening loop must kick in.
    let k = 3;
    let hits = idx.search_knn_filtered(&query, k, &allowed_from).unwrap();
    assert_eq!(hits.len(), k, "selective filter must not collapse recall");
    for (eid, _) in &hits {
        let i: usize = eid.trim_start_matches('v').parse().unwrap();
        assert!(i >= 20, "denied id {eid} leaked past the allow-list");
    }
    for w in hits.windows(2) {
        assert!(w[0].1 >= w[1].1, "scores sorted desc: {hits:?}");
    }
    // Nearest allowed neighbour ranks first.
    assert_eq!(hits[0].0, "v20", "nearest allowed neighbour leads");

    // Allow-nothing → empty, never an error.
    let none = idx.search_knn_filtered(&query, k, &|_| false).unwrap();
    assert!(none.is_empty(), "empty allow-list yields no hits");
}

#[test]
fn knn_dot_orders_normalized_vectors_by_alignment() {
    // Dot HNSW requires unit-normalized vectors. With those, the
    // most-aligned vector to the query must rank first.
    let idx = HnswCpuIndex::new(spec(2, VectorMetric::Dot, None));
    idx.add("aligned", &[1.0, 0.0]).unwrap();
    let diag = std::f32::consts::FRAC_1_SQRT_2;
    idx.add("diag", &[diag, diag]).unwrap();
    idx.add("orthogonal", &[0.0, 1.0]).unwrap();
    let hits = idx.search_knn(&[1.0, 0.0], 3).unwrap();
    for w in hits.windows(2) {
        assert!(w[0].1 >= w[1].1, "dot scores must be sorted desc: {hits:?}");
    }
    assert_eq!(hits[0].0, "aligned", "most-aligned vector ranks first");
}

#[test]
fn cosine_dot_l2_all_produce_well_formed_results() {
    use rand::SeedableRng;
    for metric in [VectorMetric::Cosine, VectorMetric::Dot, VectorMetric::L2] {
        let mut rng = rand::rngs::StdRng::seed_from_u64(3);
        let idx = HnswCpuIndex::new(spec(32, metric, None));
        for i in 0..50 {
            // `DistDot` in `hnsw_rs` assumes unit-norm inputs
            // (returns `1 - dot` with an assert that the result is
            // non-negative). Normalize for the dot case; the other
            // two work on raw vectors.
            let raw = rand_vec(&mut rng, 32);
            let v = if matches!(metric, VectorMetric::Dot) {
                normalize(raw)
            } else {
                raw
            };
            idx.add(&format!("e{i}"), &v).unwrap();
        }
        let raw_q = rand_vec(&mut rng, 32);
        let q = if matches!(metric, VectorMetric::Dot) {
            normalize(raw_q)
        } else {
            raw_q
        };
        let hits = idx.search_knn(&q, 5).unwrap();
        assert_eq!(hits.len(), 5, "metric {metric:?}");
        for w in hits.windows(2) {
            assert!(w[0].1 >= w[1].1, "metric {metric:?} non-monotone");
        }
    }
}
