use crate::index::domain::vector::hnsw_cpu_index::HnswCpuIndex;
use crate::index::domain::vector::quantize::{decode_sq, encode_sq, ScalarCodebook};
use crate::index::domain::vector::tests::{rand_vec, spec};
use crate::index::domain::vector::VectorIndex;
use crate::shared_kernel::types::schema::{VectorMetric, VectorQuantize};

#[test]
fn sq_codec_round_trip_within_tolerance() {
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let dim = 128;
    let v = rand_vec(&mut rng, dim);
    let mut cb = ScalarCodebook::empty(dim);
    cb.widen(&v);
    let bytes = encode_sq(&v, &cb);
    assert_eq!(bytes.len(), dim);
    let back = decode_sq(&bytes, &cb);
    let span = cb.max - cb.min;
    let tol = span / 255.0;
    let mean_err: f32 = v
        .iter()
        .zip(back.iter())
        .map(|(a, b)| (a - b).abs())
        .sum::<f32>()
        / dim as f32;
    assert!(
        mean_err <= tol,
        "mean SQ error {mean_err} > 1/255 of range {tol}"
    );
}

#[test]
fn sq_round_trip_through_hnsw_index() {
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::seed_from_u64(5);
    let idx = HnswCpuIndex::new(spec(64, VectorMetric::L2, Some(VectorQuantize::Sq)));
    let mut data = Vec::new();
    for i in 0..100 {
        let v = rand_vec(&mut rng, 64);
        idx.add(&format!("e{i}"), &v).unwrap();
        data.push((format!("e{i}"), v));
    }
    // Query the index with one of the inserted vectors; the index
    // should still return that vector as the top hit even though
    // storage is u8-quantized.
    let (eid, v) = &data[10];
    let hits = idx.search_knn(v, 1).unwrap();
    assert_eq!(hits[0].0, *eid);
}

#[test]
fn one_item_insert_search_and_reopen_works_with_and_without_sq() {
    for quantize in [None, Some(VectorQuantize::Sq)] {
        let spec = spec(3, VectorMetric::L2, quantize);
        let idx = HnswCpuIndex::new(spec);
        let vector = [1.0_f32, 2.0, 3.0];
        idx.add("one", &vector).unwrap();
        assert_eq!(idx.search_knn(&vector, 1).unwrap()[0].0, "one");

        let dir = tempfile::tempdir().unwrap();
        let segment = dir.path().join("vectors.lseg");
        let row_eids = idx.seal_to_segment_prod(&segment).unwrap().unwrap();
        let reader = std::sync::Arc::new(
            crate::persistence::infrastructure::segment::SegmentReader::open(&segment).unwrap(),
        );
        let reopened = HnswCpuIndex::open_from_segment(spec, reader, row_eids).unwrap();
        assert_eq!(reopened.search_knn(&vector, 1).unwrap()[0].0, "one");
    }
}
