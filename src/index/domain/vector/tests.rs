use crate::index::domain::vector::flat_cpu_index::FlatCpuIndex;
use crate::index::domain::vector::hnsw_cpu_index::HnswCpuIndex;
use crate::index::domain::vector::VectorIndex;
use crate::shared_kernel::types::schema::{VectorMetric, VectorQuantize, VectorSpec};

#[test]
fn preparation_codebook_snapshot_is_copy_sized_for_raw_and_sq_backends() {
    for index in [
        Box::new(HnswCpuIndex::new(spec(3, VectorMetric::L2, None))) as Box<dyn VectorIndex>,
        Box::new(FlatCpuIndex::new(spec(3, VectorMetric::L2, None))) as Box<dyn VectorIndex>,
    ] {
        assert!(index
            .checkpoint_codebook_for_preparation()
            .unwrap()
            .is_none());
    }
    for index in [
        Box::new(HnswCpuIndex::new(spec(
            3,
            VectorMetric::L2,
            Some(VectorQuantize::Sq),
        ))) as Box<dyn VectorIndex>,
        Box::new(FlatCpuIndex::new(spec(
            3,
            VectorMetric::L2,
            Some(VectorQuantize::Sq),
        ))) as Box<dyn VectorIndex>,
    ] {
        let initial = index
            .checkpoint_codebook_for_preparation()
            .unwrap()
            .unwrap();
        assert_eq!(initial.dim, 3);
        assert!(initial.min.is_infinite() && initial.min.is_sign_positive());
        assert!(initial.max.is_infinite() && initial.max.is_sign_negative());
        index.add("first", &[-2.0, 0.0, 5.0]).unwrap();
        let snapshot = index
            .checkpoint_codebook_for_preparation()
            .unwrap()
            .unwrap();
        assert_eq!((snapshot.min, snapshot.max, snapshot.dim), (-2.0, 5.0, 3));
        let mut widened = snapshot;
        widened.widen(&[-20.0, 0.0, 20.0]);
        assert_eq!(
            index
                .checkpoint_codebook_for_preparation()
                .unwrap()
                .unwrap()
                .min,
            -2.0
        );
        assert_eq!(
            index
                .checkpoint_codebook_for_preparation()
                .unwrap()
                .unwrap()
                .max,
            5.0
        );
        index.add("later", &[10.0, 0.0, 20.0]).unwrap();
        let later = index
            .checkpoint_codebook_for_preparation()
            .unwrap()
            .unwrap();
        assert_eq!((later.min, later.max, later.dim), (-2.0, 20.0, 3));
    }
}

fn rand_vec(rng: &mut rand::rngs::StdRng, dim: usize) -> Vec<f32> {
    use rand::Rng;
    (0..dim).map(|_| rng.gen_range(-1.0_f32..1.0)).collect()
}

fn spec(dim: u32, metric: VectorMetric, q: Option<VectorQuantize>) -> VectorSpec {
    VectorSpec {
        dim,
        metric,
        backend: crate::shared_kernel::types::schema::VectorBackend::HnswCpu,
        quantize: q,
    }
}

fn normalize(mut v: Vec<f32>) -> Vec<f32> {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-12);
    for x in &mut v {
        *x /= norm;
    }
    v
}

mod distance;

mod flat_checkpoint;

mod flat_compaction;

mod hnsw;

mod quantize;

mod snapshot;
