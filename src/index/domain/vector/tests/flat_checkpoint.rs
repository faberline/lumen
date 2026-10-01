use std::collections::HashMap;
use std::sync::Arc;

use crate::index::domain::vector::flat_cpu_index::FlatCpuIndex;
use crate::index::domain::vector::VectorIndex;
use crate::shared_kernel::types::schema::{VectorMetric, VectorSpec};

#[test]
fn flat_base_publication_retires_both_payload_stores_and_keeps_newer_changes() {
    let spec = VectorSpec {
        dim: 2,
        metric: VectorMetric::L2,
        backend: crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
        quantize: None,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("base.lseg");
    crate::persistence::infrastructure::segment::vector_writer::write_vector_segment(
        &path,
        1,
        2,
        &[Some(&[0.0, 0.0]), Some(&[1.0, 1.0]), Some(&[2.0, 2.0])],
    )
    .unwrap();
    let index = FlatCpuIndex::new(spec);
    for (eid, value) in [("ack", 0.0), ("updated", 1.0), ("deleted", 2.0)] {
        index.add(eid, &[value, value]).unwrap();
    }
    index.add("updated", &[3.0, 3.0]).unwrap();
    index.remove("deleted").unwrap();
    index.add("new", &[4.0, 4.0]).unwrap();
    index
        .install_checkpoint_base(
            Arc::new(
                crate::persistence::infrastructure::segment::SegmentReader::open(&path).unwrap(),
            ),
            &["ack".into(), "updated".into(), "deleted".into()],
            &HashMap::from([
                ("ack".into(), true),
                ("updated".into(), false),
                ("deleted".into(), false),
                ("new".into(), false),
            ]),
        )
        .unwrap();
    let inner = index.inner.lock().unwrap();
    assert!(
        !inner.store.raw.contains_key("ack"),
        "acknowledged base vector must leave VectorStore"
    );
    assert_eq!(inner.store.len(), 2);
    assert_eq!(inner.flat.as_ref().unwrap().data.len(), 2);
    drop(inner);
    assert_eq!(index.resident_vector_payload_rows(), 2);
    assert_eq!(index.checkpoint_resident_bytes(), Some(26));
    assert_eq!(
        index.checkpoint_vector("ack").unwrap(),
        Some(vec![0.0, 0.0])
    );
    assert_eq!(
        index.checkpoint_vector("updated").unwrap(),
        Some(vec![3.0, 3.0])
    );
    assert_eq!(index.checkpoint_vector("deleted").unwrap(), None);
    assert_eq!(
        index.checkpoint_vector("new").unwrap(),
        Some(vec![4.0, 4.0])
    );
    index
        .seal_to_segment_prod(&dir.path().join("next.lseg"))
        .unwrap();
    let inner = index.inner.lock().unwrap();
    assert_eq!(
        inner.store.len(),
        0,
        "sealing must release the VectorStore as well as decoded rows"
    );
    assert_eq!(inner.flat.as_ref().unwrap().data.len(), 0);
}

#[test]
fn flat_compacted_base_keeps_absent_rows_deleted_on_open_and_install() {
    let spec = VectorSpec {
        dim: 2,
        metric: VectorMetric::L2,
        backend: crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
        quantize: None,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mapped-base.lseg");
    crate::persistence::infrastructure::segment::vector_writer::write_vector_segment(
        &path,
        7,
        2,
        &[None, Some(&[2., 2.]), None],
    )
    .unwrap();
    let reader =
        Arc::new(crate::persistence::infrastructure::segment::SegmentReader::open(&path).unwrap());
    let ids = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
    let cold = FlatCpuIndex::open_from_segment(spec, reader.clone(), ids.clone()).unwrap();
    assert_eq!(
        cold.len(),
        1,
        "absent base rows cannot count as live vectors"
    );
    let oracle = FlatCpuIndex::new(spec);
    oracle.add("b", &[2., 2.]).unwrap();
    let expected = oracle.search_knn(&[0., 0.], 10).unwrap();
    assert_eq!(cold.search_knn(&[0., 0.], 10).unwrap(), expected);
    let live = FlatCpuIndex::new(spec);
    live.add("a", &[9., 9.]).unwrap();
    live.add("b", &[2., 2.]).unwrap();
    live.install_checkpoint_base(
        reader,
        &ids,
        &ids.iter().map(|id| (id.clone(), true)).collect(),
    )
    .unwrap();
    assert_eq!(
        live.len(),
        1,
        "install must apply the compacted base presence column"
    );
    assert_eq!(live.search_knn(&[0., 0.], 10).unwrap(), expected);
}
