use std::sync::Arc;

use crate::index::domain::vector::flat_cpu_index::FlatCpuIndex;
use crate::index::domain::vector::VectorIndex;
use crate::shared_kernel::types::schema::{VectorMetric, VectorSpec};

#[test]
fn flat_base_compaction_preserves_newer_layers_ram_and_deletions() {
    let spec = VectorSpec {
        dim: 2,
        metric: VectorMetric::L2,
        backend: crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
        quantize: None,
    };
    let dir = tempfile::tempdir().unwrap();
    let write = |name: &str, rows: &[Option<&[f32]>]| {
        let path = dir.path().join(name);
        crate::persistence::infrastructure::segment::vector_writer::write_vector_segment(
            &path, 1, 2, rows,
        )
        .unwrap();
        Arc::new(crate::persistence::infrastructure::segment::SegmentReader::open(&path).unwrap())
    };
    let base = write("base.lseg", &[Some(&[0., 0.]), Some(&[9., 9.])]);
    let first = write("first.lseg", &[Some(&[1., 1.]), Some(&[2., 2.])]);
    let second = write("second.lseg", &[None, Some(&[3., 3.])]);
    let later = write("later.lseg", &[Some(&[4., 4.]), Some(&[5., 5.])]);
    let index =
        FlatCpuIndex::open_from_segment(spec, base.clone(), vec!["a".into(), "base".into()])
            .unwrap();
    index
        .attach_checkpoint_delta(first.clone(), &["a".into(), "b".into()], &[true, true])
        .unwrap();
    index
        .attach_checkpoint_delta(second.clone(), &["a".into(), "c".into()], &[true, true])
        .unwrap();
    index
        .attach_checkpoint_delta(later.clone(), &["c".into(), "d".into()], &[true, true])
        .unwrap();
    index.add("b", &[6., 6.]).unwrap();
    index.remove("d").unwrap();
    let before = index.search_knn(&[0., 0.], 10).unwrap();
    let compacted = write(
        "compacted.lseg",
        &[None, Some(&[2., 2.]), Some(&[9., 9.]), Some(&[3., 3.])],
    );
    let ids = ["a".into(), "b".into(), "base".into(), "c".into()];
    index
        .replace_checkpoint_base(
            &base,
            &[first.clone(), second.clone()],
            compacted.clone(),
            &ids,
        )
        .unwrap();
    assert_eq!(index.search_knn(&[0., 0.], 10).unwrap(), before);
    assert_eq!(index.resident_vector_payload_rows(), 1);
    assert_eq!(index.checkpoint_vector("a").unwrap(), None);
    assert_eq!(index.checkpoint_vector("d").unwrap(), None);
    assert_eq!(index.checkpoint_vector("b").unwrap(), Some(vec![6., 6.]));
    assert_eq!(index.checkpoint_vector("c").unwrap(), Some(vec![4., 4.]));
    assert_eq!(Arc::strong_count(&base), 1, "old base must be released");
    assert_eq!(
        Arc::strong_count(&first),
        1,
        "first base input must be released"
    );
    assert_eq!(
        Arc::strong_count(&second),
        1,
        "second base input must be released"
    );
    let remaining = index.checkpoint_delta_readers();
    assert_eq!(remaining.len(), 1);
    assert!(Arc::ptr_eq(&remaining[0], &later));
    assert!(Arc::ptr_eq(
        &index.checkpoint_base_reader().unwrap(),
        &compacted
    ));
    assert!(index
        .replace_checkpoint_base(&base, &[first, second], compacted, &ids)
        .is_err());
    assert_eq!(index.search_knn(&[0., 0.], 10).unwrap(), before);
}

#[test]
fn flat_compaction_retargets_only_selected_layers_and_releases_input_readers() {
    let spec = VectorSpec {
        dim: 2,
        metric: VectorMetric::L2,
        backend: crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
        quantize: None,
    };
    let dir = tempfile::tempdir().unwrap();
    let write = |name: &str, values: &[Option<&[f32]>]| {
        let path = dir.path().join(name);
        crate::persistence::infrastructure::segment::vector_writer::write_vector_segment(
            &path, 1, 2, values,
        )
        .unwrap();
        Arc::new(crate::persistence::infrastructure::segment::SegmentReader::open(&path).unwrap())
    };
    let index = FlatCpuIndex::open_from_segment(
        spec,
        write("base.lseg", &[Some(&[0., 0.]), Some(&[9., 9.])]),
        vec!["a".into(), "base".into()],
    )
    .unwrap();
    let first = write("first.lseg", &[Some(&[1., 1.]), Some(&[2., 2.])]);
    let second = write("second.lseg", &[None, Some(&[3., 3.])]);
    let third = write("third.lseg", &[Some(&[4., 4.]), Some(&[5., 5.])]);
    index
        .attach_checkpoint_delta(first.clone(), &["a".into(), "b".into()], &[true, true])
        .unwrap();
    index
        .attach_checkpoint_delta(second.clone(), &["a".into(), "c".into()], &[true, true])
        .unwrap();
    index
        .attach_checkpoint_delta(third.clone(), &["c".into(), "d".into()], &[true, true])
        .unwrap();
    index.add("b", &[6., 6.]).unwrap();
    index.remove("d").unwrap();
    let before = index.search_knn(&[0., 0.], 10).unwrap();
    let compacted = write("compacted.lseg", &[None, Some(&[2., 2.]), Some(&[3., 3.])]);
    index
        .replace_checkpoint_deltas(
            &[first.clone(), second.clone()],
            compacted.clone(),
            &["a".into(), "b".into(), "c".into()],
        )
        .expect("replace the exact selected vector delta range");
    assert_eq!(index.search_knn(&[0., 0.], 10).unwrap(), before);
    assert_eq!(
        index.resident_vector_payload_rows(),
        1,
        "newer RAM update survives compaction"
    );
    assert_eq!(
        Arc::strong_count(&first),
        1,
        "first input must be released by the live index"
    );
    assert_eq!(
        Arc::strong_count(&second),
        1,
        "second input must be released by the live index"
    );
    let readers = index.checkpoint_delta_readers();
    assert_eq!(readers.len(), 2);
    assert!(Arc::ptr_eq(&readers[0], &compacted));
    assert!(Arc::ptr_eq(&readers[1], &third));
    assert!(
        index
            .replace_checkpoint_deltas(
                &[first, second],
                compacted,
                &["a".into(), "b".into(), "c".into()],
            )
            .is_err(),
        "stale input identities must not match the current layers"
    );
    assert_eq!(index.search_knn(&[0., 0.], 10).unwrap(), before);
}

#[test]
fn flat_mmap_deltas_release_acknowledged_payload_and_preserve_newer_ram() {
    let spec = VectorSpec {
        dim: 2,
        metric: VectorMetric::L2,
        backend: crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
        quantize: None,
    };
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lseg");
    crate::persistence::infrastructure::segment::vector_writer::write_vector_segment(
        &base,
        1,
        2,
        &[Some(&[0.0, 0.0]), Some(&[4.0, 4.0])],
    )
    .unwrap();
    let index = FlatCpuIndex::open_from_segment(
        spec,
        Arc::new(crate::persistence::infrastructure::segment::SegmentReader::open(&base).unwrap()),
        vec!["a".into(), "b".into()],
    )
    .unwrap();
    let oracle = FlatCpuIndex::new(spec);
    oracle.add("a", &[0.0, 0.0]).unwrap();
    oracle.add("b", &[4.0, 4.0]).unwrap();
    let first = dir.path().join("one.lseg");
    crate::persistence::infrastructure::segment::vector_writer::write_vector_segment(
        &first,
        2,
        2,
        &[Some(&[1.0, 1.0]), Some(&[9.0, 9.0])],
    )
    .unwrap();
    index
        .attach_checkpoint_delta(
            Arc::new(
                crate::persistence::infrastructure::segment::SegmentReader::open(&first).unwrap(),
            ),
            &["a".into(), "c".into()],
            &[true, true],
        )
        .unwrap();
    oracle.add("a", &[1.0, 1.0]).unwrap();
    oracle.add("c", &[9.0, 9.0]).unwrap();
    assert_eq!(index.resident_vector_payload_rows(), 0);
    assert_eq!(index.checkpoint_resident_bytes(), Some(0));
    let second = dir.path().join("two.lseg");
    crate::persistence::infrastructure::segment::vector_writer::write_vector_segment(
        &second,
        3,
        2,
        &[Some(&[2.0, 2.0]), None],
    )
    .unwrap();
    index
        .attach_checkpoint_delta(
            Arc::new(
                crate::persistence::infrastructure::segment::SegmentReader::open(&second).unwrap(),
            ),
            &["a".into(), "b".into()],
            &[true, true],
        )
        .unwrap();
    oracle.add("a", &[2.0, 2.0]).unwrap();
    oracle.remove("b").unwrap();
    index.add("a", &[3.0, 3.0]).unwrap();
    oracle.add("a", &[3.0, 3.0]).unwrap();
    assert_eq!(index.resident_vector_payload_rows(), 1);
    assert_eq!(index.checkpoint_resident_bytes(), Some(9));
    for query in [[0.0, 0.0], [3.0, 3.0], [10.0, 10.0]] {
        assert_eq!(
            index.search_knn(&query, 8).unwrap(),
            oracle.search_knn(&query, 8).unwrap()
        );
    }
    assert!(index
        .search_knn(&[4.0, 4.0], 8)
        .unwrap()
        .iter()
        .all(|(eid, _)| eid != "b"));
}
