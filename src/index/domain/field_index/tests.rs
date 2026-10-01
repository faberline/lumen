//! A scalar field index reads its live overlay over its sealed segment: a
//! number's replacement, then its delete, and a middle document's keyword, set
//! and hash overlays, which a sparse higher delta must not hide.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::index::domain::hash_index::HashIndex;
use crate::index::domain::keyword_index::KeywordIndex;
use crate::index::domain::number_index::NumberIndex;
use crate::index::domain::set_index::SetIndex;
use crate::index::domain::sortable_f64::SortableF64;
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;

#[test]
fn sealed_number_reads_replacement_overlay_and_then_delete() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("number.lseg");
    crate::persistence::infrastructure::segment::number_writer::write_number_segment(
        &path,
        1,
        &[Some(1.0)],
    )
    .unwrap();
    let mut number = NumberIndex::default();
    number.segment = Some(Arc::new(ComposedSegmentReader::from_base(Arc::new(
        crate::persistence::infrastructure::segment::SegmentReader::open(&path).unwrap(),
    ))));
    number.tombstones.insert(0);
    number.forward.insert(0, SortableF64::new(2.0).unwrap());
    assert_eq!(number.live_number_at(0).map(|v| v.to_f64()), Some(2.0));
    number.forward.remove(&0);
    assert_eq!(number.live_number_at(0), None);
}

#[test]
fn sparse_high_delta_does_not_hide_middle_live_scalar_overlays() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.lseg");
    let delta = dir.path().join("delta.lseg");

    crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment(
        &base,
        1,
        &[Some("base")],
        &BTreeMap::new(),
    )
    .unwrap();
    crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment(
        &delta,
        1,
        &[Some("delta")],
        &BTreeMap::new(),
    )
    .unwrap();
    let keyword_view = ComposedSegmentReader::from_base(Arc::new(
        crate::persistence::infrastructure::segment::SegmentReader::open(&base).unwrap(),
    ))
    .with_delta(
        Arc::new(crate::persistence::infrastructure::segment::SegmentReader::open(&delta).unwrap()),
        vec![100],
    )
    .unwrap();
    let mut keyword = KeywordIndex::default();
    keyword.segment = Some(Arc::new(keyword_view));
    keyword.forward.insert(2, "middle".into());
    assert_eq!(keyword.keyword_at(2).as_deref(), Some("middle"));

    let base_set = ["base".to_string()];
    crate::persistence::infrastructure::segment::set_writer::write_set_segment(
        &base,
        1,
        &[Some(base_set.as_slice())],
        &BTreeMap::new(),
    )
    .unwrap();
    let delta_set = ["delta".to_string()];
    crate::persistence::infrastructure::segment::set_writer::write_set_segment(
        &delta,
        1,
        &[Some(delta_set.as_slice())],
        &BTreeMap::new(),
    )
    .unwrap();
    let set_view = ComposedSegmentReader::from_base(Arc::new(
        crate::persistence::infrastructure::segment::SegmentReader::open(&base).unwrap(),
    ))
    .with_delta(
        Arc::new(crate::persistence::infrastructure::segment::SegmentReader::open(&delta).unwrap()),
        vec![100],
    )
    .unwrap();
    let mut set = SetIndex::default();
    set.segment = Some(Arc::new(set_view));
    set.forward
        .insert(2, ["middle".to_string()].into_iter().collect());
    assert!(set.set_contains(2, "middle"));
    assert_eq!(
        set.set_members(2),
        Some(["middle".to_string()].into_iter().collect())
    );

    crate::persistence::infrastructure::segment::hash_writer::write_hash_segment(
        &base,
        1,
        &[Some(1)],
    )
    .unwrap();
    crate::persistence::infrastructure::segment::hash_writer::write_hash_segment(
        &delta,
        1,
        &[Some(3)],
    )
    .unwrap();
    let hash_view = ComposedSegmentReader::from_base(Arc::new(
        crate::persistence::infrastructure::segment::SegmentReader::open(&base).unwrap(),
    ))
    .with_delta(
        Arc::new(crate::persistence::infrastructure::segment::SegmentReader::open(&delta).unwrap()),
        vec![100],
    )
    .unwrap();
    let mut hash = HashIndex::default();
    hash.segment = Some(Arc::new(hash_view));
    hash.forward.insert(2, 2);
    assert_eq!(hash.hash_at(2), Some(2));
}
