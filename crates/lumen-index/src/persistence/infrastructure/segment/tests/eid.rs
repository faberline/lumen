use crate::persistence::infrastructure::segment::eid_writer::write_eid_segment;
use crate::persistence::infrastructure::segment::tests::tmp_path;
use crate::persistence::infrastructure::segment::SegmentReader;

// -----------------------------------------------------------------------
// Collection EID column (Phase 2f-1)
// -----------------------------------------------------------------------

/// The eid-by-position column round-trips: `eid_at(i)` returns the i-th
/// external_id and `eids_all` reproduces the whole dense Vec in order, even
/// across awkward strings (empty / unicode / long).
#[test]
fn eid_segment_round_trip() {
    let path = tmp_path("eid-rt");
    let owned: Vec<String> = vec![
        "doc-0".into(),
        "".into(),           // empty eid is legal
        "日本語-doc".into(), // non-ascii
        "x".repeat(300),     // long, crosses no prefix-share
        "doc-0".into(),      // duplicate STRING but distinct docid (by position)
    ];
    let eids: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
    write_eid_segment(&path, 99, &eids).unwrap();

    let r = SegmentReader::open(&path).unwrap();
    assert_eq!(r.applied_seq(), 99);
    assert_eq!(r.eid_count(), owned.len() as u32);
    for (i, want) in owned.iter().enumerate() {
        assert_eq!(
            r.eid_at(i as u32).as_deref(),
            Some(want.as_str()),
            "eid {i}"
        );
    }
    assert_eq!(r.eid_at(owned.len() as u32), None); // out of range
    assert_eq!(r.eids_all().as_deref(), Some(&owned[..]));
    std::fs::remove_file(&path).ok();
}

/// The eid column must survive crossing multiple 64KB LZ4 blocks and resolve
/// every position through the skip-index.
#[test]
fn eid_segment_multi_block() {
    let path = tmp_path("eid-multi-block");
    let n = 30_000usize;
    let owned: Vec<String> = (0..n).map(|i| format!("external-id-{i:012}")).collect();
    let eids: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
    write_eid_segment(&path, 1, &eids).unwrap();
    let r = SegmentReader::open(&path).unwrap();
    assert_eq!(r.eid_count(), n as u32);
    for &i in &[0usize, 1, 1234, n / 2, n - 2, n - 1] {
        assert_eq!(
            r.eid_at(i as u32).as_deref(),
            Some(owned[i].as_str()),
            "eid {i}"
        );
    }
    let all = r.eids_all().unwrap();
    assert_eq!(all.len(), n);
    assert_eq!(all[n - 1], owned[n - 1]);
    std::fs::remove_file(&path).ok();
}
