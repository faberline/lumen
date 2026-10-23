use crate::persistence::infrastructure::segment::codecs::{
    decode_docid_block, decode_posting_block, encode_docid_block, encode_posting_block,
    read_varint, SortedIdCursor,
};
use crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment;
use crate::persistence::infrastructure::segment::sparse_rows::{
    decode_sparse_local_rows, encode_sparse_local_rows,
};
use crate::persistence::infrastructure::segment::tests::{kw_terms, tmp_path};
use crate::persistence::infrastructure::segment::var_column::VarEntry;
use crate::persistence::infrastructure::segment::{
    ColumnRef, Footer, SegmentReader, FOOTER_LEN, ROLE_DICT,
};

#[test]
fn var_suffix_uses_cbor_bytes_and_still_reads_legacy_integer_arrays() {
    #[derive(serde::Serialize)]
    struct LegacyEntry {
        shared: u32,
        suffix: Vec<u8>,
    }
    let mut legacy = Vec::new();
    ciborium::into_writer(
        &LegacyEntry {
            shared: 2,
            suffix: vec![0, 127, 255],
        },
        &mut legacy,
    )
    .unwrap();
    let decoded: VarEntry = ciborium::from_reader(legacy.as_slice()).unwrap();
    assert_eq!(decoded.shared, 2);
    assert_eq!(decoded.suffix, vec![0, 127, 255]);
    let mut current = Vec::new();
    ciborium::into_writer(&decoded, &mut current).unwrap();
    let value: ciborium::Value = ciborium::from_reader(current.as_slice()).unwrap();
    let ciborium::Value::Map(fields) = value else {
        panic!("entry must be a map")
    };
    let suffix = fields
        .iter()
        .find(|(key, _)| key == &ciborium::Value::Text("suffix".into()))
        .unwrap();
    assert!(matches!(&suffix.1, ciborium::Value::Bytes(bytes) if bytes == &[0, 127, 255]),
        "new segment suffixes must encode one byte string instead of visiting every byte as an integer");
    let roundtrip: VarEntry = ciborium::from_reader(current.as_slice()).unwrap();
    assert_eq!(roundtrip.suffix, decoded.suffix);
}

/// The docid-only posting-block codec round-trips exactly, incl an empty
/// list, a single posting, a large-gap stream, and the u32::MAX edge.
#[test]
fn docid_block_codec_round_trip() {
    let cases: Vec<Vec<u32>> = vec![
        vec![],
        vec![0],
        vec![0, 1, 2, 3],
        vec![3, 7, 100, 100_000, 4_000_000_000],
        vec![u32::MAX],
    ];
    for docids in cases {
        let blob = encode_docid_block(&docids);
        // df == count prefix.
        let mut pos = 0usize;
        assert_eq!(read_varint(&blob, &mut pos).unwrap() as usize, docids.len());
        assert_eq!(
            decode_docid_block(&blob),
            Some(docids.clone()),
            "docids diverged"
        );
    }
    // A truncated blob must yield None, never panic.
    let blob = encode_docid_block(&[1, 2, 3]);
    assert!(decode_docid_block(&blob[..blob.len() - 1]).is_none());
}

/// A truncated VAR block (the dict region is chopped so a dict-id's frame
/// overruns the file) must return `None`, never panic. The fixed columns
/// (dict-id / present) survive, so `keyword_at` reaches the dict read and
/// the torn-frame guard fires.
#[test]
fn truncated_var_block_returns_none() {
    let path = tmp_path("var-truncated");
    let n = 4000usize;
    let owned: Vec<String> = (0..n).map(|i| format!("key-{i:08}")).collect();
    let values: Vec<Option<&str>> = owned.iter().map(|s| Some(s.as_str())).collect();
    write_keyword_segment(&path, 1, &values, &kw_terms(&values)).unwrap();

    // Forge a file whose directory + footer are intact (crc still matches)
    // but the VAR region is chopped so the dict column overruns the file.
    // Same technique as `truncated_mid_column_with_intact_footer_returns_none`.
    let original = std::fs::read(&path).unwrap();
    let len = original.len();
    let footer = Footer::from_bytes(&original[len - FOOTER_LEN..len]).unwrap();
    let dir_off = footer.dir_offset as usize;
    let dir_end = dir_off + footer.dir_len as usize;

    // Find the dict column's byte_offset by decoding the directory.
    let dir: Vec<ColumnRef> = ciborium::from_reader(&original[dir_off..dir_end]).unwrap();
    let dict = dir.iter().find(|c| c.role == ROLE_DICT).unwrap();
    let dict_off = dict.byte_offset as usize;
    // Keep everything up to a few bytes into the dict region, then re-append
    // the unchanged directory + footer (rewriting only dir_offset).
    let keep = dict_off + 8; // a few bytes into the first frame
    assert!(keep < dir_off);
    let mut forged = Vec::new();
    forged.extend_from_slice(&original[..keep]);
    let new_dir_off = forged.len() as u64;
    forged.extend_from_slice(&original[dir_off..dir_end]); // same crc
    let new_footer = Footer {
        dir_offset: new_dir_off,
        dir_len: footer.dir_len,
        crc32: footer.crc32,
        magic2: footer.magic2,
    };
    forged.extend_from_slice(&new_footer.to_bytes());
    std::fs::write(&path, &forged).unwrap();

    match SegmentReader::open(&path) {
        Ok(r) => {
            // The dict-id column may survive; the dict block read overruns
            // the chopped file → None for every id, never a panic.
            for id in 0..n as u32 + 10 {
                let _ = r.keyword_at(id);
            }
            // At least the high ids (whose dict block is past `keep`) miss.
            assert_eq!(r.keyword_at(n as u32 - 1), None);
        }
        Err(_) => { /* also acceptable */ }
    }
    std::fs::remove_file(&path).ok();
}

/// The decompressed-block moka cache must serve a repeated dict read from
/// the cache (cold miss → warm hit), returning the identical string.
#[test]
fn var_block_cache_hit() {
    let path = tmp_path("var-cache");
    let owned: Vec<String> = (0..500).map(|i| format!("k{i:05}")).collect();
    let values: Vec<Option<&str>> = owned.iter().map(|s| Some(s.as_str())).collect();
    write_keyword_segment(&path, 1, &values, &kw_terms(&values)).unwrap();
    let r = SegmentReader::open(&path).unwrap();
    let first = r.keyword_at(123);
    let again = r.keyword_at(123); // served from the cached block
    assert_eq!(first, again);
    assert_eq!(first.as_deref(), Some("k00123"));
    std::fs::remove_file(&path).ok();
}

// -----------------------------------------------------------------------
// Text segment (Phase 2e-B)
// -----------------------------------------------------------------------

/// The varint + delta posting-block codec must round-trip exactly, incl an
/// empty list, a single posting, and a large-gap / high-tf stream.
#[test]
fn posting_block_codec_round_trip() {
    let cases: Vec<(Vec<u32>, Vec<u32>)> = vec![
        (vec![], vec![]),
        (vec![0], vec![1]),
        (vec![0, 1, 2, 3], vec![5, 4, 3, 2]),
        (
            vec![3, 7, 100, 100_000, 4_000_000_000],
            vec![1, 2, 3, 4, 999],
        ),
        (vec![u32::MAX], vec![u32::MAX]),
    ];
    for (docids, tfs) in cases {
        let blob = encode_posting_block(&docids, &tfs);
        let (d2, t2) = decode_posting_block(&blob).expect("decode");
        assert_eq!(d2, docids, "docids diverged");
        assert_eq!(t2, tfs, "tfs diverged");
    }
    // A truncated blob must yield None, never panic.
    let blob = encode_posting_block(&[1, 2, 3], &[1, 1, 1]);
    assert!(decode_posting_block(&blob[..blob.len() - 1]).is_none());
}

#[test]
fn sorted_id_cursor_is_exact_in_every_query_order() {
    let ids: Vec<u32> = (0..400u32).filter(|i| i % 3 == 0 || i % 7 == 0).collect();
    let mut cursor = SortedIdCursor::new(&ids);
    let mut state = 0x4246u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for step in 0..5000u32 {
        // Mostly ascending runs with occasional backwards jumps and repeats.
        let id = match next() % 10 {
            0 => (next() % 420) as u32,
            _ => ((step * 7 + (next() % 5) as u32) / 8) % 420,
        };
        assert_eq!(
            cursor.contains(id),
            ids.binary_search(&id).is_ok(),
            "step {step} id {id}"
        );
    }
    let mut empty = SortedIdCursor::new(&[]);
    assert!(!empty.contains(0));
    assert!(!empty.contains(u32::MAX));
}

#[test]
fn sparse_local_rows_reject_bad_cbor_without_unbounded_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.cbor");
    let ids = vec!["a".to_owned(), "日本語".to_owned()];
    encode_sparse_local_rows(&path, &ids).unwrap();
    let rows = decode_sparse_local_rows(&path, 2).unwrap();
    assert_eq!(rows.external_id(1), Some("日本語"));
    assert_eq!(rows.external_id(2), None);
    assert!(decode_sparse_local_rows(&path, 3).is_err());
    assert!(encode_sparse_local_rows(&path, &["x".into(), "x".into()]).is_err());
    // Indefinite array says it contains more than the declared one row.
    std::fs::write(&path, [0x9f, 0x61, b'a', 0x61, b'b', 0xff]).unwrap();
    assert!(decode_sparse_local_rows(&path, 1).is_err());
    std::fs::write(&path, [0x9a, 0xff, 0xff, 0xff, 0xff]).unwrap();
    assert!(decode_sparse_local_rows(&path, 0).is_err());
    std::fs::write(&path, [0x81, 0x61]).unwrap();
    assert!(decode_sparse_local_rows(&path, 1).is_err());
}
