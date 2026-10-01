use crate::persistence::infrastructure::segment::hash_writer::write_hash_segment;
use crate::persistence::infrastructure::segment::tests::tmp_path;
use crate::persistence::infrastructure::segment::vector_writer::write_vector_segment;
use crate::persistence::infrastructure::segment::{SegmentReader, HEADER_LEN};

#[test]
fn hash_round_trip_small() {
    let path = tmp_path("hash-round-trip-small");
    let values = vec![Some(0xDEAD_BEEFu64), None, Some(0), Some(u64::MAX)];
    write_hash_segment(&path, 11, &values).unwrap();

    let r = SegmentReader::open(&path).unwrap();
    assert_eq!(r.applied_seq(), 11);
    assert_eq!(r.n_docs(), 4);
    assert_eq!(r.hash_at(0), Some(0xDEAD_BEEF));
    assert_eq!(r.hash_at(1), None); // absent
    assert_eq!(r.hash_at(2), Some(0)); // present-and-zero != absent
    assert_eq!(r.hash_at(3), Some(u64::MAX));
    assert_eq!(r.hash_at(4), None); // id >= n_docs
                                    // Cross-role: a hash segment has no Number column.
    assert_eq!(r.number_at(0), None);

    std::fs::remove_file(&path).ok();
}

#[test]
fn hash_large_round_trip() {
    let path = tmp_path("hash-large");
    let n = 5000usize;
    let values: Vec<Option<u64>> = (0..n)
        .map(|i| {
            if i % 4 == 0 {
                None
            } else {
                Some((i as u64).wrapping_mul(0x9E37_79B9))
            }
        })
        .collect();
    write_hash_segment(&path, 7, &values).unwrap();
    let r = SegmentReader::open(&path).unwrap();
    for (i, v) in values.iter().enumerate() {
        assert_eq!(r.hash_at(i as u32), *v, "mismatch at {i}");
    }
    assert_eq!(r.hash_at(n as u32), None);
    std::fs::remove_file(&path).ok();
}

#[test]
fn vector_round_trip_small() {
    let path = tmp_path("vector-round-trip-small");
    let dim = 3;
    let v0 = [1.0f32, -2.0, 0.5];
    let v2 = [3.5f32, 4.0, -1.25];
    let vectors: Vec<Option<&[f32]>> = vec![Some(&v0[..]), None, Some(&v2[..])];
    write_vector_segment(&path, 5, dim, &vectors).unwrap();

    let r = SegmentReader::open(&path).unwrap();
    assert_eq!(r.applied_seq(), 5);
    assert_eq!(r.n_docs(), 3);
    assert_eq!(r.vector_at(0, dim), Some(&v0[..]));
    assert_eq!(r.vector_at(1, dim), None); // absent
    assert_eq!(r.vector_at(2, dim), Some(&v2[..]));
    assert_eq!(r.vector_at(3, dim), None); // id >= n_docs

    // The whole column is dense (absent doc is dim zeros).
    let all = r.vectors_slice(dim).unwrap();
    assert_eq!(all.len(), 3 * dim);
    assert_eq!(&all[0..3], &v0);
    assert_eq!(&all[3..6], &[0.0, 0.0, 0.0]); // absent doc = zeros
    assert_eq!(&all[6..9], &v2);

    std::fs::remove_file(&path).ok();
}

#[test]
fn vector_bits_are_exact() {
    // Bit-exactness across awkward float values (incl negative zero / inf).
    let path = tmp_path("vector-bits");
    let dim = 4;
    let v: [f32; 4] = [f32::MIN_POSITIVE, -0.0, f32::INFINITY, 1.0 / 3.0];
    let vectors: Vec<Option<&[f32]>> = vec![Some(&v[..])];
    write_vector_segment(&path, 1, dim, &vectors).unwrap();
    let r = SegmentReader::open(&path).unwrap();
    let got = r.vector_at(0, dim).unwrap();
    for (a, b) in v.iter().zip(got) {
        assert_eq!(a.to_bits(), b.to_bits(), "f32 bits diverged");
    }
    std::fs::remove_file(&path).ok();
}

#[test]
fn vector_truncated_never_panics() {
    let path = tmp_path("vector-truncated");
    let dim = 8;
    let raw: Vec<f32> = (0..dim * 500).map(|i| i as f32).collect();
    let vectors: Vec<Option<&[f32]>> = (0..500)
        .map(|i| Some(&raw[i * dim..(i + 1) * dim]))
        .collect();
    write_vector_segment(&path, 9, dim, &vectors).unwrap();
    let full = std::fs::metadata(&path).unwrap().len();
    let truncate_to = HEADER_LEN as u64 + 64;
    assert!(truncate_to < full);
    {
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(truncate_to).unwrap();
    }
    match SegmentReader::open(&path) {
        Ok(r) => {
            for id in 0..600u32 {
                let _ = r.vector_at(id, dim); // must not panic
            }
            let _ = r.vectors_slice(dim);
        }
        Err(_) => { /* torn file rejected — also fine */ }
    }
    std::fs::remove_file(&path).ok();
}
