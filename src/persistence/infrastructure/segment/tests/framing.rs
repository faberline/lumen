use std::io::{Read, Seek, SeekFrom, Write};

use crate::persistence::infrastructure::segment::number_writer::write_number_segment;
use crate::persistence::infrastructure::segment::tests::tmp_path;
use crate::persistence::infrastructure::segment::{Footer, SegmentReader, FOOTER_LEN, HEADER_LEN};

#[test]
fn crc_catches_directory_corruption() {
    let path = tmp_path("crc-corruption");
    let values = vec![Some(1.0), Some(2.0), Some(3.0)];
    write_number_segment(&path, 7, &values).unwrap();

    // Read the footer to find where the directory lives, then flip one
    // byte inside the directory region.
    let mut bytes = std::fs::read(&path).unwrap();
    let len = bytes.len();
    let footer = Footer::from_bytes(&bytes[len - FOOTER_LEN..len]).unwrap();
    let dir_off = footer.dir_offset as usize;
    // Flip a byte at the start of the directory region.
    bytes[dir_off] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();

    let err = SegmentReader::open(&path);
    assert!(err.is_err(), "crc must reject directory corruption");

    std::fs::remove_file(&path).ok();
}

#[test]
fn truncated_column_never_panics() {
    let path = tmp_path("truncated-column");
    // Large enough that the number column is non-trivial.
    let values: Vec<Option<f64>> = (0..2000).map(|i| Some(i as f64)).collect();
    write_number_segment(&path, 9, &values).unwrap();

    // Truncate the file to the middle of the fixed-width region (after the
    // header, well before the directory/footer). open() should reject it
    // (footer/dir gone); if it somehow opens, number_at must not panic.
    let full = std::fs::metadata(&path).unwrap().len();
    let truncate_to = HEADER_LEN as u64 + 64; // mid-column, no footer
    assert!(truncate_to < full);
    {
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(truncate_to).unwrap();
    }

    match SegmentReader::open(&path) {
        Ok(r) => {
            // Must not panic for any id.
            for id in 0..3000u32 {
                let _ = r.number_at(id);
            }
        }
        Err(_) => { /* expected: torn file rejected */ }
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn truncated_mid_column_with_intact_footer_returns_none() {
    // A subtler torn case: keep the footer/directory addressable but make a
    // column ref point past the actual file end by shrinking the file from
    // the middle is impossible without breaking the footer, so instead we
    // forge a directory whose number column overruns the file. We do that
    // by truncating just the trailing zero-pad + tail so the column len in
    // the directory exceeds the available bytes. Simplest robust check:
    // open a valid file, then truncate so the mmap is shorter than a
    // column ref claims, and confirm column_bytes()->None path.
    let path = tmp_path("mid-column-none");
    let values: Vec<Option<f64>> = (0..1000).map(|i| Some(i as f64)).collect();
    write_number_segment(&path, 3, &values).unwrap();

    // Read full file, then rebuild it shorter than the number column needs
    // while preserving a self-consistent footer+directory pointing at the
    // ORIGINAL (now out-of-range) offsets. We do this by chopping bytes out
    // of the middle of the number column and re-appending the original
    // directory + footer, so dir crc still matches but the column overruns.
    let original = std::fs::read(&path).unwrap();
    let len = original.len();
    let footer = Footer::from_bytes(&original[len - FOOTER_LEN..len]).unwrap();
    let dir_off = footer.dir_offset as usize;
    let dir_end = dir_off + footer.dir_len as usize;

    // New file = header + a too-short fixed region + original dir + footer.
    // Keep only HEADER_LEN + 16 bytes of the fixed region (number column
    // ref will claim far more). Then append the unchanged directory bytes
    // and footer, but rewrite the footer's dir_offset to the new location.
    let mut forged = Vec::new();
    forged.extend_from_slice(&original[..HEADER_LEN + 16]); // tiny fixed region
    let new_dir_off = forged.len() as u64;
    forged.extend_from_slice(&original[dir_off..dir_end]); // same dir bytes => same crc
    let new_footer = Footer {
        dir_offset: new_dir_off,
        dir_len: footer.dir_len,
        crc32: footer.crc32,
        magic2: footer.magic2,
    };
    forged.extend_from_slice(&new_footer.to_bytes());
    std::fs::write(&path, &forged).unwrap();

    // open() succeeds (header+footer+dir are valid), but the number column
    // ref overruns the file, so number_at must return None, never panic.
    match SegmentReader::open(&path) {
        Ok(r) => {
            for id in 0..1500u32 {
                let _ = r.number_at(id); // must not panic
            }
            // id 0 lives in the surviving 16 bytes? number col starts page
            // aligned at 4096, region is only 4096+16, so column_bytes for
            // the full claimed length is None => number_at None.
            assert_eq!(r.number_at(0), None);
        }
        Err(_) => { /* also acceptable */ }
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn bad_magic_rejected() {
    let path = tmp_path("bad-magic");
    let values = vec![Some(1.0), Some(2.0)];
    write_number_segment(&path, 1, &values).unwrap();

    // Corrupt magic2 (the last 4 bytes of the file).
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let len = f.metadata().unwrap().len();
    f.seek(SeekFrom::Start(len - 4)).unwrap();
    let mut m = [0u8; 4];
    f.read_exact(&mut m).unwrap();
    f.seek(SeekFrom::Start(len - 4)).unwrap();
    f.write_all(&[m[0] ^ 0xFF, m[1], m[2], m[3]]).unwrap();
    drop(f);

    assert!(
        SegmentReader::open(&path).is_err(),
        "bad magic2 must reject"
    );

    std::fs::remove_file(&path).ok();
}

#[test]
fn large_round_trip_page_alignment() {
    let path = tmp_path("large-round-trip");
    let n = 10_000usize;
    // Present pattern: every 3rd doc absent.
    let values: Vec<Option<f64>> = (0..n)
        .map(|i| {
            if i % 3 == 0 {
                None
            } else {
                Some(i as f64 * 0.25)
            }
        })
        .collect();
    write_number_segment(&path, 123_456, &values).unwrap();

    let r = SegmentReader::open(&path).unwrap();
    assert_eq!(r.applied_seq(), 123_456);
    assert_eq!(r.n_docs(), n as u32);
    for (i, v) in values.iter().enumerate() {
        assert_eq!(r.number_at(i as u32), *v, "mismatch at id {i}");
    }
    assert_eq!(r.number_at(n as u32), None);

    std::fs::remove_file(&path).ok();
}

/// Compile-time assertion that the reader is Send + Sync (the disk tier
/// shares it across serving threads).
#[test]
fn reader_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SegmentReader>();
}
