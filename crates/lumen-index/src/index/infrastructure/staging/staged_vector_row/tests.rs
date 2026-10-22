use crate::index::domain::vector::quantize::{decode_sq, encode_sq, ScalarCodebook};
use crate::index::infrastructure::staging::staged_vector_row::{
    StagedVectorRow, DECODE_CHUNK_BYTES, READER_METADATA_BYTES,
};

use crate::index::infrastructure::staging::staged_vector_row::last_stage_directory_for_test;

fn le_words(bits: &[u32]) -> Vec<u8> {
    bits.iter().flat_map(|bits| bits.to_le_bytes()).collect()
}
fn unaligned(bits: &[u32]) -> Vec<u8> {
    let mut bytes = vec![0xff];
    bytes.extend(le_words(bits));
    bytes
}
fn bits(values: &[f32]) -> Vec<u32> {
    values.iter().map(|value| value.to_bits()).collect()
}

#[test]
fn unaligned_le_input_round_trips_exact_floating_bits() {
    let words = [0x8000_0000, 0x7f80_0000, 0x7fc0_0042, 0x3f80_0000];
    let bytes = unaligned(&words);
    let row = StagedVectorRow::stage(&bytes[1..], words.len() as u32, |_| Ok(())).unwrap();
    assert_eq!(row.dim(), words.len());
    assert_eq!(bits(row.as_f32_slice()), words);
}
#[test]
fn wrong_length_refuses_before_stage_creation() {
    let before = last_stage_directory_for_test();
    assert!(StagedVectorRow::stage(&[0; 7], 2, |_| Ok(()))
        .unwrap_err()
        .to_string()
        .contains("wire length"));
    assert_eq!(last_stage_directory_for_test(), before);
}
#[test]
fn reservation_refusal_creates_no_private_file() {
    let before = last_stage_directory_for_test();
    assert!(
        StagedVectorRow::stage(&le_words(&[0]), 1, |_| anyhow::bail!("refuse"))
            .unwrap_err()
            .to_string()
            .contains("refuse")
    );
    assert_eq!(last_stage_directory_for_test(), before);
}
#[test]
fn clone_keeps_private_file_until_final_owner_drops() {
    let row = StagedVectorRow::stage(&le_words(&[0x3f80_0000]), 1, |_| Ok(())).unwrap();
    let directory = last_stage_directory_for_test().unwrap();
    let clone = row.clone();
    drop(row);
    assert!(directory.is_dir());
    assert_eq!(clone.as_f32_slice()[0].to_bits(), 0x3f80_0000);
    drop(clone);
    assert!(!directory.exists());
}
#[test]
fn reservation_prices_the_only_simultaneous_decode_buffer_and_metadata() {
    let mut required = None;
    let row = StagedVectorRow::stage(&le_words(&[0, 1]), 2, |bytes| {
        required = Some(bytes);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        required,
        Some(DECODE_CHUNK_BYTES.min(8) + READER_METADATA_BYTES)
    );
    assert_eq!(row.as_f32_slice().len(), 2);
}
#[cfg(unix)]
#[test]
fn private_stage_directory_is_created_with_owner_only_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let row = StagedVectorRow::stage(&le_words(&[0]), 1, |_| Ok(())).unwrap();
    let directory = last_stage_directory_for_test().unwrap();
    assert_eq!(
        std::fs::metadata(directory).unwrap().permissions().mode() & 0o777,
        0o700
    );
    drop(row);
}
#[test]
fn sq_canonical_matches_shared_codec_and_preserves_raw_bits() {
    for words in [
        vec![0x8000_0000, 0x7f80_0000, 0xff80_0000, 0x7fc0_0042],
        vec![5f32.to_bits(), 5f32.to_bits(), 5f32.to_bits()],
        vec![(-100f32).to_bits(), 0f32.to_bits(), 100f32.to_bits()],
    ] {
        let input = unaligned(&words);
        let raw = StagedVectorRow::stage(&input[1..], words.len() as u32, |_| Ok(())).unwrap();
        let mut expected_cb = ScalarCodebook::empty(words.len());
        expected_cb.widen(raw.as_f32_slice());
        let expected = decode_sq(&encode_sq(raw.as_f32_slice(), &expected_cb), &expected_cb);
        let (canonical, widened) = raw
            .stage_sq_canonical(ScalarCodebook::empty(words.len()), |_| Ok(()))
            .unwrap();
        assert_eq!(
            (widened.min.to_bits(), widened.max.to_bits(), widened.dim),
            (
                expected_cb.min.to_bits(),
                expected_cb.max.to_bits(),
                expected_cb.dim
            )
        );
        assert_eq!(bits(canonical.as_f32_slice()), bits(&expected));
        assert_eq!(bits(raw.as_f32_slice()), words);
    }
}
#[test]
fn sq_canonical_reserves_bounded_multi_chunk_workspace_before_new_directory() {
    let words = vec![1f32.to_bits(); DECODE_CHUNK_BYTES / 4 + 1];
    let raw = StagedVectorRow::stage(&le_words(&words), words.len() as u32, |_| Ok(())).unwrap();
    let mut required = None;
    let before = last_stage_directory_for_test();
    let refusal = raw
        .stage_sq_canonical(ScalarCodebook::empty(words.len()), |bytes| {
            required = Some(bytes);
            anyhow::bail!("refuse")
        })
        .unwrap_err();
    assert!(refusal.to_string().contains("refuse"));
    assert_eq!(required, Some(DECODE_CHUNK_BYTES + READER_METADATA_BYTES));
    assert_eq!(last_stage_directory_for_test(), before);
}
#[test]
fn sq_canonical_widens_the_complete_row_before_chunked_decode() {
    let mut words = vec![5.0f32.to_bits(); DECODE_CHUNK_BYTES / 4];
    words.push(5.5f32.to_bits());
    let raw = StagedVectorRow::stage(&le_words(&words), words.len() as u32, |_| Ok(())).unwrap();

    let mut expected_codebook = ScalarCodebook::empty(words.len());
    expected_codebook.widen(raw.as_f32_slice());
    let expected = decode_sq(
        &encode_sq(raw.as_f32_slice(), &expected_codebook),
        &expected_codebook,
    );

    let (canonical, widened) = raw
        .stage_sq_canonical(ScalarCodebook::empty(words.len()), |_| Ok(()))
        .unwrap();

    assert_eq!(widened.min.to_bits(), 5.0f32.to_bits());
    assert_eq!(widened.max.to_bits(), 5.5f32.to_bits());
    assert_eq!(bits(canonical.as_f32_slice()), bits(&expected));
}
