use crate::ingest::infrastructure::wire_cost::token_stats::json::scan_json;
use crate::ingest::infrastructure::wire_cost::token_stats::{
    scan, scan_cbor, scan_exact_cbor, TokenStats,
};

#[test]
fn cbor_and_json_text_stats_match_escapes_multibyte_and_indefinite_chunks() {
    let json = b" \n {\"k\":[\"A\\n\\u96ea\\uD83D\\uDE00\",\"z\"]}";
    let cbor = [
        0xa1, 0x61, b'k', 0x9f, 0x7f, 0x62, b'A', b'\n', 0x63, 0xe9, 0x9b, 0xaa, 0x64, 0xf0, 0x9f,
        0x98, 0x80, 0xff, 0x61, b'z', 0xff,
    ];
    let j = scan(json).unwrap();
    let c = scan_cbor(&cbor).unwrap();
    assert_eq!(j.text_bytes, 11);
    assert_eq!(j.token_count, 3);
    assert_eq!(c.text_bytes, 11);
    assert_eq!(c.token_count, 3);
    assert_eq!(c.largest_token_bytes, 9);
}
#[test]
fn rejects_huge_truncated_and_malformed_lengths_without_allocating() {
    assert!(scan_cbor(&[0x7b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]).is_err());
    assert!(scan_cbor(&[0x7a, 0xff, 0xff, 0xff, 0xff]).is_err());
    assert!(scan_json(br#""\uD800""#).is_err());
    assert!(scan_json(br#"["#).is_err());
    let mut stats = TokenStats {
        text_bytes: usize::MAX,
        ..TokenStats::default()
    };
    assert!(
        stats.token(1, true).is_err(),
        "checked aggregate must not wrap"
    );
}
#[test]
fn enforces_current_depth_bound_and_accepts_mixed_structure() {
    let mut ok = vec![b'['];
    ok.extend(std::iter::repeat_n(b'[', 126));
    ok.push(b'0');
    ok.extend(std::iter::repeat_n(b']', 127));
    assert!(scan_json(&ok).is_ok());
    let mut bad = vec![b'['];
    bad.extend(std::iter::repeat_n(b'[', 128));
    bad.push(b'0');
    bad.extend(std::iter::repeat_n(b']', 129));
    assert!(scan_json(&bad).is_err());
    assert!(scan_json(br#"{"a":[1,true,{"b":"x"}]}"#).is_ok());
}

#[test]
fn cbor_depth_and_trailing_match_decoder_boundaries() {
    let mut allowed = vec![0x81; 256];
    allowed.push(0);
    assert!(scan_cbor(&allowed).is_ok(), "256 containers plus scalar");
    let mut rejected = vec![0x81; 257];
    rejected.push(0);
    assert!(scan_cbor(&rejected).is_err(), "257th container");

    let trailing = [0x61, b'x', 0x00];
    assert_eq!(
        scan_cbor(&trailing).unwrap(),
        TokenStats {
            text_bytes: 1,
            byte_string_bytes: 0,
            token_count: 1,
            largest_token_bytes: 1
        }
    );
    assert!(scan_exact_cbor(&trailing).is_err());

    let mut exact_allowed = vec![0x81; 256];
    exact_allowed.push(0);
    assert!(scan_exact_cbor(&exact_allowed).is_ok());
    let mut exact_rejected = vec![0x81; 257];
    exact_rejected.push(0);
    assert!(scan_exact_cbor(&exact_rejected).is_err());
    assert!(scan_exact_cbor(&[0x61]).is_err());
}

#[test]
fn leading_json_map_or_sequence_never_uses_ambiguous_cbor_syntax() {
    let map = scan(br#" {"a":123}"#).unwrap();
    assert_eq!(map.text_bytes, 1);
    assert_eq!(map.token_count, 1);
    let sequence = scan(br#" ["snow", "\u96ea"]"#).unwrap();
    assert_eq!(sequence.text_bytes, 7);
    assert_eq!(sequence.token_count, 2);
}
#[test]
fn sixty_mib_token_has_scalar_stats_without_payload_token_allocation() {
    let n = 60 * 1024 * 1024;
    let mut json = Vec::with_capacity(n + 2);
    json.push(b'"');
    json.extend(std::iter::repeat_n(b'x', n));
    json.push(b'"');
    let s = scan_json(&json).unwrap();
    assert_eq!(s.text_bytes, n);
    assert_eq!(s.largest_token_bytes, n);
    assert_eq!(s.token_count, 1);
}
