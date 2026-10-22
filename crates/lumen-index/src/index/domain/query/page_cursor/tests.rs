//! Page cursors round-trip: offset, sort keyset and score keyset.

use crate::index::domain::query::page_cursor::{
    make_cursor, make_score_cursor, make_sort_cursor, parse_page_cursor, PageCursor,
};

#[test]
fn cursor_round_trip() {
    let c = make_cursor(42);
    assert!(matches!(
        parse_page_cursor(&c),
        Some(PageCursor::Offset(42))
    ));
    let c = make_sort_cursor(0x8000_0000_0000_0000, 7);
    assert!(matches!(
        parse_page_cursor(&c),
        Some(PageCursor::SortKeyset {
            bits: 0x8000_0000_0000_0000,
            docid: 7
        })
    ));
    let c = make_score_cursor(1.5, "u042");
    match parse_page_cursor(&c) {
        Some(PageCursor::ScoreKeyset { score_bits, eid }) => {
            assert_eq!(f32::from_bits(score_bits), 1.5);
            assert_eq!(eid, "u042");
        }
        _ => panic!("expected score keyset"),
    }
}
