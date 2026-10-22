use std::collections::BTreeSet;

use super::TokenSet;

#[test]
fn promotion_and_repeated_tokens_keep_one_owned_entry_per_distinct_term() {
    let mut tokens = TokenSet::default();
    for i in 0..8 {
        assert!(tokens.insert_str(&format!("term-{i:04}")));
    }
    assert!(matches!(&tokens, TokenSet::Inline(_)));
    let original = tokens.iter().next().unwrap().as_ptr();
    for i in 8..1000 {
        assert!(tokens.insert_str(&format!("term-{i:04}")));
    }
    assert!(matches!(&tokens, TokenSet::Indexed(_)));
    assert_eq!(
        tokens
            .iter()
            .find(|s| s.as_str() == "term-0000")
            .unwrap()
            .as_ptr(),
        original,
        "promotion must move existing token allocations"
    );
    for i in 0..1000 {
        assert!(!tokens.insert_str(&format!("term-{i:04}")));
    }
    let expected: BTreeSet<_> = (0..1000).map(|i| format!("term-{i:04}")).collect();
    assert_eq!(tokens.iter().cloned().collect::<BTreeSet<_>>(), expected);
    let rebuilt = TokenSet::from_btree_set(expected.clone());
    assert_eq!(rebuilt.iter().cloned().collect::<BTreeSet<_>>(), expected);
}
