use std::collections::{BTreeMap, BTreeSet};

use crate::ingest::domain::change_record_cost::estimate_record_exact_default_ngram;
use crate::ingest::domain::change_record_cost::ngram_distinct_table::{
    NgramDistinctTable, DEFAULT_NGRAM_DISTINCT_CAP,
};
use crate::ingest::domain::change_record_cost::tests::prepared_text::total_cost;
use crate::ingest::domain::change_record_cost::tests::{ready, TestContext};
use crate::ingest::domain::change_record_cost::text_upper_bound::{
    text_upper_bound, AnalyzerKind, TextUpperBound,
};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{
    FieldValue, IndexItem, IndexRequest, ReplaceDocItem, ReplaceDocsRequest,
};
use crate::shared_kernel::types::schema::Analyzer;

fn exact_oracle(input: &str) -> TextUpperBound {
    let mut terms = BTreeSet::new();
    crate::ngram_stream::stream_default_ngrams(input, |token| {
        terms.insert(token.to_owned());
        Ok::<_, ()>(())
    })
    .unwrap();
    TextUpperBound {
        terms: terms.len(),
        total_utf8_bytes: terms.iter().map(String::len).sum(),
    }
}

#[test]
fn exact_default_ngram_matches_repeated_and_unicode_terms() {
    let mut table = NgramDistinctTable::new();
    for input in [
        "durable search token ".repeat(16),
        "İİ 中文🦀 A\t中 İ".repeat(8),
        String::new(),
    ] {
        assert_eq!(table.bound(&input).unwrap(), exact_oracle(&input));
    }
}

#[test]
fn exact_default_ngram_compares_full_bytes_on_hash_collisions() {
    let mut table = NgramDistinctTable::new();
    for token in ["ab", "ac", "中a", "中b", "ab", "中a"] {
        table.add_with_hash(token, 7);
    }
    let actual: BTreeSet<Vec<u8>> = table
        .slots
        .iter()
        .filter(|s| s.occupied)
        .map(|s| s.bytes[..usize::from(s.len)].to_vec())
        .collect();
    let expected = ["ab", "ac", "中a", "中b"]
        .into_iter()
        .map(|s| s.as_bytes().to_vec())
        .collect();
    assert_eq!(actual, expected);
    assert!(!table.full);
}

#[test]
fn exact_default_ngram_overflow_falls_back_and_next_cell_resets() {
    let input: String = (0x4e00..0x4e00 + 300)
        .map(|n| char::from_u32(n).unwrap())
        .collect();
    assert!(exact_oracle(&input).terms > DEFAULT_NGRAM_DISTINCT_CAP);
    let mut table = NgramDistinctTable::new();
    assert_eq!(
        table.bound(&input).unwrap(),
        text_upper_bound(&input, AnalyzerKind::Ngram, 2, 3).unwrap()
    );
    assert!(
        table.full,
        "real distinct terms must exhaust the fixed table"
    );
    assert_eq!(table.bound("aaaaa").unwrap(), exact_oracle("aaaaa"));
    assert!(
        !table.full,
        "a previous large cell must not poison a later small one"
    );
}

fn ngram_context() -> TestContext {
    let mut ctx = TestContext::default();
    for field in ["a", "b"] {
        ctx.text("c", field);
        ctx.fields
            .get_mut(&("c".into(), field.into()))
            .unwrap()
            .analyzer = Some(Analyzer::Ngram);
    }
    ctx
}

#[test]
fn exact_default_ngram_prices_each_field_and_document_separately() {
    let ctx = ngram_context();
    let items = vec![
        IndexItem {
            external_id: "one".into(),
            field: "a".into(),
            value: FieldValue::String("aaaaaa".into()),
            version: None,
        },
        IndexItem {
            external_id: "two".into(),
            field: "b".into(),
            value: FieldValue::String("中文中文".into()),
            version: None,
        },
    ];
    let entry = |items| RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items,
            request_id: None,
        },
    };
    let separate: usize = items
        .iter()
        .cloned()
        .map(|item| {
            total_cost(ready(estimate_record_exact_default_ngram(
                &entry(vec![item]),
                &ctx,
            )))
        })
        .sum();
    assert_eq!(
        total_cost(ready(estimate_record_exact_default_ngram(
            &entry(items),
            &ctx
        ))),
        separate
    );
}

#[test]
fn exact_default_ngram_invalid_replace_does_not_pollute_later_doc() {
    let ctx = ngram_context();
    let large: String = (0x4e00..0x4e00 + 300)
        .map(|n| char::from_u32(n).unwrap())
        .collect();
    let bad = ReplaceDocItem {
        external_id: "bad".into(),
        version: None,
        fields: BTreeMap::from([
            ("a".into(), FieldValue::String(large)),
            ("z-missing".into(), FieldValue::String("bad".into())),
        ]),
    };
    let good = ReplaceDocItem {
        external_id: "good".into(),
        version: None,
        fields: BTreeMap::from([("b".into(), FieldValue::String("aaaaaa".into()))]),
    };
    let entry = |docs| RaftLogEntry::ReplaceDocs {
        collection_id: "c".into(),
        req: ReplaceDocsRequest { docs },
    };
    let bad_cost = total_cost(ready(estimate_record_exact_default_ngram(
        &entry(vec![bad.clone()]),
        &ctx,
    )));
    let good_cost = total_cost(ready(estimate_record_exact_default_ngram(
        &entry(vec![good.clone()]),
        &ctx,
    )));
    let combined = total_cost(ready(estimate_record_exact_default_ngram(
        &entry(vec![bad, good]),
        &ctx,
    )));
    assert_eq!(combined, bad_cost + good_cost);
    assert!(good_cost > 0);
}
