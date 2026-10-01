use super::*;
use std::collections::{BTreeMap, BTreeSet};

use crate::ingest::domain::change_record_cost::text_upper_bound::{
    ngram_upper_bound, text_upper_bound, AnalyzerKind, TextUpperBound,
};
use crate::shared_kernel::types::document::{
    IndexItem, IndexRequest, ReplaceDocItem, ReplaceDocsRequest,
};

#[derive(Default)]
struct TestContext {
    fields: BTreeMap<(String, String), FieldSpec>,
    known: BTreeSet<(String, String)>,
    coverage: BTreeMap<(String, String), BTreeSet<String>>,
    stale: BTreeSet<(String, String, String, u64)>,
}

impl TestContext {
    fn keyword(&mut self, collection: &str, field: &str) {
        self.fields.insert(
            (collection.to_owned(), field.to_owned()),
            FieldSpec {
                field_type: FieldType::Keyword,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
    }

    fn text(&mut self, collection: &str, field: &str) {
        self.fields.insert(
            (collection.to_owned(), field.to_owned()),
            FieldSpec {
                field_type: FieldType::Text,
                analyzer: Some(Analyzer::WhitespaceLower),
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
    }
}

impl CostContext for TestContext {
    fn collection_exists(&self, collection: &str) -> bool {
        self.fields.keys().any(|(known, _)| known == collection)
    }

    fn index_cell_is_stale(
        &self,
        collection: &str,
        external_id: &str,
        field: &str,
        version: Option<u64>,
    ) -> bool {
        version.is_some_and(|version| {
            self.stale.contains(&(
                collection.to_owned(),
                external_id.to_owned(),
                field.to_owned(),
                version,
            ))
        })
    }

    fn field_spec<'a>(&'a self, collection: &str, field: &str) -> Option<&'a FieldSpec> {
        self.fields.get(&(collection.to_owned(), field.to_owned()))
    }

    fn known_external_id(&self, collection: &str, external_id: &str) -> bool {
        self.known
            .contains(&(collection.to_owned(), external_id.to_owned()))
    }

    fn visit_coverage(&self, collection: &str, external_id: &str, visit: &mut dyn FnMut(&str)) {
        if let Some(fields) = self
            .coverage
            .get(&(collection.to_owned(), external_id.to_owned()))
        {
            for field in fields {
                visit(field);
            }
        }
    }

    fn request_is_deduplicated(&self, _collection: &str, _request_id: Option<&str>) -> bool {
        false
    }
}

fn ready(estimate: RecordEstimate) -> RecordCost {
    match estimate {
        RecordEstimate::Ready(cost) => cost,
        RecordEstimate::Retain { cause } => {
            panic!("must not retain ordinary apply error: {cause:?}")
        }
    }
}

#[test]
fn ngram_bound_uses_windows_without_allocating_tokens() {
    let bound = ngram_upper_bound(4, 2, 3).unwrap();
    assert_eq!(bound.terms, 5);
    assert_eq!(bound.total_utf8_bytes, (3 * 2 + 2 * 3) * 4);
}

#[test]
fn text_bound_counts_repetitions_and_unicode_lower_expansion() {
    let bound = text_upper_bound("İİ", AnalyzerKind::WhitespaceLower, 2, 3).unwrap();
    assert_eq!(bound.terms, 1);
    assert_eq!(bound.total_utf8_bytes, 6);
}

#[test]
fn invalid_ngram_range_is_empty_and_large_inputs_overflow() {
    assert_eq!(
        ngram_upper_bound(9, 3, 2).unwrap(),
        TextUpperBound::default()
    );
    assert_eq!(
        ngram_upper_bound(usize::MAX, 1, 1),
        Err(NormalizeError::Overflow)
    );
}

#[test]
fn mixed_index_prefix_is_charged_when_a_later_item_is_type_invalid() {
    let mut ctx = TestContext::default();
    ctx.keyword("c", "tag");
    let entry = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![
                IndexItem {
                    external_id: "doc".into(),
                    field: "tag".into(),
                    value: FieldValue::String("kept".into()),
                    version: None,
                },
                IndexItem {
                    external_id: "doc".into(),
                    field: "tag".into(),
                    value: FieldValue::Number(7.0),
                    version: None,
                },
            ],
            request_id: Some("idempotency-key".into()),
        },
    };

    let cost = ready(estimate_record_or_retain(&entry, &ctx));
    assert!(
        cost.active > 0 || cost.frozen > 0,
        "the earlier indexed field and request metadata survive the later validation error"
    );
}

#[test]
fn replacement_deletion_prefix_is_charged_when_a_later_doc_is_invalid() {
    let mut ctx = TestContext::default();
    ctx.keyword("c", "tag");
    ctx.known.insert(("c".into(), "old".into()));
    ctx.coverage
        .insert(("c".into(), "old".into()), BTreeSet::from(["tag".into()]));
    let entry = RaftLogEntry::ReplaceDocs {
        collection_id: "c".into(),
        req: ReplaceDocsRequest {
            docs: vec![
                ReplaceDocItem {
                    external_id: "old".into(),
                    version: None,
                    fields: BTreeMap::new(),
                },
                ReplaceDocItem {
                    external_id: "later".into(),
                    version: None,
                    fields: BTreeMap::from([(
                        "missing".into(),
                        FieldValue::String("invalid".into()),
                    )]),
                },
            ],
        },
    };

    let cost = ready(estimate_record_or_retain(&entry, &ctx));
    assert!(
        cost.active > 0 || cost.frozen > 0,
        "the first replacement deletes tag before the second document reports its error"
    );
}

#[test]
fn invalid_replacement_doc_does_not_hide_a_later_valid_document_cost() {
    let mut ctx = TestContext::default();
    ctx.keyword("c", "tag");
    let entry = RaftLogEntry::ReplaceDocs {
        collection_id: "c".into(),
        req: ReplaceDocsRequest {
            docs: vec![
                ReplaceDocItem {
                    external_id: "bad".into(),
                    version: None,
                    fields: BTreeMap::from([(
                        "missing".into(),
                        FieldValue::String("invalid".into()),
                    )]),
                },
                ReplaceDocItem {
                    external_id: "later".into(),
                    version: None,
                    fields: BTreeMap::from([("tag".into(), FieldValue::String("kept".into()))]),
                },
            ],
        },
    };

    let cost = ready(estimate_record_or_retain(&entry, &ctx));
    assert!(
        cost.active > 0 || cost.frozen > 0,
        "replace_docs continues after an item error and applies the later document"
    );
}

#[test]
fn invalid_new_replacement_document_charges_its_interned_id_metadata() {
    let mut ctx = TestContext::default();
    ctx.keyword("c", "tag");
    let entry = RaftLogEntry::ReplaceDocs {
        collection_id: "c".into(),
        req: ReplaceDocsRequest {
            docs: vec![ReplaceDocItem {
                external_id: "new-invalid".into(),
                version: None,
                fields: BTreeMap::from([("missing".into(), FieldValue::String("invalid".into()))]),
            }],
        },
    };

    let cost = ready(estimate_record_or_retain(&entry, &ctx));
    assert!(
        cost.active > 0 || cost.frozen > 0,
        "replace_one_doc interns the ID before it validates fields"
    );
}

#[test]
fn invalid_existing_index_value_charges_dirty_declared_field_without_coverage() {
    let mut ctx = TestContext::default();
    ctx.keyword("c", "tag");
    ctx.known.insert(("c".into(), "existing".into()));
    let entry = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "existing".into(),
                field: "tag".into(),
                value: FieldValue::Number(7.0),
                version: None,
            }],
            request_id: None,
        },
    };

    let cost = ready(estimate_record_or_retain(&entry, &ctx));
    assert!(
        cost.active > 0 || cost.frozen > 0,
        "apply errors mark the declared field dirty even when it had no prior coverage"
    );
}

#[test]
fn stale_invalid_index_item_does_not_hide_later_valid_work() {
    let mut ctx = TestContext::default();
    ctx.keyword("c", "tag");
    ctx.known.insert(("c".into(), "existing".into()));
    ctx.stale
        .insert(("c".into(), "existing".into(), "tag".into(), 7));
    let later = IndexItem {
        external_id: "existing".into(),
        field: "tag".into(),
        value: FieldValue::String("kept".into()),
        version: Some(8),
    };
    let stale_then_later = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![
                IndexItem {
                    external_id: "existing".into(),
                    field: "tag".into(),
                    value: FieldValue::Number(7.0),
                    version: Some(7),
                },
                later.clone(),
            ],
            request_id: None,
        },
    };
    let later_only = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![later],
            request_id: None,
        },
    };
    assert_eq!(
        ready(estimate_record_or_retain(&stale_then_later, &ctx)),
        ready(estimate_record_or_retain(&later_only, &ctx)),
        "storage skips stale cells before value validation and continues the batch"
    );
}

#[test]
fn missing_collection_is_a_noop_cost_not_indefinite_context_retention() {
    let ctx = TestContext::default();
    let entry = RaftLogEntry::Index {
        collection_id: "missing".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "doc".into(),
                field: "tag".into(),
                value: FieldValue::String("value".into()),
                version: None,
            }],
            request_id: None,
        },
    };
    assert_eq!(
        ready(estimate_record_or_retain(&entry, &ctx)),
        RecordCost::default()
    );
}

#[test]
fn text_record_cost_uses_the_borrowed_upper_bound_without_normalized_tokens() {
    let mut ctx = TestContext::default();
    ctx.text("c", "body");
    let entry = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "doc".into(),
                field: "body".into(),
                value: FieldValue::String("İstanbul rust".into()),
                version: None,
            }],
            request_id: None,
        },
    };
    let cost = ready(estimate_record_or_retain(&entry, &ctx));
    assert!(cost.active > 0 || cost.frozen > 0);
}

mod exact_default_ngram;

mod prepared_text;
