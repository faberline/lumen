use crate::ingest::domain::change_record_cost::tests::TestContext;
use crate::ingest::domain::change_record_cost::{
    estimate_record, estimate_record_prepared_text, RecordCost,
};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::Analyzer;

pub(super) fn total_cost(cost: RecordCost) -> usize {
    cost.active + cost.frozen + cost.prepublish
}

fn cost_delta(larger: RecordCost, smaller: RecordCost) -> RecordCost {
    RecordCost {
        active: larger.active - smaller.active,
        frozen: larger.frozen - smaller.frozen,
        prepublish: larger.prepublish - smaller.prepublish,
    }
}

#[test]
fn prepared_jieba_removes_only_the_normalized_term_map_charge() {
    let mut ctx = TestContext::default();
    ctx.text("c", "body");
    ctx.fields
        .get_mut(&(String::from("c"), String::from("body")))
        .unwrap()
        .analyzer = Some(Analyzer::Jieba);
    let entry = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            request_id: None,
            items: vec![IndexItem {
                external_id: "doc".into(),
                field: "body".into(),
                value: FieldValue::String("搜尋引擎 ΣΟΣ".into()),
                version: None,
            }],
        },
    };
    assert!(
        total_cost(estimate_record_prepared_text(&entry, &ctx).unwrap())
            < total_cost(estimate_record(&entry, &ctx).unwrap())
    );
}

#[test]
fn prepared_text_removes_only_the_normalized_term_map_charge() {
    let mut ctx = TestContext::default();
    ctx.text("c", "body");
    ctx.keyword("c", "tag");
    let body = "distinct-term ".repeat(4_096);
    let text_only = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "doc".into(),
                field: "body".into(),
                value: FieldValue::String(body.clone()),
                version: Some(1),
            }],
            request_id: Some("text-request".into()),
        },
    };
    let mixed_prefix_error = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![
                IndexItem {
                    external_id: "doc".into(),
                    field: "body".into(),
                    value: FieldValue::String(body),
                    version: Some(1),
                },
                IndexItem {
                    external_id: "doc".into(),
                    field: "tag".into(),
                    value: FieldValue::String("kept".into()),
                    version: Some(2),
                },
                IndexItem {
                    external_id: "doc".into(),
                    field: "tag".into(),
                    value: FieldValue::Number(7.0),
                    version: Some(3),
                },
            ],
            request_id: Some("mixed-request".into()),
        },
    };

    let normal_text = estimate_record(&text_only, &ctx).unwrap();
    let prepared_text = estimate_record_prepared_text(&text_only, &ctx).unwrap();
    assert!(
        total_cost(prepared_text) < total_cost(normal_text),
        "prepared Text must not reserve an in-RAM normalized term map"
    );
    assert!(
        total_cost(prepared_text) > 0,
        "Text row, ID, version, and request metadata stay charged"
    );
    assert_eq!(
        estimate_record(&text_only, &ctx).unwrap(),
        normal_text,
        "the ordinary estimator remains unchanged"
    );

    let normal_mixed = estimate_record(&mixed_prefix_error, &ctx).unwrap();
    let prepared_mixed = estimate_record_prepared_text(&mixed_prefix_error, &ctx).unwrap();
    assert_eq!(
        cost_delta(normal_mixed, prepared_mixed),
        cost_delta(normal_text, prepared_text),
        "keyword, ID/version/request metadata, and later partial-error work stay charged"
    );
}
