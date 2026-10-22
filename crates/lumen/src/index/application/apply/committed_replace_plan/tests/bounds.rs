use std::time::Instant;

use crate::index::application::apply::committed_index_plan::PlanView;
use crate::index::application::apply::committed_replace_plan::pass::plan;
use crate::index::application::apply::committed_replace_plan::tests::{
    item, parsed, view, wire, View,
};
use crate::index::application::apply::committed_replace_plan::{
    OldFieldsBound, ParsedValues, ReplacePlanView,
};
use crate::ingest::infrastructure::wal::borrowed_replace_spool::BorrowedReplaceSpoolDoc as ReplaceDocDescriptor;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::schema::FieldType;

#[test]
fn reserve_refuses_before_descriptor_or_action_allocation() {
    let raw = wire(vec![item("new", "kw", FieldValue::String("v".into()))]);
    let scan = FastIndexScanner::parse(&raw).unwrap();
    assert!(plan(
        &scan,
        &[ReplaceDocDescriptor {
            external_id: "new",
            version: None,
            fields_start: 0,
            fields_end: 1
        }],
        &parsed(),
        &view(),
        Instant::now(),
        |_| Err(anyhow::anyhow!("refused"))
    )
    .is_err());
}

#[test]
fn outer_collection_failure_is_err_not_a_per_item_result() {
    let raw = wire(vec![item("new", "kw", FieldValue::String("v".into()))]);
    let scan = FastIndexScanner::parse(&raw).unwrap();
    struct Gone(View);
    impl PlanView for Gone {
        fn engine_epoch(&self) -> u64 {
            1
        }
        fn collection_generation(&self) -> u64 {
            2
        }
        fn schema_version(&self) -> u32 {
            3
        }
        fn data_version(&self) -> u64 {
            4
        }
        fn revision(&self) -> u64 {
            5
        }
        fn interner_len(&self) -> usize {
            10
        }
        fn is_live(&self) -> bool {
            false
        }
        fn field_type(&self, _: &str) -> Option<FieldType> {
            None
        }
        fn vector_dimension(&self, _: &str) -> Option<u32> {
            None
        }
        fn id(&self, _: &str) -> Option<u32> {
            None
        }
        fn has_cell(&self, _: u32, _: &str) -> bool {
            false
        }
        fn cell_version(&self, _: u32, _: &str) -> Option<u64> {
            None
        }
        fn request_deadline(&self, _: &str) -> Option<Instant> {
            None
        }
    }
    impl ReplacePlanView for Gone {
        fn doc_version(&self, _: u32) -> Option<u64> {
            None
        }
        fn old_fields(&self, _: u32) -> Option<&[String]> {
            None
        }
        fn old_fields_bound(&self, _: u32) -> OldFieldsBound {
            OldFieldsBound::default()
        }
        fn existing_unchanged(
            &self,
            _: u32,
            _: &str,
            _: FieldType,
            _: usize,
            _: Option<u64>,
        ) -> bool {
            false
        }
    }
    assert!(plan(
        &scan,
        &[ReplaceDocDescriptor {
            external_id: "new",
            version: None,
            fields_start: 0,
            fields_end: 1
        }],
        &parsed(),
        &Gone(view()),
        Instant::now(),
        |_| Ok(())
    )
    .is_err());
}

#[test]
fn old_coverage_reserve_refuses_before_omitted_drop_metadata_exists() {
    let raw = wire(vec![]);
    let scan = FastIndexScanner::parse(&raw).unwrap();
    let mut v = view();
    v.coverage
        .insert(2, (0..20_000).map(|n| format!("old_{n}")).collect());
    assert!(plan(
        &scan,
        &[ReplaceDocDescriptor {
            external_id: "old",
            version: None,
            fields_start: 0,
            fields_end: 0
        }],
        &parsed(),
        &v,
        Instant::now(),
        |bytes| {
            assert!(bytes > 20_000 * 256);
            Err(anyhow::anyhow!("refused"))
        }
    )
    .is_err());
}

#[test]
fn malformed_descriptor_proof_fails_before_stale_shortcut() {
    let raw = wire(vec![item("old", "kw", FieldValue::String("x".into()))]);
    let scan = FastIndexScanner::parse(&raw).unwrap();
    let mut v = view();
    v.versions.insert(2, 9);
    assert!(plan(
        &scan,
        &[ReplaceDocDescriptor {
            external_id: "old",
            version: Some(8),
            fields_start: 1,
            fields_end: 1
        }],
        &parsed(),
        &v,
        Instant::now(),
        |_| Ok(())
    )
    .is_err());
}

#[test]
fn thirty_two_docs_can_plan_more_than_one_thousand_flattened_fields() {
    let mut items = Vec::new();
    let mut docs = Vec::new();
    let mut v = View::default();
    for d in 0..32 {
        let start = items.len();
        let id = format!("doc-{d:02}");
        for f in 0..33 {
            let field = format!("f-{f:02}");
            v.fields.insert(field.clone(), FieldType::Keyword);
            items.push(item(&id, &field, FieldValue::String("v".into())));
        }
        docs.push(ReplaceDocDescriptor {
            external_id: Box::leak(id.into_boxed_str()),
            version: None,
            fields_start: start,
            fields_end: items.len(),
        });
    }
    let raw = wire(items);
    let scan = FastIndexScanner::parse(&raw).unwrap();
    let p = plan(
        &scan,
        &docs,
        &ParsedValues::new(),
        &v,
        Instant::now(),
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(p.outcomes.len(), 32);
    assert!(p.scalar.source_items > 1000);
}
