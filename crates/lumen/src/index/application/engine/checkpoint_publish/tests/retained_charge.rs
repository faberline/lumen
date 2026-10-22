//! A checkpoint's publication releases every committed row's retained charge
//! from the change budget.

use std::collections::BTreeMap;

use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::IndexItem;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::schema::{
    Analyzer, CreateCollectionRequest, FieldSpec, FieldType,
};

fn text_field(analyzer: Analyzer) -> FieldSpec {
    FieldSpec {
        field_type: FieldType::Text,
        analyzer: Some(analyzer),
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}

/// Admit N committed rows that each retain a real
/// [`crate::ingest::domain::change_budget::RetainedCharge`], run one real checkpoint
/// freeze + write + publish through the exact Engine code path, and
/// assert the process-wide budget's `active + frozen` (its `total`) after
/// publication. If publication does not release every payload, this must
/// fail before any fix and pass after.
#[test]
fn checkpoint_publish_releases_every_committed_row_charge() {
    let budget =
        crate::ingest::domain::change_budget::ChangeBudget::with_hard_limit(8 * 1024 * 1024);
    let engine = Engine::with_change_budget(budget.clone());
    let mut fields = BTreeMap::new();
    fields.insert("body".to_string(), text_field(Analyzer::WhitespaceLower));
    engine
        .create_collection_inner("c", CreateCollectionRequest { fields })
        .unwrap();

    let mut charges = Vec::new();
    for i in 0..50 {
        let charge = engine
            .changes
            .owner
            .try_reserve(1024)
            .unwrap()
            .commit_retained()
            .unwrap();
        engine
            .index_inner(
                "c",
                IndexRequest {
                    items: vec![IndexItem {
                        external_id: format!("doc{i}"),
                        field: "body".to_string(),
                        value: FieldValue::String("hello world from lumen".to_string()),
                        version: None,
                    }],
                    request_id: None,
                },
                Some(&charge),
                None,
            )
            .unwrap();
        charges.push(charge);
    }
    // The caller's own handles drop here; the journal rows still hold
    // their own clones of the same retained charges.
    drop(charges);
    let before = budget.snapshot();
    assert!(
        before.total > 0,
        "committed rows must remain charged before any checkpoint runs"
    );

    let root = tempfile::tempdir().unwrap();
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    let mut capture = frozen.write(root.path(), 0).unwrap();
    engine
        .bind_checkpoint_origins(root.path(), &mut capture)
        .unwrap();
    engine.acknowledge_record_charges(&capture).unwrap();
    drop(capture);
    // Mirrors the real driver: `PendingFrozenLease::disarm` drops the
    // original `FrozenCheckpoint` only after publication and live
    // binding both succeed (`segment_rdb.rs`'s `pending.disarm()`).
    drop(frozen);

    let after = budget.snapshot();
    assert_eq!(
        after.total, 0,
        "publishing a checkpoint that captured every committed row must \
             release each row's retained charge: active={} frozen={} reserved={}",
        after.active, after.frozen, after.reserved
    );
}
