use std::time::Duration;

use crate::app::observability::metrics::labels::{apply_item_count, kind_label, ApplyKind};
use crate::app::observability::metrics::Metrics;

/// #4326: `ApplyKind::from_entry`/`kind_label`/`apply_item_count` must
/// agree with the `RaftLogEntry` variant they classify, and
/// `observe_coordinator_apply` must land in the right `kind`'s
/// per-kind atomic slot without disturbing a sibling kind.
#[test]
fn apply_kind_classifies_entries_and_counts_items() {
    use crate::shared_kernel::log_entry::RaftLogEntry;
    use crate::shared_kernel::types::document::{
        BatchUnindexDocsRequest, FieldValue, IndexItem, IndexRequest, ReplaceDocItem,
        ReplaceDocsRequest,
    };

    let index_entry = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![
                IndexItem {
                    external_id: "1".into(),
                    field: "f".into(),
                    value: FieldValue::String("a".into()),
                    version: None,
                },
                IndexItem {
                    external_id: "2".into(),
                    field: "f".into(),
                    value: FieldValue::String("b".into()),
                    version: None,
                },
                IndexItem {
                    external_id: "3".into(),
                    field: "f".into(),
                    value: FieldValue::String("c".into()),
                    version: None,
                },
            ],
            request_id: None,
        },
    };
    assert_eq!(kind_label(&index_entry), "index");
    assert_eq!(apply_item_count(&index_entry), 3);

    let replace_entry = RaftLogEntry::ReplaceDocs {
        collection_id: "c".into(),
        req: ReplaceDocsRequest {
            docs: vec![ReplaceDocItem {
                external_id: "1".into(),
                version: None,
                fields: Default::default(),
            }],
        },
    };
    assert_eq!(kind_label(&replace_entry), "replace");
    assert_eq!(apply_item_count(&replace_entry), 1);

    let unindex_entry = RaftLogEntry::UnindexDocs {
        collection_id: "c".into(),
        req: BatchUnindexDocsRequest {
            external_ids: vec!["1".into(), "2".into()],
        },
    };
    assert_eq!(kind_label(&unindex_entry), "unindex");
    assert_eq!(apply_item_count(&unindex_entry), 2);

    let drop_field_entry = RaftLogEntry::DropField {
        collection_id: "c".into(),
        field_name: "f".into(),
    };
    assert_eq!(kind_label(&drop_field_entry), "drop_field");
    assert_eq!(apply_item_count(&drop_field_entry), 1);

    let m = Metrics::new();
    m.observe_coordinator_apply(ApplyKind::Index, 3, Duration::from_millis(2));
    m.observe_coordinator_apply(ApplyKind::UnindexDocs, 2, Duration::from_micros(500));

    let out = m.render();
    assert!(
        out.contains("lumen_coordinator_apply_seconds_count{kind=\"index\"} 1"),
        "missing index apply count in:\n{out}"
    );
    assert!(
        out.contains("lumen_coordinator_apply_items_total{kind=\"index\"} 3"),
        "missing index item total in:\n{out}"
    );
    assert!(
        out.contains("lumen_coordinator_apply_seconds_count{kind=\"unindex\"} 1"),
        "missing unindex apply count in:\n{out}"
    );
    assert!(
        out.contains("lumen_coordinator_apply_items_total{kind=\"unindex\"} 2"),
        "missing unindex item total in:\n{out}"
    );
    // Every other kind still emits its row, at zero, so a scrape config
    // never has to tolerate a kind appearing only after its first write.
    assert!(
        out.contains("lumen_coordinator_apply_seconds_count{kind=\"replace\"} 0"),
        "unobserved kind must still emit its zero row in:\n{out}"
    );
    assert!(
        out.contains("lumen_coordinator_apply_items_total{kind=\"replace\"} 0"),
        "unobserved kind must still emit its zero row in:\n{out}"
    );
}
