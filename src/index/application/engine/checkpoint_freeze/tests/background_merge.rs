//! A background merge leaves the live dirty ownership alone: its capture takes
//! no dirty rows, changes or deltas, and its bind keeps the collection's dirty
//! fields and its full-checkpoint flag.

use std::collections::{BTreeMap, BTreeSet};

use crate::index::application::checkpoint_capture::CheckpointCollectionIdentity;
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::IndexItem;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};

#[test]
fn background_merge_capture_and_bind_preserve_live_dirty_ownership() {
    let engine = Engine::new();
    engine
        .create_collection(
            "docs",
            serde_json::from_value(serde_json::json!({"fields":{"email":{"type":"keyword"}}}))
                .unwrap(),
        )
        .unwrap();
    engine
        .index(
            "docs",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: "one".into(),
                    field: "email".into(),
                    value: FieldValue::String("old".into()),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
    let dirty = engine.checkpoint_dirty_fields().unwrap();
    let (generation, schema, fields) = dirty["docs"].clone();
    assert_eq!(fields, BTreeSet::from(["email".to_owned()]));
    let expected = BTreeMap::from([(
        "docs".to_owned(),
        CheckpointCollectionIdentity {
            generation,
            schema_version: schema,
            data_version: 0,
        },
    )]);
    let mut capture = engine.capture_background_merge(expected).unwrap();
    assert!(capture.field_dirty.is_empty());
    assert!(capture.frozen_changes.is_empty());
    assert!(capture.field_deltas.is_empty());
    {
        let mut state = engine.state.write().unwrap();
        state
            .collections
            .get_mut("docs")
            .unwrap()
            .requires_full_checkpoint = true;
    }
    let root = tempfile::tempdir().unwrap();
    engine
        .bind_background_merge(root.path(), &mut capture)
        .unwrap();
    assert!(engine.checkpoint_dirty_fields().unwrap()["docs"]
        .2
        .contains("email"));
    assert!(engine.state.read().unwrap().collections["docs"].requires_full_checkpoint);
}
