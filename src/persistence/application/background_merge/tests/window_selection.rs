use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, SegmentFormat, SegmentKind, SegmentReference, SegmentRole,
};
use crate::persistence::infrastructure::segment_rdb_store::flat_layout::collection_checkpoint_dir_name;
use crate::persistence::infrastructure::segment_rdb_store::merge_selection::select_staged_delta_window;
use std::path::Path;

/// `select_staged_delta_window` is the whole of a job's scope decision, so
/// its ordering is asserted directly rather than through the worker: the
/// collection holding the deepest stack wins, an equal-depth tie keeps
/// catalog order, and every field tied at that collection's deepest
/// eligible depth is returned in deterministic field-name order with its
/// own merge window.
#[test]
fn select_staged_delta_window_selects_all_eligible_fields_of_deepest_collection() {
    fn write(root: &Path, relative: &str, bytes: usize) {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, vec![b'x'; bytes]).unwrap();
    }

    fn segment(
        root: &Path,
        collection: &str,
        field: &str,
        kind: SegmentKind,
        ordinal: u32,
        bytes: usize,
    ) -> SegmentReference {
        let relative = format!(
            "{}/{field}-{ordinal}-{}.lseg",
            collection_checkpoint_dir_name(collection),
            match kind {
                SegmentKind::Base => "base",
                SegmentKind::Delta => "delta",
            }
        );
        write(root, &relative, bytes);
        SegmentReference {
            role: SegmentRole::Field,
            field: Some(field.to_owned()),
            ordinal,
            kind,
            format: SegmentFormat::LsegV1,
            path: relative,
            local_rows: None,
            applied_seq: None,
            payload_sha256: None,
        }
    }

    fn catalog(
        root: &Path,
        collection: &str,
        depths: &[(&str, u32)],
        base_bytes: usize,
    ) -> CollectionCatalog {
        let mut segments = Vec::new();
        for (field, depth) in depths {
            segments.push(segment(
                root,
                collection,
                field,
                SegmentKind::Base,
                0,
                base_bytes,
            ));
            for ordinal in 1..=*depth {
                segments.push(segment(
                    root,
                    collection,
                    field,
                    SegmentKind::Delta,
                    ordinal,
                    1,
                ));
            }
        }
        CollectionCatalog {
            collection_id: collection.to_owned(),
            collection_generation: 1,
            schema_version: 1,
            data_version: 1,
            schema: serde_json::json!({}),
            segments,
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    // A base far larger than the deltas keeps every candidate a
    // delta-only merge, so only the stack depth decides which collection
    // is selected.
    let deepest = vec![
        catalog(root, "first", &[("a", 4), ("b", 4)], 4096),
        catalog(root, "second", &[("a", 6), ("b", 3)], 4096),
    ];
    let selected = select_staged_delta_window(root, &deepest).unwrap();
    assert_eq!(selected.len(), 1, "the deepest field is the only candidate");
    assert_eq!(selected[0].collection_id, "second");
    assert_eq!(selected[0].field, "a");
    assert_eq!(selected[0].inputs.len(), 2);
    assert!(
        !selected[0].includes_base,
        "deltas smaller than the base must not fold the base in"
    );

    // Only fields at the selected collection depth are admitted. The
    // shallower eligible field remains for a later scheduling pass.
    let both_eligible = vec![
        catalog(root, "third", &[("a", 4), ("b", 4)], 4096),
        catalog(root, "fourth", &[("a", 7), ("b", 6)], 4096),
    ];
    let selected = select_staged_delta_window(root, &both_eligible).unwrap();
    assert_eq!(
        selected
            .iter()
            .map(|candidate| (candidate.collection_id.as_str(), candidate.field.as_str()))
            .collect::<Vec<_>>(),
        vec![("fourth", "a")],
        "the deepest collection and deepest field are selected deterministically"
    );
    assert!(selected
        .iter()
        .all(|candidate| candidate.inputs.len() == 2 && !candidate.includes_base));

    let tied = vec![
        catalog(root, "first", &[("a", 7), ("b", 7)], 4096),
        catalog(root, "second", &[("a", 7), ("b", 7)], 4096),
    ];
    let selected = select_staged_delta_window(root, &tied).unwrap();
    assert_eq!(
        selected
            .iter()
            .map(|candidate| (candidate.collection_id.as_str(), candidate.field.as_str()))
            .collect::<Vec<_>>(),
        vec![("first", "a"), ("first", "b")],
        "an equal-depth tie keeps catalog order and selects all eligible fields"
    );

    let below_threshold = vec![catalog(root, "first", &[("a", 3)], 4096)];
    assert!(
        select_staged_delta_window(root, &below_threshold)
            .unwrap()
            .is_empty(),
        "a stack below the merge threshold is not a candidate"
    );

    // Deltas that have reached the base size fold the base in.
    let over_base = vec![catalog(root, "first", &[("a", 4)], 2)];
    let selected = select_staged_delta_window(root, &over_base).unwrap();
    assert_eq!(selected.len(), 1);
    assert!(
        selected[0].includes_base,
        "a delta stack at or past the base size must fold the base in"
    );
}

#[test]
fn select_staged_delta_window_uses_smallest_adjacent_pair() {
    fn write(root: &Path, relative: &str, bytes: usize) {
        std::fs::write(root.join(relative), vec![b'x'; bytes]).unwrap();
    }

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let collection = {
        let mut collections = vec![{
            let mut collection = CollectionCatalog {
                collection_id: "pair".to_owned(),
                collection_generation: 1,
                schema_version: 1,
                data_version: 1,
                schema: serde_json::json!({}),
                segments: Vec::new(),
            };
            collection.segments.push(SegmentReference {
                role: SegmentRole::Field,
                field: Some("f".to_owned()),
                ordinal: 0,
                kind: SegmentKind::Base,
                format: SegmentFormat::LsegV1,
                path: "base.lseg".to_owned(),
                local_rows: None,
                applied_seq: None,
                payload_sha256: None,
            });
            for ordinal in 1..=5 {
                collection.segments.push(SegmentReference {
                    role: SegmentRole::Field,
                    field: Some("f".to_owned()),
                    ordinal,
                    kind: SegmentKind::Delta,
                    format: SegmentFormat::LsegV1,
                    path: format!("delta-{ordinal}.lseg"),
                    local_rows: None,
                    applied_seq: None,
                    payload_sha256: None,
                });
            }
            collection
        }];
        write(root, "base.lseg", 1000);
        for (ordinal, bytes) in [20, 1, 2, 30, 4].into_iter().enumerate() {
            write(root, &format!("delta-{}.lseg", ordinal + 1), bytes);
        }
        collections.pop().unwrap()
    };
    let selected = select_staged_delta_window(root, &[collection.clone()]).unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(
        selected[0]
            .inputs
            .iter()
            .map(|segment| segment.ordinal)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
}
