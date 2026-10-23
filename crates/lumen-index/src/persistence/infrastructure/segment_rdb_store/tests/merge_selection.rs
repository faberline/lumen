use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, SegmentFormat, SegmentKind, SegmentReference, SegmentRole,
};
use crate::persistence::infrastructure::segment_rdb_store::compacted_fields::encode_staged_candidates_in_order;
use crate::persistence::infrastructure::segment_rdb_store::merge_selection::{
    select_staged_delta_window, StagedMergeCandidate,
};
use crate::persistence::infrastructure::segment_rdb_store::{MergeObserver, MergePhase};
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::{Arc, Mutex};

fn cohort_catalog(root: &Path, fields: &[(&str, usize, usize)]) -> CollectionCatalog {
    let mut segments = Vec::new();
    for (field, depth, bytes) in fields {
        let base_path = format!("{field}.base.lseg");
        std::fs::write(root.join(&base_path), vec![b'b'; 1_000_000]).unwrap();
        segments.push(SegmentReference {
            role: SegmentRole::Field,
            field: Some((*field).to_owned()),
            ordinal: 0,
            kind: SegmentKind::Base,
            format: SegmentFormat::LsegV1,
            path: base_path,
            local_rows: None,
            applied_seq: None,
            payload_sha256: None,
        });
        for ordinal in 1..=*depth {
            let path = format!("{field}.{ordinal}.delta.lseg");
            std::fs::write(root.join(&path), vec![b'd'; *bytes]).unwrap();
            segments.push(SegmentReference {
                role: SegmentRole::Field,
                field: Some((*field).to_owned()),
                ordinal: ordinal as u32,
                kind: SegmentKind::Delta,
                format: SegmentFormat::LsegV1,
                path,
                local_rows: None,
                applied_seq: None,
                payload_sha256: None,
            });
        }
    }
    CollectionCatalog {
        collection_id: "cohort".to_owned(),
        collection_generation: 1,
        schema_version: 1,
        data_version: 1,
        schema: serde_json::json!({}),
        segments,
    }
}

#[test]
fn selector_keeps_small_fourteen_field_cohort_in_one_bounded_job() {
    // Fourteen fields in the durable workload share one checkpoint cut.
    // Splitting a small cohort repeats the whole-root publication work,
    // even when all selected inputs fit inside the existing byte budget.
    for (count, delta_bytes, expected) in [
        (14, 1, 14),
        (17, 1, 16),
        (14, 1024 * 1024, 12),
        (9, 2 * 1024 * 1024, 8),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let names: Vec<_> = (0..count).map(|index| format!("f{index:02}")).collect();
        let fields: Vec<_> = names
            .iter()
            .map(|name| (name.as_str(), 4, delta_bytes))
            .collect();
        let catalog = cohort_catalog(dir.path(), &fields);
        // Sparse base files keep these fixtures delta-only without
        // allocating or reading a large corpus. Selection uses file sizes.
        for name in &names {
            OpenOptions::new()
                .write(true)
                .open(dir.path().join(format!("{name}.base.lseg")))
                .unwrap()
                .set_len(64 * 1024 * 1024)
                .unwrap();
        }
        let selected = select_staged_delta_window(dir.path(), &[catalog]).unwrap();
        assert_eq!(
            selected.len(),
            expected,
            "count={count}, delta_bytes={delta_bytes}: small cohorts share one publication; large cohorts retain their old bound"
        );
        assert_eq!(
            selected
                .iter()
                .map(|candidate| candidate.field.as_str())
                .collect::<Vec<_>>(),
            names[..expected]
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            "field ordering stays deterministic"
        );
        assert!(selected
            .iter()
            .all(|candidate| { !candidate.includes_base && candidate.inputs.len() == 2 }));
        if selected.len() > 8 {
            assert!(
                selected.len() * 2 * delta_bytes <= 24 * 1024 * 1024,
                "an expanded cohort must fit the unchanged total byte budget"
            );
        }
    }
}

#[test]
fn selector_bounds_cohort_by_fields_bytes_and_priority() {
    let dir = tempfile::tempdir().unwrap();
    let tiny = cohort_catalog(
        dir.path(),
        &[
            ("g", 4, 1),
            ("a", 4, 1),
            ("f", 4, 1),
            ("b", 4, 1),
            ("e", 4, 1),
            ("c", 4, 1),
            ("d", 4, 1),
        ],
    );
    let selected = select_staged_delta_window(dir.path(), &[tiny]).unwrap();
    assert_eq!(selected.len(), 7);
    assert_eq!(
        selected
            .iter()
            .map(|candidate| candidate.field.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "c", "d", "e", "f", "g"]
    );

    let dir = tempfile::tempdir().unwrap();
    let ten = cohort_catalog(
        dir.path(),
        &[
            ("a", 4, 2_500_000),
            ("b", 4, 2_500_000),
            ("c", 4, 2_500_000),
        ],
    );
    assert_eq!(
        select_staged_delta_window(dir.path(), &[ten])
            .unwrap()
            .len(),
        2
    );

    let dir = tempfile::tempdir().unwrap();
    let mixed = cohort_catalog(
        dir.path(),
        &[("z", 6, 1), ("a", 7, 1), ("b", 7, 1), ("c", 3, 1)],
    );
    let selected = select_staged_delta_window(dir.path(), &[mixed]).unwrap();
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].field, "a");
    assert_eq!(selected[1].field, "b");

    let dir = tempfile::tempdir().unwrap();
    let first_large = cohort_catalog(dir.path(), &[("a", 4, 6_250_000), ("b", 4, 1)]);
    let selected = select_staged_delta_window(dir.path(), &[first_large]).unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].field, "a");
}

struct BlockingBeforeEncode {
    state: Mutex<(usize, bool)>,
    wake: std::sync::Condvar,
}

impl MergeObserver for BlockingBeforeEncode {
    fn observe(&self, phase: MergePhase) -> std::io::Result<()> {
        assert_eq!(phase, MergePhase::BeforeEncode);
        let mut state = self.state.lock().unwrap();
        state.0 += 1;
        self.wake.notify_all();
        if state.0 == 1 {
            while !state.1 {
                state = self.wake.wait(state).unwrap();
            }
        }
        Ok(())
    }
}

fn test_merge_candidate(field: &str) -> StagedMergeCandidate {
    let segment = SegmentReference {
        role: SegmentRole::Field,
        field: Some(field.to_owned()),
        ordinal: 1,
        kind: SegmentKind::Delta,
        format: SegmentFormat::LsegV1,
        path: format!("{field}.delta.lseg"),
        local_rows: None,
        applied_seq: Some(1),
        payload_sha256: None,
    };
    StagedMergeCandidate {
        collection_index: 0,
        collection_id: "test".to_owned(),
        field: field.to_owned(),
        inputs: vec![segment.clone()],
        base: SegmentReference {
            kind: SegmentKind::Base,
            path: format!("{field}.base.lseg"),
            ..segment
        },
        includes_base: false,
    }
}

#[test]
fn sequential_candidate_encoding_waits_before_starting_the_next_candidate() {
    let observer = Arc::new(BlockingBeforeEncode {
        state: Mutex::new((0, false)),
        wake: std::sync::Condvar::new(),
    });
    let encoded = Arc::new(Mutex::new(Vec::new()));
    let candidates = vec![test_merge_candidate("alpha"), test_merge_candidate("beta")];
    let worker_observer = Arc::clone(&observer);
    let worker_encoded = Arc::clone(&encoded);
    let worker = std::thread::spawn(move || {
        encode_staged_candidates_in_order(candidates, worker_observer.as_ref(), |candidate| {
            let field = candidate.field;
            worker_encoded.lock().unwrap().push(field.clone());
            Ok(field)
        })
    });

    let mut state = observer.state.lock().unwrap();
    while state.0 == 0 {
        state = observer.wake.wait(state).unwrap();
    }
    assert_eq!(state.0, 1);
    assert!(encoded.lock().unwrap().is_empty());
    state.1 = true;
    observer.wake.notify_all();
    drop(state);

    assert_eq!(worker.join().unwrap().unwrap(), vec!["alpha", "beta"]);
    assert_eq!(encoded.lock().unwrap().as_slice(), ["alpha", "beta"]);
}
