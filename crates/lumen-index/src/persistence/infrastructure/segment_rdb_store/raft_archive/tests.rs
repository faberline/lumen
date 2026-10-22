use crate::index::application::engine::Engine;
use crate::persistence::infrastructure::segment_rdb_store::flat_layout::collection_checkpoint_dir_name;
use crate::persistence::infrastructure::segment_rdb_store::raft_archive::write_archive;
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use std::sync::Arc;

fn fixture() -> (tempfile::TempDir, Vec<u8>) {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    let name = store.save_required(&engine, 2).unwrap();
    let mut bytes = Vec::new();
    write_archive(&store.generations.generation_path(&name), 2, &mut bytes).unwrap();
    (dir, bytes)
}

#[test]
fn archive_roundtrip_publishes_exact_sequence_and_survives_cold_open() {
    let (_source, bytes) = fixture();
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let live = Arc::new(Engine::new());
    store.validate_raft_archive(&mut bytes.as_slice()).unwrap();
    let mut observed = None;
    assert_eq!(
        store
            .restore_raft_archive(&live, &mut bytes.as_slice(), |seq| observed = Some(seq))
            .unwrap(),
        2
    );
    assert_eq!(observed, Some(2));
    assert_eq!(
        store.load_current_generation().unwrap().unwrap().sequence,
        2
    );
    // A subsequent checkpoint uses the imported immutable generation path.
    store.save_required(&live, 2).unwrap();
}

#[test]
fn malformed_archive_metadata_and_payload_preserve_current() {
    let (_source, bytes) = fixture();
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let live = Arc::new(Engine::new());
    store.save_required(&live, 1).unwrap();
    let before = std::fs::read(dir.path().join("CURRENT")).unwrap();
    let mut mutations = Vec::new();
    for offset in [0, 8, 9, bytes.len() - 1] {
        let mut bad = bytes.clone();
        bad[offset] ^= 0xff;
        mutations.push(bad);
    }
    mutations.push(bytes[..bytes.len() - 1].to_vec());
    let mut trailing = bytes.clone();
    trailing.push(0);
    mutations.push(trailing);
    let mut huge_path = bytes.clone();
    huge_path[25..33].copy_from_slice(&u64::MAX.to_le_bytes());
    mutations.push(huge_path);
    let mut traversal = bytes.clone();
    traversal[33..36].copy_from_slice(b"../");
    mutations.push(traversal);
    let mut duplicate = bytes.clone();
    duplicate[17..25].copy_from_slice(&2u64.to_le_bytes());
    duplicate.extend_from_slice(&bytes[25..]);
    mutations.push(duplicate);
    for (case, bad) in mutations.iter().enumerate() {
        assert!(
            store.validate_raft_archive(&mut bad.as_slice()).is_err(),
            "validation case {case}"
        );
        assert!(
            store
                .restore_raft_archive(&live, &mut bad.as_slice(), |_| panic!(
                    "invalid archive activated"
                ))
                .is_err(),
            "restore case {case}"
        );
        assert_eq!(
            std::fs::read(dir.path().join("CURRENT")).unwrap(),
            before,
            "case {case}"
        );
    }
}

#[test]
fn archive_manifest_byte_change_needs_digest_verification() {
    let (_source, mut bytes) = fixture();
    assert_eq!(bytes.last(), Some(&b'\n'));
    // Both encodings are valid identical JSON values. Only the byte digest
    // can reject this corrupted payload before a fresh backend is opened.
    *bytes.last_mut().unwrap() = b' ';
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    assert!(
        store.validate_raft_archive(&mut bytes.as_slice()).is_err(),
        "archive must verify the digest even when changed manifest bytes still decode"
    );
}

#[test]
fn archive_refuses_payload_files_not_named_in_the_complete_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine
        .create_collection(
            "docs",
            serde_json::from_value(serde_json::json!({
                "fields": {"kind": {"type": "keyword"}}
            }))
            .unwrap(),
        )
        .unwrap();
    let name = store.save_required(&engine, 2).unwrap();
    let root = store.generations.generation_path(&name);
    std::fs::write(
        root.join(collection_checkpoint_dir_name("docs"))
            .join("unlisted.payload"),
        b"unreferenced bytes",
    )
    .unwrap();
    let mut bytes = Vec::new();
    write_archive(&root, 2, &mut bytes).unwrap();
    let receiver = tempfile::tempdir().unwrap();
    let receiver = SegmentRdbStore::new(receiver.path()).unwrap();
    assert!(
        receiver
            .validate_raft_archive(&mut bytes.as_slice())
            .is_err(),
        "archive must reject a payload file absent from the complete manifest"
    );
}

#[cfg(unix)]
#[test]
fn archive_inventory_refuses_symlinks() {
    let dir = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink("/private/tmp", dir.path().join("escape")).unwrap();
    assert!(write_archive(dir.path(), 1, &mut Vec::new()).is_err());
}
