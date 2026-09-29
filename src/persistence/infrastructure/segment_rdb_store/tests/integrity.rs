use crate::persistence::domain::generation_manifest::{
    SegmentGenerationManifest, SegmentKind, SegmentRole,
};
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment_rdb_store::catalog::validate_catalog_references;
use crate::persistence::infrastructure::segment_rdb_store::delta_integrity::CHECKSUM_BYTES;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::{
    read_generation_manifest, write_generation_manifest,
};
use crate::persistence::infrastructure::segment_rdb_store::tests::{index_kw, kw_schema};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::storage::Engine;
use std::sync::Arc;

#[test]
fn v2_delta_rejects_missing_integrity_fields() {
    let (_dir, _store, _engine, root, manifest) = two_keyword_deltas();
    let mut missing = manifest.clone();
    let delta = missing.collections[0]
        .segments
        .iter_mut()
        .filter(|segment| matches!(segment.kind, SegmentKind::Delta))
        .max_by_key(|segment| segment.ordinal)
        .unwrap();
    delta.applied_seq = None;
    delta.payload_sha256 = None;
    assert!(validate_catalog_references(&root, &missing).is_err());
}

#[test]
fn unchanged_checkpoint_links_deltas_without_reading_their_payload_again() {
    let (_dir, store, engine, _root, _manifest) = two_keyword_deltas();
    CHECKSUM_BYTES.with(|bytes| bytes.set(0));
    store.save_required(&engine, 64).unwrap();
    assert_eq!(
        CHECKSUM_BYTES.with(|bytes| bytes.get()),
        0,
        "unchanged checkpoint reread inherited delta payload"
    );
    store.load_latest().unwrap().unwrap();
    assert!(
        CHECKSUM_BYTES.with(|bytes| bytes.get()) > 0,
        "cold recovery must still verify every inherited payload"
    );
}

#[test]
fn v2_base_rejects_validly_decodable_payload_change() {
    let (_dir, _store, _engine, root, manifest) = two_keyword_deltas();
    let base = manifest.collections[0]
        .segments
        .iter()
        .find(|segment| {
            matches!(segment.kind, SegmentKind::Base)
                && matches!(segment.role, SegmentRole::Field)
                && segment.field.as_deref() == Some("email")
        })
        .unwrap();
    let path = root.join(&base.path);
    let mut bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes[4104] & 1, 1);
    bytes[4104] ^= 1;
    std::fs::write(&path, bytes).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.n_docs(), 1);
    assert_eq!(reader.keyword_at(0), None);
    assert!(
        validate_catalog_references(&root, &manifest).is_err(),
        "v2 base payload corruption must not become silent data loss"
    );
}

#[test]
fn changed_verified_predecessor_manifest_cannot_be_inherited() {
    let (_dir, store, engine, root, mut manifest) = two_keyword_deltas();
    let before = std::fs::read(store.root.join("CURRENT")).unwrap();
    manifest.collections[0]
        .segments
        .iter_mut()
        .find(|segment| matches!(segment.kind, SegmentKind::Delta))
        .unwrap()
        .payload_sha256 = Some("0".repeat(64));
    write_generation_manifest(&root, &manifest).unwrap();
    assert!(
        store.save_required(&engine, 64).is_err(),
        "changed predecessor checksum must not be inherited without verification"
    );
    assert_eq!(std::fs::read(store.root.join("CURRENT")).unwrap(), before);
}

#[test]
fn new_store_verifies_predecessor_before_inheriting_payloads() {
    let (dir, _store, engine, root, mut manifest) = two_keyword_deltas();
    manifest.collections[0]
        .segments
        .iter_mut()
        .find(|segment| matches!(segment.kind, SegmentKind::Delta))
        .unwrap()
        .payload_sha256 = Some("0".repeat(64));
    write_generation_manifest(&root, &manifest).unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let before = std::fs::read(store.root.join("CURRENT")).unwrap();
    assert!(
        store.save_required(&engine, 64).is_err(),
        "new store must validate predecessor before trusting inherited checksums"
    );
    assert_eq!(std::fs::read(store.root.join("CURRENT")).unwrap(), before);
}

fn two_keyword_deltas() -> (
    tempfile::TempDir,
    SegmentRdbStore,
    Arc<Engine>,
    std::path::PathBuf,
    SegmentGenerationManifest,
) {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "first@x.com");
    // Keep a populated legacy base for the base-payload corruption tests.
    // Fresh first checkpoints now store these rows in their first delta.
    engine.restore(engine.snapshot().unwrap()).unwrap();
    store.save_required(&engine, 61).unwrap();
    index_kw(&engine, "u1", "second@x.com");
    store.save_required(&engine, 62).unwrap();
    index_kw(&engine, "u1", "third@x.com");
    let generation = store.save_required(&engine, 63).unwrap();
    let root = dir.path().join(generation.as_str());
    let manifest = read_generation_manifest(&root).unwrap();
    (dir, store, engine, root, manifest)
}

#[test]
fn v2_delta_rejects_valid_older_payload_and_row_map_substitution() {
    let (_dir, _store, _engine, root, manifest) = two_keyword_deltas();
    let deltas: Vec<_> = manifest.collections[0]
        .segments
        .iter()
        .filter(|segment| matches!(segment.kind, SegmentKind::Delta))
        .collect();
    assert_eq!(deltas.len(), 2);
    let older = deltas[0];
    let newer = deltas[1];

    // The older pair is a fully valid segment and row map for the same
    // external ID.  The old `<= checkpoint_sequence` check accepted it as
    // the newer ordinal and cold replay silently restored "second".
    std::fs::copy(root.join(&older.path), root.join(&newer.path)).unwrap();
    std::fs::copy(
        root.join(&older.local_rows.as_ref().unwrap().path),
        root.join(&newer.local_rows.as_ref().unwrap().path),
    )
    .unwrap();
    assert!(validate_catalog_references(&root, &manifest).is_err());
}

#[test]
fn v2_delta_rejects_validly_decodable_payload_bit_flip() {
    let (_dir, _store, _engine, root, manifest) = two_keyword_deltas();
    let newer = manifest.collections[0]
        .segments
        .iter()
        .filter(|segment| matches!(segment.kind, SegmentKind::Delta))
        .max_by_key(|segment| segment.ordinal)
        .unwrap();
    let payload_path = root.join(&newer.path);
    let mut payload = std::fs::read(&payload_path).unwrap();
    // One keyword row has its u32 dictionary ID at 4096 and its
    // 8-aligned present bitset at 4104. These bytes are outside the
    // header/directory CRC. Flip presence while keeping the file decodable.
    assert_eq!(payload[4104] & 1, 1);
    payload[4104] ^= 1;
    std::fs::write(&payload_path, payload).unwrap();
    let reader =
        SegmentReader::open(&payload_path).expect("structural checks accept a changed present bit");
    assert_eq!(reader.n_docs(), 1);
    assert_eq!(reader.applied_seq(), 63);
    assert_eq!(reader.keyword_at(0), None);
    assert!(validate_catalog_references(&root, &manifest).is_err());
}
