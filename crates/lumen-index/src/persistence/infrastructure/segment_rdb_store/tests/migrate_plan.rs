use crate::index::application::engine::Engine;
use crate::persistence::infrastructure::segment_rdb_store::migrate_plan::plan_standalone_source;
use crate::persistence::infrastructure::segment_rdb_store::tests::{index_kw, kw_schema};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;

fn saved_root() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let src = Arc::new(Engine::new());
    src.create_collection("u", kw_schema()).unwrap();
    index_kw(&src, "u1", "a@x.com");
    index_kw(&src, "u2", "b@x.com");
    index_kw(&src, "u3", "c@x.com");
    store.save(&src, 42).unwrap();
    drop(store);
    dir
}

/// The generation name in `CURRENT`, without the `generation:` prefix.
fn current_name(root: &Path) -> String {
    let raw = std::fs::read_to_string(root.join("CURRENT")).unwrap();
    let line = raw.strip_suffix('\n').unwrap_or(&raw);
    line.strip_prefix("generation:")
        .expect("CURRENT has the generation: prefix")
        .to_string()
}

fn refusal(root: &Path) -> String {
    plan_standalone_source(root).unwrap_err().to_string()
}

/// SHA-256 over every relative path (directories with a trailing `/`) and
/// every file's bytes, in sorted order. Modification times are not read.
fn digest(root: &Path) -> String {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        entries.sort();
        for path in entries {
            let rel = path
                .strip_prefix(base)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            if std::fs::symlink_metadata(&path).unwrap().is_dir() {
                out.push((format!("{rel}/"), Vec::new()));
                walk(base, &path, out);
            } else {
                out.push((rel, std::fs::read(&path).unwrap()));
            }
        }
    }
    let mut entries = Vec::new();
    walk(root, root, &mut entries);
    let mut hasher = Sha256::new();
    for (rel, bytes) in entries {
        hasher.update(rel.as_bytes());
        hasher.update([0u8]);
        hasher.update(&bytes);
        hasher.update([0u8]);
    }
    format!("{:x}", hasher.finalize())
}

#[test]
fn plan_reads_current_generation_and_collections() {
    let dir = saved_root();
    let expected = current_name(dir.path());
    let plan = plan_standalone_source(dir.path()).unwrap();
    assert_eq!(plan.generation.as_deref(), Some(expected.as_str()));
    assert_eq!(plan.collections.len(), 1);
    assert_eq!(plan.collections[0].id, "u");
    assert_eq!(plan.collections[0].eid_rows, 3);
    assert_eq!(plan.source_identity.len(), 64);
}

#[test]
fn plan_is_stable_across_two_runs() {
    let dir = saved_root();
    let first = plan_standalone_source(dir.path()).unwrap();
    let second = plan_standalone_source(dir.path()).unwrap();
    assert_eq!(first, second);
}

#[test]
fn plan_writes_nothing() {
    let dir = saved_root();
    let before = digest(dir.path());
    plan_standalone_source(dir.path()).unwrap();
    assert_eq!(digest(dir.path()), before);
}

#[test]
fn plan_empty_current_has_no_generation() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("CURRENT"), b"empty\n").unwrap();
    let plan = plan_standalone_source(dir.path()).unwrap();
    assert_eq!(plan.generation, None);
    assert!(plan.collections.is_empty());
    assert!(!plan.source_identity.is_empty());
}

#[test]
fn refuses_missing_current() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        refusal(dir.path()),
        "source has no CURRENT file: not a Standalone segment root"
    );
}

#[test]
fn refuses_current_directory() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("CURRENT")).unwrap();
    assert_eq!(refusal(dir.path()), "CURRENT must be a regular file");
}

#[test]
fn refuses_empty_with_unpointed_generation() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("CURRENT"), b"empty\n").unwrap();
    std::fs::create_dir(dir.path().join("gen-7")).unwrap();
    assert_eq!(
        refusal(dir.path()),
        "CURRENT is empty but generation gen-7 exists beside it; refusing to pick another generation"
    );
}

#[test]
fn refuses_unknown_generation_name() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("CURRENT"), b"garbage\n").unwrap();
    assert_eq!(
        refusal(dir.path()),
        "CURRENT names `garbage`, which is not a supported generation name"
    );
}

#[test]
fn refuses_unknown_generation_name_with_prefix() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("CURRENT"), b"generation:garbage\n").unwrap();
    assert_eq!(
        refusal(dir.path()),
        "CURRENT names `generation:garbage`, which is not a supported generation name"
    );
}

#[test]
fn refuses_missing_generation() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("CURRENT"), b"generation:gen-3\n").unwrap();
    assert_eq!(
        refusal(dir.path()),
        "CURRENT points at gen-3, which does not exist"
    );
}

#[test]
fn refuses_unreadable_manifest() {
    let dir = saved_root();
    let name = current_name(dir.path());
    std::fs::write(dir.path().join(&name).join("_generation.json"), b"{").unwrap();
    let message = refusal(dir.path());
    assert!(
        message.starts_with("cannot read generation manifest:"),
        "{message}"
    );
}

#[test]
fn refuses_unreadable_collection_segment() {
    let dir = saved_root();
    let name = current_name(dir.path());
    let generation = dir.path().join(&name);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(generation.join("_generation.json")).unwrap())
            .unwrap();
    let segment_path = manifest["collections"][0]["segments"]
        .as_array()
        .unwrap()
        .iter()
        .find(|segment| segment["role"] == "collection_eids")
        .expect("a collection eid segment")["path"]
        .as_str()
        .unwrap()
        .to_string();
    std::fs::write(generation.join(&segment_path), b"x").unwrap();
    let message = refusal(dir.path());
    assert!(
        message.starts_with("cannot read collection segment:"),
        "{message}"
    );
}

#[test]
fn refuses_stale_current_tmp() {
    let dir = saved_root();
    std::fs::write(dir.path().join("CURRENT.tmp"), b"partial").unwrap();
    assert_eq!(
        refusal(dir.path()),
        "source looks in use (CURRENT.tmp); stop the running node first"
    );
}

#[test]
fn refuses_non_empty_aof() {
    let dir = saved_root();
    std::fs::write(dir.path().join("aof.log"), b"op").unwrap();
    assert_eq!(
        refusal(dir.path()),
        "source looks in use (aof.log is not empty); stop the running node first"
    );
}
