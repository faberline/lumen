//! A bound collection is reused only from the one engine lineage root that
//! holds its directory. A namespace change turns the reuse off until one save
//! binds the engine again.

use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};

fn saved_engine() -> (Engine, tempfile::TempDir) {
    let engine = Engine::new();
    engine.create_collection("c", build_users_schema()).unwrap();
    engine
        .index(
            "c",
            IndexRequest {
                items: vec![item(
                    "doc",
                    "email",
                    FieldValue::String("a@example.test".into()),
                )],
                request_id: None,
            },
        )
        .unwrap();
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut capture = frozen.write(dir.path(), 1).unwrap();
    engine
        .bind_checkpoint_origins(dir.path(), &mut capture)
        .unwrap();
    (engine, dir)
}

fn reused(engine: &Engine, root: &std::path::Path) -> bool {
    let out = tempfile::tempdir().unwrap();
    let capture = engine
        .freeze_checkpoint_collections(Some(root))
        .unwrap()
        .write(out.path(), 2)
        .unwrap();
    capture.reused.contains("c")
}

#[test]
fn bound_collection_is_reused_from_its_root_only() {
    let (engine, dir) = saved_engine();
    assert!(reused(&engine, dir.path()));
    let other = tempfile::tempdir().unwrap();
    assert!(!reused(&engine, other.path()));
}

#[test]
fn namespace_change_turns_reuse_off_and_one_save_turns_it_on() {
    let (engine, dir) = saved_engine();
    engine
        .prepare_checkpoint_namespace(std::path::Path::new("/unused-lineage-namespace"), 1)
        .unwrap();
    assert!(!reused(&engine, dir.path()));
    assert!(engine
        .state
        .read()
        .unwrap()
        .checkpoint_lineage_root
        .is_none());

    let dir2 = tempfile::tempdir().unwrap();
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    let mut capture = frozen.write(dir2.path(), 3).unwrap();
    engine
        .bind_checkpoint_origins(dir2.path(), &mut capture)
        .unwrap();
    assert!(reused(&engine, dir2.path()));
}
