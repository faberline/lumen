//! A bound collection is reused only from the one engine lineage root that
//! holds its directory. A namespace change turns the reuse off until one save
//! binds the engine again.

use std::collections::{BTreeMap, BTreeSet};

use crate::index::application::checkpoint_capture::CheckpointCollectionIdentity;
use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::index::infrastructure::checkpoint_fs::collection_dir_name;
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

fn index_doc(engine: &Engine, collection: &str, external_id: &str) {
    engine
        .index(
            collection,
            IndexRequest {
                items: vec![item(
                    external_id,
                    "email",
                    FieldValue::String(format!("{external_id}@example.test")),
                )],
                request_id: None,
            },
        )
        .unwrap();
}

fn reused_names(engine: &Engine, root: &std::path::Path) -> BTreeSet<String> {
    let out = tempfile::tempdir().unwrap();
    engine
        .freeze_checkpoint_collections(Some(root))
        .unwrap()
        .write(out.path(), 2)
        .unwrap()
        .reused
}

fn identities(engine: &Engine) -> BTreeMap<String, CheckpointCollectionIdentity> {
    let state = engine.state.read().unwrap();
    state
        .collections
        .iter()
        .filter(|(_, coll)| coll.deleted_at.is_none())
        .map(|(name, coll)| {
            (
                name.clone(),
                CheckpointCollectionIdentity {
                    generation: coll.collection_generation,
                    data_version: coll.data_version,
                    schema_version: coll.version,
                },
            )
        })
        .collect()
}

#[test]
fn schema_change_between_freeze_and_bind_prevents_reuse() {
    let engine = Engine::new();
    engine.create_collection("c", build_users_schema()).unwrap();
    index_doc(&engine, "c", "doc");
    let dir = tempfile::tempdir().unwrap();
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    let mut capture = frozen.write(dir.path(), 1).unwrap();
    let email = build_users_schema().fields["email"].clone();
    engine.add_field("c", "extra", email).unwrap();
    engine
        .bind_checkpoint_origins(dir.path(), &mut capture)
        .unwrap();
    assert!(!reused(&engine, dir.path()));
}

#[test]
fn soft_delete_then_put_again_prevents_reuse() {
    let engine = Engine::new();
    engine.create_collection("c", build_users_schema()).unwrap();
    index_doc(&engine, "c", "doc");
    let dir = tempfile::tempdir().unwrap();
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    let mut capture = frozen.write(dir.path(), 1).unwrap();
    engine.drop_collection("c", false).unwrap();
    engine.create_collection("c", build_users_schema()).unwrap();
    engine
        .bind_checkpoint_origins(dir.path(), &mut capture)
        .unwrap();
    assert!(!reused(&engine, dir.path()));
}

#[test]
fn truncate_between_freeze_and_bind_prevents_reuse() {
    let engine = Engine::new();
    engine.create_collection("c", build_users_schema()).unwrap();
    index_doc(&engine, "c", "doc");
    let dir = tempfile::tempdir().unwrap();
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    let mut capture = frozen.write(dir.path(), 1).unwrap();
    engine.truncate_docs("c").unwrap();
    engine
        .bind_checkpoint_origins(dir.path(), &mut capture)
        .unwrap();
    assert!(!reused(&engine, dir.path()));
}

#[test]
fn collection_created_between_freeze_and_bind_is_not_reused() {
    let engine = Engine::new();
    engine.create_collection("c", build_users_schema()).unwrap();
    index_doc(&engine, "c", "doc");
    let dir = tempfile::tempdir().unwrap();
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    let mut capture = frozen.write(dir.path(), 1).unwrap();
    engine.create_collection("d", build_users_schema()).unwrap();
    index_doc(&engine, "d", "doc");
    engine
        .bind_checkpoint_origins(dir.path(), &mut capture)
        .unwrap();
    let names = reused_names(&engine, dir.path());
    assert!(names.contains("c"));
    assert!(!names.contains("d"));
}

#[test]
fn bind_failure_leaves_the_root_so_the_next_save_does_not_reuse() {
    let (engine, dir) = saved_engine();
    index_doc(&engine, "c", "doc2");
    engine
        .state
        .write()
        .unwrap()
        .collections
        .get_mut("c")
        .unwrap()
        .requires_full_checkpoint = true;
    let dir2 = tempfile::tempdir().unwrap();
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    let mut capture = frozen.write(dir2.path(), 3).unwrap();
    engine
        .prepare_scalar_checkpoint_publications(&mut capture)
        .unwrap();
    engine
        .state
        .write()
        .unwrap()
        .collections
        .get_mut("c")
        .unwrap()
        .fields
        .remove("email");
    assert!(engine
        .bind_checkpoint_origins(dir2.path(), &mut capture)
        .is_err());
    assert_eq!(
        engine
            .state
            .read()
            .unwrap()
            .checkpoint_lineage_root
            .as_deref(),
        Some(dir.path())
    );
    assert!(!reused(&engine, dir2.path()));
}

#[test]
fn background_merge_with_a_different_root_does_not_advance_the_root() {
    let (engine, dir) = saved_engine();
    let mut capture = engine
        .capture_background_merge(identities(&engine))
        .unwrap();
    let new_root = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    engine
        .bind_background_merge(new_root.path(), &mut capture, other.path())
        .unwrap();
    assert_eq!(
        engine
            .state
            .read()
            .unwrap()
            .checkpoint_lineage_root
            .as_deref(),
        Some(dir.path())
    );
}

#[test]
fn background_merge_with_the_same_root_keeps_idle_collections_reusable() {
    let (engine, dir) = saved_engine();
    let mut capture = engine
        .capture_background_merge(identities(&engine))
        .unwrap();
    let new_root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(new_root.path().join(collection_dir_name("c"))).unwrap();
    engine
        .bind_background_merge(new_root.path(), &mut capture, dir.path())
        .unwrap();
    assert_eq!(
        engine
            .state
            .read()
            .unwrap()
            .checkpoint_lineage_root
            .as_deref(),
        Some(new_root.path())
    );
    assert!(reused(&engine, new_root.path()));
}

/// Runs one checkpoint from `from` into a new root. The returned directory must
/// stay alive while the next save reuses it.
fn save(
    engine: &Engine,
    from: Option<&std::path::Path>,
    seq: u64,
) -> (
    crate::index::application::checkpoint_capture::CheckpointCapture,
    tempfile::TempDir,
) {
    let frozen = engine.freeze_checkpoint_collections(from).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut capture = frozen.write(dir.path(), seq).unwrap();
    engine
        .bind_checkpoint_origins(dir.path(), &mut capture)
        .unwrap();
    (capture, dir)
}

fn set_visits(engine: &Engine, value: usize) {
    engine.state.write().unwrap().checkpoint_bind_visits = value;
}

fn visits(engine: &Engine) -> usize {
    engine.state.read().unwrap().checkpoint_bind_visits
}

fn frozen_len(engine: &Engine, name: &str) -> usize {
    engine.state.read().unwrap().collections[name]
        .change_journal
        .frozen_len()
}

#[test]
fn bind_visits_only_dirty_collections_not_idle_ones() {
    let mut counts = Vec::new();
    for n in [4usize, 64] {
        let engine = Engine::new();
        for i in 0..n {
            let name = format!("idle-{i}");
            engine
                .create_collection(&name, build_users_schema())
                .unwrap();
            index_doc(&engine, &name, "doc");
        }
        engine
            .create_collection("hot", build_users_schema())
            .unwrap();
        index_doc(&engine, "hot", "doc");
        let (_first_capture, first) = save(&engine, None, 1);
        set_visits(&engine, 0);
        index_doc(&engine, "hot", "extra");
        let (_second_capture, _second) = save(&engine, Some(first.path()), 2);
        counts.push(visits(&engine));
    }
    assert_eq!(counts[0], counts[1]);
    assert_eq!(counts[1], 1);
}

#[test]
fn idle_collection_frozen_batches_do_not_accumulate() {
    let engine = Engine::new();
    engine
        .create_collection("idle", build_users_schema())
        .unwrap();
    engine
        .create_collection("hot", build_users_schema())
        .unwrap();
    index_doc(&engine, "idle", "doc");
    index_doc(&engine, "hot", "doc");
    let (_capture, first) = save(&engine, None, 1);
    assert_eq!(frozen_len(&engine, "idle"), 0);
    let mut roots = vec![first];
    for seq in 2..=4u64 {
        index_doc(&engine, "hot", &format!("doc{seq}"));
        let (_capture, next) = save(&engine, Some(roots.last().unwrap().path()), seq);
        assert_eq!(frozen_len(&engine, "idle"), 0);
        roots.push(next);
    }
}

#[test]
fn idle_collection_is_still_linked_after_three_saves_and_never_visited() {
    let engine = Engine::new();
    engine
        .create_collection("idle", build_users_schema())
        .unwrap();
    engine
        .create_collection("hot", build_users_schema())
        .unwrap();
    index_doc(&engine, "idle", "doc");
    index_doc(&engine, "hot", "doc");
    let (_capture, first) = save(&engine, None, 1);
    set_visits(&engine, 0);
    let mut roots = vec![first];
    for seq in 2..=4u64 {
        index_doc(&engine, "hot", &format!("doc{seq}"));
        let (capture, next) = save(&engine, Some(roots.last().unwrap().path()), seq);
        assert!(capture.reused.contains("idle"));
        roots.push(next);
    }
    assert_eq!(visits(&engine), 3);
}

#[test]
fn change_made_to_an_idle_collection_after_freeze_keeps_it_reusable_and_is_saved_next() {
    let engine = Engine::new();
    engine
        .create_collection("idle", build_users_schema())
        .unwrap();
    index_doc(&engine, "idle", "doc");
    let (_capture, first) = save(&engine, None, 1);
    let frozen = engine
        .freeze_checkpoint_collections(Some(first.path()))
        .unwrap();
    let second = tempfile::tempdir().unwrap();
    let mut capture = frozen.write(second.path(), 2).unwrap();
    index_doc(&engine, "idle", "changed");
    engine
        .bind_checkpoint_origins(second.path(), &mut capture)
        .unwrap();
    let out = tempfile::tempdir().unwrap();
    let next = engine
        .freeze_checkpoint_collections(Some(second.path()))
        .unwrap()
        .write(out.path(), 3)
        .unwrap();
    assert!(next.reused.contains("idle"));
    assert!(next.field_dirty["idle"]
        .values()
        .any(|rows| !rows.is_empty()));
}

#[test]
fn collection_with_a_complete_journal_is_always_visited() {
    let engine = Engine::new();
    engine.create_collection("c", build_users_schema()).unwrap();
    index_doc(&engine, "c", "doc");
    set_visits(&engine, 0);
    let (_capture, _dir) = save(&engine, None, 1);
    assert_eq!(visits(&engine), 1);
    assert!(!engine.state.read().unwrap().collections["c"].journal_complete_since_empty);
}
