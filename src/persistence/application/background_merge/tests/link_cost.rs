use crate::index::application::engine::Engine;
use crate::persistence::application::background_merge::link::{
    background_merge_supports_manifest, uses_flat_generation_layout,
};
use crate::persistence::application::background_merge::tests::{
    count_tree, keyword_schema, published_merge_jobs, put_row,
};
use crate::persistence::infrastructure::segment_rdb_store::flat_layout::collection_checkpoint_dir_name;
use crate::persistence::infrastructure::segment_rdb_store::generation_validation::validate_generation_layout;
use crate::persistence::infrastructure::segment_rdb_store::{
    SegmentRdbStore, GENERATION_MANIFEST_V2, GENERATION_MANIFEST_V3,
};
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::{QueryNode, TermQuery};
use crate::shared_kernel::types::search::SearchRequest;
use std::sync::Arc;
use std::time::Duration;
use storage_durable::CurrentTarget;

#[test]
fn v2_generation_manifests_use_directory_layout() {
    assert!(!uses_flat_generation_layout(GENERATION_MANIFEST_V2));
    assert!(uses_flat_generation_layout(GENERATION_MANIFEST_V3));
}

#[test]
fn background_merge_does_not_publish_v3_manifest() {
    assert!(background_merge_supports_manifest(GENERATION_MANIFEST_V2));
    assert!(!background_merge_supports_manifest(GENERATION_MANIFEST_V3));
}

/// The shape of one drained merge job: what each hard-link pass paid for,
/// and what the published generation holds afterwards.
struct MergeJobShape {
    jobs: u64,
    scratch_files: u64,
    generation_link_files: u64,
    generation_files: usize,
    hot_files: usize,
}

fn term_hits(engine: &Engine, collection: &str, field: &str, value: &str) -> usize {
    engine
        .search(
            collection,
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: field.into(),
                    value: FieldValue::String(value.into()),
                }),
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
        .hits
        .len()
}

/// Drive one hot field over the merge threshold in a root that also holds
/// `idle_collections` collections nothing ever writes to again, drain the
/// background worker, and report what the single published job cost.
/// Every assertion here is invariant in `idle_collections`, so the caller
/// can compare two runs directly.
fn drain_one_merge_job(idle_collections: usize) -> MergeJobShape {
    use crate::app::observability::metrics::labels::MergeStep;

    const HOT_FIELD: &str = "h_alpha";
    const IDLE_FIELDS: [&str; 2] = ["i_alpha", "i_beta"];

    let directory = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(directory.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine
        .create_collection("hot", keyword_schema(&[HOT_FIELD]))
        .unwrap();
    let idle: Vec<String> = (0..idle_collections)
        .map(|index| format!("idle-{index:03}"))
        .collect();
    for collection in &idle {
        engine
            .create_collection(collection, keyword_schema(&IDLE_FIELDS))
            .unwrap();
    }
    put_row(&engine, "hot", HOT_FIELD, "seed", "seed-value");
    for collection in &idle {
        for field in IDLE_FIELDS {
            put_row(&engine, collection, field, "seed", "seed-value");
        }
    }
    store.save_required(&engine, 1).unwrap();
    for sequence in 2..=4u64 {
        put_row(&engine, "hot", HOT_FIELD, "hot", &format!("hot-{sequence}"));
        store.save_required(&engine, sequence).unwrap();
    }
    store
        .wait_for_merges(Duration::from_secs(120))
        .expect("background merge worker must drain");

    let metrics = engine.metrics();
    let (_, scratch_observations, scratch_files) =
        metrics.segment_merge_step_observation(MergeStep::LinkScratch);
    let (_, _, generation_link_files) =
        metrics.segment_merge_step_observation(MergeStep::LinkGeneration);
    assert_eq!(
        scratch_observations, 1,
        "this fixture must publish exactly one merge job"
    );

    let CurrentTarget::Generation(name) = store.generations.read_current().unwrap() else {
        panic!("the merge fixture must publish CURRENT")
    };
    let record = store.record_for_name(name.clone()).unwrap();
    validate_generation_layout(&record).expect("the published generation must keep a valid layout");
    let generation = store.root.join(name.as_str());
    let (generation_files, _) = count_tree(&generation);
    let (hot_files, _) = count_tree(&generation.join(collection_checkpoint_dir_name("hot")));

    // Cold-open the published generation: every collection, hot and idle,
    // must still answer for the rows it last wrote.
    let (cold, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 4);
    assert_eq!(
        term_hits(&cold, "hot", HOT_FIELD, "hot-4"),
        1,
        "the compacted field must answer for its newest layer"
    );
    assert_eq!(
        term_hits(&cold, "hot", HOT_FIELD, "seed-value"),
        1,
        "the compacted field must answer for its oldest layer"
    );
    for collection in &idle {
        for field in IDLE_FIELDS {
            assert_eq!(
                term_hits(&cold, collection, field, "seed-value"),
                1,
                "an idle collection must survive the merge: {collection}/{field}"
            );
        }
    }

    MergeJobShape {
        jobs: published_merge_jobs(&store),
        scratch_files,
        generation_link_files,
        generation_files,
        hot_files,
    }
}

/// A merge job's scratch stage is job-private — it never carries a
/// generation manifest and no reader ever opens it — so linking every
/// idle collection into it buys nothing and costs one hard link per file
/// in the whole root. The oracle is structural: the same fixture run at
/// two idle-collection counts must link the same number of files into
/// scratch, bounded by the compacted collection's own file count.
#[test]
fn scratch_link_cost_is_independent_of_idle_collection_count() {
    let small = drain_one_merge_job(4);
    let large = drain_one_merge_job(20);

    assert_eq!(small.jobs, 1, "the small fixture must publish one job");
    assert_eq!(large.jobs, 1, "the large fixture must publish one job");
    assert!(
        large.generation_files > small.generation_files,
        "the fixtures must differ in root size: {} vs {}",
        small.generation_files,
        large.generation_files
    );
    assert_eq!(
        large.scratch_files, small.scratch_files,
        "the scratch link pass must cost the compacted collection, not the root: \
             {} files at 20 idle collections vs {} at 4",
        large.scratch_files, small.scratch_files
    );
    assert!(
        large.scratch_files <= large.hot_files as u64 + 8,
        "the scratch link pass must stay within the compacted collection: \
             {} files linked, compacted collection holds {}",
        large.scratch_files,
        large.hot_files
    );
}

/// The publication-side pass is a different contract from the scratch
/// pass: the new generation inherits every file of the one it replaces, so
/// its link count must still cover the whole root and grow with it.
#[test]
fn generation_link_pass_still_covers_the_whole_root() {
    let small = drain_one_merge_job(4);
    let large = drain_one_merge_job(20);

    assert!(
        large.generation_link_files >= (large.generation_files - large.hot_files) as u64,
        "the generation link pass must cover every idle file: linked {} of {} idle files",
        large.generation_link_files,
        large.generation_files - large.hot_files
    );
    assert!(
        large.generation_link_files > small.generation_link_files,
        "the generation link pass is proportional to the root: {} vs {}",
        large.generation_link_files,
        small.generation_link_files
    );
}
