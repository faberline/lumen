use crate::persistence::application::background_merge::tests::{
    count_tree, keyword_schema, published_merge_jobs, put_row,
};
use crate::persistence::infrastructure::segment_rdb_store::flat_layout::collection_checkpoint_dir_name;
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::storage::Engine;
use std::sync::Arc;
use std::time::Duration;
use storage_durable::CurrentTarget;

/// A published merge job must leave a per-step cost breakdown in the same
/// metric registry `/metrics` renders from. Without it the only readable
/// fact about a multi-second job is that it finished, which cannot say
/// whether the whole-root link/inherit passes or the per-field payload
/// dominated it.
#[test]
fn a_published_merge_job_records_every_phase_step_in_the_metric_registry() {
    use crate::metrics::MergeStep;

    const IDLE_COLLECTIONS: usize = 8;
    const HOT_FIELDS: [&str; 1] = ["h_alpha"];
    const IDLE_FIELDS: [&str; 2] = ["i_alpha", "i_beta"];

    let directory = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(directory.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine
        .create_collection("hot", keyword_schema(&HOT_FIELDS))
        .unwrap();
    let idle: Vec<String> = (0..IDLE_COLLECTIONS)
        .map(|index| format!("idle-{index:03}"))
        .collect();
    for collection in &idle {
        engine
            .create_collection(collection, keyword_schema(&IDLE_FIELDS))
            .unwrap();
    }
    for field in HOT_FIELDS {
        put_row(&engine, "hot", field, "seed", "seed-value");
    }
    for collection in &idle {
        for field in IDLE_FIELDS {
            put_row(&engine, collection, field, "seed", "seed-value");
        }
    }
    store.save_required(&engine, 1).unwrap();
    for sequence in 2..=4u64 {
        for field in HOT_FIELDS {
            put_row(&engine, "hot", field, "hot", &format!("hot-{sequence}"));
        }
        store.save_required(&engine, sequence).unwrap();
    }
    store
        .wait_for_merges(Duration::from_secs(120))
        .expect("background merge worker must drain");
    assert_eq!(
        published_merge_jobs(&store),
        1,
        "this fixture must publish exactly one merge job"
    );

    let metrics = engine.metrics();
    for step in MergeStep::ALL {
        let (_, count, _) = metrics.segment_merge_step_observation(step);
        assert_eq!(
            count,
            1,
            "one published job must observe phase {} exactly once",
            step.name()
        );
    }
    let (total_us, _, _) = metrics.segment_merge_step_observation(MergeStep::Total);
    let parts: u64 = MergeStep::ALL
        .iter()
        .filter(|step| **step != MergeStep::Total)
        .map(|step| metrics.segment_merge_step_observation(*step).0)
        .sum();
    assert!(
        parts <= total_us,
        "the named phases must be disjoint spans inside the job total: \
             parts={parts}us total={total_us}us"
    );
    for step in [MergeStep::LinkScratch, MergeStep::LinkGeneration] {
        let (_, _, files) = metrics.segment_merge_step_observation(step);
        assert!(
            files > 0,
            "{} must report the files it hard-linked",
            step.name()
        );
    }
    assert!(
        metrics.segment_merge_linked_files_total.get()
            >= metrics
                .segment_merge_step_observation(MergeStep::LinkScratch)
                .2,
        "the linked-file counter must cover both whole-root link passes"
    );
    assert_eq!(
        metrics.segment_merge_fields_total.get(),
        1,
        "this fixture's collection has one eligible field, and the job \
             must report it"
    );
    assert_eq!(
        metrics.segment_merge_save_gate_count.get(),
        1,
        "the publication-side save_gate hold must be observed once per job"
    );
}

/// MEASUREMENT, not a gate: prints one background merge job's cost broken
/// down by phase for a root of `LUMEN_MEASURE_COLLECTIONS` idle
/// collections (default 1) plus one hot collection. It asserts nothing
/// about wall-clock — a timing assertion here would fail on a loaded
/// machine for reasons that have nothing to do with lumen — so it stays
/// `#[ignore]`d and is run by hand:
///
/// ```text
/// LUMEN_MEASURE_COLLECTIONS=182 cargo test -p lumen --lib -- --ignored \
///   --exact persistence::application::background_merge::tests::phase_metrics::\
///   measure_background_merge_job_cost_by_collection_count --nocapture
/// ```
///
/// The phase numbers are read back from the same metric registry
/// `/metrics` renders, so the breakdown cannot drift from what a
/// production scrape of the same build would report.
#[test]
#[ignore = "measurement: prints a per-phase cost breakdown, asserts no wall-clock budget"]
fn measure_background_merge_job_cost_by_collection_count() {
    use crate::metrics::MergeStep;

    const HOT_FIELDS: [&str; 3] = ["h_alpha", "h_beta", "h_gamma"];
    // The durable perf probe's schema width, so an idle collection here
    // costs the same number of field files it costs in production.
    const IDLE_FIELD_COUNT: usize = 14;

    let idle_collections: usize = std::env::var("LUMEN_MEASURE_COLLECTIONS")
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .unwrap_or(1);
    let idle_field_names: Vec<String> = (0..IDLE_FIELD_COUNT)
        .map(|index| format!("f{index:02}"))
        .collect();
    let idle_fields: Vec<&str> = idle_field_names.iter().map(String::as_str).collect();

    let directory = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(directory.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine
        .create_collection("hot", keyword_schema(&HOT_FIELDS))
        .unwrap();
    let idle: Vec<String> = (0..idle_collections)
        .map(|index| format!("idle-{index:04}"))
        .collect();
    for collection in &idle {
        engine
            .create_collection(collection, keyword_schema(&idle_fields))
            .unwrap();
    }
    for field in HOT_FIELDS {
        put_row(&engine, "hot", field, "seed", "seed-value");
    }
    for collection in &idle {
        for field in &idle_fields {
            put_row(&engine, collection, field, "seed", "seed-value");
        }
    }
    store.save_required(&engine, 1).unwrap();
    for sequence in 2..=4u64 {
        for field in HOT_FIELDS {
            put_row(&engine, "hot", field, "hot", &format!("hot-{sequence}"));
        }
        store.save_required(&engine, sequence).unwrap();
    }
    store
        .wait_for_merges(Duration::from_secs(600))
        .expect("background merge worker must drain");

    let n = idle_collections;
    let jobs = published_merge_jobs(&store);
    let metrics = engine.metrics();
    // A job publishes exactly one compacted field, so this fixture's
    // eligible fields cost one job each. Every line therefore carries the
    // per-job value as well as the sum: what a waiting checkpoint pays is
    // one job's save-gate hold, not the whole drain's.
    for step in MergeStep::ALL {
        let (micros, count, files) = metrics.segment_merge_step_observation(step);
        let seconds = micros as f64 / 1_000_000.0;
        let per_job = seconds / jobs.max(1) as f64;
        println!(
            "MEASURE n={n} phase={} seconds={seconds:.6} per_job_seconds={per_job:.6} \
                 files={files} per_job_files={} observations={count}",
            step.name(),
            files / jobs.max(1),
        );
    }
    let gate_seconds = metrics.segment_merge_save_gate_us_sum.get() as f64 / 1_000_000.0;
    println!(
        "MEASURE n={n} phase=save_gate_held seconds={gate_seconds:.6} \
             per_job_seconds={:.6} files=0",
        gate_seconds / jobs.max(1) as f64
    );
    println!(
        "MEASURE n={n} jobs={jobs} fields={} linked_files={}",
        metrics.segment_merge_fields_total.get(),
        metrics.segment_merge_linked_files_total.get()
    );

    let CurrentTarget::Generation(name) = store.generations.read_current().unwrap() else {
        panic!("the measurement fixture must publish CURRENT")
    };
    let generation = store.root.join(name.as_str());
    let (files, dirs) = count_tree(&generation);
    let hot_dir = generation.join(collection_checkpoint_dir_name("hot"));
    let (hot_files, hot_dirs) = count_tree(&hot_dir);
    println!(
        "MEASURE n={n} generation_files={files} generation_dirs={dirs} \
             hot_files={hot_files} hot_dirs={hot_dirs} idle_files={} idle_dirs={}",
        files - hot_files,
        dirs - hot_dirs
    );
}
