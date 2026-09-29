// CODEGEN-BEGIN
//! In-memory storage and query execution.
//!
//! The engine is `BTreeMap`-backed inverted indexes per field,
//! constructed by [`Engine::new`]. Single-pod, single-shard; durability
//! comes from the CBOR RDB snapshot path and (when segment persistence is
//! selected) the columnar mmap segment tier — not from this module.
//!
//! The shape below maps 1:1 to the field-type table in the README:
//!
//! | FieldType | Index                                  |
//! |-----------|----------------------------------------|
//! | `text`    | `BTreeMap<token, BTreeSet<eid>>`       |
//! | `keyword` | `BTreeMap<value, BTreeSet<eid>>`       |
//! | `number`  | `BTreeMap<SortableF64, BTreeSet<eid>>` |
//! | `set`     | `BTreeMap<element, BTreeSet<eid>>`     |
//!
//! Every field also carries a per-`external_id` "forward" map so
//! re-indexing the same `(eid, field)` cleanly evicts the old postings
//! before appending the new ones.

mod committed_index_apply;
mod committed_index_plan;
mod committed_replace_apply;
mod committed_replace_plan;
mod committed_replace_view;
mod committed_scalar_files;
mod committed_text_apply;
mod large_text_row;
pub(crate) mod record_admission;
mod record_apply;
mod record_charges;
mod scalar_projection;
pub(crate) mod staged_text_row;
pub(crate) mod staged_vector_row;
pub(crate) mod text_preparation;
pub(crate) mod text_projection;
pub(crate) use record_admission::{
    RecordAdmissionError, RecordApplyGuard, RecordReservation, RecordTransientReservation,
    RepriceRecord,
};
pub(crate) use scalar_projection::write_checkpoint_rows as write_scalar_checkpoint_rows;
pub(crate) use text_projection::write_checkpoint_rows as write_text_checkpoint_rows;
// Moved to the index domain and application; re-exported until storage.rs
// becomes the compat facade, so lumen::storage keeps its public surface.
pub use crate::index::application::engine::collections::DropOutcome;
pub use crate::index::application::engine::index::MAX_INDEX_ITEMS;
pub use crate::index::application::engine::raft_dispatch::ApplyOutcome;
pub use crate::index::application::engine::reshard_apply::{
    ReshardApplyOutcome, ReshardEvictOutcome,
};
pub use crate::index::application::engine::reshard_prune::ReshardPruneOutcome;
pub use crate::index::application::engine::Engine;
pub use crate::index::domain::collection::coverage::{FieldNotAudited, ReindexNeeded};
pub use crate::index::domain::query::sort::MAX_SORT_KEYS;
pub use crate::index::domain::query::validate_query;
pub use crate::index::domain::sortable_f64::SortableF64;
pub use crate::index::domain::storage_error::StorageError;

#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

use anyhow::{anyhow, bail, Result};
#[cfg(test)]
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use crate::index::application::checkpoint_capture::{
    CheckpointCollectionIdentity, CheckpointValue,
};
#[cfg(test)]
use crate::index::application::engine::reshard_prune::{
    PRUNE_ACCUM_MAX_AGE_TICKS, PRUNE_ACCUM_MAX_ENTRIES, PRUNE_ACCUM_MAX_TOTAL_CHUNKS,
};
#[cfg(test)]
use crate::index::application::frozen_checkpoint::FrozenCollectionFiles;
#[cfg(test)]
use crate::index::application::recovery_profile::{RecoveryPhase, RecoveryProfile};
#[cfg(test)]
use crate::index::domain::analysis::tokenize;
use crate::index::domain::collection::coverage::{FieldAudit, TEXT_UNAUDITABLE};
use crate::index::domain::collection::Collection;
use crate::index::domain::fast_hash::FastHashMap;
#[cfg(test)]
use crate::index::domain::field_index::FieldIndex;
#[cfg(test)]
use crate::index::domain::hash_index::HashIndex;
#[cfg(test)]
use crate::index::domain::interner::Interner;
#[cfg(test)]
use crate::index::domain::keyword_index::KeywordIndex;
#[cfg(test)]
use crate::index::domain::number_index::NumberIndex;
#[cfg(test)]
use crate::index::domain::number_index::NumberRangeStats;
#[cfg(test)]
use crate::index::domain::postings::Postings;
#[cfg(test)]
use crate::index::domain::query::clause::{clause_matches, eval_filter_bitmap};
#[cfg(test)]
use crate::index::domain::query::knn::eval_hamming;
#[cfg(test)]
use crate::index::domain::query::page_cursor::{
    make_cursor, make_score_cursor, make_sort_cursor, parse_page_cursor, PageCursor,
};
#[cfg(test)]
use crate::index::domain::query::selectivity::{
    estimate_selectivity, is_exact_hamming, is_predicable, plan_filter_candidates,
    SPARSE_CANDIDATE_MAX,
};
#[cfg(test)]
use crate::index::domain::query::sort::SortValue;
#[cfg(test)]
use crate::index::domain::set_index::SetIndex;
#[cfg(test)]
use crate::index::domain::sortable_f64::MISSING_SORTABLE_F64_BITS;
#[cfg(test)]
use crate::index::domain::text_index::TextIndex;
use crate::index::domain::vector::quantize::ScalarCodebook;
#[cfg(test)]
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
#[cfg(test)]
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;
#[cfg(test)]
use crate::shared_kernel::types::document::ReplaceDocItem;
#[cfg(test)]
use crate::shared_kernel::types::query::{
    HammingQuery, HasChildQuery, KnnQuery, MatchOp, MatchQuery, PrefixQuery, QueryNode, RangeBound,
    RangeQuery, SortMissing, SortOrder, SortSpec, TermQuery, TermsQuery,
};
#[cfg(test)]
use crate::shared_kernel::types::search::{
    DuplicatesRequest, SearchHit, SearchRequest, SearchResponse,
};
use crate::shared_kernel::types::{
    document::{
        BatchUnindexDocsRequest, FieldValue, IndexRequest, IndexResponse, ReplaceDocResult,
        ReplaceDocsRequest, ReplaceDocsResponse, MAX_BATCH_REPLACE_SIZE,
    },
    schema::{
        Analyzer, CreateCollectionRequest, CreateCollectionResponse, FieldSpec, FieldType,
        VectorSpec,
    },
};

// #3992 deterministic test oracle.  This is thread-local so concurrent unit
// tests and the asynchronous reclaimer cannot affect the caller-thread check.
// The truncate test uses an unavailable worker and requires this to remain
// zero, proving the apply thread did not reach the per-document primitive.
#[cfg(test)]
thread_local! {
    pub(crate) static DROP_EID_CALLS: Cell<u64> = const { Cell::new(0) };
    // #3997 structural oracles are thread-local so parallel storage tests
    // cannot perturb the counter that one test resets and asserts.
    pub(crate) static MATERIALIZED_SORT_COMPARISONS: Cell<u64> = const { Cell::new(0) };
    pub(crate) static MATERIALIZED_SORT_RETAINED_HIGH_WATER: Cell<u64> = const { Cell::new(0) };
    // #4246 cost oracle: how many (term, staged row) pairs a read path
    // inspected. `/stats` must stay O(live terms + staged tokens); the
    // per-term staged scan it replaced was O(terms x staged_rows).
    static STAGED_TERM_PROBES: Cell<u64> = const { Cell::new(0) };
}

// #4246: the thread `Engine::stats` last ran on. The HTTP handler must hand
// that read to the blocking executor, never the reactor worker, so this is
// process-wide: the observing test thread is not the thread being recorded.
#[cfg(test)]
pub(crate) static STATS_THREAD: Mutex<Option<std::thread::ThreadId>> = Mutex::new(None);
#[cfg(test)]
static RETIREMENT_FAILSAFE_RETAINS: AtomicU64 = AtomicU64::new(0);

/// Count one inspection of a staged Text row on behalf of one term. Compiled
/// out entirely outside `cfg(test)`.
#[inline]
pub(crate) fn note_staged_term_probes(_probes: u64) {
    #[cfg(test)]
    STAGED_TERM_PROBES.with(|probes| probes.set(probes.get().saturating_add(_probes)));
}

#[cfg(test)]
fn reset_staged_term_probes() {
    STAGED_TERM_PROBES.with(|probes| probes.set(0));
}

#[cfg(test)]
fn staged_term_probes() -> u64 {
    STAGED_TERM_PROBES.with(Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_stats_thread() {
    *STATS_THREAD.lock().expect("stats thread record") = None;
}

/// The thread the most recent `Engine::stats` call ran on, or `None` when no
/// call has been recorded since the last reset.
#[cfg(test)]
pub(crate) fn last_stats_thread() -> Option<std::thread::ThreadId> {
    *STATS_THREAD.lock().expect("stats thread record")
}

/// One detached-generation reclaim token removes no more than this many
/// external-id items. This is an item-count work bound only. It is not a hard
/// byte limit, a portable allocator-time limit, or a complete destructor-time
/// limit: one item can still contain an unbounded value, and final container
/// destruction is reported separately below.
const RETIRED_DOCUMENTS_PER_TASK: usize = 1_024;

/// Read-only process-wide diagnostics for detached collection reclamation.
///
/// These counters never participate in logical apply. They let the scale
/// harness measure the queue after `docs:truncate` without turning local
/// cleanup speed into a Raft admission or success condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollectionReclaimerSnapshot {
    /// Generations retained by the registry, whether queued or active.
    pub pending_generations: usize,
    /// Generations handed to the reclaimer since process start.
    pub submitted_generations: u64,
    /// Generations whose final destructor completed off the apply thread.
    pub completed_generations: u64,
    /// Task tokens currently waiting in the one shared queue.
    pub queued_tasks: usize,
    /// Largest observed `queued_tasks` value since process start.
    pub queue_high_water: usize,
    /// Task tokens currently executing across all configured workers.
    pub active_tasks: usize,
}

static RETIREMENT_SUBMITTED_GENERATIONS: AtomicU64 = AtomicU64::new(0);
static RETIREMENT_COMPLETED_GENERATIONS: AtomicU64 = AtomicU64::new(0);
static RETIREMENT_QUEUED_TASKS: AtomicUsize = AtomicUsize::new(0);
static RETIREMENT_QUEUE_HIGH_WATER: AtomicUsize = AtomicUsize::new(0);
static RETIREMENT_ACTIVE_TASKS: AtomicUsize = AtomicUsize::new(0);
// A receiver may dequeue immediately after `send`. This lock makes the queue
// counter increment visible before the matching decrement without wrapping
// the channel or holding a lock while `recv` blocks.
static RETIREMENT_QUEUE_METRICS_LOCK: Mutex<()> = Mutex::new(());

#[cfg(test)]
thread_local! {
    pub(crate) static CHECKPOINT_COLLECTION_OPENS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(crate) static CHECKPOINT_WRITE_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

pub(crate) fn checkpoint_write_boundary() {
    #[cfg(test)]
    CHECKPOINT_WRITE_HOOK.with(|hook| {
        let hook = hook.borrow_mut().take();
        if let Some(hook) = hook {
            hook();
        }
    });
}

/// One detached generation remains in this registry until the background
/// reclaimer has drained its per-document work and performed final cleanup.
/// Keeping the registry's strong reference is deliberate: a failed handoff
/// must retain memory, never fall back to a synchronous destructor in Raft
/// apply.
struct RetiredGeneration {
    id: u64,
    collection: Mutex<Option<Collection>>,
}

impl RetiredGeneration {
    fn new(id: u64, collection: Collection) -> Self {
        Self {
            id,
            collection: Mutex::new(Some(collection)),
        }
    }

    /// Drain one indivisible reclaim task. The mutex makes this generation
    /// single-owner even when several shared-queue workers are active. A task
    /// completes only when its next token is accepted by the shared queue or
    /// its generation is removed from the registry; destruction itself cannot
    /// be rolled back once Rust begins dropping the final collection.
    fn drain_document_task(&self) -> RetireTaskProgress {
        let mut guard = self
            .collection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let collection = guard
            .as_mut()
            .expect("retired generation must have one collection while queued");
        let retired_documents = collection.retire_document_batch(RETIRED_DOCUMENTS_PER_TASK);
        if collection.interner.to_eid.is_empty() {
            let collection = guard
                .take()
                .expect("retired generation collection disappeared during final task");
            RetireTaskProgress::Complete {
                retired_documents,
                collection,
            }
        } else {
            RetireTaskProgress::More { retired_documents }
        }
    }
}

enum RetireTaskProgress {
    More {
        retired_documents: usize,
    },
    Complete {
        retired_documents: usize,
        collection: Collection,
    },
}

/// A token represents one bounded slice of one retired generation. There is
/// at most one queued token per generation: completing a slice creates the
/// next token only after the worker owns the current one.
pub(crate) struct RetireTask {
    generation: Arc<RetiredGeneration>,
}

/// Process-wide ownership registry. It is intentionally separate from the
/// queue: the queue has one small token per active generation, while this map
/// retains a generation if a worker fails before requeueing it.
struct RetiredGenerationRegistry {
    next_id: AtomicU64,
    generations: Mutex<FastHashMap<u64, Arc<RetiredGeneration>>>,
}

impl RetiredGenerationRegistry {
    fn register(&self, collection: Collection) -> Arc<RetiredGeneration> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let generation = Arc::new(RetiredGeneration::new(id, collection));
        self.generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id, Arc::clone(&generation));
        RETIREMENT_SUBMITTED_GENERATIONS.fetch_add(1, Ordering::Release);
        generation
    }

    fn complete(&self, id: u64) {
        let removed = self
            .generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&id)
            .is_some();
        if removed {
            RETIREMENT_COMPLETED_GENERATIONS.fetch_add(1, Ordering::Release);
        }
    }
}

static RETIRED_GENERATIONS: OnceLock<RetiredGenerationRegistry> = OnceLock::new();

fn retired_generation_registry() -> &'static RetiredGenerationRegistry {
    RETIRED_GENERATIONS.get_or_init(|| RetiredGenerationRegistry {
        next_id: AtomicU64::new(1),
        generations: Mutex::new(FastHashMap::default()),
    })
}

/// Return a non-blocking diagnostic snapshot of the shared reclaimer.
///
/// The values are observational and can change immediately after return. A
/// caller that needs to measure one truncate should take a baseline, issue the
/// API request, then wait until `completed_generations` advances and
/// `pending_generations` returns to its baseline.
pub fn collection_reclaimer_snapshot() -> CollectionReclaimerSnapshot {
    let pending_generations = retired_generation_registry()
        .generations
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .len();
    CollectionReclaimerSnapshot {
        pending_generations,
        submitted_generations: RETIREMENT_SUBMITTED_GENERATIONS.load(Ordering::Acquire),
        completed_generations: RETIREMENT_COMPLETED_GENERATIONS.load(Ordering::Acquire),
        queued_tasks: RETIREMENT_QUEUED_TASKS.load(Ordering::Acquire),
        queue_high_water: RETIREMENT_QUEUE_HIGH_WATER.load(Ordering::Acquire),
        active_tasks: RETIREMENT_ACTIVE_TASKS.load(Ordering::Acquire),
    }
}

/// Process-wide retirement queue for collections detached by `docs:truncate`.
///
/// A truncate state-machine apply never waits for reclaimer progress and never
/// makes its durable outcome depend on local queue progress. The queue holds
/// one bounded-work token per active retired generation. Configured workers
/// share one receiver. The default is one worker; `LUMEN_RECLAIM_WORKERS`
/// allows up to four. A long retirement yields after each token and lets
/// another worker run another generation. A token processes at most
/// [`RETIRED_DOCUMENTS_PER_TASK`] external-id items. This does not claim a
/// hard byte bound or portable CPU/allocator-time bound because a current
/// field value and final container have no such cap.
///
/// If no worker starts or a requeue fails, the registry intentionally retains
/// the detached generation. Synchronous destruction could make one replica's
/// committed apply depend on local cleanup speed. The failure is logged and
/// never changes visible collection state.
const DEFAULT_COLLECTION_RETIREMENT_WORKERS: usize = 1;
const MAX_COLLECTION_RETIREMENT_WORKERS: usize = 4;

fn collection_retirement_worker_count(configured: Option<&str>) -> usize {
    configured
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|count| *count > 0)
        .map(|count| count.min(MAX_COLLECTION_RETIREMENT_WORKERS))
        .unwrap_or(DEFAULT_COLLECTION_RETIREMENT_WORKERS)
}

fn configured_collection_retirement_worker_count() -> usize {
    let configured = std::env::var("LUMEN_RECLAIM_WORKERS").ok();
    collection_retirement_worker_count(configured.as_deref())
}

pub(crate) enum CollectionRetirementWorker {
    Ready(mpsc::Sender<RetireTask>),
    Unavailable,
}

static COLLECTION_RETIREMENT_WORKER: OnceLock<CollectionRetirementWorker> = OnceLock::new();

pub(crate) fn collection_retirement_worker() -> &'static CollectionRetirementWorker {
    COLLECTION_RETIREMENT_WORKER.get_or_init(|| {
        let (sender, receiver) = mpsc::channel::<RetireTask>();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers_started = 0;

        for worker_index in 0..configured_collection_retirement_worker_count() {
            let receiver = Arc::clone(&receiver);
            let sender_for_worker = sender.clone();
            let name = format!("lumen-collection-reclaimer-{worker_index}");
            match std::thread::Builder::new().name(name).spawn(move || {
                collection_retirement_loop(receiver, sender_for_worker);
            }) {
                Ok(_) => workers_started += 1,
                Err(error) => {
                    tracing::error!(%error, worker_index, "start collection retirement worker")
                }
            }
        }

        if workers_started == 0 {
            CollectionRetirementWorker::Unavailable
        } else {
            CollectionRetirementWorker::Ready(sender)
        }
    })
}

fn collection_retirement_loop(
    receiver: Arc<Mutex<mpsc::Receiver<RetireTask>>>,
    sender: mpsc::Sender<RetireTask>,
) {
    loop {
        // The receiver lock covers only dequeue. A worker never holds it while
        // it drains a generation, which lets another worker own the next task.
        let Some(task) = receive_retirement_task(receiver.as_ref()) else {
            return;
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_retire_task(task, &sender);
        }));
        finish_retirement_task();
        if result.is_err() {
            // The registry still owns the generation entry after a task panic.
            // Do not re-run partly-mutated detached state or block an apply.
            tracing::error!("collection retirement task panicked; generation retained");
        }
    }
}

fn receive_retirement_task(receiver: &Mutex<mpsc::Receiver<RetireTask>>) -> Option<RetireTask> {
    let task = receiver
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .recv()
        .ok();
    if task.is_some() {
        mark_retirement_task_dequeued();
    }
    task
}

#[cfg(test)]
fn try_receive_retirement_task(
    receiver: &mpsc::Receiver<RetireTask>,
) -> std::result::Result<RetireTask, mpsc::TryRecvError> {
    let task = receiver.try_recv()?;
    mark_retirement_task_dequeued();
    Ok(task)
}

fn mark_retirement_task_dequeued() {
    let _metrics = RETIREMENT_QUEUE_METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let queued = RETIREMENT_QUEUED_TASKS.fetch_sub(1, Ordering::AcqRel);
    debug_assert!(queued > 0, "a dequeued retirement task must be counted");
    RETIREMENT_ACTIVE_TASKS.fetch_add(1, Ordering::Release);
}

fn finish_retirement_task() {
    let active = RETIREMENT_ACTIVE_TASKS.fetch_sub(1, Ordering::AcqRel);
    debug_assert!(active > 0, "a completed retirement task must be active");
}

fn send_retirement_task(
    sender: &mpsc::Sender<RetireTask>,
    task: RetireTask,
) -> std::result::Result<(), mpsc::SendError<RetireTask>> {
    let _metrics = RETIREMENT_QUEUE_METRICS_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    sender.send(task)?;
    let queued = RETIREMENT_QUEUED_TASKS.fetch_add(1, Ordering::AcqRel) + 1;
    RETIREMENT_QUEUE_HIGH_WATER.fetch_max(queued, Ordering::AcqRel);
    Ok(())
}

fn run_retire_task(task: RetireTask, sender: &mpsc::Sender<RetireTask>) {
    let generation_id = task.generation.id;
    match task.generation.drain_document_task() {
        RetireTaskProgress::More { retired_documents } => {
            debug_assert_eq!(retired_documents, RETIRED_DOCUMENTS_PER_TASK);
            let next = RetireTask {
                generation: task.generation,
            };
            if let Err(error) = send_retirement_task(sender, next) {
                retain_retirement_task_failsafe(error.0, "worker channel disconnected");
            }
        }
        RetireTaskProgress::Complete {
            retired_documents,
            collection,
        } => {
            debug_assert!(retired_documents <= RETIRED_DOCUMENTS_PER_TASK);
            // This final drop can still be unbounded: `HashMap` capacities,
            // segment handles, and field/vector internals do not expose an
            // incremental destruction API. It remains off the apply thread.
            // A panic cannot be rolled back, so retain the registry entry and
            // let the worker's outer catch retain it without acknowledgement.
            drop(collection);
            retired_generation_registry().complete(generation_id);
        }
    }
}

fn retain_retirement_task_failsafe(task: RetireTask, reason: &'static str) {
    tracing::error!(%reason, generation_id = task.generation.id, "collection retirement unavailable; retaining detached generation");
    #[cfg(test)]
    RETIREMENT_FAILSAFE_RETAINS.fetch_add(1, Ordering::Relaxed);
    // The registry holds a strong reference. Dropping this failed token cannot
    // run the collection destructor on the apply thread.
    drop(task);
}

pub(crate) fn retire_collection_with(worker: &CollectionRetirementWorker, old: Collection) {
    let generation = retired_generation_registry().register(old);
    let task = RetireTask { generation };
    match worker {
        CollectionRetirementWorker::Ready(sender) => {
            if let Err(error) = send_retirement_task(sender, task) {
                retain_retirement_task_failsafe(error.0, "worker channel disconnected");
            }
        }
        CollectionRetirementWorker::Unavailable => {
            retain_retirement_task_failsafe(task, "worker failed to start");
        }
    }
}

pub(crate) fn hard_link_checkpoint_tree(
    origin: &std::path::Path,
    target: &std::path::Path,
) -> Result<()> {
    std::fs::create_dir_all(target)?;
    for entry in std::fs::read_dir(origin)? {
        let entry = entry?;
        let metadata = std::fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            bail!("checkpoint origin contains a symlink");
        }
        let destination = target.join(entry.file_name());
        if metadata.is_dir() {
            hard_link_checkpoint_tree(&entry.path(), &destination)?;
        } else if metadata.is_file() {
            std::fs::hard_link(entry.path(), destination)
                .map_err(|e| anyhow!("hard link checkpoint origin: {e}"))?;
        } else {
            bail!("checkpoint origin contains a nonregular file");
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Snapshot wire types
// ---------------------------------------------------------------------------

/// The format this build WRITES. Readers accept `1..=SNAPSHOT_VERSION`; see
/// [`SnapshotV1::version`].
///
/// 2 dropped the CONTENTS of the `terms` / `elements` inverted maps from the
/// Keyword and Set arms. Reading forward is unaffected — a format-1 document's
/// populated map is dropped on arrival and `forward` restores the field, which
/// `tests/it/snapshot_ships_only_the_forward_column.rs` pins.
///
/// Reading BACKWARD needed care, because 0.4.29 is released and a version gate
/// does not work the way it looks like it does. `version` is a field of the
/// same struct being deserialised; there is no point at which it is read
/// first. So an 0.4.29 build, whose `terms` is a required field with no
/// `#[serde(default)]`, would fail inside serde on the missing key before its
/// own `!= 1` check ever ran — reporting a missing field on a file that is
/// perfectly intact. The sharp case is a ROLLBACK: `rdb.rs` writes a
/// format-2 snapshot to the data directory in CBOR, the operator rolls the
/// node back to 0.4.29 mid-incident, and 0.4.29 fails to decode its own data
/// directory with a message that reads like corruption.
///
/// [`LegacyInvertedIndex`] is why that does not happen: the two keys stay on
/// the wire as empty maps, so a released 0.4.29 parses the document and
/// refuses it by version, in its own words, with the remedy (upgrade the
/// binary) named. The payload saving is unchanged — what cost bytes was the
/// dictionary, not the key.
///
/// The same applies to every other boundary these documents cross:
/// `/admin/restore`, a reshard delta between shards, and a Raft catch-up into
/// a peer that has not been upgraded yet. All of them now refuse by version.
pub(crate) const SNAPSHOT_VERSION: u32 = 2;

/// Top-level snapshot document. JSON-serialisable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotV1 {
    /// Format version. Bump when the wire layout changes
    /// incompatibly so old snapshots can be detected at restore.
    pub version: u32,
    pub collections: BTreeMap<String, CollectionSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionSnapshot {
    pub schema: BTreeMap<String, FieldSpec>,
    pub version: u32,
    pub eid_fields: HashMap<String, BTreeSet<String>>,
    pub fields: BTreeMap<String, FieldIndexSnapshot>,
}

impl SnapshotV1 {
    /// The reindex audit, asked of the DOCUMENT instead of a restored engine.
    ///
    /// This must answer exactly what [`Engine::reindex_needed`] answers for the
    /// same bytes, and `tests/it/reopen_names_the_fields_that_need_reindexing.rs`
    /// runs both over one document and requires the same rows — the differential
    /// is what keeps the two from drifting.
    ///
    /// It exists because the audience is an operator holding a backup file,
    /// deciding whether to import it at all. Restoring into a throwaway
    /// `Engine` to ask would materialise every interner, roaring bitmap and
    /// forward map of the entire backup in RAM — a multiple of the file size,
    /// on the machine that most needs the answer — while every fact the audit
    /// reads is already a plain field of the parsed document.
    pub fn reindex_needed(&self) -> Vec<ReindexNeeded> {
        struct Tally<'a> {
            field: &'a FieldIndexSnapshot,
            covered: u64,
            holds: bool,
            probe: bool,
        }
        let mut out = Vec::new();
        // `collections` and `fields` are both `BTreeMap`, so the rows come out
        // ordered by collection then field with no sort — the same order
        // `Engine::reindex_needed` sorts into.
        for (collection, coll) in &self.collections {
            let mut tally: BTreeMap<&str, Tally<'_>> = coll
                .fields
                .iter()
                .filter_map(|(name, field)| {
                    let (holds, probe) = match field.audit_kind() {
                        FieldAudit::PerId => (false, true),
                        FieldAudit::WholeIndex(populated) => (populated, false),
                        FieldAudit::Unauditable(_) => return None,
                    };
                    Some((
                        name.as_str(),
                        Tally {
                            field,
                            covered: 0,
                            holds,
                            probe,
                        },
                    ))
                })
                .collect();
            for (eid, cov) in &coll.eid_fields {
                for name in cov {
                    if let Some(t) = tally.get_mut(name.as_str()) {
                        t.covered += 1;
                        if t.probe && !t.holds {
                            t.holds = t.field.holds(eid);
                        }
                    }
                }
            }
            out.extend(
                tally
                    .into_iter()
                    .filter(|(_, t)| t.covered > 0 && !t.holds)
                    .map(|(field, t)| ReindexNeeded {
                        collection: collection.clone(),
                        field: field.to_string(),
                        documents_covered: t.covered,
                    }),
            );
        }
        out
    }

    /// Every field [`SnapshotV1::reindex_needed`] did not examine. See
    /// [`FieldNotAudited`].
    pub fn fields_not_audited(&self) -> Vec<FieldNotAudited> {
        self.collections
            .iter()
            .flat_map(|(collection, coll)| {
                coll.fields
                    .iter()
                    .filter_map(move |(field, index)| match index.audit_kind() {
                        FieldAudit::Unauditable(reason) => Some(FieldNotAudited {
                            collection: collection.clone(),
                            field: field.clone(),
                            reason: reason.to_string(),
                        }),
                        _ => None,
                    })
            })
            .collect()
    }

    /// Per collection, how many live documents this document's census carries —
    /// the same figure `stats` reports as `documents_indexed`. It travels with
    /// the verdict so "nothing is damaged" reads differently from "nothing was
    /// read".
    pub fn documents_scanned(&self) -> BTreeMap<String, u64> {
        self.collections
            .iter()
            .map(|(id, coll)| (id.clone(), coll.eid_fields.len() as u64))
            .collect()
    }
}

/// A field that exists only so a format-1 READER can parse a format-2
/// document and reach its own version check.
///
/// 0.4.29 is released. Its `FieldIndexSnapshot::Keyword` requires a `terms`
/// key and its `Set` requires `elements`, neither carrying `#[serde(default)]`
/// — so a 0.4.29 build fails inside serde on the missing key BEFORE
/// `Engine::restore` ever compares versions. The operator then reads
/// `missing field \`terms\`` (or, from `rdb.rs`'s CBOR, something less legible
/// still) off a file that is not damaged, on a rollback where the only thing
/// wrong is that the binary is too old to say so. Emitting `{}` here costs two
/// bytes per field and hands 0.4.29 back the version error it already knows
/// how to print.
///
/// It is written and never read: `skip_deserializing` drops a format-1
/// document's populated map on arrival, which is what
/// `FieldIndexSnapshot::from_snapshot` wants anyway — the inverted index is
/// rebuilt from `forward` in both formats, so the map on the wire never had a
/// reader.
///
/// Delete this, and bump the format again, once no supported release still
/// requires the key. `tests/it/snapshot_ships_only_the_forward_column.rs` pins that
/// it stays empty; nothing else may put a value in it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LegacyInvertedIndex;

impl Serialize for LegacyInvertedIndex {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        serializer.serialize_map(Some(0))?.end()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FieldIndexSnapshot {
    Text {
        analyzer: Analyzer,
        tokens: BTreeMap<String, BTreeMap<String, u32>>,
        forward: HashMap<String, (BTreeSet<String>, u32)>,
        doc_count: u64,
        total_doc_len: u64,
        bytes: u64,
    },
    /// Only the forward column travels. `from_snapshot` rebuilds `terms` from
    /// it — it has to, because a snapshot whose persisted inverted index
    /// disagreed with its own forward column would otherwise restore into an
    /// index that answers queries no document satisfies. Once the reader
    /// derives one representation from the other, serialising both is writing
    /// a value nobody reads: a `terms` field on the wire is decoded out of the
    /// segment at snapshot time, written to disk, shipped to a Raft follower
    /// mid-catch-up, and dropped on arrival.
    ///
    /// A format-1 document still carries a populated one; `terms` below drops
    /// it on arrival, and `forward` — always complete, even there — is what
    /// restores. The key itself stays on the wire, empty, so a released 0.4.29
    /// can still parse this document and refuse it by version rather than by
    /// serde; see [`LegacyInvertedIndex`].
    Keyword {
        #[serde(default, skip_deserializing)]
        terms: LegacyInvertedIndex,
        forward: HashMap<String, String>,
        bytes: u64,
    },
    Number {
        /// Stored as `f64` on the wire; `SortableF64` is re-derived on
        /// restore.
        forward: HashMap<String, f64>,
        bytes: u64,
    },
    /// Forward column only, for the reason the Keyword arm above states, with
    /// the same empty [`LegacyInvertedIndex`] under the key 0.4.29 requires.
    Set {
        #[serde(default, skip_deserializing)]
        elements: LegacyInvertedIndex,
        forward: HashMap<String, BTreeSet<String>>,
        bytes: u64,
    },
    /// Vector snapshot.
    ///
    /// HNSW graphs are not serialized directly — on restore the
    /// vectors are bulk-reinserted into a fresh graph, which is fast
    /// enough (millions per second on CPU) and avoids tying us to the
    /// upstream graph format. The codebook is carried verbatim when
    /// SQ is enabled so decoding reproduces the exact same f32 values
    /// that were originally indexed.
    Vector {
        spec: VectorSpec,
        vectors: Vec<(String, Vec<f32>)>,
        codebook: Option<ScalarCodebook>,
        bytes: u64,
    },
    Hash {
        /// external_id → 64-bit hash.
        forward: HashMap<String, u64>,
        bytes: u64,
    },
}

impl FieldIndexSnapshot {
    /// What the reindex audit can learn about this arm, in the same three
    /// shapes [`FieldIndex::audit_kind`] answers in. The two must agree arm for
    /// arm: `SnapshotV1::reindex_needed` and `Engine::reindex_needed` are one
    /// verdict asked of the document and of the restored engine.
    ///
    /// [`FieldIndex::audit_kind`]: crate::index::domain::field_index::FieldIndex::audit_kind
    fn audit_kind(&self) -> FieldAudit {
        match self {
            FieldIndexSnapshot::Text { .. } => FieldAudit::Unauditable(TEXT_UNAUDITABLE),
            FieldIndexSnapshot::Keyword { .. }
            | FieldIndexSnapshot::Number { .. }
            | FieldIndexSnapshot::Set { .. }
            | FieldIndexSnapshot::Hash { .. } => FieldAudit::PerId,
            FieldIndexSnapshot::Vector { vectors, .. } => {
                FieldAudit::WholeIndex(!vectors.is_empty())
            }
        }
    }

    /// Whether the forward column carries `eid`.
    ///
    /// This is the document-side twin of [`FieldIndex::holds`], and it reads the
    /// exact column `from_snapshot` restores the index from — so "the document
    /// holds it" and "the restored index answers for it" cannot come apart
    /// without a `from_snapshot` bug, which is a different failure from the one
    /// this audit is looking for.
    ///
    /// [`FieldIndex::holds`]: crate::index::domain::field_index::FieldIndex::holds
    fn holds(&self, eid: &str) -> bool {
        match self {
            FieldIndexSnapshot::Keyword { forward, .. } => forward.contains_key(eid),
            FieldIndexSnapshot::Number { forward, .. } => forward.contains_key(eid),
            FieldIndexSnapshot::Set { forward, .. } => forward.contains_key(eid),
            FieldIndexSnapshot::Hash { forward, .. } => forward.contains_key(eid),
            FieldIndexSnapshot::Text { .. } | FieldIndexSnapshot::Vector { .. } => true,
        }
    }
}

// ---------------------------------------------------------------------------
// Production checkpoint (Stage 2 Phase 2f-2): the disk engine as the running
// binary's persistence — a segment checkpoint supersedes the CBOR RDB.
// ---------------------------------------------------------------------------
//
// A checkpoint is a directory `dir/<collection>/` per collection, each holding
// `<field>.lseg` segments, the `_collection.lmeta.lseg` EID column, any vector
// `<field>.eids.lseg` sidecars, and a `_schema.json` (the field specs + version
// + applied_seq, carried out-of-band so reopen knows each field's type without a
// CBOR snapshot). `flush_to_segments` is the periodic snapshotter's call:
// re-seal-capable (`Collection::seal_to_segments` gathers base-doc values through
// the segment-aware dispatch, so a checkpoint AFTER a prior seal+drop is correct),
// idempotent, and repeatable. `reopen_from_segment_dir` is cold-start: reopen
// every collection via `Collection::open_from_segments` (no whole-collection load)
// and return the max applied_seq so the WAL tail replays from there.
//
// Atomicity is the caller's (`SegmentRdbStore`): it stages a whole generation
// under a temp dir and atomically renames it into place, so a torn checkpoint
// never replaces a good one. `flush_to_segments` writes into whatever `dir` it is
// handed; it does not own the atomic-rename.

/// Per-collection checkpoint sidecar persisted next to the segments so a reopen
/// knows each field's type + the collection version + the WAL position the seal
/// is current as of — the schema the live `Collection::open_from_segments` needs
/// out-of-band. Phase 2f-2.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CheckpointSchema {
    pub(crate) version: u32,
    pub(crate) applied_seq: u64,
    pub(crate) fields: BTreeMap<String, FieldSpec>,
    #[serde(default)]
    pub(crate) segment_layout: CheckpointLayout,
}

/// The marker is absent in shipped checkpoints. Public field names never
/// become path components in newly published generations.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum CheckpointLayout {
    #[default]
    #[serde(rename = "legacy-raw-v0")]
    Legacy,
    #[serde(rename = "encoded-fields-v1")]
    Encoded,
}

impl CheckpointLayout {
    pub(crate) fn field_stem(self, name: &str) -> String {
        match self {
            Self::Legacy => name.to_owned(),
            Self::Encoded => format!("fields/{}", collection_dir_name(name)),
        }
    }

    pub(crate) fn from_sidecar(sidecar: &serde_json::Value) -> Result<Self> {
        sidecar
            .get("segment_layout")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map(|layout| layout.unwrap_or_default())
            .map_err(Into::into)
    }
}

pub(crate) const CHECKPOINT_SCHEMA_FILE: &str = "_schema.json";

/// A collection id → filename-safe subdir name (hex-encoded), so any
/// collection id is a valid directory.
pub(crate) fn collection_dir_name(name: &str) -> String {
    name.bytes().map(|b| format!("{b:02x}")).collect()
}

/// Decode a checkpoint subdir's hex-encoded name back to the collection id.
/// `None` if the leaf is not valid hex (a stray file/dir in the checkpoint).
pub(crate) fn collection_name_from_dir(dir: &std::path::Path) -> Option<String> {
    let leaf = dir.file_name()?.to_str()?;
    if leaf.is_empty() || leaf.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(leaf.len() / 2);
    let raw = leaf.as_bytes();
    let mut i = 0;
    while i < raw.len() {
        let hi = (raw[i] as char).to_digit(16)?;
        let lo = (raw[i + 1] as char).to_digit(16)?;
        bytes.push((hi * 16 + lo) as u8);
        i += 2;
    }
    String::from_utf8(bytes).ok()
}

// ---------------------------------------------------------------------------
// Test seam: seal a Number field to a disk segment (disk tier)
// ---------------------------------------------------------------------------

#[cfg(test)]
impl Engine {
    /// TEST SEAM (Stage 2 Phase 2c): seal the current in-RAM state of a Number
    /// field into a columnar mmap segment under `dir`, then attach it so per-doc
    /// PREDICATE point lookups read the segment for the sealed id range. Mirrors
    /// what a real flush would do: dumps `forward` in dense docid order
    /// `[0..n_docs)` (absent docs → `None`) via [`crate::persistence::infrastructure::segment::number_writer::write_number_segment`],
    /// opens a [`crate::persistence::infrastructure::segment::SegmentReader`], and sets `NumberIndex::segment`.
    ///
    /// `n_docs` is the interner's dense id count, so any doc indexed AFTER
    /// sealing (id >= n_docs) is NOT covered by the segment and stays served
    /// from the live `forward` tail — exactly the live/sealed split the runtime
    /// will use. Returns the sealed doc count.
    pub(crate) fn __seal_number_field_to_segment(
        &self,
        collection_id: &str,
        field: &str,
        dir: &std::path::Path,
    ) -> Result<u32> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        // Dense doc-id space is `[0..interner.to_eid.len())`.
        let n_docs = coll.interner.to_eid.len();
        let fi = coll
            .fields
            .get_mut(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        let FieldIndex::Number(n) = fi else {
            bail!("field `{field}` is not a Number field");
        };
        // Column in dense docid order: each sealed id's live value (or None).
        let values: Vec<Option<f64>> = (0..n_docs as u32)
            .map(|id| n.live_number_at(id).map(|s| s.to_f64()))
            .collect();

        let path = dir.join(format!("{field}.lseg"));
        crate::persistence::infrastructure::segment::number_writer::write_number_segment(
            &path,
            n_docs as u64,
            &values,
        )?;
        let reader = crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
        debug_assert_eq!(reader.n_docs() as usize, n_docs);
        n.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
            std::sync::Arc::new(reader),
        )));
        // Phase 2h-3: mirror PRODUCTION `seal_to_segment` — drop BOTH the in-RAM
        // `forward` tail AND the inverted/range `values` driver (the sorted-value
        // column + per-value postings are on disk now). Queries drive from the
        // mmap; a doc indexed AFTER sealing (id >= n_docs) re-populates the live
        // `values`/`forward` tail, which the unified accessors compose with the
        // segment base.
        n.forward = FastHashMap::default();
        n.dense_forward = Vec::new();
        n.values = BTreeMap::new();
        n.dup_values = BTreeSet::new();
        n.clear_keyword_range_cache();
        // First-time seal from the live `forward`: no prior tombstone exists, but
        // reset for symmetry with the production re-seal path (Phase 2h-3).
        n.tombstones = RoaringBitmap::new();
        Ok(n_docs as u32)
    }

    /// TEST SEAM (Stage 2 Phase 2e-A, extended 2h-1): seal a Keyword field's
    /// in-RAM state into a columnar mmap segment under `dir`, then attach it so
    /// per-doc Keyword PREDICATE lookups (`keyword_at`) AND the inverted
    /// Term/Terms/boolean driver (`term_postings`/`term_df`) serve the sealed id
    /// range from the segment. Dumps `forward` in dense docid order
    /// `[0..n_docs)` (absent docs → `None`) plus the INVERTED postings (folded
    /// from `forward`) via [`crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment`] — a sorted
    /// prefix-compressed string DICT + a fixed `u32[n_docs]` dict-id forward
    /// column + a parallel per-term [`ROLE_KEYWORD_POSTINGS`] posting column.
    ///
    /// Phase 2h-1: this seam now mirrors PRODUCTION `seal_to_segment` — after
    /// attaching the reader it DROPS BOTH the in-RAM `forward` tail AND the
    /// inverted `terms` index (the RAM win). Queries then drive entirely from
    /// the mmap. A doc indexed AFTER sealing (id >= n_docs) re-populates the
    /// live `terms`/`forward` tail, which `term_postings`/`keyword_at` compose
    /// with the segment base. Returns the sealed doc count.
    pub(crate) fn __seal_keyword_field_to_segment(
        &self,
        collection_id: &str,
        field: &str,
        dir: &std::path::Path,
    ) -> Result<u32> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        let n_docs = coll.interner.to_eid.len();
        let fi = coll
            .fields
            .get_mut(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        let FieldIndex::Keyword(k) = fi else {
            bail!("field `{field}` is not a Keyword field");
        };
        // Column in dense docid order: each sealed id's live keyword (or None).
        let owned: Vec<Option<String>> = (0..n_docs as u32).map(|id| k.keyword_at(id)).collect();
        let values: Vec<Option<&str>> = owned.iter().map(|o| o.as_deref()).collect();
        // INVERTED postings to seal: fold the live values (== the live `terms`
        // index restricted to the sealed id range) into a fresh BTreeMap.
        let mut terms: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
        for (id, v) in values.iter().enumerate() {
            if let Some(s) = v {
                terms.entry((*s).to_string()).or_default().insert(id as u32);
            }
        }

        let path = dir.join(format!("{field}.lseg"));
        crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment(
            &path,
            n_docs as u64,
            &values,
            &terms,
        )?;
        let reader = crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
        debug_assert_eq!(reader.n_docs() as usize, n_docs);
        k.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
            std::sync::Arc::new(reader),
        )));
        // Drop the RAM index — the whole [0..n_docs) inverted+forward state is
        // on disk now (Phase 2h-1). Queries drive from the mmap segment.
        k.forward = FastHashMap::default();
        k.dense_forward = Vec::new();
        k.terms = BTreeMap::new();
        k.dup_values = BTreeSet::new();
        // First-time seal from the live `forward`: no prior tombstone exists, but
        // reset for symmetry with the production re-seal path (Phase 2h-1 FIX).
        k.tombstones = RoaringBitmap::new();
        Ok(n_docs as u32)
    }

    /// TEST SEAM (Stage 2 Phase 2e-A, extended 2h-2): seal a Set field's in-RAM
    /// state into a columnar mmap segment under `dir`, then attach it so per-doc
    /// Set membership PREDICATE lookups (`set_contains` / `set_contains_any`)
    /// AND the inverted membership / Terms / boolean driver (`element_postings`
    /// / `element_df`) serve the sealed id range from the segment. Dumps
    /// `forward` in dense docid order `[0..n_docs)` (absent docs → `None`,
    /// present-empty → `Some(&[])`) plus the INVERTED postings (folded from
    /// `forward`) via [`crate::persistence::infrastructure::segment::set_writer::write_set_segment`] — a shared sorted
    /// string DICT + a fixed `u32[n_docs + 1]` CSR offsets column + a fixed
    /// packed dict-id column + a parallel per-element [`ROLE_SET_POSTINGS`]
    /// posting column.
    ///
    /// Phase 2h-2: this seam now mirrors PRODUCTION `seal_to_segment` — after
    /// attaching the reader it DROPS BOTH the in-RAM `forward` tail AND the
    /// inverted `elements` index (the RAM win). Queries then drive entirely from
    /// the mmap. A doc indexed AFTER sealing (id >= n_docs) re-populates the live
    /// `elements`/`forward` tail, which `element_postings`/`set_contains` compose
    /// with the segment base. Returns the sealed doc count.
    pub(crate) fn __seal_set_field_to_segment(
        &self,
        collection_id: &str,
        field: &str,
        dir: &std::path::Path,
    ) -> Result<u32> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        let n_docs = coll.interner.to_eid.len();
        let fi = coll
            .fields
            .get_mut(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        let FieldIndex::Set(s) = fi else {
            bail!("field `{field}` is not a Set field");
        };
        // Materialize each sealed doc's members as an ascending Vec<String>
        // (BTreeSet iterates sorted), or None for a doc with no set value. Owned
        // because the writer borrows the slices; keep them alive in `owned`.
        let owned: Vec<Option<Vec<String>>> = (0..n_docs as u32)
            .map(|id| s.forward.get(&id).map(|set| set.iter().cloned().collect()))
            .collect();
        let values: Vec<Option<&[String]>> = owned.iter().map(|o| o.as_deref()).collect();
        // INVERTED postings to seal: fold the live members (== the live `elements`
        // index restricted to the sealed id range) into a fresh BTreeMap.
        let mut elements: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
        for (id, v) in values.iter().enumerate() {
            if let Some(members) = v {
                for m in members.iter() {
                    elements.entry(m.clone()).or_default().insert(id as u32);
                }
            }
        }

        let path = dir.join(format!("{field}.lseg"));
        crate::persistence::infrastructure::segment::set_writer::write_set_segment(
            &path,
            n_docs as u64,
            &values,
            &elements,
        )?;
        let reader = crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
        debug_assert_eq!(reader.n_docs() as usize, n_docs);
        s.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
            std::sync::Arc::new(reader),
        )));
        // Drop the RAM index — the whole [0..n_docs) inverted+forward state is on
        // disk now (Phase 2h-2). Queries drive from the mmap segment.
        s.forward = FastHashMap::default();
        s.elements = BTreeMap::new();
        s.dup_values = BTreeSet::new();
        // First-time seal from the live `forward`: no prior tombstone exists, but
        // reset for symmetry with the production re-seal path (Phase 2h-2).
        s.tombstones = RoaringBitmap::new();
        Ok(n_docs as u32)
    }

    /// TEST SEAM (Stage 2 Phase 2e-B): seal a Text field's WHOLE in-RAM inverted
    /// index into a columnar mmap segment under `dir`, then attach it so the
    /// BM25 scan (`eval_match` / `match_doc_score`) and `estimate_selectivity`
    /// read from the sealed base plus any live overlays. Text term-frequency is
    /// NOT rebuildable,
    /// so unlike the Keyword/Set seams the inverted postings ARE stored: a sorted
    /// token DICT + a parallel per-token STORED posting block + a fixed
    /// `u32[n_docs]` DocLen column + the BM25 corpus scalars in the header (see
    /// [`crate::persistence::infrastructure::segment::text_writer::write_text_segment`]).
    ///
    /// This slice seals the whole field for ids `[0..n_docs)`. Phase 2h-4: after
    /// attaching, the bulky sealed-base `tokens` postings AND `distinct` AND
    /// `lens` are DROPPED (no RAM rebuild); later writes stay as live overlays,
    /// and `drop_eid` tombstones a sealed base id. `doc_len()` reads the explicit
    /// overlay before the segment DocLen column. Mirrors PRODUCTION
    /// `seal_to_segment`. Returns the sealed doc count.
    pub(crate) fn __seal_text_field_to_segment(
        &self,
        collection_id: &str,
        field: &str,
        dir: &std::path::Path,
    ) -> Result<u32> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        let n_docs = coll.interner.to_eid.len();
        let fi = coll
            .fields
            .get_mut(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        let FieldIndex::Text { idx, .. } = fi else {
            bail!("field `{field}` is not a Text field");
        };
        // DocLen column in dense docid order `[0..n_docs)`: each id's stored
        // length (0 for an absent doc), reproducing `TextIndex::doc_len`.
        let lens: Vec<u32> = (0..n_docs as u32).map(|id| idx.doc_len(id)).collect();
        let present = idx.present_for_seal(n_docs as u32, &|_| true);
        let tokens = idx.tokens_for_seal(&|_| true);

        let path = dir.join(format!("{field}.lseg"));
        crate::persistence::infrastructure::segment::text_writer::write_text_segment(
            &path,
            n_docs as u64,
            &tokens,
            &lens,
            &present,
            idx.doc_count,
            idx.total_doc_len,
        )?;
        let reader = crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
        debug_assert_eq!(reader.n_docs() as usize, n_docs);

        idx.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
            std::sync::Arc::new(reader),
        )));
        // Phase 2h-4: mirror PRODUCTION `seal_to_segment` — DROP the bulky `tokens`
        // postings AND `distinct` AND `lens` to disk (no rebuild). `drop_eid`
        // tombstones a sealed base id instead of consuming `distinct`; `doc_len()`
        // reads the segment DocLen column. First-time seal: no prior tombstone, but
        // reset for symmetry with the production re-seal path.
        idx.tokens = BTreeMap::new();
        idx.delta_docs.clear();
        idx.distinct = Vec::new();
        idx.lens = Vec::new();
        idx.clear_match_rank_cache();
        idx.tombstones = RoaringBitmap::new();
        Ok(n_docs as u32)
    }

    /// TEST SEAM (Stage 2 Phase 2d): seal a Hash field's in-RAM state into a
    /// columnar mmap segment under `dir`, then attach it so the per-doc Hamming
    /// hash read (`hash_at`) serves the sealed id range from the segment. Dumps
    /// `forward` in dense docid order `[0..n_docs)` (absent docs → `None`) via
    /// [`crate::persistence::infrastructure::segment::hash_writer::write_hash_segment`]. Mirrors the Number seam. Returns
    /// the sealed doc count.
    pub(crate) fn __seal_hash_field_to_segment(
        &self,
        collection_id: &str,
        field: &str,
        dir: &std::path::Path,
    ) -> Result<u32> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        let n_docs = coll.interner.to_eid.len();
        let fi = coll
            .fields
            .get_mut(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        let FieldIndex::Hash(h) = fi else {
            bail!("field `{field}` is not a Hash field");
        };
        let values: Vec<Option<u64>> = (0..n_docs as u32)
            .map(|id| h.forward.get(&id).copied())
            .collect();

        let path = dir.join(format!("{field}.lseg"));
        crate::persistence::infrastructure::segment::hash_writer::write_hash_segment(
            &path,
            n_docs as u64,
            &values,
        )?;
        let reader = crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
        debug_assert_eq!(reader.n_docs() as usize, n_docs);
        h.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
            std::sync::Arc::new(reader),
        )));
        h.tombstones.clear();
        Ok(n_docs as u32)
    }

    /// TEST SEAM (Stage 2 Phase 2d): seal a Vector field's exact-CPU
    /// (`flat-cpu`) corpus into a columnar mmap vector segment under `dir`,
    /// then attach it so the flat kNN scan reads each vector zero-copy off the
    /// page. Delegates to [`VectorIndex::__seal_flat_to_segment`]; returns the
    /// sealed vector count, or an error if the field is not a `flat-cpu` Vector
    /// (HNSW is out of scope for this slice).
    ///
    /// [`VectorIndex::__seal_flat_to_segment`]: crate::index::domain::vector::VectorIndex::__seal_flat_to_segment
    pub(crate) fn __seal_vector_field_to_segment(
        &self,
        collection_id: &str,
        field: &str,
        dir: &std::path::Path,
    ) -> Result<u32> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        let fi = coll
            .fields
            .get_mut(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        let FieldIndex::Vector { idx, .. } = fi else {
            bail!("field `{field}` is not a Vector field");
        };
        let path = dir.join(format!("{field}.lseg"));
        idx.__seal_flat_to_segment(&path)?
            .ok_or_else(|| anyhow!("field `{field}` is not a flat-cpu vector backend"))
    }

    /// TEST HELPER (Phase 2f-1): run the PRODUCTION collection-level
    /// `seal_to_segments` on a collection in place (seal every field, write the
    /// EID column, drop the forward payload). Used by the triple-path diff test
    /// to materialize PATH B (engine after seal-and-drop).
    pub(crate) fn __seal_collection_to_segments(
        &self,
        collection_id: &str,
        dir: &std::path::Path,
        applied_seq: u64,
    ) -> Result<()> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        coll.seal_to_segments(dir, applied_seq)
    }

    /// TEST HELPER (Phase 2f-1): the collection's field-spec schema (for driving
    /// `Collection::open_from_segments`, which needs the field types out-of-band).
    pub(crate) fn __collection_schema(
        &self,
        collection_id: &str,
    ) -> Result<BTreeMap<String, FieldSpec>> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        Ok(coll.schema.clone())
    }

    /// TEST HELPER (Phase 2f-1): build a fresh Engine whose single collection
    /// `collection_id` is reopened from the segments under `dir` via the
    /// PRODUCTION `Collection::open_from_segments` — NO CBOR snapshot, NO
    /// whole-collection load. Materializes PATH C of the triple-path diff test.
    pub(crate) fn __open_collection_from_segments(
        collection_id: &str,
        dir: &std::path::Path,
        schema: BTreeMap<String, FieldSpec>,
        version: u32,
    ) -> Result<std::sync::Arc<Engine>> {
        let coll = Collection::open_from_segments(dir, schema, version)?;
        let engine = Engine::new();
        {
            let mut state = engine
                .state
                .write()
                .map_err(|_| anyhow!("state poisoned"))?;
            state.collections.insert(collection_id.to_string(), coll);
        }
        Ok(std::sync::Arc::new(engine))
    }

    /// TEST HELPER (Phase 2f-1): a direct probe that a field's forward payload
    /// left RAM after a seal-and-drop — the "drop really frees RAM" assertion.
    /// Returns `(forward_len, tokens_len, has_segment)` for the named field.
    pub(crate) fn __field_forward_probe(
        &self,
        collection_id: &str,
        field: &str,
    ) -> Result<(usize, usize, bool)> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        let fi = coll
            .fields
            .get(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        Ok(match fi {
            FieldIndex::Number(n) => (n.forward_len(), 0, n.segment.is_some()),
            FieldIndex::Hash(h) => (h.forward.len(), 0, h.segment.is_some()),
            FieldIndex::Keyword(k) => (k.forward_len(), 0, k.segment.is_some()),
            FieldIndex::Set(s) => (s.forward.len(), 0, s.segment.is_some()),
            FieldIndex::Text { idx, .. } => (0, idx.tokens.len(), idx.segment.is_some()),
            FieldIndex::Vector { .. } => (0, 0, true),
        })
    }
}

// ---------------------------------------------------------------------------
// Dual-path diff test: segment-backed Number predicate read must be
// byte-identical to the live in-RAM read (Stage 2 Phase 2c).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod segment_predicate_diff_tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    fn fieldspec(t: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    /// `age` (Number, the field we seal), `kw` (Keyword), `body` (Text).
    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("age".into(), fieldspec(FieldType::Number, None));
        fields.insert("kw".into(), fieldspec(FieldType::Keyword, None));
        fields.insert(
            "body".into(),
            fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
        );
        CreateCollectionRequest { fields }
    }

    fn req(query: QueryNode) -> SearchRequest {
        SearchRequest {
            query,
            limit: 100_000, // larger than any corpus → page == full match set
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    /// (external_id, score) pairs for a query, mirroring planner_diff's shape.
    fn run(e: &Engine, query: QueryNode) -> Vec<(String, f32)> {
        e.search("c", req(query))
            .unwrap()
            .hits
            .into_iter()
            .map(|h| (h.external_id, h.score))
            .collect()
    }

    fn set_of(rows: &[(String, f32)]) -> BTreeSet<String> {
        rows.iter().map(|(e, _)| e.clone()).collect()
    }

    /// Scores keyed by external_id — so we can assert score byte-equality
    /// independent of result ordering.
    fn scores_of(rows: &[(String, f32)]) -> BTreeMap<String, u32> {
        rows.iter().map(|(e, s)| (e.clone(), s.to_bits())).collect()
    }

    /// Index one doc: always writes `kw` + `body`; writes `age` only when
    /// `age` is `Some` (so absent-value docs are part of the corpus).
    fn index_doc(e: &Engine, eid: &str, age: Option<f64>, kw: &str, tok: bool) {
        let mut items = vec![
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "kw".into(),
                value: FieldValue::String(kw.into()),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "body".into(),
                value: FieldValue::String(if tok {
                    "tok filler".into()
                } else {
                    "filler".into()
                }),
                version: None,
            },
        ];
        if let Some(a) = age {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "age".into(),
                value: FieldValue::Number(a),
                version: None,
            });
        }
        e.index(
            "c",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }

    /// A match-DRIVEN AND so the `range age` conjunct is applied as a per-doc
    /// PREDICATE (`clause_matches` → `NumberIndex::number_at`), which is the
    /// segment-backed read site under test. `tok` is the rare driver token.
    fn bool_filter(gte: f64, lt: f64) -> QueryNode {
        QueryNode::And(vec![
            QueryNode::Match(MatchQuery {
                field: "body".into(),
                text: "tok".into(),
                op: MatchOp::And,
            }),
            QueryNode::Range(RangeQuery {
                field: "age".into(),
                gt: None,
                gte: Some(RangeBound::Number(gte)),
                lt: Some(RangeBound::Number(lt)),
                lte: None,
            }),
        ])
    }

    /// filtered_search: match-driven AND with BOTH a `term kw` and a
    /// `range age` predicate — exercises the Term-Number and Range-Number
    /// segment predicate sites together.
    fn filtered_search(kw: &str, gte: f64, lt: f64) -> QueryNode {
        QueryNode::And(vec![
            QueryNode::Match(MatchQuery {
                field: "body".into(),
                text: "tok".into(),
                op: MatchOp::And,
            }),
            QueryNode::Term(TermQuery {
                field: "kw".into(),
                value: FieldValue::String(kw.into()),
            }),
            QueryNode::Range(RangeQuery {
                field: "age".into(),
                gt: None,
                gte: Some(RangeBound::Number(gte)),
                lt: Some(RangeBound::Number(lt)),
                lte: None,
            }),
        ])
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// PATH A (segment OFF, live in-RAM) must equal PATH B (Number field
        /// sealed to an mmap segment, then served from it) — same result SET
        /// and byte-identical scores — for both query shapes over a randomized
        /// corpus (varied N, value distribution, absent-`age` docs).
        #[test]
        fn segment_read_matches_live_read(
            docs in proptest::collection::vec(
                (
                    // age: ~1-in-5 docs have NO age value (absent column entry).
                    proptest::option::weighted(0.8, 0u32..30),
                    prop::sample::select(vec!["a", "b", "c", "d"]),
                    any::<bool>(),
                ),
                1..60,
            ),
            lo in 0u32..30,
            span in 1u32..30,
        ) {
            let hi = lo + span;

            // --- PATH A: build the live engine, run both shapes (segment OFF). ---
            let e = Arc::new(Engine::new());
            e.create_collection("c", schema()).unwrap();
            for (i, (age, kw, tok)) in docs.iter().enumerate() {
                index_doc(&e, &format!("d{i}"), age.map(|a| a as f64), kw, *tok);
            }

            let a_bool = run(&e, bool_filter(lo as f64, hi as f64));
            let a_filt = run(&e, filtered_search("c", lo as f64, hi as f64));

            // --- PATH B: seal `age` to a segment, flip it ON, rerun. ---
            let dir = tempfile::tempdir().unwrap();
            let sealed = e.__seal_number_field_to_segment("c", "age", dir.path()).unwrap();
            prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");

            let b_bool = run(&e, bool_filter(lo as f64, hi as f64));
            let b_filt = run(&e, filtered_search("c", lo as f64, hi as f64));

            // Result SET equality.
            prop_assert_eq!(set_of(&a_bool), set_of(&b_bool), "bool_filter set diverged");
            prop_assert_eq!(set_of(&a_filt), set_of(&b_filt), "filtered_search set diverged");
            // Scores byte-identical (f64::to_bits keyed by eid).
            prop_assert_eq!(scores_of(&a_bool), scores_of(&b_bool), "bool_filter scores diverged");
            prop_assert_eq!(scores_of(&a_filt), scores_of(&b_filt), "filtered_search scores diverged");
        }
    }

    /// A doc indexed AFTER sealing (docid >= segment n_docs) lives in the live
    /// `forward` tail and must still match through `number_at`'s fallback.
    #[test]
    fn doc_indexed_after_sealing_served_from_live_tail() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        // Two docs sealed into the segment.
        index_doc(&e, "sealed_in", Some(10.0), "c", true);
        index_doc(&e, "sealed_out", Some(99.0), "c", true);

        let dir = tempfile::tempdir().unwrap();
        let n = e
            .__seal_number_field_to_segment("c", "age", dir.path())
            .unwrap();
        assert_eq!(n, 2, "two docs sealed");

        // A NEW doc after sealing → docid 2 (>= n_docs) → lives in the live tail.
        index_doc(&e, "tail", Some(15.0), "c", true);

        // Range [12,20): only the tail doc qualifies. It is NOT in the segment,
        // so this proves number_at's `id >= n_docs` fallback to `forward`.
        let got = set_of(&run(&e, bool_filter(12.0, 20.0)));
        let want: BTreeSet<String> = ["tail".to_string()].into_iter().collect();
        assert_eq!(got, want, "tail doc must match via live fallback");

        // Range [8,12): only the sealed doc qualifies — served from the segment.
        let got = set_of(&run(&e, bool_filter(8.0, 12.0)));
        let want: BTreeSet<String> = ["sealed_in".to_string()].into_iter().collect();
        assert_eq!(got, want, "sealed doc must match via segment read");
    }

    /// Direct check that `NumberIndex::number_at` reads from the segment for
    /// sealed ids and falls back to the live tail past `n_docs` — independent
    /// of the query planner.
    #[test]
    fn number_at_segment_then_live_split() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_doc(&e, "a", Some(1.5), "x", false); // id 0
        index_doc(&e, "b", None, "x", false); // id 1, absent age
        index_doc(&e, "c", Some(-3.0), "x", false); // id 2

        let dir = tempfile::tempdir().unwrap();
        e.__seal_number_field_to_segment("c", "age", dir.path())
            .unwrap();
        index_doc(&e, "d", Some(7.0), "x", false); // id 3, live tail

        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Number(n) = coll.fields.get("age").unwrap() else {
            panic!("age must be a Number field");
        };
        assert!(n.segment.is_some(), "segment attached");
        assert_eq!(n.number_at(0), Some(SortableF64::new(1.5).unwrap())); // segment
        assert_eq!(n.number_at(1), None); // segment, absent
        assert_eq!(n.number_at(2), Some(SortableF64::new(-3.0).unwrap())); // segment
        assert_eq!(n.number_at(3), Some(SortableF64::new(7.0).unwrap())); // live tail
        assert_eq!(n.number_at(99), None); // unknown
    }
}

// ---------------------------------------------------------------------------
// Dual-path diff test: segment-backed Keyword predicate read must be
// byte-identical to the live in-RAM read (Stage 2 Phase 2e-A).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod segment_keyword_diff_tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    fn fieldspec(t: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    /// `kw` (Keyword, the field we seal) + `body` (Text, the AND driver).
    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("kw".into(), fieldspec(FieldType::Keyword, None));
        fields.insert(
            "body".into(),
            fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
        );
        CreateCollectionRequest { fields }
    }

    fn req(query: QueryNode) -> SearchRequest {
        SearchRequest {
            query,
            limit: 100_000,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    fn run(e: &Engine, query: QueryNode) -> Vec<(String, f32)> {
        e.search("c", req(query))
            .unwrap()
            .hits
            .into_iter()
            .map(|h| (h.external_id, h.score))
            .collect()
    }

    fn set_of(rows: &[(String, f32)]) -> BTreeSet<String> {
        rows.iter().map(|(e, _)| e.clone()).collect()
    }

    fn scores_of(rows: &[(String, f32)]) -> BTreeMap<String, u32> {
        rows.iter().map(|(e, s)| (e.clone(), s.to_bits())).collect()
    }

    /// Index one doc: always writes `body`; writes `kw` only when `kw` is
    /// `Some` (so absent-keyword docs are part of the corpus).
    fn index_doc(e: &Engine, eid: &str, kw: Option<&str>, tok: bool) {
        let mut items = vec![crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "body".into(),
            value: FieldValue::String(if tok {
                "tok filler".into()
            } else {
                "filler".into()
            }),
            version: None,
        }];
        if let Some(k) = kw {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "kw".into(),
                value: FieldValue::String(k.into()),
                version: None,
            });
        }
        e.index(
            "c",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }

    /// A match-DRIVEN AND so the `term kw` conjunct is applied as a per-doc
    /// PREDICATE (`clause_matches` → `KeywordIndex::keyword_at`) — the
    /// segment-backed read site under test. `tok` is the rare driver token.
    fn term_conjunct(kw: &str) -> QueryNode {
        QueryNode::And(vec![
            QueryNode::Match(MatchQuery {
                field: "body".into(),
                text: "tok".into(),
                op: MatchOp::And,
            }),
            QueryNode::Term(TermQuery {
                field: "kw".into(),
                value: FieldValue::String(kw.into()),
            }),
        ])
    }

    /// A match-DRIVEN AND with a `terms kw` (multi-value OR-of-terms) conjunct
    /// — exercises the Terms-Keyword segment predicate site (`keyword_at` in
    /// the `Terms` arm of `clause_matches`).
    fn terms_conjunct(kws: &[&str]) -> QueryNode {
        QueryNode::And(vec![
            QueryNode::Match(MatchQuery {
                field: "body".into(),
                text: "tok".into(),
                op: MatchOp::And,
            }),
            QueryNode::Terms(TermsQuery {
                field: "kw".into(),
                values: kws
                    .iter()
                    .map(|s| FieldValue::String((*s).into()))
                    .collect(),
            }),
        ])
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// PATH A (segment OFF, live in-RAM `forward`) must equal PATH B
        /// (Keyword field sealed to a var-width DICT + dict-id segment, then
        /// served from it) — same result SET and byte-identical scores — for
        /// both the `term kw` conjunct and the `terms kw` conjunct over a
        /// randomized corpus (varied N, value distribution, absent-`kw` docs).
        #[test]
        fn segment_read_matches_live_read(
            docs in proptest::collection::vec(
                (
                    // kw: ~1-in-5 docs have NO keyword (absent column entry).
                    proptest::option::weighted(
                        0.8,
                        prop::sample::select(vec!["alpha", "beta", "gamma", "delta"]),
                    ),
                    any::<bool>(),
                ),
                1..60,
            ),
        ) {
            // --- PATH A: build the live engine, run both shapes (segment OFF). ---
            let e = Arc::new(Engine::new());
            e.create_collection("c", schema()).unwrap();
            for (i, (kw, tok)) in docs.iter().enumerate() {
                index_doc(&e, &format!("d{i}"), *kw, *tok);
            }

            let a_term = run(&e, term_conjunct("alpha"));
            let a_terms = run(&e, terms_conjunct(&["beta", "gamma"]));

            // --- PATH B: seal `kw` to a segment, flip it ON, rerun. ---
            let dir = tempfile::tempdir().unwrap();
            let sealed = e.__seal_keyword_field_to_segment("c", "kw", dir.path()).unwrap();
            prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");

            let b_term = run(&e, term_conjunct("alpha"));
            let b_terms = run(&e, terms_conjunct(&["beta", "gamma"]));

            prop_assert_eq!(set_of(&a_term), set_of(&b_term), "term conjunct set diverged");
            prop_assert_eq!(set_of(&a_terms), set_of(&b_terms), "terms conjunct set diverged");
            prop_assert_eq!(scores_of(&a_term), scores_of(&b_term), "term scores diverged");
            prop_assert_eq!(scores_of(&a_terms), scores_of(&b_terms), "terms scores diverged");
        }
    }

    /// A doc indexed AFTER sealing (docid >= segment n_docs) lives in the live
    /// `forward` tail and must still match through `keyword_at`'s fallback.
    #[test]
    fn doc_indexed_after_sealing_served_from_live_tail() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_doc(&e, "sealed", Some("alpha"), true);
        index_doc(&e, "absent", None, true); // present-but-no-kw, id 1

        let dir = tempfile::tempdir().unwrap();
        let n = e
            .__seal_keyword_field_to_segment("c", "kw", dir.path())
            .unwrap();
        assert_eq!(n, 2, "two docs sealed");

        // A NEW doc after sealing → docid 2 (>= n_docs) → lives in the live tail.
        index_doc(&e, "tail", Some("beta"), true);

        // term beta: only the tail doc qualifies, NOT in the segment → proves
        // keyword_at's id >= n_docs fallback to forward.
        let got = set_of(&run(&e, term_conjunct("beta")));
        let want: BTreeSet<String> = ["tail".to_string()].into_iter().collect();
        assert_eq!(got, want, "tail doc must match via live fallback");

        // term alpha: only the sealed doc qualifies → served from the segment.
        let got = set_of(&run(&e, term_conjunct("alpha")));
        let want: BTreeSet<String> = ["sealed".to_string()].into_iter().collect();
        assert_eq!(got, want, "sealed doc must match via segment read");
    }

    /// Direct planner-free check that `KeywordIndex::keyword_at` reads from the
    /// segment for sealed ids (incl an absent doc) and falls back to the live
    /// tail past `n_docs`.
    #[test]
    fn keyword_at_segment_then_live_split() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_doc(&e, "a", Some("alpha"), false); // id 0
        index_doc(&e, "b", None, false); // id 1, absent kw
        index_doc(&e, "c", Some("gamma"), false); // id 2

        let dir = tempfile::tempdir().unwrap();
        e.__seal_keyword_field_to_segment("c", "kw", dir.path())
            .unwrap();
        index_doc(&e, "d", Some("delta"), false); // id 3, live tail

        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
            panic!("kw must be a Keyword field");
        };
        assert!(k.segment.is_some(), "segment attached");
        assert_eq!(k.keyword_at(0).as_deref(), Some("alpha")); // segment
        assert_eq!(k.keyword_at(1), None); // segment, absent
        assert_eq!(k.keyword_at(2).as_deref(), Some("gamma")); // segment
        assert_eq!(k.keyword_at(3).as_deref(), Some("delta")); // live tail
        assert_eq!(k.keyword_at(99), None); // unknown
    }
}

// ---------------------------------------------------------------------------
// Dual-path diff test for the INVERTED Keyword driver (Stage 2 Phase 2h-1):
// the segment-driven Term/Terms/boolean RoaringBitmap algebra (`term_postings`
// / `term_df`) must be byte-identical to the in-RAM `terms` index, AND after a
// seal the RAM index is DROPPED (the disk=all win) while queries keep serving
// entirely from the mmap segment. This is the keystone test for Phase 2h-1.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod segment_keyword_inverted_diff_tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    fn fieldspec(t: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    /// `kw` (Keyword, sealed) + `cat` (a second Keyword, for boolean AND/OR
    /// cross-field algebra). Both are inverted-driver fields.
    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("kw".into(), fieldspec(FieldType::Keyword, None));
        fields.insert("cat".into(), fieldspec(FieldType::Keyword, None));
        CreateCollectionRequest { fields }
    }

    fn req(query: QueryNode) -> SearchRequest {
        SearchRequest {
            query,
            limit: 100_000,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    fn run(e: &Engine, query: QueryNode) -> Vec<(String, f32)> {
        e.search("c", req(query))
            .unwrap()
            .hits
            .into_iter()
            .map(|h| (h.external_id, h.score))
            .collect()
    }

    fn set_of(rows: &[(String, f32)]) -> BTreeSet<String> {
        rows.iter().map(|(e, _)| e.clone()).collect()
    }

    /// (external_id, total) result for a query — drives the `try_plan`
    /// standalone-Term page+total path too.
    fn search_total(e: &Engine, query: QueryNode) -> u64 {
        e.search("c", req(query)).unwrap().total
    }

    fn index_kw(e: &Engine, eid: &str, kw: Option<&str>, cat: Option<&str>) {
        let mut items = Vec::new();
        if let Some(k) = kw {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "kw".into(),
                value: FieldValue::String(k.into()),
                version: None,
            });
        }
        if let Some(c) = cat {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "cat".into(),
                value: FieldValue::String(c.into()),
                version: None,
            });
        }
        // A doc with neither field would have no postings anywhere; ensure at
        // least the kw field so the doc is interned.
        if items.is_empty() {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "kw".into(),
                value: FieldValue::String("zzz_filler".into()),
                version: None,
            });
        }
        e.index(
            "c",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }

    fn term(field: &str, v: &str) -> QueryNode {
        QueryNode::Term(TermQuery {
            field: field.into(),
            value: FieldValue::String(v.into()),
        })
    }

    fn terms(field: &str, vs: &[&str]) -> QueryNode {
        QueryNode::Terms(TermsQuery {
            field: field.into(),
            values: vs.iter().map(|s| FieldValue::String((*s).into())).collect(),
        })
    }

    /// Assert the Keyword field `kw` has an EMPTY in-RAM `terms` index but an
    /// attached segment — the RAM-bounded invariant after a seal/reopen.
    fn assert_terms_dropped(e: &Engine) {
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
            panic!("kw must be a Keyword field");
        };
        assert!(k.segment.is_some(), "segment must be attached");
        assert!(
            k.terms.is_empty(),
            "RAM `terms` index must be DROPPED after seal (got {} entries)",
            k.terms.len()
        );
        assert!(
            k.forward.is_empty(),
            "RAM `forward` must be dropped after seal"
        );
        assert!(
            k.dense_forward.is_empty(),
            "RAM dense `forward` must be dropped after seal"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// PATH A (segment OFF, in-RAM `terms`) must equal PATH B (sealed: RAM
        /// `terms` DROPPED, driven from the mmap posting column) for the whole
        /// inverted-driver surface: standalone `Term`, standalone `Terms`,
        /// boolean `Or` of two terms, boolean `And` across two keyword fields,
        /// and the `try_plan` standalone-Term total. Byte-identical result sets
        /// AND identical totals on both paths.
        #[test]
        fn inverted_segment_matches_live(
            docs in proptest::collection::vec(
                (
                    proptest::option::weighted(
                        0.85,
                        prop::sample::select(vec!["alpha", "beta", "gamma", "delta"]),
                    ),
                    proptest::option::weighted(
                        0.7,
                        prop::sample::select(vec!["red", "green"]),
                    ),
                ),
                1..70,
            ),
        ) {
            // --- PATH A: in-RAM inverted index (segment OFF). ---
            let e = Arc::new(Engine::new());
            e.create_collection("c", schema()).unwrap();
            for (i, (kw, cat)) in docs.iter().enumerate() {
                index_kw(&e, &format!("d{i}"), *kw, *cat);
            }

            let a_term = run(&e, term("kw", "alpha"));
            let a_terms = run(&e, terms("kw", &["beta", "gamma"]));
            let a_or = run(&e, QueryNode::Or(vec![term("kw", "alpha"), term("kw", "delta")]));
            let a_and = run(&e, QueryNode::And(vec![term("kw", "beta"), term("cat", "red")]));
            let a_total = search_total(&e, term("kw", "gamma"));

            // --- PATH B: seal `kw` (drops RAM `terms`), rerun from the mmap. ---
            let dir = tempfile::tempdir().unwrap();
            let sealed = e.__seal_keyword_field_to_segment("c", "kw", dir.path()).unwrap();
            prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");
            assert_terms_dropped(&e); // RAM-bounded: terms index gone

            let b_term = run(&e, term("kw", "alpha"));
            let b_terms = run(&e, terms("kw", &["beta", "gamma"]));
            let b_or = run(&e, QueryNode::Or(vec![term("kw", "alpha"), term("kw", "delta")]));
            let b_and = run(&e, QueryNode::And(vec![term("kw", "beta"), term("cat", "red")]));
            let b_total = search_total(&e, term("kw", "gamma"));

            prop_assert_eq!(set_of(&a_term), set_of(&b_term), "Term set diverged");
            prop_assert_eq!(set_of(&a_terms), set_of(&b_terms), "Terms set diverged");
            prop_assert_eq!(set_of(&a_or), set_of(&b_or), "Or set diverged");
            prop_assert_eq!(set_of(&a_and), set_of(&b_and), "And set diverged");
            prop_assert_eq!(a_total, b_total, "standalone-term total diverged");
        }
    }

    /// RAM-BOUNDED + REOPEN-NO-REBUILD: seal the WHOLE collection to disk, reopen
    /// from the segments alone (no CBOR snapshot), and assert the reopened
    /// Keyword field's `terms` map is EMPTY while Term/Terms queries still return
    /// correct results entirely from the mmap segment.
    #[test]
    fn reopen_drives_from_segment_with_empty_terms() {
        let dir = tempfile::tempdir().unwrap();
        // Build the collection and capture the wanted answers (segment OFF).
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_kw(&e, "a", Some("alpha"), Some("red"));
        index_kw(&e, "b", Some("beta"), Some("green"));
        index_kw(&e, "c2", Some("gamma"), None);
        index_kw(&e, "d", Some("alpha"), Some("red"));
        index_kw(&e, "e", None, Some("green")); // present-but-no-kw

        let want_alpha = set_of(&run(&e, term("kw", "alpha")));
        let want_beta_gamma = set_of(&run(&e, terms("kw", &["beta", "gamma"])));

        // PRODUCTION whole-collection seal (drops every field's RAM driver), then
        // reopen from the segments alone (no CBOR snapshot, no whole-collection
        // load) via the production `open_from_segments`.
        e.__seal_collection_to_segments("c", dir.path(), 1).unwrap();
        let schema = e.__collection_schema("c").unwrap();
        let e2 = Engine::__open_collection_from_segments("c", dir.path(), schema, 1).unwrap();

        // The reopened Keyword `terms` map must be EMPTY (no RAM rebuild) ...
        {
            let state = e2.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
                panic!("kw must be a Keyword field");
            };
            assert!(k.segment.is_some(), "reopened segment attached");
            assert!(
                k.terms.is_empty(),
                "reopen must NOT rebuild `terms` in RAM (got {} entries)",
                k.terms.len()
            );
        }

        // ... yet the inverted queries still resolve, entirely from the mmap.
        assert_eq!(
            set_of(&run(&e2, term("kw", "alpha"))),
            want_alpha,
            "Term post-reopen"
        );
        assert_eq!(
            set_of(&run(&e2, terms("kw", &["beta", "gamma"]))),
            want_beta_gamma,
            "Terms post-reopen"
        );
    }

    /// LIVE-TAIL UNION: seal, then index more docs (tail into the live `terms`),
    /// and assert a Term query returns base (segment) + tail (RAM) composed.
    #[test]
    fn live_tail_unions_with_segment_base() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_kw(&e, "base0", Some("alpha"), None); // id 0, sealed base
        index_kw(&e, "base1", Some("beta"), None); // id 1, sealed base

        let dir = tempfile::tempdir().unwrap();
        let n = e
            .__seal_keyword_field_to_segment("c", "kw", dir.path())
            .unwrap();
        assert_eq!(n, 2);
        assert_terms_dropped(&e); // base index now on disk

        // Tail docs after seal → land in the live `terms` tail (ids >= n_docs).
        index_kw(&e, "tail0", Some("alpha"), None); // id 2, tail, SAME term as base0
        index_kw(&e, "tail1", Some("gamma"), None); // id 3, tail, NEW term

        // term alpha: base id (base0) UNION tail id (tail0).
        let got = set_of(&run(&e, term("kw", "alpha")));
        let want: BTreeSet<String> = ["base0".into(), "tail0".into()].into_iter().collect();
        assert_eq!(got, want, "alpha must union segment base + live tail");

        // term gamma: ONLY the tail (not in the segment).
        let got = set_of(&run(&e, term("kw", "gamma")));
        let want: BTreeSet<String> = ["tail1".into()].into_iter().collect();
        assert_eq!(got, want, "gamma must come from the live tail alone");

        // term beta: ONLY the segment base.
        let got = set_of(&run(&e, term("kw", "beta")));
        let want: BTreeSet<String> = ["base1".into()].into_iter().collect();
        assert_eq!(got, want, "beta must come from the segment base alone");

        // df composition: term_df(alpha) = 1 (base) + 1 (tail) = 2.
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
            panic!("kw must be a Keyword field");
        };
        assert_eq!(
            k.term_df("alpha"),
            2,
            "df must sum segment base + live tail"
        );
        assert_eq!(k.term_df("beta"), 1, "df beta segment-only");
        assert_eq!(k.term_df("gamma"), 1, "df gamma tail-only");
        assert_eq!(k.term_df("missing"), 0, "df absent term");
    }

    #[test]
    fn prefix_composes_segment_tail_and_delete_tombstones() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_kw(&e, "sealed-keep", Some("台北市/大安區"), None);
        index_kw(&e, "sealed-delete", Some("台北市/信義區"), None);
        index_kw(&e, "sealed-other", Some("新北市/板橋區"), None);

        let dir = tempfile::tempdir().unwrap();
        e.__seal_keyword_field_to_segment("c", "kw", dir.path())
            .unwrap();
        index_kw(&e, "tail-keep", Some("台北市/中正區"), None);
        index_kw(&e, "tail-other", Some("桃園市/桃園區"), None);
        e.delete("c", "sealed-delete", None).unwrap();

        let query = QueryNode::Prefix(PrefixQuery {
            field: "kw".into(),
            value: "台北市/".into(),
        });
        let got = set_of(&run(&e, query));
        let want: BTreeSet<String> = ["sealed-keep".into(), "tail-keep".into()]
            .into_iter()
            .collect();
        assert_eq!(got, want);
    }

    // -----------------------------------------------------------------------
    // Phase 2h-1 FIX: delete-after-seal query-time tombstone. After 2h-1 dropped
    // the in-RAM `terms` index at seal, `drop_eid` on a SEALED base docid was a
    // NO-OP (the on-disk posting is immutable), so segment-driven Keyword queries
    // LEAKED the deleted doc until the next re-seal. The fix records sealed-base
    // deletes in a per-field `tombstones` RoaringBitmap that every segment-ON
    // accessor subtracts. These tests pin the fix and prove its teeth.
    // -----------------------------------------------------------------------

    /// (value → external_ids) of every duplicate group, as a comparable map.
    fn dup_map(e: &Engine, field: &str) -> BTreeMap<String, BTreeSet<String>> {
        e.duplicates(
            "c",
            crate::shared_kernel::types::search::DuplicatesRequest {
                field: field.into(),
                min_group_size: 2,
                limit: 100_000,
                offset: 0,
            },
        )
        .unwrap()
        .groups
        .into_iter()
        .map(|g| {
            let v = g.value.as_str().unwrap().to_string();
            (v, g.external_ids.into_iter().collect::<BTreeSet<String>>())
        })
        .collect()
    }

    /// `unique_terms` of `field` via the public stats surface.
    fn uniq(e: &Engine, field: &str) -> u64 {
        e.stats("c")
            .unwrap()
            .fields
            .get(field)
            .unwrap()
            .unique_terms
    }

    /// DELETE-AFTER-SEAL **WITHOUT** RE-SEAL: seal a Keyword corpus, delete
    /// several BASE docs (no re-seal), then assert every segment-ON query equals
    /// an in-RAM ORACLE built from the identical op sequence but NEVER sealed —
    /// byte-identical result SETS for standalone Term, multi-term Terms, boolean
    /// Or, cross-field And, plus identical `duplicates`/`unique_terms`. This is
    /// the window 2g-A's seal-time GC does NOT cover; the tombstone closes it.
    #[test]
    fn delete_after_seal_without_reseal_matches_oracle() {
        // Shared corpus builder so the oracle and the sealed engine see the
        // SAME index sequence. d0..d5 on `kw`/`cat`; alpha appears 3x, beta 2x,
        // red 3x → real duplicate groups before any delete.
        fn build(e: &Engine) {
            e.create_collection("c", schema()).unwrap();
            index_kw(e, "d0", Some("alpha"), Some("red"));
            index_kw(e, "d1", Some("alpha"), Some("red"));
            index_kw(e, "d2", Some("beta"), Some("green"));
            index_kw(e, "d3", Some("alpha"), Some("red"));
            index_kw(e, "d4", Some("beta"), Some("green"));
            index_kw(e, "d5", Some("gamma"), None);
        }
        // The deletes to apply on BOTH paths (whole-doc deletes).
        let to_delete = ["d1", "d3", "d4"];

        // --- ORACLE: in-RAM, never sealed. Same build + same deletes. ---
        let oracle = Arc::new(Engine::new());
        build(&oracle);
        for d in to_delete {
            oracle.delete("c", d, None).unwrap();
        }

        // --- SUBJECT: seal kw + cat to segments, THEN delete (no re-seal). ---
        let subject = Arc::new(Engine::new());
        build(&subject);
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_keyword_field_to_segment("c", "kw", dir.path())
            .unwrap();
        subject
            .__seal_keyword_field_to_segment("c", "cat", dir.path())
            .unwrap();
        assert_terms_dropped(&subject); // RAM `terms` gone — drives from mmap
                                        // Delete AFTER the seal, with NO re-seal → exercises the tombstone path.
        for d in to_delete {
            subject.delete("c", d, None).unwrap();
        }

        // The deleted ids are now tombstoned (base ids < n_docs), NOT removed
        // from any on-disk posting. Confirm the bitmap actually recorded them.
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
                panic!("kw");
            };
            assert!(k.terms.is_empty(), "sealed: terms still dropped");
            assert_eq!(k.tombstones.len(), 3, "three base deletes tombstoned");
        }

        // Result SETS must match the oracle on every driver surface.
        let q_term = term("kw", "alpha");
        let q_terms = terms("kw", &["alpha", "beta"]);
        let q_or = QueryNode::Or(vec![term("kw", "alpha"), term("kw", "beta")]);
        let q_and = QueryNode::And(vec![term("kw", "alpha"), term("cat", "red")]);

        assert_eq!(
            set_of(&run(&subject, q_term.clone())),
            set_of(&run(&oracle, q_term)),
            "standalone Term leaked a deleted doc"
        );
        assert_eq!(
            set_of(&run(&subject, q_terms.clone())),
            set_of(&run(&oracle, q_terms)),
            "multi-term Terms leaked a deleted doc"
        );
        assert_eq!(
            set_of(&run(&subject, q_or.clone())),
            set_of(&run(&oracle, q_or)),
            "boolean Or leaked a deleted doc"
        );
        assert_eq!(
            set_of(&run(&subject, q_and.clone())),
            set_of(&run(&oracle, q_and)),
            "cross-field And leaked a deleted doc"
        );

        // duplicates + unique_terms must match the oracle on sealed data.
        // After deleting d1,d3,d4: alpha={d0}, beta={d2}, gamma={d5} on kw →
        // NO kw duplicate group survives; on cat red={d0}, green={d2} → none.
        assert_eq!(
            dup_map(&subject, "kw"),
            dup_map(&oracle, "kw"),
            "find_duplicates(kw) diverged on sealed-after-delete"
        );
        assert_eq!(
            dup_map(&subject, "cat"),
            dup_map(&oracle, "cat"),
            "find_duplicates(cat) diverged on sealed-after-delete"
        );
        assert_eq!(
            uniq(&subject, "kw"),
            uniq(&oracle, "kw"),
            "unique_terms(kw) diverged on sealed-after-delete"
        );
        assert_eq!(
            uniq(&subject, "cat"),
            uniq(&oracle, "cat"),
            "unique_terms(cat) diverged on sealed-after-delete"
        );

        // Concrete spot-checks (not just oracle-equality): the deleted docs are
        // GONE and a fully-deleted term yields None (empty result), not a leak.
        let alpha = set_of(&run(&subject, term("kw", "alpha")));
        assert_eq!(
            alpha,
            ["d0".to_string()].into_iter().collect::<BTreeSet<_>>(),
            "alpha must be only the surviving d0"
        );
        assert!(
            run(&subject, term("kw", "beta"))
                .iter()
                .all(|(eid, _)| eid != "d4"),
            "deleted d4 must not appear under beta"
        );
    }

    /// RE-SEAL (CHECKPOINT) after delete: once the field is re-sealed the
    /// deletions are BAKED into the new segment (via 2g-A's live(id) GC), the
    /// tombstone is CLEARED, and queries still match the oracle — composing the
    /// query-time tombstone with the seal-time GC.
    #[test]
    fn reseal_bakes_deletes_and_clears_tombstone() {
        fn build(e: &Engine) {
            e.create_collection("c", schema()).unwrap();
            index_kw(e, "d0", Some("alpha"), Some("red"));
            index_kw(e, "d1", Some("alpha"), Some("red"));
            index_kw(e, "d2", Some("beta"), Some("green"));
            index_kw(e, "d3", Some("gamma"), None);
        }
        let to_delete = ["d1", "d2"];

        let oracle = Arc::new(Engine::new());
        build(&oracle);
        for d in to_delete {
            oracle.delete("c", d, None).unwrap();
        }

        let subject = Arc::new(Engine::new());
        build(&subject);
        let dir = tempfile::tempdir().unwrap();
        // First seal (the WHOLE collection so re-seal has a working live(id)/
        // eid_fields GC), then delete, then RE-SEAL (checkpoint).
        subject
            .__seal_collection_to_segments("c", dir.path(), 1)
            .unwrap();
        for d in to_delete {
            subject.delete("c", d, None).unwrap();
        }
        // Tombstone holds the two base deletes pre-re-seal.
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
                panic!("kw");
            };
            assert_eq!(k.tombstones.len(), 2, "deletes tombstoned before re-seal");
        }
        // RE-SEAL: the live(id) gather (2g-A) excludes the tombstoned ids, so the
        // NEW segment has them absent; the tombstone is reset to empty.
        let dir2 = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir2.path(), 2)
            .unwrap();
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
                panic!("kw");
            };
            assert!(
                k.tombstones.is_empty(),
                "tombstone must be CLEARED after re-seal (deletes baked in)"
            );
        }

        // Post-re-seal queries match the oracle, now with an EMPTY tombstone (the
        // new segment itself has the deleted docs absent).
        let q_term = term("kw", "alpha");
        assert_eq!(
            set_of(&run(&subject, q_term.clone())),
            set_of(&run(&oracle, q_term)),
            "post-re-seal Term diverged from oracle"
        );
        assert_eq!(
            set_of(&run(&subject, term("kw", "beta"))),
            BTreeSet::new(),
            "beta fully deleted — must be empty after re-seal"
        );
        assert_eq!(
            uniq(&subject, "kw"),
            uniq(&oracle, "kw"),
            "unique_terms diverged after re-seal"
        );
    }
}

// ---------------------------------------------------------------------------
// Dual-path diff test: the segment-backed Number RANGE index (Phase 2h-3, WS2
// BKD) must answer range / exact / boolean / sort queries byte-identically to
// the in-RAM `values: BTreeMap<SortableF64, RoaringBitmap>` range walk. The crux
// is the on-disk SORTED-VALUE column (`ROLE_NUMBER_SORTED`) binary-searched by
// `number_range` honoring every inclusive/exclusive bound + open-endedness case,
// with the 2h tombstone reused for delete-after-seal.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod segment_number_range_diff_tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    fn fieldspec(t: FieldType) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    /// `price` (Number, the field we seal) + `cat` (Keyword, for boolean
    /// cross-field And/Or algebra).
    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("price".into(), fieldspec(FieldType::Number));
        fields.insert("cat".into(), fieldspec(FieldType::Keyword));
        fields.insert("kw".into(), fieldspec(FieldType::Keyword));
        CreateCollectionRequest { fields }
    }

    fn req(query: QueryNode) -> SearchRequest {
        SearchRequest {
            query,
            limit: 100_000,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    fn req_sort(query: QueryNode, field: &str, order: SortOrder) -> SearchRequest {
        SearchRequest {
            query,
            limit: 100_000,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
                field: field.into(),
                order,
                missing: SortMissing::Exclude,
            }]),
            track_total: true,
            collapse: None,
        }
    }

    fn run(e: &Engine, query: QueryNode) -> Vec<(String, f32)> {
        e.search("c", req(query))
            .unwrap()
            .hits
            .into_iter()
            .map(|h| (h.external_id, h.score))
            .collect()
    }

    /// Sort-driven page (drives `try_plan`'s number-sort path over the
    /// sorted-value column). Returns the ORDERED external_ids.
    fn run_sorted(e: &Engine, query: QueryNode, field: &str, order: SortOrder) -> Vec<String> {
        e.search("c", req_sort(query, field, order))
            .unwrap()
            .hits
            .into_iter()
            .map(|h| h.external_id)
            .collect()
    }

    fn set_of(rows: &[(String, f32)]) -> BTreeSet<String> {
        rows.iter().map(|(e, _)| e.clone()).collect()
    }

    fn search_total(e: &Engine, query: QueryNode) -> u64 {
        e.search("c", req(query)).unwrap().total
    }

    fn index_num(e: &Engine, eid: &str, price: Option<f64>, cat: Option<&str>) {
        let mut items = Vec::new();
        if let Some(p) = price {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "price".into(),
                value: FieldValue::Number(p),
                version: None,
            });
        }
        if let Some(c) = cat {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "cat".into(),
                value: FieldValue::String(c.into()),
                version: None,
            });
        }
        // Ensure the doc is interned even when both fields are absent.
        if items.is_empty() {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "cat".into(),
                value: FieldValue::String("zzz_filler".into()),
                version: None,
            });
        }
        e.index(
            "c",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }

    fn rangeq(gte: Option<f64>, gt: Option<f64>, lte: Option<f64>, lt: Option<f64>) -> QueryNode {
        QueryNode::Range(RangeQuery {
            field: "price".into(),
            gte: gte.map(RangeBound::Number),
            gt: gt.map(RangeBound::Number),
            lte: lte.map(RangeBound::Number),
            lt: lt.map(RangeBound::Number),
        })
    }

    fn termnum(v: f64) -> QueryNode {
        QueryNode::Term(TermQuery {
            field: "price".into(),
            value: FieldValue::Number(v),
        })
    }

    fn termsnum(vs: &[f64]) -> QueryNode {
        QueryNode::Terms(TermsQuery {
            field: "price".into(),
            values: vs.iter().map(|v| FieldValue::Number(*v)).collect(),
        })
    }

    fn termkw(field: &str, v: &str) -> QueryNode {
        QueryNode::Term(TermQuery {
            field: field.into(),
            value: FieldValue::String(v.into()),
        })
    }

    /// The full battery of range/exact/boolean queries the dual-path test runs.
    /// Every inclusive/exclusive bound + open-endedness combination, exact-match,
    /// empty ranges, and boolean composition with another field.
    fn all_queries() -> Vec<(&'static str, QueryNode)> {
        vec![
            // closed ranges (inclusive both)
            ("closed_incl", rangeq(Some(-5.0), None, Some(5.0), None)),
            // closed range exclusive both
            ("closed_excl", rangeq(None, Some(-5.0), None, Some(5.0))),
            // closed range mixed inclusivity
            ("mixed_gte_lt", rangeq(Some(-2.0), None, None, Some(7.0))),
            ("mixed_gt_lte", rangeq(None, Some(-2.0), Some(7.0), None)),
            // open-low (..hi]
            ("open_low_incl", rangeq(None, None, Some(3.0), None)),
            ("open_low_excl", rangeq(None, None, None, Some(3.0))),
            // open-high [lo..
            ("open_high_incl", rangeq(Some(-3.0), None, None, None)),
            ("open_high_excl", rangeq(None, Some(-3.0), None, None)),
            // fully open
            ("fully_open", rangeq(None, None, None, None)),
            // exact via inclusive lo==hi (single-value)
            ("single_val", rangeq(Some(0.0), None, Some(0.0), None)),
            // empty range: lo > hi
            ("empty_inverted", rangeq(Some(5.0), None, Some(-5.0), None)),
            // empty range: exclusive at the same point
            ("empty_excl_point", rangeq(None, Some(1.0), None, Some(1.0))),
            // exact-match Term (lo==hi semantics)
            ("exact_term_0", termnum(0.0)),
            ("exact_term_neg", termnum(-3.0)),
            ("exact_term_missing", termnum(999.0)),
            // multi-value Terms
            ("terms_multi", termsnum(&[-3.0, 0.0, 3.0])),
            // boolean And with another field
            (
                "and_range_cat",
                QueryNode::And(vec![
                    rangeq(Some(-5.0), None, Some(5.0), None),
                    termkw("cat", "red"),
                ]),
            ),
            // boolean Or of two ranges
            (
                "or_two_ranges",
                QueryNode::Or(vec![
                    rangeq(None, None, None, Some(-2.0)),
                    rangeq(Some(2.0), None, None, None),
                ]),
            ),
            // boolean And of exact + range (drives the cheapest-clause planner)
            (
                "and_exact_range",
                QueryNode::And(vec![
                    termnum(0.0),
                    rangeq(Some(-5.0), None, Some(5.0), None),
                ]),
            ),
        ]
    }

    /// Assert the Number field `price` has an EMPTY in-RAM `values` index but an
    /// attached segment — the RAM-bounded invariant after a seal.
    fn assert_values_dropped(e: &Engine) {
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Number(n) = coll.fields.get("price").unwrap() else {
            panic!("price must be a Number field");
        };
        assert!(n.segment.is_some(), "segment must be attached");
        assert!(
            n.values.is_empty(),
            "RAM `values` index must be DROPPED after seal (got {} entries)",
            n.values.len()
        );
        assert!(
            n.forward.is_empty(),
            "RAM `forward` must be dropped after seal"
        );
    }

    /// (value → external_ids) duplicate groups for a Number field, comparable.
    fn dup_map(e: &Engine, field: &str) -> BTreeMap<String, BTreeSet<String>> {
        e.duplicates(
            "c",
            crate::shared_kernel::types::search::DuplicatesRequest {
                field: field.into(),
                min_group_size: 2,
                limit: 100_000,
                offset: 0,
            },
        )
        .unwrap()
        .groups
        .into_iter()
        .map(|g| {
            // Number duplicate group values serialize as JSON numbers.
            let v = g.value.to_string();
            (v, g.external_ids.into_iter().collect::<BTreeSet<String>>())
        })
        .collect()
    }

    fn uniq(e: &Engine, field: &str) -> u64 {
        e.stats("c")
            .unwrap()
            .fields
            .get(field)
            .unwrap()
            .unique_terms
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// PATH A (segment OFF, in-RAM `values` range walk) must equal PATH B
        /// (sealed: RAM `values` DROPPED, range/exact driven from the on-disk
        /// SORTED-VALUE column binary-search) for the WHOLE battery: closed /
        /// open-low / open-high / fully-open ranges, EXCLUSIVE vs INCLUSIVE
        /// bounds, empty ranges, exact-match (lo==hi), single-value, multi-Terms,
        /// boolean And/Or with another field, AND the asc/desc number-sort page.
        /// Byte-identical result SETS + totals on both paths. Stress varied f64:
        /// negatives, zeros (±0.0), duplicates, large/small magnitudes.
        #[test]
        fn range_segment_matches_live(
            docs in proptest::collection::vec(
                (
                    proptest::option::weighted(
                        0.9,
                        prop::sample::select(vec![
                            -1000.5_f64, -7.0, -3.0, -2.0, -0.0, 0.0, 1.0, 2.0, 3.0,
                            3.0, 0.0, -3.0, 7.0, 42.0, 1e9, -1e9, 0.5, -0.5,
                        ]),
                    ),
                    proptest::option::weighted(
                        0.7,
                        prop::sample::select(vec!["red", "green", "blue"]),
                    ),
                ),
                1..80,
            ),
        ) {
            // --- PATH A: in-RAM range index (segment OFF). ---
            let e = Arc::new(Engine::new());
            e.create_collection("c", schema()).unwrap();
            for (i, (price, cat)) in docs.iter().enumerate() {
                index_num(&e, &format!("d{i}"), *price, *cat);
            }

            let qs = all_queries();
            let a_sets: Vec<BTreeSet<String>> =
                qs.iter().map(|(_, q)| set_of(&run(&e, q.clone()))).collect();
            let a_totals: Vec<u64> =
                qs.iter().map(|(_, q)| search_total(&e, q.clone())).collect();
            let a_sort_asc = run_sorted(&e, rangeq(None, None, None, None), "price", SortOrder::Asc);
            let a_sort_desc = run_sorted(&e, rangeq(None, None, None, None), "price", SortOrder::Desc);
            let a_uniq = uniq(&e, "price");

            // --- PATH B: seal `price` (drops RAM `values`), rerun from the mmap. ---
            let dir = tempfile::tempdir().unwrap();
            let sealed = e.__seal_number_field_to_segment("c", "price", dir.path()).unwrap();
            prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");
            assert_values_dropped(&e); // RAM-bounded: values index gone

            for (i, (name, q)) in qs.iter().enumerate() {
                let b_set = set_of(&run(&e, q.clone()));
                prop_assert_eq!(&a_sets[i], &b_set, "result SET diverged for `{}`", name);
                let b_total = search_total(&e, q.clone());
                prop_assert_eq!(a_totals[i], b_total, "total diverged for `{}`", name);
            }
            let b_sort_asc = run_sorted(&e, rangeq(None, None, None, None), "price", SortOrder::Asc);
            let b_sort_desc = run_sorted(&e, rangeq(None, None, None, None), "price", SortOrder::Desc);
            // Sort order is the field-sorted walk — must be value-identical (the
            // ORDERED sequence, not just the set), since `try_plan` drives the page
            // straight off the sorted-value column.
            prop_assert_eq!(&a_sort_asc, &b_sort_asc, "asc number-sort page diverged");
            prop_assert_eq!(&a_sort_desc, &b_sort_desc, "desc number-sort page diverged");
            prop_assert_eq!(a_uniq, uniq(&e, "price"), "unique_terms diverged");
        }
    }

    /// RAM-BOUNDED + REOPEN-NO-REBUILD: seal the WHOLE collection to disk, reopen
    /// from the segments alone (no CBOR snapshot), and assert the reopened Number
    /// field's `values` map is EMPTY while range/exact queries still answer from
    /// the mmap (a binary-search on the sorted-value column, NOT an O(n) forward
    /// scan — the rebuild loop in `open_from_segment` was deleted in 2h-3).
    #[test]
    fn reopen_drives_from_segment_with_empty_values() {
        let dir = tempfile::tempdir().unwrap();
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_num(&e, "a", Some(-3.0), Some("red"));
        index_num(&e, "b", Some(0.0), Some("green"));
        index_num(&e, "c2", Some(3.0), None);
        index_num(&e, "d", Some(0.0), Some("red")); // dup value 0.0
        index_num(&e, "e", None, Some("green")); // present-but-no-price

        let want_range = set_of(&run(&e, rangeq(Some(-3.0), None, None, Some(3.0))));
        let want_exact0 = set_of(&run(&e, termnum(0.0)));
        let want_uniq = uniq(&e, "price");

        e.__seal_collection_to_segments("c", dir.path(), 1).unwrap();
        let schema = e.__collection_schema("c").unwrap();
        let e2 = Engine::__open_collection_from_segments("c", dir.path(), schema, 1).unwrap();

        // The reopened Number `values` map must be EMPTY (no RAM rebuild) ...
        {
            let state = e2.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Number(n) = coll.fields.get("price").unwrap() else {
                panic!("price must be a Number field");
            };
            assert!(n.segment.is_some(), "reopened segment attached");
            assert!(
                n.values.is_empty(),
                "reopen must NOT rebuild `values` in RAM (got {} entries) — the \
                 range read is a binary-search on the mmap sorted-value column, \
                 not an O(N) forward scan that rebuilds `values`",
                n.values.len()
            );
            // The on-disk sorted-value column carries the distinct values, so the
            // segment can answer a range WITHOUT the RAM map.
            let seg = n.segment.as_ref().unwrap();
            assert_eq!(
                seg.number_range_distinct_count(None, None).unwrap(),
                3,
                "distinct values {{-3.0, 0.0, 3.0}} live on the mmap sorted column"
            );
        }

        // ... yet range / exact queries still resolve, entirely from the mmap.
        assert_eq!(
            set_of(&run(&e2, rangeq(Some(-3.0), None, None, Some(3.0)))),
            want_range,
            "range post-reopen"
        );
        assert_eq!(
            set_of(&run(&e2, termnum(0.0))),
            want_exact0,
            "exact post-reopen"
        );
        assert_eq!(uniq(&e2, "price"), want_uniq, "unique_terms post-reopen");
    }

    #[test]
    fn segment_keyword_range_skip_cache_matches_live_after_delete_and_tail() {
        fn build(e: &Engine, n: usize) {
            e.create_collection("c", schema()).unwrap();
            for i in 0..n {
                index_num(
                    e,
                    &format!("d{i}"),
                    Some((i % 100) as f64),
                    Some(if i % 2 == 0 { "red" } else { "blue" }),
                );
            }
        }

        let n = 20_000usize; // red df=10k, high enough to take the dense fast path.
        let query = QueryNode::And(vec![
            termkw("cat", "red"),
            rangeq(Some(10.0), None, None, Some(20.0)),
        ]);

        let oracle = Arc::new(Engine::new());
        build(&oracle, n);
        oracle.delete("c", "d10", None).unwrap();
        index_num(&oracle, "tail", Some(12.0), Some("red"));
        let want = set_of(&run(&oracle, query.clone()));
        let want_total = search_total(&oracle, query.clone());
        let want_sort = run_sorted(&oracle, termkw("cat", "red"), "price", SortOrder::Asc);

        let subject = Arc::new(Engine::new());
        build(&subject, n);
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir.path(), 1)
            .unwrap();
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Keyword(k) = coll.fields.get("cat").unwrap() else {
                panic!("cat");
            };
            let FieldIndex::Number(num) = coll.fields.get("price").unwrap() else {
                panic!("price");
            };
            assert!(k.segment.is_some(), "keyword segment attached");
            assert!(k.terms.is_empty(), "keyword RAM driver dropped after seal");
            assert!(num.segment.is_some(), "number segment attached");
            assert!(
                num.values.is_empty(),
                "number RAM driver dropped after seal"
            );
        }

        subject.delete("c", "d10", None).unwrap();
        index_num(&subject, "tail", Some(12.0), Some("red"));

        let got = set_of(&run(&subject, query.clone()));
        let got_total = search_total(&subject, query);
        let got_sort = run_sorted(&subject, termkw("cat", "red"), "price", SortOrder::Asc);
        assert_eq!(got, want, "segment term+range fast path set diverged");
        assert_eq!(
            got_total, want_total,
            "segment term+range fast path total diverged"
        );
        assert_eq!(
            got_sort, want_sort,
            "segment keyword-filtered number sort page diverged"
        );

        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Number(num) = coll.fields.get("price").unwrap() else {
            panic!("price");
        };
        let cache = num
            .keyword_range_cache
            .read()
            .expect("number keyword-range cache poisoned");
        assert!(
            cache.contains_key("cat\0red"),
            "planner should have built the lazy segment keyword+range skip cache"
        );
    }

    /// LIVE-TAIL UNION: seal, then index more docs (tail into the live `values`),
    /// and assert range/exact compose segment-base (sorted-value column) with the
    /// live-tail `values.range`.
    #[test]
    fn live_tail_unions_with_segment_base() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_num(&e, "base0", Some(1.0), None); // id 0, sealed base
        index_num(&e, "base1", Some(5.0), None); // id 1, sealed base

        let dir = tempfile::tempdir().unwrap();
        let n = e
            .__seal_number_field_to_segment("c", "price", dir.path())
            .unwrap();
        assert_eq!(n, 2);
        assert_values_dropped(&e); // base index now on disk

        // Tail docs after seal → live `values` tail (ids >= n_docs).
        index_num(&e, "tail0", Some(1.0), None); // id 2, tail, SAME value as base0
        index_num(&e, "tail1", Some(9.0), None); // id 3, tail, NEW value

        // range [0, 10]: base {base0=1, base1=5} ∪ tail {tail0=1, tail1=9}.
        let got = set_of(&run(&e, rangeq(Some(0.0), None, Some(10.0), None)));
        let want: BTreeSet<String> = ["base0", "base1", "tail0", "tail1"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(got, want, "range must union segment base + live tail");

        // exact 1.0: base0 (segment) ∪ tail0 (tail).
        let got = set_of(&run(&e, termnum(1.0)));
        let want: BTreeSet<String> = ["base0", "tail0"].iter().map(|s| s.to_string()).collect();
        assert_eq!(got, want, "exact 1.0 must union segment base + live tail");

        // exact 9.0: ONLY the tail (not in the segment).
        let got = set_of(&run(&e, termnum(9.0)));
        let want: BTreeSet<String> = ["tail1".to_string()].into_iter().collect();
        assert_eq!(got, want, "9.0 must come from the live tail alone");

        // exact 5.0: ONLY the segment base.
        let got = set_of(&run(&e, termnum(5.0)));
        let want: BTreeSet<String> = ["base1".to_string()].into_iter().collect();
        assert_eq!(got, want, "5.0 must come from the segment base alone");

        // df composition: value_df(1.0) = 1 (base) + 1 (tail) = 2.
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Number(nidx) = coll.fields.get("price").unwrap() else {
            panic!("price");
        };
        let k1 = SortableF64::new(1.0).unwrap();
        let k9 = SortableF64::new(9.0).unwrap();
        let k5 = SortableF64::new(5.0).unwrap();
        let k7 = SortableF64::new(7.0).unwrap();
        assert_eq!(
            nidx.value_df(k1),
            2,
            "df 1.0 must sum segment base + live tail"
        );
        assert_eq!(nidx.value_df(k5), 1, "df 5.0 segment-only");
        assert_eq!(nidx.value_df(k9), 1, "df 9.0 tail-only");
        assert_eq!(nidx.value_df(k7), 0, "df absent value");
    }

    /// DELETE-AFTER-SEAL **WITHOUT** RE-SEAL: seal a Number corpus, delete several
    /// BASE docs (no re-seal), then assert every segment-ON query equals an in-RAM
    /// ORACLE built from the identical op sequence but NEVER sealed — byte-identical
    /// result SETS for range, exact Term, multi Terms, boolean Or/And, plus
    /// duplicates/unique_terms. The 2h tombstone closes the immutable-posting gap.
    #[test]
    fn delete_after_seal_without_reseal_matches_oracle() {
        fn build(e: &Engine) {
            e.create_collection("c", schema()).unwrap();
            index_num(e, "d0", Some(0.0), Some("red"));
            index_num(e, "d1", Some(0.0), Some("red")); // dup 0.0
            index_num(e, "d2", Some(5.0), Some("green"));
            index_num(e, "d3", Some(0.0), Some("red")); // dup 0.0
            index_num(e, "d4", Some(5.0), Some("green")); // dup 5.0
            index_num(e, "d5", Some(-3.0), None);
        }
        let to_delete = ["d1", "d3", "d4"];

        // --- ORACLE: in-RAM, never sealed. ---
        let oracle = Arc::new(Engine::new());
        build(&oracle);
        for d in to_delete {
            oracle.delete("c", d, None).unwrap();
        }

        // --- SUBJECT: seal price + cat, THEN delete (no re-seal). ---
        let subject = Arc::new(Engine::new());
        build(&subject);
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_number_field_to_segment("c", "price", dir.path())
            .unwrap();
        subject
            .__seal_keyword_field_to_segment("c", "cat", dir.path())
            .unwrap();
        assert_values_dropped(&subject);
        for d in to_delete {
            subject.delete("c", d, None).unwrap();
        }

        // The deleted base ids are tombstoned, NOT removed from any on-disk posting.
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Number(n) = coll.fields.get("price").unwrap() else {
                panic!("price");
            };
            assert!(n.values.is_empty(), "sealed: values still dropped");
            assert_eq!(n.tombstones.len(), 3, "three base deletes tombstoned");
        }

        // Result SETS must match the oracle on every driver surface.
        let q_range = rangeq(Some(-10.0), None, Some(10.0), None);
        let q_exact = termnum(0.0);
        let q_terms = termsnum(&[0.0, 5.0]);
        let q_or = QueryNode::Or(vec![termnum(0.0), termnum(5.0)]);
        let q_and = QueryNode::And(vec![
            rangeq(Some(-1.0), None, Some(1.0), None),
            termkw("cat", "red"),
        ]);
        // Or-of-RANGES forces the MATERIALIZED `eval_range` → `range_postings`
        // path (NOT the `try_plan` standalone shortcut), so the range tombstone
        // subtraction is exercised: each range child is fully materialized and the
        // deleted base docids must be subtracted from the on-disk union.
        let q_or_ranges = QueryNode::Or(vec![
            rangeq(Some(-1.0), None, Some(1.0), None),
            rangeq(Some(4.0), None, Some(6.0), None),
        ]);

        assert_eq!(
            set_of(&run(&subject, q_range.clone())),
            set_of(&run(&oracle, q_range)),
            "range leaked a deleted doc"
        );
        assert_eq!(
            set_of(&run(&subject, q_exact.clone())),
            set_of(&run(&oracle, q_exact)),
            "exact Term leaked a deleted doc"
        );
        assert_eq!(
            set_of(&run(&subject, q_terms.clone())),
            set_of(&run(&oracle, q_terms)),
            "Terms leaked a deleted doc"
        );
        assert_eq!(
            set_of(&run(&subject, q_or.clone())),
            set_of(&run(&oracle, q_or)),
            "Or leaked a deleted doc"
        );
        assert_eq!(
            set_of(&run(&subject, q_and.clone())),
            set_of(&run(&oracle, q_and)),
            "And leaked a deleted doc"
        );
        assert_eq!(
            set_of(&run(&subject, q_or_ranges.clone())),
            set_of(&run(&oracle, q_or_ranges)),
            "Or-of-ranges (eval_range) leaked a deleted doc"
        );
        assert_eq!(
            dup_map(&subject, "price"),
            dup_map(&oracle, "price"),
            "duplicates(price) diverged after delete"
        );
        assert_eq!(
            uniq(&subject, "price"),
            uniq(&oracle, "price"),
            "unique_terms(price) diverged after delete"
        );

        // Concrete spot-check: 0.0 now only d0 survives (d1, d3 deleted).
        let zero = set_of(&run(&subject, termnum(0.0)));
        assert_eq!(
            zero,
            ["d0".to_string()].into_iter().collect::<BTreeSet<_>>(),
            "0.0 must be only the surviving d0"
        );
        // 5.0: only d2 survives (d4 deleted).
        let five = set_of(&run(&subject, termnum(5.0)));
        assert_eq!(
            five,
            ["d2".to_string()].into_iter().collect::<BTreeSet<_>>(),
            "5.0 must be only the surviving d2"
        );
    }

    /// SORT-after-delete: the segment sort-via-sorted-index walk (Phase 2m) must
    /// drop tombstoned base docids per value, so the ORDERED page matches the
    /// in-RAM oracle exactly (asc + desc). Guards the sort walk's tombstone
    /// subtraction — without it the walk emits deleted base docs (proven by
    /// temporarily disabling the subtraction).
    #[test]
    fn sort_after_delete_matches_oracle() {
        fn build(e: &Engine) {
            e.create_collection("c", schema()).unwrap();
            index_num(e, "d0", Some(0.0), Some("red"));
            index_num(e, "d1", Some(0.0), Some("red"));
            index_num(e, "d2", Some(5.0), Some("green"));
            index_num(e, "d3", Some(0.0), Some("red"));
            index_num(e, "d4", Some(5.0), Some("green"));
            index_num(e, "d5", Some(-3.0), None);
        }
        let to_delete = ["d1", "d3", "d4"];
        let oracle = Arc::new(Engine::new());
        build(&oracle);
        for d in to_delete {
            oracle.delete("c", d, None).unwrap();
        }
        let subject = Arc::new(Engine::new());
        build(&subject);
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_number_field_to_segment("c", "price", dir.path())
            .unwrap();
        for d in to_delete {
            subject.delete("c", d, None).unwrap();
        }
        let o_asc = run_sorted(
            &oracle,
            rangeq(None, None, None, None),
            "price",
            SortOrder::Asc,
        );
        let s_asc = run_sorted(
            &subject,
            rangeq(None, None, None, None),
            "price",
            SortOrder::Asc,
        );
        assert_eq!(o_asc, s_asc, "asc sort leaked a deleted doc");
        let o_desc = run_sorted(
            &oracle,
            rangeq(None, None, None, None),
            "price",
            SortOrder::Desc,
        );
        let s_desc = run_sorted(
            &subject,
            rangeq(None, None, None, None),
            "price",
            SortOrder::Desc,
        );
        assert_eq!(o_desc, s_desc, "desc sort leaked a deleted doc");
    }

    /// RE-SEAL (CHECKPOINT) after delete: once re-sealed the deletions are BAKED
    /// into the new segment (2g-A live(id) GC), the tombstone is CLEARED, and
    /// queries still match the oracle.
    #[test]
    fn reseal_bakes_deletes_and_clears_tombstone() {
        fn build(e: &Engine) {
            e.create_collection("c", schema()).unwrap();
            index_num(e, "d0", Some(0.0), Some("red"));
            index_num(e, "d1", Some(0.0), Some("red"));
            index_num(e, "d2", Some(5.0), Some("green"));
            index_num(e, "d3", Some(-3.0), None);
        }
        let to_delete = ["d1", "d2"];

        let oracle = Arc::new(Engine::new());
        build(&oracle);
        for d in to_delete {
            oracle.delete("c", d, None).unwrap();
        }

        let subject = Arc::new(Engine::new());
        build(&subject);
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir.path(), 1)
            .unwrap();
        for d in to_delete {
            subject.delete("c", d, None).unwrap();
        }
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Number(n) = coll.fields.get("price").unwrap() else {
                panic!("price");
            };
            assert_eq!(n.tombstones.len(), 2, "deletes tombstoned before re-seal");
        }
        let dir2 = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir2.path(), 2)
            .unwrap();
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Number(n) = coll.fields.get("price").unwrap() else {
                panic!("price");
            };
            assert!(
                n.tombstones.is_empty(),
                "tombstone must be CLEARED after re-seal"
            );
        }

        let q_range = rangeq(Some(-10.0), None, Some(10.0), None);
        assert_eq!(
            set_of(&run(&subject, q_range.clone())),
            set_of(&run(&oracle, q_range)),
            "post-re-seal range diverged"
        );
        // value 5.0 fully deleted (d2 gone) → empty.
        assert_eq!(
            set_of(&run(&subject, termnum(5.0))),
            BTreeSet::new(),
            "5.0 fully deleted — empty after re-seal"
        );
        assert_eq!(
            uniq(&subject, "price"),
            uniq(&oracle, "price"),
            "unique_terms diverged after re-seal"
        );
    }

    /// TEETH: a wrong binary-search bound (inclusivity flip) MUST diverge from the
    /// in-RAM oracle. This pins that `number_range_window` honors INCLUSIVE vs
    /// EXCLUSIVE exactly: a value sitting exactly on a bound is the difference
    /// between the two, so flipping inclusivity changes the result SET.
    #[test]
    fn teeth_inclusivity_flip_changes_result() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_num(&e, "lo", Some(0.0), None);
        index_num(&e, "mid", Some(5.0), None);
        index_num(&e, "hi", Some(10.0), None);
        let dir = tempfile::tempdir().unwrap();
        e.__seal_number_field_to_segment("c", "price", dir.path())
            .unwrap();
        assert_values_dropped(&e);

        // [0, 10] inclusive → {lo, mid, hi}; (0, 10) exclusive → {mid}. If the
        // window math ignored inclusivity these would be equal — they are NOT, so
        // this is the teeth assertion the spec asks for.
        let incl = set_of(&run(&e, rangeq(Some(0.0), None, Some(10.0), None)));
        let excl = set_of(&run(&e, rangeq(None, Some(0.0), None, Some(10.0))));
        assert_eq!(
            incl.len(),
            3,
            "[0,10] inclusive must include both endpoints"
        );
        assert_eq!(
            excl,
            ["mid".to_string()].into_iter().collect::<BTreeSet<_>>(),
            "(0,10) exclusive must drop both endpoints"
        );
        assert_ne!(
            incl, excl,
            "inclusive and exclusive bounds MUST differ on boundary values"
        );

        // Direct reader-level teeth: an off-by-one in `number_range_window` would
        // make Included(5.0)..=Included(5.0) miss the exact value. Pin it.
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Number(n) = coll.fields.get("price").unwrap() else {
            panic!("price")
        };
        let seg = n.segment.as_ref().unwrap();
        let b5 = SortableF64::new(5.0).unwrap().bits();
        let r = seg
            .number_range(Some((b5, true)), Some((b5, true)))
            .unwrap();
        assert_eq!(
            r.len(),
            1,
            "[5,5] inclusive must select exactly the one 5.0 doc"
        );
        let r_excl = seg
            .number_range(Some((b5, false)), Some((b5, false)))
            .unwrap();
        assert_eq!(r_excl.len(), 0, "(5,5) exclusive must be empty");
    }
}

// ---------------------------------------------------------------------------
// Dual-path diff test: segment-backed Set membership read must be
// byte-identical to the live in-RAM read (Stage 2 Phase 2e-A).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod segment_set_diff_tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    fn fieldspec(t: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    /// `tags` (Set, the field we seal) + `body` (Text, the AND driver).
    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("tags".into(), fieldspec(FieldType::Set, None));
        fields.insert(
            "body".into(),
            fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
        );
        CreateCollectionRequest { fields }
    }

    fn req(query: QueryNode) -> SearchRequest {
        SearchRequest {
            query,
            limit: 100_000,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    fn run(e: &Engine, query: QueryNode) -> Vec<(String, f32)> {
        e.search("c", req(query))
            .unwrap()
            .hits
            .into_iter()
            .map(|h| (h.external_id, h.score))
            .collect()
    }

    fn set_of(rows: &[(String, f32)]) -> BTreeSet<String> {
        rows.iter().map(|(e, _)| e.clone()).collect()
    }

    fn scores_of(rows: &[(String, f32)]) -> BTreeMap<String, u32> {
        rows.iter().map(|(e, s)| (e.clone(), s.to_bits())).collect()
    }

    /// Index one doc: always writes `body`; writes `tags` only when `tags` is
    /// `Some` (so absent-set docs are part of the corpus). A present-but-empty
    /// set is `Some(&[])`.
    fn index_doc(e: &Engine, eid: &str, tags: Option<&[&str]>, tok: bool) {
        let mut items = vec![crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "body".into(),
            value: FieldValue::String(if tok {
                "tok filler".into()
            } else {
                "filler".into()
            }),
            version: None,
        }];
        if let Some(ts) = tags {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "tags".into(),
                value: FieldValue::StringList(ts.iter().map(|s| (*s).to_string()).collect()),
                version: None,
            });
        }
        e.index(
            "c",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }

    /// A match-DRIVEN AND so the `term tags` membership conjunct is applied as a
    /// per-doc PREDICATE (`clause_matches` → `SetIndex::set_contains`).
    fn term_conjunct(el: &str) -> QueryNode {
        QueryNode::And(vec![
            QueryNode::Match(MatchQuery {
                field: "body".into(),
                text: "tok".into(),
                op: MatchOp::And,
            }),
            QueryNode::Term(TermQuery {
                field: "tags".into(),
                value: FieldValue::String(el.into()),
            }),
        ])
    }

    /// A match-DRIVEN AND with a `terms tags` (OR-of-members) conjunct —
    /// exercises `SetIndex::set_contains_any`.
    fn terms_conjunct(els: &[&str]) -> QueryNode {
        QueryNode::And(vec![
            QueryNode::Match(MatchQuery {
                field: "body".into(),
                text: "tok".into(),
                op: MatchOp::And,
            }),
            QueryNode::Terms(TermsQuery {
                field: "tags".into(),
                values: els
                    .iter()
                    .map(|s| FieldValue::String((*s).into()))
                    .collect(),
            }),
        ])
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// PATH A (segment OFF, live in-RAM `forward`) must equal PATH B (Set
        /// field sealed to a shared DICT + CSR offsets + packed segment, then
        /// served from it) — same result SET and byte-identical scores — for
        /// both the `term tags` and `terms tags` membership conjuncts over a
        /// randomized multi-valued corpus (varied N, cardinality incl 0,
        /// absent-`tags` docs).
        #[test]
        fn segment_read_matches_live_read(
            docs in proptest::collection::vec(
                (
                    // tags: ~1-in-5 docs have NO set value; others get 0..4
                    // members drawn from a small pool (multi-valued, deduped).
                    proptest::option::weighted(
                        0.8,
                        proptest::collection::vec(
                            prop::sample::select(vec!["x", "y", "z", "w", "v"]),
                            0..4,
                        ),
                    ),
                    any::<bool>(),
                ),
                1..60,
            ),
        ) {
            // --- PATH A: build the live engine, run both shapes (segment OFF). ---
            let e = Arc::new(Engine::new());
            e.create_collection("c", schema()).unwrap();
            for (i, (tags, tok)) in docs.iter().enumerate() {
                let slice: Option<Vec<&str>> =
                    tags.as_ref().map(|v| v.iter().map(|s| *s).collect());
                index_doc(&e, &format!("d{i}"), slice.as_deref(), *tok);
            }

            let a_term = run(&e, term_conjunct("x"));
            let a_terms = run(&e, terms_conjunct(&["y", "z"]));

            // --- PATH B: seal `tags` to a segment, flip it ON, rerun. ---
            let dir = tempfile::tempdir().unwrap();
            let sealed = e.__seal_set_field_to_segment("c", "tags", dir.path()).unwrap();
            prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");

            let b_term = run(&e, term_conjunct("x"));
            let b_terms = run(&e, terms_conjunct(&["y", "z"]));

            prop_assert_eq!(set_of(&a_term), set_of(&b_term), "term conjunct set diverged");
            prop_assert_eq!(set_of(&a_terms), set_of(&b_terms), "terms conjunct set diverged");
            prop_assert_eq!(scores_of(&a_term), scores_of(&b_term), "term scores diverged");
            prop_assert_eq!(scores_of(&a_terms), scores_of(&b_terms), "terms scores diverged");
        }
    }

    /// A doc indexed AFTER sealing (docid >= segment n_docs) lives in the live
    /// `forward` tail and must still match through `set_contains`'s fallback.
    #[test]
    fn doc_indexed_after_sealing_served_from_live_tail() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_doc(&e, "sealed", Some(&["x", "y"]), true); // id 0
        index_doc(&e, "empty", Some(&[]), true); // id 1, present-but-empty

        let dir = tempfile::tempdir().unwrap();
        let n = e
            .__seal_set_field_to_segment("c", "tags", dir.path())
            .unwrap();
        assert_eq!(n, 2, "two docs sealed");

        // New doc after sealing → docid 2 (>= n_docs) → lives in the live tail.
        index_doc(&e, "tail", Some(&["z"]), true);

        // term z: only the tail doc qualifies (NOT in the segment) → proves the
        // set_contains id >= n_docs fallback.
        let got = set_of(&run(&e, term_conjunct("z")));
        let want: BTreeSet<String> = ["tail".to_string()].into_iter().collect();
        assert_eq!(got, want, "tail doc must match via live fallback");

        // term x: only the sealed doc qualifies → served from the segment.
        let got = set_of(&run(&e, term_conjunct("x")));
        let want: BTreeSet<String> = ["sealed".to_string()].into_iter().collect();
        assert_eq!(got, want, "sealed doc must match via segment read");
    }

    /// Direct planner-free check that `SetIndex::set_contains` reads CSR-packed
    /// members from the segment (incl multi-valued + present-empty + absent
    /// docs) and falls back to the live tail past `n_docs`.
    #[test]
    fn set_contains_segment_then_live_split() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_doc(&e, "a", Some(&["x", "y"]), false); // id 0, multi
        index_doc(&e, "b", None, false); // id 1, absent
        index_doc(&e, "c", Some(&[]), false); // id 2, present-empty

        let dir = tempfile::tempdir().unwrap();
        e.__seal_set_field_to_segment("c", "tags", dir.path())
            .unwrap();
        index_doc(&e, "d", Some(&["z"]), false); // id 3, live tail

        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
            panic!("tags must be a Set field");
        };
        assert!(s.segment.is_some(), "segment attached");
        // doc 0: {x, y} from the segment (CSR slice of 2 members).
        assert!(s.set_contains(0, "x"));
        assert!(s.set_contains(0, "y"));
        assert!(!s.set_contains(0, "z"));
        // doc 1: absent → no membership.
        assert!(!s.set_contains(1, "x"));
        // doc 2: present-but-empty → no membership but is present.
        assert!(!s.set_contains(2, "x"));
        // doc 3: {z} from the live tail (id >= n_docs).
        assert!(s.set_contains(3, "z"));
        assert!(!s.set_contains(3, "x"));
        // unknown id.
        assert!(!s.set_contains(99, "x"));
    }
}

// ---------------------------------------------------------------------------
// Dual-path diff test for the INVERTED Set driver (Stage 2 Phase 2h-2): the
// segment-driven membership / Terms / boolean RoaringBitmap algebra
// (`element_postings` / `element_df`) must be byte-identical to the in-RAM
// `elements` index, AND after a seal the RAM index is DROPPED (the disk=all
// win) while queries keep serving entirely from the mmap segment. This is the
// Set analogue of `segment_keyword_inverted_diff_tests` and the keystone test
// for Phase 2h-2 — it reuses the same query-time tombstone mechanism.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod segment_set_inverted_diff_tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    fn fieldspec(t: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    /// `tags` (Set, sealed) + `cat` (a second Set, for boolean AND/OR
    /// cross-field algebra). Both are inverted-driver fields.
    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("tags".into(), fieldspec(FieldType::Set, None));
        fields.insert("cat".into(), fieldspec(FieldType::Set, None));
        CreateCollectionRequest { fields }
    }

    fn req(query: QueryNode) -> SearchRequest {
        SearchRequest {
            query,
            limit: 100_000,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    fn run(e: &Engine, query: QueryNode) -> Vec<(String, f32)> {
        e.search("c", req(query))
            .unwrap()
            .hits
            .into_iter()
            .map(|h| (h.external_id, h.score))
            .collect()
    }

    fn set_of(rows: &[(String, f32)]) -> BTreeSet<String> {
        rows.iter().map(|(e, _)| e.clone()).collect()
    }

    fn search_total(e: &Engine, query: QueryNode) -> u64 {
        e.search("c", req(query)).unwrap().total
    }

    /// Index one doc: writes `tags` (multi-valued) when `Some`, `cat` when
    /// `Some`. A doc with neither still interns via a filler `tags` member so it
    /// is part of the corpus (mirrors the keyword module's filler).
    fn index_set(e: &Engine, eid: &str, tags: Option<&[&str]>, cat: Option<&[&str]>) {
        let mut items = Vec::new();
        if let Some(ts) = tags {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "tags".into(),
                value: FieldValue::StringList(ts.iter().map(|s| (*s).to_string()).collect()),
                version: None,
            });
        }
        if let Some(cs) = cat {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "cat".into(),
                value: FieldValue::StringList(cs.iter().map(|s| (*s).to_string()).collect()),
                version: None,
            });
        }
        if items.is_empty() {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "tags".into(),
                value: FieldValue::StringList(vec!["zzz_filler".into()]),
                version: None,
            });
        }
        e.index(
            "c",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }

    fn term(field: &str, v: &str) -> QueryNode {
        QueryNode::Term(TermQuery {
            field: field.into(),
            value: FieldValue::String(v.into()),
        })
    }

    fn terms(field: &str, vs: &[&str]) -> QueryNode {
        QueryNode::Terms(TermsQuery {
            field: field.into(),
            values: vs.iter().map(|s| FieldValue::String((*s).into())).collect(),
        })
    }

    /// Assert the Set field `tags` has an EMPTY in-RAM `elements` index but an
    /// attached segment — the RAM-bounded invariant after a seal/reopen.
    fn assert_elements_dropped(e: &Engine) {
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
            panic!("tags must be a Set field");
        };
        assert!(s.segment.is_some(), "segment must be attached");
        assert!(
            s.elements.is_empty(),
            "RAM `elements` index must be DROPPED after seal (got {} entries)",
            s.elements.len()
        );
        assert!(
            s.forward.is_empty(),
            "RAM `forward` must be dropped after seal"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// PATH A (segment OFF, in-RAM `elements`) must equal PATH B (sealed: RAM
        /// `elements` DROPPED, driven from the mmap posting column) for the whole
        /// inverted-driver surface: standalone `Term`, standalone `Terms`,
        /// boolean `Or` of two members, boolean `And` across two set fields, and
        /// the `try_plan` standalone-Term total. Byte-identical result sets AND
        /// identical totals on both paths.
        #[test]
        fn inverted_segment_matches_live(
            docs in proptest::collection::vec(
                (
                    // tags: ~1-in-5 docs absent; others 0..4 deduped members.
                    proptest::option::weighted(
                        0.8,
                        proptest::collection::vec(
                            prop::sample::select(vec!["alpha", "beta", "gamma", "delta"]),
                            0..4,
                        ),
                    ),
                    // cat: ~1-in-3 docs absent; others 0..3 members.
                    proptest::option::weighted(
                        0.7,
                        proptest::collection::vec(
                            prop::sample::select(vec!["red", "green"]),
                            0..3,
                        ),
                    ),
                ),
                1..70,
            ),
        ) {
            // --- PATH A: in-RAM inverted index (segment OFF). ---
            let e = Arc::new(Engine::new());
            e.create_collection("c", schema()).unwrap();
            for (i, (tags, cat)) in docs.iter().enumerate() {
                let ts: Option<Vec<&str>> = tags.as_ref().map(|v| v.iter().map(|s| *s).collect());
                let cs: Option<Vec<&str>> = cat.as_ref().map(|v| v.iter().map(|s| *s).collect());
                index_set(&e, &format!("d{i}"), ts.as_deref(), cs.as_deref());
            }

            let a_term = run(&e, term("tags", "alpha"));
            let a_terms = run(&e, terms("tags", &["beta", "gamma"]));
            let a_or = run(&e, QueryNode::Or(vec![term("tags", "alpha"), term("tags", "delta")]));
            let a_and = run(&e, QueryNode::And(vec![term("tags", "beta"), term("cat", "red")]));
            let a_total = search_total(&e, term("tags", "gamma"));

            // --- PATH B: seal `tags` (drops RAM `elements`), rerun from the mmap. ---
            let dir = tempfile::tempdir().unwrap();
            let sealed = e.__seal_set_field_to_segment("c", "tags", dir.path()).unwrap();
            prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");
            assert_elements_dropped(&e); // RAM-bounded: elements index gone

            let b_term = run(&e, term("tags", "alpha"));
            let b_terms = run(&e, terms("tags", &["beta", "gamma"]));
            let b_or = run(&e, QueryNode::Or(vec![term("tags", "alpha"), term("tags", "delta")]));
            let b_and = run(&e, QueryNode::And(vec![term("tags", "beta"), term("cat", "red")]));
            let b_total = search_total(&e, term("tags", "gamma"));

            prop_assert_eq!(set_of(&a_term), set_of(&b_term), "Term set diverged");
            prop_assert_eq!(set_of(&a_terms), set_of(&b_terms), "Terms set diverged");
            prop_assert_eq!(set_of(&a_or), set_of(&b_or), "Or set diverged");
            prop_assert_eq!(set_of(&a_and), set_of(&b_and), "And set diverged");
            prop_assert_eq!(a_total, b_total, "standalone-term total diverged");
        }
    }

    /// RAM-BOUNDED + REOPEN-NO-REBUILD: seal the WHOLE collection to disk, reopen
    /// from the segments alone (no CBOR snapshot), and assert the reopened Set
    /// field's `elements` map is EMPTY while membership/Terms queries still return
    /// correct results entirely from the mmap segment.
    #[test]
    fn reopen_drives_from_segment_with_empty_elements() {
        let dir = tempfile::tempdir().unwrap();
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_set(&e, "a", Some(&["alpha", "beta"]), Some(&["red"]));
        index_set(&e, "b", Some(&["beta"]), Some(&["green"]));
        index_set(&e, "c2", Some(&["gamma"]), None);
        index_set(&e, "d", Some(&["alpha"]), Some(&["red"]));
        index_set(&e, "e", None, Some(&["green"])); // present-but-no-tags

        let want_alpha = set_of(&run(&e, term("tags", "alpha")));
        let want_beta_gamma = set_of(&run(&e, terms("tags", &["beta", "gamma"])));

        e.__seal_collection_to_segments("c", dir.path(), 1).unwrap();
        let schema = e.__collection_schema("c").unwrap();
        let e2 = Engine::__open_collection_from_segments("c", dir.path(), schema, 1).unwrap();

        // The reopened Set `elements` map must be EMPTY (no RAM rebuild) ...
        {
            let state = e2.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
                panic!("tags must be a Set field");
            };
            assert!(s.segment.is_some(), "reopened segment attached");
            assert!(
                s.elements.is_empty(),
                "reopen must NOT rebuild `elements` in RAM (got {} entries)",
                s.elements.len()
            );
        }

        // ... yet the inverted queries still resolve, entirely from the mmap.
        assert_eq!(
            set_of(&run(&e2, term("tags", "alpha"))),
            want_alpha,
            "Term post-reopen"
        );
        assert_eq!(
            set_of(&run(&e2, terms("tags", &["beta", "gamma"]))),
            want_beta_gamma,
            "Terms post-reopen"
        );
    }

    /// LIVE-TAIL UNION: seal, then index more docs (tail into the live
    /// `elements`), and assert a Term query returns base (segment) + tail (RAM)
    /// composed, plus the `element_df` sum.
    #[test]
    fn live_tail_unions_with_segment_base() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_set(&e, "base0", Some(&["alpha"]), None); // id 0, sealed base
        index_set(&e, "base1", Some(&["beta"]), None); // id 1, sealed base

        let dir = tempfile::tempdir().unwrap();
        let n = e
            .__seal_set_field_to_segment("c", "tags", dir.path())
            .unwrap();
        assert_eq!(n, 2);
        assert_elements_dropped(&e); // base index now on disk

        // Tail docs after seal → land in the live `elements` tail (ids >= n_docs).
        index_set(&e, "tail0", Some(&["alpha"]), None); // id 2, SAME element as base0
        index_set(&e, "tail1", Some(&["gamma"]), None); // id 3, NEW element

        // element alpha: base id (base0) UNION tail id (tail0).
        let got = set_of(&run(&e, term("tags", "alpha")));
        let want: BTreeSet<String> = ["base0".into(), "tail0".into()].into_iter().collect();
        assert_eq!(got, want, "alpha must union segment base + live tail");

        // element gamma: ONLY the tail (not in the segment).
        let got = set_of(&run(&e, term("tags", "gamma")));
        let want: BTreeSet<String> = ["tail1".into()].into_iter().collect();
        assert_eq!(got, want, "gamma must come from the live tail alone");

        // element beta: ONLY the segment base.
        let got = set_of(&run(&e, term("tags", "beta")));
        let want: BTreeSet<String> = ["base1".into()].into_iter().collect();
        assert_eq!(got, want, "beta must come from the segment base alone");

        // df composition: element_df(alpha) = 1 (base) + 1 (tail) = 2.
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
            panic!("tags must be a Set field");
        };
        assert_eq!(
            s.element_df("alpha"),
            2,
            "df must sum segment base + live tail"
        );
        assert_eq!(s.element_df("beta"), 1, "df beta segment-only");
        assert_eq!(s.element_df("gamma"), 1, "df gamma tail-only");
        assert_eq!(s.element_df("missing"), 0, "df absent element");
    }

    // -----------------------------------------------------------------------
    // Phase 2h-2: delete-after-seal query-time tombstone (REUSED from 2h-1).
    // After the seal drops the in-RAM `elements` index, `drop_eid` on a SEALED
    // base docid is a NO-OP on the immutable on-disk posting, so segment-driven
    // Set queries would LEAK the deleted doc until the next re-seal. The fix
    // records sealed-base deletes in the per-field `tombstones` RoaringBitmap
    // (the same field shape as the Keyword index) that every segment-ON accessor
    // subtracts. These tests pin the fix and prove its teeth.
    // -----------------------------------------------------------------------

    /// (value → external_ids) of every duplicate group, as a comparable map.
    fn dup_map(e: &Engine, field: &str) -> BTreeMap<String, BTreeSet<String>> {
        e.duplicates(
            "c",
            crate::shared_kernel::types::search::DuplicatesRequest {
                field: field.into(),
                min_group_size: 2,
                limit: 100_000,
                offset: 0,
            },
        )
        .unwrap()
        .groups
        .into_iter()
        .map(|g| {
            let v = g.value.as_str().unwrap().to_string();
            (v, g.external_ids.into_iter().collect::<BTreeSet<String>>())
        })
        .collect()
    }

    /// `unique_terms` of `field` via the public stats surface.
    fn uniq(e: &Engine, field: &str) -> u64 {
        e.stats("c")
            .unwrap()
            .fields
            .get(field)
            .unwrap()
            .unique_terms
    }

    /// DELETE-AFTER-SEAL **WITHOUT** RE-SEAL: seal a Set corpus, delete several
    /// BASE docs (no re-seal), then assert every segment-ON query equals an
    /// in-RAM ORACLE built from the identical op sequence but NEVER sealed —
    /// byte-identical result SETS for standalone Term, multi-member Terms,
    /// boolean Or, cross-field And, plus identical `duplicates`/`unique_terms`.
    /// This is the window the seal-time GC does NOT cover; the tombstone closes it.
    #[test]
    fn delete_after_seal_without_reseal_matches_oracle() {
        // Shared corpus builder so the oracle and the sealed engine see the SAME
        // index sequence. Multi-valued: alpha in 3 docs, beta in 2, red in 3.
        fn build(e: &Engine) {
            e.create_collection("c", schema()).unwrap();
            index_set(e, "d0", Some(&["alpha", "beta"]), Some(&["red"]));
            index_set(e, "d1", Some(&["alpha"]), Some(&["red"]));
            index_set(e, "d2", Some(&["beta"]), Some(&["green"]));
            index_set(e, "d3", Some(&["alpha"]), Some(&["red"]));
            index_set(e, "d4", Some(&["beta", "gamma"]), Some(&["green"]));
            index_set(e, "d5", Some(&["gamma"]), None);
        }
        let to_delete = ["d1", "d3", "d4"];

        // --- ORACLE: in-RAM, never sealed. Same build + same deletes. ---
        let oracle = Arc::new(Engine::new());
        build(&oracle);
        for d in to_delete {
            oracle.delete("c", d, None).unwrap();
        }

        // --- SUBJECT: seal tags + cat to segments, THEN delete (no re-seal). ---
        let subject = Arc::new(Engine::new());
        build(&subject);
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_set_field_to_segment("c", "tags", dir.path())
            .unwrap();
        subject
            .__seal_set_field_to_segment("c", "cat", dir.path())
            .unwrap();
        assert_elements_dropped(&subject); // RAM `elements` gone — drives from mmap
        for d in to_delete {
            subject.delete("c", d, None).unwrap();
        }

        // The deleted ids are now tombstoned (base ids < n_docs), NOT removed
        // from any on-disk posting. Confirm the bitmap actually recorded them.
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
                panic!("tags");
            };
            assert!(s.elements.is_empty(), "sealed: elements still dropped");
            assert_eq!(s.tombstones.len(), 3, "three base deletes tombstoned");
        }

        // Result SETS must match the oracle on every driver surface.
        let q_term = term("tags", "alpha");
        let q_terms = terms("tags", &["alpha", "beta"]);
        let q_or = QueryNode::Or(vec![term("tags", "alpha"), term("tags", "beta")]);
        let q_and = QueryNode::And(vec![term("tags", "alpha"), term("cat", "red")]);

        assert_eq!(
            set_of(&run(&subject, q_term.clone())),
            set_of(&run(&oracle, q_term)),
            "standalone Term leaked a deleted doc"
        );
        assert_eq!(
            set_of(&run(&subject, q_terms.clone())),
            set_of(&run(&oracle, q_terms)),
            "multi-member Terms leaked a deleted doc"
        );
        assert_eq!(
            set_of(&run(&subject, q_or.clone())),
            set_of(&run(&oracle, q_or)),
            "boolean Or leaked a deleted doc"
        );
        assert_eq!(
            set_of(&run(&subject, q_and.clone())),
            set_of(&run(&oracle, q_and)),
            "cross-field And leaked a deleted doc"
        );

        // duplicates + unique_terms must match the oracle on sealed data.
        assert_eq!(
            dup_map(&subject, "tags"),
            dup_map(&oracle, "tags"),
            "find_duplicates(tags) diverged on sealed-after-delete"
        );
        assert_eq!(
            dup_map(&subject, "cat"),
            dup_map(&oracle, "cat"),
            "find_duplicates(cat) diverged on sealed-after-delete"
        );
        assert_eq!(
            uniq(&subject, "tags"),
            uniq(&oracle, "tags"),
            "unique_terms(tags) diverged on sealed-after-delete"
        );
        assert_eq!(
            uniq(&subject, "cat"),
            uniq(&oracle, "cat"),
            "unique_terms(cat) diverged on sealed-after-delete"
        );

        // Concrete spot-checks: deleted docs GONE; a fully-deleted element yields
        // None (empty result), not a leak. After deleting d1,d3,d4:
        // alpha={d0}, beta={d2}, gamma={d5}.
        let alpha = set_of(&run(&subject, term("tags", "alpha")));
        assert_eq!(
            alpha,
            ["d0".to_string()].into_iter().collect::<BTreeSet<_>>(),
            "alpha must be only the surviving d0"
        );
        assert!(
            run(&subject, term("tags", "beta"))
                .iter()
                .all(|(eid, _)| eid != "d4"),
            "deleted d4 must not appear under beta"
        );
        // gamma was on d4 (deleted) and d5 (alive) → only d5 survives.
        let gamma = set_of(&run(&subject, term("tags", "gamma")));
        assert_eq!(
            gamma,
            ["d5".to_string()].into_iter().collect::<BTreeSet<_>>(),
            "gamma must drop deleted d4, keep d5"
        );
    }

    /// RE-SEAL (CHECKPOINT) after delete: once the field is re-sealed the
    /// deletions are BAKED into the new segment (via the live(id) GC), the
    /// tombstone is CLEARED, and queries still match the oracle.
    #[test]
    fn reseal_bakes_deletes_and_clears_tombstone() {
        fn build(e: &Engine) {
            e.create_collection("c", schema()).unwrap();
            // beta lives ONLY on d2 (deleted) so it is fully removed after re-seal;
            // alpha lives on d0 (kept) and d1 (deleted).
            index_set(e, "d0", Some(&["alpha"]), Some(&["red"]));
            index_set(e, "d1", Some(&["alpha"]), Some(&["red"]));
            index_set(e, "d2", Some(&["beta"]), Some(&["green"]));
            index_set(e, "d3", Some(&["gamma"]), None);
        }
        let to_delete = ["d1", "d2"];

        let oracle = Arc::new(Engine::new());
        build(&oracle);
        for d in to_delete {
            oracle.delete("c", d, None).unwrap();
        }

        let subject = Arc::new(Engine::new());
        build(&subject);
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir.path(), 1)
            .unwrap();
        for d in to_delete {
            subject.delete("c", d, None).unwrap();
        }
        // Tombstone holds the two base deletes pre-re-seal.
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
                panic!("tags");
            };
            assert_eq!(s.tombstones.len(), 2, "deletes tombstoned before re-seal");
        }
        // RE-SEAL: the live(id) gather excludes the tombstoned ids, so the NEW
        // segment has them absent; the tombstone is reset to empty.
        let dir2 = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir2.path(), 2)
            .unwrap();
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
                panic!("tags");
            };
            assert!(
                s.tombstones.is_empty(),
                "tombstone must be CLEARED after re-seal (deletes baked in)"
            );
        }

        // Post-re-seal queries match the oracle, now with an EMPTY tombstone.
        let q_term = term("tags", "alpha");
        assert_eq!(
            set_of(&run(&subject, q_term.clone())),
            set_of(&run(&oracle, q_term)),
            "post-re-seal Term diverged from oracle"
        );
        assert_eq!(
            set_of(&run(&subject, term("tags", "beta"))),
            BTreeSet::new(),
            "beta fully deleted — must be empty after re-seal"
        );
        assert_eq!(
            uniq(&subject, "tags"),
            uniq(&oracle, "tags"),
            "unique_terms diverged after re-seal"
        );
    }
}

// ---------------------------------------------------------------------------
// Dual-path diff test: the segment-backed BM25 scan (stored postings + DocLen
// column + header scalars) must be byte-identical to the live in-RAM scan
// (Stage 2 Phase 2e-B). Text tf is NOT rebuildable, so the postings are STORED.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod segment_text_diff_tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    fn fieldspec(t: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    /// `body` (Text, the field we seal) + `price` (Number, a filter for the
    /// filtered_search shape).
    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert(
            "body".into(),
            fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
        );
        fields.insert("price".into(), fieldspec(FieldType::Number, None));
        CreateCollectionRequest { fields }
    }

    fn req(query: QueryNode) -> SearchRequest {
        SearchRequest {
            query,
            limit: 100_000,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    fn run(e: &Engine, query: QueryNode) -> Vec<(String, f32)> {
        e.search("c", req(query))
            .unwrap()
            .hits
            .into_iter()
            .map(|h| (h.external_id, h.score))
            .collect()
    }

    fn set_of(rows: &[(String, f32)]) -> BTreeSet<String> {
        rows.iter().map(|(e, _)| e.clone()).collect()
    }

    /// Key the f32 score BITS by external id, so the dual-path compare proves
    /// byte-identical scores (not just approximate equality).
    fn scores_of(rows: &[(String, f32)]) -> BTreeMap<String, u32> {
        rows.iter().map(|(e, s)| (e.clone(), s.to_bits())).collect()
    }

    /// `unique_terms` for a field via the stats surface (Phase 2h-4 `unique_terms`).
    fn uniq(e: &Engine, field: &str) -> u64 {
        e.stats("c")
            .unwrap()
            .fields
            .get(field)
            .unwrap()
            .unique_terms
    }

    /// Index one doc's `body` text and (optionally) a `price` filter value. The
    /// body is a tf-realistic bag: each chosen token is repeated `tf` times so
    /// the stored term-frequencies vary across the corpus.
    fn index_doc(e: &Engine, eid: &str, body: &str, price: Option<f64>) {
        let mut items = vec![crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "body".into(),
            value: FieldValue::String(body.into()),
            version: None,
        }];
        if let Some(p) = price {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "price".into(),
                value: FieldValue::Number(p),
                version: None,
            });
        }
        e.index(
            "c",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }

    /// Build a tf-realistic body string from `(token, repeat)` pairs.
    fn body_from(parts: &[(&str, u32)]) -> String {
        let mut out: Vec<&str> = Vec::new();
        for (tok, rep) in parts {
            for _ in 0..*rep {
                out.push(tok);
            }
        }
        out.join(" ")
    }

    /// text_bm25: a single-token OR match — the pure BM25 scan over one token's
    /// posting list (the `score_token` hot loop).
    fn bm25_single(tok: &str) -> QueryNode {
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: tok.into(),
            op: MatchOp::Or,
        })
    }

    /// text_and: a 2-token AND match — the intersect-and-sum path (drives from
    /// the rarer token, probes the other by binary-search).
    fn text_and(a: &str, b: &str) -> QueryNode {
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: format!("{a} {b}"),
            op: MatchOp::And,
        })
    }

    /// filtered_search: a `match` AND a `price` range filter. The match BM25 is
    /// scored over the candidate set; both the bitmap-driven and match-driven
    /// AND plans route through `eval_match` / `match_doc_score`, so the sealed
    /// postings/doc-len feed the scoring on either plan.
    fn filtered(tok: &str, lo: f64, hi: f64) -> QueryNode {
        QueryNode::And(vec![
            QueryNode::Match(MatchQuery {
                field: "body".into(),
                text: tok.into(),
                op: MatchOp::Or,
            }),
            QueryNode::Range(RangeQuery {
                field: "price".into(),
                gte: Some(RangeBound::Number(lo)),
                lte: Some(RangeBound::Number(hi)),
                gt: None,
                lt: None,
            }),
        ])
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// PATH A (segment OFF, live in-RAM postings) must equal PATH B (Text
        /// field sealed to a token DICT + STORED posting blocks + DocLen column,
        /// then served entirely from it) — same result SET and BYTE-IDENTICAL
        /// f32 scores — across text_bm25 (single token), text_and (2-token AND),
        /// and filtered_search (match + range) over a tf-realistic corpus with
        /// varied token frequencies and document lengths.
        #[test]
        fn segment_bm25_matches_live_scan(
            docs in proptest::collection::vec(
                (
                    // tf of "alpha" (0 == token absent), 0..=4.
                    0u32..5,
                    // tf of "beta", 0..=4.
                    0u32..5,
                    // tf of "gamma" (filler that varies doc_len), 0..=3.
                    0u32..4,
                    // price (always present so the range filter is meaningful).
                    0u32..100,
                ),
                1..50,
            ),
        ) {
            // --- PATH A: build the live engine, run all shapes (segment OFF). ---
            let e = Arc::new(Engine::new());
            e.create_collection("c", schema()).unwrap();
            for (i, (a, b, g, price)) in docs.iter().enumerate() {
                // Every doc has SOME body token so doc_count tracks all docs;
                // a doc with all-zero tfs still posts a single "filler" token.
                let mut parts: Vec<(&str, u32)> = Vec::new();
                if *a > 0 { parts.push(("alpha", *a)); }
                if *b > 0 { parts.push(("beta", *b)); }
                if *g > 0 { parts.push(("gamma", *g)); }
                if parts.is_empty() { parts.push(("filler", 1)); }
                index_doc(&e, &format!("d{i}"), &body_from(&parts), Some(*price as f64));
            }

            let a_single = run(&e, bm25_single("alpha"));
            let a_and = run(&e, text_and("alpha", "beta"));
            let a_filt = run(&e, filtered("alpha", 10.0, 80.0));

            // --- PATH B: seal `body` to a segment, flip it ON, rerun. ---
            let dir = tempfile::tempdir().unwrap();
            let sealed = e.__seal_text_field_to_segment("c", "body", dir.path()).unwrap();
            prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");

            let b_single = run(&e, bm25_single("alpha"));
            let b_and = run(&e, text_and("alpha", "beta"));
            let b_filt = run(&e, filtered("alpha", 10.0, 80.0));

            prop_assert_eq!(set_of(&a_single), set_of(&b_single), "single-token set diverged");
            prop_assert_eq!(set_of(&a_and), set_of(&b_and), "2-token AND set diverged");
            prop_assert_eq!(set_of(&a_filt), set_of(&b_filt), "filtered set diverged");
            prop_assert_eq!(scores_of(&a_single), scores_of(&b_single), "single-token scores diverged");
            prop_assert_eq!(scores_of(&a_and), scores_of(&b_and), "2-token AND scores diverged");
            prop_assert_eq!(scores_of(&a_filt), scores_of(&b_filt), "filtered scores diverged");
        }
    }

    /// Direct planner-free check that the sealed `TextIndex` reads postings /
    /// doc-len / df / corpus scalars from the segment, bit-identical to live.
    #[test]
    fn text_reads_from_segment_after_seal() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_doc(&e, "a", &body_from(&[("alpha", 3), ("beta", 1)]), Some(5.0)); // id 0, len 4
        index_doc(&e, "b", &body_from(&[("alpha", 1)]), Some(6.0)); // id 1, len 1
        index_doc(&e, "c", &body_from(&[("gamma", 2)]), Some(7.0)); // id 2, len 2

        // Capture live BM25 scores before sealing.
        let live = scores_of(&run(&e, bm25_single("alpha")));

        let dir = tempfile::tempdir().unwrap();
        e.__seal_text_field_to_segment("c", "body", dir.path())
            .unwrap();

        // After sealing, the same query must yield byte-identical scores.
        let sealed_scores = scores_of(&run(&e, bm25_single("alpha")));
        assert_eq!(
            live, sealed_scores,
            "sealed BM25 must be bit-identical to live"
        );

        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
            panic!("body must be a Text field");
        };
        assert!(idx.segment.is_some(), "segment attached");
        // Postings come from the segment.
        let p = idx.tok_postings("alpha").unwrap();
        assert_eq!(p.docids(), &[0u32, 1]);
        assert_eq!(p.tfs(), &[3u32, 1]);
        // doc_len routes through the segment DocLen column.
        assert_eq!(idx.doc_len(0), 4);
        assert_eq!(idx.doc_len(1), 1);
        assert_eq!(idx.doc_len(2), 2);
        // df from the segment posting length.
        assert_eq!(idx.tok_df("alpha"), Some(2));
        assert_eq!(idx.tok_df("durian"), None);
        // corpus scalars now read the LIVE counters (initialized from the header
        // at seal: doc_count=3, total_doc_len=4+1+2). Phase 2h-4.
        assert_eq!(idx.bm25_corpus(), (3, 4 + 1 + 2));
        // Phase 2h-4: `distinct` (and `tokens`/`lens`) are DROPPED at seal — no RAM
        // rebuild. `drop_eid` tombstones a sealed base id instead of consuming
        // `distinct`. The BM25 scan answers entirely from the mmap segment.
        assert!(
            idx.tokens.is_empty(),
            "tokens dropped at seal (postings on disk)"
        );
        assert!(
            idx.distinct_is_empty(),
            "distinct dropped at seal (drop_eid uses tombstones)"
        );
        assert!(
            idx.lens.is_empty(),
            "lens dropped at seal (doc_len reads the segment column)"
        );
        assert!(idx.tombstones.is_empty(), "no deletes yet");
    }

    /// BM25 REOPEN BYTE-IDENTICAL + RAM-BOUNDED (Phase 2h-4): seal the WHOLE
    /// collection to disk, reopen into a FRESH engine from the segments alone (no
    /// CBOR snapshot), and assert (a) the reopened Text field has `tokens` AND
    /// `distinct` EMPTY (no RAM rebuild — RAM is O(live tail), not O(corpus)), yet
    /// (b) the BM25 scan — text_bm25 (single), text_and (multi), filtered_search —
    /// is byte-identical f32 (to_bits) AND same result-set as an in-RAM oracle,
    /// driven entirely from the mmap with `tokens.is_empty()`.
    #[test]
    fn reopen_drives_bm25_from_segment_with_empty_tokens() {
        // Build the same tf-realistic corpus into a SEAL-then-REOPEN subject and an
        // in-RAM oracle.
        fn build(e: &Engine) {
            e.create_collection("c", schema()).unwrap();
            index_doc(
                e,
                "d0",
                &body_from(&[("alpha", 3), ("beta", 1)]),
                Some(20.0),
            );
            index_doc(
                e,
                "d1",
                &body_from(&[("alpha", 1), ("gamma", 2)]),
                Some(40.0),
            );
            index_doc(e, "d2", &body_from(&[("beta", 4)]), Some(55.0));
            index_doc(
                e,
                "d3",
                &body_from(&[("alpha", 2), ("beta", 1), ("gamma", 1)]),
                Some(70.0),
            );
            index_doc(e, "d4", &body_from(&[("gamma", 3)]), Some(90.0));
        }

        let oracle = Arc::new(Engine::new());
        build(&oracle);
        let o_single = run(&oracle, bm25_single("alpha"));
        let o_and = run(&oracle, text_and("alpha", "beta"));
        let o_filt = run(&oracle, filtered("alpha", 10.0, 80.0));

        // Subject: build, seal the whole collection, reopen into a FRESH engine.
        let subject = Arc::new(Engine::new());
        build(&subject);
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir.path(), 1)
            .unwrap();
        let sch = subject.__collection_schema("c").unwrap();
        let reopened = Engine::__open_collection_from_segments("c", dir.path(), sch, 1).unwrap();

        // RAM-BOUNDED: the reopened Text field drives BM25 from the mmap with NO
        // in-RAM tokens/distinct rebuild.
        {
            let state = reopened.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
                panic!("body must be a Text field");
            };
            assert!(idx.segment.is_some(), "reopened segment attached");
            assert!(
                idx.tokens.is_empty(),
                "reopen must NOT rebuild `tokens` (got {})",
                idx.tokens.len()
            );
            assert!(
                idx.distinct_is_empty(),
                "reopen must NOT rebuild `distinct` (got {})",
                idx.distinct_iter().count()
            );
            assert!(
                idx.lens.is_empty(),
                "reopen must NOT rebuild `lens` (doc_len reads the segment column)"
            );
            // Live corpus scalars were initialized from the header.
            assert_eq!(
                idx.bm25_corpus(),
                (5, 4 + 3 + 4 + 4 + 3),
                "live corpus initialized from header"
            );
        }

        // BYTE-IDENTICAL BM25 from the mmap, same sets.
        let r_single = run(&reopened, bm25_single("alpha"));
        let r_and = run(&reopened, text_and("alpha", "beta"));
        let r_filt = run(&reopened, filtered("alpha", 10.0, 80.0));
        assert_eq!(
            set_of(&o_single),
            set_of(&r_single),
            "reopen single-token set diverged"
        );
        assert_eq!(
            set_of(&o_and),
            set_of(&r_and),
            "reopen 2-token AND set diverged"
        );
        assert_eq!(
            set_of(&o_filt),
            set_of(&r_filt),
            "reopen filtered set diverged"
        );
        assert_eq!(
            scores_of(&o_single),
            scores_of(&r_single),
            "reopen single-token scores diverged"
        );
        assert_eq!(
            scores_of(&o_and),
            scores_of(&r_and),
            "reopen 2-token AND scores diverged"
        );
        assert_eq!(
            scores_of(&o_filt),
            scores_of(&r_filt),
            "reopen filtered scores diverged"
        );
    }

    /// DELETE-AFTER-SEAL BM25 — THE CRUX (Phase 2h-4): seal a Text corpus, DELETE
    /// base docs (NO re-seal), and assert the segment-ON BM25 equals an in-RAM
    /// oracle that indexed the SAME docs and physically deleted the SAME ones —
    /// byte-identical f32 scores AND result-sets for single/multi-token + the
    /// corpus-sensitive filtered case. Deleting a doc shifts doc_count/avgdl/df, so
    /// EVERY surviving score changes; the tombstone subtraction + live corpus must
    /// reproduce that shift exactly. `tombstones.len()` matches the deletes.
    #[test]
    fn delete_after_seal_bm25_matches_oracle() {
        fn build(e: &Engine) {
            e.create_collection("c", schema()).unwrap();
            // A corpus where the deleted docs carry the query terms, so removing
            // them moves df AND the corpus length factor.
            index_doc(
                e,
                "d0",
                &body_from(&[("alpha", 3), ("beta", 1)]),
                Some(20.0),
            );
            index_doc(
                e,
                "d1",
                &body_from(&[("alpha", 1), ("gamma", 4)]),
                Some(35.0),
            );
            index_doc(
                e,
                "d2",
                &body_from(&[("alpha", 2), ("beta", 2)]),
                Some(50.0),
            );
            index_doc(
                e,
                "d3",
                &body_from(&[("beta", 3), ("gamma", 1)]),
                Some(65.0),
            );
            index_doc(
                e,
                "d4",
                &body_from(&[("alpha", 1), ("beta", 1)]),
                Some(80.0),
            );
            index_doc(e, "d5", &body_from(&[("gamma", 5)]), Some(95.0));
        }
        // Delete docs that DO carry alpha/beta so df + avgdl both shift.
        let to_delete = ["d1", "d2", "d4"];

        // ORACLE: in-RAM, physically delete, NEVER sealed.
        let oracle = Arc::new(Engine::new());
        build(&oracle);
        for d in to_delete {
            oracle.delete("c", d, None).unwrap();
        }

        // SUBJECT: seal body + price, THEN delete (no re-seal).
        let subject = Arc::new(Engine::new());
        build(&subject);
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir.path(), 1)
            .unwrap();
        for d in to_delete {
            subject.delete("c", d, None).unwrap();
        }

        // The deletes are tombstoned, NOT removed from the on-disk postings; the
        // LIVE corpus scalars were decremented (so avgdl shifts).
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
                panic!("body");
            };
            assert!(idx.tokens.is_empty(), "sealed: tokens still dropped");
            assert_eq!(idx.tombstones.len(), 3, "three base deletes tombstoned");
            // 6 docs - 3 deleted = 3 live; total_doc_len = d0(4)+d3(4)+d5(5)=13.
            assert_eq!(
                idx.bm25_corpus(),
                (3, 4 + 4 + 5),
                "live corpus reflects deletes (drives avgdl)"
            );
            // The oracle and subject must agree on the live corpus EXACTLY.
            let ostate = oracle.state.read().unwrap();
            let ocoll = ostate.collections.get("c").unwrap();
            let FieldIndex::Text { idx: oidx, .. } = ocoll.fields.get("body").unwrap() else {
                panic!("oracle body");
            };
            assert_eq!(
                idx.bm25_corpus(),
                (oidx.doc_count, oidx.total_doc_len),
                "corpus must match oracle"
            );
        }

        // BYTE-IDENTICAL f32 scores AND sets across the corpus-sensitive shapes.
        // (Deleting d1/d2/d4 changes N, avgdl, and df(alpha)/df(beta) — every
        // surviving doc's score shifts, and it must match the oracle bit-for-bit.)
        let s_single = run(&subject, bm25_single("alpha"));
        let o_single = run(&oracle, bm25_single("alpha"));
        let s_beta = run(&subject, bm25_single("beta"));
        let o_beta = run(&oracle, bm25_single("beta"));
        let s_and = run(&subject, text_and("alpha", "beta"));
        let o_and = run(&oracle, text_and("alpha", "beta"));
        let s_filt = run(&subject, filtered("alpha", 10.0, 90.0));
        let o_filt = run(&oracle, filtered("alpha", 10.0, 90.0));

        assert_eq!(
            set_of(&s_single),
            set_of(&o_single),
            "alpha set leaked a deleted doc"
        );
        assert_eq!(
            scores_of(&s_single),
            scores_of(&o_single),
            "alpha scores diverged (corpus shift not reproduced)"
        );
        assert_eq!(
            set_of(&s_beta),
            set_of(&o_beta),
            "beta set leaked a deleted doc"
        );
        assert_eq!(
            scores_of(&s_beta),
            scores_of(&o_beta),
            "beta scores diverged"
        );
        assert_eq!(
            set_of(&s_and),
            set_of(&o_and),
            "AND set leaked a deleted doc"
        );
        assert_eq!(scores_of(&s_and), scores_of(&o_and), "AND scores diverged");
        assert_eq!(
            set_of(&s_filt),
            set_of(&o_filt),
            "filtered set leaked a deleted doc"
        );
        assert_eq!(
            scores_of(&s_filt),
            scores_of(&o_filt),
            "filtered scores diverged"
        );

        // Concrete spot-check: alpha now only d0 survives (d1, d2, d4 deleted).
        assert_eq!(
            set_of(&s_single),
            ["d0".to_string()].into_iter().collect::<BTreeSet<_>>(),
            "alpha must be only the surviving d0"
        );
        // beta: d0 and d3 survive (d2, d4 deleted).
        assert_eq!(
            set_of(&s_beta),
            ["d0".to_string(), "d3".to_string()]
                .into_iter()
                .collect::<BTreeSet<_>>(),
            "beta must be d0 + d3"
        );
    }

    /// AND-STREAMING PERF FIX regression (the 500k-hot-doc `match … op: "and"`
    /// fix): seal `body` to a segment, add MORE docs afterward (a live-tail
    /// overlay on top of the sealed base — one of the four sources
    /// `TokProbe`/`eval_match_topk`'s streaming intersection must compose),
    /// THEN delete some of the sealed docs (tombstones — the segment, live
    /// tail, AND tombstones are all simultaneously non-empty, matching the
    /// perf brief's corpus shape). The AND result set and f32 scores through
    /// BOTH `eval_match_topk` (`run`, the search hot path) and the nested
    /// `eval_match` map path (`QueryNode::And([Match])`, which is NOT the
    /// `eval_match_topk` fast path) must equal an in-RAM oracle that indexed
    /// only the surviving docs and never sealed.
    #[test]
    fn and_query_matches_oracle_over_segment_plus_live_tail_plus_tombstones() {
        let subject = Arc::new(Engine::new());
        subject.create_collection("c", schema()).unwrap();
        // Sealed base: d0..d3.
        index_doc(
            &subject,
            "d0",
            &body_from(&[("alpha", 3), ("beta", 1)]),
            Some(10.0),
        );
        index_doc(
            &subject,
            "d1",
            &body_from(&[("alpha", 1), ("beta", 2)]),
            Some(20.0),
        );
        index_doc(&subject, "d2", &body_from(&[("alpha", 2)]), Some(30.0));
        index_doc(&subject, "d3", &body_from(&[("beta", 3)]), Some(40.0));
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_text_field_to_segment("c", "body", dir.path())
            .unwrap();
        // Live tail, added AFTER the seal (never sealed — pure live overlay).
        index_doc(
            &subject,
            "d4",
            &body_from(&[("alpha", 4), ("beta", 1)]),
            Some(50.0),
        );
        index_doc(
            &subject,
            "d5",
            &body_from(&[("alpha", 1), ("beta", 4)]),
            Some(60.0),
        );
        // Tombstone a sealed base doc that carries BOTH query tokens.
        subject.delete("c", "d1", None).unwrap();

        let oracle = Arc::new(Engine::new());
        oracle.create_collection("c", schema()).unwrap();
        index_doc(
            &oracle,
            "d0",
            &body_from(&[("alpha", 3), ("beta", 1)]),
            Some(10.0),
        );
        index_doc(&oracle, "d2", &body_from(&[("alpha", 2)]), Some(30.0));
        index_doc(&oracle, "d3", &body_from(&[("beta", 3)]), Some(40.0));
        index_doc(
            &oracle,
            "d4",
            &body_from(&[("alpha", 4), ("beta", 1)]),
            Some(50.0),
        );
        index_doc(
            &oracle,
            "d5",
            &body_from(&[("alpha", 1), ("beta", 4)]),
            Some(60.0),
        );

        // The `eval_match_topk` hot path (through `search`, top-level Match).
        let s_topk = run(&subject, text_and("alpha", "beta"));
        let o_topk = run(&oracle, text_and("alpha", "beta"));
        assert_eq!(set_of(&s_topk), set_of(&o_topk), "topk AND set diverged");
        assert_eq!(
            scores_of(&s_topk),
            scores_of(&o_topk),
            "topk AND scores diverged"
        );
        // d1 is gone (tombstoned); d0/d4/d5 carry both tokens, d2/d3 carry only one.
        assert_eq!(
            set_of(&s_topk),
            ["d0", "d4", "d5"]
                .into_iter()
                .map(String::from)
                .collect::<BTreeSet<_>>(),
            "AND must keep exactly the docs carrying both surviving tokens"
        );

        // The `eval_match` map path: nest the Match under a single-child And so
        // the top-level query is NOT `QueryNode::Match` and the search router's
        // `eval_match_topk` fast path is bypassed, reaching `eval_query`'s
        // `QueryNode::Match(m) => eval_match(coll, m)?` instead.
        let wrapped = QueryNode::And(vec![text_and("alpha", "beta")]);
        let s_map = run(&subject, wrapped.clone());
        let o_map = run(&oracle, wrapped);
        assert_eq!(set_of(&s_map), set_of(&o_map), "map-path AND set diverged");
        assert_eq!(
            scores_of(&s_map),
            scores_of(&o_map),
            "map-path AND scores diverged"
        );
    }

    /// RE-SEAL after delete (Phase 2h-4): once re-sealed the deletes are BAKED into
    /// the new segment (2g-A live(id) GC), the tombstone is CLEARED, and a fresh
    /// reopen excludes the deleted docs with a correct corpus.
    #[test]
    fn reseal_bakes_text_deletes_and_clears_tombstone() {
        fn build(e: &Engine) {
            e.create_collection("c", schema()).unwrap();
            index_doc(
                e,
                "d0",
                &body_from(&[("alpha", 2), ("beta", 1)]),
                Some(20.0),
            );
            index_doc(e, "d1", &body_from(&[("alpha", 3)]), Some(40.0));
            index_doc(
                e,
                "d2",
                &body_from(&[("beta", 2), ("gamma", 1)]),
                Some(60.0),
            );
            index_doc(
                e,
                "d3",
                &body_from(&[("alpha", 1), ("gamma", 2)]),
                Some(80.0),
            );
        }
        let to_delete = ["d1", "d2"];

        let oracle = Arc::new(Engine::new());
        build(&oracle);
        for d in to_delete {
            oracle.delete("c", d, None).unwrap();
        }

        let subject = Arc::new(Engine::new());
        build(&subject);
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir.path(), 1)
            .unwrap();
        for d in to_delete {
            subject.delete("c", d, None).unwrap();
        }
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
                panic!("body");
            };
            assert_eq!(idx.tombstones.len(), 2, "deletes tombstoned before re-seal");
        }

        // RE-SEAL into a new dir: deletes baked in, tombstone cleared.
        let dir2 = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir2.path(), 2)
            .unwrap();
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
                panic!("body");
            };
            assert!(
                idx.tombstones.is_empty(),
                "tombstone must be CLEARED after re-seal"
            );
        }

        // Reopen from the RE-SEALED dir: deleted docs gone, corpus correct.
        let sch = subject.__collection_schema("c").unwrap();
        let reopened = Engine::__open_collection_from_segments("c", dir2.path(), sch, 2).unwrap();
        {
            let state = reopened.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
                panic!("body");
            };
            // 4 docs - 2 deleted = 2 live; total = d0(3) + d3(3) = 6.
            assert_eq!(
                idx.bm25_corpus(),
                (2, 3 + 3),
                "re-sealed corpus excludes deletes"
            );
        }

        let s_single = run(&reopened, bm25_single("alpha"));
        let o_single = run(&oracle, bm25_single("alpha"));
        assert_eq!(
            set_of(&s_single),
            set_of(&o_single),
            "post-re-seal alpha set diverged"
        );
        assert_eq!(
            scores_of(&s_single),
            scores_of(&o_single),
            "post-re-seal alpha scores diverged"
        );
        // beta fully deleted only via d2; d0 still has beta → survives.
        assert_eq!(
            set_of(&run(&reopened, bm25_single("beta"))),
            ["d0".to_string()].into_iter().collect::<BTreeSet<_>>(),
            "beta must be only the surviving d0 after re-seal"
        );
        assert_eq!(
            uniq(&reopened, "body"),
            uniq(&oracle, "body"),
            "unique_terms diverged after re-seal"
        );
    }

    #[test]
    fn sealed_text_unions_tail_and_reused_overlay_postings() {
        let oracle = Arc::new(Engine::new());
        let subject = Arc::new(Engine::new());
        oracle.create_collection("c", schema()).unwrap();
        subject.create_collection("c", schema()).unwrap();
        index_doc(
            &oracle,
            "d0",
            &body_from(&[("shared", 2), ("base", 1)]),
            Some(1.0),
        );
        index_doc(
            &subject,
            "d0",
            &body_from(&[("shared", 2), ("base", 1)]),
            Some(1.0),
        );
        index_doc(&oracle, "d1", &body_from(&[("base", 1)]), Some(2.0));
        index_doc(&subject, "d1", &body_from(&[("base", 1)]), Some(2.0));
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_text_field_to_segment("c", "body", dir.path())
            .unwrap();

        // d0 reuses a sealed id, while d2 is a pure live tail. Both must be
        // visible through the same active posting accessor.
        index_doc(
            &oracle,
            "d0",
            &body_from(&[("shared", 1), ("overlay", 2)]),
            Some(3.0),
        );
        index_doc(
            &subject,
            "d0",
            &body_from(&[("shared", 1), ("overlay", 2)]),
            Some(3.0),
        );
        index_doc(
            &oracle,
            "d2",
            &body_from(&[("shared", 1), ("tailonly", 1)]),
            Some(4.0),
        );
        index_doc(
            &subject,
            "d2",
            &body_from(&[("shared", 1), ("tailonly", 1)]),
            Some(4.0),
        );

        for token in ["shared", "base", "overlay", "tailonly"] {
            let query = bm25_single(token);
            assert_eq!(
                set_of(&run(&subject, query.clone())),
                set_of(&run(&oracle, query.clone()))
            );
            assert_eq!(
                scores_of(&run(&subject, query.clone())),
                scores_of(&run(&oracle, query))
            );
        }
        for query in [text_and("shared", "overlay"), filtered("shared", 3.0, 3.0)] {
            assert_eq!(
                set_of(&run(&subject, query.clone())),
                set_of(&run(&oracle, query.clone()))
            );
            assert_eq!(
                scores_of(&run(&subject, query.clone())),
                scores_of(&run(&oracle, query))
            );
        }
        assert_eq!(
            set_of(&run(&subject, bm25_single("shared"))),
            ["d0", "d2"].into_iter().map(String::from).collect()
        );

        // Snapshot restore must preserve both the appended tail and the
        // replacement overlay on the reused base id.
        let restored = Arc::new(Engine::new());
        restored.restore(subject.snapshot().unwrap()).unwrap();
        for token in ["shared", "overlay", "tailonly"] {
            let query = bm25_single(token);
            assert_eq!(
                set_of(&run(&restored, query.clone())),
                set_of(&run(&subject, query.clone()))
            );
            assert_eq!(
                scores_of(&run(&restored, query.clone())),
                scores_of(&run(&subject, query))
            );
        }

        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
            panic!("body must be text");
        };
        assert_eq!(idx.tok_df("shared"), Some(2));
        assert_eq!(idx.tok_df("tailonly"), Some(1));
        assert_eq!(idx.tok_df("base"), Some(1));
        let posting = idx.tok_postings("shared").unwrap();
        assert_eq!(posting.docids(), &[0, 2]);
        assert_eq!(posting.tfs(), &[1, 1]);
    }

    #[test]
    fn sealed_text_replacement_twice_keeps_latest_overlay_and_snapshot_state() {
        let oracle = Arc::new(Engine::new());
        let subject = Arc::new(Engine::new());
        oracle.create_collection("c", schema()).unwrap();
        subject.create_collection("c", schema()).unwrap();
        index_doc(&oracle, "d0", "old", Some(1.0));
        index_doc(&subject, "d0", "old", Some(1.0));
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_text_field_to_segment("c", "body", dir.path())
            .unwrap();

        for engine in [&oracle, &subject] {
            index_doc(engine, "d0", "new", Some(2.0));
            index_doc(engine, "d0", "latest", Some(3.0));
        }
        assert_eq!(
            scores_of(&run(&subject, bm25_single("old"))),
            scores_of(&run(&oracle, bm25_single("old")))
        );
        assert!(run(&subject, bm25_single("new")).is_empty());
        assert_eq!(
            set_of(&run(&subject, bm25_single("latest"))),
            ["d0"].into_iter().map(String::from).collect()
        );
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
                panic!("body must be text");
            };
            assert!(idx.tok_postings("new").is_none());
            assert_eq!(idx.tok_df("latest"), Some(1));
            assert_eq!(idx.bm25_corpus(), (1, 1));
            assert!(idx
                .distinct_at(0)
                .is_some_and(|tokens| tokens.iter().next().is_some()));
            assert_eq!(idx.doc_len(0), 1);
            assert!(idx.tombstones.contains(0));
        }

        // Snapshot must retain the latest live overlay and restore the same
        // query state without a segment.
        let restored = Arc::new(Engine::new());
        restored.restore(subject.snapshot().unwrap()).unwrap();
        assert_eq!(
            run(&restored, bm25_single("latest")),
            run(&subject, bm25_single("latest"))
        );
        let state = restored.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
            panic!("body must be text");
        };
        assert!(idx.distinct.get(0).is_some_and(|tokens| {
            tokens
                .as_ref()
                .is_some_and(|tokens| tokens.iter().next().is_some())
        }));
        assert_eq!(idx.doc_len(0), 1);
    }

    #[test]
    fn sealed_text_absent_base_replacement_does_not_tombstone_overlay() {
        fn index_body(e: &Engine, eid: &str, body: &str) {
            e.index(
                "c",
                IndexRequest {
                    items: vec![crate::shared_kernel::types::document::IndexItem {
                        external_id: eid.into(),
                        field: "body".into(),
                        value: FieldValue::String(body.into()),
                        version: None,
                    }],
                    request_id: None,
                },
            )
            .unwrap();
        }
        fn index_price(e: &Engine, eid: &str, price: f64) {
            e.index(
                "c",
                IndexRequest {
                    items: vec![crate::shared_kernel::types::document::IndexItem {
                        external_id: eid.into(),
                        field: "price".into(),
                        value: FieldValue::Number(price),
                        version: None,
                    }],
                    request_id: None,
                },
            )
            .unwrap();
        }

        let oracle = Arc::new(Engine::new());
        let subject = Arc::new(Engine::new());
        oracle.create_collection("c", schema()).unwrap();
        subject.create_collection("c", schema()).unwrap();
        for engine in [&oracle, &subject] {
            index_price(engine, "d0", 1.0);
            index_body(engine, "d1", "seed");
        }
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir.path(), 1)
            .unwrap();

        // d0 had no body at the seal. Its first post-seal body write creates a
        // live overlay without a base tombstone; the second write must remove
        // that overlay before applying the latest value.
        for engine in [&oracle, &subject] {
            index_body(engine, "d0", "old");
            index_body(engine, "d0", "new");
        }
        let old = bm25_single("old");
        let new = bm25_single("new");
        assert!(run(&subject, old.clone()).is_empty());
        assert_eq!(
            set_of(&run(&subject, new.clone())),
            ["d0"].into_iter().map(String::from).collect()
        );
        assert_eq!(
            scores_of(&run(&subject, new.clone())),
            scores_of(&run(&oracle, new.clone()))
        );
        {
            let state = subject.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
                panic!("body must be text");
            };
            assert!(idx.tombstones.is_empty());
            assert_eq!(idx.bm25_corpus(), (2, 2));
            let posting = idx.tok_postings("new").unwrap();
            assert_eq!(posting.docids(), &[0]);
            assert_eq!(posting.tfs(), &[1]);
        }

        let restored = Arc::new(Engine::new());
        restored.restore(subject.snapshot().unwrap()).unwrap();
        assert_eq!(
            scores_of(&run(&restored, new.clone())),
            scores_of(&run(&subject, new.clone()))
        );
        assert!(run(&restored, old.clone()).is_empty());

        let dir2 = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir2.path(), 2)
            .unwrap();
        let schema = subject.__collection_schema("c").unwrap();
        let reopened =
            Engine::__open_collection_from_segments("c", dir2.path(), schema, 2).unwrap();
        assert_eq!(
            scores_of(&run(&reopened, new)),
            scores_of(&run(&oracle, bm25_single("new")))
        );
        assert!(run(&reopened, old).is_empty());
    }

    #[test]
    fn ngram_streamed_write_survives_checkpoint_and_cold_reopen() {
        let engine = Arc::new(Engine::new());
        let mut ngram_schema = schema();
        ngram_schema.fields.get_mut("body").unwrap().analyzer = Some(Analyzer::Ngram);
        engine.create_collection("c", ngram_schema).unwrap();

        let text = "İstanbul ABcd";
        index_doc(&engine, "unicode", text, Some(1.0));
        let expected_len = u32::try_from(tokenize::tokenize(text, Analyzer::Ngram).len()).unwrap();
        let doc_len = |subject: &Engine| {
            let state = subject.state.read().unwrap();
            let collection = state.collections.get("c").unwrap();
            let id = collection.interner.id("unicode").unwrap();
            let FieldIndex::Text { idx, .. } = collection.fields.get("body").unwrap() else {
                panic!("body must be text");
            };
            idx.doc_len(id)
        };
        assert_eq!(doc_len(&engine), expected_len);
        let before = run(&engine, bm25_single("ABCD"));
        assert_eq!(set_of(&before), BTreeSet::from(["unicode".to_owned()]));

        let directory = tempfile::tempdir().unwrap();
        engine
            .__seal_collection_to_segments("c", directory.path(), 1)
            .unwrap();
        let cold = Engine::__open_collection_from_segments(
            "c",
            directory.path(),
            engine.__collection_schema("c").unwrap(),
            1,
        )
        .unwrap();
        assert_eq!(doc_len(&cold), expected_len);
        assert_eq!(
            scores_of(&run(&cold, bm25_single("ABCD"))),
            scores_of(&before),
            "cold search must retain the ngram stream postings and document length"
        );
    }

    #[test]
    fn sealed_text_reseal_and_cold_reopen_match_ram_scores() {
        let oracle = Arc::new(Engine::new());
        let subject = Arc::new(Engine::new());
        oracle.create_collection("c", schema()).unwrap();
        subject.create_collection("c", schema()).unwrap();
        for (eid, body, price) in [("d0", "shared base", 1.0), ("d1", "base", 2.0)] {
            index_doc(&oracle, eid, body, Some(price));
            index_doc(&subject, eid, body, Some(price));
        }
        let dir = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir.path(), 1)
            .unwrap();
        for (eid, body, price) in [
            ("d0", "shared overlay", 3.0),
            ("d2", "shared tailonly", 4.0),
        ] {
            index_doc(&oracle, eid, body, Some(price));
            index_doc(&subject, eid, body, Some(price));
        }
        let dir2 = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir2.path(), 2)
            .unwrap();
        let schema = subject.__collection_schema("c").unwrap();
        let reopened =
            Engine::__open_collection_from_segments("c", dir2.path(), schema, 2).unwrap();
        for token in ["shared", "overlay", "tailonly"] {
            let query = bm25_single(token);
            assert_eq!(
                set_of(&run(&reopened, query.clone())),
                set_of(&run(&oracle, query.clone()))
            );
            assert_eq!(
                scores_of(&run(&reopened, query.clone())),
                scores_of(&run(&oracle, query))
            );
        }
        assert_eq!(run(&reopened, bm25_single("shared")).len(), 2);
    }

    #[test]
    fn sealed_empty_text_presence_survives_reseal_snapshot_and_delete() {
        fn index_body(e: &Engine, eid: &str, body: &str) {
            e.index(
                "c",
                IndexRequest {
                    items: vec![crate::shared_kernel::types::document::IndexItem {
                        external_id: eid.into(),
                        field: "body".into(),
                        value: FieldValue::String(body.into()),
                        version: None,
                    }],
                    request_id: None,
                },
            )
            .unwrap();
        }
        fn index_price(e: &Engine, eid: &str, price: f64) {
            e.index(
                "c",
                IndexRequest {
                    items: vec![crate::shared_kernel::types::document::IndexItem {
                        external_id: eid.into(),
                        field: "price".into(),
                        value: FieldValue::Number(price),
                        version: None,
                    }],
                    request_id: None,
                },
            )
            .unwrap();
        }
        fn build(e: &Engine) {
            e.create_collection("c", schema()).unwrap();
            index_body(e, "d0", "");
            index_price(e, "d0", 1.0);
            index_body(e, "d1", "control");
            index_price(e, "d1", 2.0);
        }
        fn body_corpus(e: &Engine) -> (u64, u64) {
            let state = e.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
                panic!("body must be text");
            };
            idx.bm25_corpus()
        }
        fn body_is_covered(e: &Engine, eid: &str) -> bool {
            let state = e.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let id = coll.interner.id(eid).unwrap();
            coll.eid_fields
                .get(&id)
                .is_some_and(|fields| fields.contains("body"))
        }

        let oracle = Arc::new(Engine::new());
        build(&oracle);

        let subject = Arc::new(Engine::new());
        build(&subject);
        let dir1 = tempfile::tempdir().unwrap();
        subject
            .__seal_collection_to_segments("c", dir1.path(), 1)
            .unwrap();
        let schema1 = subject.__collection_schema("c").unwrap();
        let cold = Engine::__open_collection_from_segments("c", dir1.path(), schema1, 1).unwrap();
        assert!(
            body_is_covered(&cold, "d0"),
            "empty body coverage after cold reopen"
        );
        assert!(body_is_covered(&cold, "d1"));
        assert_eq!(body_corpus(&cold), body_corpus(&oracle));

        // A re-seal must carry the explicit empty presence bit forward, even
        // though d0's DocLen and posting list are both empty.
        let dir2 = tempfile::tempdir().unwrap();
        cold.__seal_collection_to_segments("c", dir2.path(), 2)
            .unwrap();
        let schema2 = cold.__collection_schema("c").unwrap();
        let cold2 = Engine::__open_collection_from_segments("c", dir2.path(), schema2, 2).unwrap();
        assert!(
            body_is_covered(&cold2, "d0"),
            "empty body coverage after re-seal"
        );
        assert_eq!(body_corpus(&cold2), body_corpus(&oracle));
        let cold_snapshot = cold2.snapshot().unwrap();

        // The cold segment-backed path must recognize the empty base value as
        // covered. Updating d0 must tombstone that base once before adding the
        // replacement posting.
        index_body(&oracle, "d0", "updated");
        index_body(&cold2, "d0", "updated");
        assert_eq!(body_corpus(&cold2), body_corpus(&oracle));
        let updated = bm25_single("updated");
        assert_eq!(
            set_of(&run(&cold2, updated.clone())),
            set_of(&run(&oracle, updated.clone()))
        );
        assert_eq!(
            scores_of(&run(&cold2, updated.clone())),
            scores_of(&run(&oracle, updated.clone()))
        );
        let control = bm25_single("control");
        assert_eq!(
            set_of(&run(&cold2, control.clone())),
            set_of(&run(&oracle, control.clone()))
        );
        assert_eq!(
            scores_of(&run(&cold2, control.clone())),
            scores_of(&run(&oracle, control.clone()))
        );

        // Delete the formerly empty doc's field and compare exact corpus, IDs,
        // and score bits with the pure-RAM path.
        oracle.delete("c", "d0", Some("body")).unwrap();
        cold2.delete("c", "d0", Some("body")).unwrap();
        assert_eq!(body_corpus(&cold2), body_corpus(&oracle));
        assert!(run(&cold2, updated.clone()).is_empty());
        assert_eq!(
            set_of(&run(&cold2, control.clone())),
            set_of(&run(&oracle, control.clone()))
        );
        assert_eq!(
            scores_of(&run(&cold2, control.clone())),
            scores_of(&run(&oracle, control.clone()))
        );

        // A snapshot taken after the cold reopen must retain empty-field
        // coverage and permit a later replacement through the RAM path.
        let restored = Arc::new(Engine::new());
        restored.restore(cold_snapshot).unwrap();
        assert!(
            body_is_covered(&restored, "d0"),
            "snapshot lost empty coverage"
        );
        let snapshot_oracle = Arc::new(Engine::new());
        build(&snapshot_oracle);
        index_body(&restored, "d0", "snapshot-updated");
        index_body(&snapshot_oracle, "d0", "snapshot-updated");
        assert_eq!(body_corpus(&restored), body_corpus(&snapshot_oracle));
        let snapshot_updated = bm25_single("snapshot-updated");
        assert_eq!(
            set_of(&run(&restored, snapshot_updated.clone())),
            set_of(&run(&snapshot_oracle, snapshot_updated.clone()))
        );
        assert_eq!(
            scores_of(&run(&restored, snapshot_updated.clone())),
            scores_of(&run(&snapshot_oracle, snapshot_updated))
        );
    }
}

// ---------------------------------------------------------------------------
// Dual-path diff test: segment-backed Hash (Hamming) read must be
// byte-identical to the live in-RAM read (Stage 2 Phase 2d).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod segment_hash_diff_tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    fn fieldspec(t: FieldType) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    /// A single `sig` Hash field.
    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("sig".into(), fieldspec(FieldType::Hash));
        CreateCollectionRequest { fields }
    }

    fn index_hash(e: &Engine, eid: &str, hash: u64) {
        e.index(
            "c",
            IndexRequest {
                items: vec![crate::shared_kernel::types::document::IndexItem {
                    external_id: eid.into(),
                    field: "sig".into(),
                    value: FieldValue::String(format!("{hash:016x}")),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
    }

    fn hamming(hash: u64, max: u32) -> SearchRequest {
        SearchRequest {
            query: QueryNode::Hamming(HammingQuery {
                field: "sig".into(),
                hash: format!("{hash:016x}"),
                max_distance: max,
            }),
            limit: 100_000,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    /// (external_id, score_bits) keyed map — order-independent, byte-exact.
    fn run(e: &Engine, hash: u64, max: u32) -> BTreeMap<String, u32> {
        e.search("c", hamming(hash, max))
            .unwrap()
            .hits
            .into_iter()
            .map(|h| (h.external_id, h.score.to_bits()))
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// PATH A (segment OFF, brute-force over `forward`) must equal PATH B
        /// (Hash field sealed to an mmap segment, hash read via the segment) —
        /// same matching docs AND byte-identical Hamming similarity scores —
        /// over a randomized corpus and query.
        #[test]
        fn hamming_segment_matches_live(
            hashes in proptest::collection::vec(any::<u64>(), 1..60),
            q in any::<u64>(),
            max in 0u32..=64,
        ) {
            // --- PATH A: build, query (segment OFF). ---
            let e = Arc::new(Engine::new());
            e.create_collection("c", schema()).unwrap();
            for (i, h) in hashes.iter().enumerate() {
                index_hash(&e, &format!("d{i}"), *h);
            }
            let a = run(&e, q, max);

            // --- PATH B: seal `sig`, flip it ON, rerun. ---
            let dir = tempfile::tempdir().unwrap();
            let sealed = e.__seal_hash_field_to_segment("c", "sig", dir.path()).unwrap();
            prop_assert_eq!(sealed as usize, hashes.len(), "all docs sealed");
            let b = run(&e, q, max);

            prop_assert_eq!(a, b, "hamming result/scores diverged after seal");
        }
    }

    /// A doc indexed AFTER sealing (docid >= n_docs) must still match through
    /// `hash_at`'s live-tail fallback; the sealed doc is served from the mmap.
    #[test]
    fn hamming_live_tail_after_seal() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index_hash(&e, "sealed", 0x0000_0000_0000_0000); // id 0
        index_hash(&e, "other", 0xFFFF_FFFF_FFFF_FFFF); // id 1

        let dir = tempfile::tempdir().unwrap();
        let n = e
            .__seal_hash_field_to_segment("c", "sig", dir.path())
            .unwrap();
        assert_eq!(n, 2);

        index_hash(&e, "tail", 0x0000_0000_0000_0003); // id 2 (2 bits set), live tail

        // Query hash 0, max distance 2: sealed (dist 0) + tail (dist 2) match,
        // `other` (dist 64) does not. Proves segment read + live-tail fallback.
        let got = run(&e, 0, 2);
        let mut want = BTreeMap::new();
        want.insert("sealed".to_string(), (1.0f32).to_bits()); // dist 0 → 64/64
        want.insert("tail".to_string(), ((64 - 2) as f32 / 64.0).to_bits());
        assert_eq!(got, want);

        // Direct check on hash_at: segment for [0,2), live tail for id 2.
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Hash(h) = coll.fields.get("sig").unwrap() else {
            panic!("sig must be a Hash field");
        };
        assert!(h.segment.is_some(), "segment attached");
        assert_eq!(h.hash_at(0), Some(0)); // segment
        assert_eq!(h.hash_at(1), Some(u64::MAX)); // segment
        assert_eq!(h.hash_at(2), Some(3)); // live tail
        assert_eq!(h.hash_at(99), None);
    }
}

// ---------------------------------------------------------------------------
// Exact hamming as an AND filter (#4246): `and[hamming(max_distance 0), match]`
// must plan as filter-driven (bitmap driver / per-doc predicate) and return
// the SAME hit set with byte-identical scores as the materialize-and-intersect
// fallback, which a fuzzy hamming (`max_distance > 0`) still takes.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod exact_hamming_filter_tests {
    use super::*;
    use std::sync::Arc;

    fn fieldspec(t: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    /// `sig` (Hash) + `body` (Text, whitespace-lower).
    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("sig".into(), fieldspec(FieldType::Hash, None));
        fields.insert(
            "body".into(),
            fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
        );
        CreateCollectionRequest { fields }
    }

    /// Even-weight code `(i << 1) | parity(i)`: every pair of hashes sits at
    /// Hamming distance ≥ 2, so `max_distance 1` (fallback path) and
    /// `max_distance 0` (filter path) select exactly the same docs.
    fn sig(i: u32) -> u64 {
        ((i as u64) << 1) | (i.count_ones() & 1) as u64
    }

    /// Every doc shares most tokens (like the durable workload's ngram text)
    /// and carries one unique token, so a full-text `match … op=and` selects
    /// exactly one doc while every token's posting spans the corpus.
    fn body(i: u32) -> String {
        let parity = if i % 2 == 0 { "even" } else { "odd" };
        format!("doc {i} shared token stream alpha beta gamma {parity}")
    }

    fn index(e: &Engine, eid: &str, hash: u64, text: &str) {
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    crate::shared_kernel::types::document::IndexItem {
                        external_id: eid.into(),
                        field: "sig".into(),
                        value: FieldValue::String(format!("{hash:016x}")),
                        version: None,
                    },
                    crate::shared_kernel::types::document::IndexItem {
                        external_id: eid.into(),
                        field: "body".into(),
                        value: FieldValue::String(text.into()),
                        version: None,
                    },
                ],
                request_id: None,
            },
        )
        .unwrap();
    }

    fn seed(n: u32) -> Arc<Engine> {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        for i in 0..n {
            index(&e, &format!("d{i}"), sig(i), &body(i));
        }
        e
    }

    fn hamming_raw(hash: u64, max: u32) -> QueryNode {
        QueryNode::Hamming(HammingQuery {
            field: "sig".into(),
            hash: format!("{hash:016x}"),
            max_distance: max,
        })
    }

    fn hamming(i: u32, max: u32) -> QueryNode {
        hamming_raw(sig(i), max)
    }

    fn matchq(text: &str) -> QueryNode {
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: text.into(),
            op: MatchOp::And,
        })
    }

    /// (external_id, score_bits) — order-independent, byte-exact.
    fn run(e: &Engine, query: QueryNode) -> BTreeMap<String, u32> {
        e.search(
            "c",
            SearchRequest {
                query,
                limit: 100,
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
        .into_iter()
        .map(|h| (h.external_id, h.score.to_bits()))
        .collect()
    }

    #[test]
    fn exact_hamming_is_a_filter_and_fuzzy_is_not() {
        assert!(is_exact_hamming(&hamming(3, 0)));
        assert!(is_predicable(&hamming(3, 0)));
        assert!(!is_exact_hamming(&hamming(3, 1)));
        assert!(!is_predicable(&hamming(3, 1)));

        let e = seed(8);
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        assert_eq!(estimate_selectivity(coll, &hamming(3, 0)), 1);
        assert_eq!(estimate_selectivity(coll, &hamming(3, 1)), u64::MAX);
        // The exact hamming drives the AND ahead of a corpus-wide match, so
        // the match is scored over the hash hits, never materialized.
        assert!(
            estimate_selectivity(coll, &hamming(3, 0))
                <= estimate_selectivity(coll, &matchq("shared token")),
            "exact hamming must be the cheaper driver"
        );
    }

    #[test]
    fn exact_hamming_bitmap_and_predicate_match_eval_hamming() {
        let e = seed(16);
        // Seal the hash field so `hash_at` serves ids from the segment, then
        // add a live-tail doc: the filter reads must cover both sources.
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            e.__seal_hash_field_to_segment("c", "sig", dir.path())
                .unwrap(),
            16
        );
        index(&e, "d16", sig(16), &body(16));

        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        for i in [0u32, 5, 15, 16, 40] {
            let q = hamming(i, 0);
            let QueryNode::Hamming(hq) = &q else {
                unreachable!()
            };
            let want: RoaringBitmap = eval_hamming(coll, hq).unwrap().into_keys().collect();
            assert_eq!(want.len(), u64::from(i < 17), "sig({i}) hit count");
            let got = eval_filter_bitmap(coll, &q).unwrap();
            assert_eq!(got, want, "bitmap for sig({i})");
            for id in 0..17u32 {
                let pred = clause_matches(coll, &q, id).unwrap();
                assert_eq!(
                    pred,
                    want.contains(id).then_some(1.0),
                    "predicate sig({i}) on id {id}"
                );
            }
        }
    }

    #[test]
    fn exact_hamming_and_match_scores_are_byte_identical_to_fallback() {
        let e = seed(24);
        for i in [0u32, 7, 23] {
            let full = body(i);
            // Fallback: a fuzzy hamming is never predicable, and the pairwise
            // distance ≥ 2 makes `max_distance 1` select the same single doc.
            let fallback = run(&e, QueryNode::And(vec![hamming(i, 1), matchq(&full)]));
            assert_eq!(fallback.len(), 1, "sig({i}) selects exactly its doc");
            assert!(fallback.contains_key(&format!("d{i}")));

            // Filter path, top-k entry (`eval_predicable_and_topk`).
            let topk = run(&e, QueryNode::And(vec![hamming(i, 0), matchq(&full)]));
            assert_eq!(topk, fallback, "top-k filter path vs fallback for d{i}");

            // Filter path, general `eval_query` AND branch (reached through a
            // single-child `or`), with the conjuncts in the other order.
            let general = run(
                &e,
                QueryNode::Or(vec![QueryNode::And(vec![matchq(&full), hamming(i, 0)])]),
            );
            let general_fallback = run(
                &e,
                QueryNode::Or(vec![QueryNode::And(vec![matchq(&full), hamming(i, 1)])]),
            );
            assert_eq!(
                general, general_fallback,
                "eval_query filter path vs fallback for d{i}"
            );
            assert_eq!(
                general, fallback,
                "conjunct order must not change the score for d{i}"
            );

            // A wrong-field text under the same hash misses on every path.
            let wrong = format!("readback mismatch window {i} body");
            assert!(run(&e, QueryNode::And(vec![hamming(i, 0), matchq(&wrong)])).is_empty());
            assert!(run(&e, QueryNode::And(vec![hamming(i, 1), matchq(&wrong)])).is_empty());
        }
    }

    #[test]
    fn fuzzy_hamming_keeps_its_graded_score_on_the_fallback() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        index(&e, "near", 0, "tok");
        index(&e, "far", 1, "tok"); // distance 1 from the query hash 0

        let bm25 = run(&e, matchq("tok"));
        let got = run(&e, QueryNode::And(vec![hamming_raw(0, 1), matchq("tok")]));
        assert_eq!(got.len(), 2);
        assert_eq!(
            got["near"],
            (1.0f32 + f32::from_bits(bm25["near"])).to_bits()
        );
        assert_eq!(
            got["far"],
            ((64 - 1) as f32 / 64.0 + f32::from_bits(bm25["far"])).to_bits()
        );
        // And the exact form drops the distance-1 doc.
        let exact = run(&e, QueryNode::And(vec![hamming_raw(0, 0), matchq("tok")]));
        assert_eq!(exact.len(), 1);
        assert_eq!(exact["near"], got["near"]);
    }

    fn sparse_layered_match_does_not_materialize_for_planning(general: bool, analyzer: Analyzer) {
        use crate::persistence::infrastructure::composed_segment::TextPostingAt;

        let e = Arc::new(Engine::new());
        let mut schema = schema();
        schema.fields.get_mut("body").unwrap().analyzer = Some(analyzer);
        e.create_collection("c", schema).unwrap();
        let text_for = |i| match analyzer {
            Analyzer::Ngram => format!(
                "ngram document {i} slot 0 {}",
                "durable search token ".repeat(12)
            ),
            _ => body(i),
        };
        for i in 0..128 {
            index(&e, &format!("d{i}"), sig(i), &text_for(i));
        }
        let text = text_for(7);
        let query = QueryNode::And(vec![hamming(7, 0), matchq(&text)]);
        let query = if general {
            QueryNode::Or(vec![query])
        } else {
            query
        };
        let expected = run(&e, query.clone());
        assert_eq!(expected.len(), 1);
        assert!(expected.contains_key("d7"));
        let wrong = matchq(&format!("{text} neverindexedpredicate"));
        let wrong_query = QueryNode::And(vec![hamming(7, 0), wrong.clone()]);
        let not_query = QueryNode::And(vec![hamming(7, 0), QueryNode::Not(Box::new(wrong))]);
        assert!(run(&e, wrong_query.clone()).is_empty());
        let expected_not = run(&e, not_query.clone());

        let dir = tempfile::tempdir().unwrap();
        e.__seal_text_field_to_segment("c", "body", dir.path())
            .unwrap();
        // A real replacement layer prevents the dense-base df shortcut.
        // It carries the same row, so the in-memory result remains the oracle.
        let tokens = tokenize::tokenize(&text, analyzer);
        let mut postings: BTreeMap<String, Postings> = BTreeMap::new();
        for token in &tokens {
            let posting = postings.entry(token.clone()).or_default();
            posting.upsert(0, posting.tf(0).unwrap_or(0) + 1);
        }
        let layer_path = dir.path().join("replacement.lseg");
        crate::persistence::infrastructure::segment::text_writer::write_text_segment(
            &layer_path,
            1,
            &postings,
            &[tokens.len() as u32],
            &[true],
            1,
            tokens.len() as u64,
        )
        .unwrap();
        let reader = Arc::new(
            crate::persistence::infrastructure::segment::SegmentReader::open(&layer_path).unwrap(),
        );
        let segment = {
            let mut state = e.state.write().unwrap();
            let coll = state.collections.get_mut("c").unwrap();
            // The fixture swaps the segment directly instead of publishing
            // through the normal write path. Do not reuse its live oracle.
            coll.clear_search_cache();
            let FieldIndex::Text { idx, .. } = coll.fields.get_mut("body").unwrap() else {
                unreachable!()
            };
            let segment = Arc::new(
                idx.segment
                    .as_ref()
                    .unwrap()
                    .with_delta(reader, vec![7])
                    .unwrap(),
            );
            idx.segment = Some(segment.clone());
            segment
        };
        assert!(matches!(
            segment.text_posting_at(&tokens[0], &[7], |_| false),
            Some(TextPostingAt::Sparse { .. })
        ));

        crate::persistence::infrastructure::composed_segment::reset_text_term_probes();
        assert_eq!(run(&e, query), expected, "layered BM25 score bits");
        let probes = crate::persistence::infrastructure::composed_segment::text_term_probes();
        let distinct = tokens.iter().collect::<BTreeSet<_>>().len() as u64;
        assert!(probes > 0, "the query must read the layered index");
        assert!(
            probes <= distinct,
            "planning probed {probes} postings for {distinct} distinct terms"
        );
        assert!(
            matches!(
                segment.text_posting_at(&tokens[0], &[7], |_| false),
                Some(TextPostingAt::Sparse { .. })
            ),
            "a one-document filter must not fill the whole-posting cache just to plan its match"
        );
        assert!(run(&e, wrong_query).is_empty());
        assert_eq!(run(&e, not_query), expected_not);
    }

    #[test]
    fn sparse_layered_topk_skips_materializing_match_estimates() {
        sparse_layered_match_does_not_materialize_for_planning(false, Analyzer::WhitespaceLower);
    }

    #[test]
    fn sparse_layered_general_and_skips_materializing_match_estimates() {
        sparse_layered_match_does_not_materialize_for_planning(true, Analyzer::WhitespaceLower);
    }

    #[test]
    fn sparse_layered_ngram_topk_skips_materializing_match_estimates() {
        sparse_layered_match_does_not_materialize_for_planning(false, Analyzer::Ngram);
    }

    #[test]
    fn sparse_layered_ngram_general_and_skips_materializing_match_estimates() {
        sparse_layered_match_does_not_materialize_for_planning(true, Analyzer::Ngram);
    }

    #[test]
    fn sparse_filter_planning_checks_actual_hash_collision_count() {
        for n in [SPARSE_CANDIDATE_MAX as u32, SPARSE_CANDIDATE_MAX as u32 + 1] {
            let e = seed(n);
            for i in 0..n {
                index(&e, &format!("d{i}"), sig(0), &body(i));
            }
            let filter = hamming(0, 0);
            // The absent term has estimate zero. Only a truly bounded set may
            // skip this estimate; above the bound the original match driver wins.
            let absent = matchq("neverindexedpredicate");
            let state = e.state.read().unwrap();
            let coll = state.collections.get("c").unwrap();
            let plan = plan_filter_candidates(coll, &[&filter], &[], &[&absent]).unwrap();
            if u64::from(n) <= SPARSE_CANDIDATE_MAX {
                let ids = plan
                    .expect("bounded actual candidates")
                    .resolve(coll, &[&filter], &[])
                    .unwrap();
                assert_eq!(ids.len(), u64::from(n));
            } else {
                assert!(
                    plan.is_none(),
                    "large collisions must retain the original text estimate"
                );
            }
            drop(state);
            assert!(run(&e, QueryNode::And(vec![filter.clone(), absent])).is_empty());
            let text = matchq("7");
            let exact = run(&e, QueryNode::And(vec![filter, text.clone()]));
            let fallback = run(&e, QueryNode::And(vec![hamming(0, 1), text]));
            assert_eq!(exact, fallback);
            assert_eq!(exact.len(), 1);
            assert!(exact.contains_key("d7"));
        }
    }
}

// ---------------------------------------------------------------------------
// Dual-path diff test: a flat-cpu Vector field's exact kNN scan served from an
// mmap'd segment must return IDENTICAL top-k (eids + byte-identical distances)
// as the in-RAM scan (Stage 2 Phase 2d).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod segment_vector_diff_tests {
    use super::*;
    use crate::shared_kernel::types::schema::{VectorBackend, VectorMetric};
    use proptest::prelude::*;
    use std::sync::Arc;

    const DIM: usize = 8;

    fn vec_fieldspec(metric: VectorMetric) -> FieldSpec {
        FieldSpec {
            field_type: FieldType::Vector,
            analyzer: None,
            multi: None,
            dim: Some(DIM as u32),
            metric: Some(metric),
            // The slice is FlatCpu/exact only — HNSW is untouched.
            backend: Some(VectorBackend::FlatCpu),
            quantize: None,
        }
    }

    fn schema(metric: VectorMetric) -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("emb".into(), vec_fieldspec(metric));
        CreateCollectionRequest { fields }
    }

    fn index_vec(e: &Engine, eid: &str, v: &[f32]) {
        e.index(
            "c",
            IndexRequest {
                items: vec![crate::shared_kernel::types::document::IndexItem {
                    external_id: eid.into(),
                    field: "emb".into(),
                    value: FieldValue::Vector(v.to_vec()),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
    }

    fn knn(query: Vec<f32>, k: u32) -> SearchRequest {
        SearchRequest {
            query: QueryNode::Knn(crate::shared_kernel::types::query::KnnQuery {
                field: "emb".into(),
                vector: query,
                k,
            }),
            limit: k,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    /// Ordered (eid, score_bits) pairs — the kNN result is RANKED, so order is
    /// part of the contract; scores compared as exact f32 bits.
    fn run(e: &Engine, query: Vec<f32>, k: u32) -> Vec<(String, u32)> {
        e.search("c", knn(query, k))
            .unwrap()
            .hits
            .into_iter()
            .map(|h| (h.external_id, h.score.to_bits()))
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(120))]

        /// PATH A (segment OFF, scan reads in-RAM `FlatVecs::data`) must equal
        /// PATH B (the corpus sealed to an f32 mmap segment, scan reads each
        /// row zero-copy off the page) — IDENTICAL ranked top-k, eids and
        /// byte-identical distances (the f32 bits on disk are the same bits).
        #[test]
        fn knn_segment_matches_live(
            raw in proptest::collection::vec(
                proptest::collection::vec(-4.0f32..4.0, DIM..=DIM),
                1..40,
            ),
            qraw in proptest::collection::vec(-4.0f32..4.0, DIM..=DIM),
            k in 1u32..12,
            metric in prop::sample::select(vec![
                VectorMetric::L2, VectorMetric::Cosine, VectorMetric::Dot,
            ]),
        ) {
            // --- PATH A: build the flat-cpu corpus, run kNN (segment OFF). ---
            let e = Arc::new(Engine::new());
            e.create_collection("c", schema(metric)).unwrap();
            for (i, v) in raw.iter().enumerate() {
                index_vec(&e, &format!("d{i}"), v);
            }
            let a = run(&e, qraw.clone(), k);

            // --- PATH B: seal `emb` to a segment, flip it ON, rerun. ---
            let dir = tempfile::tempdir().unwrap();
            let sealed = e.__seal_vector_field_to_segment("c", "emb", dir.path()).unwrap();
            prop_assert_eq!(sealed as usize, raw.len(), "all vectors sealed");
            let b = run(&e, qraw, k);

            // IDENTICAL ranked top-k: same eids in the same order, byte-exact scores.
            prop_assert_eq!(a, b, "kNN top-k diverged after seal");
        }
    }

    /// A direct, planner-free check: after sealing, the flat buffer's in-RAM
    /// `data` is dropped and every row is served from the segment, yet a kNN
    /// scan returns the same ranked neighbours as before the seal.
    #[test]
    fn knn_served_from_segment_after_seal() {
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema(VectorMetric::L2)).unwrap();
        // Points on a 1-D ray so the nearest order is deterministic.
        for i in 0..10usize {
            let mut v = vec![0.0f32; DIM];
            v[0] = i as f32;
            index_vec(&e, &format!("p{i}"), &v);
        }
        let mut q = vec![0.0f32; DIM];
        q[0] = 0.0;
        let before = run(&e, q.clone(), 5);

        let dir = tempfile::tempdir().unwrap();
        let n = e
            .__seal_vector_field_to_segment("c", "emb", dir.path())
            .unwrap();
        assert_eq!(n, 10);

        let after = run(&e, q, 5);
        assert_eq!(before, after, "kNN diverged when served from the segment");
        // Nearest to [0,..] is p0, then p1, ... (L2 on the ray).
        let eids: Vec<String> = after.iter().map(|(e, _)| e.clone()).collect();
        assert_eq!(eids, ["p0", "p1", "p2", "p3", "p4"]);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_profile_is_opt_in_and_keeps_only_aggregate_counts_and_timings() {
        let disabled = RecoveryProfile::new(false);
        disabled.collection_opened(Duration::from_millis(9));
        assert!(
            disabled.snapshot().is_none(),
            "disabled recovery must not report"
        );

        let profile = RecoveryProfile::for_test();
        profile.collection_opened(Duration::from_millis(3));
        profile.collection_opened(Duration::from_millis(9));
        profile.vector_opened(
            crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
            Duration::from_millis(2),
        );
        profile.vector_opened(
            crate::shared_kernel::types::schema::VectorBackend::HnswCpu,
            Duration::from_millis(4),
        );
        profile.coverage_rebuilt(Duration::from_millis(5));

        let report = profile.snapshot().expect("explicit opt-in must report");
        assert_eq!(report.collection_open_count, 2);
        assert_eq!(report.collection_open_total_ms, 12);
        assert_eq!(report.collection_open_max_ms, 9);
        assert_eq!(report.vector_flat_open_count, 1);
        assert_eq!(report.vector_flat_open_ms, 2);
        assert_eq!(report.vector_hnsw_open_count, 1);
        assert_eq!(report.vector_hnsw_open_ms, 4);
        assert_eq!(report.coverage_rebuild_ms, 5);
    }

    #[test]
    fn recovery_phase_start_runs_work_for_enabled_and_disabled_profiles() {
        let disabled = RecoveryProfile::new(false);
        let mut disabled_work = false;
        let disabled_result = disabled.phase_start(RecoveryPhase::CheckpointHnswGraph, || {
            disabled_work = true;
            7
        });
        assert_eq!(disabled_result, 7);
        assert!(disabled_work);
        assert!(disabled.snapshot().is_none());

        let enabled = RecoveryProfile::for_test();
        let mut enabled_work = false;
        let enabled_result = enabled.phase_start(RecoveryPhase::CheckpointHnswGraph, || {
            enabled_work = true;
            11
        });
        assert_eq!(enabled_result, 11);
        assert!(enabled_work);
        assert!(enabled.snapshot().is_some());
    }

    #[test]
    fn hash_snapshot_keeps_sealed_base_values() {
        let dir = tempfile::tempdir().unwrap();
        let mut interner = Interner::default();
        let id = interner.intern("document");
        let mut hash = HashIndex::default();
        hash.forward.insert(id, 42);
        let mut field = FieldIndex::Hash(hash);
        field
            .seal_to_segment("sig", dir.path(), 1, 7, &|_| true)
            .unwrap();
        let FieldIndexSnapshot::Hash { forward, .. } = field.to_snapshot(&interner, &[id]).unwrap()
        else {
            panic!("hash snapshot")
        };
        assert_eq!(
            forward.get("document"),
            Some(&42),
            "sealed hash value missing from backup"
        );
    }

    use crate::shared_kernel::types::query::{DuplicatedQuery, ExistsQuery};

    fn build_users_schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert(
            "bio".into(),
            FieldSpec {
                field_type: FieldType::Text,
                analyzer: Some(Analyzer::WhitespaceLower),
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        fields.insert(
            "email".into(),
            FieldSpec {
                field_type: FieldType::Keyword,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        fields.insert(
            "tags".into(),
            FieldSpec {
                field_type: FieldType::Set,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        fields.insert(
            "age".into(),
            FieldSpec {
                field_type: FieldType::Number,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        CreateCollectionRequest { fields }
    }

    fn item(
        eid: &str,
        field: &str,
        value: FieldValue,
    ) -> crate::shared_kernel::types::document::IndexItem {
        crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: field.into(),
            value,
            version: None,
        }
    }

    fn record_cost_schema(
        vector_backend: Option<crate::shared_kernel::types::schema::VectorBackend>,
    ) -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert(
            "text".into(),
            FieldSpec {
                field_type: FieldType::Text,
                analyzer: Some(Analyzer::Ngram),
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        fields.insert(
            "email".into(),
            FieldSpec {
                field_type: FieldType::Keyword,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        if let Some(backend) = vector_backend {
            fields.insert(
                "vector".into(),
                FieldSpec {
                    field_type: FieldType::Vector,
                    analyzer: None,
                    multi: None,
                    dim: Some(3),
                    metric: Some(crate::shared_kernel::types::schema::VectorMetric::Cosine),
                    backend: Some(backend),
                    quantize: None,
                },
            );
        }
        CreateCollectionRequest { fields }
    }

    fn estimated_total(record: crate::ingest::domain::change_record_cost::RecordCost) -> usize {
        record.active + record.frozen + record.prepublish
    }

    fn ready_record_cost(
        engine: &Engine,
        entry: &crate::shared_kernel::log_entry::RaftLogEntry,
    ) -> crate::ingest::domain::change_record_cost::RecordCost {
        match engine.estimate_record_cost(entry) {
            crate::ingest::domain::change_record_cost::RecordEstimate::Ready(cost) => cost,
            retained => panic!("expected a decidable record cost, got {retained:?}"),
        }
    }

    #[test]
    fn record_cost_uses_engine_schema_and_known_external_id() {
        let engine = Engine::new();
        engine
            .create_collection("c", record_cost_schema(None))
            .unwrap();
        let entry = crate::shared_kernel::log_entry::RaftLogEntry::Index {
            collection_id: "c".into(),
            req: IndexRequest {
                items: vec![item(
                    "doc",
                    "email",
                    FieldValue::String("a@example.test".into()),
                )],
                request_id: None,
            },
        };
        let new_document = ready_record_cost(&engine, &entry);
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
        let existing_document = ready_record_cost(&engine, &entry);

        assert!(
            estimated_total(new_document) > estimated_total(existing_document),
            "a new external ID must include interner and coverage ownership"
        );
    }

    #[test]
    fn record_cost_charges_replace_tombstones_and_actual_unindex_coverage() {
        let engine = Engine::new();
        engine
            .create_collection("c", record_cost_schema(None))
            .unwrap();
        engine
            .index(
                "c",
                IndexRequest {
                    items: vec![
                        item("doc", "email", FieldValue::String("a@example.test".into())),
                        item("doc", "text", FieldValue::String("alphabet".into())),
                    ],
                    request_id: None,
                },
            )
            .unwrap();

        let replace = crate::shared_kernel::log_entry::RaftLogEntry::ReplaceDocs {
            collection_id: "c".into(),
            req: ReplaceDocsRequest {
                docs: vec![crate::shared_kernel::types::document::ReplaceDocItem {
                    external_id: "doc".into(),
                    version: None,
                    fields: BTreeMap::from([(
                        "email".into(),
                        FieldValue::String("next@example.test".into()),
                    )]),
                }],
            },
        };
        let replace_cost = ready_record_cost(&engine, &replace);
        let unindex = crate::shared_kernel::log_entry::RaftLogEntry::UnindexDocs {
            collection_id: "c".into(),
            req: crate::shared_kernel::types::document::BatchUnindexDocsRequest {
                external_ids: vec!["doc".into()],
            },
        };
        let covered_unindex = ready_record_cost(&engine, &unindex);
        let missing_unindex = ready_record_cost(
            &engine,
            &crate::shared_kernel::log_entry::RaftLogEntry::UnindexDocs {
                collection_id: "c".into(),
                req: crate::shared_kernel::types::document::BatchUnindexDocsRequest {
                    external_ids: vec!["missing".into()],
                },
            },
        );

        assert!(
            estimated_total(replace_cost) > 0,
            "omitted text must add a tombstone cost"
        );
        assert!(
            estimated_total(covered_unindex) > estimated_total(missing_unindex),
            "unindex must use current coverage instead of a caller supplied field count"
        );
    }

    #[test]
    fn record_cost_skips_deduplicated_requests_and_excludes_vector_graph() {
        let flat = Engine::new();
        let hnsw = Engine::new();
        flat.create_collection(
            "c",
            record_cost_schema(Some(
                crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
            )),
        )
        .unwrap();
        hnsw.create_collection(
            "c",
            record_cost_schema(Some(
                crate::shared_kernel::types::schema::VectorBackend::HnswCpu,
            )),
        )
        .unwrap();
        let vector_entry =
            |collection_id: &str| crate::shared_kernel::log_entry::RaftLogEntry::Index {
                collection_id: collection_id.into(),
                req: IndexRequest {
                    items: vec![item(
                        "doc",
                        "vector",
                        FieldValue::Vector(vec![1.0, 2.0, 3.0]),
                    )],
                    request_id: None,
                },
            };
        assert_eq!(
            ready_record_cost(&flat, &vector_entry("c")),
            ready_record_cost(&hnsw, &vector_entry("c")),
            "pending vector payload cost must not charge an HNSW graph"
        );

        let request_id = "request-1";
        let valid_request = IndexRequest {
            items: vec![item(
                "doc",
                "vector",
                FieldValue::Vector(vec![1.0, 2.0, 3.0]),
            )],
            request_id: Some(request_id.into()),
        };
        flat.index("c", valid_request.clone()).unwrap();
        let duplicate = crate::shared_kernel::log_entry::RaftLogEntry::Index {
            collection_id: "c".into(),
            req: valid_request,
        };
        assert_eq!(
            ready_record_cost(&flat, &duplicate),
            crate::ingest::domain::change_record_cost::RecordCost::default()
        );
    }

    #[test]
    fn record_cost_ngram_bound_covers_public_tokenizer_for_ascii_and_unicode() {
        for input in ["abcd", "İstanbul 42"] {
            let actual = tokenize::tokenize(input, Analyzer::Ngram);
            let bound = crate::ingest::domain::change_record_cost::text_upper_bound::text_upper_bound(
                input,
                crate::ingest::domain::change_record_cost::text_upper_bound::AnalyzerKind::Ngram,
                tokenize::DEFAULT_NGRAM_MIN,
                tokenize::DEFAULT_NGRAM_MAX,
            )
            .unwrap();
            assert!(bound.terms >= actual.len());
            assert!(bound.total_utf8_bytes >= actual.iter().map(|term| term.len()).sum::<usize>());
        }
    }

    #[test]
    fn frozen_checkpoint_replays_the_same_captured_payload_after_live_mutation() {
        let engine = Engine::new();
        engine.create_collection("c", build_users_schema()).unwrap();
        engine
            .index(
                "c",
                IndexRequest {
                    items: vec![
                        item(
                            "doc",
                            "bio",
                            FieldValue::String("captured biography".into()),
                        ),
                        item(
                            "doc",
                            "email",
                            FieldValue::String("captured@example.test".into()),
                        ),
                        item(
                            "doc",
                            "tags",
                            FieldValue::StringList(vec!["captured".into()]),
                        ),
                        item("doc", "age", FieldValue::Number(42.0)),
                    ],
                    request_id: None,
                },
            )
            .unwrap();
        // A restored legacy collection has no complete change journal. Keep
        // this test on the full-base retry path; fresh journals have their own
        // sparse ownership and generation round-trip tests below.
        engine.restore(engine.snapshot().unwrap()).unwrap();
        let frozen = engine.freeze_checkpoint_collections(None).unwrap();
        let first = tempfile::tempdir().unwrap();
        frozen.write(first.path(), 7).unwrap();

        engine
            .index(
                "c",
                IndexRequest {
                    items: vec![item(
                        "doc",
                        "email",
                        FieldValue::String("later@example.test".into()),
                    )],
                    request_id: None,
                },
            )
            .unwrap();
        let replay = tempfile::tempdir().unwrap();
        frozen.write(replay.path(), 7).unwrap();

        let first_engine = Engine::new();
        first_engine.reopen_from_segment_dir(first.path()).unwrap();
        let replay_engine = Engine::new();
        replay_engine
            .reopen_from_segment_dir(replay.path())
            .unwrap();
        for checkpoint in [&first_engine, &replay_engine] {
            let result = checkpoint
                .search(
                    "c",
                    SearchRequest {
                        query: QueryNode::Term(crate::shared_kernel::types::query::TermQuery {
                            field: "email".into(),
                            value: FieldValue::String("captured@example.test".into()),
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
                .unwrap();
            assert_eq!(result.hits.len(), 1);
            assert_eq!(result.hits[0].external_id, "doc");
            let text = checkpoint
                .search(
                    "c",
                    SearchRequest {
                        query: QueryNode::Match(crate::shared_kernel::types::query::MatchQuery {
                            field: "bio".into(),
                            text: "captured".into(),
                            op: crate::shared_kernel::types::query::MatchOp::And,
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
                .unwrap();
            assert_eq!(text.hits.len(), 1, "replayed text field missing");
            for field in ["email", "tags", "age"] {
                let exists = checkpoint
                    .search(
                        "c",
                        SearchRequest {
                            query: QueryNode::Exists(ExistsQuery {
                                field: field.into(),
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
                    .unwrap();
                assert_eq!(exists.hits.len(), 1, "replayed field `{field}` missing");
            }
        }
    }

    #[test]
    fn initial_sparse_capture_requires_complete_journal_provenance() {
        let engine = Engine::new();
        engine.create_collection("c", build_users_schema()).unwrap();
        engine
            .index(
                "c",
                IndexRequest {
                    items: vec![item(
                        "legacy",
                        "email",
                        FieldValue::String("legacy-value".into()),
                    )],
                    request_id: None,
                },
            )
            .unwrap();
        engine.restore(engine.snapshot().unwrap()).unwrap();
        engine
            .prepare_checkpoint_namespace(
                std::path::Path::new("/unused-test-checkpoint-namespace"),
                1,
            )
            .unwrap();
        let frozen = engine.freeze_checkpoint_collections(None).unwrap();
        assert!(
            frozen.capture.initial_sparse.is_empty(),
            "a missing origin must not imply journal completeness"
        );
        assert!(
            matches!(&frozen.files[0].1, FrozenCollectionFiles::Base { eids, .. } if !eids.is_empty())
        );

        let fresh = Engine::new();
        fresh.create_collection("c", build_users_schema()).unwrap();
        fresh.drop_field("c", "age").unwrap();
        let frozen = fresh.freeze_checkpoint_collections(None).unwrap();
        assert!(
            frozen.capture.initial_sparse.is_empty(),
            "schema changes must invalidate initial journal provenance"
        );
        assert!(matches!(
            &frozen.files[0].1,
            FrozenCollectionFiles::Base { .. }
        ));
    }

    #[test]
    fn first_checkpoint_freeze_does_not_clone_live_field_rows() {
        for backend in [
            crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
            crate::shared_kernel::types::schema::VectorBackend::HnswCpu,
        ] {
            let engine = Engine::new();
            let mut schema = build_users_schema();
            let mut vector = record_cost_schema(Some(backend));
            schema
                .fields
                .insert("vector".into(), vector.fields.remove("vector").unwrap());
            let mut hash = schema.fields["email"].clone();
            hash.field_type = FieldType::Hash;
            schema.fields.insert("sig".into(), hash);
            engine.create_collection("c", schema).unwrap();
            let fields = [
                ("email", FieldValue::String("captured".into())),
                ("bio", FieldValue::String("captured captured text".into())),
                (
                    "tags",
                    serde_json::from_value(serde_json::json!(["one", "two"])).unwrap(),
                ),
                ("age", FieldValue::Number(42.0)),
                ("sig", FieldValue::String("000000000000002a".into())),
                (
                    "vector",
                    serde_json::from_value(serde_json::json!([1.0, 0.0, 0.0])).unwrap(),
                ),
            ];
            engine
                .index(
                    "c",
                    IndexRequest {
                        items: fields
                            .iter()
                            .map(|(field, value)| item("sparse-first", field, value.clone()))
                            .collect(),
                        request_id: None,
                    },
                )
                .unwrap();
            let frozen = engine.freeze_checkpoint_collections(None).unwrap();
            assert!(
                frozen
                    .files
                    .iter()
                    .all(|(_, files)| !matches!(files, FrozenCollectionFiles::Base { .. })),
                "fresh checkpoint must freeze journal handles instead of cloning live field rows"
            );
            engine.delete("c", "sparse-first", None).unwrap();
            let output = tempfile::tempdir().unwrap();
            let capture = frozen.write(output.path(), 0).unwrap();
            for (field, _) in fields {
                let saved = capture.field_deltas["c"][field][0].1.as_ref().unwrap();
                let row = capture.frozen_changes["c"]
                    .row(field, "sparse-first")
                    .unwrap();
                assert!(
                    saved.same_identity(row.value().unwrap()),
                    "first checkpoint must retain the captured row handle for {field}"
                );
            }
        }
    }

    #[test]
    fn checkpoint_freeze_exchanges_journal_for_all_field_backends() {
        for backend in [
            crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
            crate::shared_kernel::types::schema::VectorBackend::HnswCpu,
        ] {
            let engine = std::sync::Arc::new(Engine::new());
            let mut schema = build_users_schema();
            let mut vector = record_cost_schema(Some(backend));
            schema
                .fields
                .insert("vector".into(), vector.fields.remove("vector").unwrap());
            let mut hash = schema.fields["email"].clone();
            hash.field_type = FieldType::Hash;
            schema.fields.insert("sig".into(), hash);
            engine.create_collection("c", schema).unwrap();
            let directory = tempfile::tempdir().unwrap();
            let store =
                crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore::new(
                    directory.path(),
                )
                .unwrap();
            store.save(&engine, 0).unwrap();
            let fields = [
                ("email", FieldValue::String("captured".into())),
                ("bio", FieldValue::String("captured captured text".into())),
                (
                    "tags",
                    serde_json::from_value(serde_json::json!(["one", "two"])).unwrap(),
                ),
                (
                    "age",
                    serde_json::from_value(serde_json::json!(42)).unwrap(),
                ),
                ("sig", FieldValue::String("000000000000002a".into())),
                (
                    "vector",
                    serde_json::from_value(serde_json::json!([1.0, 0.0, 0.0])).unwrap(),
                ),
            ];
            engine
                .index(
                    "c",
                    IndexRequest {
                        items: fields
                            .iter()
                            .map(|(name, value)| item("sparse-1000000", name, value.clone()))
                            .collect(),
                        request_id: None,
                    },
                )
                .unwrap();
            let lineage = {
                let state = engine.state.read().unwrap();
                let coll = &state.collections["c"];
                for (field, _) in &fields {
                    assert!(coll
                        .change_journal
                        .active_revision(field, "sparse-1000000")
                        .is_some());
                }
                coll.checkpoint_lineage
                    .as_ref()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .to_path_buf()
            };
            let frozen = engine
                .freeze_checkpoint_collections(Some(&lineage))
                .unwrap();
            {
                let state = engine.state.read().unwrap();
                for (field, _) in &fields {
                    assert_eq!(
                        state.collections["c"]
                            .change_journal
                            .active_revision(field, "sparse-1000000"),
                        None,
                        "checkpoint must exchange active journal ownership during capture"
                    );
                }
            }
            engine.delete("c", "sparse-1000000", None).unwrap();
            let written = tempfile::tempdir().unwrap();
            let captured = frozen.write(written.path(), 1).unwrap();
            let values = &captured.field_deltas["c"];
            for (field, _) in &fields {
                assert!(
                    values[*field][0].1.as_ref().unwrap().same_identity(
                        captured.frozen_changes["c"]
                            .row(field, "sparse-1000000")
                            .unwrap()
                            .value()
                            .unwrap(),
                    ),
                    "encoding must borrow the frozen typed payload without copying it"
                );
            }
            assert!(
                matches!(values["email"][0].1.as_deref(), Some(CheckpointValue::Keyword(value)) if value == "captured")
            );
            assert!(
                matches!(values["age"][0].1.as_deref(), Some(CheckpointValue::Number(value)) if *value == 42.0)
            );
            assert!(matches!(
                values["sig"][0].1.as_deref(),
                Some(CheckpointValue::Hash(42))
            ));
            assert!(
                matches!(values["tags"][0].1.as_deref(), Some(CheckpointValue::Set(value)) if value == &["one", "two"])
            );
            assert!(
                matches!(values["vector"][0].1.as_deref(), Some(CheckpointValue::Vector(value)) if value == &[1.0, 0.0, 0.0])
            );
            assert!(
                matches!(values["bio"][0].1.as_deref(), Some(CheckpointValue::Text { doc_len: 3, tokens }) if tokens.get("captured") == Some(&2))
            );
            let deleted = engine
                .freeze_checkpoint_collections(Some(&lineage))
                .unwrap();
            let deleted_dir = tempfile::tempdir().unwrap();
            let deleted_capture = deleted.write(deleted_dir.path(), 2).unwrap();
            for (field, _) in &fields {
                assert!(
                    deleted_capture.field_deltas["c"][*field][0].1.is_none(),
                    "delete must be an explicit frozen tombstone for {field}"
                );
            }
        }
    }

    #[test]
    fn field_dirty_rows_are_per_field_latest_and_revision_safe() {
        let mut schema = BTreeMap::new();
        for name in ["left", "right"] {
            schema.insert(
                name.to_owned(),
                FieldSpec {
                    field_type: FieldType::Keyword,
                    analyzer: None,
                    multi: None,
                    dim: None,
                    metric: None,
                    backend: None,
                    quantize: None,
                },
            );
        }
        schema.insert(
            "number".to_owned(),
            FieldSpec {
                field_type: FieldType::Number,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        let mut collection = Collection::new(schema).unwrap();
        collection.mark_field_dirty("left", "same").unwrap();
        let captured = collection.field_dirty_snapshot();
        collection.mark_field_dirty("left", "same").unwrap();
        collection.mark_field_dirty("right", "same").unwrap();
        collection.mark_field_dirty("left", "other").unwrap();

        assert_eq!(collection.field_dirty_len("left"), 2);
        assert_eq!(collection.field_dirty_len("right"), 1);
        collection.mark_field_dirty("number", "same").unwrap();
        assert_eq!(collection.field_dirty_len("number"), 1);
        assert!(!collection.requires_full_checkpoint());
        // A field not handled by this capture format must take a full checkpoint.
        collection
            .mark_field_dirty("unknown-field", "same")
            .unwrap();
        assert!(collection.requires_full_checkpoint());
        collection.acknowledge_field_dirty(&captured);
        assert_eq!(collection.field_dirty_len("left"), 2);
        assert_eq!(collection.field_dirty_len("right"), 1);
    }

    /// Walk every page through the returned cursors; assert the
    /// concatenation equals one exhaustive query (order included), with no
    /// duplicates or gaps — the keyset-pagination contract.
    fn walk_pages(e: &Engine, base: &SearchRequest) -> Vec<String> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..1000 {
            let mut req = base.clone();
            req.cursor = cursor.clone();
            let resp = e.search("users", req).unwrap();
            let n = resp.hits.len();
            out.extend(resp.hits.into_iter().map(|h| h.external_id));
            match resp.cursor {
                Some(c) => cursor = Some(c),
                None => return out,
            }
            if n == 0 {
                return out;
            }
        }
        panic!("cursor never exhausted");
    }

    #[test]
    fn unsupported_text_sort_is_rejected_instead_of_silent_score_ranking() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![item("u1", "bio", FieldValue::String("rust".into()))],
                request_id: None,
            },
        )
        .unwrap();
        let err = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Match(crate::shared_kernel::types::query::MatchQuery {
                        field: "bio".into(),
                        text: "rust".into(),
                        op: MatchOp::And,
                    }),
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
                        field: "bio".into(),
                        order: SortOrder::Asc,
                        missing: SortMissing::Exclude,
                    }]),
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<StorageError>(),
                Some(StorageError::UnsupportedSort(_))
            ),
            "text sort must be a 400-class unsupported sort error: {err:?}"
        );
    }

    #[test]
    fn keyword_sort_keyset_pagination_walks_lexicographically() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let docs = [
            ("u00", "delta", 4.0),
            ("u01", "alpha", 1.0),
            ("u02", "charlie", 3.0),
            ("u03", "alpha", 2.0),
            ("u04", "bravo", 5.0),
        ];
        let mut items = Vec::new();
        for (eid, email, age) in docs {
            items.push(item(eid, "email", FieldValue::String(email.into())));
            items.push(item(eid, "age", FieldValue::Number(age)));
        }
        e.index(
            "users",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();

        for (order, expected) in [
            (SortOrder::Asc, vec!["u01", "u03", "u04", "u02", "u00"]),
            (SortOrder::Desc, vec!["u00", "u02", "u04", "u01", "u03"]),
        ] {
            let base = SearchRequest {
                query: QueryNode::Range(crate::shared_kernel::types::query::RangeQuery {
                    field: "age".into(),
                    gt: None,
                    gte: None,
                    lt: None,
                    lte: None,
                }),
                limit: 2,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
                    field: "email".into(),
                    order,
                    missing: SortMissing::Exclude,
                }]),
                track_total: true,
                collapse: None,
            };
            let first = e.search("users", base.clone()).unwrap();
            match parse_page_cursor(first.cursor.as_deref().expect("more pages")).unwrap() {
                PageCursor::SortValuesKeyset { values, .. } => {
                    assert!(matches!(values.as_slice(), [SortValue::Keyword(_)]));
                }
                other => panic!("expected keyword sort cursor, got {other:?}"),
            }
            assert_eq!(walk_pages(&e, &base), expected, "order {order:?}");
        }
    }

    #[test]
    fn composite_keyword_number_sort_keyset_paginates_to_oracle() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let docs = [
            ("u00", "todo", 10.0),
            ("u01", "done", 20.0),
            ("u02", "todo", 30.0),
            ("u03", "done", 15.0),
            ("u04", "blocked", 40.0),
            ("u05", "todo", 30.0),
        ];
        let mut items = Vec::new();
        for (eid, status, age) in docs {
            items.push(item(eid, "email", FieldValue::String(status.into())));
            items.push(item(eid, "age", FieldValue::Number(age)));
        }
        e.index(
            "users",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
        let base = SearchRequest {
            query: QueryNode::Range(crate::shared_kernel::types::query::RangeQuery {
                field: "age".into(),
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }),
            limit: 2,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: Some(vec![
                crate::shared_kernel::types::query::SortSpec {
                    field: "email".into(),
                    order: SortOrder::Asc,
                    missing: SortMissing::Exclude,
                },
                crate::shared_kernel::types::query::SortSpec {
                    field: "age".into(),
                    order: SortOrder::Desc,
                    missing: SortMissing::Exclude,
                },
            ]),
            track_total: true,
            collapse: None,
        };
        let first = e.search("users", base.clone()).unwrap();
        match parse_page_cursor(first.cursor.as_deref().expect("more pages")).unwrap() {
            PageCursor::SortValuesKeyset { values, .. } => {
                assert!(matches!(
                    values.as_slice(),
                    [SortValue::Keyword(_), SortValue::Number(_)]
                ));
            }
            other => panic!("expected composite sort cursor, got {other:?}"),
        }
        assert_eq!(
            walk_pages(&e, &base),
            vec!["u04", "u01", "u03", "u02", "u05", "u00"]
        );
    }

    #[test]
    fn keyword_sort_paginates_over_sealed_segment_plus_live_tail() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let mut sealed = Vec::new();
        for (eid, email, age) in [
            ("u00", "delta", 4.0),
            ("u01", "alpha", 1.0),
            ("u02", "charlie", 3.0),
        ] {
            sealed.push(item(eid, "email", FieldValue::String(email.into())));
            sealed.push(item(eid, "age", FieldValue::Number(age)));
        }
        e.index(
            "users",
            IndexRequest {
                items: sealed,
                request_id: None,
            },
        )
        .unwrap();
        e.__seal_keyword_field_to_segment("users", "email", dir.path())
            .unwrap();
        let mut tail = Vec::new();
        for (eid, email, age) in [("u03", "alpha", 2.0), ("u04", "bravo", 5.0)] {
            tail.push(item(eid, "email", FieldValue::String(email.into())));
            tail.push(item(eid, "age", FieldValue::Number(age)));
        }
        e.index(
            "users",
            IndexRequest {
                items: tail,
                request_id: None,
            },
        )
        .unwrap();

        let base = SearchRequest {
            query: QueryNode::Range(crate::shared_kernel::types::query::RangeQuery {
                field: "age".into(),
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }),
            limit: 2,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
                field: "email".into(),
                order: SortOrder::Asc,
                missing: SortMissing::Exclude,
            }]),
            track_total: true,
            collapse: None,
        };
        assert_eq!(
            walk_pages(&e, &base),
            vec!["u01", "u03", "u04", "u02", "u00"]
        );
    }

    #[test]
    fn sorted_keyset_pagination_walks_exhaustively_with_ties() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        // 97 docs, age = i % 10 → heavy duplicate sort keys exercise the
        // (value, docid) tie-break across page boundaries.
        let items: Vec<_> = (0..97)
            .flat_map(|i| {
                vec![item(
                    &format!("u{i:03}"),
                    "age",
                    FieldValue::Number((i % 10) as f64),
                )]
            })
            .collect();
        e.index(
            "users",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();

        for order in [SortOrder::Asc, SortOrder::Desc] {
            let base = SearchRequest {
                query: QueryNode::Range(crate::shared_kernel::types::query::RangeQuery {
                    field: "age".into(),
                    gt: None,
                    gte: None,
                    lt: None,
                    lte: None,
                }),
                limit: 7,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
                    field: "age".into(),
                    order,
                    missing: SortMissing::Exclude,
                }]),
                track_total: true,
                collapse: None,
            };
            // One exhaustive page as the oracle.
            let mut oracle_req = base.clone();
            oracle_req.limit = 1000;
            let oracle: Vec<String> = e
                .search("users", oracle_req)
                .unwrap()
                .hits
                .into_iter()
                .map(|h| h.external_id)
                .collect();
            assert_eq!(oracle.len(), 97);

            let paged = walk_pages(&e, &base);
            assert_eq!(paged, oracle, "order {order:?}");
        }
    }

    #[test]
    fn sorted_keyset_cursor_is_v2_and_filtered_walks_match() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let mut items = Vec::new();
        for i in 0..60 {
            items.push(item(
                &format!("u{i:03}"),
                "age",
                FieldValue::Number(i as f64),
            ));
            items.push(item(
                &format!("u{i:03}"),
                "email",
                FieldValue::String(format!("{}@x.com", if i % 2 == 0 { "even" } else { "odd" })),
            ));
        }
        e.index(
            "users",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();

        // Filtered (query predicate) + sorted + paged.
        let base = SearchRequest {
            query: QueryNode::Term(crate::shared_kernel::types::query::TermQuery {
                field: "email".into(),
                value: FieldValue::String("even@x.com".into()),
            }),
            limit: 4,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
                field: "age".into(),
                order: SortOrder::Desc,
                missing: SortMissing::Exclude,
            }]),
            track_total: true,
            collapse: None,
        };
        let first = e.search("users", base.clone()).unwrap();
        // The first page of a sorted query hands out a v2 keyset cursor.
        let cursor = first.cursor.clone().expect("more pages");
        match parse_page_cursor(&cursor).expect("parseable") {
            PageCursor::SortKeyset { .. } => {}
            _ => panic!("expected a sort keyset cursor"),
        }

        let paged = walk_pages(&e, &base);
        let expected: Vec<String> = (0..60)
            .rev()
            .filter(|i| i % 2 == 0)
            .map(|i| format!("u{i:03}"))
            .collect();
        assert_eq!(paged, expected);
    }

    #[test]
    fn score_keyset_pagination_matches_full_ranking() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let items: Vec<_> = (0..45)
            .map(|i| {
                item(
                    &format!("u{i:03}"),
                    "bio",
                    FieldValue::String(format!(
                        "engineer {}",
                        if i % 3 == 0 { "rust rust" } else { "rust" }
                    )),
                )
            })
            .collect();
        e.index(
            "users",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();

        let base = SearchRequest {
            query: QueryNode::Match(crate::shared_kernel::types::query::MatchQuery {
                field: "bio".into(),
                text: "rust".into(),
                op: MatchOp::And,
            }),
            limit: 6,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        };
        let mut oracle_req = base.clone();
        oracle_req.limit = 1000;
        let oracle: Vec<String> = e
            .search("users", oracle_req)
            .unwrap()
            .hits
            .into_iter()
            .map(|h| h.external_id)
            .collect();
        assert_eq!(oracle.len(), 45);

        let first = e.search("users", base.clone()).unwrap();
        match parse_page_cursor(&first.cursor.clone().unwrap()).unwrap() {
            PageCursor::ScoreKeyset { .. } => {}
            _ => panic!("expected a score keyset cursor"),
        }
        let paged = walk_pages(&e, &base);
        assert_eq!(paged, oracle);
    }

    #[test]
    fn legacy_offset_cursor_still_pages() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let items: Vec<_> = (0..30)
            .map(|i| item(&format!("u{i:03}"), "age", FieldValue::Number(i as f64)))
            .collect();
        e.index(
            "users",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
        let req = SearchRequest {
            query: QueryNode::Range(crate::shared_kernel::types::query::RangeQuery {
                field: "age".into(),
                gt: None,
                gte: Some(RangeBound::Number(0.0)),
                lt: None,
                lte: None,
            }),
            limit: 10,
            offset: 0,
            cursor: Some(make_cursor(25)),
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        };
        let resp = e.search("users", req).unwrap();
        assert_eq!(resp.hits.len(), 5);
        assert_eq!(resp.total, 30);
        assert!(resp.cursor.is_none());
    }

    #[test]
    fn sorted_keyset_pagination_over_sealed_segment_plus_tail() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        // 50 sealed docs with duplicate keys, then a live tail of 13 more —
        // the keyset walk must seek correctly across BOTH sources.
        let items: Vec<_> = (0..50)
            .map(|i| {
                item(
                    &format!("u{i:03}"),
                    "age",
                    FieldValue::Number((i % 7) as f64),
                )
            })
            .collect();
        e.index(
            "users",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
        e.__seal_number_field_to_segment("users", "age", dir.path())
            .unwrap();
        let tail: Vec<_> = (50..63)
            .map(|i| {
                item(
                    &format!("u{i:03}"),
                    "age",
                    FieldValue::Number((i % 7) as f64),
                )
            })
            .collect();
        e.index(
            "users",
            IndexRequest {
                items: tail,
                request_id: None,
            },
        )
        .unwrap();

        for order in [SortOrder::Asc, SortOrder::Desc] {
            let base = SearchRequest {
                query: QueryNode::Range(crate::shared_kernel::types::query::RangeQuery {
                    field: "age".into(),
                    gt: None,
                    gte: None,
                    lt: None,
                    lte: None,
                }),
                limit: 5,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
                    field: "age".into(),
                    order,
                    missing: SortMissing::Exclude,
                }]),
                track_total: true,
                collapse: None,
            };
            let mut oracle_req = base.clone();
            oracle_req.limit = 1000;
            let oracle: Vec<String> = e
                .search("users", oracle_req)
                .unwrap()
                .hits
                .into_iter()
                .map(|h| h.external_id)
                .collect();
            assert_eq!(oracle.len(), 63);
            let paged = walk_pages(&e, &base);
            assert_eq!(paged, oracle, "order {order:?}");
        }
    }

    /// Deep-pagination latency proof (run explicitly, release):
    /// `cargo test -p lumen --release --lib deep_pagination_depth_invariance -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn deep_pagination_depth_invariance() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let n = 100_000;
        for chunk in (0..n).collect::<Vec<_>>().chunks(5000) {
            let items: Vec<_> = chunk
                .iter()
                .map(|i| item(&format!("u{i:06}"), "age", FieldValue::Number(*i as f64)))
                .collect();
            e.index(
                "users",
                IndexRequest {
                    items,
                    request_id: None,
                },
            )
            .unwrap();
        }
        let base = SearchRequest {
            query: QueryNode::Range(crate::shared_kernel::types::query::RangeQuery {
                field: "age".into(),
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }),
            limit: 10,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
                field: "age".into(),
                order: SortOrder::Asc,
                missing: SortMissing::Exclude,
            }]),
            track_total: false,
            collapse: None,
        };

        // Page 1, then jump a keyset cursor to depth ~50_000 and time a page.
        let t0 = std::time::Instant::now();
        let first = e.search("users", base.clone()).unwrap();
        let first_us = t0.elapsed().as_micros();
        assert_eq!(first.hits.len(), 10);

        let deep_cursor = make_sort_cursor(
            SortableF64::new(50_000.0).unwrap().bits(),
            0, // before any docid at that key
        );
        let mut deep_req = base.clone();
        deep_req.cursor = Some(deep_cursor);
        let t1 = std::time::Instant::now();
        let deep = e.search("users", deep_req).unwrap();
        let deep_us = t1.elapsed().as_micros();
        assert_eq!(deep.hits.len(), 10);
        assert_eq!(deep.hits[0].external_id, "u050000");

        // Legacy offset to the same depth for contrast.
        let mut offset_req = base.clone();
        offset_req.cursor = Some(make_cursor(50_000));
        let t2 = std::time::Instant::now();
        let via_offset = e.search("users", offset_req).unwrap();
        let offset_us = t2.elapsed().as_micros();

        eprintln!(
            "page#1 {first_us}us | keyset@50k {deep_us}us | offset@50k {offset_us}us (hits {})",
            via_offset.hits.len()
        );
        // The keyset deep page must be the same order of magnitude as page 1 —
        // depth invariance. (Loose 20x bound to survive CI jitter.)
        assert!(
            deep_us < first_us.max(1) * 20,
            "keyset deep page degraded: first={first_us}us deep={deep_us}us"
        );
    }

    #[test]
    fn duplicates_side_index_tracks_inserts_and_deletes() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("u1", "email", FieldValue::String("a@x.com".into())),
                    item("u2", "email", FieldValue::String("a@x.com".into())),
                    item("u3", "email", FieldValue::String("b@y.com".into())),
                    item("u4", "email", FieldValue::String("b@y.com".into())),
                    item("u5", "email", FieldValue::String("solo@z.com".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let groups = |min: u32| {
            e.duplicates(
                "users",
                DuplicatesRequest {
                    field: "email".into(),
                    min_group_size: min,
                    limit: 10,
                    offset: 0,
                },
            )
            .unwrap()
            .groups
        };
        let g = groups(2);
        assert_eq!(g.len(), 2);
        // Delete one of the `a@x.com` pair — the group must drop out, and the
        // side-index must not leak a stale candidate.
        e.delete("users", "u2", None).unwrap();
        let g = groups(2);
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].value, serde_json::Value::String("b@y.com".into()));
        // Re-index the deleted doc back into the pair — group returns.
        e.index(
            "users",
            IndexRequest {
                items: vec![item("u2", "email", FieldValue::String("a@x.com".into()))],
                request_id: None,
            },
        )
        .unwrap();
        assert_eq!(groups(2).len(), 2);
    }

    #[test]
    fn number_range_stats_matches_walk_on_all_bound_shapes() {
        use std::ops::Bound;
        let mut idx = NumberIndex::default();
        // Values 0.0, 1.0, ..., 99.0; value k carries k+1 docs so df != distinct.
        let mut id = 0u32;
        for k in 0..100u32 {
            let key = SortableF64::new(k as f64).unwrap();
            for _ in 0..=k {
                idx.values.entry(key).or_default().insert(id);
                id += 1;
            }
        }
        let stats = NumberRangeStats::build(&idx.values);
        let s = |x: f64| SortableF64::new(x).unwrap();
        let cases: Vec<(Bound<SortableF64>, Bound<SortableF64>)> = vec![
            (Bound::Unbounded, Bound::Unbounded),
            (Bound::Included(s(10.0)), Bound::Excluded(s(20.0))),
            (Bound::Excluded(s(10.0)), Bound::Included(s(20.0))),
            (Bound::Included(s(10.5)), Bound::Excluded(s(10.6))), // empty window
            (Bound::Unbounded, Bound::Excluded(s(0.0))),          // before first
            (Bound::Excluded(s(99.0)), Bound::Unbounded),         // after last
            (Bound::Included(s(99.0)), Bound::Included(s(99.0))), // single key
        ];
        for (lo, hi) in cases {
            let walk_distinct = idx.values.range((lo, hi)).count() as u64;
            let walk_df: u64 = idx.values.range((lo, hi)).map(|(_, s)| s.len()).sum();
            assert_eq!(
                stats.range(lo, hi),
                (walk_distinct, walk_df),
                "bounds {lo:?}..{hi:?}"
            );
            // The public estimate entry points agree with the walk too.
            assert_eq!(idx.range_df(lo, hi), walk_df, "range_df {lo:?}..{hi:?}");
            assert_eq!(
                idx.range_distinct_count(lo, hi),
                walk_distinct,
                "distinct {lo:?}..{hi:?}"
            );
        }
    }

    #[test]
    fn number_range_stats_invalidated_by_cache_clear() {
        use std::ops::Bound;
        let mut idx = NumberIndex::default();
        for k in 0..10u32 {
            let key = SortableF64::new(k as f64).unwrap();
            idx.values.entry(key).or_default().insert(k);
        }
        let all = (Bound::Unbounded, Bound::Unbounded);
        idx.build_range_stats();
        assert_eq!(idx.range_df(all.0, all.1), 10);
        // Mutate the tree the way the write path does, then clear caches —
        // the next estimate must see the new value, not the stale snapshot.
        idx.values
            .entry(SortableF64::new(100.0).unwrap())
            .or_default()
            .insert(10);
        idx.clear_keyword_range_cache();
        assert_eq!(idx.range_df(all.0, all.1), 11);
        assert_eq!(idx.range_distinct_count(all.0, all.1), 11);
    }

    #[test]
    fn create_collection_returns_version_one() {
        let e = Engine::new();
        let r = e.create_collection("users", build_users_schema()).unwrap();
        assert_eq!(r.collection_id, "users");
        assert_eq!(r.version, 1);
        assert_eq!(r.fields_count, 4);
    }

    // #1271: `:` is reserved for custom-method routes (`POST
    // /collections:search`) so it must never be a valid collection id.
    #[test]
    fn create_collection_rejects_colon_in_collection_id() {
        let e = Engine::new();
        let err = e
            .create_collection("users:search", build_users_schema())
            .unwrap_err();
        let se = err
            .downcast_ref::<StorageError>()
            .expect("StorageError variant");
        assert!(
            matches!(se, StorageError::InvalidCollectionName(id) if id == "users:search"),
            "expected InvalidCollectionName, got {se:?}"
        );
    }

    #[test]
    fn index_and_term_search_keyword() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("u1", "email", FieldValue::String("a@x.com".into())),
                    item("u2", "email", FieldValue::String("b@y.com".into())),
                    item("u3", "email", FieldValue::String("a@x.com".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let resp = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Term(TermQuery {
                        field: "email".into(),
                        value: FieldValue::String("a@x.com".into()),
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
            .unwrap();
        assert_eq!(resp.total, 2);
        let eids: Vec<_> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
        assert_eq!(eids, vec!["u1", "u3"]);
    }

    #[test]
    fn match_query_finds_text() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item(
                        "u1",
                        "bio",
                        FieldValue::String("senior engineer in Taipei".into()),
                    ),
                    item(
                        "u2",
                        "bio",
                        FieldValue::String("designer in Hsinchu".into()),
                    ),
                    item("u3", "bio", FieldValue::String("engineer in Tokyo".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let resp = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Match(MatchQuery {
                        field: "bio".into(),
                        text: "engineer taipei".into(),
                        op: MatchOp::And,
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
            .unwrap();
        assert_eq!(resp.total, 1);
        assert_eq!(resp.hits[0].external_id, "u1");
    }

    #[test]
    fn search_result_cache_is_cleared_by_index_write() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![item(
                    "u1",
                    "bio",
                    FieldValue::String("engineer in Taipei".into()),
                )],
                request_id: None,
            },
        )
        .unwrap();

        let req = SearchRequest {
            query: QueryNode::Match(MatchQuery {
                field: "bio".into(),
                text: "engineer".into(),
                op: MatchOp::Or,
            }),
            limit: 10,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        };
        let first = e.search("users", req.clone()).unwrap();
        assert_eq!(first.total, 1);
        {
            let state = e.state.read().unwrap();
            let coll = state.collections.get("users").unwrap();
            assert_eq!(coll.search_cache.read().unwrap().len(), 1);
        }

        e.index(
            "users",
            IndexRequest {
                items: vec![item(
                    "u2",
                    "bio",
                    FieldValue::String("engineer in Tokyo".into()),
                )],
                request_id: None,
            },
        )
        .unwrap();
        {
            let state = e.state.read().unwrap();
            let coll = state.collections.get("users").unwrap();
            assert!(coll.search_cache.read().unwrap().is_empty());
        }

        let second = e.search("users", req).unwrap();
        assert_eq!(second.total, 2);
    }

    #[test]
    fn duplicate_field_in_one_index_request_is_replacement() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("u1", "bio", FieldValue::String("old token".into())),
                    item("u1", "bio", FieldValue::String("new token".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();

        let old = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Match(MatchQuery {
                        field: "bio".into(),
                        text: "old".into(),
                        op: MatchOp::Or,
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
            .unwrap();
        let new = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Match(MatchQuery {
                        field: "bio".into(),
                        text: "new".into(),
                        op: MatchOp::Or,
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
            .unwrap();

        assert_eq!(old.total, 0);
        assert_eq!(new.total, 1);
        assert_eq!(new.hits[0].external_id, "u1");
    }

    #[test]
    fn range_query_on_number() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let items = (1..=5)
            .map(|i| item(&format!("u{i}"), "age", FieldValue::Number(i as f64 * 10.0)))
            .collect();
        e.index(
            "users",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
        let resp = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Range(RangeQuery {
                        field: "age".into(),
                        gte: Some(RangeBound::Number(20.0)),
                        lt: Some(RangeBound::Number(50.0)),
                        gt: None,
                        lte: None,
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
            .unwrap();
        assert_eq!(resp.total, 3);
    }

    /// #1307 AC1: string `gt`/`gte`/`lt`/`lte` bounds on a `keyword` field
    /// (`email`) filter by byte/lexicographic comparison, matching a
    /// reference sort of the same values — the ordering ISO-8601
    /// date/datetime strings rely on for chronological sort.
    #[test]
    fn range_query_on_keyword_byte_lexicographic() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let emails = [
            "alice@example.com",
            "bob@example.com",
            "carol@example.com",
            "dave@example.com",
            "erin@example.com",
        ];
        let items = emails
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                item(
                    &format!("u{i}"),
                    "email",
                    FieldValue::String((*addr).into()),
                )
            })
            .collect();
        e.index(
            "users",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();

        let run = |gte: Option<&str>, lt: Option<&str>| {
            e.search(
                "users",
                SearchRequest {
                    query: QueryNode::Range(RangeQuery {
                        field: "email".into(),
                        gt: None,
                        gte: gte.map(|s| RangeBound::Keyword(s.into())),
                        lt: lt.map(|s| RangeBound::Keyword(s.into())),
                        lte: None,
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
        };

        // bob..dave (exclusive) → {bob, carol} = 2, matching a reference sort
        // of the same strings.
        let resp = run(Some("bob@example.com"), Some("dave@example.com"));
        assert_eq!(resp.total, 2);
        let mut hit_ids: Vec<&str> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
        hit_ids.sort();
        assert_eq!(hit_ids, vec!["u1", "u2"]);

        // Unbounded above "carol@example.com" (inclusive) → {carol, dave, erin} = 3.
        let resp = run(Some("carol@example.com"), None);
        assert_eq!(resp.total, 3);
    }

    /// #1307 AC2: a numeric bound against a non-`number` (`keyword`) field, or
    /// a string bound against a non-`keyword` (`number`) field, returns an
    /// error (mapped to 400 at the API layer, not a silent misparse or
    /// panic) rather than a result set.
    #[test]
    fn range_query_bound_type_mismatch_rejected() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("u1", "age", FieldValue::Number(30.0)),
                    item("u1", "email", FieldValue::String("a@example.com".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();

        // String bound against the `number` field `age`.
        let err = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Range(RangeQuery {
                        field: "age".into(),
                        gt: None,
                        gte: Some(RangeBound::Keyword("20".into())),
                        lt: None,
                        lte: None,
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
            .unwrap_err();
        assert!(
            err.to_string().contains("numeric bound"),
            "unexpected error: {err}"
        );

        // Numeric bound against the `keyword` field `email`.
        let err = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Range(RangeQuery {
                        field: "email".into(),
                        gt: None,
                        gte: Some(RangeBound::Number(1.0)),
                        lt: None,
                        lte: None,
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
            .unwrap_err();
        assert!(
            err.to_string().contains("string bound"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn and_combines_text_term_range() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("u1", "bio", FieldValue::String("rust engineer".into())),
                    item(
                        "u1",
                        "tags",
                        FieldValue::StringList(vec!["rust".into(), "db".into()]),
                    ),
                    item("u1", "age", FieldValue::Number(30.0)),
                    item("u2", "bio", FieldValue::String("rust engineer".into())),
                    item("u2", "tags", FieldValue::StringList(vec!["go".into()])),
                    item("u2", "age", FieldValue::Number(30.0)),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let q = QueryNode::And(vec![
            QueryNode::Match(MatchQuery {
                field: "bio".into(),
                text: "rust".into(),
                op: MatchOp::And,
            }),
            QueryNode::Term(TermQuery {
                field: "tags".into(),
                value: FieldValue::String("rust".into()),
            }),
            QueryNode::Range(RangeQuery {
                field: "age".into(),
                gte: Some(RangeBound::Number(25.0)),
                lt: Some(RangeBound::Number(40.0)),
                gt: None,
                lte: None,
            }),
        ]);
        let resp = e
            .search(
                "users",
                SearchRequest {
                    query: q,
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: None,
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap();
        assert_eq!(resp.total, 1);
        assert_eq!(resp.hits[0].external_id, "u1");
    }

    #[test]
    fn duplicates_keyword() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("u1", "email", FieldValue::String("a@x.com".into())),
                    item("u2", "email", FieldValue::String("a@x.com".into())),
                    item("u3", "email", FieldValue::String("a@x.com".into())),
                    item("u4", "email", FieldValue::String("b@y.com".into())),
                    item("u5", "email", FieldValue::String("b@y.com".into())),
                    item("u6", "email", FieldValue::String("c@z.com".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let resp = e
            .duplicates(
                "users",
                DuplicatesRequest {
                    field: "email".into(),
                    min_group_size: 2,
                    limit: 100,
                    offset: 0,
                },
            )
            .unwrap();
        assert_eq!(resp.groups.len(), 2);
        // Largest first.
        assert_eq!(resp.groups[0].external_ids.len(), 3);
        assert_eq!(resp.groups[1].external_ids.len(), 2);
    }

    #[test]
    fn duplicates_text_rejected() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let err = e
            .duplicates(
                "users",
                DuplicatesRequest {
                    field: "bio".into(),
                    min_group_size: 2,
                    limit: 10,
                    offset: 0,
                },
            )
            .unwrap_err();
        assert!(err.to_string().contains("duplicates not supported on text"));
    }

    // ----- Exists / Duplicated query primitives (DataTable composite search) -----

    fn search_ids(e: &Engine, coll: &str, query: QueryNode) -> (u64, Vec<String>) {
        let resp = e
            .search(
                coll,
                SearchRequest {
                    query,
                    limit: 100,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: None,
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap();
        let mut ids: Vec<String> = resp.hits.iter().map(|h| h.external_id.clone()).collect();
        ids.sort();
        (resp.total, ids)
    }

    #[test]
    fn exists_filters_missing_field() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("u1", "email", FieldValue::String("a@x.com".into())),
                    item("u1", "age", FieldValue::Number(30.0)),
                    // u2 carries no email — only age. Exists("email") must skip it.
                    item("u2", "age", FieldValue::Number(40.0)),
                    item("u3", "email", FieldValue::String("c@z.com".into())),
                    // u4 multi-valued set; Exists must see it via element_postings.
                    item(
                        "u4",
                        "tags",
                        FieldValue::StringList(vec!["a".into(), "b".into()]),
                    ),
                ],
                request_id: None,
            },
        )
        .unwrap();

        // Keyword field: only docs that actually hold an email value.
        let (total, ids) = search_ids(
            &e,
            "users",
            QueryNode::Exists(ExistsQuery {
                field: "email".into(),
            }),
        );
        assert_eq!(total, 2);
        assert_eq!(ids, vec!["u1", "u3"]);

        // Set field: presence via element postings.
        let (total, ids) = search_ids(
            &e,
            "users",
            QueryNode::Exists(ExistsQuery {
                field: "tags".into(),
            }),
        );
        assert_eq!(total, 1);
        assert_eq!(ids, vec!["u4"]);

        // Number field.
        let (total, ids) = search_ids(
            &e,
            "users",
            QueryNode::Exists(ExistsQuery {
                field: "age".into(),
            }),
        );
        assert_eq!(total, 2);
        assert_eq!(ids, vec!["u1", "u2"]);
    }

    #[test]
    fn exists_composes_with_boolean() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("u1", "email", FieldValue::String("a@x.com".into())),
                    item("u1", "age", FieldValue::Number(30.0)),
                    item("u2", "email", FieldValue::String("b@y.com".into())),
                    item("u2", "age", FieldValue::Number(50.0)),
                    item("u3", "age", FieldValue::Number(30.0)), // no email
                ],
                request_id: None,
            },
        )
        .unwrap();
        // has-email AND age in [25,40): u1 only (u2 too old, u3 no email).
        let q = QueryNode::And(vec![
            QueryNode::Exists(ExistsQuery {
                field: "email".into(),
            }),
            QueryNode::Range(RangeQuery {
                field: "age".into(),
                gte: Some(RangeBound::Number(25.0)),
                lt: Some(RangeBound::Number(40.0)),
                gt: None,
                lte: None,
            }),
        ]);
        let (total, ids) = search_ids(&e, "users", q);
        assert_eq!(total, 1);
        assert_eq!(ids, vec!["u1"]);

        // Inverse: missing email (NOT Exists) → u3.
        let q = QueryNode::Not(Box::new(QueryNode::Exists(ExistsQuery {
            field: "email".into(),
        })));
        let (total, ids) = search_ids(&e, "users", q);
        assert_eq!(total, 1);
        assert_eq!(ids, vec!["u3"]);
    }

    #[test]
    fn duplicated_as_query_leaf() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("u1", "email", FieldValue::String("a@x.com".into())),
                    item("u2", "email", FieldValue::String("a@x.com".into())),
                    item("u3", "email", FieldValue::String("a@x.com".into())),
                    item("u4", "email", FieldValue::String("b@y.com".into())),
                    item("u5", "email", FieldValue::String("b@y.com".into())),
                    item("u6", "email", FieldValue::String("c@z.com".into())), // unique
                ],
                request_id: None,
            },
        )
        .unwrap();

        // min_group_size defaults to >=2: every doc whose email collides.
        let (total, ids) = search_ids(
            &e,
            "users",
            QueryNode::Duplicated(DuplicatedQuery {
                field: "email".into(),
                min_group_size: 2,
            }),
        );
        assert_eq!(total, 5);
        assert_eq!(ids, vec!["u1", "u2", "u3", "u4", "u5"]);

        // Raise the threshold: only the 3-way group survives.
        let (total, ids) = search_ids(
            &e,
            "users",
            QueryNode::Duplicated(DuplicatedQuery {
                field: "email".into(),
                min_group_size: 3,
            }),
        );
        assert_eq!(total, 3);
        assert_eq!(ids, vec!["u1", "u2", "u3"]);
    }

    #[test]
    fn duplicated_composes_with_boolean() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("u1", "email", FieldValue::String("a@x.com".into())),
                    item("u1", "age", FieldValue::Number(30.0)),
                    item("u2", "email", FieldValue::String("a@x.com".into())),
                    item("u2", "age", FieldValue::Number(60.0)),
                    item("u3", "email", FieldValue::String("a@x.com".into())),
                    item("u3", "age", FieldValue::Number(35.0)),
                    item("u4", "email", FieldValue::String("u@u.com".into())), // unique email
                    item("u4", "age", FieldValue::Number(30.0)),
                ],
                request_id: None,
            },
        )
        .unwrap();
        // duplicate-email AND age<40: u1, u3 (u2 too old, u4 not a duplicate).
        let q = QueryNode::And(vec![
            QueryNode::Duplicated(DuplicatedQuery {
                field: "email".into(),
                min_group_size: 2,
            }),
            QueryNode::Range(RangeQuery {
                field: "age".into(),
                gte: None,
                lt: Some(RangeBound::Number(40.0)),
                gt: None,
                lte: None,
            }),
        ]);
        let (total, ids) = search_ids(&e, "users", q);
        assert_eq!(total, 2);
        assert_eq!(ids, vec!["u1", "u3"]);
    }

    #[test]
    fn duplicated_min_group_size_floor_is_two() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("u1", "email", FieldValue::String("a@x.com".into())),
                    item("u2", "email", FieldValue::String("a@x.com".into())),
                    item("u3", "email", FieldValue::String("solo@x.com".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();
        // min_group_size 0/1 would make every doc a "duplicate"; the leaf floors it
        // at 2 so a singleton never matches.
        let (total, ids) = search_ids(
            &e,
            "users",
            QueryNode::Duplicated(DuplicatedQuery {
                field: "email".into(),
                min_group_size: 0,
            }),
        );
        assert_eq!(total, 2);
        assert_eq!(ids, vec!["u1", "u2"]);
    }

    #[test]
    fn exists_duplicated_segment_paths_equal_tail_paths() {
        // Guards the eval_field_doc_union asymmetry: segment OFF answers from the
        // in-RAM map (+ dup_values candidates for min>=2), segment ON from the
        // segment-aware live_* accessors. Seal must not change any answer, a
        // checkpoint reopen must agree, and a post-seal delete must obey the
        // group-size semantics on the sealed path (a 2-group losing a member
        // drops BOTH docs from `duplicated`).
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("u1", "email", FieldValue::String("a@x.com".into())),
                    item("u2", "email", FieldValue::String("a@x.com".into())),
                    item("u3", "email", FieldValue::String("a@x.com".into())),
                    item("u4", "email", FieldValue::String("b@y.com".into())),
                    item("u5", "email", FieldValue::String("b@y.com".into())),
                    item("u6", "email", FieldValue::String("solo@z.com".into())),
                    item("u1", "age", FieldValue::Number(30.0)),
                    item("u2", "age", FieldValue::Number(30.0)),
                    item(
                        "u7",
                        "tags",
                        FieldValue::StringList(vec!["x".into(), "y".into()]),
                    ),
                    item("u8", "tags", FieldValue::StringList(vec!["x".into()])),
                ],
                request_id: None,
            },
        )
        .unwrap();

        let queries: Vec<(&str, QueryNode)> = vec![
            (
                "exists email",
                QueryNode::Exists(ExistsQuery {
                    field: "email".into(),
                }),
            ),
            (
                "exists age",
                QueryNode::Exists(ExistsQuery {
                    field: "age".into(),
                }),
            ),
            (
                "exists tags",
                QueryNode::Exists(ExistsQuery {
                    field: "tags".into(),
                }),
            ),
            (
                "dup email >=2",
                QueryNode::Duplicated(DuplicatedQuery {
                    field: "email".into(),
                    min_group_size: 2,
                }),
            ),
            (
                "dup email >=3",
                QueryNode::Duplicated(DuplicatedQuery {
                    field: "email".into(),
                    min_group_size: 3,
                }),
            ),
            (
                "dup age >=2",
                QueryNode::Duplicated(DuplicatedQuery {
                    field: "age".into(),
                    min_group_size: 2,
                }),
            ),
            (
                "dup tags >=2",
                QueryNode::Duplicated(DuplicatedQuery {
                    field: "tags".into(),
                    min_group_size: 2,
                }),
            ),
        ];
        let tail: Vec<_> = queries
            .iter()
            .map(|(_, q)| search_ids(&e, "users", q.clone()))
            .collect();

        // Seal in place → the same queries now answer off the segment path.
        let dir = tempfile::tempdir().unwrap();
        e.flush_to_segments(dir.path(), 1).unwrap();
        for ((label, q), want) in queries.iter().zip(&tail) {
            let got = search_ids(&e, "users", q.clone());
            assert_eq!(&got, want, "sealed path diverged from tail path: {label}");
        }

        // Checkpoint reopen must agree too.
        let reopened = Engine::new();
        reopened.reopen_from_segment_dir(dir.path()).unwrap();
        for ((label, q), want) in queries.iter().zip(&tail) {
            let got = search_ids(&reopened, "users", q.clone());
            assert_eq!(&got, want, "reopened path diverged from tail path: {label}");
        }

        // Post-seal delete: u5 leaves → b@y.com group shrinks 2→1, so u4 must
        // ALSO leave `duplicated`; exists drops u5 only.
        e.delete("users", "u5", None).unwrap();
        let (_, ids) = search_ids(
            &e,
            "users",
            QueryNode::Duplicated(DuplicatedQuery {
                field: "email".into(),
                min_group_size: 2,
            }),
        );
        assert_eq!(
            ids,
            vec!["u1", "u2", "u3"],
            "2-group survivor must exit duplicated"
        );
        let (_, ids) = search_ids(
            &e,
            "users",
            QueryNode::Exists(ExistsQuery {
                field: "email".into(),
            }),
        );
        assert_eq!(
            ids,
            vec!["u1", "u2", "u3", "u4", "u6"],
            "exists must drop only the deleted doc"
        );
    }

    #[test]
    fn exists_on_text_field_rejected() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let err = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Exists(ExistsQuery {
                        field: "bio".into(),
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
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("text"), "unexpected error: {msg}");
    }

    #[test]
    fn reindex_replaces_field_value() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![item("u1", "email", FieldValue::String("old@x.com".into()))],
                request_id: None,
            },
        )
        .unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![item("u1", "email", FieldValue::String("new@x.com".into()))],
                request_id: None,
            },
        )
        .unwrap();
        // Old value gone, new value present.
        let r_old = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Term(TermQuery {
                        field: "email".into(),
                        value: FieldValue::String("old@x.com".into()),
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
            .unwrap();
        assert_eq!(r_old.total, 0);
        let r_new = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Term(TermQuery {
                        field: "email".into(),
                        value: FieldValue::String("new@x.com".into()),
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
            .unwrap();
        assert_eq!(r_new.total, 1);
    }

    #[test]
    fn delete_external_id_removes_all_fields() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("u1", "email", FieldValue::String("a@x.com".into())),
                    item("u1", "bio", FieldValue::String("rust engineer".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();
        e.delete("users", "u1", None).unwrap();
        let r = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Term(TermQuery {
                        field: "email".into(),
                        value: FieldValue::String("a@x.com".into()),
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
            .unwrap();
        assert_eq!(r.total, 0);
    }

    /// #3992: a truncate is a document-state swap, not a disguised batch of
    /// per-document deletes. This checks the precise state that has to reset
    /// and uses an unavailable worker to prove the caller thread does not
    /// reach the per-document removal primitive.
    #[test]
    fn truncate_docs_preserves_schema_version_and_never_walks_documents() {
        let e = Engine::new();
        let created = e.create_collection("users", build_users_schema()).unwrap();
        let schema = {
            let state = e.state.read().unwrap();
            state.collections["users"].schema.clone()
        };
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    crate::shared_kernel::types::document::IndexItem {
                        external_id: "u1".into(),
                        field: "email".into(),
                        value: FieldValue::String("u1@example.com".into()),
                        version: Some(7),
                    },
                    item("u1", "bio", FieldValue::String("rust engineer".into())),
                    item(
                        "u1",
                        "tags",
                        FieldValue::StringList(vec!["systems".into(), "search".into()]),
                    ),
                    item("u1", "age", FieldValue::Number(42.0)),
                ],
                request_id: Some("before-truncate".into()),
            },
        )
        .unwrap();
        let mut replacement = BTreeMap::new();
        replacement.insert("bio".into(), FieldValue::String("new bio".into()));
        e.replace_docs(
            "users",
            ReplaceDocsRequest {
                docs: vec![ReplaceDocItem {
                    external_id: "u1".into(),
                    version: Some(9),
                    fields: replacement,
                }],
            },
        )
        .unwrap();
        let _ = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Exists(ExistsQuery {
                        field: "email".into(),
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
            .unwrap();

        DROP_EID_CALLS.with(|calls| calls.set(0));
        e.truncate_docs_with_retirement("users", &CollectionRetirementWorker::Unavailable)
            .unwrap();
        DROP_EID_CALLS.with(|calls| {
            assert_eq!(
                calls.get(),
                0,
                "truncate must not call the per-document drop primitive on its caller thread"
            );
        });

        let state = e.state.read().unwrap();
        let coll = &state.collections["users"];
        assert_eq!(coll.version, created.version);
        assert_eq!(coll.schema, schema);
        assert!(coll.interner.to_eid.is_empty());
        assert!(coll.eid_fields.is_empty());
        assert!(coll.seen_requests.is_empty());
        assert!(coll.cell_versions.is_empty());
        assert!(coll.doc_versions.is_empty());
        assert!(coll.field_checksums.is_empty());
        assert!(coll.search_cache.read().unwrap().is_empty());
        assert!(coll.last_indexed_at.is_none());
        for index in coll.fields.values() {
            assert_eq!(
                index.bytes(),
                0,
                "fresh schema field must hold no documents"
            );
        }
        drop(state);

        e.index(
            "users",
            IndexRequest {
                items: vec![item(
                    "u2",
                    "email",
                    FieldValue::String("u2@example.com".into()),
                )],
                request_id: None,
            },
        )
        .unwrap();
        assert_eq!(
            e.stats("users").unwrap().documents_indexed,
            1,
            "the retained schema must accept a new document immediately"
        );
    }

    #[test]
    fn unavailable_retirement_worker_fails_safe_without_changing_apply_semantics() {
        let before = RETIREMENT_FAILSAFE_RETAINS.load(Ordering::Relaxed);
        let detached = Collection::new(BTreeMap::new()).unwrap();
        retire_collection_with(&CollectionRetirementWorker::Unavailable, detached);
        assert_eq!(
            RETIREMENT_FAILSAFE_RETAINS.load(Ordering::Relaxed),
            before + 1,
            "a failed worker handoff must retain the old generation instead of dropping it on the apply thread"
        );
    }

    #[test]
    fn retired_generation_splits_document_reclaim_into_fixed_size_tasks() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let documents = RETIRED_DOCUMENTS_PER_TASK * 2 + 1;
        let mut items = Vec::with_capacity(documents);
        for document in 0..documents {
            items.push(item(
                &format!("u{document}"),
                "email",
                FieldValue::String(format!("u{document}@example.com")),
            ));
        }
        e.index(
            "users",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
        assert_eq!(
            e.stats("users").unwrap().documents_indexed,
            documents as u64
        );

        let detached = e
            .state
            .write()
            .unwrap()
            .collections
            .remove("users")
            .expect("test collection exists");
        DROP_EID_CALLS.with(|calls| calls.set(0));
        let generation = RetiredGeneration::new(0, detached);
        let mut task_sizes = Vec::new();
        let final_collection = loop {
            match generation.drain_document_task() {
                RetireTaskProgress::More { retired_documents } => {
                    task_sizes.push(retired_documents);
                }
                RetireTaskProgress::Complete {
                    retired_documents,
                    collection,
                } => {
                    task_sizes.push(retired_documents);
                    break collection;
                }
            }
        };

        assert_eq!(
            task_sizes,
            vec![RETIRED_DOCUMENTS_PER_TASK, RETIRED_DOCUMENTS_PER_TASK, 1],
            "each token owns one bounded document slice"
        );
        DROP_EID_CALLS.with(|calls| {
            assert_eq!(
                calls.get(),
                documents as u64,
                "the detached worker removed each indexed field exactly once"
            );
        });
        assert!(final_collection.interner.to_eid.is_empty());
        assert!(final_collection.interner.to_hash.is_empty());
        assert!(final_collection.eid_fields.is_empty());
        assert!(final_collection.cell_versions.is_empty());
        assert!(final_collection.doc_versions.is_empty());
        assert!(final_collection.field_checksums.is_empty());
        assert_eq!(final_collection.fields["email"].bytes(), 0);
        drop(final_collection);
    }

    #[test]
    fn retired_generation_requeues_exactly_one_token_until_completion() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let documents = RETIRED_DOCUMENTS_PER_TASK + 1;
        let items = (0..documents)
            .map(|document| {
                item(
                    &format!("u{document}"),
                    "email",
                    FieldValue::String(format!("u{document}@example.com")),
                )
            })
            .collect();
        e.index(
            "users",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
        let detached = e
            .state
            .write()
            .unwrap()
            .collections
            .remove("users")
            .expect("test collection exists");
        let generation = Arc::new(RetiredGeneration::new(0, detached));
        let (sender, receiver) = mpsc::channel();

        run_retire_task(
            RetireTask {
                generation: Arc::clone(&generation),
            },
            &sender,
        );
        let completion = try_receive_retirement_task(&receiver)
            .expect("one incomplete generation must requeue one token");
        assert!(
            receiver.try_recv().is_err(),
            "one generation may not enqueue multiple concurrent tokens"
        );

        run_retire_task(completion, &sender);
        finish_retirement_task();
        assert!(
            receiver.try_recv().is_err(),
            "the final token must acknowledge completion without requeueing"
        );
        assert!(
            generation.collection.lock().unwrap().is_none(),
            "completion consumes the detached collection exactly once"
        );
    }

    #[test]
    fn shared_retirement_receiver_assigns_distinct_generation_tokens_to_workers() {
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let first = Arc::new(RetiredGeneration::new(
            41,
            Collection::new(BTreeMap::new()).unwrap(),
        ));
        let second = Arc::new(RetiredGeneration::new(
            42,
            Collection::new(BTreeMap::new()).unwrap(),
        ));
        send_retirement_task(
            &sender,
            RetireTask {
                generation: Arc::clone(&first),
            },
        )
        .unwrap();
        send_retirement_task(
            &sender,
            RetireTask {
                generation: Arc::clone(&second),
            },
        )
        .unwrap();
        drop(sender);

        let barrier = Arc::new(std::sync::Barrier::new(3));
        let (observed_sender, observed_receiver) = mpsc::channel();
        std::thread::scope(|scope| {
            for _ in 0..2 {
                let receiver = Arc::clone(&receiver);
                let barrier = Arc::clone(&barrier);
                let observed_sender = observed_sender.clone();
                scope.spawn(move || {
                    barrier.wait();
                    let task = receive_retirement_task(receiver.as_ref())
                        .expect("each worker receives one pre-enqueued token");
                    observed_sender.send(task.generation.id).unwrap();
                    finish_retirement_task();
                });
            }
            barrier.wait();
        });

        let mut observed = vec![
            observed_receiver.recv().unwrap(),
            observed_receiver.recv().unwrap(),
        ];
        observed.sort_unstable();
        assert_eq!(observed, vec![41, 42]);
        assert!(
            receiver.lock().unwrap().try_recv().is_err(),
            "the two workers consumed both tokens exactly once"
        );
    }

    #[test]
    fn collection_reclaimer_worker_count_defaults_and_caps() {
        assert_eq!(collection_retirement_worker_count(None), 1);
        assert_eq!(collection_retirement_worker_count(Some("0")), 1);
        assert_eq!(collection_retirement_worker_count(Some("invalid")), 1);
        assert_eq!(collection_retirement_worker_count(Some("2")), 2);
        assert_eq!(
            collection_retirement_worker_count(Some("99")),
            MAX_COLLECTION_RETIREMENT_WORKERS
        );
    }

    #[test]
    fn idempotency_skips_duplicate_request_id() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let req = IndexRequest {
            items: vec![item("u1", "email", FieldValue::String("a@x.com".into()))],
            request_id: Some("req-1".into()),
        };
        e.index("users", req.clone()).unwrap();
        let r = e.index("users", req).unwrap();
        assert_eq!(r.indexed, 0);
    }

    #[test]
    fn cursor_round_trip() {
        let c = make_cursor(42);
        assert!(matches!(
            parse_page_cursor(&c),
            Some(PageCursor::Offset(42))
        ));
        let c = make_sort_cursor(0x8000_0000_0000_0000, 7);
        assert!(matches!(
            parse_page_cursor(&c),
            Some(PageCursor::SortKeyset {
                bits: 0x8000_0000_0000_0000,
                docid: 7
            })
        ));
        let c = make_score_cursor(1.5, "u042");
        match parse_page_cursor(&c) {
            Some(PageCursor::ScoreKeyset { score_bits, eid }) => {
                assert_eq!(f32::from_bits(score_bits), 1.5);
                assert_eq!(eid, "u042");
            }
            _ => panic!("expected score keyset"),
        }
    }

    // -----------------------------------------------------------------
    // Ordering contract (Contract 1 of the coverage goal).
    //
    // These assert the *order* and relative magnitude of scores, not
    // just membership — so a mutated BM25 / score-combination operator
    // (e.g. `+`→`-`, `/`→`*`, `cmp(a,b)`→`cmp(b,a)`) makes at least one
    // of them fail. Membership-only tests above cannot catch those.
    // -----------------------------------------------------------------

    fn text_only_schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert(
            "body".into(),
            FieldSpec {
                field_type: FieldType::Text,
                analyzer: Some(Analyzer::WhitespaceLower),
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        CreateCollectionRequest { fields }
    }

    fn search_match(e: &Engine, coll: &str, text: &str, op: MatchOp) -> Vec<SearchHit> {
        e.search(
            coll,
            SearchRequest {
                query: QueryNode::Match(MatchQuery {
                    field: "body".into(),
                    text: text.into(),
                    op,
                }),
                limit: 50,
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
    }

    fn score_of(hits: &[SearchHit], eid: &str) -> f32 {
        hits.iter()
            .find(|h| h.external_id == eid)
            .unwrap_or_else(|| panic!("eid {eid} not in hits"))
            .score
    }

    #[test]
    fn bm25_higher_tf_scores_strictly_higher() {
        let e = Engine::new();
        e.create_collection("c", text_only_schema()).unwrap();
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    // identical doc length, differing only in TF of "rust"
                    item(
                        "hi",
                        "body",
                        FieldValue::String("rust rust rust pad pad".into()),
                    ),
                    item(
                        "lo",
                        "body",
                        FieldValue::String("rust pad pad pad pad".into()),
                    ),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let hits = search_match(&e, "c", "rust", MatchOp::Or);
        // hi has TF=3, lo has TF=1 → hi must score strictly higher AND
        // sort first. Kills `tf` numerator / denominator operator flips.
        assert_eq!(hits[0].external_id, "hi");
        assert!(
            score_of(&hits, "hi") > score_of(&hits, "lo"),
            "higher TF must score higher: {hits:?}"
        );
    }

    #[test]
    fn bm25_shorter_doc_scores_higher_for_same_tf() {
        let e = Engine::new();
        e.create_collection("c", text_only_schema()).unwrap();
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    // same TF(rust)=1, but `short` is a shorter doc →
                    // length normalization gives it the higher score.
                    item("short", "body", FieldValue::String("rust pad".into())),
                    item(
                        "long",
                        "body",
                        FieldValue::String("rust pad pad pad pad pad pad pad".into()),
                    ),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let hits = search_match(&e, "c", "rust", MatchOp::Or);
        assert_eq!(hits[0].external_id, "short");
        assert!(
            score_of(&hits, "short") > score_of(&hits, "long"),
            "BM25 length-norm: shorter doc ranks higher at equal TF: {hits:?}"
        );
    }

    #[test]
    fn bm25_rarer_term_contributes_more_idf() {
        let e = Engine::new();
        e.create_collection("c", text_only_schema()).unwrap();
        // "common" appears in every doc (low IDF); "rare" in just one
        // (high IDF). A doc matched by "rare" must outscore one matched
        // only by "common" — kills IDF numerator/denominator flips.
        let mut items = vec![
            item("rare_doc", "body", FieldValue::String("common rare".into())),
            item(
                "common_doc",
                "body",
                FieldValue::String("common filler".into()),
            ),
        ];
        for i in 0..8 {
            items.push(item(
                &format!("bg{i}"),
                "body",
                FieldValue::String("common filler".into()),
            ));
        }
        e.index(
            "c",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
        let hits = search_match(&e, "c", "rare common", MatchOp::Or);
        assert_eq!(
            hits[0].external_id, "rare_doc",
            "doc matching the rare (high-IDF) term must rank first: {hits:?}"
        );
        assert!(score_of(&hits, "rare_doc") > score_of(&hits, "common_doc"));
    }

    #[test]
    fn or_combines_scores_additively_doc_matching_both_ranks_first() {
        let e = Engine::new();
        e.create_collection("c", text_only_schema()).unwrap();
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    item("both", "body", FieldValue::String("alpha beta".into())),
                    item(
                        "alpha_only",
                        "body",
                        FieldValue::String("alpha gamma".into()),
                    ),
                    item("beta_only", "body", FieldValue::String("beta gamma".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let hits = search_match(&e, "c", "alpha beta", MatchOp::Or);
        // `both` matched on two tokens → its summed score must exceed
        // either single-token doc. Kills the OR `+=`→`-=`/`*=` mutants.
        assert_eq!(
            hits[0].external_id, "both",
            "doc matching both tokens ranks first: {hits:?}"
        );
        let s_both = score_of(&hits, "both");
        assert!(s_both > score_of(&hits, "alpha_only"));
        assert!(s_both > score_of(&hits, "beta_only"));
    }

    #[test]
    fn and_sums_matched_token_scores() {
        let e = Engine::new();
        e.create_collection("c", text_only_schema()).unwrap();
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    item("d1", "body", FieldValue::String("alpha beta".into())),
                    item(
                        "d2",
                        "body",
                        FieldValue::String("alpha beta gamma delta eps".into()),
                    ),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let and_hits = search_match(&e, "c", "alpha beta", MatchOp::And);
        // Both docs contain both tokens → both returned, and the AND
        // score equals the sum of the two per-token contributions. The
        // shorter doc (d1) wins on length-norm. Kills AND `score + s`
        // → `score - s` (which would invert or zero the combination).
        assert_eq!(and_hits.len(), 2);
        assert_eq!(and_hits[0].external_id, "d1");
        assert!(score_of(&and_hits, "d1") > 0.0);
        assert!(score_of(&and_hits, "d1") >= score_of(&and_hits, "d2"));
    }

    #[test]
    fn search_sorts_by_score_desc_then_eid_asc() {
        let e = Engine::new();
        e.create_collection("c", text_only_schema()).unwrap();
        // Two docs with identical content → identical score → tie
        // broken by external_id ascending. Kills the tie-break
        // `a.cmp(b)`→`b.cmp(a)` mutant and the score-cmp flip.
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    item("zeta", "body", FieldValue::String("rust rust".into())),
                    item("alpha", "body", FieldValue::String("rust rust".into())),
                    item("solo", "body", FieldValue::String("rust".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let hits = search_match(&e, "c", "rust", MatchOp::Or);
        // solo has lower TF → lowest score → must be last.
        assert_eq!(hits.last().unwrap().external_id, "solo");
        // zeta & alpha tie on score → alpha first (eid asc).
        let alpha_pos = hits.iter().position(|h| h.external_id == "alpha").unwrap();
        let zeta_pos = hits.iter().position(|h| h.external_id == "zeta").unwrap();
        assert!(alpha_pos < zeta_pos, "tie broken by eid asc: {hits:?}");
    }

    #[test]
    fn bm25_exact_golden_scores() {
        // Pins the formula to textbook BM25 (K1=1.2, B=0.75). Ordering
        // assertions can't catch magnitude-only mutations that preserve
        // relative order (e.g. `(n-df+0.5)/(df+0.5)` → `*`); a
        // hand-computed reference does.
        //
        // Corpus (field "body", whitespace_lower):
        //   a = "x y"      → tf(x)=1, doc_len=2
        //   b = "x x z w"  → tf(x)=2, doc_len=4
        // n=2, total_doc_len=6, avgdl=3, df(x)=2
        // idf = ln((2-2+0.5)/(2+0.5) + 1) = ln(1.2)             = 0.1823215
        // a: denom = 1 + 1.2*(1-0.75 + 0.75*2/3) = 1.9
        //    score = 0.1823215 * 1 * 2.2 / 1.9                  = 0.211110
        // b: denom = 2 + 1.2*(1-0.75 + 0.75*4/3) = 3.5
        //    score = 0.1823215 * 2 * 2.2 / 3.5                  = 0.229204
        let e = Engine::new();
        e.create_collection("c", text_only_schema()).unwrap();
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    item("a", "body", FieldValue::String("x y".into())),
                    item("b", "body", FieldValue::String("x x z w".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let hits = search_match(&e, "c", "x", MatchOp::Or);
        let sa = score_of(&hits, "a");
        let sb = score_of(&hits, "b");
        assert!(
            (sa - 0.211110).abs() < 5e-4,
            "BM25(a) golden mismatch: got {sa}, want ≈0.211110"
        );
        assert!(
            (sb - 0.229204).abs() < 5e-4,
            "BM25(b) golden mismatch: got {sb}, want ≈0.229204"
        );
    }

    fn two_keyword_schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        for name in ["tag", "region"] {
            fields.insert(
                name.to_string(),
                FieldSpec {
                    field_type: FieldType::Keyword,
                    analyzer: None,
                    multi: None,
                    dim: None,
                    metric: None,
                    backend: None,
                    quantize: None,
                },
            );
        }
        CreateCollectionRequest { fields }
    }

    #[test]
    fn eval_query_and_sums_child_scores_exactly() {
        // QueryNode::And of two term queries. Each term contributes a
        // constant score of 1.0, so a doc matching both must score
        // exactly 2.0. Kills the eval_query AND `score + s` → `-`/`*`.
        let e = Engine::new();
        e.create_collection("c", two_keyword_schema()).unwrap();
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    item("d", "tag", FieldValue::String("rust".into())),
                    item("d", "region", FieldValue::String("apac".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let resp = e
            .search(
                "c",
                SearchRequest {
                    query: QueryNode::And(vec![
                        QueryNode::Term(TermQuery {
                            field: "tag".into(),
                            value: FieldValue::String("rust".into()),
                        }),
                        QueryNode::Term(TermQuery {
                            field: "region".into(),
                            value: FieldValue::String("apac".into()),
                        }),
                    ]),
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: None,
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap();
        assert_eq!(resp.hits.len(), 1);
        assert!(
            (resp.hits[0].score - 2.0).abs() < 1e-6,
            "AND of two terms must sum to exactly 2.0, got {}",
            resp.hits[0].score
        );
    }

    #[test]
    fn eval_query_or_sums_and_ranks_multi_match_first() {
        // QueryNode::Or of two terms. A doc matching both scores 2.0 and
        // must rank above a doc matching one (1.0). Kills eval_query OR
        // `+= score` → `-=`/`*=` (which would flip or zero the ranking).
        let e = Engine::new();
        e.create_collection("c", two_keyword_schema()).unwrap();
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    item("both", "tag", FieldValue::String("rust".into())),
                    item("both", "region", FieldValue::String("apac".into())),
                    item("one", "tag", FieldValue::String("rust".into())),
                    item("one", "region", FieldValue::String("emea".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let resp = e
            .search(
                "c",
                SearchRequest {
                    query: QueryNode::Or(vec![
                        QueryNode::Term(TermQuery {
                            field: "tag".into(),
                            value: FieldValue::String("rust".into()),
                        }),
                        QueryNode::Term(TermQuery {
                            field: "region".into(),
                            value: FieldValue::String("apac".into()),
                        }),
                    ]),
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: None,
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap();
        assert_eq!(resp.hits[0].external_id, "both");
        assert!((score_of(&resp.hits, "both") - 2.0).abs() < 1e-6);
        assert!((score_of(&resp.hits, "one") - 1.0).abs() < 1e-6);
    }

    #[test]
    fn eval_query_not_excludes_exactly_the_matched_set() {
        // NOT over a term: universe minus the matched eids, each scored
        // 1.0. Kills the `delete !` mutant on the Not branch.
        let e = Engine::new();
        e.create_collection("c", two_keyword_schema()).unwrap();
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    item("a", "tag", FieldValue::String("rust".into())),
                    item("b", "tag", FieldValue::String("go".into())),
                    item("c", "tag", FieldValue::String("python".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let resp = e
            .search(
                "c",
                SearchRequest {
                    query: QueryNode::Not(Box::new(QueryNode::Term(TermQuery {
                        field: "tag".into(),
                        value: FieldValue::String("rust".into()),
                    }))),
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: None,
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap();
        assert_eq!(resp.total, 2);
        let ids: BTreeSet<&str> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
        assert!(ids.contains("b") && ids.contains("c") && !ids.contains("a"));
    }

    #[test]
    fn validate_query_rejects_pathological_trees_but_allows_normal() {
        let e = Engine::new();
        e.create_collection("c", two_keyword_schema()).unwrap();
        let term = || {
            QueryNode::Term(TermQuery {
                field: "tag".into(),
                value: FieldValue::String("rust".into()),
            })
        };
        let is_too_complex = |r: Result<SearchResponse>| {
            matches!(
                r.unwrap_err().downcast_ref::<StorageError>(),
                Some(StorageError::QueryTooComplex(_))
            )
        };
        let search = |q: QueryNode| {
            e.search(
                "c",
                SearchRequest {
                    query: q,
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: None,
                    track_total: true,
                    collapse: None,
                },
            )
        };

        // Deeply nested (would stack-overflow eval without the guard). Build
        // iteratively so the test itself doesn't recurse.
        let mut deep = term();
        for _ in 0..1000 {
            deep = QueryNode::And(vec![deep]);
        }
        assert!(is_too_complex(search(deep)), "deep query must be rejected");

        // Very wide (node-count DoS).
        let wide = QueryNode::And((0..100_000).map(|_| term()).collect());
        assert!(is_too_complex(search(wide)), "wide query must be rejected");

        // Huge terms fan-out.
        let huge_terms = QueryNode::Terms(TermsQuery {
            field: "tag".into(),
            values: (0..2000)
                .map(|i| FieldValue::String(format!("v{i}")))
                .collect(),
        });
        assert!(
            is_too_complex(search(huge_terms)),
            "huge terms must be rejected"
        );

        // A normal shallow query still works.
        assert!(
            search(QueryNode::And(vec![term()])).is_ok(),
            "normal query must pass"
        );
    }

    #[test]
    fn number_exact_term_query_matches_only_that_value() {
        // Exercises the (Number, Number) arm of eval_term — number
        // *exact* match, distinct from range. Without this, deleting
        // that match arm goes uncaught (the range tests never hit it).
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        e.index(
            "users",
            IndexRequest {
                items: vec![
                    item("a", "age", FieldValue::Number(30.0)),
                    item("b", "age", FieldValue::Number(30.0)),
                    item("c", "age", FieldValue::Number(31.0)),
                ],
                request_id: None,
            },
        )
        .unwrap();
        let resp = e
            .search(
                "users",
                SearchRequest {
                    query: QueryNode::Term(TermQuery {
                        field: "age".into(),
                        value: FieldValue::Number(30.0),
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
            .unwrap();
        assert_eq!(resp.total, 2);
        let ids: BTreeSet<&str> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
        assert!(ids.contains("a") && ids.contains("b") && !ids.contains("c"));
    }

    #[test]
    fn and_match_score_is_sum_not_product_of_token_scores() {
        // Single doc "p q" (the only doc). Each token scores identically:
        //   n=1, df=1, idf=ln((0.5)/(1.5)+1)=ln(1.3333)=0.287682
        //   denom = 1 + 1.2*(1-0.75 + 0.75*2/2) = 2.2
        //   per-token = 0.287682 * 1 * 2.2 / 2.2 = 0.287682
        // AND(p,q) sums the two ⇒ 0.575364.
        // The `score + s` → `score * s` mutant would give
        // 0.287682² = 0.082761, so an exact assert kills it.
        let e = Engine::new();
        e.create_collection("c", text_only_schema()).unwrap();
        e.index(
            "c",
            IndexRequest {
                items: vec![item("d", "body", FieldValue::String("p q".into()))],
                request_id: None,
            },
        )
        .unwrap();
        let hits = search_match(&e, "c", "p q", MatchOp::And);
        assert_eq!(hits.len(), 1);
        let s = score_of(&hits, "d");
        assert!(
            (s - 0.575364).abs() < 5e-4,
            "AND match must SUM token scores (≈0.575364), got {s} (product would be ≈0.0828)"
        );
    }

    #[test]
    fn range_bounds_are_exclusive_vs_inclusive() {
        let e = Engine::new();
        e.create_collection("users", build_users_schema()).unwrap();
        let items = (0..=10)
            .map(|i| item(&format!("u{i}"), "age", FieldValue::Number(i as f64)))
            .collect();
        e.index(
            "users",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();

        let run = |q: RangeQuery| {
            e.search(
                "users",
                SearchRequest {
                    query: QueryNode::Range(q),
                    limit: 50,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: None,
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap()
            .total
        };

        // gte=2, lte=5 → {2,3,4,5} = 4
        assert_eq!(
            run(RangeQuery {
                field: "age".into(),
                gt: None,
                gte: Some(RangeBound::Number(2.0)),
                lt: None,
                lte: Some(RangeBound::Number(5.0))
            }),
            4
        );
        // gt=2, lt=5 → {3,4} = 2  (exclusive both ends)
        assert_eq!(
            run(RangeQuery {
                field: "age".into(),
                gt: Some(RangeBound::Number(2.0)),
                gte: None,
                lt: Some(RangeBound::Number(5.0)),
                lte: None
            }),
            2
        );
        // gte=2, lt=5 → {2,3,4} = 3  (mixed)
        assert_eq!(
            run(RangeQuery {
                field: "age".into(),
                gt: None,
                gte: Some(RangeBound::Number(2.0)),
                lt: Some(RangeBound::Number(5.0)),
                lte: None
            }),
            3
        );
    }

    fn kw_only_schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert(
            "email".into(),
            FieldSpec {
                field_type: FieldType::Keyword,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        CreateCollectionRequest { fields }
    }

    fn index_kw(e: &Engine, collection_id: &str, eid: &str) {
        e.index(
            collection_id,
            IndexRequest {
                items: vec![item(
                    eid,
                    "email",
                    FieldValue::String(format!("{eid}@x.com")),
                )],
                request_id: None,
            },
        )
        .unwrap();
    }

    /// Ground truth read of one collection's own byte footprint, independent
    /// of the engine-wide gauge under test.
    fn collection_live_bytes(e: &Engine, collection_id: &str) -> u64 {
        let state = e.state.read().unwrap();
        state
            .collections
            .get(collection_id)
            .unwrap()
            .fields
            .values()
            .map(|fi| fi.bytes())
            .sum()
    }

    /// Capacity control scrapes the engine metric directly; it must not need
    /// a separate `/stats` request after a normal write, a segment/snapshot
    /// restore, or a full-document replacement. The same `index` method is
    /// used for AOF replay, so this also protects the post-restart path.
    #[test]
    fn mutations_and_restore_publish_storage_bytes_without_stats() {
        let e = Engine::new();
        e.create_collection("a", kw_only_schema()).unwrap();
        index_kw(&e, "a", "before-restart");
        let before_restart_bytes = collection_live_bytes(&e, "a");
        assert!(before_restart_bytes > 0);
        assert_eq!(e.metrics().storage_bytes.get(), before_restart_bytes);

        let snapshot = e.snapshot().unwrap();
        let restored = Engine::new();
        restored.restore(snapshot).unwrap();
        assert_eq!(
            restored.metrics().storage_bytes.get(),
            collection_live_bytes(&restored, "a"),
            "restore must publish the rebuilt segment footprint before any stats request"
        );

        let mut fields = BTreeMap::new();
        fields.insert(
            "email".to_string(),
            FieldValue::String("after-restart@example.com".to_string()),
        );
        restored
            .replace_docs(
                "a",
                ReplaceDocsRequest {
                    docs: vec![ReplaceDocItem {
                        external_id: "before-restart".to_string(),
                        version: None,
                        fields,
                    }],
                },
            )
            .unwrap();
        assert_eq!(
            restored.metrics().storage_bytes.get(),
            collection_live_bytes(&restored, "a"),
            "full replacement must refresh the capacity gauge"
        );

        restored.delete("a", "before-restart", None).unwrap();
        assert_eq!(
            restored.metrics().storage_bytes.get(),
            collection_live_bytes(&restored, "a"),
            "delete must refresh the capacity gauge for scale-down decisions"
        );

        assert_eq!(
            restored.drop_collection("a", false).unwrap(),
            DropOutcome::Marked
        );
        assert_eq!(
            restored.metrics().storage_bytes.get(),
            0,
            "soft-deleted collections must stop contributing to live capacity"
        );
    }

    #[test]
    fn activate_replacement_swaps_the_complete_collection_set() {
        let active = Engine::new();
        active.create_collection("old", kw_only_schema()).unwrap();
        index_kw(&active, "old", "old-doc");

        let replacement = Engine::new();
        replacement
            .create_collection("new", kw_only_schema())
            .unwrap();
        index_kw(&replacement, "new", "new-doc");
        let expected_bytes = collection_live_bytes(&replacement, "new");

        active.activate_replacement(replacement).unwrap();

        assert!(
            active.stats("old").is_err(),
            "collections absent from the replacement must be removed"
        );
        assert_eq!(active.stats("new").unwrap().documents_indexed, 1);
        assert_eq!(active.metrics().storage_bytes.get(), expected_bytes);
    }

    // ---- #3953: create-over-tombstone supersede ------------------------

    /// #3953(a) at the `Engine` layer: `create_collection` for a
    /// soft-deleted id must not stay wedged behind `check_live` — it must
    /// supersede the tombstone with a fresh, empty collection whose schema
    /// comes from the new request alone (not merged with the deleted
    /// predecessor's), and the pre-delete doc must not survive.
    ///
    /// Everything above is about CONTENTS. `version` is the one thing that
    /// does cross the tombstone, because it is not content — it is the
    /// number the caller keys cached schema state on, and a supersede that
    /// re-answered 1 would move it backwards. See the supersede branch's own
    /// comment, and `tests/it/collection_version_never_moves_backwards.rs`.
    #[test]
    fn create_collection_supersedes_tombstone_with_fresh_empty_collection() {
        let e = Engine::new();
        e.create_collection("a", kw_only_schema()).unwrap();
        index_kw(&e, "a", "old1");
        assert_eq!(e.drop_collection("a", false).unwrap(), DropOutcome::Marked);

        // Live routes must still see the tombstone as 410 Gone before the
        // recreate — this fix must not touch that behavior.
        let stats_err = e.stats("a").unwrap_err();
        assert!(
            matches!(
                stats_err.downcast_ref::<StorageError>(),
                Some(StorageError::Gone(id)) if id == "a"
            ),
            "a soft-deleted collection must still answer 410 Gone on reads \
             right up until it is recreated: {stats_err:?}"
        );

        let resp = e.create_collection("a", kw_only_schema()).unwrap();
        assert_eq!(
            resp.version, 2,
            "a superseding create continues the id's version line from the \
             tombstone rather than restarting it — the predecessor reached 1, \
             so the supersede answers 2"
        );

        // The recreated collection is genuinely empty: the pre-delete doc
        // must not be visible, and stats must report zero docs.
        assert_eq!(e.stats("a").unwrap().documents_indexed, 0);
        let hits = e
            .search(
                "a",
                SearchRequest {
                    query: QueryNode::Term(TermQuery {
                        field: "email".into(),
                        value: FieldValue::String("old1@x.com".into()),
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
            .unwrap();
        assert_eq!(
            hits.total, 0,
            "the recreated collection must not surface the deleted \
             predecessor's documents"
        );
    }

    /// #3953(a): a superseding create must not additive-merge the new
    /// request's schema onto the deleted predecessor's — a field the
    /// deleted collection had but the new request omits must be gone, not
    /// carried forward.
    #[test]
    fn create_collection_supersede_does_not_inherit_deleted_schema() {
        let e = Engine::new();
        let mut wide_fields = BTreeMap::new();
        wide_fields.insert(
            "email".into(),
            FieldSpec {
                field_type: FieldType::Keyword,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        wide_fields.insert(
            "extra".into(),
            FieldSpec {
                field_type: FieldType::Keyword,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        e.create_collection(
            "a",
            CreateCollectionRequest {
                fields: wide_fields,
            },
        )
        .unwrap();
        e.drop_collection("a", false).unwrap();

        let resp = e.create_collection("a", kw_only_schema()).unwrap();
        assert_eq!(
            resp.fields_count, 1,
            "a superseding create must start from the new request's schema \
             alone, not merge in the deleted predecessor's extra field"
        );
    }

    /// #3953: `sweep_deleted`'s grace-window path must keep working for a
    /// collection that is deleted and never recreated — this fix only
    /// changes what an explicit `create_collection` (PUT) call does to a
    /// tombstone; it must not remove or shortcut the background sweep.
    #[test]
    fn sweep_deleted_still_reclaims_a_tombstone_nobody_recreates() {
        let e = Engine::new();
        e.create_collection("a", kw_only_schema()).unwrap();
        assert_eq!(e.drop_collection("a", false).unwrap(), DropOutcome::Marked);

        // Immediately: the grace window has not elapsed, sweep does nothing.
        assert_eq!(e.sweep_deleted(Duration::from_secs(3600)).unwrap(), 0);
        assert!(e.stats("a").is_err(), "still tombstoned, still 410");

        // A zero grace window sweeps it away right away — proving the sweep
        // path itself, independent of any PUT, still physically reclaims a
        // tombstone that nobody recreated.
        assert_eq!(e.sweep_deleted(Duration::from_secs(0)).unwrap(), 1);
        assert!(
            e.stats("a").is_err(),
            "physically removed collection must still report not-found, \
             not silently reappear as live"
        );
    }

    /// #1397 R2 / AC2: `evict_not_owned` must publish the ENGINE-WIDE byte
    /// total (summed across every live collection) after eviction, not just
    /// whichever collection the loop happened to touch last. Two
    /// collections, both with enough documents spread across the shard
    /// map's virtual buckets that eviction touches both (a real
    /// `balanced()` map, same shape production reshard uses) — the old
    /// last-writer-wins gauge update would equal only the
    /// later-in-iteration-order collection's post-eviction bytes ("b"
    /// sorts after "a" in the `BTreeMap` the loop walks), not the sum.
    #[test]
    fn evict_not_owned_publishes_engine_wide_byte_total() {
        let e = Engine::new();
        e.create_collection("a", kw_only_schema()).unwrap();
        e.create_collection("b", kw_only_schema()).unwrap();
        for i in 0..40 {
            index_kw(&e, "a", &format!("a{i:02}"));
        }
        for i in 0..40 {
            index_kw(&e, "b", &format!("b{i:02}"));
        }

        let map = VirtualBucketShardMap::balanced(1, 8, 2).unwrap();
        let touches_a =
            (0..40).any(|i| map.route_document("a", None, &format!("a{i:02}")).shard != 0);
        let touches_b =
            (0..40).any(|i| map.route_document("b", None, &format!("b{i:02}")).shard != 0);
        assert!(
            touches_a && touches_b,
            "fixture must evict from both collections to exercise the \
             engine-wide sum (touches_a={touches_a}, touches_b={touches_b})"
        );

        let outcome = e.evict_not_owned(&map, 0).unwrap();
        assert_eq!(outcome.collections_touched, 2);
        assert!(outcome.documents_evicted > 0);

        let expected_total = collection_live_bytes(&e, "a") + collection_live_bytes(&e, "b");
        assert!(expected_total > 0);
        assert_eq!(
            e.metrics().storage_bytes.get(),
            expected_total,
            "gauge must equal the sum of both collections' post-eviction bytes, \
             not just one of them"
        );
    }

    /// #1397 R2 / AC2: `stats()` has the same last-writer-wins defect as
    /// `evict_not_owned` — it must also publish the engine-wide byte total,
    /// summed across every live collection, even though the API response it
    /// returns stays scoped to the one requested collection.
    #[test]
    fn stats_publishes_engine_wide_byte_total() {
        let e = Engine::new();
        e.create_collection("a", kw_only_schema()).unwrap();
        e.create_collection("b", kw_only_schema()).unwrap();
        for i in 0..10 {
            index_kw(&e, "a", &format!("a{i:02}"));
        }
        for i in 0..25 {
            index_kw(&e, "b", &format!("b{i:02}"));
        }

        let bytes_a = collection_live_bytes(&e, "a");
        let bytes_b = collection_live_bytes(&e, "b");
        assert!(bytes_a > 0 && bytes_b != bytes_a);

        // Calling stats on the SMALLER collection last is the case the old
        // per-collection-only update got wrong: a last-writer-wins gauge
        // would equal `bytes_a` alone, not `bytes_a + bytes_b`.
        let stats_b = e.stats("b").unwrap();
        assert_eq!(stats_b.storage.total_bytes, bytes_b);
        e.stats("a").unwrap();

        assert_eq!(
            e.metrics().storage_bytes.get(),
            bytes_a + bytes_b,
            "gauge must equal the sum across both collections after a \
             single-collection stats() call"
        );
    }

    // ---- #1467 R1/R2/R4: prune-chunk accumulator hardening -----------

    fn prune_test_schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert(
            "email".into(),
            FieldSpec {
                field_type: FieldType::Keyword,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        CreateCollectionRequest { fields }
    }

    fn prune_index_user(e: &Engine, collection: &str, eid: &str) {
        e.index(
            collection,
            IndexRequest {
                items: vec![item(
                    eid,
                    "email",
                    FieldValue::String(format!("{eid}@x.com")),
                )],
                request_id: None,
            },
        )
        .unwrap();
    }

    fn prune_has_doc(e: &Engine, collection: &str, eid: &str) -> bool {
        let resp = e
            .search(
                collection,
                SearchRequest {
                    query: QueryNode::Term(TermQuery {
                        field: "email".into(),
                        value: FieldValue::String(format!("{eid}@x.com")),
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
            .unwrap();
        resp.total == 1
    }

    fn prune_bucket_of(collection_id: &str, external_id: &str) -> u32 {
        VirtualBucketShardMap::balanced(0, 4, 1)
            .unwrap()
            .route_document(collection_id, None, external_id)
            .bucket
    }

    fn prune_chunk(
        to_map_version: u64,
        bucket: u32,
        collection_id: &str,
        chunk_index: u32,
        total_chunks: u32,
        keep_ids: &[&str],
    ) -> crate::sharding::domain::prune_chunk::ReshardPruneChunk {
        crate::sharding::domain::prune_chunk::ReshardPruneChunk {
            to_map_version,
            bucket,
            virtual_bucket_count: 4,
            collection_id: collection_id.to_string(),
            chunk_index,
            total_chunks,
            keep_ids: keep_ids.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// #1467 R1/AC1: two chunks that BOTH complete the same group (a
    /// duplicate final chunk racing its original, e.g. a client retry
    /// in flight concurrently with the original request) must never both
    /// observe "ready" and neither may ever prune against an empty keep
    /// set — the fixed readiness-check-and-removal is one critical section,
    /// so exactly one call drains the group with the correct, fully-unioned
    /// keep set and every later racer starts a fresh, empty accumulation.
    #[test]
    fn apply_reshard_prune_chunk_concurrent_completions_never_use_empty_keep_set() {
        let e = std::sync::Arc::new(Engine::new());
        e.create_collection("u", prune_test_schema()).unwrap();
        prune_index_user(&e, "u", "kept-1");
        prune_index_user(&e, "u", "kept-2");
        prune_index_user(&e, "u", "dropped");
        // The scope only prunes documents that route to `bucket` — derive it
        // from "dropped" itself so the doc under test is actually in scope
        // regardless of where "kept-1"/"kept-2" happen to hash (they are
        // safe either way: present in `keep_ids`, so kept if co-bucketed,
        // and untouched if not).
        let bucket = prune_bucket_of("u", "dropped");

        // Two threads race the SAME final chunk of a 1-chunk group — the
        // keep set never includes "dropped", so a correct outcome always
        // prunes exactly it, exactly once (a re-run against already-pruned
        // state prunes 0 more), and never wipes "kept-1"/"kept-2".
        let mut handles = Vec::new();
        for _ in 0..8 {
            let e = e.clone();
            handles.push(std::thread::spawn(move || {
                e.apply_reshard_prune_chunk(prune_chunk(
                    1,
                    bucket,
                    "u",
                    0,
                    1,
                    &["kept-1", "kept-2"],
                ))
                .unwrap()
            }));
        }
        let outcomes: Vec<ReshardPruneOutcome> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();

        assert!(
            outcomes.iter().all(|o| o.complete),
            "every racer completes its own 1-chunk group: {outcomes:?}"
        );
        let total_pruned: u32 = outcomes.iter().map(|o| o.documents_pruned).sum();
        assert_eq!(
            total_pruned, 1,
            "the dropped doc must be pruned exactly once across every racer, \
             never an empty-keep-set full-bucket wipe: {outcomes:?}"
        );
        assert!(prune_has_doc(&e, "u", "kept-1"));
        assert!(prune_has_doc(&e, "u", "kept-2"));
        assert!(!prune_has_doc(&e, "u", "dropped"));
    }

    /// #1467 R2/AC2: an abandoned pass that only sent chunk 0 of a
    /// multi-chunk group (driver crash/restart mid-pass) leaves a stale
    /// partial accumulation. A retried pass's fresh `chunk_index == 0` must
    /// reset it rather than union into it — otherwise a keep_id carried
    /// over from the abandoned attempt could resurrect a doc the retried
    /// pass's own keep set actually drops.
    #[test]
    fn apply_reshard_prune_chunk_chunk_index_zero_resets_stale_partial() {
        let e = Engine::new();
        e.create_collection("u", prune_test_schema()).unwrap();
        prune_index_user(&e, "u", "kept");
        prune_index_user(&e, "u", "stale-only");
        // The scope only prunes documents that route to `bucket` — derive it
        // from "stale-only" itself so the doc under test is actually in
        // scope. "kept" is safe either way: it is always in the retried
        // pass's `keep_ids`, so it survives whether or not it shares a
        // bucket with "stale-only".
        let bucket = prune_bucket_of("u", "stale-only");

        // Abandoned first pass: chunk 0 of 2 lands, keeping "stale-only"
        // (as if the retried pass's keep set will differ); chunk 1 never
        // arrives.
        let out = e
            .apply_reshard_prune_chunk(prune_chunk(1, bucket, "u", 0, 2, &["stale-only"]))
            .unwrap();
        assert!(!out.complete);

        // Retried pass restarts from chunk 0 with the corrected keep set
        // (drops "stale-only", keeps "kept"), then completes with chunk 1.
        let out = e
            .apply_reshard_prune_chunk(prune_chunk(1, bucket, "u", 0, 2, &["kept"]))
            .unwrap();
        assert!(!out.complete);
        let out = e
            .apply_reshard_prune_chunk(prune_chunk(1, bucket, "u", 1, 2, &[]))
            .unwrap();
        assert!(out.complete);

        assert!(
            prune_has_doc(&e, "u", "kept"),
            "the retried pass's own keep set must be honored"
        );
        assert!(
            !prune_has_doc(&e, "u", "stale-only"),
            "the abandoned pass's stale chunk-0 keep_id must not have survived \
             the chunk_index==0 reset"
        );
    }

    /// #1467 R4/AC4: `total_chunks == 0` and `total_chunks` beyond the sanity
    /// cap are both rejected before ever touching the accumulator.
    #[test]
    fn apply_reshard_prune_chunk_rejects_invalid_total_chunks() {
        let e = Engine::new();
        e.create_collection("u", prune_test_schema()).unwrap();

        for total_chunks in [0, PRUNE_ACCUM_MAX_TOTAL_CHUNKS + 1] {
            let err = e
                .apply_reshard_prune_chunk(prune_chunk(1, 0, "u", 0, total_chunks, &[]))
                .unwrap_err();
            assert!(
                matches!(
                    err.downcast_ref::<StorageError>(),
                    Some(StorageError::InvalidPruneChunk { .. })
                ),
                "total_chunks={total_chunks} must be rejected as InvalidPruneChunk: {err:?}"
            );
        }
    }

    /// #1467 R4/AC4: once [`PRUNE_ACCUM_MAX_ENTRIES`] distinct incomplete
    /// groups are already held, a brand-new key is rejected rather than
    /// growing the accumulator without bound; an already-tracked key may
    /// still make progress.
    #[test]
    fn apply_reshard_prune_chunk_rejects_new_key_once_accumulator_is_full() {
        let e = Engine::new();
        e.create_collection("u", prune_test_schema()).unwrap();

        // Every filler key uses a 3-chunk group and only ever receives chunk
        // 0, so all `PRUNE_ACCUM_MAX_ENTRIES` entries stay held (incomplete,
        // never removed) at once.
        for v in 0..PRUNE_ACCUM_MAX_ENTRIES as u64 {
            let out = e
                .apply_reshard_prune_chunk(prune_chunk(v, 0, "u", 0, 3, &[]))
                .unwrap();
            assert!(!out.complete);
        }

        // An already-tracked key still makes progress without completing
        // (2 of 3 chunks received) — the accumulator count must not drop,
        // so the capacity check below still holds.
        let out = e
            .apply_reshard_prune_chunk(prune_chunk(0, 0, "u", 1, 3, &[]))
            .unwrap();
        assert!(!out.complete);

        // A brand-new key is rejected: the accumulator is at capacity.
        let err = e
            .apply_reshard_prune_chunk(prune_chunk(
                PRUNE_ACCUM_MAX_ENTRIES as u64,
                0,
                "u",
                0,
                3,
                &[],
            ))
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<StorageError>(),
                Some(StorageError::PruneAccumulatorFull { .. })
            ),
            "a new key beyond PRUNE_ACCUM_MAX_ENTRIES must be rejected: {err:?}"
        );
    }

    /// #1467 R4/AC4: an incomplete group older than
    /// [`PRUNE_ACCUM_MAX_AGE_TICKS`] is age-GC'd on a later call — its
    /// earlier chunks are gone, so a later chunk for the same key starts a
    /// fresh (still-incomplete) accumulation instead of completing.
    #[test]
    fn apply_reshard_prune_chunk_gc_evicts_stale_incomplete_groups_by_age() {
        let e = Engine::new();
        e.create_collection("u", prune_test_schema()).unwrap();

        // Key under test: only chunk 0 of 2 ever lands.
        let out = e
            .apply_reshard_prune_chunk(prune_chunk(999, 0, "u", 0, 2, &[]))
            .unwrap();
        assert!(!out.complete);

        // Advance the tick well past PRUNE_ACCUM_MAX_AGE_TICKS via distinct,
        // SELF-COMPLETING single-chunk (total_chunks=1) groups — each is
        // removed from the accumulator the moment it lands, so this loop
        // advances the tick counter without ever growing the accumulator
        // past `PRUNE_ACCUM_MAX_ENTRIES` (which would otherwise reject
        // long before the age budget is reached, since
        // `PRUNE_ACCUM_MAX_AGE_TICKS` far exceeds `PRUNE_ACCUM_MAX_ENTRIES`).
        for v in 0..(PRUNE_ACCUM_MAX_AGE_TICKS + 2) {
            let out = e
                .apply_reshard_prune_chunk(prune_chunk(2_000_000 + v, 0, "u", 0, 1, &[]))
                .unwrap();
            assert!(out.complete);
        }

        // The key under test's chunk 0 must have been age-GC'd: its
        // "final" chunk 1 now starts a fresh, still-incomplete group rather
        // than completing.
        let out = e
            .apply_reshard_prune_chunk(prune_chunk(999, 0, "u", 1, 2, &[]))
            .unwrap();
        assert!(
            !out.complete,
            "chunk 0 of the aged-out group must have been GC'd, so chunk 1 alone \
             cannot complete a fresh 2-chunk group: {out:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Triple-path diff test (Stage 2 Phase 2f-1): the RAM=hot / disk=all keystone.
//
// PATH A = a pure live engine (segments OFF).
// PATH B = the SAME engine after `seal_to_segments` + forward-payload drop.
// PATH C = a fresh engine whose collection is `open_from_segments`'d from B's
//          directory (NO CBOR snapshot, NO whole-collection load).
//
// Over a randomized multi-field corpus (Number + Keyword + Set + Text + Hash +
// Vector) the three paths must return IDENTICAL result-SETS, byte-identical f32
// scores (to_bits), identical retrieved field values, and identical kNN
// ordering — for point lookups, range, term, set-membership, BM25, kNN, AND
// direct value retrieval. A missed forward-read site or a wrong inverted-driver
// rebuild MUST fail this. The test also asserts the forward payload provably
// left RAM after the seal-and-drop.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod triple_path_diff_tests {
    use super::*;
    use crate::shared_kernel::types::schema::{VectorBackend, VectorMetric};
    use proptest::prelude::*;
    use std::sync::Arc;

    const DIM: usize = 6;

    /// One full query-battery snapshot of an engine: the seven query legs plus
    /// the direct value-retrieval leg. Fields (in order): range, term, setmem,
    /// point, bm25, knn (ordered), hamming, retrieval.
    type Snapshot = (
        Vec<(String, u32)>,
        Vec<(String, u32)>,
        Vec<(String, u32)>,
        Vec<(String, u32)>,
        Vec<(String, u32)>,
        Vec<(String, u32)>,
        Vec<(String, u32)>,
        Vec<(
            String,
            Option<u64>,
            Option<String>,
            Option<Vec<String>>,
            Option<u64>,
        )>,
    );

    fn fieldspec(t: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    fn vec_fieldspec() -> FieldSpec {
        FieldSpec {
            field_type: FieldType::Vector,
            analyzer: None,
            multi: None,
            dim: Some(DIM as u32),
            metric: Some(VectorMetric::L2),
            backend: Some(VectorBackend::FlatCpu),
            quantize: None,
        }
    }

    /// Multi-field corpus: num (Number), kw (Keyword), tags (Set), body (Text),
    /// sig (Hash), emb (Vector).
    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("num".into(), fieldspec(FieldType::Number, None));
        fields.insert("kw".into(), fieldspec(FieldType::Keyword, None));
        fields.insert("tags".into(), fieldspec(FieldType::Set, None));
        fields.insert(
            "body".into(),
            fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
        );
        fields.insert("sig".into(), fieldspec(FieldType::Hash, None));
        fields.insert("emb".into(), vec_fieldspec());
        CreateCollectionRequest { fields }
    }

    fn req(query: QueryNode, limit: u32) -> SearchRequest {
        SearchRequest {
            query,
            limit,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    fn run(e: &Engine, query: QueryNode, limit: u32) -> Vec<(String, u32)> {
        e.search("c", req(query, limit))
            .unwrap()
            .hits
            .into_iter()
            .map(|h| (h.external_id, h.score.to_bits()))
            .collect()
    }

    /// Result SET (eids only) — order-independent assertion for filter queries.
    fn set_of(rows: &[(String, u32)]) -> BTreeSet<String> {
        rows.iter().map(|(e, _)| e.clone()).collect()
    }

    /// Scores keyed by eid (f32 bits) — order-independent score byte-equality.
    fn scores_of(rows: &[(String, u32)]) -> BTreeMap<String, u32> {
        rows.iter().map(|(e, s)| (e.clone(), *s)).collect()
    }

    /// Index one doc across all six fields. `tok` selects the rare driver token
    /// so BM25 has a non-trivial corpus.
    #[allow(clippy::too_many_arguments)]
    fn index_doc(
        e: &Engine,
        eid: &str,
        num: Option<f64>,
        kw: &str,
        tags: &[&str],
        tok: bool,
        sig: u64,
        emb: &[f32],
    ) {
        let mut items = vec![
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "kw".into(),
                value: FieldValue::String(kw.into()),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "tags".into(),
                value: FieldValue::StringList(tags.iter().map(|s| s.to_string()).collect()),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "body".into(),
                value: FieldValue::String(if tok {
                    "tok filler".into()
                } else {
                    "filler".into()
                }),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "sig".into(),
                value: FieldValue::String(format!("{sig:016x}")),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "emb".into(),
                value: FieldValue::Vector(emb.to_vec()),
                version: None,
            },
        ];
        if let Some(n) = num {
            items.push(crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "num".into(),
                value: FieldValue::Number(n),
                version: None,
            });
        }
        e.index(
            "c",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }

    /// A match-DRIVEN AND so the non-text conjunct is applied as a per-doc
    /// PREDICATE (`number_at`/`keyword_at`/`set_contains` — the segment-backed
    /// read sites), not a posting-walk.
    fn driven(extra: QueryNode) -> QueryNode {
        QueryNode::And(vec![
            QueryNode::Match(MatchQuery {
                field: "body".into(),
                text: "tok".into(),
                op: MatchOp::And,
            }),
            extra,
        ])
    }

    /// Direct field-value RETRIEVAL through the segment-aware accessors, for
    /// every docid — the "value retrieval" leg of the contract. Resolves each
    /// docid to (eid, num, kw, tags, sig). Routes through `number_at` /
    /// `keyword_at` / `set_members` / `hash_at`, which after a seal-and-drop must
    /// read the segment, not the (empty) forward map.
    fn retrieve_all(
        e: &Engine,
    ) -> Vec<(
        String,
        Option<u64>,
        Option<String>,
        Option<Vec<String>>,
        Option<u64>,
    )> {
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let n = coll.interner.to_eid.len() as u32;
        let mut out = Vec::with_capacity(n as usize);
        for id in 0..n {
            let eid = coll.interner.resolve(id).to_string();
            let num = match coll.fields.get("num") {
                Some(FieldIndex::Number(nx)) => nx.number_at(id).map(|s| s.to_f64().to_bits()),
                _ => None,
            };
            let kw = match coll.fields.get("kw") {
                Some(FieldIndex::Keyword(k)) => k.keyword_at(id),
                _ => None,
            };
            let tags = match coll.fields.get("tags") {
                Some(FieldIndex::Set(s)) => s.set_members(id).map(|m| m.into_iter().collect()),
                _ => None,
            };
            let sig = match coll.fields.get("sig") {
                Some(FieldIndex::Hash(h)) => h.hash_at(id),
                _ => None,
            };
            out.push((eid, num, kw, tags, sig));
        }
        // Order by eid so the comparison is interner-order-independent (PATH C
        // re-interns in docid order, which equals PATH A/B docid order, but
        // sorting makes the contract explicit).
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(80))]

        #[test]
        fn triple_path_a_eq_b_eq_c(
            docs in proptest::collection::vec(
                (
                    proptest::option::weighted(0.8, 0u32..30),          // num (some absent)
                    prop::sample::select(vec!["a", "b", "c", "d"]),     // kw
                    proptest::collection::vec(
                        prop::sample::select(vec!["red", "green", "blue", "x"]),
                        0..3,
                    ),                                                  // tags
                    any::<bool>(),                                      // tok
                    0u64..64,                                           // sig (low bits)
                    proptest::collection::vec(-3.0f32..3.0, DIM..=DIM), // emb
                ),
                1..40,
            ),
            lo in 0u32..30,
            span in 1u32..30,
            qsig in 0u64..64,
            qraw in proptest::collection::vec(-3.0f32..3.0, DIM..=DIM),
            kw_pick in prop::sample::select(vec!["a", "b", "c", "d"]),
            tag_pick in prop::sample::select(vec!["red", "green", "blue", "x"]),
        ) {
            let hi = lo + span;

            // --- PATH A: live engine, segments OFF. ---
            let e = Arc::new(Engine::new());
            e.create_collection("c", schema()).unwrap();
            for (i, (num, kw, tags, tok, sig, emb)) in docs.iter().enumerate() {
                let tagrefs: Vec<&str> = tags.iter().copied().collect();
                index_doc(
                    &e,
                    &format!("d{i}"),
                    num.map(|n| n as f64),
                    kw,
                    &tagrefs,
                    *tok,
                    *sig,
                    emb,
                );
            }

            // The query battery (built once, reused across paths).
            let q_range = || driven(QueryNode::Range(RangeQuery {
                field: "num".into(), gt: None, gte: Some(RangeBound::Number(lo as f64)), lt: Some(RangeBound::Number(hi as f64)), lte: None,
            }));
            let q_term = || driven(QueryNode::Term(TermQuery {
                field: "kw".into(), value: FieldValue::String(kw_pick.to_string()),
            }));
            let q_setmem = || driven(QueryNode::Terms(TermsQuery {
                field: "tags".into(),
                values: vec![FieldValue::String(tag_pick.to_string())],
            }));
            let q_point = || QueryNode::Term(TermQuery {
                field: "kw".into(), value: FieldValue::String(kw_pick.to_string()),
            });
            let q_bm25 = || QueryNode::Match(MatchQuery {
                field: "body".into(), text: "tok".into(), op: MatchOp::And,
            });
            let q_knn = || QueryNode::Knn(crate::shared_kernel::types::query::KnnQuery {
                field: "emb".into(), vector: qraw.clone(), k: 8,
            });
            let q_ham = || QueryNode::Hamming(crate::shared_kernel::types::query::HammingQuery {
                field: "sig".into(), hash: format!("{qsig:016x}"), max_distance: 6,
            });

            let snapshot = |e: &Engine| -> Snapshot {
                (
                    run(e, q_range(), 100_000),
                    run(e, q_term(), 100_000),
                    run(e, q_setmem(), 100_000),
                    run(e, q_point(), 100_000),
                    run(e, q_bm25(), 100_000),
                    run(e, q_knn(), 8),
                    run(e, q_ham(), 100_000),
                    retrieve_all(e),
                )
            };

            let a = snapshot(&e);

            // --- PATH B: production seal_to_segments + drop, rerun. ---
            let dir = tempfile::tempdir().unwrap();
            e.__seal_collection_to_segments("c", dir.path(), 1).unwrap();
            let b = snapshot(&e);

            // The forward payload provably LEFT RAM (and the inverted driver did
            // NOT): every dropped field's forward map is empty / tokens dropped,
            // yet a segment is attached and queries still answer.
            for f in ["num", "kw", "tags", "sig"] {
                let (fwd, _toks, has_seg) = e.__field_forward_probe("c", f).unwrap();
                prop_assert_eq!(fwd, 0, "field `{}` forward map not freed after drop", f);
                prop_assert!(has_seg, "field `{}` has no segment after seal", f);
            }
            let (_f, toks, has_seg) = e.__field_forward_probe("c", "body").unwrap();
            prop_assert_eq!(toks, 0, "text tokens not freed after drop");
            prop_assert!(has_seg, "text field has no segment after seal");

            // --- PATH C: reopen from segments (no snapshot), rerun. ---
            let schema = e.__collection_schema("c").unwrap();
            let ce = Engine::__open_collection_from_segments("c", dir.path(), schema, 1).unwrap();
            let c = snapshot(&ce);

            // A == B and B == C, leg by leg. Filter legs compare result SET +
            // byte-identical scores; kNN compares the full ordered ranked vec;
            // retrieval compares the resolved field values. Filter tuple fields:
            // 0 range, 1 term, 2 setmem, 3 point, 4 bm25, 6 ham (5 knn, 7 retrieval
            // are compared separately because their contract is ordered / value).
            let filt = |x: &Snapshot| {
                vec![
                    (set_of(&x.0), scores_of(&x.0)),
                    (set_of(&x.1), scores_of(&x.1)),
                    (set_of(&x.2), scores_of(&x.2)),
                    (set_of(&x.3), scores_of(&x.3)),
                    (set_of(&x.4), scores_of(&x.4)),
                    (set_of(&x.6), scores_of(&x.6)),
                ]
            };
            let names = ["range", "term", "setmem", "point", "bm25", "hamming"];
            let (fa, fb, fc) = (filt(&a), filt(&b), filt(&c));
            for (i, name) in names.iter().enumerate() {
                prop_assert_eq!(&fa[i].0, &fb[i].0, "A!=B {} set", name);
                prop_assert_eq!(&fa[i].1, &fb[i].1, "A!=B {} scores", name);
                prop_assert_eq!(&fb[i].0, &fc[i].0, "B!=C {} set", name);
                prop_assert_eq!(&fb[i].1, &fc[i].1, "B!=C {} scores", name);
            }
            // kNN is RANKED: the whole ordered vec must match bit-for-bit.
            prop_assert_eq!(&a.5, &b.5, "A!=B knn ordering/score");
            prop_assert_eq!(&b.5, &c.5, "B!=C knn ordering/score");
            // Value retrieval through the segment-aware accessors.
            prop_assert_eq!(&a.7, &b.7, "A!=B value retrieval");
            prop_assert_eq!(&b.7, &c.7, "B!=C value retrieval");
        }
    }
}

// ---------------------------------------------------------------------------
// Engine-level checkpoint (Stage 2 Phase 2f-2): the disk engine as the running
// binary's persistence. Two contracts:
//   (a) ENGINE REOPEN — a multi-collection, all-field-type engine, flushed to a
//       checkpoint dir and reopened into a FRESH engine, answers every query leg
//       identically (the disk engine IS a faithful persistence).
//   (b) IDEMPOTENT DOUBLE-FLUSH — flush, index MORE docs, flush again, reopen
//       yields ALL docs (base + tail) identical to a pure-live engine. This is
//       the re-seal-after-drop proof: the second flush reads base docs from the
//       prior segment (their live forward was dropped), not the empty forward map.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod checkpoint_engine_tests {
    use super::*;
    use crate::shared_kernel::types::{
        query::{KnnQuery, MatchOp, MatchQuery, RangeQuery, TermQuery, TermsQuery},
        schema::{VectorBackend, VectorMetric},
    };
    use std::sync::Arc;

    const DIM: usize = 4;

    fn fieldspec(t: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    fn vec_fieldspec() -> FieldSpec {
        FieldSpec {
            field_type: FieldType::Vector,
            analyzer: None,
            multi: None,
            dim: Some(DIM as u32),
            metric: Some(VectorMetric::L2),
            backend: Some(VectorBackend::FlatCpu),
            quantize: None,
        }
    }

    /// Multi-field corpus matching the triple-path schema: num (Number), kw
    /// (Keyword), tags (Set), body (Text), sig (Hash), emb (Vector).
    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("num".into(), fieldspec(FieldType::Number, None));
        fields.insert("kw".into(), fieldspec(FieldType::Keyword, None));
        fields.insert("tags".into(), fieldspec(FieldType::Set, None));
        fields.insert(
            "body".into(),
            fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
        );
        fields.insert("sig".into(), fieldspec(FieldType::Hash, None));
        fields.insert("emb".into(), vec_fieldspec());
        CreateCollectionRequest { fields }
    }

    fn index_doc(
        e: &Engine,
        coll: &str,
        eid: &str,
        n: f64,
        kw: &str,
        tag: &str,
        tok: bool,
        sig: u64,
        emb: &[f32],
    ) {
        let items = vec![
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "num".into(),
                value: FieldValue::Number(n),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "kw".into(),
                value: FieldValue::String(kw.into()),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "tags".into(),
                value: FieldValue::StringList(vec![tag.into()]),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "body".into(),
                value: FieldValue::String(if tok {
                    "tok filler".into()
                } else {
                    "filler".into()
                }),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "sig".into(),
                value: FieldValue::String(format!("{sig:016x}")),
                version: None,
            },
            crate::shared_kernel::types::document::IndexItem {
                external_id: eid.into(),
                field: "emb".into(),
                value: FieldValue::Vector(emb.to_vec()),
                version: None,
            },
        ];
        e.index(
            coll,
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }

    fn req(query: QueryNode, limit: u32) -> SearchRequest {
        SearchRequest {
            query,
            limit,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    fn run(e: &Engine, coll: &str, query: QueryNode, limit: u32) -> Vec<(String, u32)> {
        e.search(coll, req(query, limit))
            .unwrap()
            .hits
            .into_iter()
            .map(|h| (h.external_id, h.score.to_bits()))
            .collect()
    }

    fn set_of(rows: &[(String, u32)]) -> BTreeSet<String> {
        rows.iter().map(|(e, _)| e.clone()).collect()
    }
    fn scores_of(rows: &[(String, u32)]) -> BTreeMap<String, u32> {
        rows.iter().map(|(e, s)| (e.clone(), *s)).collect()
    }

    fn driven(extra: QueryNode) -> QueryNode {
        QueryNode::And(vec![
            QueryNode::Match(MatchQuery {
                field: "body".into(),
                text: "tok".into(),
                op: MatchOp::And,
            }),
            extra,
        ])
    }

    /// The full query battery for one collection (predicate legs go through the
    /// segment-aware per-doc accessors; kNN/hamming/bm25 through the segment scan).
    fn battery(e: &Engine, coll: &str) -> Vec<(BTreeSet<String>, BTreeMap<String, u32>)> {
        let legs = vec![
            driven(QueryNode::Range(RangeQuery {
                field: "num".into(),
                gt: None,
                gte: Some(RangeBound::Number(2.0)),
                lt: Some(RangeBound::Number(8.0)),
                lte: None,
            })),
            driven(QueryNode::Term(TermQuery {
                field: "kw".into(),
                value: FieldValue::String("a".into()),
            })),
            driven(QueryNode::Terms(TermsQuery {
                field: "tags".into(),
                values: vec![FieldValue::String("red".into())],
            })),
            QueryNode::Term(TermQuery {
                field: "kw".into(),
                value: FieldValue::String("b".into()),
            }),
            QueryNode::Match(MatchQuery {
                field: "body".into(),
                text: "tok".into(),
                op: MatchOp::And,
            }),
            QueryNode::Hamming(crate::shared_kernel::types::query::HammingQuery {
                field: "sig".into(),
                hash: format!("{:016x}", 0u64),
                max_distance: 8,
            }),
        ];
        legs.into_iter()
            .map(|q| {
                let r = run(e, coll, q, 100_000);
                (set_of(&r), scores_of(&r))
            })
            .collect()
    }

    fn knn(e: &Engine, coll: &str, q: &[f32]) -> Vec<(String, u32)> {
        run(
            e,
            coll,
            QueryNode::Knn(KnnQuery {
                field: "emb".into(),
                vector: q.to_vec(),
                k: 8,
            }),
            8,
        )
    }

    // Some fixed multi-collection corpus. Two collections, all field types.
    fn seed(e: &Engine) {
        e.create_collection("alpha", schema()).unwrap();
        e.create_collection("beta", schema()).unwrap();
        let docs = [
            ("d0", 1.0, "a", "red", true, 0u64, [0.1f32, 0.2, 0.3, 0.4]),
            ("d1", 3.0, "b", "blue", true, 3, [0.9, 0.8, 0.7, 0.6]),
            ("d2", 5.0, "a", "red", false, 7, [0.5, 0.5, 0.5, 0.5]),
            ("d3", 7.0, "c", "green", true, 1, [0.2, 0.4, 0.6, 0.8]),
        ];
        for (eid, n, kw, tag, tok, sig, emb) in docs {
            index_doc(e, "alpha", eid, n, kw, tag, tok, sig, &emb);
            index_doc(
                e,
                "beta",
                &format!("b{eid}"),
                n + 1.0,
                kw,
                tag,
                tok,
                sig + 1,
                &emb,
            );
        }
    }

    // ----- (a) ENGINE REOPEN -------------------------------------------------
    #[test]
    fn flush_then_reopen_into_fresh_engine_is_identical() {
        let live = Arc::new(Engine::new());
        seed(&live);

        let qa = [0.15f32, 0.25, 0.35, 0.45];
        let live_battery_alpha = battery(&live, "alpha");
        let live_battery_beta = battery(&live, "beta");
        let live_knn_alpha = knn(&live, "alpha", &qa);

        let dir = tempfile::tempdir().unwrap();
        live.flush_to_segments(dir.path(), 11).unwrap();

        // Fresh engine reopened ONLY from the checkpoint dir (no CBOR, no log).
        let reopened = Arc::new(Engine::new());
        let seq = reopened.reopen_from_segment_dir(dir.path()).unwrap();
        assert_eq!(
            seq, 11,
            "applied_seq must round-trip through the checkpoint"
        );

        assert_eq!(
            reopened.list_collections().unwrap().len(),
            2,
            "both collections reopened"
        );
        assert_eq!(
            battery(&reopened, "alpha"),
            live_battery_alpha,
            "alpha legs diverged after reopen"
        );
        assert_eq!(
            battery(&reopened, "beta"),
            live_battery_beta,
            "beta legs diverged after reopen"
        );
        assert_eq!(
            knn(&reopened, "alpha", &qa),
            live_knn_alpha,
            "alpha kNN diverged after reopen"
        );
    }

    // ----- (b) IDEMPOTENT DOUBLE-FLUSH (re-seal-after-drop proof) -------------
    #[test]
    fn double_flush_with_tail_matches_pure_live() {
        // The persisted engine: seed, FLUSH (drops forward for base docs), index
        // MORE docs (the live tail), FLUSH AGAIN, reopen.
        let persisted = Arc::new(Engine::new());
        seed(&persisted);
        let dir = tempfile::tempdir().unwrap();
        persisted.flush_to_segments(dir.path(), 4).unwrap(); // first checkpoint

        // After the first flush the base docs' forward maps are dropped. Add a
        // tail of new docs whose docids are > the sealed n_docs.
        let tail = [
            (
                "d4",
                2.5,
                "b",
                "red",
                true,
                0u64,
                [0.11f32, 0.22, 0.33, 0.44],
            ),
            ("d5", 6.5, "a", "blue", true, 7, [0.6, 0.6, 0.6, 0.6]),
        ];
        for (eid, n, kw, tag, tok, sig, emb) in tail {
            index_doc(&persisted, "alpha", eid, n, kw, tag, tok, sig, &emb);
            index_doc(
                &persisted,
                "beta",
                &format!("b{eid}"),
                n + 1.0,
                kw,
                tag,
                tok,
                sig + 1,
                &emb,
            );
        }
        // SECOND flush: this RE-SEALS. Base docs must be gathered from the prior
        // segment (their forward is empty), the tail from the live forward. If the
        // gather read raw `forward`, the base docs would seal as ABSENT here.
        persisted.flush_to_segments(dir.path(), 6).unwrap();

        let reopened = Arc::new(Engine::new());
        let seq = reopened.reopen_from_segment_dir(dir.path()).unwrap();
        assert_eq!(seq, 6, "second checkpoint's seq must win");

        // The oracle: a pure-live engine that NEVER flushed, with the SAME docs.
        let pure = Arc::new(Engine::new());
        seed(&pure);
        for (eid, n, kw, tag, tok, sig, emb) in tail {
            index_doc(&pure, "alpha", eid, n, kw, tag, tok, sig, &emb);
            index_doc(
                &pure,
                "beta",
                &format!("b{eid}"),
                n + 1.0,
                kw,
                tag,
                tok,
                sig + 1,
                &emb,
            );
        }

        let qa = [0.15f32, 0.25, 0.35, 0.45];
        // EVERY leg of BOTH collections must match the pure-live oracle — base AND
        // tail docs (sets AND byte-identical scores). This is the re-seal-after-drop
        // correctness proof: the second flush gathered base docs from the prior
        // segment (their forward was dropped) and the tail from the live state.
        assert_eq!(
            battery(&reopened, "alpha"),
            battery(&pure, "alpha"),
            "alpha legs diverged after double-flush"
        );
        assert_eq!(
            battery(&reopened, "beta"),
            battery(&pure, "beta"),
            "beta legs diverged after double-flush"
        );
        assert_eq!(
            knn(&reopened, "alpha", &qa),
            knn(&pure, "alpha", &qa),
            "alpha kNN diverged after double-flush"
        );
        assert_eq!(
            knn(&reopened, "beta", &qa),
            knn(&pure, "beta", &qa),
            "beta kNN diverged after double-flush"
        );

        // And direct doc-count parity (base 4 + tail 2 = 6 per collection).
        assert_eq!(reopened.stats("alpha").unwrap().documents_indexed, 6);
        assert_eq!(reopened.stats("beta").unwrap().documents_indexed, 6);
    }

    /// A keyword replacement of a sealed doc is a live overlay, not a delete.
    /// The base id stays tombstoned so old postings stay hidden until re-seal,
    /// while `keyword_at` must fall through to the overlay to persist the new
    /// value into the next checkpoint.
    #[test]
    fn sealed_keyword_update_survives_reseal_and_cold_reopen() {
        let persisted = Arc::new(Engine::new());
        seed(&persisted);
        index_doc(
            &persisted,
            "alpha",
            "d0",
            1.0,
            "sealed-before",
            "red",
            true,
            0,
            &[0.1, 0.2, 0.3, 0.4],
        );
        let dir = tempfile::tempdir().unwrap();
        persisted.flush_to_segments(dir.path(), 4).unwrap();

        // d0 is a sealed base id. Replacing its keyword must preserve the base
        // tombstone and write the new value into the live forward overlay.
        index_doc(
            &persisted,
            "alpha",
            "d0",
            1.0,
            "updated",
            "red",
            true,
            0,
            &[0.1, 0.2, 0.3, 0.4],
        );
        persisted.delete("alpha", "d1", None).unwrap();

        // A second checkpoint must carry the replacement, and it must not
        // resurrect a true delete whose base value no longer has live coverage.
        persisted.flush_to_segments(dir.path(), 6).unwrap();

        let reopened = Arc::new(Engine::new());
        assert_eq!(reopened.reopen_from_segment_dir(dir.path()).unwrap(), 6);

        let term = |value: &str| {
            set_of(&run(
                &reopened,
                "alpha",
                QueryNode::Term(TermQuery {
                    field: "kw".into(),
                    value: FieldValue::String(value.into()),
                }),
                100,
            ))
        };
        assert_eq!(term("updated"), BTreeSet::from(["d0".to_string()]));
        assert!(
            term("sealed-before").is_empty(),
            "old keyword must stay absent"
        );
        assert!(term("b").is_empty(), "true delete must not resurrect");
        assert_eq!(
            set_of(&run(
                &reopened,
                "alpha",
                QueryNode::Exists(crate::shared_kernel::types::query::ExistsQuery {
                    field: "kw".into()
                }),
                100,
            )),
            BTreeSet::from(["d0".to_string(), "d2".to_string(), "d3".to_string()]),
            "the updated document still exists while the true delete is absent"
        );
    }

    // ----- (c) TOMBSTONE GC ACROSS A CHECKPOINT (Phase 2g-A) -----------------
    //
    // THE CRUX. A base doc DELETED after the first checkpoint must be GC'd by the
    // second checkpoint and stay absent on reopen — never resurrected, never an
    // inflated BM25 corpus. Sequence: seed → flush S1 (base docs' forward dropped,
    // values now ONLY on the immutable segment) → DELETE several base docs (in the
    // sealed range) + index a live tail → flush S2 (re-seal) → reopen fresh.
    //
    // The oracle is a PURE-LIVE engine that ran the identical op sequence but NEVER
    // flushed (no segments at all). The reopened-from-disk engine must match it on
    // every leg: result SETs, byte-identical f32 BM25 scores (corpus scalars must
    // exclude the deleted docs), ordered kNN (deleted vectors absent), retrieved
    // values, and doc_count. Plus a direct assertion that a deleted eid is gone.
    //
    // Without the liveness-aware gather, flush S2's `(0..n_docs).map(number_at)`
    // re-reads the deleted base doc's STALE value off the prior segment and writes
    // it back, so reopen RESURRECTS the doc and the BM25 corpus is inflated.
    #[test]
    fn delete_across_checkpoint_is_gc_not_resurrected() {
        // Persisted engine: seed (d0..d3), checkpoint, delete, tail, checkpoint.
        let persisted = Arc::new(Engine::new());
        seed(&persisted);
        let dir = tempfile::tempdir().unwrap();
        persisted.flush_to_segments(dir.path(), 4).unwrap(); // S1: base forward dropped

        // Delete BASE docs that live entirely in the sealed segment range. d0
        // (tok=true) is in the BM25 corpus, so deleting it MUST shrink doc_count /
        // total_doc_len; d2 (tok=false) exercises a non-text-bearing delete. On
        // beta, delete bd1 (tok=true) so both collections are GC-tested.
        let deletes_alpha = ["d0", "d2"];
        let deletes_beta = ["bd1"];
        for eid in deletes_alpha {
            persisted.delete("alpha", eid, None).unwrap();
        }
        for eid in deletes_beta {
            persisted.delete("beta", eid, None).unwrap();
        }

        // Live tail (docids > sealed n_docs), some sharing the deleted docs' terms
        // so a resurrected base doc would be detectable as an extra set member.
        let tail = [
            (
                "d4",
                2.5,
                "a",
                "red",
                true,
                0u64,
                [0.11f32, 0.22, 0.33, 0.44],
            ),
            ("d5", 6.5, "b", "blue", true, 7, [0.6, 0.6, 0.6, 0.6]),
        ];
        for (eid, n, kw, tag, tok, sig, emb) in tail {
            index_doc(&persisted, "alpha", eid, n, kw, tag, tok, sig, &emb);
            index_doc(
                &persisted,
                "beta",
                &format!("b{eid}"),
                n + 1.0,
                kw,
                tag,
                tok,
                sig + 1,
                &emb,
            );
        }
        persisted.flush_to_segments(dir.path(), 6).unwrap(); // S2: re-seal must GC deletes

        // Fresh reopen ONLY from the checkpoint dir.
        let reopened = Arc::new(Engine::new());
        let seq = reopened.reopen_from_segment_dir(dir.path()).unwrap();
        assert_eq!(seq, 6, "second checkpoint's seq must win");

        // Oracle: pure-live engine, identical op sequence, NEVER flushed.
        let pure = Arc::new(Engine::new());
        seed(&pure);
        for eid in deletes_alpha {
            pure.delete("alpha", eid, None).unwrap();
        }
        for eid in deletes_beta {
            pure.delete("beta", eid, None).unwrap();
        }
        for (eid, n, kw, tag, tok, sig, emb) in tail {
            index_doc(&pure, "alpha", eid, n, kw, tag, tok, sig, &emb);
            index_doc(
                &pure,
                "beta",
                &format!("b{eid}"),
                n + 1.0,
                kw,
                tag,
                tok,
                sig + 1,
                &emb,
            );
        }

        let qa = [0.15f32, 0.25, 0.35, 0.45];
        // Every leg of BOTH collections must match the pure-live oracle, SETS and
        // byte-identical f32 BM25 scores. The BM25 leg of `battery` is the corpus
        // teeth: if a deleted tok=true doc were resurrected, doc_count/avgdl shift
        // and EVERY surviving doc's BM25 score changes — a byte diff.
        assert_eq!(
            battery(&reopened, "alpha"),
            battery(&pure, "alpha"),
            "alpha legs diverged after delete+checkpoint"
        );
        assert_eq!(
            battery(&reopened, "beta"),
            battery(&pure, "beta"),
            "beta legs diverged after delete+checkpoint"
        );
        // Ordered kNN: a resurrected vector row would re-enter the scan and reorder.
        assert_eq!(
            knn(&reopened, "alpha", &qa),
            knn(&pure, "alpha", &qa),
            "alpha kNN diverged after delete+checkpoint"
        );
        assert_eq!(
            knn(&reopened, "beta", &qa),
            knn(&pure, "beta", &qa),
            "beta kNN diverged after delete+checkpoint"
        );

        // doc_count: base 4 - 2 deleted + 2 tail = 4 (alpha); 4 - 1 + 2 = 5 (beta).
        assert_eq!(
            reopened.stats("alpha").unwrap().documents_indexed,
            4,
            "alpha doc_count inflated by resurrected docs"
        );
        assert_eq!(
            reopened.stats("beta").unwrap().documents_indexed,
            5,
            "beta doc_count inflated by resurrected docs"
        );

        // DIRECT GC assertion: every deleted eid is absent from every leg AND from
        // direct value retrieval after reopen — it was GC'd, not resurrected.
        let alpha_hits: BTreeSet<String> = {
            // A broad query that would surface a resurrected doc on any field.
            let mut s = BTreeSet::new();
            for kwv in ["a", "b", "c", "d"] {
                let r = run(
                    &reopened,
                    "alpha",
                    QueryNode::Term(TermQuery {
                        field: "kw".into(),
                        value: FieldValue::String(kwv.into()),
                    }),
                    100_000,
                );
                s.extend(r.into_iter().map(|(e, _)| e));
            }
            s
        };
        for eid in deletes_alpha {
            assert!(
                !alpha_hits.contains(eid),
                "deleted alpha eid `{eid}` RESURRECTED after reopen"
            );
        }
        // And the deleted eid resolves to NO value through the segment-aware
        // accessors (its interner slot is a tombstone, excluded by eid_fields).
        {
            let state = reopened.state.read().unwrap();
            let coll = state.collections.get("alpha").unwrap();
            for eid in deletes_alpha {
                // A deleted eid is still interned (positionally stable docids), but
                // it must carry NO live field coverage.
                if let Some(id) = coll.interner.id(eid) {
                    assert!(
                        coll.eid_fields.get(&id).is_none_or(|fs| fs.is_empty()),
                        "deleted alpha eid `{eid}` (id {id}) still has live field coverage after reopen",
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// #179: an offset cursor combined with `sort` must be REJECTED (400), never
// silently fall through to score ranking and ignore the sort. Sequential
// sorted paging uses the keyset cursor handed back in the response; native
// `offset` provides direct page jumps without client over-fetch.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod offset_sort_guard_tests {
    use super::*;
    use crate::shared_kernel::types::document::IndexItem;

    fn fieldspec(t: FieldType) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("price".into(), fieldspec(FieldType::Number));
        fields.insert("cat".into(), fieldspec(FieldType::Keyword));
        fields.insert("code".into(), fieldspec(FieldType::Keyword));
        CreateCollectionRequest { fields }
    }

    /// Five docs d0..d4 with prices 10,20,30,40,50, all `cat = "x"`.
    fn seed() -> Engine {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        for (i, p) in [10.0_f64, 20.0, 30.0, 40.0, 50.0].iter().enumerate() {
            e.index(
                "c",
                IndexRequest {
                    items: vec![
                        IndexItem {
                            external_id: format!("d{i}"),
                            field: "price".into(),
                            value: FieldValue::Number(*p),
                            version: None,
                        },
                        IndexItem {
                            external_id: format!("d{i}"),
                            field: "cat".into(),
                            value: FieldValue::String("x".into()),
                            version: None,
                        },
                        IndexItem {
                            external_id: format!("d{i}"),
                            field: "code".into(),
                            value: FieldValue::String(format!("k{i}")),
                            version: None,
                        },
                    ],
                    request_id: None,
                },
            )
            .unwrap();
        }
        e
    }

    fn cat_x() -> QueryNode {
        QueryNode::Term(TermQuery {
            field: "cat".into(),
            value: FieldValue::String("x".into()),
        })
    }

    fn base() -> SearchRequest {
        SearchRequest {
            query: cat_x(),
            limit: 2,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        }
    }

    fn sort_price_asc() -> Vec<SortSpec> {
        vec![SortSpec {
            field: "price".into(),
            order: SortOrder::Asc,
            missing: SortMissing::Exclude,
        }]
    }

    fn ids(resp: &SearchResponse) -> Vec<String> {
        resp.hits.iter().map(|h| h.external_id.clone()).collect()
    }

    /// R1: an offset cursor (N>0) + a non-empty sort → 400 UnsupportedSort,
    /// instead of silently mis-ordering by score.
    #[test]
    fn offset_cursor_with_sort_is_rejected() {
        let e = seed();
        let mut r = base();
        r.sort = Some(sort_price_asc());
        r.cursor = Some(make_cursor(2)); // {"offset":2}
        let err = e.search("c", r).unwrap_err();
        let se = err
            .downcast_ref::<StorageError>()
            .expect("offset+sort error must be a StorageError");
        assert!(
            matches!(se, StorageError::UnsupportedSort(_)),
            "offset+sort must map to UnsupportedSort (HTTP 400), got {se:?}"
        );
    }

    /// R2: an offset cursor WITHOUT sort still paginates relevance/constant
    /// results — the guard must not regress the unsorted offset path.
    #[test]
    fn offset_cursor_without_sort_paginates() {
        let e = seed();
        let mut r = base();
        r.cursor = Some(make_cursor(2));
        let resp = e
            .search("c", r)
            .expect("offset cursor without sort must succeed");
        assert_eq!(resp.total, 5, "exact total across the unsorted match set");
        assert!(resp.hits.len() <= 2, "page honors the limit");
    }

    #[test]
    fn native_offset_applies_after_numeric_sort() {
        let e = seed();
        let mut r = base();
        r.sort = Some(sort_price_asc());
        r.offset = 2;
        let resp = e.search("c", r).expect("native sorted offset succeeds");
        assert_eq!(ids(&resp), ["d2", "d3"]);
        assert_eq!(resp.total, 5);
        assert!(
            resp.cursor.is_none(),
            "an offset jump does not emit a cursor"
        );
    }

    #[test]
    fn native_offset_applies_after_keyword_and_composite_sort() {
        let e = seed();
        let mut r = base();
        r.offset = 1;
        r.sort = Some(vec![
            SortSpec {
                field: "cat".into(),
                order: SortOrder::Asc,
                missing: SortMissing::Exclude,
            },
            SortSpec {
                field: "code".into(),
                order: SortOrder::Desc,
                missing: SortMissing::Exclude,
            },
        ]);
        let resp = e
            .search("c", r)
            .expect("native keyword/composite offset succeeds");
        assert_eq!(ids(&resp), ["d3", "d2"]);
    }

    #[test]
    fn native_offset_applies_after_score_ordering() {
        let e = seed();
        let mut r = base();
        r.offset = 2;
        let resp = e.search("c", r).expect("native score offset succeeds");
        assert_eq!(ids(&resp), ["d2", "d3"]);
    }

    #[test]
    fn nonzero_native_offset_and_cursor_are_rejected() {
        let e = seed();
        let mut r = base();
        r.offset = 1;
        r.cursor = Some(make_cursor(1));
        let err = e.search("c", r).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<StorageError>(),
            Some(StorageError::InvalidPagination(_))
        ));
    }

    /// R3: a keyset cursor combined with sort paginates correctly (page 1 with
    /// no cursor hands back a keyset cursor; following it yields the next page).
    #[test]
    fn keyset_cursor_with_sort_paginates() {
        let e = seed();

        let mut p1 = base();
        p1.sort = Some(sort_price_asc());
        let r1 = e.search("c", p1).expect("sorted page 1 must succeed");
        assert_eq!(ids(&r1), vec!["d0".to_string(), "d1".to_string()]);
        let cursor = r1
            .cursor
            .expect("a full sorted page must hand back a keyset cursor");

        let mut p2 = base();
        p2.sort = Some(sort_price_asc());
        p2.cursor = Some(cursor);
        let r2 = e
            .search("c", p2)
            .expect("keyset + sort page 2 must succeed");
        assert_eq!(ids(&r2), vec!["d2".to_string(), "d3".to_string()]);
    }
}

// ---------------------------------------------------------------------------
// #184: external-version last-write-wins. An IndexItem may carry an optional
// `version`; lumen keeps the highest version per (external_id, field) and drops
// strictly-older writes. Absent version = arrival order (today's behavior).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod external_version_lww_tests {
    use super::*;
    use crate::shared_kernel::types::document::IndexItem;

    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert(
            "price".into(),
            FieldSpec {
                field_type: FieldType::Number,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        CreateCollectionRequest { fields }
    }

    fn setup() -> Engine {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        e
    }

    fn write(e: &Engine, eid: &str, price: f64, version: Option<u64>) {
        e.index(
            "c",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: eid.into(),
                    field: "price".into(),
                    value: FieldValue::Number(price),
                    version,
                }],
                request_id: None,
            },
        )
        .unwrap();
    }

    /// external_ids whose `price` equals `price`.
    fn matches_price(e: &Engine, price: f64) -> Vec<String> {
        let req = SearchRequest {
            query: QueryNode::Term(TermQuery {
                field: "price".into(),
                value: FieldValue::Number(price),
            }),
            limit: 100,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        };
        e.search("c", req)
            .unwrap()
            .hits
            .into_iter()
            .map(|h| h.external_id)
            .collect()
    }

    /// R1: a versioned write older than the stored version is dropped.
    #[test]
    fn stale_versioned_write_is_dropped() {
        let e = setup();
        write(&e, "d0", 10.0, Some(5));
        write(&e, "d0", 20.0, Some(3)); // stale: 3 < stored 5
        assert_eq!(
            matches_price(&e, 10.0),
            vec!["d0".to_string()],
            "value must remain at the v5 write"
        );
        assert!(
            matches_price(&e, 20.0).is_empty(),
            "the stale v3 write must not apply"
        );
    }

    /// R2: a newer versioned write advances the cell.
    #[test]
    fn newer_versioned_write_wins() {
        let e = setup();
        write(&e, "d0", 10.0, Some(5));
        write(&e, "d0", 20.0, Some(6)); // newer: 6 > stored 5
        assert_eq!(matches_price(&e, 20.0), vec!["d0".to_string()]);
        assert!(matches_price(&e, 10.0).is_empty());
    }

    /// R3: writes without a version apply in arrival order (last wins) —
    /// unchanged from today.
    #[test]
    fn unversioned_writes_keep_arrival_order() {
        let e = setup();
        write(&e, "d0", 10.0, None);
        write(&e, "d0", 20.0, None);
        assert_eq!(matches_price(&e, 20.0), vec!["d0".to_string()]);
        assert!(matches_price(&e, 10.0).is_empty());
    }
}

// ---------------------------------------------------------------------------
// #180: opt-in `missing: first|last|exclude` on a sort key. exclude (default)
// drops rows lacking the value (today's behavior); first/last keep them, placed
// before/after the present rows, and count them in an exact total.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod sort_missing_tests {
    use super::*;
    use crate::shared_kernel::types::{
        document::IndexItem,
        query::{ExistsQuery, SortMissing},
    };

    fn fieldspec(t: FieldType) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("price".into(), fieldspec(FieldType::Number));
        fields.insert("cat".into(), fieldspec(FieldType::Keyword));
        fields.insert("kw".into(), fieldspec(FieldType::Keyword));
        CreateCollectionRequest { fields }
    }

    fn idx(e: &Engine, eid: &str, price: Option<f64>) {
        let mut items = vec![IndexItem {
            external_id: eid.into(),
            field: "cat".into(),
            value: FieldValue::String("x".into()),
            version: None,
        }];
        if let Some(p) = price {
            items.push(IndexItem {
                external_id: eid.into(),
                field: "price".into(),
                value: FieldValue::Number(p),
                version: None,
            });
        }
        e.index(
            "c",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }

    /// d0=10, d1=20 have a price; d2 has none (all share cat="x").
    fn seed() -> Engine {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        idx(&e, "d0", Some(10.0));
        idx(&e, "d1", Some(20.0));
        idx(&e, "d2", None);
        e
    }

    fn search(
        e: &Engine,
        missing: SortMissing,
        limit: u32,
        cursor: Option<String>,
    ) -> SearchResponse {
        e.search(
            "c",
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "cat".into(),
                    value: FieldValue::String("x".into()),
                }),
                limit,
                offset: 0,
                cursor,
                routing_key: None,
                sort: Some(vec![SortSpec {
                    field: "price".into(),
                    order: SortOrder::Asc,
                    missing,
                }]),
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
    }

    fn ids(r: &SearchResponse) -> Vec<String> {
        r.hits.iter().map(|h| h.external_id.clone()).collect()
    }

    /// R1: missing:last places the value-less row after present rows, counted.
    #[test]
    fn missing_last_placed_after_and_counted() {
        let e = seed();
        let r = search(&e, SortMissing::Last, 100, None);
        assert_eq!(ids(&r), vec!["d0", "d1", "d2"]);
        assert_eq!(r.total, 3);
    }

    /// R2: missing:first places the value-less row before present rows.
    #[test]
    fn missing_first_placed_before() {
        let e = seed();
        let r = search(&e, SortMissing::First, 100, None);
        assert_eq!(ids(&r), vec!["d2", "d0", "d1"]);
        assert_eq!(r.total, 3);
    }

    /// R3: default exclude drops the value-less row from results and total.
    #[test]
    fn exclude_default_drops_missing() {
        let e = seed();
        let r = search(&e, SortMissing::Exclude, 100, None);
        assert_eq!(ids(&r), vec!["d0", "d1"]);
        assert_eq!(r.total, 2);
    }

    /// R4: the missing-inclusive order paginates, each row once, exact total.
    #[test]
    fn missing_paginates_each_once() {
        let e = seed();
        let p1 = search(&e, SortMissing::Last, 2, None);
        assert_eq!(ids(&p1), vec!["d0", "d1"]);
        assert_eq!(p1.total, 3);
        let cursor = p1.cursor.expect("a full page hands back a cursor");
        let p2 = search(&e, SortMissing::Last, 2, Some(cursor));
        assert_eq!(ids(&p2), vec!["d2"]);
        assert_eq!(p2.total, 3);
    }

    /// #3997: a single high-cardinality keyword key with `missing:last` must
    /// not send a small page through the generic full tuple-sort fallback.
    /// Values are deliberately permuted so the old all-row sort needs many
    /// comparisons; the new keyword planner streams dictionary buckets.
    #[test]
    fn keyword_missing_last_small_page_bypasses_full_tuple_sort() {
        const DOCS: usize = 1_024;
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        for i in 0..DOCS {
            let mut items = vec![IndexItem {
                external_id: format!("d{i:04}"),
                field: "cat".into(),
                value: FieldValue::String("x".into()),
                version: None,
            }];
            if i % 8 != 0 {
                items.push(IndexItem {
                    external_id: format!("d{i:04}"),
                    field: "kw".into(),
                    value: FieldValue::String(format!("k{:04}", DOCS - i)),
                    version: None,
                });
            }
            e.index(
                "c",
                IndexRequest {
                    items,
                    request_id: None,
                },
            )
            .unwrap();
        }

        let segment_dir = tempfile::tempdir().expect("keyword segment tempdir");
        e.__seal_keyword_field_to_segment("c", "kw", segment_dir.path())
            .expect("seal high-cardinality keyword field");
        e.__seal_keyword_field_to_segment("c", "cat", segment_dir.path())
            .expect("seal exact exists filter field");

        MATERIALIZED_SORT_COMPARISONS.with(|comparisons| comparisons.set(0));
        let response = e
            .search(
                "c",
                SearchRequest {
                    query: QueryNode::Exists(ExistsQuery {
                        field: "cat".into(),
                    }),
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: Some(vec![SortSpec {
                        field: "kw".into(),
                        order: SortOrder::Asc,
                        missing: SortMissing::Last,
                    }]),
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap();
        assert_eq!(response.total, DOCS as u64);
        assert!(
            response.hits.iter().all(|hit| hit.score == 1.0),
            "keyword-present rows retain the constant filter score"
        );
        assert_eq!(
            ids(&response),
            (0..DOCS)
                .rev()
                .filter(|i| i % 8 != 0)
                .take(10)
                .map(|i| format!("d{i:04}"))
                .collect::<Vec<_>>()
        );
        assert!(
            MATERIALIZED_SORT_COMPARISONS.with(|comparisons| comparisons.get()) <= DOCS as u64,
            "small-page keyword sort must not comparison-sort all {DOCS} matches"
        );
    }

    /// A sealed ordinal stream must merge tail-only values, an equal tail value,
    /// and a post-seal tombstone without materializing either dictionary.
    #[test]
    fn sealed_keyword_bucket_walk_merges_tombstone_and_live_tail_both_orders() {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        let write = |eid: &str, keyword: &str| {
            e.index(
                "c",
                IndexRequest {
                    items: vec![
                        IndexItem {
                            external_id: eid.into(),
                            field: "cat".into(),
                            value: FieldValue::String("x".into()),
                            version: None,
                        },
                        IndexItem {
                            external_id: eid.into(),
                            field: "kw".into(),
                            value: FieldValue::String(keyword.into()),
                            version: None,
                        },
                    ],
                    request_id: None,
                },
            )
            .unwrap();
        };
        write("base-a", "a");
        write("base-b", "b");
        write("base-c", "c");
        let segment_dir = tempfile::tempdir().unwrap();
        e.__seal_keyword_field_to_segment("c", "kw", segment_dir.path())
            .unwrap();
        e.delete("c", "base-b", None).unwrap();
        write("tail-aa", "aa");
        write("tail-c", "c");
        write("tail-z", "z");

        let run = |order| {
            e.search(
                "c",
                SearchRequest {
                    query: QueryNode::Exists(ExistsQuery {
                        field: "cat".into(),
                    }),
                    limit: 100,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: Some(vec![SortSpec {
                        field: "kw".into(),
                        order,
                        missing: SortMissing::Last,
                    }]),
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap()
        };
        let asc = run(SortOrder::Asc);
        assert_eq!(
            ids(&asc),
            ["base-a", "tail-aa", "base-c", "tail-c", "tail-z"]
        );
        assert!(asc.hits.iter().all(|hit| hit.score == 1.0));
        let desc = run(SortOrder::Desc);
        assert_eq!(
            ids(&desc),
            ["tail-z", "base-c", "tail-c", "tail-aa", "base-a"]
        );
        assert!(desc.hits.iter().all(|hit| hit.score == 1.0));
    }

    /// Non-keyword/multi-key missing sorts use the exact bounded fallback.
    /// Counting may scan all matches, but retained tuples must never exceed the
    /// requested native prefix.
    #[test]
    fn missing_sort_fallback_retains_at_most_offset_plus_limit() {
        const DOCS: usize = 1_024;
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        for i in 0..DOCS {
            idx(
                &e,
                &format!("d{i:04}"),
                (i % 5 != 0).then_some((DOCS - i) as f64),
            );
        }
        MATERIALIZED_SORT_RETAINED_HIGH_WATER.with(|high_water| high_water.set(0));
        let response = e
            .search(
                "c",
                SearchRequest {
                    query: QueryNode::Exists(ExistsQuery {
                        field: "cat".into(),
                    }),
                    limit: 13,
                    offset: 7,
                    cursor: None,
                    routing_key: None,
                    sort: Some(vec![SortSpec {
                        field: "price".into(),
                        order: SortOrder::Asc,
                        missing: SortMissing::Last,
                    }]),
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap();
        assert_eq!(response.total, DOCS as u64);
        assert_eq!(response.hits.len(), 13);
        assert!(
            MATERIALIZED_SORT_RETAINED_HIGH_WATER.with(|high_water| high_water.get()) <= 20,
            "fallback retained more than offset + limit tuples"
        );
    }
}

// ---------------------------------------------------------------------------
// #181: a has_child query may be combined with sort. It resolves to a parent
// bitmap via the materialized path, which is then sorted by a parent field.
// knn/rrf/hamming + sort stay rejected.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod has_child_sort_tests {
    use super::*;
    use crate::shared_kernel::types::document::IndexItem;

    fn kw() -> FieldSpec {
        FieldSpec {
            field_type: FieldType::Keyword,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }
    fn num() -> FieldSpec {
        FieldSpec {
            field_type: FieldType::Number,
            ..kw()
        }
    }

    fn order(e: &Engine, eid: &str, ts: f64, status: &str) {
        e.index(
            "orders",
            IndexRequest {
                items: vec![
                    IndexItem {
                        external_id: eid.into(),
                        field: "ts".into(),
                        value: FieldValue::Number(ts),
                        version: None,
                    },
                    IndexItem {
                        external_id: eid.into(),
                        field: "status".into(),
                        value: FieldValue::String(status.into()),
                        version: None,
                    },
                ],
                request_id: None,
            },
        )
        .unwrap();
    }

    fn child(e: &Engine, parent: &str, sku: &str) {
        e.index(
            "items",
            IndexRequest {
                items: vec![
                    IndexItem {
                        external_id: format!("{parent}#0"),
                        field: "parent".into(),
                        value: FieldValue::String(parent.into()),
                        version: None,
                    },
                    IndexItem {
                        external_id: format!("{parent}#0"),
                        field: "sku".into(),
                        value: FieldValue::String(sku.into()),
                        version: None,
                    },
                ],
                request_id: None,
            },
        )
        .unwrap();
    }

    /// orders o1(ts100,open) o2(ts200,closed) o3(ts300,open); items link each
    /// order; o1,o2 have sku=S0, o3 has sku=X.
    fn setup() -> Engine {
        let e = Engine::new();
        let mut pf = BTreeMap::new();
        pf.insert("status".into(), kw());
        pf.insert("rank".into(), kw());
        pf.insert("ts".into(), num());
        e.create_collection("orders", CreateCollectionRequest { fields: pf })
            .unwrap();
        let mut cf = BTreeMap::new();
        cf.insert("parent".into(), kw());
        cf.insert("sku".into(), kw());
        e.create_collection("items", CreateCollectionRequest { fields: cf })
            .unwrap();
        order(&e, "o1", 100.0, "open");
        order(&e, "o2", 200.0, "closed");
        order(&e, "o3", 300.0, "open");
        child(&e, "o1", "S0");
        child(&e, "o2", "S0");
        child(&e, "o3", "X");
        e
    }

    fn has_child_s0() -> QueryNode {
        QueryNode::HasChild(HasChildQuery {
            collection: "items".into(),
            field: "parent".into(),
            query: Box::new(QueryNode::Term(TermQuery {
                field: "sku".into(),
                value: FieldValue::String("S0".into()),
            })),
        })
    }

    fn sort_ts_desc() -> Option<Vec<SortSpec>> {
        Some(vec![SortSpec {
            field: "ts".into(),
            order: SortOrder::Desc,
            missing: SortMissing::Exclude,
        }])
    }

    fn run(e: &Engine, query: QueryNode, sort: Option<Vec<SortSpec>>) -> SearchResponse {
        e.search(
            "orders",
            SearchRequest {
                query,
                limit: 100,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
    }

    fn ids(r: &SearchResponse) -> Vec<String> {
        r.hits.iter().map(|h| h.external_id.clone()).collect()
    }

    /// R1: has_child + sort returns matching parents ordered by the parent field.
    #[test]
    fn has_child_sort_orders_parents() {
        let e = setup();
        let r = run(&e, has_child_s0(), sort_ts_desc());
        assert_eq!(ids(&r), vec!["o2", "o1"]); // ts 200, 100 desc
        assert_eq!(r.total, 2);
    }

    /// R2: has_child AND a parent-field filter, sorted, intersect + exact total.
    #[test]
    fn has_child_sort_composes_with_filter() {
        let e = setup();
        let q = QueryNode::And(vec![
            has_child_s0(),
            QueryNode::Term(TermQuery {
                field: "status".into(),
                value: FieldValue::String("open".into()),
            }),
        ]);
        let r = run(&e, q, sort_ts_desc());
        assert_eq!(ids(&r), vec!["o1"]); // o2 is closed
        assert_eq!(r.total, 1);
    }

    /// #3997: a child query keeps the exact bounded materialized fallback,
    /// even when its one sort key could otherwise use the keyword stream.
    #[test]
    fn has_child_missing_keyword_sort_keeps_materialized_fallback() {
        let e = setup();
        e.index(
            "orders",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: "o1".into(),
                    field: "rank".into(),
                    value: FieldValue::String("a".into()),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
        MATERIALIZED_SORT_RETAINED_HIGH_WATER.with(|high_water| high_water.set(0));

        let r = run(
            &e,
            has_child_s0(),
            Some(vec![SortSpec {
                field: "rank".into(),
                order: SortOrder::Asc,
                missing: SortMissing::Last,
            }]),
        );

        assert_eq!(ids(&r), vec!["o1", "o2"]);
        assert_eq!(r.total, 2);
        assert_eq!(
            MATERIALIZED_SORT_RETAINED_HIGH_WATER.with(|high_water| high_water.get()),
            2,
            "has_child must retain its bounded materialized fallback"
        );
    }

    /// R3: sort + knn is still rejected (400 UnsupportedSort).
    #[test]
    fn knn_sort_still_rejected() {
        let e = setup();
        let err = e
            .search(
                "orders",
                SearchRequest {
                    query: QueryNode::Knn(KnnQuery {
                        field: "v".into(),
                        vector: vec![0.1, 0.2],
                        k: 5,
                    }),
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: sort_ts_desc(),
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap_err();
        let se = err.downcast_ref::<StorageError>().expect("StorageError");
        assert!(
            matches!(se, StorageError::UnsupportedSort(_)),
            "knn + sort must stay rejected, got {se:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// #182: native `ids` query — filter by a set of external_ids, resolved through
// the interner. Constant-scored, predicable, composes under and/or/not + sort.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod ids_query_tests {
    use super::*;
    use crate::shared_kernel::types::{document::IndexItem, query::IdsQuery};

    fn fieldspec(t: FieldType) -> FieldSpec {
        FieldSpec {
            field_type: t,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    fn schema() -> CreateCollectionRequest {
        let mut fields = BTreeMap::new();
        fields.insert("price".into(), fieldspec(FieldType::Number));
        fields.insert("status".into(), fieldspec(FieldType::Keyword));
        CreateCollectionRequest { fields }
    }

    /// d0(10,open) d1(20,closed) d2(30,open).
    fn seed() -> Engine {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        for (eid, price, status) in [
            ("d0", 10.0, "open"),
            ("d1", 20.0, "closed"),
            ("d2", 30.0, "open"),
        ] {
            e.index(
                "c",
                IndexRequest {
                    items: vec![
                        IndexItem {
                            external_id: eid.into(),
                            field: "price".into(),
                            value: FieldValue::Number(price),
                            version: None,
                        },
                        IndexItem {
                            external_id: eid.into(),
                            field: "status".into(),
                            value: FieldValue::String(status.into()),
                            version: None,
                        },
                    ],
                    request_id: None,
                },
            )
            .unwrap();
        }
        e
    }

    fn ids_q(vals: &[&str]) -> QueryNode {
        QueryNode::Ids(IdsQuery {
            values: vals.iter().map(|s| s.to_string()).collect(),
        })
    }

    fn run(e: &Engine, query: QueryNode, sort: Option<Vec<SortSpec>>) -> SearchResponse {
        e.search(
            "c",
            SearchRequest {
                query,
                limit: 100,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
    }

    fn id_set(r: &SearchResponse) -> BTreeSet<String> {
        r.hits.iter().map(|h| h.external_id.clone()).collect()
    }

    /// R1: returns exactly the named existing ids, skipping unknown ones.
    #[test]
    fn ids_returns_named_set_skips_unknown() {
        let e = seed();
        let r = run(&e, ids_q(&["d0", "d2", "does-not-exist"]), None);
        assert_eq!(
            id_set(&r),
            ["d0".to_string(), "d2".to_string()].into_iter().collect()
        );
        assert_eq!(r.total, 2);
    }

    /// R2: composes under a boolean AND with another clause.
    #[test]
    fn ids_composes_under_and() {
        let e = seed();
        let q = QueryNode::And(vec![
            ids_q(&["d0", "d1", "d2"]),
            QueryNode::Term(TermQuery {
                field: "status".into(),
                value: FieldValue::String("open".into()),
            }),
        ]);
        let r = run(&e, q, None);
        assert_eq!(
            id_set(&r),
            ["d0".to_string(), "d2".to_string()].into_iter().collect(),
            "d1 is closed, so the AND drops it"
        );
    }

    /// R3: combines with sort (ids is a predicable filter).
    #[test]
    fn ids_combines_with_sort() {
        let e = seed();
        let r = run(
            &e,
            ids_q(&["d2", "d0"]),
            Some(vec![SortSpec {
                field: "price".into(),
                order: SortOrder::Asc,
                missing: SortMissing::Exclude,
            }]),
        );
        let ordered: Vec<String> = r.hits.iter().map(|h| h.external_id.clone()).collect();
        assert_eq!(ordered, vec!["d0".to_string(), "d2".to_string()]); // 10 then 30
    }

    /// #1487/R1: a fully-deleted doc (all fields removed) must not match an
    /// `ids` query, consistent with `term`/`terms` on the same state.
    #[test]
    fn ids_excludes_fully_deleted_doc() {
        let e = seed();
        e.delete("c", "d1", None).unwrap();
        let r = run(&e, ids_q(&["d0", "d1", "d2"]), None);
        assert_eq!(
            id_set(&r),
            ["d0".to_string(), "d2".to_string()].into_iter().collect(),
            "d1 was fully deleted and must not be a hit"
        );
        assert_eq!(r.total, 2);

        // Same doc-state, term query on the surviving docs' field agrees.
        let term_r = run(
            &e,
            QueryNode::Terms(TermsQuery {
                field: "status".into(),
                values: vec![
                    FieldValue::String("open".into()),
                    FieldValue::String("closed".into()),
                ],
            }),
            None,
        );
        assert_eq!(
            id_set(&term_r),
            ["d0".to_string(), "d2".to_string()].into_iter().collect()
        );
    }

    /// #1487: mixed batch — a request naming live and deleted ids together
    /// returns only the live subset.
    #[test]
    fn ids_mixed_batch_returns_only_live_subset() {
        let e = seed();
        e.delete("c", "d0", None).unwrap();
        e.delete("c", "d2", None).unwrap();
        let r = run(&e, ids_q(&["d0", "d1", "d2", "does-not-exist"]), None);
        assert_eq!(
            id_set(&r),
            ["d1".to_string()].into_iter().collect(),
            "only the still-live doc survives, deleted + unknown ids drop out"
        );
        assert_eq!(r.total, 1);
    }

    /// #1487: partial-field deletion — a doc with SOME fields deleted but at
    /// least one field still live stays a hit under `ids` (matches the
    /// engine's liveness definition used by `term`: live iff any field
    /// lives).
    #[test]
    fn ids_matches_doc_with_partial_field_deletion() {
        let e = seed();
        // Delete only the `price` field on d0 — `status` is still live.
        e.delete("c", "d0", Some("price")).unwrap();
        let r = run(&e, ids_q(&["d0", "d1", "d2"]), None);
        assert_eq!(
            id_set(&r),
            ["d0".to_string(), "d1".to_string(), "d2".to_string()]
                .into_iter()
                .collect(),
            "d0 still has a live field (status), so it remains a hit"
        );
        assert_eq!(r.total, 3);

        // Now delete the remaining field too — d0 becomes fully dead.
        e.delete("c", "d0", Some("status")).unwrap();
        let r2 = run(&e, ids_q(&["d0", "d1", "d2"]), None);
        assert_eq!(
            id_set(&r2),
            ["d1".to_string(), "d2".to_string()].into_iter().collect(),
            "d0 has no live fields left, so it drops out"
        );
    }
}

// ---------------------------------------------------------------------------
// #183: multi-key sort cap raised to MAX_SORT_KEYS (4). The generic plan
// compares every key in priority order; > 4 keys is rejected.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod multikey_sort_cap_tests {
    use super::*;
    use crate::shared_kernel::types::document::IndexItem;

    fn schema() -> CreateCollectionRequest {
        let num = || FieldSpec {
            field_type: FieldType::Number,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        };
        let mut fields = BTreeMap::new();
        fields.insert("a".into(), num());
        fields.insert("b".into(), num());
        fields.insert("c".into(), num());
        CreateCollectionRequest { fields }
    }

    fn idx(e: &Engine, eid: &str, a: f64, b: f64, c: f64) {
        let item = |field: &str, v: f64| IndexItem {
            external_id: eid.into(),
            field: field.into(),
            value: FieldValue::Number(v),
            version: None,
        };
        e.index(
            "c",
            IndexRequest {
                items: vec![item("a", a), item("b", b), item("c", c)],
                request_id: None,
            },
        )
        .unwrap();
    }

    fn sort_asc(fields: &[&str]) -> Vec<SortSpec> {
        fields
            .iter()
            .map(|f| SortSpec {
                field: (*f).into(),
                order: SortOrder::Asc,
                missing: SortMissing::Exclude,
            })
            .collect()
    }

    fn all() -> QueryNode {
        QueryNode::Range(RangeQuery {
            field: "a".into(),
            gte: None,
            gt: None,
            lte: None,
            lt: None,
        })
    }

    /// R1: a 3-key sort orders by each key in priority.
    #[test]
    fn three_key_sort_orders() {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        idx(&e, "d0", 1.0, 1.0, 1.0);
        idx(&e, "d1", 1.0, 1.0, 2.0);
        idx(&e, "d2", 1.0, 2.0, 1.0);
        idx(&e, "d3", 2.0, 1.0, 1.0);
        let r = e
            .search(
                "c",
                SearchRequest {
                    query: all(),
                    limit: 100,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: Some(sort_asc(&["a", "b", "c"])),
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap();
        let ordered: Vec<String> = r.hits.iter().map(|h| h.external_id.clone()).collect();
        assert_eq!(ordered, vec!["d0", "d1", "d2", "d3"]);
        assert_eq!(r.total, 4);
    }

    /// R2: more than MAX_SORT_KEYS keys is rejected with UnsupportedSort.
    #[test]
    fn over_four_keys_rejected() {
        let e = Engine::new();
        e.create_collection("c", schema()).unwrap();
        idx(&e, "d0", 1.0, 1.0, 1.0);
        let err = e
            .search(
                "c",
                SearchRequest {
                    query: all(),
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: Some(sort_asc(&["a", "b", "c", "a", "b"])), // 5 keys
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap_err();
        let se = err.downcast_ref::<StorageError>().expect("StorageError");
        assert!(
            matches!(se, StorageError::UnsupportedSort(_)),
            "more than {MAX_SORT_KEYS} keys must be rejected, got {se:?}"
        );
    }
}
#[cfg(test)]
mod sparse_scalar_overlay_tests {
    use super::*;

    #[test]
    fn sealed_number_reads_replacement_overlay_and_then_delete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("number.lseg");
        crate::persistence::infrastructure::segment::number_writer::write_number_segment(
            &path,
            1,
            &[Some(1.0)],
        )
        .unwrap();
        let mut number = NumberIndex::default();
        number.segment = Some(Arc::new(ComposedSegmentReader::from_base(Arc::new(
            crate::persistence::infrastructure::segment::SegmentReader::open(&path).unwrap(),
        ))));
        number.tombstones.insert(0);
        number.forward.insert(0, SortableF64::new(2.0).unwrap());
        assert_eq!(number.live_number_at(0).map(|v| v.to_f64()), Some(2.0));
        number.forward.remove(&0);
        assert_eq!(number.live_number_at(0), None);
    }

    #[test]
    fn sparse_high_delta_does_not_hide_middle_live_scalar_overlays() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.lseg");
        let delta = dir.path().join("delta.lseg");

        crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment(
            &base,
            1,
            &[Some("base")],
            &BTreeMap::new(),
        )
        .unwrap();
        crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment(
            &delta,
            1,
            &[Some("delta")],
            &BTreeMap::new(),
        )
        .unwrap();
        let keyword_view = ComposedSegmentReader::from_base(Arc::new(
            crate::persistence::infrastructure::segment::SegmentReader::open(&base).unwrap(),
        ))
        .with_delta(
            Arc::new(
                crate::persistence::infrastructure::segment::SegmentReader::open(&delta).unwrap(),
            ),
            vec![100],
        )
        .unwrap();
        let mut keyword = KeywordIndex::default();
        keyword.segment = Some(Arc::new(keyword_view));
        keyword.forward.insert(2, "middle".into());
        assert_eq!(keyword.keyword_at(2).as_deref(), Some("middle"));

        let base_set = ["base".to_string()];
        crate::persistence::infrastructure::segment::set_writer::write_set_segment(
            &base,
            1,
            &[Some(base_set.as_slice())],
            &BTreeMap::new(),
        )
        .unwrap();
        let delta_set = ["delta".to_string()];
        crate::persistence::infrastructure::segment::set_writer::write_set_segment(
            &delta,
            1,
            &[Some(delta_set.as_slice())],
            &BTreeMap::new(),
        )
        .unwrap();
        let set_view = ComposedSegmentReader::from_base(Arc::new(
            crate::persistence::infrastructure::segment::SegmentReader::open(&base).unwrap(),
        ))
        .with_delta(
            Arc::new(
                crate::persistence::infrastructure::segment::SegmentReader::open(&delta).unwrap(),
            ),
            vec![100],
        )
        .unwrap();
        let mut set = SetIndex::default();
        set.segment = Some(Arc::new(set_view));
        set.forward
            .insert(2, ["middle".to_string()].into_iter().collect());
        assert!(set.set_contains(2, "middle"));
        assert_eq!(
            set.set_members(2),
            Some(["middle".to_string()].into_iter().collect())
        );

        crate::persistence::infrastructure::segment::hash_writer::write_hash_segment(
            &base,
            1,
            &[Some(1)],
        )
        .unwrap();
        crate::persistence::infrastructure::segment::hash_writer::write_hash_segment(
            &delta,
            1,
            &[Some(3)],
        )
        .unwrap();
        let hash_view = ComposedSegmentReader::from_base(Arc::new(
            crate::persistence::infrastructure::segment::SegmentReader::open(&base).unwrap(),
        ))
        .with_delta(
            Arc::new(
                crate::persistence::infrastructure::segment::SegmentReader::open(&delta).unwrap(),
            ),
            vec![100],
        )
        .unwrap();
        let mut hash = HashIndex::default();
        hash.segment = Some(Arc::new(hash_view));
        hash.forward.insert(2, 2);
        assert_eq!(hash.hash_at(2), Some(2));
    }
}

#[cfg(test)]
mod scalar_checkpoint_cut_tests {
    use super::*;

    #[test]
    fn checkpoint_capture_marks_unsealed_scalar_fields_with_empty_cuts() {
        let mut fields = BTreeMap::new();
        for (name, field_type) in [
            ("keyword", FieldType::Keyword),
            ("number", FieldType::Number),
            ("set", FieldType::Set),
        ] {
            fields.insert(
                name.into(),
                FieldSpec {
                    field_type,
                    analyzer: None,
                    multi: None,
                    dim: None,
                    metric: None,
                    backend: None,
                    quantize: None,
                },
            );
        }
        let engine = Engine::new();
        engine
            .create_collection("c", CreateCollectionRequest { fields })
            .unwrap();
        let frozen = engine.freeze_checkpoint_collections(None).unwrap();
        let cuts = frozen.capture.scalar_cuts.get("c").unwrap();
        assert_eq!(cuts.len(), 3);
        for field in ["keyword", "number", "set"] {
            assert!(cuts.contains_key(field), "{field} must get an empty cut");
        }
    }

    #[test]
    fn scalar_checkpoint_retirement_keeps_a_mutation_after_preparation() {
        let engine = Engine::with_change_budget(
            crate::ingest::domain::change_budget::ChangeBudget::with_hard_limit(32 * 1024 * 1024),
        );
        engine
            .create_collection_inner(
                "c",
                CreateCollectionRequest {
                    fields: serde_json::from_value(serde_json::json!({
                        "keyword":{"type":"keyword"}, "number":{"type":"number"}
                    }))
                    .unwrap(),
                },
            )
            .unwrap();
        let write = |value: &str, include_number: bool| {
            let mut items = vec![crate::shared_kernel::types::document::IndexItem {
                external_id: "e".into(),
                field: "keyword".into(),
                value: FieldValue::String(value.into()),
                version: None,
            }];
            if include_number {
                items.push(crate::shared_kernel::types::document::IndexItem {
                    external_id: "e".into(),
                    field: "number".into(),
                    value: FieldValue::Number(3.0),
                    version: None,
                });
            }
            engine
                .index_inner(
                    "c",
                    IndexRequest {
                        items,
                        request_id: None,
                    },
                    None,
                    None,
                )
                .unwrap();
        };
        write("captured", true);
        // Select the full-base branch so both ordinary overlay retirement and
        // a newer ordinary overlay cross the real Engine publication seam.
        engine
            .state
            .write()
            .unwrap()
            .collections
            .get_mut("c")
            .unwrap()
            .requires_full_checkpoint = true;
        let root = tempfile::tempdir().unwrap();
        let frozen = engine.freeze_checkpoint_collections(None).unwrap();
        let mut capture = frozen.write(root.path(), 0).unwrap();
        engine
            .prepare_scalar_checkpoint_publications(&mut capture)
            .unwrap();
        write("later", false);
        engine
            .bind_checkpoint_origins(root.path(), &mut capture)
            .unwrap();
        let state = engine.state.read().unwrap();
        let coll = &state.collections["c"];
        let id = coll.interner.id("e").unwrap();
        let FieldIndex::Keyword(keyword) = &coll.fields["keyword"] else {
            unreachable!()
        };
        assert_eq!(
            keyword.keyword_at(id).as_deref(),
            Some("later"),
            "publication must preserve the ordinary write made after preparation"
        );
        assert!(
            keyword
                .dense_forward
                .get(id as usize)
                .and_then(Option::as_ref)
                .is_some()
                || keyword.forward.contains_key(&id)
        );
        let FieldIndex::Number(number) = &coll.fields["number"] else {
            unreachable!()
        };
        assert_eq!(number.number_at(id).unwrap().to_f64(), 3.0);
        assert!(
            number.forward.is_empty(),
            "unchanged captured overlays must be retired"
        );
        assert!(number
            .dense_forward
            .get(id as usize)
            .is_none_or(|value| *value == MISSING_SORTABLE_F64_BITS));
        assert!(number.segment.is_some());
    }
}
// CODEGEN-END

#[cfg(test)]
mod batch_unindex_docs_tests {
    use super::*;
    use crate::shared_kernel::types::document::{BatchUnindexDocsRequest, IndexItem};

    #[test]
    fn batch_unindex_removes_a_known_document() {
        let engine = Engine::new();
        let mut fields = BTreeMap::new();
        fields.insert(
            "email".to_string(),
            FieldSpec {
                field_type: FieldType::Keyword,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        engine
            .create_collection("docs", CreateCollectionRequest { fields })
            .unwrap();
        engine
            .index(
                "docs",
                IndexRequest {
                    items: vec![IndexItem {
                        external_id: "old".to_string(),
                        field: "email".to_string(),
                        value: FieldValue::String("old@example.com".to_string()),
                        version: None,
                    }],
                    request_id: None,
                },
            )
            .unwrap();

        engine
            .unindex_docs(
                "docs",
                BatchUnindexDocsRequest {
                    external_ids: vec!["old".to_string()],
                },
            )
            .unwrap();
        assert_eq!(engine.stats("docs").unwrap().documents_indexed, 0);
    }

    #[test]
    fn batch_unindex_clears_lww_and_replace_side_state_before_rewrite() {
        let engine = Engine::new();
        let mut fields = BTreeMap::new();
        fields.insert(
            "email".to_string(),
            FieldSpec {
                field_type: FieldType::Keyword,
                analyzer: None,
                multi: None,
                dim: None,
                metric: None,
                backend: None,
                quantize: None,
            },
        );
        engine
            .create_collection("docs", CreateCollectionRequest { fields })
            .unwrap();
        engine
            .index(
                "docs",
                IndexRequest {
                    items: vec![IndexItem {
                        external_id: "old".to_string(),
                        field: "email".to_string(),
                        value: FieldValue::String("old@example.com".to_string()),
                        version: Some(4),
                    }],
                    request_id: None,
                },
            )
            .unwrap();
        engine
            .replace_docs(
                "docs",
                ReplaceDocsRequest {
                    docs: vec![ReplaceDocItem {
                        external_id: "old".to_string(),
                        version: Some(9),
                        fields: BTreeMap::from([(
                            "email".to_string(),
                            FieldValue::String("replace@example.com".to_string()),
                        )]),
                    }],
                },
            )
            .unwrap();

        engine
            .unindex_docs(
                "docs",
                BatchUnindexDocsRequest {
                    external_ids: vec!["old".to_string()],
                },
            )
            .unwrap();
        let state = engine.state.read().unwrap();
        let coll = state.collections.get("docs").unwrap();
        let id = coll.interner.id("old").expect("append-only interner entry");
        assert!(!coll.eid_fields.contains_key(&id));
        assert!(!coll.cell_versions.contains_key(&id));
        assert!(!coll.doc_versions.contains_key(&id));
        assert!(!coll.field_checksums.contains_key(&id));
        drop(state);

        // No unindex tombstone is retained.  An older external version can
        // become the first version of the rewritten row.
        let rewritten = engine
            .index(
                "docs",
                IndexRequest {
                    items: vec![IndexItem {
                        external_id: "old".to_string(),
                        field: "email".to_string(),
                        value: FieldValue::String("rewritten@example.com".to_string()),
                        version: Some(1),
                    }],
                    request_id: None,
                },
            )
            .unwrap();
        assert_eq!(rewritten.indexed, 1);
    }

    #[test]
    fn batch_unindex_revalidates_before_taking_any_mutation_path() {
        let engine = Engine::new();
        engine
            .create_collection(
                "docs",
                CreateCollectionRequest {
                    fields: BTreeMap::new(),
                },
            )
            .unwrap();
        let err = engine
            .unindex_docs(
                "docs",
                BatchUnindexDocsRequest {
                    external_ids: Vec::new(),
                },
            )
            .unwrap_err();
        assert!(err.to_string().contains("at least one"), "got: {err}");
        assert_eq!(engine.stats("docs").unwrap().documents_indexed, 0);
    }

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
}

/// `/stats` cost with un-absorbed staged Text rows present (#4246). A
/// committed-WAL Text value stays in `TextIndex::staged_rows` until a
/// checkpoint publication absorbs it, so `unique_terms` must stay linear in
/// live terms plus staged tokens instead of scanning every staged row once
/// per term.
#[cfg(test)]
mod staged_text_stats_tests {
    use super::*;
    use crate::persistence::infrastructure::segment::text_row_stage::TextRowStageOptions;

    fn staged_row(input: &str) -> Arc<staged_text_row::StagedTextRow> {
        Arc::new(
            staged_text_row::StagedTextRow::stage(
                input,
                Analyzer::WhitespaceLower,
                TextRowStageOptions::minimum_scratch_bytes() + 4096,
                |_| Ok(()),
            )
            .expect("stage one Text row"),
        )
    }

    /// `tail` distinct live-tail tokens on doc 0, then `rows` staged rows each
    /// carrying one token shared with every other row plus one of its own.
    fn index_with_staged_rows(tail: u32, rows: u32) -> TextIndex {
        let mut idx = TextIndex {
            doc_count: 1,
            total_doc_len: u64::from(tail),
            ..Default::default()
        };
        idx.lens.push(tail);
        for n in 0..tail {
            let mut posting = Postings::default();
            posting.upsert(0, 1);
            idx.tokens.insert(format!("tail{n}"), posting);
        }
        for row in 0..rows {
            idx.staged_rows
                .insert(row + 1, staged_row(&format!("shared only{row}")));
            idx.doc_count += 1;
        }
        idx
    }

    #[test]
    fn live_unique_tokens_counts_every_staged_and_tail_token_exactly_once() {
        let idx = index_with_staged_rows(30, 50);
        // 30 tail tokens + the one token every staged row shares + 50
        // row-private tokens.
        assert_eq!(idx.live_unique_tokens(), 30 + 1 + 50);
    }

    #[test]
    fn live_unique_tokens_cost_stays_linear_in_the_staged_row_count() {
        let small = index_with_staged_rows(30, 25);
        reset_staged_term_probes();
        assert_eq!(small.live_unique_tokens(), 30 + 1 + 25);
        let small_probes = staged_term_probes();

        let large = index_with_staged_rows(30, 100);
        reset_staged_term_probes();
        assert_eq!(large.live_unique_tokens(), 30 + 1 + 100);
        let large_probes = staged_term_probes();

        // Reading each staged row's dictionary once costs its 2 tokens and
        // nothing per live tail term: 2 x rows probes. Asking `tok_postings`
        // per union term instead costs (tail + 1 + rows) x rows, which is
        // 13_100 here and is what made `/stats` superlinear in document count.
        assert!(
            large_probes <= 4 * (30 + 2 * 100),
            "counting staged tokens must cost O(live terms + staged tokens), \
             observed {large_probes} probes"
        );
        // Four times the rows may cost at most four times the probes.
        assert!(
            large_probes <= 4 * small_probes + 16,
            "staged-row cost must grow linearly, not quadratically: \
             {small_probes} probes at 25 rows, {large_probes} at 100"
        );
    }
}

#[cfg(test)]
mod checkpoint_publish_releases_retained_charge_tests {
    use super::*;
    use crate::shared_kernel::types::document::IndexItem;

    fn text_field(analyzer: Analyzer) -> FieldSpec {
        FieldSpec {
            field_type: FieldType::Text,
            analyzer: Some(analyzer),
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        }
    }

    /// Admit N committed rows that each retain a real
    /// [`crate::ingest::domain::change_budget::RetainedCharge`], run one real checkpoint
    /// freeze + write + publish through the exact Engine code path, and
    /// assert the process-wide budget's `active + frozen` (its `total`) after
    /// publication. If publication does not release every payload, this must
    /// fail before any fix and pass after.
    #[test]
    fn checkpoint_publish_releases_every_committed_row_charge() {
        let budget =
            crate::ingest::domain::change_budget::ChangeBudget::with_hard_limit(8 * 1024 * 1024);
        let engine = Engine::with_change_budget(budget.clone());
        let mut fields = BTreeMap::new();
        fields.insert("body".to_string(), text_field(Analyzer::WhitespaceLower));
        engine
            .create_collection_inner("c", CreateCollectionRequest { fields })
            .unwrap();

        let mut charges = Vec::new();
        for i in 0..50 {
            let charge = engine
                .changes
                .owner
                .try_reserve(1024)
                .unwrap()
                .commit_retained()
                .unwrap();
            engine
                .index_inner(
                    "c",
                    IndexRequest {
                        items: vec![IndexItem {
                            external_id: format!("doc{i}"),
                            field: "body".to_string(),
                            value: FieldValue::String("hello world from lumen".to_string()),
                            version: None,
                        }],
                        request_id: None,
                    },
                    Some(&charge),
                    None,
                )
                .unwrap();
            charges.push(charge);
        }
        // The caller's own handles drop here; the journal rows still hold
        // their own clones of the same retained charges.
        drop(charges);
        let before = budget.snapshot();
        assert!(
            before.total > 0,
            "committed rows must remain charged before any checkpoint runs"
        );

        let root = tempfile::tempdir().unwrap();
        let frozen = engine.freeze_checkpoint_collections(None).unwrap();
        let mut capture = frozen.write(root.path(), 0).unwrap();
        engine
            .bind_checkpoint_origins(root.path(), &mut capture)
            .unwrap();
        engine.acknowledge_record_charges(&capture).unwrap();
        drop(capture);
        // Mirrors the real driver: `PendingFrozenLease::disarm` drops the
        // original `FrozenCheckpoint` only after publication and live
        // binding both succeed (`segment_rdb.rs`'s `pending.disarm()`).
        drop(frozen);

        let after = budget.snapshot();
        assert_eq!(
            after.total, 0,
            "publishing a checkpoint that captured every committed row must \
             release each row's retained charge: active={} frozen={} reserved={}",
            after.active, after.frozen, after.reserved
        );
    }
}
