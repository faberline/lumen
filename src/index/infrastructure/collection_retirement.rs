//! The background reclaimer for collections detached by `docs:truncate`: a
//! process-wide registry holds each retired generation until a shared queue of
//! bounded tasks has drained it off the apply thread, and the counters it keeps
//! feed a read-only diagnostic snapshot.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};

use crate::index::domain::collection::Collection;
use crate::index::domain::fast_hash::FastHashMap;

#[cfg(test)]
static RETIREMENT_FAILSAFE_RETAINS: AtomicU64 = AtomicU64::new(0);

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

pub(in crate::index) fn collection_retirement_worker() -> &'static CollectionRetirementWorker {
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

pub(in crate::index) fn retire_collection_with(
    worker: &CollectionRetirementWorker,
    old: Collection,
) {
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

#[cfg(test)]
mod tests;
