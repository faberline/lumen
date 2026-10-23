//! `MemWal`, the in-process log: records live in memory and are truncated
//! behind the slowest subscriber.

use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::watch;

use crate::ingest::domain::wal_log::{WalAdmissionStream, WalLog, WalStream};
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::delivery::{
    WalDelivery, WalSourceRecord, WalSourceRelease,
};
use crate::ingest::infrastructure::wal_source_stage::{StagedWalRecord, WalSourceStager};

// ---------------------------------------------------------------------------
// MemWal — in-process backend
// ---------------------------------------------------------------------------

/// In-memory log with **truncation behind the consumed watermark**.
///
/// Sequences are 1-based and stable: `base` counts records already
/// dropped, so the record at `records[i]` has seq `base + i + 1`. Once
/// every live subscriber has consumed past a record, it is dropped from
/// the front — so under a steady single-subscriber workload (the serving
/// node's apply loop, always caught up) memory stays flat regardless of
/// throughput, instead of the log growing forever.
///
/// A registered subscriber never loses data: truncation only drops up to
/// the *minimum* delivered sequence across all subscribers, which is by
/// definition ≤ what each one has already consumed. With no subscribers,
/// nothing is dropped (a future `subscribe(0)` can still replay).
#[derive(Clone)]
pub struct MemWal {
    pub(super) shared: Arc<Mutex<MemWalInner>>,
    len_tx: Arc<watch::Sender<u64>>,
    pub(super) source_stager: Arc<Mutex<Option<WalSourceStager>>>,
}

pub(super) struct MemWalInner {
    pub(super) records: std::collections::VecDeque<Arc<Mutex<MemWalSlot>>>,
    pub(super) base: u64,
    // Sub id → highest sequence safe to discard. The most recently returned
    // record stays pinned until the subscriber begins its next poll.
    subs: std::collections::HashMap<u64, u64>,
    next_sub_id: u64,
}

pub(super) enum MemWalSlot {
    Resident(
        WalRecord,
        Vec<crate::ingest::domain::change_budget::SourceRetention>,
    ),
    Staged(Arc<StagedWalRecord>),
}

impl MemWalInner {
    fn latest(&self) -> u64 {
        self.base + self.records.len() as u64
    }

    fn maybe_truncate(&mut self) {
        if self.subs.is_empty() {
            return; // no consumers → keep everything for a future replay
        }
        let low_water = self.subs.values().copied().min().unwrap_or(0);
        while !self.records.is_empty() && self.base + 1 <= low_water {
            self.records.pop_front();
            self.base += 1;
        }
    }
}

/// Removes a subscription from `subs` when its stream is dropped, so a
/// gone subscriber never pins truncation forever.
struct SubGuard {
    shared: Arc<Mutex<MemWalInner>>,
    id: u64,
}

impl Drop for SubGuard {
    fn drop(&mut self) {
        if let Ok(mut s) = self.shared.lock() {
            s.subs.remove(&self.id);
        }
    }
}

impl Default for MemWal {
    fn default() -> Self {
        Self::new()
    }
}

impl MemWal {
    pub fn new() -> Self {
        Self::starting_at(0)
    }

    /// Like [`new`](Self::new) but the sequence domain starts above
    /// `base_seq` instead of `0` — required whenever the engine was seeded
    /// from a restored checkpoint (and, if applicable, replayed AOF tail):
    /// without this, a fresh in-process `MemWal` reassigns sequences `1..N`
    /// to genuinely new writes while the coordinator's `applied` watermark
    /// (seeded from the same restore) is already `>= N`, so the apply
    /// loop's redelivery-dedup guard silently discards them (#1486). The
    /// caller passes the final restored watermark (checkpoint `up_to_seq`,
    /// or the AOF-tail-replayed sequence if that is higher) so the first
    /// `publish` after restore is assigned `base_seq + 1` — strictly above
    /// anything the watermark already considers applied.
    pub fn starting_at(base_seq: u64) -> Self {
        let (len_tx, _rx) = watch::channel(base_seq);
        Self {
            shared: Arc::new(Mutex::new(MemWalInner {
                records: std::collections::VecDeque::new(),
                base: base_seq,
                subs: std::collections::HashMap::new(),
                next_sub_id: 0,
            })),
            len_tx: Arc::new(len_tx),
            source_stager: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl WalLog for MemWal {
    async fn publish(&self, record: WalRecord) -> Result<u64> {
        let seq = {
            let mut s = self
                .shared
                .lock()
                .map_err(|_| anyhow::anyhow!("MemWal poisoned"))?;
            s.records
                .push_back(Arc::new(Mutex::new(MemWalSlot::Resident(
                    record,
                    Vec::new(),
                ))));
            let seq = s.latest();
            s.maybe_truncate();
            seq
        };
        let _ = self.len_tx.send(seq);
        Ok(seq)
    }

    async fn subscribe(&self, from_seq: u64) -> Result<WalStream> {
        let shared = self.shared.clone();
        let rx = self.len_tx.subscribe();
        let id = {
            let mut s = shared
                .lock()
                .map_err(|_| anyhow::anyhow!("MemWal poisoned"))?;
            let id = s.next_sub_id;
            s.next_sub_id += 1;
            s.subs.insert(id, from_seq);
            id
        };
        let guard = SubGuard {
            shared: shared.clone(),
            id,
        };
        // State: (most recently delivered seq, watch rx, shared, guard).
        // A poll first acknowledges the prior record, so the record returned
        // by this poll remains replayable until the caller polls again.
        // Dropping the stream drops the guard → unregisters the subscription.
        let stream = futures::stream::unfold(
            (from_seq, rx, shared, guard),
            |(delivered, mut rx, shared, guard)| async move {
                loop {
                    let next = {
                        let mut s = match shared.lock() {
                            Ok(s) => s,
                            Err(_) => return None,
                        };
                        // The caller has started another poll, so it no
                        // longer needs its prior delivered record. Do this
                        // before selecting the next item; a record returned
                        // below is retained until the following poll.
                        s.subs.insert(guard.id, delivered);
                        s.maybe_truncate();
                        // Deliver the next seq after `delivered`, clamped
                        // above the truncation floor (never < base+1).
                        let want = (delivered + 1).max(s.base + 1);
                        let idx = (want - s.base - 1) as usize;
                        match s.records.get(idx).cloned() {
                            Some(slot) => Some((want, slot)),
                            None => None,
                        }
                    };
                    if let Some((seq, slot)) = next {
                        let rec = slot
                            .lock()
                            .map_err(|_| anyhow!("MemWal source slot poisoned"))
                            .and_then(|slot| match &*slot {
                                MemWalSlot::Resident(record, _) => Ok(record.clone()),
                                MemWalSlot::Staged(stage) => {
                                    stage.read(usize::MAX).map_err(Into::into)
                                }
                            });
                        return Some((rec.map(|rec| (seq, rec)), (seq, rx, shared, guard)));
                    }
                    if rx.changed().await.is_err() {
                        return None;
                    }
                }
            },
        );
        Ok(Box::pin(stream))
    }

    async fn subscribe_admitted(&self, from_seq: u64) -> Result<WalAdmissionStream> {
        let shared = self.shared.clone();
        let rx = self.len_tx.subscribe();
        let id = {
            let mut state = shared.lock().map_err(|_| anyhow!("MemWal poisoned"))?;
            let id = state.next_sub_id;
            state.next_sub_id += 1;
            state.subs.insert(id, from_seq);
            id
        };
        let guard = SubGuard {
            shared: shared.clone(),
            id,
        };
        let stream = futures::stream::unfold(
            (from_seq, rx, shared, guard),
            |(delivered, mut rx, shared, guard)| async move {
                loop {
                    let next = {
                        let mut state = match shared.lock() {
                            Ok(state) => state,
                            Err(_) => return None,
                        };
                        state.subs.insert(guard.id, delivered);
                        state.maybe_truncate();
                        let want = (delivered + 1).max(state.base + 1);
                        let index = (want - state.base - 1) as usize;
                        state.records.get(index).cloned().map(|slot| (want, slot))
                    };
                    if let Some((sequence, slot)) = next {
                        let delivery = WalDelivery::Deferred(WalSourceRecord { sequence, slot });
                        return Some((Ok((sequence, delivery)), (sequence, rx, shared, guard)));
                    }
                    if rx.changed().await.is_err() {
                        return None;
                    }
                }
            },
        );
        Ok(Box::pin(stream))
    }

    async fn stage_source(&self, sequence: u64) -> Result<Option<WalSourceRelease>> {
        let slot = {
            let state = self.shared.lock().map_err(|_| anyhow!("MemWal poisoned"))?;
            if sequence <= state.base {
                return Ok(None);
            }
            state
                .records
                .get((sequence - state.base - 1) as usize)
                .cloned()
        };
        let Some(slot) = slot else {
            return Ok(None);
        };
        let source_stager = self.source_stager.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<WalSourceRelease>> {
            let stager = {
                let mut source_stager = source_stager
                    .lock()
                    .map_err(|_| anyhow!("MemWal stager poisoned"))?;
                if source_stager.is_none() {
                    *source_stager = Some(WalSourceStager::for_mem_wal()?);
                }
                source_stager.as_ref().expect("just created stager").clone()
            };
            let mut slot = slot
                .lock()
                .map_err(|_| anyhow!("MemWal source slot poisoned"))?;
            match &mut *slot {
                MemWalSlot::Staged(stage) => {
                    if stage.sequence() != sequence || stage.source_id() != stager.source_id() {
                        anyhow::bail!("staged MemWal source identity mismatch");
                    }
                    Ok(Some(WalSourceRelease { sequence }))
                }
                MemWalSlot::Resident(record, _) => {
                    let staged = match stager.stage_fast_index(sequence, record)? {
                        Some(staged) => staged,
                        None => stager.stage(sequence, record)?,
                    };
                    if staged.sequence() != sequence || staged.source_id() != stager.source_id() {
                        anyhow::bail!("staged MemWal receipt identity mismatch");
                    }
                    *slot = MemWalSlot::Staged(Arc::new(staged));
                    Ok(Some(WalSourceRelease { sequence }))
                }
            }
        })
        .await
        .map_err(|error| anyhow!("MemWal stage worker failed: {error}"))?
    }

    async fn latest_seq(&self) -> Result<u64> {
        Ok(self
            .shared
            .lock()
            .map_err(|_| anyhow::anyhow!("MemWal poisoned"))?
            .latest())
    }
}
