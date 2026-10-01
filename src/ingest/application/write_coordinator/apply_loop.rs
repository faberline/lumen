//! The apply loop: one task per coordinator that tails the log from the last
//! applied sequence, folds each record into the engine in order, appends it to
//! the local AOF, and completes its waiter.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use raft_runtime::OutcomeWindow;
use rustc_hash::FxHashMap;
use tokio::sync::Mutex as AsyncMutex;

use crate::app::observability::metrics::labels::{apply_item_count, ApplyKind, CoordinatorStage};
use crate::index::application::admission::record_reservation::{
    RecordReservation, RecordTransientReservation,
};
use crate::index::application::admission::RecordAdmissionError;
use crate::index::application::engine::Engine;
use crate::ingest::application::write_coordinator::committed_scalar::MappedApply;
use crate::ingest::application::write_coordinator::errors::{
    is_storage_full, RestartRequired, StorageFullError,
};
use crate::ingest::application::write_coordinator::local_record::{
    prepare_local_record, PreparedLocalRecord,
};
use crate::ingest::application::write_coordinator::mutation_gate::MutationGate;
use crate::ingest::application::write_coordinator::{
    CompletionState, PendingApply, SharedAof, WriteCoordinator, OUTCOME_WINDOW,
};
use crate::ingest::domain::change_budget::AdmissionError;
use crate::ingest::domain::wal_log::SharedWal;
use crate::ingest::domain::wal_record::WalRecord;

impl WriteCoordinator {
    /// The apply-loop spawner. Identical structure regardless of the AOF; the
    /// only AOF-specific work is the append after `complete`, conditioned on the
    /// `aof` being `Some` (the default `start_from` passes `None`).
    pub(super) fn start_from_inner(
        wal: SharedWal,
        engine: Arc<Engine>,
        from_seq: u64,
        aof: Option<SharedAof>,
    ) -> Arc<Self> {
        engine.capture_barrier.apply().initialize_sequence(from_seq);
        let coord = Arc::new_cyclic(|self_weak| Self {
            wal: wal.clone(),
            engine: engine.clone(),
            local_reservations: AsyncMutex::new(FxHashMap::default()),
            has_local_aof: aof.is_some(),
            capacity_relief_requested: AtomicBool::new(false),
            diagnostic_refusal_revision: AtomicU64::new(0),
            #[cfg(test)]
            diagnostic_capture: Mutex::new(None),
            layer_capacity_owner: Mutex::new(None),
            applied: AtomicU64::new(from_seq),
            completions: Mutex::new(CompletionState {
                outcomes: OutcomeWindow::new(OUTCOME_WINDOW),
                unresolved: FxHashMap::default(),
                waiters: FxHashMap::default(),
                mutation_permits: FxHashMap::default(),
            }),
            failed_head_reservations: Mutex::new(Vec::new()),
            failed_head_transient_reservations: Mutex::new(Vec::new()),
            failed_head_retentions: Mutex::new(FxHashMap::default()),
            self_weak: self_weak.clone(),
            mutation_gate: MutationGate::default(),
        });
        Self::start_capacity_relief(&coord);
        if let Some(aof) = aof.clone() {
            Self::start_aof_sync(&coord, aof);
        }
        let loop_coord = coord.clone();
        tokio::spawn(async move {
            let mut backoff = std::time::Duration::from_millis(100);
            // Outer loop: re-subscribe from the last-applied sequence whenever
            // the stream ends or the subscribe fails. An external-log restart can tear
            // down our ephemeral subscription, so the apply loop MUST recreate
            // it and resume tailing — otherwise writes silently stop applying
            // after a broker blip. Resuming from `applied` is safe:
            // redelivery is skipped idempotently below.
            loop {
                let from = loop_coord.applied.load(Ordering::Acquire);
                let mut sub = match wal.subscribe_admitted(from).await {
                    Ok(s) => {
                        backoff = std::time::Duration::from_millis(100);
                        s
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, from, "apply loop: subscribe failed; retrying");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(std::time::Duration::from_secs(5));
                        continue;
                    }
                };
                // A retry reservation covers the next decoded working copy.
                // Its source subscription is installed before the old one drops.
                let mut replay_reservation: Option<(u64, RecordReservation)> = None;
                while let Some(item) = sub.next().await {
                    match item {
                        Ok((seq, mut delivery)) => {
                            // Idempotent under redelivery: skip anything at or
                            // below what we've already applied. Defense-in-depth
                            // (#1486): a skipped sequence never reaches `complete`,
                            // so any local waiter for it (a submit() that published
                            // this exact seq) would otherwise hang forever — release
                            // it with a distinct, retryable error instead.
                            let local_reservation = loop_coord.take_local_reservation(seq).await;
                            let (mut reservation, mut transient) = if let Some((
                                expected,
                                reservation,
                            )) = replay_reservation.take()
                            {
                                if seq != expected || local_reservation.is_some() {
                                    // Losing the pinned head is a broken WAL contract. Never
                                    // acknowledge a later sequence as if that head had applied.
                                    engine.capture_barrier.apply().mark_uncertain();
                                    loop_coord.mutation_gate.require_restart();
                                    tracing::error!(
                                        seq,
                                        expected,
                                        "WAL replay did not return its capacity-blocked head; restart required"
                                    );
                                    return;
                                }
                                (Some(reservation), None)
                            } else {
                                match local_reservation {
                                    Some(local) => (Some(local.apply), local.transient),
                                    None => (None, None),
                                }
                            };
                            if seq <= loop_coord.applied.load(Ordering::Acquire) {
                                loop_coord.complete_stale(seq);
                                continue;
                            }
                            if reservation.is_none() {
                                match loop_coord
                                    .try_apply_mapped_delivery(seq, delivery, aof.clone())
                                    .await
                                {
                                    Ok(MappedApply::Fallback(retained)) => delivery = retained,
                                    Ok(MappedApply::Applied) => {
                                        loop_coord
                                            .failed_head_retentions
                                            .lock()
                                            .expect("failed-head retentions poisoned")
                                            .remove(&seq);
                                        continue;
                                    }
                                    Ok(MappedApply::Unresolved) => {
                                        // Advancing this subscription would release the source
                                        // whose AOF completion is uncertain. Retain it at head.
                                        futures::future::pending::<()>().await;
                                        unreachable!("unresolved mapped source remains pinned");
                                    }
                                    Err(error) => {
                                        engine.capture_barrier.apply().mark_uncertain();
                                        loop_coord.mutation_gate.require_restart();
                                        loop_coord.fail_unresolved(seq, Err(anyhow::Error::new(
                                            RestartRequired(format!("committed mapped WAL apply failed: {error}; source retained"))
                                        )), None);
                                        futures::future::pending::<()>().await;
                                        unreachable!("failed mapped source remains pinned");
                                    }
                                }
                                // Foreign delivery has no local publication reservation. Own
                                // its raw working copy first; the apply preparation below must
                                // acquire the rest before cloning AOF or normalized values.
                                let request = delivery.decoded_owned_bytes().ok().and_then(|raw| {
                                    delivery
                                        .extra_delivery_bytes(aof.is_some())
                                        .ok()
                                        .map(|extra| {
                                            engine.record_ram_request_from_bound(raw, extra)
                                        })
                                });
                                if let Some(request) = request {
                                    match engine.try_reserve_record_ram(&request) {
                                        Ok(admitted) => reservation = Some(admitted),
                                        Err(RecordAdmissionError::Capacity(
                                            AdmissionError::Full { .. },
                                        )) => {
                                            // The current MemWal delivery is not acknowledged
                                            // until another poll. Pin the unchanged applied cut
                                            // before dropping that subscription and working copy.
                                            let replay = loop {
                                                match wal
                                                    .subscribe_admitted(
                                                        loop_coord.applied.load(Ordering::Acquire),
                                                    )
                                                    .await
                                                {
                                                    Ok(replay) => break replay,
                                                    Err(error) => {
                                                        tracing::warn!(seq, %error, "could not pin committed WAL head for capacity retry");
                                                        tokio::time::sleep(
                                                            std::time::Duration::from_millis(200),
                                                        )
                                                        .await;
                                                    }
                                                }
                                            };
                                            drop(delivery);
                                            sub = replay;
                                            if let Err(error) = loop_coord.ensure_capacity_owner() {
                                                engine.capture_barrier.apply().mark_uncertain();
                                                loop_coord.mutation_gate.require_restart();
                                                loop_coord.fail_unresolved(
                                                    seq,
                                                    Err(anyhow::Error::new(RestartRequired(format!(
                                                        "could not start committed WAL capacity maintenance: {error}; source retained"
                                                    )))),
                                                    None,
                                                );
                                                futures::future::pending::<()>().await;
                                                unreachable!(
                                                    "failed committed head remains pinned"
                                                );
                                            }
                                            let eng = engine.clone();
                                            match tokio::task::spawn_blocking(move || {
                                                eng.wait_reserve_record_ram(&request)
                                            })
                                            .await
                                            {
                                                Ok(Ok(admitted)) => {
                                                    replay_reservation = Some((seq, admitted))
                                                }
                                                result => {
                                                    engine.capture_barrier.apply().mark_uncertain();
                                                    loop_coord.mutation_gate.require_restart();
                                                    tracing::error!(seq, error = ?result.map(|admission| admission.map(|_| ())), "committed WAL admission wait failed; restart required");
                                                    return;
                                                }
                                            }
                                            continue;
                                        }
                                        // Oversized raw records still need the streaming source
                                        // adapter. This branch retains the established dispatch
                                        // until that distinct preparation route is installed.
                                        Err(error) => tracing::warn!(
                                            seq,
                                            ?error,
                                            "committed WAL raw record requires streaming preparation"
                                        ),
                                    }
                                }
                            }
                            let Some(admitted_bytes) = reservation.as_ref().map(|apply| {
                                apply
                                    .bytes()
                                    .checked_add(
                                        transient
                                            .as_ref()
                                            .map(RecordTransientReservation::bytes)
                                            .unwrap_or_default(),
                                    )
                                    .expect("local admission total overflow")
                            }) else {
                                // A source larger than the process budget needs a streaming
                                // apply representation. Never decode it with invented credit.
                                engine.capture_barrier.apply().mark_uncertain();
                                loop_coord.mutation_gate.require_restart();
                                loop_coord.fail_unresolved(
                                    seq,
                                    Err(anyhow::Error::new(RestartRequired(
                                        "committed WAL record has no bounded delivery admission; source retained".into(),
                                    ))),
                                    reservation,
                                );
                                // Keep the subscription at the unresolved source. A next poll
                                // would release its bytes and permit a successor to apply.
                                futures::future::pending::<()>().await;
                                unreachable!(
                                    "the unresolved failed head must retain its subscription"
                                )
                            };
                            let mut pending = PendingApply {
                                seq,
                                delivery,
                                admitted_bytes,
                                reservation,
                                transient,
                                enqueued_at: std::time::Instant::now(),
                            };
                            if let Some(retention) = pending
                                .reservation
                                .as_mut()
                                .map(RecordReservation::source_retention)
                            {
                                let retained = pending.delivery.retain_source(retention.clone());
                                loop_coord
                                    .failed_head_retentions
                                    .lock()
                                    .expect("failed-head retentions poisoned")
                                    .insert(seq, retention);
                                if let Err(error) = retained {
                                    engine.capture_barrier.apply().mark_uncertain();
                                    loop_coord.mutation_gate.require_restart();
                                    loop_coord.fail_unresolved(
                                        seq,
                                        Err(anyhow::Error::new(RestartRequired(format!(
                                            "cannot retain committed WAL source {seq}: {error}"
                                        )))),
                                        pending.reservation.take(),
                                    );
                                    loop_coord.retain_failed_transient(pending.transient.take());
                                    futures::future::pending::<()>().await;
                                }
                            }
                            // A local record is admitted before publish and then applied one at a
                            // time. Do not prefetch another decoded record while this record owns
                            // capacity through its AOF and watermark boundary.
                            let eng = engine.clone();
                            let applying_coord = loop_coord.clone();
                            let local_aof = aof.clone();
                            let applied = tokio::task::spawn_blocking(move || {
                                // Keep this outside the unwind boundary. A panic after a
                                // locally admitted WAL record must retain both halves at the
                                // unresolved head, even though the transient half has no
                                // source-retention bridge of its own.
                                let mut transient = pending.transient.take();
                                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                let aof = local_aof;
                                let seq = pending.seq;
                                // The transient half is already admitted. It must remain alive
                                // across source decode, reprice waits, AOF clone/encode, append,
                                // and flush. It is never passed to Engine preparation.
                                let rec = match pending.delivery.read(pending.admitted_bytes) {
                                    Ok(rec) => rec,
                                    Err(error) => {
                                        eng.capture_barrier.apply().mark_uncertain();
                                        applying_coord.mutation_gate.require_restart();
                                        applying_coord.fail_unresolved(
                                            seq,
                                            Err(anyhow::Error::new(RestartRequired(format!(
                                                "committed WAL source read failed: {error}; restart required"
                                            )))),
                                            pending.reservation,
                                        );
                                        applying_coord.retain_failed_transient(transient.take());
                                        return false;
                                    }
                                };
                                let apply_kind = ApplyKind::from_entry(&rec.entry);
                                eng.metrics().observe_coordinator_stage(
                                    apply_kind,
                                    CoordinatorStage::PublishToApplyStart,
                                    pending.enqueued_at.elapsed(),
                                );
                                let version = rec.version;
                                // Persist the recoverable WAL tail before preparing the
                                // record.  Preparation acquires the CaptureBarrier apply
                                // lease; waiting for the AOF mutex while holding that lease
                                // can deadlock a checkpoint that is trying to capture the
                                // engine while another writer holds the AOF mutex.
                                let aof_rec = aof.as_ref().map(|_| WalRecord {
                                    version,
                                    entry: rec.entry.clone(),
                                });
                                if !applying_coord.mutation_gate.is_restart_required() {
                                    if let (Some(aof), Some(rec)) = (aof.as_ref(), aof_rec.as_ref()) {
                                        let persisted = {
                                            let mut writer =
                                                aof.lock().expect("aof writer poisoned");
                                            writer
                                                .append(seq, rec)
                                                .and_then(|()| writer.flush())
                                        };
                                        if let Err(e) = persisted {
                                            applying_coord.mutation_gate.require_restart();
                                            if is_storage_full(&e) {
                                                tracing::error!(
                                                    seq,
                                                    error = %e,
                                                    "AOF persist failed: ENOSPC — entering degraded read-only mode"
                                                );
                                                eng.metrics().mark_storage_degraded();
                                                applying_coord.fail_unresolved(
                                                    seq,
                                                    Err(anyhow::Error::new(StorageFullError(
                                                        format!(
                                                            "local storage is full (ENOSPC) persisting sequence {seq}; node entered degraded read-only mode"
                                                        ),
                                                    ))),
                                                    pending.reservation.take(),
                                                );
                                            } else {
                                                tracing::error!(
                                                    seq,
                                                    error = %e,
                                                    "AOF persist failed; restart required"
                                                );
                                                applying_coord.fail_unresolved(
                                                    seq,
                                                    Err(anyhow::Error::new(RestartRequired(
                                                        format!(
                                                            "could not persist local AOF sequence {seq}: {e}; restart this Lumen process"
                                                        ),
                                                    ))),
                                                    pending.reservation.take(),
                                                );
                                            }
                                            applying_coord.retain_failed_transient(transient.take());
                                            return false;
                                        }
                                    }
                                }
                                // A prepared guard owns the apply lease and its retained charge
                                // until after AOF persistence and watermark advancement below.
                                let mut prepared = None;
                                // #4326: per-record apply cost, measured across the whole
                                // prepare+apply span below (including any reprice/
                                // wait_grow_to retry inside `prepare_local_record`) so a
                                // 500k-doc perf probe can read per-record apply cost from
                                // `GET /metrics` — see `Metrics::observe_coordinator_apply`.
                                let apply_started_at = std::time::Instant::now();
                                let mut outcome = match pending.reservation {
                                    Some(reservation) => {
                                        // A post-WAL exact reprice may need capacity that was
                                        // unavailable at publication time.  Keep this ordered
                                        // head in the existing dedicated blocking worker.  No
                                        // apply lease is held while the reservation waits, and
                                        // the subscription cannot advance to a later record.
                                        let mut entry = rec.entry;
                                        let mut reservation = reservation;
                                        let prepared_local = loop {
                                            match prepare_local_record(
                                                eng.as_ref(),
                                                entry,
                                                reservation,
                                            ) {
                                                Ok(PreparedLocalRecord::CapacityBlocked {
                                                    entry: blocked_entry,
                                                    reservation: mut blocked_reservation,
                                                    required,
                                                }) => {
                                                    if let Err(error) =
                                                        applying_coord.ensure_capacity_owner()
                                                    {
                                                        break Ok(PreparedLocalRecord::Unresolved {
                                                            reservation: blocked_reservation,
                                                            error,
                                                        });
                                                    }
                                                    eng.request_pending_checkpoint();
                                                    match blocked_reservation.wait_grow_to(required) {
                                                        Ok(()) => {
                                                            entry = blocked_entry;
                                                            reservation = blocked_reservation;
                                                        }
                                                        Err(error) => {
                                                            break Ok(PreparedLocalRecord::Unresolved {
                                                                reservation: blocked_reservation,
                                                                error: anyhow::Error::new(
                                                                    RecordAdmissionError::Capacity(
                                                                        error,
                                                                    ),
                                                                ),
                                                            });
                                                        }
                                                    }
                                                }
                                                other => break other,
                                            }
                                        };
                                        match prepared_local {
                                        Ok(PreparedLocalRecord::Prepared(mut guard)) => {
                                            // This copy is made only after the full normalized
                                            // and transport reservation became a retained charge.
                                            let prepared_entry =
                                                guard.entry().expect("unapplied prepared entry");
                                            let apply_kind =
                                                ApplyKind::from_entry(prepared_entry);
                                            let apply_items =
                                                apply_item_count(prepared_entry);
                                            let outcome = eng.apply_prepared_raft_entry(&mut guard);
                                            eng.metrics().observe_coordinator_apply(
                                                apply_kind,
                                                apply_items,
                                                apply_started_at.elapsed(),
                                            );
                                            prepared = Some(guard);
                                            outcome
                                        }
                                        Ok(PreparedLocalRecord::Unresolved {
                                            reservation,
                                            error,
                                        }) => {
                                            // No apply guard proves the full normalized record is
                                            // charged. Retain this committed source and stop at its
                                            // prefix instead of using generic uncharged apply.
                                            eng.capture_barrier.apply().mark_uncertain();
                                            applying_coord.mutation_gate.require_restart();
                                            applying_coord.fail_unresolved(
                                                seq,
                                                Err(anyhow::Error::new(RestartRequired(format!(
                                                "committed WAL record has no charged apply guard: {error}; source retained"
                                                )))),
                                                Some(reservation),
                                            );
                                            applying_coord
                                                .retain_failed_transient(transient.take());
                                            return false;
                                        }
                                        Ok(PreparedLocalRecord::CapacityBlocked { .. }) => {
                                            unreachable!("capacity-blocked local record must wait or resolve")
                                        }
                                        Err(error) => {
                                            eng.capture_barrier.apply().mark_uncertain();
                                            applying_coord.mutation_gate.require_restart();
                                            applying_coord.fail_unresolved(
                                                seq,
                                                Err(anyhow::Error::new(RestartRequired(format!(
                                                "committed WAL preparation returned unexpectedly: {error}; source retained"
                                                )))),
                                                None,
                                            );
                                            applying_coord
                                                .retain_failed_transient(transient.take());
                                            return false;
                                        }
                                        }
                                    }
                                    None => {
                                        // Admission reaches this branch only when no bounded
                                        // committed representation exists. Do not let it escape
                                        // through an uncharged generic apply.
                                        eng.capture_barrier.apply().mark_uncertain();
                                        applying_coord.mutation_gate.require_restart();
                                        applying_coord.fail_unresolved(
                                            seq,
                                            Err(anyhow::Error::new(RestartRequired(
                                                "committed WAL record has no charged apply guard; source retained"
                                                    .into(),
                                            ))),
                                            None,
                                        );
                                        applying_coord.retain_failed_transient(transient.take());
                                        return false;
                                    }
                                };
                                let apply = prepared.as_ref().map(|guard| guard.apply_lease());
                                // A preceding AOF gap makes every later record
                                // unrecoverable, including one whose Engine
                                // application reports a normal validation
                                // error.  Do not append or acknowledge it.
                                if applying_coord.mutation_gate.is_restart_required() {
                                    outcome = Err(anyhow::Error::new(RestartRequired(format!(
                                        "sequence {seq} applied after an earlier local AOF gap; restart this Lumen process"
                                    ))));
                                }
                                if let Err(e) = &outcome {
                                    tracing::warn!(seq, error = %e, "apply error (entry no-ops)");
                                }
                                apply
                                    .expect("completed committed record has a charged apply guard")
                                    .advance_sequence(seq);
                                eng.metrics().observe_coordinator_stage(
                                    apply_kind,
                                    CoordinatorStage::ApplyToWaiter,
                                    apply_started_at.elapsed(),
                                );
                                // Release both halves before publishing the applied watermark.
                                // A cancelled local submit has no waiter to observe completion,
                                // so `applied_seq` is its only completion signal. Publishing it
                                // first would let callers observe an applied record while its
                                // reservation is still charged on the budget.
                                drop(apply);
                                drop(prepared.take());
                                drop(transient.take());
                                applying_coord.complete(seq, outcome);
                                true
                                }));
                                (result, transient)
                            }).await;
                            match applied {
                                Ok((Ok(true), transient)) => {
                                    debug_assert!(
                                        transient.is_none(),
                                        "completed local apply must release its transient reservation"
                                    );
                                    loop_coord
                                        .failed_head_retentions
                                        .lock()
                                        .expect("failed-head retentions poisoned")
                                        .remove(&seq);
                                }
                                Ok((Ok(false), transient)) => {
                                    loop_coord.retain_failed_transient(transient);
                                    // Keep this subscription at its failed head. Polling again
                                    // would acknowledge source bytes and permit a successor.
                                    futures::future::pending::<()>().await;
                                }
                                Ok((Err(_), transient)) => {
                                    // The lease also latches uncertainty while unwinding, before
                                    // another checkpoint can enter the failed apply interval.
                                    engine.capture_barrier.apply().mark_uncertain();
                                    loop_coord.mutation_gate.require_restart();
                                    loop_coord.retain_failed_transient(transient);
                                    loop_coord.fail_unresolved(
                                        seq,
                                        Err(anyhow::Error::new(RestartRequired(format!(
                                            "apply task panicked; restart required"
                                        )))),
                                        None,
                                    );
                                    // Retain the subscription floor after a panic too. The source
                                    // might still own the committed head's bytes.
                                    futures::future::pending::<()>().await;
                                }
                                Err(error) => {
                                    // A JoinError means the worker did not return its transient
                                    // half (for example, runtime shutdown). The retained source
                                    // still pins its apply half; require restart and preserve the
                                    // subscription floor rather than claiming completion.
                                    engine.capture_barrier.apply().mark_uncertain();
                                    loop_coord.mutation_gate.require_restart();
                                    loop_coord.fail_unresolved(
                                        seq,
                                        Err(anyhow::Error::new(RestartRequired(format!(
                                            "apply task stopped: {error}; restart required"
                                        )))),
                                        None,
                                    );
                                    futures::future::pending::<()>().await;
                                }
                            }
                        }
                        Err(e) => tracing::warn!(error = %e, "apply loop: stream item error"),
                    }
                }
                // Stream ended (e.g. external-log restart killed the ephemeral
                // consumer). Re-subscribe from the applied head after a short
                // pause so we don't tight-spin if the broker is flapping.
                tracing::warn!("apply loop: stream ended; re-subscribing from applied seq");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        });
        coord
    }
}
