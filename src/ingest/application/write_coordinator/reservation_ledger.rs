//! The sequence-keyed ledger of locally admitted reservations.
//!
//! A local submit installs its reservation after WAL publication returns the
//! sequence. The apply loop can observe that sequence before publication
//! returns, so it must not apply the record through the unadmitted path while
//! the publisher is still installing it.
//!
//! Publications are serialized, so at most one can be in flight. The apply
//! loop waits only for the publication that was in flight when it first looked
//! up its sequence. A later publication cannot own an already delivered
//! sequence. The apply loop therefore never queues behind admitted submits
//! that are still waiting for their turn to publish.

use std::sync::{Mutex, MutexGuard};

use rustc_hash::FxHashMap;
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard, Notify};

pub(super) struct ReservationLedger<T> {
    publication: AsyncMutex<()>,
    state: Mutex<LedgerState<T>>,
    changed: Notify,
}

pub(super) struct LedgerState<T> {
    pub(super) reservations: FxHashMap<u64, T>,
    /// Generation of the latest publication that obtained its turn.
    started: u64,
    /// Generation of the latest publication that installed or abandoned its
    /// reservation. Publications are serialized, so this never passes
    /// `started` and every earlier generation has also completed.
    completed: u64,
    /// A capacity-relief pass is deciding whether to drop this reservation.
    staging: Option<u64>,
}

/// One publication's turn. Dropping it without installing a reservation
/// still completes its generation, so the apply loop cannot wait on it forever.
pub(super) struct Publication<'a, T> {
    ledger: &'a ReservationLedger<T>,
    _turn: AsyncMutexGuard<'a, ()>,
    generation: u64,
    completed: bool,
}

/// A reservation that capacity relief is staging. The apply loop cannot take
/// it until relief either removes it or drops this guard to retain it.
pub(super) struct Staging<'a, T> {
    ledger: &'a ReservationLedger<T>,
    seq: u64,
}

impl<T> Default for ReservationLedger<T> {
    fn default() -> Self {
        Self {
            publication: AsyncMutex::new(()),
            state: Mutex::new(LedgerState {
                reservations: FxHashMap::default(),
                started: 0,
                completed: 0,
                staging: None,
            }),
            changed: Notify::new(),
        }
    }
}

impl<T> ReservationLedger<T> {
    fn state(&self) -> MutexGuard<'_, LedgerState<T>> {
        self.state
            .lock()
            .expect("local reservation ledger poisoned")
    }

    /// Wait for this publication's turn. Hold the returned value from before
    /// WAL publication until its reservation is installed.
    pub(super) async fn begin_publication(&self) -> Publication<'_, T> {
        let turn = self.publication.lock().await;
        let mut state = self.state();
        state.started += 1;
        let generation = state.started;
        drop(state);
        Publication {
            ledger: self,
            _turn: turn,
            generation,
            completed: false,
        }
    }

    /// Remove the reservation for a delivered sequence. Returns `None` once
    /// no publication that could own `seq` remains in flight.
    pub(super) async fn take(&self, seq: u64) -> Option<T> {
        let mut owner = None;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.state();
                if state.staging != Some(seq) {
                    if let Some(reservation) = state.reservations.remove(&seq) {
                        return Some(reservation);
                    }
                    let owner = *owner.get_or_insert(state.started);
                    if state.completed >= owner {
                        return None;
                    }
                }
            }
            changed.await;
        }
    }

    /// Mark the reservation selected by `select` as staging.
    pub(super) fn begin_staging(
        &self,
        select: impl FnOnce(&FxHashMap<u64, T>) -> Option<u64>,
    ) -> Option<Staging<'_, T>> {
        let mut state = self.state();
        debug_assert!(
            state.staging.is_none(),
            "one capacity relief pass stages at a time"
        );
        let seq = select(&state.reservations)?;
        state.staging = Some(seq);
        Some(Staging { ledger: self, seq })
    }

    #[cfg(test)]
    pub(super) fn lock(&self) -> MutexGuard<'_, LedgerState<T>> {
        self.state()
    }
}

impl<T> Publication<'_, T> {
    /// Install the reservation for the sequence this publication returned.
    /// Returns a reservation already installed for that sequence.
    pub(super) fn install(mut self, seq: u64, reservation: T) -> Option<T> {
        let mut state = self.ledger.state();
        let previous = state.reservations.insert(seq, reservation);
        state.completed = self.generation;
        drop(state);
        self.completed = true;
        self.ledger.changed.notify_waiters();
        previous
    }
}

impl<T> Drop for Publication<'_, T> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut state = self
            .ledger
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.completed = self.generation;
        drop(state);
        self.ledger.changed.notify_waiters();
    }
}

impl<T> Staging<'_, T> {
    pub(super) fn seq(&self) -> u64 {
        self.seq
    }

    /// Remove the staged reservation. Drop the result outside the ledger.
    pub(super) fn remove(self) -> Option<T> {
        let removed = self.ledger.state().reservations.remove(&self.seq);
        removed
    }
}

impl<T> Drop for Staging<'_, T> {
    fn drop(&mut self) {
        let mut state = self
            .ledger
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.staging = None;
        drop(state);
        self.ledger.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::ReservationLedger;

    const SETTLE: Duration = Duration::from_millis(50);

    #[tokio::test]
    async fn take_finds_an_installed_reservation_while_a_later_publication_is_in_flight() {
        let ledger = Arc::new(ReservationLedger::<&str>::default());
        assert!(ledger
            .begin_publication()
            .await
            .install(1, "first")
            .is_none());
        // The next admitted submit holds its turn across a slow WAL publish.
        let later = ledger.begin_publication().await;
        let taken = tokio::time::timeout(SETTLE, ledger.take(1)).await;
        assert_eq!(
            taken,
            Ok(Some("first")),
            "the apply loop must not queue behind a later publication"
        );
        drop(later);
    }

    #[tokio::test]
    async fn take_does_not_wait_behind_queued_publications() {
        let ledger = Arc::new(ReservationLedger::<u64>::default());
        assert!(ledger.begin_publication().await.install(1, 1).is_none());
        let holder = ledger.begin_publication().await;
        let queued: Vec<_> = (0..8)
            .map(|_| {
                let ledger = ledger.clone();
                tokio::spawn(async move {
                    let publication = ledger.begin_publication().await;
                    tokio::time::sleep(SETTLE).await;
                    drop(publication);
                })
            })
            .collect();
        tokio::task::yield_now().await;
        let taken = tokio::time::timeout(SETTLE, ledger.take(1)).await;
        assert_eq!(taken, Ok(Some(1)));
        drop(holder);
        for task in queued {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn take_waits_for_the_in_flight_publication_to_install() {
        let ledger = Arc::new(ReservationLedger::<&str>::default());
        let publication = ledger.begin_publication().await;
        let take = tokio::spawn({
            let ledger = ledger.clone();
            async move { ledger.take(1).await }
        });
        tokio::time::sleep(SETTLE).await;
        assert!(
            !take.is_finished(),
            "a sequence delivered while its publication returns must wait for the reservation"
        );
        assert!(publication.install(1, "admitted").is_none());
        assert_eq!(take.await.unwrap(), Some("admitted"));
    }

    #[tokio::test]
    async fn take_without_publication_in_flight_returns_none() {
        let ledger = ReservationLedger::<&str>::default();
        assert_eq!(ledger.take(7).await, None);
        drop(ledger.begin_publication().await);
        assert_eq!(ledger.take(7).await, None);
    }

    #[tokio::test]
    async fn take_is_not_starved_by_successive_publications() {
        let ledger = Arc::new(ReservationLedger::<&str>::default());
        let first = ledger.begin_publication().await;
        let take = tokio::spawn({
            let ledger = ledger.clone();
            async move { ledger.take(5).await }
        });
        tokio::time::sleep(SETTLE).await;
        assert!(first.install(4, "other").is_none());
        // A later publication cannot own the already delivered sequence.
        let second = ledger.begin_publication().await;
        let taken = tokio::time::timeout(SETTLE, take).await;
        assert_eq!(taken.map(Result::unwrap), Ok(None));
        drop(second);
    }

    #[tokio::test]
    async fn abandoned_publication_releases_waiting_take() {
        let ledger = Arc::new(ReservationLedger::<&str>::default());
        let publication = ledger.begin_publication().await;
        let take = tokio::spawn({
            let ledger = ledger.clone();
            async move { ledger.take(1).await }
        });
        tokio::time::sleep(SETTLE).await;
        drop(publication);
        assert_eq!(take.await.unwrap(), None);
    }

    #[tokio::test]
    async fn take_waits_while_capacity_relief_stages_its_reservation() {
        let ledger = Arc::new(ReservationLedger::<&str>::default());
        assert!(ledger
            .begin_publication()
            .await
            .install(3, "kept")
            .is_none());
        let staging = ledger
            .begin_staging(|reservations| reservations.keys().next().copied())
            .expect("one reservation is pending");
        assert_eq!(staging.seq(), 3);
        let take = tokio::spawn({
            let ledger = ledger.clone();
            async move { ledger.take(3).await }
        });
        tokio::time::sleep(SETTLE).await;
        assert!(
            !take.is_finished(),
            "relief owns the reservation while it stages"
        );
        drop(staging);
        assert_eq!(take.await.unwrap(), Some("kept"));

        assert!(ledger
            .begin_publication()
            .await
            .install(4, "staged")
            .is_none());
        let staging = ledger
            .begin_staging(|reservations| reservations.keys().next().copied())
            .expect("one reservation is pending");
        let take = tokio::spawn({
            let ledger = ledger.clone();
            async move { ledger.take(4).await }
        });
        tokio::time::sleep(SETTLE).await;
        assert_eq!(staging.remove(), Some("staged"));
        assert_eq!(take.await.unwrap(), None);
    }
}
