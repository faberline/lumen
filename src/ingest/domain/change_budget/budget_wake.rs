//! How checkpoint drivers and blocked admissions wait for the budget to change.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::ingest::domain::change_budget::BudgetWake;

impl BudgetWake {
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub fn wait_for_change(&self, observed: u64) {
        let mut guard = self.lock.lock().expect("budget wake lock poisoned");
        while self.epoch() == observed {
            guard = self.changed.wait(guard).expect("budget wake lock poisoned");
        }
    }

    /// Returns true when a new epoch arrives before `timeout` expires.
    /// The wake mutex is dropped before this method returns.
    pub fn wait_for_change_timeout(&self, observed: u64, timeout: Duration) -> bool {
        let deadline = Instant::now().checked_add(timeout);
        let mut guard = self.lock.lock().expect("budget wake lock poisoned");
        while self.epoch() == observed {
            let remaining = deadline
                .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
                .unwrap_or_default();
            if remaining.is_zero() {
                return false;
            }
            let (next, timeout) = self
                .changed
                .wait_timeout(guard, remaining)
                .expect("budget wake lock poisoned");
            guard = next;
            if timeout.timed_out() {
                return self.epoch() != observed;
            }
        }
        true
    }

    /// Wait without occupying a blocking thread. Register before checking the
    /// epoch so a publication between those operations cannot be missed.
    pub async fn wait_for_change_async(&self, observed: u64, timeout: Duration) -> bool {
        let notified = self.async_changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.epoch() != observed {
            return true;
        }
        tokio::time::timeout(timeout, notified).await.is_ok() || self.epoch() != observed
    }

    pub(super) fn signal(&self) {
        // This shares the waiter predicate mutex, so notify cannot race a
        // predicate check immediately before Condvar::wait.
        let _guard = self.lock.lock().expect("budget wake lock poisoned");
        self.epoch.fetch_add(1, Ordering::Release);
        self.changed.notify_all();
        self.async_changed.notify_waiters();
    }
}
