//! The process-wide gauges each render refreshes: the Linux `VmHWM` RSS high
//! water, and the pending durable-change bytes the shared ChangeBudget
//! accounts.

use crate::app::observability::metrics::Metrics;
use crate::ingest::domain::change_budget::ChangeBudget;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ProcessRssSnapshot {
    pub(super) bytes: u64,
    pub(super) available: bool,
}

#[derive(Default)]
// Linux reports a process-wide peak. Keep the largest actual observation;
// an unavailable scrape must not erase its history or advertise availability.
struct RssHighWaterState {
    bytes: u64,
}

impl RssHighWaterState {
    fn observe(&mut self, observed: Option<u64>) -> ProcessRssSnapshot {
        match observed {
            Some(bytes) => {
                self.bytes = self.bytes.max(bytes);
                ProcessRssSnapshot {
                    bytes: self.bytes,
                    available: true,
                }
            }
            None => ProcessRssSnapshot {
                bytes: 0,
                available: false,
            },
        }
    }
}

static PROCESS_RSS_HIGH_WATER: std::sync::Mutex<RssHighWaterState> =
    std::sync::Mutex::new(RssHighWaterState { bytes: 0 });

#[derive(Clone, Copy)]
pub(super) struct PendingChangeAccounting {
    pub(super) reserved: u64,
    pub(super) active: u64,
    pub(super) frozen: u64,
    pub(super) total: u64,
    pub(super) high_water: u64,
}

impl Metrics {
    /// Refresh the Linux process peak resident set size from `/proc`. Returns
    /// `true` only when the current scrape parsed `VmHWM`; a caller can use
    /// the rendered availability gauge to reject unavailable values.
    pub fn refresh_process_rss_high_water(&self) -> bool {
        self.read_process_rss_high_water().available
    }

    pub(super) fn read_process_rss_high_water(&self) -> ProcessRssSnapshot {
        let observed = process_rss_high_water_bytes();
        let mut state = PROCESS_RSS_HIGH_WATER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let sample = state.observe(observed);
        self.process_rss_high_water_bytes.set(sample.bytes);
        self.process_rss_high_water_available
            .set(u64::from(sample.available));
        sample
    }

    /// Refresh process-wide pending-change accounting from the shared budget.
    /// The high-water gauge is monotonic in the budget and therefore retains
    /// increases that happened between Prometheus scrapes.
    pub fn refresh_pending_change_accounting(&self) {
        let budget = ChangeBudget::process_shared();
        self.read_pending_change_accounting(&budget);
    }

    /// Read one coherent budget state and mirror it into the compatibility
    /// gauges. Render uses the returned local values, never five later atomic
    /// reads that another concurrent render could mix.
    pub(super) fn read_pending_change_accounting(
        &self,
        budget: &ChangeBudget,
    ) -> PendingChangeAccounting {
        let (snapshot, high_water) = budget.snapshot_with_high_water();
        let values = PendingChangeAccounting {
            reserved: snapshot.reserved as u64,
            active: snapshot.active as u64,
            frozen: snapshot.frozen as u64,
            total: snapshot.total as u64,
            high_water: high_water as u64,
        };
        self.pending_change_reserved_bytes.set(values.reserved);
        self.pending_change_active_bytes.set(values.active);
        self.pending_change_frozen_bytes.set(values.frozen);
        self.pending_change_total_bytes.set(values.total);
        self.pending_change_high_water_bytes.set(values.high_water);
        values
    }
}

/// Parse the Linux `/proc/<pid>/status` `VmHWM` row. Linux reports this
/// value in exact `kB` units; any missing row, wrong unit, malformed value,
/// duplicate fields, or multiplication overflow is unavailable rather than a
/// fabricated byte count.
#[cfg(any(target_os = "linux", test))]
fn parse_linux_vmhwm_bytes(status: &str) -> Option<u64> {
    let mut saw_vmhwm = false;
    let mut bytes = None;
    for line in status.lines() {
        let Some(value) = line.strip_prefix("VmHWM:") else {
            continue;
        };
        if saw_vmhwm {
            return None;
        }
        saw_vmhwm = true;
        let mut words = value.split_ascii_whitespace();
        let kibibytes = words.next()?.parse::<u64>().ok()?;
        if words.next()? != "kB" || words.next().is_some() {
            return None;
        }
        bytes = kibibytes.checked_mul(1024);
    }
    bytes
}

fn process_rss_high_water_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| parse_linux_vmhwm_bytes(&status))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(test)]
mod tests;
