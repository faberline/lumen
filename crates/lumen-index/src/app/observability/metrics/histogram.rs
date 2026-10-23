//! The bounded duration histogram the coordinator apply, lock and
//! capacity-relief families share: its buckets, the stack-local observations a
//! committed apply collects, and the helpers that record and render it.

use std::fmt::Write as _;
use std::time::Duration;

use metrics_prometheus::Counter;

#[cfg(doc)]
use crate::app::observability::metrics::search::SEARCH_LATENCY_BUCKETS_US;
#[cfg(doc)]
use crate::app::observability::metrics::Metrics;

/// #4326: number of finite `lumen_coordinator_apply_seconds_bucket{le=...}`
/// rows per `kind` — see [`APPLY_SECONDS_BUCKETS_US`].
pub(super) const APPLY_SECONDS_BUCKET_COUNT: usize = 12;

/// #4326: `lumen_coordinator_apply_seconds` histogram bucket upper bounds,
/// one entry per `(the Prometheus "le" label, the same bound in whole
/// microseconds)`. Mirrors [`SEARCH_LATENCY_BUCKETS_US`]'s shape and
/// [`Metrics::observe_search`]'s bucket-assignment rule (`<=`, cumulative at
/// render time — see [`Metrics::render_coordinator_apply_histogram`]), sized
/// instead for a single-record apply: sub-millisecond to a 10s outlier.
pub(super) const APPLY_SECONDS_BUCKETS_US: [(&str, u64); APPLY_SECONDS_BUCKET_COUNT] = [
    ("0.001", 1_000),
    ("0.005", 5_000),
    ("0.01", 10_000),
    ("0.025", 25_000),
    ("0.05", 50_000),
    ("0.1", 100_000),
    ("0.25", 250_000),
    ("0.5", 500_000),
    ("1", 1_000_000),
    ("2.5", 2_500_000),
    ("5", 5_000_000),
    ("10", 10_000_000),
];

/// Stack-local histogram observations for one committed-apply scope.
#[derive(Default)]
pub(super) struct ApplyDurationObservations {
    buckets: [u64; APPLY_SECONDS_BUCKET_COUNT],
    micros_sum: u64,
    count: u64,
}

impl ApplyDurationObservations {
    pub(super) fn record(&mut self, elapsed: Duration) {
        let micros = duration_to_micros(elapsed);
        if let Some(bucket_idx) = APPLY_SECONDS_BUCKETS_US
            .iter()
            .position(|&(_, bound_us)| micros <= bound_us)
        {
            self.buckets[bucket_idx] = self.buckets[bucket_idx].wrapping_add(1);
        }
        self.micros_sum = self.micros_sum.wrapping_add(micros);
        self.count = self.count.wrapping_add(1);
    }
}

pub(super) fn duration_to_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// Record one observation in an unlabelled histogram that shares the bounded
/// apply-latency buckets. The stored buckets are exclusive; rendering makes
/// them cumulative as Prometheus requires.
pub(super) fn observe_apply_duration_histogram(
    buckets: &[Counter; APPLY_SECONDS_BUCKET_COUNT],
    micros_sum: &Counter,
    count: &Counter,
    elapsed: Duration,
) {
    let micros = duration_to_micros(elapsed);
    if let Some(bucket_idx) = APPLY_SECONDS_BUCKETS_US
        .iter()
        .position(|&(_, bound_us)| micros <= bound_us)
    {
        buckets[bucket_idx].incr();
    }
    micros_sum.add(micros);
    count.incr();
}

/// Merge a stack-local batch of committed-apply observations after its state
/// write lock has dropped. Each bucket remains an exclusive bucket.
pub(super) fn observe_apply_duration_observations(
    buckets: &[Counter; APPLY_SECONDS_BUCKET_COUNT],
    micros_sum: &Counter,
    count: &Counter,
    observations: &ApplyDurationObservations,
) {
    for (bucket, observed) in buckets.iter().zip(observations.buckets) {
        bucket.add(observed);
    }
    micros_sum.add(observations.micros_sum);
    count.add(observations.count);
}

/// Render one unlabelled bounded histogram that uses
/// [`APPLY_SECONDS_BUCKETS_US`] and an explicit `+Inf` count.
pub(super) fn render_apply_duration_histogram(
    out: &mut String,
    name: &str,
    help: &str,
    buckets: &[Counter; APPLY_SECONDS_BUCKET_COUNT],
    micros_sum: &Counter,
    count: &Counter,
) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} histogram");
    let mut cumulative = 0u64;
    for ((le, _), bucket) in APPLY_SECONDS_BUCKETS_US.iter().zip(buckets.iter()) {
        cumulative += bucket.get();
        let _ = writeln!(out, "{name}_bucket{{le=\"{le}\"}} {cumulative}");
    }
    let total = count.get();
    let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {total}");
    let sum_seconds = micros_sum.get() as f64 / 1_000_000.0;
    let _ = writeln!(out, "{name}_sum {sum_seconds}");
    let _ = writeln!(out, "{name}_count {total}");
}

pub(super) fn render_duration_histogram(
    out: &mut String,
    base: &str,
    help: &str,
    micros_sum: u64,
    count: u64,
) {
    let seconds = micros_sum as f64 / 1_000_000.0;
    let _ = writeln!(out, "# HELP {base} {help}");
    let _ = writeln!(out, "# TYPE {base} histogram");
    let _ = writeln!(out, "{base}_bucket{{le=\"+Inf\"}} {count}");
    let _ = writeln!(out, "{base}_sum {seconds}");
    let _ = writeln!(out, "{base}_count {count}");
}
