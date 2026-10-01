//! The search metrics (#2519): the latency histogram's buckets, observe_search,
//! which also counts a search slower than `LUMEN_SLOW_QUERY_MS`, and the
//! histogram's render.

use std::fmt::Write as _;
use std::time::Duration;

use crate::app::observability::metrics::Metrics;

/// #2519: number of finite `lumen_search_latency_seconds_bucket{le=...}`
/// rows — see [`SEARCH_LATENCY_BUCKETS_US`].
pub(super) const SEARCH_LATENCY_BUCKET_COUNT: usize = 12;

/// #2519: search-latency histogram bucket upper bounds, sized for search
/// SLOs from a sub-millisecond fast path up to a 5s outlier tail. Each pair
/// is `(the Prometheus "le" label, the same bound in whole microseconds)` —
/// microseconds because every bound here is an exact conversion of a "nice"
/// decimal-seconds SLO value (e.g. `0.0025s == 2_500us`), so bucket
/// assignment in [`Metrics::observe_search`] is exact integer comparison,
/// never float rounding. Bucket `i` (see `search_latency_buckets`) counts
/// observations in `(bound[i-1], bound[i]]`, with `bound[-1] == 0`.
/// Observations past the last bound aren't stored in a 13th counter — the
/// Prometheus `+Inf` bucket renders as the plain total observation count
/// instead (see `render_search_latency_histogram`).
pub(super) const SEARCH_LATENCY_BUCKETS_US: [(&str, u64); SEARCH_LATENCY_BUCKET_COUNT] = [
    ("0.001", 1_000),
    ("0.0025", 2_500),
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
];

/// #2519: `lumen_slow_queries_total`'s threshold (milliseconds) when
/// `LUMEN_SLOW_QUERY_MS` is unset or unparseable — see
/// `slow_query_threshold_ms_from_env`.
pub(super) const DEFAULT_SLOW_QUERY_THRESHOLD_MS: u64 = 500;

impl Metrics {
    /// Record one search observation of `elapsed`. Updates the deprecated
    /// millisecond sum/count pair (back-compat, see `search_latency_ms_sum`),
    /// the `lumen_search_latency_seconds` histogram (#2519), and
    /// `slow_queries_total` when `elapsed` meets or exceeds
    /// `slow_query_threshold_ms` — `>=`, not `>`, so a
    /// `LUMEN_SLOW_QUERY_MS=0` override (used by tests to force every
    /// search to count as slow) actually fires on a `0`ms observation.
    pub fn observe_search(&self, elapsed: Duration) {
        self.search_requests_total.incr();

        let latency_ms = elapsed.as_millis() as u64;
        self.search_latency_ms_sum.add(latency_ms);
        self.search_latency_ms_count.incr();

        let latency_us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.search_latency_us_sum.add(latency_us);
        if let Some(idx) = SEARCH_LATENCY_BUCKETS_US
            .iter()
            .position(|&(_, bound_us)| latency_us <= bound_us)
        {
            self.search_latency_buckets[idx].incr();
        }

        if latency_ms >= self.slow_query_threshold_ms.get() {
            self.slow_queries_total.incr();
        }
    }

    /// #2519: renders `lumen_search_latency_seconds_bucket{le=...}`
    /// (cumulative, per the Prometheus histogram convention) followed by
    /// `_sum` and `_count`, all under one `# HELP`/`# TYPE histogram`
    /// declaration. See [`SEARCH_LATENCY_BUCKETS_US`] for the bucket
    /// bounds and `search_latency_ms_count`'s doc for why `_count`/`+Inf`
    /// reuse that field instead of a dedicated histogram-count atomic.
    pub(super) fn render_search_latency_histogram(&self) -> String {
        const NAME: &str = "lumen_search_latency_seconds";
        let mut out = String::new();
        let _ = writeln!(
            out,
            "# HELP {NAME} Search latency histogram in seconds, bucketed for search SLOs."
        );
        let _ = writeln!(out, "# TYPE {NAME} histogram");
        let mut cumulative = 0u64;
        for ((le, _bound_us), bucket) in SEARCH_LATENCY_BUCKETS_US
            .iter()
            .zip(self.search_latency_buckets.iter())
        {
            cumulative += bucket.get();
            let _ = writeln!(out, "{NAME}_bucket{{le=\"{le}\"}} {cumulative}");
        }
        let total = self.search_latency_ms_count.get();
        let _ = writeln!(out, "{NAME}_bucket{{le=\"+Inf\"}} {total}");
        let sum_seconds = self.search_latency_us_sum.get() as f64 / 1_000_000.0;
        let _ = writeln!(out, "{NAME}_sum {sum_seconds}");
        let _ = writeln!(out, "{NAME}_count {total}");
        out
    }
}

/// #2519: `LUMEN_SLOW_QUERY_MS` (milliseconds) if set and parseable to a
/// `u64`, else [`DEFAULT_SLOW_QUERY_THRESHOLD_MS`]. Read once at
/// `Metrics::new()` construction — mirrors `hnsw_search_ef`'s
/// read-once-per-construction convention in `src/index/domain/vector/hnsw_cpu_index.rs`.
pub(super) fn slow_query_threshold_ms_from_env() -> u64 {
    std::env::var("LUMEN_SLOW_QUERY_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_SLOW_QUERY_THRESHOLD_MS)
}

#[cfg(test)]
mod tests;
