use std::time::Duration;

use crate::app::observability::metrics::Metrics;

/// #2519: a search observation lands in the correct cumulative
/// histogram bucket, and every bucket >= its own falls in too (a
/// Prometheus histogram bucket is "le", not exclusive).
#[test]
fn observe_search_updates_histogram_buckets() {
    let m = Metrics::new();
    m.observe_search(Duration::from_micros(1_500)); // 1.5ms -> le=0.0025 bucket
    let out = m.render();
    assert!(
        out.contains("lumen_search_latency_seconds_bucket{le=\"0.001\"} 0"),
        "1.5ms observation must not count in the 1ms bucket:\n{out}"
    );
    assert!(
        out.contains("lumen_search_latency_seconds_bucket{le=\"0.0025\"} 1"),
        "1.5ms observation must count in the 2.5ms bucket:\n{out}"
    );
    assert!(
        out.contains("lumen_search_latency_seconds_bucket{le=\"5\"} 1"),
        "cumulative buckets past the observation's own bucket must include it:\n{out}"
    );
    assert!(
        out.contains("lumen_search_latency_seconds_bucket{le=\"+Inf\"} 1"),
        "+Inf bucket must equal the total observation count:\n{out}"
    );
    assert!(
        out.contains("lumen_search_latency_seconds_count 1"),
        "histogram _count must equal the total observation count:\n{out}"
    );
}

/// #2519 AC: an artificially-slow observation (here, `Duration::MAX`
/// against a default threshold) increments `lumen_slow_queries_total`,
/// and it stays folded into the `+Inf` bucket rather than a stored
/// 13th bucket counter.
#[test]
fn observe_search_increments_slow_queries_over_threshold() {
    let m = Metrics::new();
    m.observe_search(Duration::from_secs(10)); // past every finite bucket
    assert_eq!(m.slow_queries_total.get(), 1);
    let out = m.render();
    assert!(out.contains("lumen_slow_queries_total 1"), "{out}");
    assert!(
        out.contains("lumen_search_latency_seconds_bucket{le=\"5\"} 0"),
        "a 10s observation must not land in the 5s bucket:\n{out}"
    );
    assert!(out.contains("lumen_search_latency_seconds_bucket{le=\"+Inf\"} 1"));
}

// Process-global env mutex shared across LUMEN_SLOW_QUERY_MS-mutating
// tests (mirrors `access::application::auth_config`'s `AUTH_ENV_LOCK`).
static SLOW_QUERY_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// #2519 AC: the "threshold=0 trick" — `LUMEN_SLOW_QUERY_MS=0` makes
/// every search, however fast, count as slow. `Metrics::new()` reads
/// the env var once at construction, so it must be set before that
/// call.
#[test]
fn slow_query_threshold_zero_counts_every_search_as_slow() {
    let _g = SLOW_QUERY_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var("LUMEN_SLOW_QUERY_MS", "0");
    }
    let m = Metrics::new();
    unsafe {
        std::env::remove_var("LUMEN_SLOW_QUERY_MS");
    }
    m.observe_search(Duration::from_micros(1));
    assert_eq!(
        m.slow_queries_total.get(),
        1,
        "a threshold=0 override must count even a ~0ms search as slow"
    );
}

/// A fast search under the default 500ms threshold must NOT count as
/// slow (regression guard against an inverted comparison).
#[test]
fn fast_search_under_default_threshold_is_not_slow() {
    let _g = SLOW_QUERY_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::remove_var("LUMEN_SLOW_QUERY_MS");
    }
    let m = Metrics::new();
    m.observe_search(Duration::from_millis(5));
    assert_eq!(m.slow_queries_total.get(), 0);
}
