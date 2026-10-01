//! A serving node's tracing subscriber and its optional OTLP meter.

use anyhow::Result;

use crate::cli::serve::LogFormat;

#[cfg(feature = "otel")]
use std::sync::Arc;
#[cfg(feature = "otel")]
use std::time::Duration;

#[cfg(feature = "otel")]
use lumen::storage::Engine;

pub(super) fn init_tracing(
    level: &str,
    format: LogFormat,
    otlp_endpoint: Option<&str>,
) -> Result<()> {
    let format = match format {
        LogFormat::Pretty => service_http::LogFormat::Pretty,
        LogFormat::Json => service_http::LogFormat::Json,
    };
    let config = service_http::HttpConfig::new(
        "127.0.0.1",
        0,
        format!("info,lumen={level}"),
        format,
        0,
        0,
        otlp_endpoint.map(str::to_owned),
    );
    let identity = service_http::ServiceIdentity::new("lumen", env!("CARGO_PKG_VERSION"))?;
    service_http::init_tracing_with_identity(&config, &identity)
}

/// Build + install a global OTLP (tonic) meter provider that PUSHES lumen's
/// counters to `endpoint` every 60s. The observable instruments read the
/// engine's existing atomic counters, so the OTLP push and the `/metrics` pull
/// share one source of truth (no double counting). This is what lets a fleet of
/// stateless replicas report without anyone scraping each pod — the collector
/// aggregates and Prometheus scrapes only the collector.
#[cfg(feature = "otel")]
pub(super) fn init_otel_meter(
    endpoint: &str,
    engine: Arc<Engine>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_otlp::WithExportConfig;
    use std::sync::atomic::Ordering;

    let provider = opentelemetry_otlp::new_pipeline()
        .metrics(opentelemetry_sdk::runtime::Tokio)
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint.to_string()),
        )
        .with_resource(opentelemetry_sdk::Resource::new(vec![
            opentelemetry::KeyValue::new("service.name", "lumen"),
            opentelemetry::KeyValue::new("service.version", env!("CARGO_PKG_VERSION").to_string()),
        ]))
        .with_period(Duration::from_secs(60))
        .build()?;

    let meter = provider.meter("lumen");

    // Each atomic counter → an observable instrument whose callback reads the
    // live value at every collection interval. Closures own an Arc<Engine>.
    macro_rules! obs_counter {
        ($name:literal, $field:ident, $desc:literal) => {{
            let eng = engine.clone();
            let _ = meter
                .u64_observable_counter($name)
                .with_description($desc)
                .with_callback(move |o| {
                    o.observe(eng.metrics().$field.load(Ordering::Relaxed), &[])
                })
                .init();
        }};
    }
    obs_counter!(
        "lumen_index_writes_total",
        index_writes_total,
        "Total index writes"
    );
    obs_counter!(
        "lumen_index_bytes_total",
        index_bytes_total,
        "Total bytes indexed"
    );
    obs_counter!(
        "lumen_search_requests_total",
        search_requests_total,
        "Total search requests"
    );
    obs_counter!(
        "lumen_search_latency_ms_sum",
        search_latency_ms_sum,
        "Search latency ms sum"
    );
    obs_counter!(
        "lumen_search_latency_ms_count",
        search_latency_ms_count,
        "Search latency count"
    );
    obs_counter!(
        "lumen_duplicates_requests_total",
        duplicates_requests_total,
        "Total duplicates requests"
    );
    obs_counter!(
        "lumen_collections_created_total",
        collections_created_total,
        "Total collections created"
    );
    obs_counter!(
        "lumen_schema_fields_total",
        schema_fields_total,
        "Total schema fields"
    );
    obs_counter!(
        "lumen_posting_cache_hits_total",
        posting_cache_hits_total,
        "Posting cache hits"
    );
    obs_counter!(
        "lumen_posting_cache_misses_total",
        posting_cache_misses_total,
        "Posting cache misses"
    );

    // storage_bytes is a gauge (can decrease).
    {
        let eng = engine.clone();
        let _ = meter
            .u64_observable_gauge("lumen_storage_bytes")
            .with_description("Current storage bytes")
            .with_callback(move |o| {
                o.observe(eng.metrics().storage_bytes.load(Ordering::Relaxed), &[])
            })
            .init();
    }

    opentelemetry::global::set_meter_provider(provider);
    Ok(())
}
