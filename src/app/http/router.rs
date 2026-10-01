//! HTTP/2 API surface.
//!
//! Reads (`/search`, `/duplicates`, `/stats`) can be served by any
//! replica. Writes (`PUT /collections/...`, `POST .../index`,
//! `DELETE .../index/...`) currently target the local in-memory
//! [`Engine`]; when Raft is wired in they will be forwarded to the
//! shard leader before being applied.
//!
//! The contract for external consumers is `GET /openapi.json`,
//! generated at runtime from `app::http::openapi::ApiDoc`.

use std::sync::Arc;

use axum::extract::Request;
use axum::http::Method;
use axum::middleware::{from_fn, from_fn_with_state, Next};
use axum::routing::{delete, get, post, put};
use axum::Router;
use service_http::{MetricsProvider, ReadinessHook};

use crate::access::application::authorization::AuthContext;
use crate::access::infrastructure::lumen_verifier::LumenVerifier;
use crate::access::interfaces::http::auth_middleware;
use crate::app::http::app_state::AppState;
use crate::app::http::probes::{debug_cluster, version};
use crate::index::application::engine::Engine;
use crate::index::interfaces::http::batch_search::batch_search;
use crate::index::interfaces::http::collections::{
    create_collection, drop_collection, drop_field, list_collections,
};
use crate::index::interfaces::http::duplicates::duplicates;
use crate::index::interfaces::http::query_method::{
    collection_id_query_dispatch, collection_id_query_probe, collections_query_dispatch,
    collections_query_probe,
};
use crate::index::interfaces::http::search::{search, search_all};
use crate::index::interfaces::http::stats::stats;
use crate::ingest::application::write_coordinator::WriteSink;
// The router still serves the deprecated `index` and `delete_external_id`.
#[allow(deprecated)]
use crate::ingest::interfaces::http::delete::{
    delete_doc, delete_external_id, truncate_docs, unindex_docs,
};
#[allow(deprecated)]
use crate::ingest::interfaces::http::index::{index, reindex_stream};
use crate::ingest::interfaces::http::replace::{replace_doc, replace_docs};
use crate::persistence::interfaces::http::backup::{backup, backup_to_local, restore};
use crate::persistence::interfaces::http::checkpoint::{
    admin_checkpoint, admin_restart_seal_hnsw_cache,
};
use crate::sharding::interfaces::http::fence::reshard_fence;
use crate::sharding::interfaces::http::reshard::{
    backup_scoped, reshard_apply, reshard_evict, reshard_prune,
};

/// The `/metrics` body: the engine's domain counters plus the delegated-auth
/// counters.
///
/// They are composed here rather than merged into the engine because they
/// answer different questions and are owned by different layers — and because
/// an operator diagnosing a 503 needs `delegated_auth_unavailable_total` on the
/// same scrape as the request counters, not in a second place they have to know
/// to look. The auth half renders empty when auth is off, so a scrape's shape
/// tells you which mode the process is in.
struct ServingMetrics {
    engine: Arc<Engine>,
    verifier: Arc<LumenVerifier>,
}

impl MetricsProvider for ServingMetrics {
    fn render_metrics(&self) -> String {
        let mut rendered = self.engine.render_metrics();
        rendered.push_str(&self.verifier.render_metrics());
        rendered
    }
}

/// Readiness is false after either graceful drain or an unresolved durable
/// boundary. Liveness remains true so Kubernetes can restart the process
/// without hiding its diagnostic endpoints.
struct ServingReadiness {
    engine: Arc<Engine>,
    writer: Arc<dyn WriteSink>,
}

impl ReadinessHook for ServingReadiness {
    fn is_draining(&self) -> bool {
        self.engine.is_draining() || self.writer.restart_required()
    }
}

impl ReadinessHook for Engine {
    fn is_draining(&self) -> bool {
        Engine::is_draining(self)
    }
}

impl MetricsProvider for Engine {
    fn render_metrics(&self) -> String {
        self.metrics().render()
    }
}

/// Middleware that records the authenticated subject to the request span.
/// This is used by the access log to include subject information in per-request logs.
/// Called after auth_middleware, so AuthContext is already in the extensions.
async fn record_subject_to_span(req: Request, next: Next) -> axum::response::Response {
    // Record subject to the current span for inclusion in access logs.
    // If AuthContext exists in extensions, use its subject; otherwise use "anonymous".
    let subject = req
        .extensions()
        .get::<AuthContext>()
        .and_then(|auth| auth.subject())
        .unwrap_or("anonymous");
    tracing::Span::current().record("subject", subject);
    next.run(req).await
}

pub fn router(state: AppState) -> Router {
    router_with_admission(state, None)
}

/// Build the Lumen router with optional shared request admission. Lumen owns
/// the route-class mapping and policy values; `service-http` owns enforcement.
/// The established [`router`] entry point passes `None`, so admission remains
/// disabled unless a serving adapter explicitly supplies policies.
///
/// Deprecated handlers remain registered as compatibility routes. Their Rust
/// deprecation attributes generate the OpenAPI deprecation marker.
#[allow(deprecated)]
pub fn router_with_admission(
    state: AppState,
    admission: Option<service_http::AdmissionController>,
) -> Router {
    // Apply auth middleware only to data-plane routes. Admin/Probe
    // endpoints (`/healthz`, `/readyz`, `/metrics`, `/openapi.json`,
    // `/docs`) stay open so K8s probes and Prometheus scrape can hit
    // them without a token even when auth is required.
    let auth_state = state.verifier();
    let data_plane = Router::new()
        .route(
            "/collections",
            get(list_collections)
                .options(collections_query_probe)
                .head(collections_query_probe)
                // Epic #1296 R1: `QUERY /collections` is a dual-registered
                // twin of `POST /collections:search` (#1271 batch search).
                // Axum has no native `Method::QUERY` support yet
                // (tokio-rs/axum#3799, PR #3801 open), so this is the interim
                // dispatch — `fallback` runs for any method not explicitly
                // registered above (`GET`, `OPTIONS`, `HEAD`), and the
                // handler re-checks by hand. Replace with a native
                // `MethodFilter::QUERY` combinator once that PR lands.
                .fallback(collections_query_dispatch),
        )
        .route(
            "/collections/{collection_id}",
            put(create_collection)
                .delete(drop_collection)
                .options(collection_id_query_probe)
                .head(collection_id_query_probe)
                // Epic #1296 R1: `QUERY /collections/{collection_id}` is a
                // dual-registered twin of `POST
                // /collections/{collection_id}/search`. See the
                // `/collections` route above for the interim-fallback
                // rationale.
                .fallback(collection_id_query_dispatch),
        )
        .route("/collections/{collection_id}/index", post(index))
        .route(
            "/collections/{collection_id}/index/{external_id}",
            delete(delete_external_id),
        )
        .route(
            "/collections/{collection_id}/docs:replace",
            put(replace_docs),
        )
        .route(
            "/collections/{collection_id}/docs/{external_id}",
            put(replace_doc).delete(delete_doc),
        )
        .route(
            "/collections/{collection_id}/docs:truncate",
            post(truncate_docs),
        )
        .route(
            "/collections/{collection_id}/docs:unindex",
            post(unindex_docs),
        )
        .route("/collections/{collection_id}/search", post(search))
        .route("/collections/{collection_id}/search:all", post(search_all))
        .route("/collections:search", post(batch_search))
        .route("/collections/{collection_id}/duplicates", post(duplicates))
        .route("/collections/{collection_id}/stats", get(stats))
        .route(
            "/collections/{collection_id}/fields/{field_name}",
            delete(drop_field),
        )
        .route(
            "/collections/{collection_id}/reindex/stream",
            post(reindex_stream),
        )
        .route("/admin/backup", get(backup))
        .route("/admin/backup/local", post(backup_to_local))
        .route("/admin/backup:scoped", post(backup_scoped))
        .route("/admin/restore", post(restore))
        .route("/admin/reshard:apply", post(reshard_apply))
        .route("/admin/reshard:prune", post(reshard_prune))
        .route("/admin/reshard:evict", post(reshard_evict))
        .route("/admin/reshard:fence", post(reshard_fence))
        .route("/admin/checkpoint", post(admin_checkpoint))
        .route(
            "/admin/restart:seal-hnsw-cache",
            post(admin_restart_seal_hnsw_cache),
        )
        .layer(from_fn(record_subject_to_span))
        .layer(from_fn_with_state(auth_state, auth_middleware))
        // Bound request bodies: a bulk index is ~MBs (the item cap is the real
        // guard); 8MiB is the broker payload budget. Enforces the cap at the HTTP
        // layer with a structured 413 envelope and streams/chunked bodies bounded
        // mid-read, disabling axum's extractor-side default so this layer governs.
        // Shared with `crate::sharding::domain::reshard_batch::ADMIN_ROUTE_BODY_LIMIT_BYTES` (#1444 R2)
        // so the reshard driver's oversize-batch detection can never drift from the
        // limit actually enforced here. Probe routes (/healthz, /readyz, /metrics,
        // /version, /docs) are unaffected as they are merged separately and stay
        // unbounded.
        .layer(service_http::body_limit_layer(
            crate::sharding::infrastructure::body_limit::body_limit_bytes_from_env(),
        ))
        .layer(axum::extract::DefaultBodyLimit::disable());
    let data_plane = match admission {
        Some(controller) => data_plane.route_layer(from_fn_with_state(
            service_http::AdmissionMiddleware::new(controller, |request| {
                let path = request.uri().path();
                let class = if path.starts_with("/admin/") {
                    "lumen.admin"
                } else if matches!(*request.method(), Method::GET | Method::HEAD)
                    || path.contains("/search")
                    || path.ends_with("/duplicates")
                    || path.ends_with("/stats")
                {
                    "lumen.read"
                } else {
                    "lumen.write"
                };
                let key = request
                    .headers()
                    .get(axum::http::header::AUTHORIZATION)
                    .map(|value| value.as_bytes())
                    .unwrap_or(b"anonymous");
                Some(service_http::AdmissionInput::new(class, key))
            }),
            service_http::admission_middleware,
        )),
        None => data_plane,
    };

    let metrics: Arc<dyn MetricsProvider> = Arc::new(ServingMetrics {
        engine: state.engine.clone(),
        verifier: state.verifier.clone(),
    });
    let readiness = Arc::new(ServingReadiness {
        engine: state.engine.clone(),
        writer: state.writer.clone(),
    });
    let probes = service_http::standard_probe_routes_canonical_json(
        readiness,
        Some(metrics),
        crate::app::spec::openapi_json,
    );
    let admin = Router::new()
        .route("/version", get(version))
        .route("/debug/cluster", get(debug_cluster));

    probes
        .merge(admin.with_state(state.clone()))
        .merge(data_plane.with_state(state))
        // One tracing span per HTTP request — structured request logs always, and
        // the source spans the OTLP layer exports as traces when LUMEN_OTLP_ENDPOINT
        // is set. INFO level so the default `info` EnvFilter keeps it.
        .layer(service_http::trace_layer())
        // Per-request Server-Timing response attribution, composed at the
        // same outermost position as trace_layer() above (#2490).
        .layer(axum::middleware::from_fn(
            service_http::server_timing_middleware,
        ))
}
