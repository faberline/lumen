// CODEGEN-BEGIN
//! HTTP/2 API surface.
//!
//! Reads (`/search`, `/duplicates`, `/stats`) can be served by any
//! replica. Writes (`PUT /collections/...`, `POST .../index`,
//! `DELETE .../index/...`) currently target the local in-memory
//! [`Engine`]; when Raft is wired in they will be forwarded to the
//! shard leader before being applied.
//!
//! The contract for external consumers is `GET /openapi.json`,
//! generated at runtime from this module.

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::from_fn_with_state,
    response::{IntoResponse, Json},
    routing::{delete, get, post, put},
    Router,
};
use service_http::{MetricsProvider, ReadinessHook};
use utoipa::{
    openapi::{
        self,
        security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
    },
    Modify, OpenApi,
};

use axum::http::HeaderMap;
use axum::middleware::{from_fn, Next};

use crate::access::{
    application::{auth_config::AuthConfig, authorization::AuthContext},
    infrastructure::lumen_verifier::LumenVerifier,
    interfaces::http::auth_middleware,
};
use crate::index::application::engine::Engine;
use crate::index::application::ports::search_backend::LocalEngineSearch;
pub use crate::index::application::ports::search_backend::SearchBackend;
use crate::index::domain::storage_error::StorageError;
use crate::index::infrastructure::search_executor::BlockingSearchExecutor;
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
use crate::ingest::application::ports::write_backend::LocalWriteBackend;
pub use crate::ingest::application::ports::write_backend::WriteBackend;
use crate::ingest::application::write_coordinator::{
    errors::{RestartRequired, StorageFullError, SubmitStalled},
    WriteCoordinator, WriteSink,
};
use crate::ingest::domain::change_admission::PendingChangeCapacity;
use crate::ingest::domain::wal_log::SharedWal;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
// The router still serves the deprecated `index` and `delete_external_id`.
#[allow(deprecated)]
use crate::ingest::interfaces::http::delete::{
    delete_doc, delete_external_id, truncate_docs, unindex_docs,
};
#[allow(deprecated)]
use crate::ingest::interfaces::http::index::{index, reindex_stream};
use crate::ingest::interfaces::http::replace::{replace_doc, replace_docs};
use crate::persistence::application::ports::checkpoint_sink::NoopCheckpoint;
pub use crate::persistence::application::ports::checkpoint_sink::{
    CheckpointSink, HnswCacheDurability, HnswCacheSealReceipt,
};
use crate::persistence::application::ports::restore_sink::InMemoryRestoreSink;
pub use crate::persistence::application::ports::restore_sink::RestoreSink;
use crate::persistence::application::restore::{RestoreNotCommitted, RestoreUnavailable};
use crate::persistence::interfaces::http::backup::{backup, backup_to_local, restore};
use crate::persistence::interfaces::http::checkpoint::{
    admin_checkpoint, admin_restart_seal_hnsw_cache, HnswCacheSealMutationStamp,
    HnswCacheSealResponse,
};
use crate::replication::domain::{
    cluster_state::ReadConsistency, cluster_state_view::ClusterStateView, raft_role::RaftRole,
};
pub use crate::sharding::application::ports::routed_backend::RoutedBackend;
pub use crate::sharding::domain::forward_error::{
    ShardForwardMisrouted, ShardForwardRemoteError, ShardForwardUnavailable,
    ShardMapVersionMismatch,
};
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;
use crate::sharding::interfaces::http::fence::reshard_fence;
use crate::sharding::interfaces::http::reshard::{
    backup_scoped, reshard_apply, reshard_evict, reshard_prune,
};
use crate::shared_kernel::types::{
    api_error::ApiError,
    document::{
        BatchUnindexDocsRequest, FieldValue, IndexItem, IndexRequest, IndexResponse,
        ReplaceDocBody, ReplaceDocItem, ReplaceDocResult, ReplaceDocsRequest, ReplaceDocsResponse,
    },
    query::{KnnQuery, MatchOp, MatchQuery, QueryNode, RangeQuery, TermQuery, TermsQuery},
    schema::{
        Analyzer, CreateCollectionRequest, CreateCollectionResponse, FieldSpec, FieldType,
        VectorBackend, VectorMetric, VectorQuantize, VectorSpec,
    },
    search::{
        BatchSearchRequest, BatchSearchResponse, BatchSearchResult, DuplicateGroup,
        DuplicatesRequest, DuplicatesResponse, SearchAllRequest, SearchAllResponse, SearchHit,
        SearchRequest, SearchResponse,
    },
    stats::{CacheStats, FieldStats, StatsResponse, StorageStats},
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

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<Engine>,
    pub auth: Arc<AuthConfig>,
    verifier: Arc<LumenVerifier>,
    pub cluster: Option<Arc<crate::replication::domain::cluster_state::ClusterState>>,
    /// Read/search backend. Defaults to the local engine; sharded serving can
    /// replace it with a fan-in router while keeping writes/stats local.
    pub search_backend: Arc<dyn SearchBackend>,
    /// Shared bounded bridge for every local synchronous HTTP search path.
    pub(crate) search_executor: BlockingSearchExecutor,
    /// Writes go through a [`WriteSink`]: the WAL-seam coordinator for
    /// embedded, or the raft host for `--wal raft`. Reads use
    /// `engine` directly. See `coordinator` / `wal` / `raft_sm`.
    pub writer: Arc<dyn WriteSink>,
    /// Write/mutation backend. Defaults to the local coordinator; sharded
    /// serving can replace it with a document-router that fans out writes
    /// across independent shard coordinators.
    pub write_backend: Arc<dyn WriteBackend>,
    /// Durability-on-demand seam for `POST /admin/checkpoint` (#1389).
    /// Defaults to [`NoopCheckpoint`]; the server binary wires a real
    /// segment-checkpoint implementation when segment persistence is
    /// configured. See [`CheckpointSink`].
    pub checkpoint: Arc<dyn CheckpointSink>,
    /// Restore backend for `POST /admin/restore`. Defaults to an in-memory
    /// candidate-and-swap sink; the server binary may wire a durable variant.
    pub(crate) restore_sink: Arc<dyn RestoreSink>,
    /// Bounded write pause on still-moving virtual buckets during a
    /// reshard's final `CatchingUp` pass (#1396 R2). Defaults to unarmed
    /// (every write passes through unchanged); the reshard driver arms it
    /// via `POST /admin/reshard:fence`. See [`WriteFence`].
    pub write_fence: WriteFence,
    /// Cross-pod shard router for operator/k8s serving (#1398 R1-R3).
    /// `None` for every deployment shape except the routed one (`SHARD_COUNT`
    /// > 1, `replicasPerShard <= 1`, no `--search-shard-segment-dirs`) — see
    /// [`RoutedBackend`]. The server binary wires a real
    /// `routing_remote::RoutedRouter` via [`Self::with_routed`]; tests and
    /// every other deployment shape leave this `None`.
    pub routed: Option<Arc<dyn RoutedBackend>>,
}

/// A bounded, status-visible write pause on a set of still-moving virtual
/// buckets (#1396 R2), the mechanism #1381's R5 review sanctioned: "a
/// bounded final pause of writes to still-moving buckets is acceptable if
/// needed for convergence, but must be bounded and reported in status."
///
/// Closes the copy-to-evict gap in the reshard driver's `CatchingUp` pass:
/// without a pause, a write landing on a source shard's moving bucket after
/// the last migration-copy read but before that bucket's eviction is never
/// re-copied to the target and is silently dropped by eviction. The driver
/// arms a fence over exactly the buckets its final migration pass is about
/// to copy (`POST /admin/reshard:fence`) immediately before that pass, so
/// the single pass taken under the fence is already a complete/converged
/// snapshot of those buckets — no repeat-until-converged loop is needed —
/// and clears the fence (an empty-`buckets` call to the same verb) on every
/// exit path of that tick, success or `Blocked`.
///
/// Crash safety: a driver process that dies between arming and clearing
/// cannot leave a bucket permanently unwritable. [`WriteFence::blocks`]
/// checks `deadline` on every call and treats an expired fence as unarmed —
/// this check runs on the *serving pod*, independent of whether the driver
/// process that armed it is still alive, so expiry is enforced even if the
/// driver never comes back. The reshard driver re-arms a fresh deadline
/// every tick it needs one, so a healthy, slow-but-progressing driver never
/// races its own TTL; see `reshard_driver::WRITE_FENCE_TTL`.
#[derive(Clone, Default)]
pub struct WriteFence {
    state: Arc<Mutex<Option<FenceState>>>,
}

struct FenceState {
    virtual_bucket_count: u32,
    buckets: BTreeSet<u32>,
    deadline: Instant,
}

impl WriteFence {
    /// Arm the fence over `buckets` (computed against `virtual_bucket_count`)
    /// until `ttl` from now, replacing any prior armed state. Returns `false`
    /// (leaving any prior armed state untouched) when `Instant::now() + ttl`
    /// would overflow (#1443 R3) — the caller must treat that as a failed arm
    /// rather than silently panicking with the fence lock held, which would
    /// poison it for every subsequent write/clear on this pod.
    pub(crate) fn arm(
        &self,
        virtual_bucket_count: u32,
        buckets: BTreeSet<u32>,
        ttl: Duration,
    ) -> bool {
        let Some(deadline) = Instant::now().checked_add(ttl) else {
            return false;
        };
        let mut guard = self.lock();
        *guard = Some(FenceState {
            virtual_bucket_count,
            buckets,
            deadline,
        });
        true
    }

    /// Explicitly disarm, independent of `deadline`.
    pub(crate) fn clear(&self) {
        *self.lock() = None;
    }

    /// `Some(bucket)` when `collection_id`/`external_id` route to a
    /// currently-fenced bucket; `None` (unblocked) once armed but past
    /// `deadline`, or never armed at all.
    fn blocks(&self, collection_id: &str, external_id: &str) -> Option<u32> {
        let guard = self.lock();
        let fence = guard.as_ref()?;
        if Instant::now() >= fence.deadline {
            return None;
        }
        let map = VirtualBucketShardMap::balanced(0, fence.virtual_bucket_count, 1).ok()?;
        let bucket = map.route_document(collection_id, None, external_id).bucket;
        fence.buckets.contains(&bucket).then_some(bucket)
    }

    /// A collection-wide mutation has no document id from which to derive one
    /// bucket.  During a reshard cutover it must therefore wait for every
    /// active bucket fence, rather than slipping through a fenced subset.
    fn blocks_any(&self) -> bool {
        let guard = self.lock();
        guard
            .as_ref()
            .is_some_and(|fence| Instant::now() < fence.deadline && !fence.buckets.is_empty())
    }

    /// Poison-proof lock acquisition (#1443 R3), matching `segment_rdb.rs`'s
    /// `save_lock` precedent: a panic anywhere else in the process while
    /// holding this lock must never turn into a permanent write outage on
    /// this pod by propagating a poisoned-mutex panic into every later
    /// `arm`/`clear`/`blocks` call.
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<FenceState>> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl AppState {
    /// Build state with an explicit write log. Spawns the apply loop.
    pub fn with_wal(engine: Arc<Engine>, auth: Arc<AuthConfig>, wal: SharedWal) -> Self {
        let writer = WriteCoordinator::start(wal, engine.clone());
        Self::with_components(engine, auth, writer)
    }

    /// Build state from an already-constructed coordinator — used by the
    /// server binary, which wires the WAL + RDB bootstrap itself and
    /// hands in the resulting coordinator.
    pub fn with_components(
        engine: Arc<Engine>,
        auth: Arc<AuthConfig>,
        writer: Arc<dyn WriteSink>,
    ) -> Self {
        Self {
            search_backend: Arc::new(LocalEngineSearch {
                engine: engine.clone(),
            }),
            search_executor: BlockingSearchExecutor::new(),
            write_backend: Arc::new(LocalWriteBackend {
                writer: writer.clone(),
            }),
            engine: engine.clone(),
            verifier: Arc::new(LumenVerifier::new(auth.clone())),
            auth,
            cluster: None,
            writer: writer.clone(),
            checkpoint: Arc::new(NoopCheckpoint),
            restore_sink: Arc::new(InMemoryRestoreSink::new(
                engine.clone(),
                writer.mutation_gate(),
            )),
            write_fence: WriteFence::default(),
            routed: None,
        }
    }

    /// Build state with an in-process [`MemWal`] — single-node /
    /// dev / tests. Writes feel synchronous.
    pub fn new(engine: Arc<Engine>, auth: Arc<AuthConfig>) -> Self {
        Self::with_wal(engine, auth, Arc::new(MemWal::new()))
    }

    pub fn with_cluster(
        mut self,
        cluster: Arc<crate::replication::domain::cluster_state::ClusterState>,
    ) -> Self {
        self.cluster = Some(cluster);
        self
    }

    pub fn with_search_backend(mut self, search_backend: Arc<dyn SearchBackend>) -> Self {
        self.search_backend = search_backend;
        self
    }

    pub fn with_write_backend(mut self, write_backend: Arc<dyn WriteBackend>) -> Self {
        self.write_backend = write_backend;
        self
    }

    /// Wire a real [`CheckpointSink`] (#1389) — used by the server binary
    /// when segment persistence is configured, and by tests that need to
    /// control/observe `POST /admin/checkpoint` behavior.
    pub fn with_checkpoint(mut self, checkpoint: Arc<dyn CheckpointSink>) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    pub fn with_restore_sink(mut self, restore_sink: Arc<dyn RestoreSink>) -> Self {
        self.restore_sink = restore_sink;
        self
    }

    /// Wire a [`RoutedBackend`] (#1398) — the server binary calls this only
    /// in the routed serving topology (`SHARD_COUNT` env > 1 at
    /// `replicasPerShard <= 1`, no `--search-shard-segment-dirs`); every
    /// other deployment shape leaves `routed` at its `None` default.
    pub fn with_routed(mut self, routed: Arc<dyn RoutedBackend>) -> Self {
        self.routed = Some(routed);
        self
    }

    /// The exact auth verifier used by every router built from this state.
    pub fn verifier(&self) -> Arc<LumenVerifier> {
        Arc::clone(&self.verifier)
    }

    /// Install a verifier the caller built itself.
    ///
    /// [`with_components`](Self::with_components) can only build the two
    /// verifiers that need nothing: open, and required-but-unwired. A delegated
    /// verifier has to reach kube-apiserver and prove its delegation grant
    /// before it exists, which is async and can fail — so the serving binary
    /// builds it and hands it in here (#2869).
    pub fn with_verifier(mut self, verifier: Arc<LumenVerifier>) -> Self {
        self.verifier = verifier;
        self
    }

    /// No-auth state over an in-process log. Used by tests and the
    /// simplest single-node runs.
    pub fn open(engine: Arc<Engine>) -> Self {
        Self::with_wal(
            engine,
            Arc::new(AuthConfig::open()),
            Arc::new(MemWal::new()),
        )
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "lumen",
        description = "Standalone search and duplicate-detection index. Generic Collection / Field primitive; the caller owns the source of truth.",
        license(name = "MIT")
    ),
    servers(
        // Production is private ClusterIP TLS the serving pod terminates
        // itself, so the in-cluster server is `https` and its host is the
        // Service DNS name the leaf asserts (#3113). A generated client that
        // took `http://` from here would send its bearer token in the clear
        // against a port that no longer answers plaintext.
        (
            url = "https://{instance}.{namespace}.svc:7373",
            description = "in-cluster ClusterIP, TLS terminated by lumen",
            variables(
                ("instance" = (default = "lumen", description = "Lumen CR / Service name")),
                ("namespace" = (default = "default", description = "namespace that owns the instance"))
            )
        ),
        (url = "http://localhost:7373", description = "local dev (h2c, auth disabled)")
    ),
    tags(
        (name = "Collections", description = "Schema lifecycle"),
        (name = "Index",       description = "Document writes & deletes"),
        (name = "Query",       description = "Search & duplicate detection"),
        (name = "Admin",       description = "Health, stats, OpenAPI")
    ),
    paths(
        healthz,
        readyz,
        version,
        metrics,
        debug_cluster,
        crate::index::interfaces::http::collections::list_collections,
        crate::index::interfaces::http::collections::create_collection,
        crate::index::interfaces::http::collections::drop_collection,
        crate::index::interfaces::http::collections::drop_field,
        crate::ingest::interfaces::http::index::index,
        crate::ingest::interfaces::http::delete::delete_external_id,
        crate::ingest::interfaces::http::replace::replace_docs,
        crate::ingest::interfaces::http::replace::replace_doc,
        crate::ingest::interfaces::http::delete::delete_doc,
        crate::ingest::interfaces::http::delete::truncate_docs,
        crate::ingest::interfaces::http::delete::unindex_docs,
        crate::ingest::interfaces::http::index::reindex_stream,
        crate::index::interfaces::http::search::search,
        crate::index::interfaces::http::search::search_all,
        crate::index::interfaces::http::batch_search::batch_search,
        crate::index::interfaces::http::duplicates::duplicates,
        crate::index::interfaces::http::stats::stats,
        crate::persistence::interfaces::http::backup::backup,
        crate::persistence::interfaces::http::backup::backup_to_local,
        crate::persistence::interfaces::http::backup::restore,
        crate::sharding::interfaces::http::reshard::backup_scoped,
        crate::sharding::interfaces::http::reshard::reshard_apply,
        crate::sharding::interfaces::http::reshard::reshard_prune,
        crate::sharding::interfaces::http::reshard::reshard_evict,
        crate::sharding::interfaces::http::fence::reshard_fence,
        crate::persistence::interfaces::http::checkpoint::admin_checkpoint,
        crate::persistence::interfaces::http::checkpoint::admin_restart_seal_hnsw_cache,
    ),
    components(schemas(
        CreateCollectionRequest,
        CreateCollectionResponse,
        FieldSpec,
        FieldType,
        Analyzer,
        VectorSpec,
        VectorMetric,
        VectorBackend,
        VectorQuantize,
        IndexRequest,
        IndexItem,
        FieldValue,
        IndexResponse,
        ReplaceDocsRequest,
        ReplaceDocItem,
        ReplaceDocsResponse,
        ReplaceDocResult,
        ReplaceDocBody,
        BatchUnindexDocsRequest,
        SearchRequest,
        QueryNode,
        MatchQuery,
        MatchOp,
        TermQuery,
        TermsQuery,
        crate::shared_kernel::types::query::PrefixQuery,
        RangeQuery,
        // #1307: $ref'd by RangeQuery's gt/gte/lt/lte bounds (untagged f64 | String) —
        // same dangling-ref reason as the #200 note below, registered explicitly.
        crate::shared_kernel::types::query::RangeBound,
        KnnQuery,
        crate::shared_kernel::types::query::RrfQuery,
        crate::shared_kernel::types::query::ExistsQuery,
        crate::shared_kernel::types::query::DuplicatedQuery,
        // #200: these are $ref'd by QueryNode / SearchRequest but were not
        // registered, so the emitted OpenAPI had dangling refs. SortSpec also
        // pulls in SortOrder + SortMissing.
        crate::shared_kernel::types::query::IdsQuery,
        crate::shared_kernel::types::query::HasChildQuery,
        crate::shared_kernel::types::query::HammingQuery,
        crate::shared_kernel::types::query::SortSpec,
        crate::shared_kernel::types::query::SortOrder,
        crate::shared_kernel::types::query::SortMissing,
        SearchHit,
        SearchResponse,
        SearchAllRequest,
        SearchAllResponse,
        BatchSearchRequest,
        crate::shared_kernel::types::search::BatchSearchItem,
        BatchSearchResponse,
        BatchSearchResult,
        DuplicatesRequest,
        DuplicateGroup,
        DuplicatesResponse,
        StatsResponse,
        FieldStats,
        StorageStats,
        CacheStats,
        ApiError,
        crate::replication::domain::cluster_state_view::ClusterStateView,
        crate::replication::domain::peer_addr::PeerAddr,
        crate::replication::domain::raft_role::RaftRole,
        HnswCacheSealMutationStamp,
        HnswCacheSealResponse,
    )),
    modifiers(&SecurityAddon),
    security(("bearerAuth" = []))
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearerAuth",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .bearer_format("opaque")
                        .description(Some(
                            "A short-lived Kubernetes ServiceAccount bearer token, verified by \
                             TokenReview for caller identity and SubjectAccessReview for each \
                             operation. Managed `LUMEN_AUTH=required` keeps the \
                             `lumen.axiom.dev` audience and private TLS contract. Standalone \
                             `LUMEN_AUTH=in-cluster` accepts the Kubernetes default \
                             ServiceAccount token only on its private ClusterIP Service; the \
                             generated clients attach that token only to an exact \
                             `*.svc.cluster.local` URL. `LUMEN_AUTH=off` ignores the header. A \
                             Google access token, ID token, ADC credential, or metadata-server \
                             token is never accepted. Neither profile exposes Ingress, Gateway, \
                             LoadBalancer, or NodePort.",
                        ))
                        .build(),
                ),
            );
        }
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
        crate::spec::openapi_json,
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

#[utoipa::path(
    get,
    path = "/metrics",
    tag = "Admin",
    security(()),
    responses((status = 200, description = "Prometheus text-format metrics", body = String))
)]
/// OpenAPI metadata for the shared `/metrics` implementation in service-http.
#[allow(dead_code)]
async fn metrics(
    State(state): State<AppState>,
) -> (StatusCode, [(&'static str, &'static str); 1], String) {
    let body = state.engine.metrics().render();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

#[utoipa::path(
    get,
    path = "/debug/cluster",
    tag = "Admin",
    security(()),
    responses((status = 200, description = "Cluster state snapshot", body = ClusterStateView))
)]
async fn debug_cluster(State(state): State<AppState>) -> Json<ClusterStateView> {
    let view = match state.cluster.as_ref() {
        Some(c) => c.snapshot(),
        None => ClusterStateView {
            pod_name: "local".into(),
            shard_index: 0,
            replica_index: 0,
            role: crate::replication::domain::raft_role::RaftRole::Leader,
            peers: vec![],
            applied_index: 0,
            leader_term: 0,
            replication_lag_ms: 0,
        },
    };
    Json(view)
}

pub(crate) fn read_consistency_from(headers: &HeaderMap) -> ReadConsistency {
    ReadConsistency::from_header(
        headers
            .get("x-read-consistency")
            .and_then(|h| h.to_str().ok()),
    )
}

/// Enforces a resolved `x-read-consistency` against this pod's live
/// per-shard cluster state (`AppState::cluster`) before a read reaches the
/// local engine (#1310).
///
/// Standalone and legacy external-log builds (`state.cluster` is `None`)
/// have exactly one authoritative copy per shard, so every consistency
/// level is trivially satisfied there — this is a no-op, matching today's
/// behavior unchanged. Primary-replica mode (`state.cluster` is `Some`) is
/// the only place a request's resolved [`ReadConsistency`] can actually
/// diverge from what gets served:
/// - [`ReadConsistency::Any`] is unconstrained.
/// - [`ReadConsistency::Leader`] only succeeds on the pod that currently
///   holds `RaftRole::Leader` for this shard; lumen has no read-forwarding
///   surface, so a non-leader replica rejects the request rather than
///   silently serving a possibly-stale local copy.
/// - [`ReadConsistency::Bounded`] succeeds on the leader (never stale) or
///   on a follower/learner whose `replication_lag_ms` is at or under the
///   requested bound; a replica over the bound rejects rather than
///   silently serving a stale read. In `lumen serve --wal raft`, a
///   follower/learner's `replication_lag_ms` is the conservative "unknown"
///   sentinel (`u64::MAX`) — `RaftHost` doesn't expose a peer-timing RPC
///   today, so `Bounded` on a non-leader replica always rejects rather than
///   report a fabricated lag figure (see `spawn_cluster_state_poller` in
///   `src/bin/lumen.rs`, #1349).
pub(crate) fn enforce_read_consistency(
    state: &AppState,
    consistency: ReadConsistency,
) -> Result<(), ApiErr> {
    let Some(cluster) = state.cluster.as_ref() else {
        return Ok(());
    };
    match consistency {
        ReadConsistency::Any => Ok(()),
        ReadConsistency::Leader => {
            if cluster.role() == RaftRole::Leader {
                return Ok(());
            }
            Err(match cluster.leader_peer() {
                Some(leader) => ApiErr::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "read_consistency_not_leader",
                    format!(
                        "replica `{}` is not the shard {} leader (current leader is `{}`); \
                         leader-consistency reads must reach it",
                        cluster.pod_name, cluster.shard_index, leader.pod_name
                    ),
                ),
                None => ApiErr::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "read_consistency_no_leader",
                    format!(
                        "shard {} has no reachable leader; leader-consistency reads cannot be satisfied",
                        cluster.shard_index
                    ),
                ),
            })
        }
        ReadConsistency::Bounded(bound_ms) => {
            if cluster.role() == RaftRole::Leader {
                return Ok(());
            }
            let lag_ms = cluster.replication_lag_ms.load(Ordering::Relaxed);
            if lag_ms <= bound_ms {
                Ok(())
            } else {
                Err(ApiErr::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "read_consistency_lag_exceeded",
                    format!(
                        "replica `{}` lag {lag_ms}ms exceeds bounded({bound_ms}ms) consistency",
                        cluster.pod_name
                    ),
                ))
            }
        }
    }
}

/// Reject a write whose `(collection_id, external_id)` routes to a
/// currently-fenced virtual bucket (#1396 R2). Checked in [`index`],
/// [`replace_docs`], [`replace_doc`], and [`delete_external_id`] — every
/// write path a reshard's final migration pass must observe a converged
/// snapshot of.
///
/// `delete_external_id` is fenced too (#1458 R2): an earlier revision left
/// DELETE exempt on the theory that `apply_reshard_batch`'s
/// authoritative-subset `replace_ids` scoping (see
/// [`crate::sharding::domain::reshard_batch::snapshot_reshard_batches`]'s `replace_mode` and
/// [`crate::index::application::engine::Engine::apply_reshard_batch`]'s `replace` parameter)
/// already closes the resurrection gap for a delete acked *before* the
/// final pass's scoped-backup read. That leaves a delete racing strictly
/// inside the sub-window between that read and the same pass's eviction
/// uncovered — fencing DELETE like every other write closes it fully, at
/// the ordinary cost (a retryable 503) of any write to a fenced bucket. See
/// the module's #1396 R2 write-fence doc on [`WriteFence`].
pub(crate) fn enforce_write_fence(
    state: &AppState,
    collection_id: &str,
    external_id: &str,
) -> Result<(), ApiErr> {
    if let Some(bucket) = state.write_fence.blocks(collection_id, external_id) {
        return Err(ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "bucket_write_paused",
            format!(
                "virtual bucket {bucket} is paused for an in-progress reshard cutover; retry shortly"
            ),
        ));
    }
    Ok(())
}

pub(crate) fn enforce_collection_write_fence(state: &AppState) -> Result<(), ApiErr> {
    if state.write_fence.blocks_any() {
        return Err(ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "bucket_write_paused",
            "a collection-wide write is paused for an in-progress reshard cutover; retry shortly",
        ));
    }
    Ok(())
}

/// #2516: sticky ENOSPC degraded read-only mode. Called first (before any
/// other check) by every mutating admin/data-plane handler — `index`,
/// `docs:replace` (both `replace_docs` and `replace_doc`), delete,
/// create/drop collection, and admin restore — so a node that has already
/// taken a genuine ENOSPC hit on its durable write path (see
/// `crate::ingest::application::write_coordinator::errors::is_storage_full` /
/// `Metrics::mark_storage_degraded`) fast-fails every subsequent mutating
/// request with `507 Insufficient Storage` instead of re-attempting (and
/// re-failing) the same durable write. Deliberately a pure gauge read: no
/// I/O, so this never itself contributes to a full disk. Reads/search/health
/// are exempt — they keep serving while degraded (see the `readyz`
/// discussion in this issue's report: a degraded node still answers reads).
pub(crate) fn enforce_storage_writable(state: &AppState) -> Result<(), ApiErr> {
    if state.writer.restart_required() {
        return Err(ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "restart_required",
            "node observed an unresolved durability boundary; restart this Lumen process before retrying any mutation"
                .to_string(),
        ));
    }
    if state.engine.metrics().is_storage_degraded() {
        return Err(ApiErr::new(
            StatusCode::INSUFFICIENT_STORAGE,
            "storage_full",
            "node is in degraded read-only mode: local storage reported ENOSPC on a durable \
             write path; retry once the periodic re-probe clears it, or restart the pod once \
             space has been freed"
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn acquire_direct_mutation_permit(
    state: &AppState,
) -> Result<Option<tokio::sync::OwnedRwLockReadGuard<()>>, ApiErr> {
    enforce_storage_writable(state)?;
    let Some(gate) = state.writer.mutation_gate() else {
        return Ok(None);
    };
    gate.shared().await.map(Some).map_err(ApiErr::from)
}

// ---------------------------------------------------------------------------
// Admin
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/healthz",
    tag = "Admin",
    security(()),
    responses((status = 200, description = "Process is alive", body = String))
)]
/// OpenAPI metadata for the shared `/healthz` implementation in service-http.
#[allow(dead_code)]
async fn healthz() -> &'static str {
    "ok"
}

#[utoipa::path(
    get,
    path = "/version",
    tag = "Admin",
    security(()),
    responses((status = 200, description = "Build provenance: version, git sha, build time", body = serde_json::Value))
)]
/// Build provenance. `version` is the crate version; `git_sha` and `built_at`
/// are stamped by `build.rs` and degrade to "unknown" outside a git checkout.
async fn version() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "git_sha": option_env!("LUMEN_GIT_SHA").unwrap_or("unknown"),
        "built_at": option_env!("LUMEN_BUILT_AT").unwrap_or("unknown"),
    }))
}

#[utoipa::path(
    get,
    path = "/readyz",
    tag = "Admin",
    security(()),
    responses(
        (status = 200, description = "Engine ready"),
        (status = 503, description = "Not ready")
    )
)]
/// OpenAPI metadata for the shared `/readyz` implementation in service-http.
#[allow(dead_code)]
async fn readyz(State(state): State<AppState>) -> (StatusCode, &'static str) {
    if state.writer.restart_required() {
        (StatusCode::SERVICE_UNAVAILABLE, "restart required")
    } else if state.engine.is_draining() {
        (StatusCode::SERVICE_UNAVAILABLE, "draining")
    } else {
        (StatusCode::OK, "ok")
    }
}

/// Classify one batch item's search failure into a
/// [`BatchSearchResult::Error`] instead of failing the whole batch. Mirrors
/// `From<anyhow::Error> for ApiErr`'s `StorageError` classification, but the
/// `code` values line up with the batch wire contract
/// (`"collection_not_found"`, ...) rather than `ApiErr`'s internal `kind`
/// strings.
pub(crate) fn batch_search_storage_error(e: anyhow::Error) -> BatchSearchResult {
    let code = match e.downcast_ref::<StorageError>() {
        Some(StorageError::CollectionNotFound(_)) => "collection_not_found",
        Some(StorageError::InvalidCollectionName(_)) => "invalid_collection_name",
        Some(StorageError::UnknownField { .. }) => "unknown_field",
        Some(StorageError::TypeMismatch { .. }) => "type_mismatch",
        Some(StorageError::DuplicatesOnText(_)) => "bad_request",
        Some(StorageError::InvalidNumber(_)) => "invalid_number",
        Some(StorageError::BulkLimit { .. }) => "bulk_limit",
        Some(StorageError::QueryTooComplex(_)) => "query_too_complex",
        Some(StorageError::InvalidPagination(_)) => "invalid_pagination",
        Some(StorageError::Gone(_)) => "gone",
        Some(StorageError::UnsupportedSort(_)) => "unsupported_sort",
        Some(StorageError::InvalidPruneChunk { .. }) => "invalid_prune_chunk",
        Some(StorageError::PruneAccumulatorFull { .. }) => "prune_accumulator_full",
        None => "bad_request",
    };
    BatchSearchResult::Error {
        code: code.to_string(),
        message: e.to_string(),
    }
}

// ---------------------------------------------------------------------------
// OpenAPI
// ---------------------------------------------------------------------------

pub fn openapi() -> utoipa::openapi::OpenApi {
    let mut doc = ApiDoc::openapi();
    doc.info.version = env!("CARGO_PKG_VERSION").to_string();
    inject_query_twins(&mut doc);
    doc
}

/// Describe the #1297 `QUERY` twins (OpenAPI 3.2 / RFC 10008, epic #1296 R1)
/// in the generated document: `QUERY /collections` (twin of `POST
/// /collections:search`) and `QUERY /collections/{collection_id}` (twin of
/// `POST /collections/{collection_id}/search`).
///
/// utoipa 4.2.3 predates OpenAPI 3.2 and has no `PathItemType::Query`
/// variant, so the operation is injected as raw JSON via
/// `PathItem::extensions` — utoipa `#[serde(flatten)]`s that map into the
/// serialized path-item object next to `get`/`post`/etc, giving a `"query"`
/// key byte-identical in shape to a native one. `libs/openapi-codegen`'s IR
/// (`ir/operations.rs`, #1298) only needs that serialized `"query"` key plus
/// an `x-post-twin` extension pointing at the POST twin path; it does not
/// require a typed enum variant to parse the operation. (The `"openapi"`
/// version field itself stays at utoipa's fixed `3.0.3` here — that enum has
/// no 3.2 variant — `lumen spec`'s offline output stamps 3.2 on top; see
/// `spec::openapi_value`.)
fn inject_query_twins(doc: &mut utoipa::openapi::OpenApi) {
    let twin = |doc: &utoipa::openapi::OpenApi, twin_path: &str, operation_id: &str| {
        let mut op = doc
            .paths
            .paths
            .get(twin_path)?
            .operations
            .get(&openapi::PathItemType::Post)?
            .clone();
        op.operation_id = Some(operation_id.to_string());
        op.extensions
            .get_or_insert_with(Default::default)
            .insert("x-post-twin".to_string(), serde_json::json!(twin_path));
        Some(serde_json::to_value(&op).expect("Operation serializes to JSON"))
    };

    if let Some(query_op) = twin(
        doc,
        "/collections/{collection_id}/search",
        "query_collection",
    ) {
        if let Some(item) = doc.paths.paths.get_mut("/collections/{collection_id}") {
            item.extensions
                .get_or_insert_with(Default::default)
                .insert("query".to_string(), query_op);
        }
    }

    if let Some(query_op) = twin(doc, "/collections:search", "query_collections") {
        if let Some(item) = doc.paths.paths.get_mut("/collections") {
            item.extensions
                .get_or_insert_with(Default::default)
                .insert("query".to_string(), query_op);
        }
    }
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

/// HTTP-friendly wrapper that classifies storage errors to status codes.
/// A newtype over the shared `service_http::ApiErr` (status + kind +
/// message, `IntoResponse` renders `service_http::ErrorEnvelope` JSON) —
/// this file keeps only the `StorageError` / `AuthErr` → (status, kind)
/// classification arms. (`crate::shared_kernel::types::api_error::ApiError`
/// stays a distinct local struct of the same `{error, message}` shape purely
/// for OpenAPI schema identity — see its doc comment.)
pub struct ApiErr(service_http::ApiErr);

impl ApiErr {
    pub(crate) fn new(status: StatusCode, kind: &'static str, message: impl Into<String>) -> Self {
        Self(service_http::ApiErr::new(status, kind, message))
    }

    pub(crate) fn not_found(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", msg)
    }

    fn pending_change_capacity(msg: impl Into<String>) -> Self {
        Self(
            service_http::ApiErr::new(
                StatusCode::TOO_MANY_REQUESTS,
                "pending_change_capacity",
                msg,
            )
            .with_retry_after_seconds(1),
        )
    }
}

impl From<anyhow::Error> for ApiErr {
    fn from(e: anyhow::Error) -> Self {
        if e.downcast_ref::<PendingChangeCapacity>().is_some() {
            return Self::pending_change_capacity(e.to_string());
        }
        // #1486 R2: a write waiter released without a genuine apply outcome
        // (dedup-guard skip or a bounded submit() timeout) is transient —
        // surface it as a retryable 503, never the generic 400 fallback
        // below (which would misreport it as a bad request) and never a
        // silent hang (the original defect).
        if e.downcast_ref::<SubmitStalled>().is_some() {
            return Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "write_stalled",
                e.to_string(),
            );
        }
        if e.downcast_ref::<RestartRequired>().is_some() {
            return Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "restart_required",
                e.to_string(),
            );
        }
        if e.downcast_ref::<RestoreNotCommitted>().is_some() {
            return Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "restore_not_committed",
                e.to_string(),
            );
        }
        if e.downcast_ref::<RestoreUnavailable>().is_some() {
            return Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "restore_unavailable",
                e.to_string(),
            );
        }
        // #2516: a durable write path genuinely hit ENOSPC — the write
        // coordinator already flipped the sticky degraded gauge before
        // producing this error. Report 507 Insufficient Storage with the
        // stable `storage_full` code rather than falling through to the
        // generic 400 default.
        if e.downcast_ref::<StorageFullError>().is_some() {
            return Self::new(
                StatusCode::INSUFFICIENT_STORAGE,
                "storage_full",
                e.to_string(),
            );
        }
        if e.downcast_ref::<ShardForwardUnavailable>().is_some() {
            return Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_forward_unavailable",
                e.to_string(),
            );
        }
        if let Some(re) = e.downcast_ref::<ShardForwardRemoteError>() {
            let status = StatusCode::from_u16(re.status).unwrap_or(StatusCode::BAD_GATEWAY);
            return Self::new(status, "shard_forwarded_error", re.message.clone());
        }
        if e.downcast_ref::<ShardMapVersionMismatch>().is_some() {
            return Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_map_version_mismatch",
                e.to_string(),
            );
        }
        if e.downcast_ref::<ShardForwardMisrouted>().is_some() {
            return Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_forward_misrouted",
                e.to_string(),
            );
        }
        if let Some(se) = e.downcast_ref::<StorageError>() {
            return match se {
                StorageError::CollectionNotFound(_) => {
                    Self::new(StatusCode::NOT_FOUND, "not_found", e.to_string())
                }
                StorageError::InvalidCollectionName(_) => Self::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_collection_name",
                    e.to_string(),
                ),
                StorageError::UnknownField { .. } => Self::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "unknown_field",
                    e.to_string(),
                ),
                StorageError::TypeMismatch { .. } => Self::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "type_mismatch",
                    e.to_string(),
                ),
                StorageError::DuplicatesOnText(_) => {
                    Self::new(StatusCode::BAD_REQUEST, "bad_request", e.to_string())
                }
                StorageError::InvalidNumber(_) => Self::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "invalid_number",
                    e.to_string(),
                ),
                StorageError::BulkLimit { .. } => {
                    Self::new(StatusCode::PAYLOAD_TOO_LARGE, "bulk_limit", e.to_string())
                }
                StorageError::QueryTooComplex(_) => {
                    Self::new(StatusCode::BAD_REQUEST, "query_too_complex", e.to_string())
                }
                StorageError::InvalidPagination(_) => {
                    Self::new(StatusCode::BAD_REQUEST, "invalid_pagination", e.to_string())
                }
                StorageError::Gone(_) => Self::new(StatusCode::GONE, "gone", e.to_string()),
                StorageError::UnsupportedSort(_) => {
                    Self::new(StatusCode::BAD_REQUEST, "unsupported_sort", e.to_string())
                }
                // #1467 R4: caller-declared `total_chunks` failed the sanity
                // cap — a client/protocol error, not a transient one.
                StorageError::InvalidPruneChunk { .. } => Self::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_prune_chunk",
                    e.to_string(),
                ),
                // #1467 R4: the prune accumulator is at its entry cap — a
                // 429-class signal (retryable once the driver's other,
                // presumably-stuck passes GC out or complete) rather than a
                // permanent 4xx.
                StorageError::PruneAccumulatorFull { .. } => Self::new(
                    StatusCode::TOO_MANY_REQUESTS,
                    "prune_accumulator_full",
                    e.to_string(),
                ),
            };
        }
        Self::new(StatusCode::BAD_REQUEST, "bad_request", e.to_string())
    }
}

impl IntoResponse for ApiErr {
    fn into_response(self) -> axum::response::Response {
        self.0.into_response()
    }
}

impl From<crate::access::application::authorization::AuthErr> for ApiErr {
    fn from(e: crate::access::application::authorization::AuthErr) -> Self {
        // A denial and an unanswered SubjectAccessReview reach the wire as
        // different statuses (403 vs 503); `AuthErr::wire` owns that split so
        // this conversion cannot quietly flatten it into one.
        let (status, code, message) = e.wire();
        Self::new(status, code, message)
    }
}
// CODEGEN-END

#[cfg(test)]
mod restore_sink_tests {
    use super::*;
    use crate::shared_kernel::types::schema::CreateCollectionRequest;

    fn collection_request() -> CreateCollectionRequest {
        serde_json::from_value(serde_json::json!({
            "fields": { "value": { "type": "keyword" } }
        }))
        .expect("valid collection request")
    }

    #[tokio::test]
    async fn default_sink_atomically_replaces_live_state() {
        let live = Arc::new(Engine::new());
        live.create_collection("old", collection_request())
            .expect("old collection");
        let state = AppState::open(live.clone());

        let source = Engine::new();
        source
            .create_collection("new", collection_request())
            .expect("new collection");
        let snapshot = source.snapshot().expect("snapshot");

        state
            .restore_sink
            .restore(snapshot)
            .await
            .expect("restore succeeds");
        let restored = live.snapshot().expect("restored snapshot");
        assert!(!restored.collections.contains_key("old"));
        assert!(restored.collections.contains_key("new"));
    }

    #[tokio::test]
    async fn pending_change_capacity_is_retryable_without_changing_other_error_mappings() {
        let pending = PendingChangeCapacity::from_prepublication(
            crate::ingest::domain::change_budget::AdmissionError::Full {
                requested: 64,
                used: 256,
                hard_limit: 256,
            },
        )
        .expect("pre-publication Full is retryable capacity pressure");
        let oversized = PendingChangeCapacity::from_prepublication(
            crate::ingest::domain::change_budget::AdmissionError::Oversized {
                requested: 512,
                hard_limit: 256,
            },
        )
        .expect("pre-publication oversized requests are refused before committing");
        let oversized = ApiErr::from(anyhow::Error::new(oversized)).into_response();
        assert_eq!(oversized.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(oversized.headers()[axum::http::header::RETRY_AFTER], "1");
        let response = ApiErr::from(anyhow::Error::new(pending)).into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(envelope["error"], "pending_change_capacity");
        assert_eq!(envelope["retryable"], true);
        assert_eq!(envelope["retry_after_seconds"], 1);

        let bad_request = ApiErr::from(anyhow::Error::msg("bad request")).into_response();
        assert_eq!(bad_request.status(), StatusCode::BAD_REQUEST);
        assert!(bad_request
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .is_none());
        let body = axum::body::to_bytes(bad_request.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"],
            "bad_request"
        );

        let storage_full = ApiErr::from(anyhow::Error::new(StorageFullError(
            "disk full".to_string(),
        )))
        .into_response();
        assert_eq!(storage_full.status(), StatusCode::INSUFFICIENT_STORAGE);
        assert!(storage_full
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .is_none());
        let body = axum::body::to_bytes(storage_full.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"],
            "storage_full"
        );

        let unrelated_429 = ApiErr::from(anyhow::Error::new(StorageError::PruneAccumulatorFull {
            count: 4,
            max: 4,
        }))
        .into_response();
        assert_eq!(unrelated_429.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(unrelated_429
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .is_none());
    }

    #[tokio::test]
    async fn restore_not_committed_maps_to_stable_500_envelope() {
        let response = ApiErr::from(anyhow::Error::new(RestoreNotCommitted(
            "CURRENT was not moved".to_string(),
        )))
        .into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(envelope["error"], "restore_not_committed");
    }

    #[tokio::test]
    async fn restore_unavailable_maps_to_stable_503_envelope() {
        let response = ApiErr::from(anyhow::Error::new(RestoreUnavailable(
            "non-embedded topology is unsupported".to_string(),
        )))
        .into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(envelope["error"], "restore_unavailable");
    }
}
