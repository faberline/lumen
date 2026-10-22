//! The OpenAPI document: ApiDoc with its bearer security scheme, and the QUERY
//! twins injected into it.

use utoipa::openapi;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

use crate::persistence::interfaces::http::checkpoint::{
    HnswCacheSealMutationStamp, HnswCacheSealResponse,
};
use crate::shared_kernel::types::api_error::ApiError;
use crate::shared_kernel::types::document::{
    BatchUnindexDocsRequest, FieldValue, IndexItem, IndexRequest, IndexResponse, ReplaceDocBody,
    ReplaceDocItem, ReplaceDocResult, ReplaceDocsRequest, ReplaceDocsResponse,
};
use crate::shared_kernel::types::query::{
    KnnQuery, MatchOp, MatchQuery, QueryNode, RangeQuery, TermQuery, TermsQuery,
};
use crate::shared_kernel::types::schema::{
    Analyzer, CreateCollectionRequest, CreateCollectionResponse, FieldSpec, FieldType,
    VectorBackend, VectorMetric, VectorQuantize, VectorSpec,
};
use crate::shared_kernel::types::search::{
    BatchSearchRequest, BatchSearchResponse, BatchSearchResult, DuplicateGroup, DuplicatesRequest,
    DuplicatesResponse, SearchAllRequest, SearchAllResponse, SearchHit, SearchRequest,
    SearchResponse,
};
use crate::shared_kernel::types::stats::{CacheStats, FieldStats, StatsResponse, StorageStats};

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
        crate::app::http::probes::healthz,
        crate::app::http::probes::readyz,
        crate::app::http::probes::version,
        crate::app::http::probes::metrics,
        crate::app::http::probes::debug_cluster,
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
