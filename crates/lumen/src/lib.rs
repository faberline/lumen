//! lumen — standalone search and duplicate-detection index.
//!
//! Solves the gap B-tree indexes can't fill: keyword search (incl. Chinese
//! tokenisation) and duplicate detection. Exposed as a generic
//! `Collection / Field` primitive over `external_id` — lumen never owns
//! the source of truth and has no document concept of its own.
//!
//! - Durable via the configured write log; multi-pod Lumen uses Lumen-owned
//!   primary/replica replication. Rebuildable from the caller.
//! - HTTP/2 transport, client-side collection-shard routing.
//!
//! Full surface and v1 scope: `README.md`.
//!
//! ## Contracts inherited from the retired EC shells
//!
//! These 3 sentences were the whole of the `// Contract:` comment in 3 AW-EC shells
//! under `e2e/`, each of which ran `cargo test -p lumen --lib` in a
//! subprocess and asserted the child's exit status. `cargo test -p lumen` already runs
//! this crate's colocated unit tests directly, so the shells added a second, nested run
//! and nothing else. They were deleted on 2026-08-20 with the EC machinery they
//! belonged to, and the sentence is the only thing they held that nothing else did.
//! Each line below is prefixed with the EC id its shell was filed under.
//!
//! - `lumen-claim-dynamic-versioned-virtual-bucket-map` — Versioned virtual-bucket
//!   routing remains the stable shard ownership contract.
//! - `lumen-claim-exact-keyword-lexicographic-range` — Keyword range queries use
//!   deterministic byte-lexicographic bounds rather than text analysis semantics.
//! - `lumen-claim-security-tls-rustls` — The rustls-backed TLS surface passes the
//!   runtime TLS gate.

/// Local append-only log (Stage 2 Phase 2f-3): the binary's "AOF" — a framed,
/// crash-safe record of every APPLIED `(seq, WalRecord)`. Recovery is RDB (the
/// segment checkpoint, up to seq S) → AOF replay (S+1..A) → broker tail (A+1..),
/// so broker retention can be TRIMMED instead of kept from seq 0. Compiled by
/// default; only the runtime segment-persistence path (`--persistence=segment`)
/// drives the apply loop + cold-start through it.
pub mod aof {
    pub use crate::persistence::infrastructure::aof::aof_writer::AofWriter;
    pub use crate::persistence::infrastructure::aof::replay::{replay_aof_into, AofReader};
    pub use crate::persistence::infrastructure::aof::{FramedLogTrimObserver, FsyncPolicy};
}
pub mod api {
    pub use crate::app::http::api_err::ApiErr;
    pub use crate::app::http::app_state::AppState;
    pub use crate::app::http::openapi::{openapi, ApiDoc};
    pub use crate::app::http::router::{router, router_with_admission};
    pub use crate::app::http::write_fence::WriteFence;
    pub use crate::index::application::ports::search_backend::SearchBackend;
    pub use crate::ingest::application::ports::write_backend::WriteBackend;
    pub use crate::persistence::application::ports::checkpoint_sink::{
        CheckpointSink, HnswCacheDurability, HnswCacheSealReceipt,
    };
    pub use crate::persistence::application::ports::restore_sink::RestoreSink;
    pub use crate::sharding::application::ports::routed_backend::RoutedBackend;
    pub use crate::sharding::domain::forward_error::{
        ShardForwardMisrouted, ShardForwardRemoteError, ShardForwardUnavailable,
        ShardMapVersionMismatch,
    };
}
pub mod auth {
    pub use crate::access::application::auth_config::{AuthConfig, AuthProfile};
    pub use crate::access::application::authorization::{
        verb, AuthContext, AuthErr, AuthTarget, Role,
    };
    pub use crate::access::application::control_plane_token::{
        control_plane_token_file, CONTROL_PLANE_TOKEN_FILE, CONTROL_PLANE_TOKEN_MOUNT,
        CONTROL_PLANE_TOKEN_VOLUME,
    };
    pub use crate::access::domain::identity::{
        ADMIN_RESOURCE, API_GROUP, AUDIENCE, COLLECTIONS_RESOURCE,
    };
    pub use crate::access::infrastructure::lumen_verifier::LumenVerifier;
    pub use crate::access::interfaces::http::auth_middleware;
}
/// `lumen backup` (#808): fetches a consistent snapshot from a running
/// serving fleet's existing `GET /admin/backup` endpoint and hands it to a
/// `libs/service-backup` destination sink. No new snapshot mechanism — this
/// is transport/scheduling only, meant to be driven by the operator's
/// optional backup CronJob (`spec.backup`, see `service_k8s::render`) or ad hoc.
/// Behind the `backup` feature (pulled in by `operator`) since it needs an
/// HTTP client; `raft-wal` already links one into every shipped binary.
#[cfg(feature = "backup")]
pub mod backup {
    pub use crate::persistence::application::backup::{
        fetch_snapshot_bytes, restore_snapshot_bytes, run_backup,
    };
    pub use crate::persistence::application::backup_set_report::{
        run_queries, verify_restored_set, Check, QueryAnswer, ShardReport, VerificationReport,
        REPORT_VERSION,
    };
    pub use crate::persistence::application::backup_set_restore::{
        restore_backup_set, BackupSetSource, DirSource, EngineSet, RestoreError, RestoreOptions,
        RestoreReport, ShardRestoreTarget,
    };
    pub use crate::persistence::domain::backup_manifest::{
        build_backup_manifest, read_backup_manifest, verify_artifact, BackupManifest,
        ManifestError, ShardArtifact, ShardEntry, BACKUP_MANIFEST_KEY, BACKUP_MANIFEST_VERSION,
    };
}
pub mod backup_sink {
    pub use crate::persistence::infrastructure::backup_sink::{
        run_backup_once, sink_from_destination, BackupDestination, BackupObject, BackupPolicy,
        BackupRunResult, BackupSink, LocalFsSink, RetentionPolicy, UnsupportedCloudSink,
    };
}

pub mod config {
    pub use crate::replication::domain::cluster_config::ClusterConfig;
    pub use crate::sharding::infrastructure::shard_map_env::{
        check_fan_in_shard_count, fan_in_shard_count, routed_activation_shard_count,
        routed_pod_topology, routed_shard_count_from_env, shard_map_from_env,
    };
}
pub mod coordinator {
    pub use crate::ingest::application::write_coordinator::errors::{
        is_storage_full, RestartRequired, StorageFullError, SubmitStalled,
    };
    pub use crate::ingest::application::write_coordinator::mutation_gate::MutationGate;
    pub use crate::ingest::application::write_coordinator::{
        SharedAof, WriteCoordinator, WriteSink,
    };
}
pub mod dx {
    pub use crate::app::spec::dx::{field_catalog, llm_protocol, render_llm};
}
pub mod metrics {
    pub use crate::app::observability::metrics::labels::{
        apply_item_count, kind_label, ApplyKind, CoordinatorStage, MergeStep, APPLY_KIND_COUNT,
        MERGE_STEP_COUNT,
    };
    pub use crate::app::observability::metrics::Metrics;
}
/// Native length-prefixed CBOR search wire for Rust clients that need the engine
/// over a lower fixed-cost transport than HTTP/JSON.
pub mod native_wire {
    pub use crate::index::interfaces::native_wire::codec::{
        encode_range_frame, encode_search_frame, encode_term_frame, encode_term_range_frame,
    };
    pub use crate::index::interfaces::native_wire::server::serve_search;
    pub use crate::index::interfaces::native_wire::{
        search_prepared, NativeSearchRequest, NativeSearchResponse,
    };

    #[cfg(unix)]
    pub use crate::index::interfaces::native_wire::server::serve_unix_search;
}
pub use crate::sharding::application::consumer;
/// Write-log entry vocabulary (always compiled; the active write path uses it).
pub use crate::shared_kernel::log_entry;
use lumen_access as access;
use lumen_index::{app, index, ingest, persistence, sharding};
use lumen_replication as replication;
use lumen_shared_kernel as shared_kernel;
/// K8s Operator: the `Lumen` CRD plus the reconcile loop that renders + applies
/// the Lumen serving/data-plane resources. The CRD and reconcile loop are behind
/// the `operator` feature so the serving binary never pulls in kube-rs; pure
/// configuration stays in the operator and shared libraries; Lumen consumes
/// externally provisioned TLS Secrets rather than resolving issuers.
pub mod operator;
pub use crate::access::infrastructure::tls;
/// Cluster-state view types backing the read/admin API. This surface is the
/// compatibility bridge for Lumen-owned primary/replica replication.
pub mod raft {
    pub use crate::replication::domain::cluster_state::{ClusterState, ReadConsistency};
    pub use crate::replication::domain::peer_addr::RaftGroup;
    pub use crate::shared_kernel::types::cluster_state_view::ClusterStateView;
    pub use crate::shared_kernel::types::peer_addr::PeerAddr;
    pub use crate::shared_kernel::types::raft_role::RaftRole;
}
/// `EngineSm` — lumen's `Engine` as a shared-`raft_runtime` state machine: the
/// convergence onto `libs/raft-runtime` (#524). The host is the sole applier, so
/// the per-service driver, durable hard state, and the WAL seam are no longer
/// lumen's to own — they live in the shared lib.
#[cfg(feature = "raft-wal")]
pub mod raft_sm {
    pub use crate::ingest::application::engine_sm::write_sink::RaftWriteSink;
    pub use crate::ingest::application::engine_sm::EngineSm;
}
pub mod rdb {
    pub use crate::persistence::domain::rdb_store::RdbStore;
    pub use crate::persistence::infrastructure::rdb::{LocalFsRdbStore, RdbSnapshot};
}
pub mod reshard {
    pub use crate::sharding::domain::bucket_move::{bucket_moves, BucketMove};
    pub use crate::sharding::domain::merge_delta::merge_snapshot_delta;
    pub use crate::sharding::domain::prune_chunk::{
        snapshot_reshard_prune_chunks, ReshardBatchReplaceScope, ReshardPruneChunk,
    };
    pub use crate::sharding::domain::reshard_batch::{
        snapshot_reshard_batches, ReshardBatch, ADMIN_ROUTE_BODY_LIMIT_BYTES, MAX_BATCH_BYTES,
    };
    pub use crate::sharding::domain::snapshot_subset::snapshot_bucket_subset;
    pub use crate::sharding::infrastructure::body_limit::body_limit_bytes_from_env;
}
pub mod routing {
    pub use crate::sharding::application::engine_shard_search::EngineShardSearch;
    pub use crate::sharding::application::engine_shard_write::EngineShardWrite;
    pub use crate::sharding::application::search_fanout::{
        merge_shard_search_responses, search_shards_parallel,
    };
    pub use crate::sharding::domain::shard_index::{document_shard_index, shard_host, shard_index};
    pub use crate::sharding::domain::shard_route::{SearchShardTarget, ShardRoute};
    pub use crate::sharding::domain::virtual_bucket_shard_map::{
        VirtualBucketShardMap, DEFAULT_VIRTUAL_BUCKET_COUNT,
    };
}
/// Cross-pod shard routing for operator/k8s serving pods (#1398 R1-R3): local
/// reads/writes hit the engine directly, remote-owned buckets forward one hop
/// over h2c. Behind `operator` because it is the only module that needs
/// `reqwest` as a directly-nameable type; every real deployment already links
/// it via `operator`'s `backup` feature, so this adds no new crate.
#[cfg(feature = "operator")]
pub mod routing_remote {
    pub use crate::sharding::infrastructure::routed_router::RoutedRouter;
}
pub mod segment_checkpoint {
    pub use crate::persistence::application::segment_checkpoint_sink::driver::SegmentCheckpointDriver;
    pub use crate::persistence::application::segment_checkpoint_sink::pending_spill::PendingChangeSpill;
    pub use crate::persistence::application::segment_checkpoint_sink::{
        EngineWatermarkSink, SegmentCheckpointSink,
    };
}
/// Segment-checkpoint persistence store (Stage 2 Phase 2f-2): the disk engine
/// as the running binary's "RDB" — a generation-versioned directory of per-
/// collection segment checkpoints, written atomically (stage + rename) so a torn
/// checkpoint never replaces a good one. Parallels [`rdb::LocalFsRdbStore`].
/// Compiled by default; selected at runtime via `--persistence=segment` (the
/// default binary keeps the CBOR RDB).
pub mod segment_rdb {
    pub use crate::persistence::infrastructure::segment_rdb_store::startup::{
        SegmentStartupDecision, SegmentStartupOutcome,
    };
    pub use crate::persistence::infrastructure::segment_rdb_store::{
        LoadedSegmentGeneration, MergeObserver, MergePhase, SegmentRdbStore,
    };
}
pub mod segment_restore {
    pub use crate::persistence::application::restore::{
        RestorePublicationObserver, SegmentRestoreSink, UnavailableRestoreSink,
    };
}
/// Offline machine-readable self-description (`lumen spec`): OpenAPI / JSON
/// schema, the query-shape cookbook, and the field/analyzer catalog — the
/// agent-integration surface, emitted without a running server.
pub mod spec {
    pub use crate::app::spec::llm_auth::llm_auth_md;
    pub use crate::app::spec::llm_deployment::llm_deployment_md;
    pub use crate::app::spec::llm_quickstart::llm_quickstart_md;
    pub use crate::app::spec::llm_storage::llm_storage_md;
    pub use crate::app::spec::llm_workflow::llm_workflow_md;
    pub use crate::app::spec::query_shapes::query_shapes;
    pub use crate::app::spec::{
        field_catalog, json_schema_json, llm_integration_md, llm_outline_md, llm_recipes_md,
        openapi_json, openapi_yaml,
    };
}
pub mod storage {
    //! `crate::storage` before the DDD split. Re-exports its public items from
    //! their new homes so callers outside the crate keep compiling.
    //!
    //! In-memory storage and query execution.
    //!
    //! The engine is `BTreeMap`-backed inverted indexes per field,
    //! constructed by [`Engine::new`]. Single-pod, single-shard; durability
    //! comes from the CBOR RDB snapshot path and (when segment persistence is
    //! selected) the columnar mmap segment tier — not from this module.
    //!
    //! The shape below maps 1:1 to the field-type table in the README:
    //!
    //! | FieldType | Index                                  |
    //! |-----------|----------------------------------------|
    //! | `text`    | `BTreeMap<token, BTreeSet<eid>>`       |
    //! | `keyword` | `BTreeMap<value, BTreeSet<eid>>`       |
    //! | `number`  | `BTreeMap<SortableF64, BTreeSet<eid>>` |
    //! | `set`     | `BTreeMap<element, BTreeSet<eid>>`     |
    //!
    //! Every field also carries a per-`external_id` "forward" map so
    //! re-indexing the same `(eid, field)` cleanly evicts the old postings
    //! before appending the new ones.
    pub use crate::index::application::engine::collections::DropOutcome;
    pub use crate::index::application::engine::index::MAX_INDEX_ITEMS;
    pub use crate::index::application::engine::raft_dispatch::ApplyOutcome;
    pub use crate::index::application::engine::reshard_apply::{
        ReshardApplyOutcome, ReshardEvictOutcome,
    };
    pub use crate::index::application::engine::reshard_prune::ReshardPruneOutcome;
    pub use crate::index::application::engine::Engine;
    pub use crate::index::domain::collection::coverage::{FieldNotAudited, ReindexNeeded};
    pub use crate::index::domain::query::sort::MAX_SORT_KEYS;
    pub use crate::index::domain::query::validate_query;
    pub use crate::index::domain::sortable_f64::SortableF64;
    pub use crate::index::domain::storage_error::StorageError;
    pub use crate::index::infrastructure::collection_retirement::{
        collection_reclaimer_snapshot, CollectionReclaimerSnapshot,
    };
    pub use crate::index::infrastructure::snapshot_v1::{
        CollectionSnapshot, FieldIndexSnapshot, LegacyInvertedIndex, SnapshotV1,
    };
}
pub mod tokenize {
    pub use crate::index::domain::analysis::tokenize::{
        tokenize, DEFAULT_NGRAM_MAX, DEFAULT_NGRAM_MIN,
    };
}
pub mod types {
    pub use crate::shared_kernel::types::api_error::ApiError;
    pub use crate::shared_kernel::types::document::{
        validate_batch_unindex_docs_request, BatchUnindexDocsRequest,
        BatchUnindexDocsValidationError, FieldValue, IndexItem, IndexRequest, IndexResponse,
        ReplaceDocBody, ReplaceDocItem, ReplaceDocResult, ReplaceDocsRequest, ReplaceDocsResponse,
        MAX_BATCH_REPLACE_SIZE, MAX_BATCH_UNINDEX_DOCS_SIZE, MAX_INDEX_BATCH_SIZE,
    };
    pub use crate::shared_kernel::types::query::{
        DuplicatedQuery, ExistsQuery, HammingQuery, HasChildQuery, IdsQuery, KnnQuery, MatchOp,
        MatchQuery, PrefixQuery, QueryNode, RangeBound, RangeQuery, RrfQuery, SortMissing,
        SortOrder, SortSpec, TermQuery, TermsQuery,
    };
    pub use crate::shared_kernel::types::schema::{
        Analyzer, CreateCollectionRequest, CreateCollectionResponse, FieldCapabilities, FieldSpec,
        FieldType, VectorBackend, VectorMetric, VectorQuantize, VectorSpec,
    };
    pub use crate::shared_kernel::types::search::{
        BatchSearchItem, BatchSearchRequest, BatchSearchResponse, BatchSearchResult,
        DuplicateGroup, DuplicatesRequest, DuplicatesResponse, SearchAllRequest, SearchAllResponse,
        SearchHit, SearchRequest, SearchResponse, TrackTotal, MAX_BATCH_SEARCH_SIZE,
    };
    pub use crate::shared_kernel::types::stats::{
        CacheStats, FieldStats, StatsResponse, StorageStats,
    };
}
pub mod vector_index {
    pub use crate::index::domain::vector::flat_cpu_index::FlatCpuIndex;
    pub use crate::index::domain::vector::hnsw_cpu_index::HnswCpuIndex;
    pub use crate::index::domain::vector::quantize::{decode_sq, encode_sq, ScalarCodebook};
    pub use crate::index::domain::vector::{open_backend, VectorIndex};
}
pub mod wal {
    pub use crate::ingest::domain::wal_log::{SharedWal, WalAdmissionStream, WalLog, WalStream};
    pub use crate::ingest::domain::wal_record::{
        WalRecord, WAL_CONTROL_FORMAT_VERSION, WAL_FORMAT_VERSION,
    };
    pub use crate::ingest::infrastructure::wal::delivery::{
        WalDelivery, WalSourceRecord, WalSourceRelease,
    };
    pub use crate::ingest::infrastructure::wal::mem_wal::MemWal;
}

/// Product-neutral text-index contracts used by Lumen and other products.
/// Lumen keeps its existing public collection API and storage engine.
pub use index_text as text_index;

#[cfg(test)]
mod tests;
