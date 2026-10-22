//! ApiErr, the error every handler answers with: it maps storage, write-path,
//! restore and authorization errors to an HTTP status and a stable `kind`.
//! batch_search_storage_error classifies a storage error the same way into
//! one batch search item's error code.

use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::index::domain::storage_error::StorageError;
use crate::ingest::application::write_coordinator::errors::{
    RestartRequired, StorageFullError, SubmitStalled,
};
use crate::ingest::domain::change_admission::PendingChangeCapacity;
use crate::persistence::application::restore::{RestoreNotCommitted, RestoreUnavailable};
use crate::sharding::domain::forward_error::{
    ShardForwardMisrouted, ShardForwardRemoteError, ShardForwardUnavailable,
    ShardMapVersionMismatch,
};
use crate::shared_kernel::types::search::BatchSearchResult;

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

#[cfg(test)]
mod tests;
