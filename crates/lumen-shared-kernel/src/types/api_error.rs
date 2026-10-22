//! The OpenAPI schema of the `{error, message}` body a failed request gets
//! back.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// The runtime `{error, message}` envelope this shape describes now lives in
// `service_http::ErrorEnvelope` (`src/app/http/api_err.rs`'s `ApiErr` renders it); this
// struct stays a distinct local definition purely to keep the OpenAPI schema
// name (`ApiError`) and its doc-comment-derived `description` byte-identical
// — `#[derive(ToSchema)]` fixes both at the type's own definition site, so
// neither a `pub type` alias nor a `#[schema(as = ...)]` override on the
// shared struct can reproduce lumen's pre-existing name/description without
// baking lumen's spec-path text into the generic `libs/service-http` crate
// (see #1005).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiError {
    pub error: String,
    pub message: String,
}
