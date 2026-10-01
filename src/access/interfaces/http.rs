//! The middleware that authenticates every request, and the response an
//! authorization failure renders as.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Json, Response};

use crate::access::application::authorization::AuthErr;
use crate::access::infrastructure::lumen_verifier::LumenVerifier;
use crate::shared_kernel::types::api_error::ApiError;

pub async fn auth_middleware(
    State(verifier): State<Arc<LumenVerifier>>,
    req: Request,
    next: Next,
) -> Response {
    service_auth::async_auth_middleware::<LumenVerifier>(State(verifier), req, next).await
}

impl IntoResponse for AuthErr {
    fn into_response(self) -> Response {
        match &self {
            AuthErr::Forbidden {
                subject,
                needed,
                resource,
            } => tracing::warn!(
                target: "lumen.audit",
                event = "rbac_denied",
                %subject,
                resource = %resource,
                needed = ?needed,
            ),
            AuthErr::Unavailable {
                subject,
                needed,
                resource,
                reason,
            } => tracing::warn!(
                target: "lumen.audit",
                event = "rbac_unavailable",
                %subject,
                resource = %resource,
                needed = ?needed,
                reason = %reason,
            ),
        }
        let (status, error, message) = self.wire();
        (
            status,
            Json(ApiError {
                error: error.to_string(),
                message,
            }),
        )
            .into_response()
    }
}
