use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::ingest::application::write_coordinator::errors::{RestartRequired, StorageFullError};
use crate::persistence::application::ports::checkpoint_sink::{
    HnswCacheSealInvalidated, HnswCacheSealUnavailable,
};
use crate::persistence::interfaces::http::checkpoint::hnsw_cache_seal_api_error;

async fn assert_hnsw_cache_seal_error(
    error: anyhow::Error,
    expected_status: StatusCode,
    expected_code: &str,
) {
    let response = hnsw_cache_seal_api_error(error).into_response();
    assert_eq!(response.status(), expected_status);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("error body");
    let envelope: serde_json::Value = serde_json::from_slice(&body).expect("JSON envelope");
    assert_eq!(envelope["error"], expected_code);
}

#[tokio::test]
async fn hnsw_cache_seal_errors_fail_closed_with_stable_envelopes() {
    assert_hnsw_cache_seal_error(
        anyhow::Error::new(HnswCacheSealUnavailable("no graph".to_string())),
        StatusCode::CONFLICT,
        "planned_restart_cache_unavailable",
    )
    .await;
    assert_hnsw_cache_seal_error(
        anyhow::Error::new(HnswCacheSealInvalidated("graph changed".to_string())),
        StatusCode::CONFLICT,
        "planned_restart_cache_invalidated",
    )
    .await;
    assert_hnsw_cache_seal_error(
        anyhow::Error::new(RestartRequired("replay first".to_string())),
        StatusCode::SERVICE_UNAVAILABLE,
        "restart_required",
    )
    .await;
    assert_hnsw_cache_seal_error(
        anyhow::Error::new(StorageFullError("disk full".to_string())),
        StatusCode::INSUFFICIENT_STORAGE,
        "storage_full",
    )
    .await;
    assert_hnsw_cache_seal_error(
        anyhow::Error::msg("cache write failed"),
        StatusCode::INTERNAL_SERVER_ERROR,
        "planned_restart_cache_failed",
    )
    .await;
}
