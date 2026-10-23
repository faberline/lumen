use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::app::http::api_err::ApiErr;
use crate::index::domain::storage_error::StorageError;
use crate::ingest::application::write_coordinator::errors::StorageFullError;
use crate::ingest::domain::change_admission::PendingChangeCapacity;
use crate::persistence::application::restore::{RestoreNotCommitted, RestoreUnavailable};

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
