use super::*;
use axum::http::HeaderValue;
use axum::response::IntoResponse;

fn headers(values: &[&str]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for value in values {
        map.append(
            "idempotency-key",
            HeaderValue::from_str(value).expect("test header value is valid"),
        );
    }
    map
}

/// Unwraps an accepted result. `ApiErr` has no `Debug`, so `Result::unwrap` is unavailable.
fn accepted(result: Result<Option<String>, ApiErr>) -> Option<String> {
    match result {
        Ok(value) => value,
        Err(_) => panic!("expected Ok, got a 400 rejection"),
    }
}

/// Unwraps the 400 error and returns its status, parsed JSON envelope and raw body text.
async fn rejected(
    result: Result<Option<String>, ApiErr>,
) -> (StatusCode, serde_json::Value, String) {
    let err = match result {
        Err(err) => err,
        Ok(value) => panic!("expected a 400 rejection, got Ok({value:?})"),
    };
    let response = err.into_response();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body).into_owned();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (status, json, text)
}

#[test]
fn header_wins_over_body_request_id() {
    let result = idempotency_key_from(&headers(&["hdr-1"]), Some("body-1".to_string()));
    assert_eq!(accepted(result), Some("hdr-1".to_string()));
}

// Negative control: a body-only request must keep its body value. A helper that
// returns None here, or rewrites the body value, fails this test.
#[test]
fn body_only_passes_through_unchanged() {
    let result = idempotency_key_from(&HeaderMap::new(), Some("body-1".to_string()));
    assert_eq!(accepted(result), Some("body-1".to_string()));
}

#[test]
fn neither_is_none() {
    let result = idempotency_key_from(&HeaderMap::new(), None);
    assert_eq!(accepted(result), None);
}

#[test]
fn key_of_255_bytes_is_accepted() {
    let key = "k".repeat(255);
    let result = idempotency_key_from(&headers(&[key.as_str()]), None);
    assert_eq!(accepted(result), Some(key));
}

#[tokio::test]
async fn key_of_256_bytes_is_rejected() {
    let key = "k".repeat(256);
    let (status, json, _) = rejected(idempotency_key_from(&headers(&[key.as_str()]), None)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json["error"], "invalid_idempotency_key");
}

#[tokio::test]
async fn empty_key_is_rejected() {
    let (status, json, _) = rejected(idempotency_key_from(&headers(&[""]), None)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json["error"], "invalid_idempotency_key");
}

#[tokio::test]
async fn key_with_a_space_is_rejected() {
    let (status, json, _) = rejected(idempotency_key_from(&headers(&["a b"]), None)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json["error"], "invalid_idempotency_key");
}

#[tokio::test]
async fn duplicated_header_is_rejected() {
    let (status, json, _) = rejected(idempotency_key_from(&headers(&["one", "two"]), None)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json["error"], "invalid_idempotency_key");
}

#[tokio::test]
async fn error_message_never_contains_the_key() {
    let key = format!("secret-key-{}", "x".repeat(300));
    let (status, json, text) =
        rejected(idempotency_key_from(&headers(&[key.as_str()]), None)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json["error"], "invalid_idempotency_key");
    assert!(
        !text.contains("secret-key-"),
        "error body must not echo the key: {text}"
    );
}
