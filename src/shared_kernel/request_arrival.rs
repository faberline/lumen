//! When the HTTP server received the request a write is running for.
//!
//! A client's request timeout starts before the server reads the body. A
//! bulk write spends part of that time in body receive and JSON parsing, so a
//! deadline measured from the start of admission can end after the client has
//! already given up. The data-plane router records the arrival time around
//! each request, and admission measures its relief deadline from it.

use std::future::Future;

tokio::task_local! {
    static REQUEST_RECEIVED_AT: tokio::time::Instant;
}

/// Run `request` with `received_at` as its arrival time.
pub(crate) async fn scope<F: Future>(received_at: tokio::time::Instant, request: F) -> F::Output {
    REQUEST_RECEIVED_AT.scope(received_at, request).await
}

/// The arrival time of the request this task serves, or `None` outside an
/// HTTP request (replay, internal callers, or a task spawned by a handler).
pub(crate) fn received_at() -> Option<tokio::time::Instant> {
    REQUEST_RECEIVED_AT
        .try_with(|received_at| *received_at)
        .ok()
}
