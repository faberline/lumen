//! QUERY (RFC 10008) — dual-registered POST twins (epic #1296 R1).
//!
//! axum has no native `Method::QUERY`/`MethodFilter::QUERY` yet
//! (tokio-rs/axum#3799, PR #3801 open). The interim dispatch below registers
//! each route's `fallback` — the handler axum calls for any method not
//! explicitly claimed by that route's `get`/`post`/`put`/`delete`/`options`/
//! `head` combinators — and re-checks the method by hand via
//! `Method::from_bytes(b"QUERY")`. Replace `is_query_method` and both
//! `*_query_dispatch` fallbacks with native `MethodFilter::QUERY` combinators
//! once that PR lands; `*_query_probe` (OPTIONS/HEAD) can move to ordinary
//! combinators unchanged.

use axum::extract::{Extension, FromRequest, Path, Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Json};

use crate::access::application::authorization::AuthContext;
use crate::api::AppState;
use crate::index::interfaces::http::batch_search::batch_search_core;
use crate::index::interfaces::http::search::search_core;
use crate::shared_kernel::types::search::{BatchSearchRequest, SearchRequest};

/// `true` for the RFC 10008 QUERY method. `http::Method` has no `QUERY`
/// constant yet, so this matches the wire token the same way
/// `Method::from_bytes(b"QUERY")` would.
fn is_query_method(method: &Method) -> bool {
    Method::from_bytes(b"QUERY").is_ok_and(|query| *method == query)
}

/// 405 for any method that reaches a QUERY-dispatch fallback without
/// actually being QUERY. Normal traffic never hits this arm — `PUT`/
/// `DELETE`/`GET`/`OPTIONS`/`HEAD` are all claimed by explicit combinators
/// ahead of the fallback — it only guards stray/unsupported methods.
fn query_method_not_allowed(allow: &'static str) -> axum::response::Response {
    axum::response::Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .header(axum::http::header::ALLOW, allow)
        .body(axum::body::Body::empty())
        .expect("static not-allowed headers are always valid")
}

/// `OPTIONS`/`HEAD` probe response shared by both QUERY targets: advertises
/// `Accept-Query: application/json` (RFC 10008 discovery) and lists the
/// target's full method set, QUERY included, in `Allow`.
fn query_probe_response(allow: &'static str) -> axum::response::Response {
    axum::response::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(axum::http::header::ALLOW, allow)
        .header("accept-query", "application/json")
        .body(axum::body::Body::empty())
        .expect("static probe headers are always valid")
}

pub(crate) async fn collection_id_query_probe() -> axum::response::Response {
    query_probe_response("PUT, DELETE, QUERY, OPTIONS, HEAD")
}

pub(crate) async fn collections_query_probe() -> axum::response::Response {
    query_probe_response("GET, QUERY, OPTIONS, HEAD")
}

/// `QUERY /collections/{collection_id}` — dual-registered twin of `POST
/// /collections/{collection_id}/search` (same [`search_core`] handler,
/// identical response for identical bodies). Content-Type is mandatory on
/// QUERY per RFC 10008; reusing [`Json`]'s own `FromRequest` for the body
/// gives that for free — missing/mismatched `Content-Type` rejects with 415,
/// byte-identical to what the POST twin already returns for the same input.
pub(crate) async fn collection_id_query_dispatch(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(collection_id): Path<String>,
    request: Request,
) -> axum::response::Response {
    if !is_query_method(request.method()) {
        return query_method_not_allowed("PUT, DELETE, QUERY, OPTIONS, HEAD");
    }
    let headers = request.headers().clone();
    match Json::<SearchRequest>::from_request(request, &state).await {
        Ok(Json(req)) => match search_core(&state, &auth, &headers, &collection_id, req).await {
            Ok(resp) => Json(resp).into_response(),
            Err(e) => e.into_response(),
        },
        Err(rejection) => rejection.into_response(),
    }
}

/// `QUERY /collections` — dual-registered twin of `POST /collections:search`
/// (same [`batch_search_core`] handler, identical response for identical
/// bodies). See [`collection_id_query_dispatch`] for the Content-Type/415
/// and interim-fallback rationale.
pub(crate) async fn collections_query_dispatch(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    request: Request,
) -> axum::response::Response {
    if !is_query_method(request.method()) {
        return query_method_not_allowed("GET, QUERY, OPTIONS, HEAD");
    }
    let headers = request.headers().clone();
    match Json::<BatchSearchRequest>::from_request(request, &state).await {
        Ok(Json(req)) => match batch_search_core(&state, &auth, &headers, req).await {
            Ok(resp) => Json(resp).into_response(),
            Err(e) => e.into_response(),
        },
        Err(rejection) => rejection.into_response(),
    }
}
