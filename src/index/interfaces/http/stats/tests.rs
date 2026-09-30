//! `/stats` is a CPU-bound Engine read (#4246): with a checkpoint interval of
//! staged Text rows resident it walks every live term, so it belongs on the
//! blocking executor with the search handlers, never on a reactor worker.

use std::sync::Arc;

use axum::extract::{Extension, Path, State};

use crate::access::application::authorization::AuthContext;
use crate::api::AppState;
use crate::index::application::engine::Engine;
use crate::index::interfaces::http::stats::stats;
use crate::shared_kernel::types::schema::CreateCollectionRequest;

fn collection_request() -> CreateCollectionRequest {
    serde_json::from_value(serde_json::json!({
        "fields": { "body": { "type": "text" } }
    }))
    .expect("valid collection request")
}

#[tokio::test]
async fn stats_runs_the_engine_read_off_the_reactor_thread() {
    let engine = Arc::new(Engine::new());
    engine
        .create_collection("c", collection_request())
        .expect("create collection");
    let state = AppState::open(engine);
    crate::index::application::engine::stats::reset_stats_thread();
    // `#[tokio::test]` is a current-thread runtime: this IS the reactor
    // worker, so an inline `Engine::stats` records exactly this thread.
    let reactor = std::thread::current().id();

    let response = stats(
        State(state),
        Extension(AuthContext::Open),
        Path("c".to_string()),
    )
    .await
    .unwrap_or_else(|_| panic!("stats responds"));

    assert_eq!(response.0.documents_indexed, 0);
    let ran_on = crate::index::application::engine::stats::last_stats_thread()
        .expect("Engine::stats must have run");
    assert_ne!(
        ran_on, reactor,
        "CPU-bound Engine::stats must run on the blocking executor, not the reactor"
    );
}
