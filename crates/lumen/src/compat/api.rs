//! `crate::api` before the DDD split. Re-exports its public items from their
//! new homes so callers outside the crate keep compiling.

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
