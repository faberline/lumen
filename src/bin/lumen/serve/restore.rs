//! The restore sink a serving node installs for its persistence mode.

use std::sync::Arc;

use anyhow::Result;
use lumen::storage::Engine;

use crate::cli::serve::WalBackend;

pub(super) fn segment_restore_sink(
    segment_mode: bool,
    backend: WalBackend,
    engine: Arc<Engine>,
    store: Option<Arc<lumen::segment_rdb::SegmentRdbStore>>,
    writer: Arc<dyn lumen::coordinator::WriteSink>,
    aof: Option<lumen::coordinator::SharedAof>,
) -> Result<Option<Arc<dyn lumen::api::RestoreSink>>> {
    if !segment_mode {
        return Ok(None);
    }

    const UNAVAILABLE_REASON: &str =
        "durable segment restore requires wal=embedded, a configured data directory, and the local AOF";
    if backend != WalBackend::Embedded {
        return Ok(Some(Arc::new(
            lumen::segment_restore::UnavailableRestoreSink::new(UNAVAILABLE_REASON),
        )));
    }

    match (store, aof) {
        (Some(store), Some(aof)) => Ok(Some(Arc::new(
            lumen::segment_restore::SegmentRestoreSink::new(engine, store, writer, aof)?,
        ))),
        _ => Ok(Some(Arc::new(
            lumen::segment_restore::UnavailableRestoreSink::new(UNAVAILABLE_REASON),
        ))),
    }
}

#[cfg(test)]
mod tests;
