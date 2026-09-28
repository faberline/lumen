//! The log seam: `WalLog`, which writes publish to and the apply loop tails,
//! and the streams a subscription hands out.

use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::{Stream, StreamExt};

use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::delivery::{WalDelivery, WalSourceRelease};

/// A live, ordered subscription: `(seq, record)` pairs with strictly
/// increasing `seq`, delivered as they become available. Never
/// completes on its own (it tails the log) unless the backend closes.
pub type WalStream = Pin<Box<dyn Stream<Item = Result<(u64, WalRecord)>> + Send>>;
pub type WalAdmissionStream = Pin<Box<dyn Stream<Item = Result<(u64, WalDelivery)>> + Send>>;

/// The log seam. `publish` appends and returns the assigned global
/// sequence; `subscribe` tails from a sequence; `latest_seq` reports the
/// head. Object-safe so it can live behind `Arc<dyn WalLog>`.
#[async_trait]
pub trait WalLog: Send + Sync {
    /// Append `record`, returning the global sequence assigned to it.
    async fn publish(&self, record: WalRecord) -> Result<u64>;

    /// Tail every record with `seq > from_seq` (use `0` for "from the
    /// beginning"), in order, including future appends.
    async fn subscribe(&self, from_seq: u64) -> Result<WalStream>;

    async fn subscribe_admitted(&self, from_seq: u64) -> Result<WalAdmissionStream> {
        Ok(Box::pin(self.subscribe(from_seq).await?.map(|item| {
            item.map(|(seq, record)| (seq, WalDelivery::Resident(record)))
        })))
    }

    async fn stage_source(&self, _: u64) -> Result<Option<WalSourceRelease>> {
        Ok(None)
    }

    /// Highest sequence currently in the log (`0` if empty).
    async fn latest_seq(&self) -> Result<u64>;
}

pub type SharedWal = Arc<dyn WalLog>;
