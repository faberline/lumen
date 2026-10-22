//! The Raft state-machine boundary: one committed log entry applied to the
//! Engine, with the outcome its write op returns.

use anyhow::Result;

use crate::index::application::engine::collections::DropOutcome;
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{IndexResponse, ReplaceDocsResponse};
use crate::shared_kernel::types::schema::CreateCollectionResponse;

impl Engine {
    /// Apply a single committed Raft log entry to the engine.
    ///
    /// This is the state-machine boundary used by `raft_node.rs`: every
    /// write op that goes through Raft consensus arrives here once it has
    /// been committed, and is dispatched to the same internal method the
    /// HTTP API layer would call. Read ops never flow through this path.
    ///
    /// Errors from the underlying methods are surfaced unchanged so the
    /// caller (the Raft state-machine impl) can log/translate them. The
    /// engine's RwLock is taken per call, identical to a direct API call.
    pub(in crate::index::application) fn dispatch_raft_entry(
        &self,
        entry: crate::shared_kernel::log_entry::RaftLogEntry,
        charge: Option<&crate::ingest::domain::change_budget::RetainedCharge>,
        prepared_text: Option<&crate::index::application::text_preparation::PreparedTextRows>,
    ) -> Result<ApplyOutcome> {
        let _apply = self.capture_barrier.apply();
        use crate::shared_kernel::log_entry::RaftLogEntry;
        Ok(match entry {
            RaftLogEntry::CreateCollection { collection_id, req } => {
                ApplyOutcome::Created(self.create_collection_inner(&collection_id, req)?)
            }
            RaftLogEntry::Index { collection_id, req } => ApplyOutcome::Indexed(self.index_inner(
                &collection_id,
                req,
                charge,
                prepared_text,
            )?),
            RaftLogEntry::ReplaceDocs { collection_id, req } => ApplyOutcome::Replaced(
                self.replace_docs_inner(&collection_id, req, charge, prepared_text)?,
            ),
            RaftLogEntry::TruncateDocs { collection_id } => {
                self.truncate_docs_inner(&collection_id)?;
                ApplyOutcome::DocsTruncated
            }
            RaftLogEntry::UnindexDocs { collection_id, req } => {
                self.unindex_docs_inner(&collection_id, req, charge)?;
                ApplyOutcome::DocsUnindexed
            }
            RaftLogEntry::Delete {
                collection_id,
                external_id,
                field,
            } => {
                self.delete_inner(&collection_id, &external_id, field.as_deref(), charge)?;
                ApplyOutcome::Deleted
            }
            RaftLogEntry::DropCollection {
                collection_id,
                force,
            } => ApplyOutcome::Dropped(self.drop_collection_inner(&collection_id, force)?),
            RaftLogEntry::AddField {
                collection_id,
                field_name,
                spec,
            } => ApplyOutcome::FieldChanged(self.add_field_inner(
                &collection_id,
                &field_name,
                spec,
            )?),
            RaftLogEntry::DropField {
                collection_id,
                field_name,
            } => ApplyOutcome::FieldChanged(self.drop_field_inner(&collection_id, &field_name)?),
        })
    }
}

/// Result of applying one mutation — routed from the apply loop back to
/// the waiting write handler (by sequence) so the HTTP response keeps
/// its rich shape even though apply happens in the subscribe layer.
#[derive(Debug, Clone)]
pub enum ApplyOutcome {
    Created(CreateCollectionResponse),
    Indexed(IndexResponse),
    Replaced(ReplaceDocsResponse),
    DocsTruncated,
    DocsUnindexed,
    Deleted,
    Dropped(DropOutcome),
    /// New collection version after add-field / drop-field.
    FieldChanged(u32),
}
