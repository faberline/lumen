//! Pricing a log entry before admission: a borrowed view of the Engine's
//! schemas and document coverage for the change-record cost, and the default
//! n-gram workspace an exact price needs.

use std::time::Instant;

use crate::index::application::engine::Engine;
use crate::index::domain::collection::IDEMPOTENCY_TTL;
use crate::index::domain::engine_state::EngineState;
use crate::shared_kernel::types::schema::FieldSpec;

/// Borrowed state view for pending-change cost normalization.  It only exposes
/// metadata already held by the engine; it never expands postings or clones a
/// request before admission.
pub(in crate::index::application) struct EngineCostContext<'a> {
    pub(in crate::index::application) state: &'a EngineState,
}

impl crate::ingest::domain::change_record_cost::CostContext for EngineCostContext<'_> {
    fn collection_exists(&self, collection_id: &str) -> bool {
        self.state.collections.contains_key(collection_id)
    }

    fn index_cell_is_stale(
        &self,
        collection_id: &str,
        external_id: &str,
        field: &str,
        version: Option<u64>,
    ) -> bool {
        let Some(version) = version else {
            return false;
        };
        self.state
            .collections
            .get(collection_id)
            .and_then(|collection| {
                collection
                    .interner
                    .id(external_id)
                    .map(|id| (collection, id))
            })
            .and_then(|(collection, id)| collection.cell_versions.get(&id))
            .and_then(|versions| versions.get(field))
            .is_some_and(|stored| *stored >= version)
    }

    fn field_spec<'a>(&'a self, collection_id: &str, field: &str) -> Option<&'a FieldSpec> {
        self.state.collections.get(collection_id)?.schema.get(field)
    }

    fn known_external_id(&self, collection_id: &str, external_id: &str) -> bool {
        self.state
            .collections
            .get(collection_id)
            .and_then(|collection| collection.interner.id(external_id))
            .is_some()
    }

    fn visit_coverage(&self, collection_id: &str, external_id: &str, visit: &mut dyn FnMut(&str)) {
        let Some(collection) = self.state.collections.get(collection_id) else {
            return;
        };
        let Some(id) = collection.interner.id(external_id) else {
            return;
        };
        let Some(coverage) = collection.eid_fields.get(&id) else {
            return;
        };
        for field in coverage.iter() {
            visit(field);
        }
    }

    fn request_is_deduplicated(&self, collection_id: &str, request_id: Option<&str>) -> bool {
        let Some(request_id) = request_id else {
            return false;
        };
        let Some(collection) = self.state.collections.get(collection_id) else {
            return false;
        };
        let now = Instant::now();
        collection.seen_requests.iter().any(|(known, seen)| {
            known == request_id && now.duration_since(*seen) <= IDEMPOTENCY_TTL
        })
    }
}

impl Engine {
    /// Estimate the additional owned pending work for one log entry using a
    /// borrowed view of the current schema and document coverage.  This is a
    /// preflight helper only.  A `Retain(ContextMissing)` result is not an API
    /// error: admission must let the ordered apply owner run the existing
    /// validation/no-op behavior rather than wait forever on an unknown field
    /// or collection.
    pub(crate) fn estimate_record_cost(
        &self,
        entry: &crate::shared_kernel::log_entry::RaftLogEntry,
    ) -> crate::ingest::domain::change_record_cost::RecordEstimate {
        let Ok(state) = self.state.read() else {
            return crate::ingest::domain::change_record_cost::RecordEstimate::Retain {
                cause: crate::ingest::domain::change_record_cost::text_upper_bound::NormalizeError::ContextMissing,
            };
        };
        crate::ingest::domain::change_record_cost::estimate_record_or_retain(
            entry,
            &EngineCostContext { state: &state },
        )
    }

    /// Allocation-free selection before reserving the exact cost workspace.
    pub(crate) fn may_need_default_ngram_workspace(
        &self,
        entry: &crate::shared_kernel::log_entry::RaftLogEntry,
    ) -> bool {
        let Ok(state) = self.state.read() else {
            return false;
        };
        crate::ingest::domain::change_record_cost::may_need_default_ngram_workspace(
            entry,
            &EngineCostContext { state: &state },
        )
    }

    /// The admission owner reserves the fixed Ngram table before this call.
    pub(crate) fn estimate_record_exact_default_ngram_cost(
        &self,
        entry: &crate::shared_kernel::log_entry::RaftLogEntry,
    ) -> crate::ingest::domain::change_record_cost::RecordEstimate {
        let Ok(state) = self.state.read() else {
            return crate::ingest::domain::change_record_cost::RecordEstimate::Retain {
                cause: crate::ingest::domain::change_record_cost::text_upper_bound::NormalizeError::ContextMissing,
            };
        };
        crate::ingest::domain::change_record_cost::estimate_record_exact_default_ngram(
            entry,
            &EngineCostContext { state: &state },
        )
    }
}
