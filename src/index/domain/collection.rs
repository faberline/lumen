//! A collection: its schema and field indexes, the interner and per-doc field
//! coverage, the request-id window that makes a write idempotent, and the
//! cached search responses a write clears. The Engine holds one per collection
//! id.

pub(crate) mod checkpoint;
pub(crate) mod coverage;
pub(crate) mod segments;
pub(crate) mod snapshot;

use std::collections::{BTreeMap, VecDeque};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::index::application::checkpoint_capture::CheckpointValue;
use crate::index::domain::fast_hash::FastHashMap;
use crate::index::domain::field_coverage::FieldCoverage;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::interner::Interner;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::schema::FieldSpec;
use crate::shared_kernel::types::search::SearchResponse;

pub(crate) const IDEMPOTENCY_TTL: Duration = Duration::from_secs(300);

const SEARCH_RESULT_CACHE_MAX: usize = 256;

#[derive(Debug)]
pub(crate) struct Collection {
    pub(crate) collection_generation: u64,
    pub(crate) data_version: u64,
    pub(crate) checkpoint_origin: Option<std::path::PathBuf>,
    pub(crate) checkpoint_lineage: Option<std::path::PathBuf>,
    pub(crate) checkpoint_lineage_schema: Option<u32>,
    pub(crate) field_dirty: BTreeMap<String, BTreeMap<String, u64>>,
    pub(crate) next_field_dirty_revision: u64,
    pub(crate) change_journal:
        crate::ingest::domain::change_journal::ChangeJournal<CheckpointValue>,
    pub(crate) requires_full_checkpoint: bool,
    /// True only while the journal describes every row since this collection
    /// was empty. A missing origin alone cannot prove this after restore or a
    /// checkpoint namespace change.
    pub(crate) journal_complete_since_empty: bool,
    pub(crate) version: u32,
    pub(crate) schema: BTreeMap<String, FieldSpec>,
    pub(crate) fields: FastHashMap<String, FieldIndex>,
    /// external_id ↔ dense u32 doc-id. Posting lists carry the u32.
    pub(crate) interner: Interner,
    /// Tracks which fields each doc-id wrote into — supports
    /// "delete all fields for this eid".
    pub(crate) eid_fields: FastHashMap<u32, FieldCoverage>,
    /// Recent request_id → timestamp; drives idempotency.
    pub(crate) seen_requests: VecDeque<(String, Instant)>,
    /// When set, the collection is soft-deleted — reads/writes return
    /// 410 Gone, and `Engine::sweep_deleted` will physically drop it
    /// once the grace window has elapsed.
    pub(crate) deleted_at: Option<Instant>,
    /// Wall-clock of the most recent successful index write. Exposed
    /// via `/stats.last_indexed_at` so callers can verify "I wrote N
    /// docs at T, did they land?" without trawling the audit log.
    pub(crate) last_indexed_at: Option<std::time::SystemTime>,
    /// Hot query results keyed by full `SearchRequest` JSON. Mutations clear it
    /// before changing postings so repeated serving queries can skip planner work
    /// without returning stale hits.
    pub(crate) search_cache: RwLock<FastHashMap<String, SearchResponse>>,
    /// #184: external-version last-write-wins. Sparse `doc-id → field → highest
    /// applied version`, populated only for cells written with an explicit
    /// `IndexItem.version`. A strictly-older versioned write is dropped at apply
    /// time. In-memory only (reconstructed by WAL replay); durability across
    /// snapshot/seal is a follow-up.
    pub(crate) cell_versions: FastHashMap<u32, FastHashMap<String, u64>>,
    /// #1292: doc-level last-write-wins for `PUT .../docs:replace`. Sparse
    /// `doc-id → highest applied doc version`, populated only for docs
    /// replaced with an explicit `ReplaceDocItem.version`. Unlike
    /// `cell_versions` (per `(doc, field)`), this is one version per doc:
    /// a strictly-older versioned replace drops the entire item. In-memory
    /// only (reconstructed by WAL replay), same as `cell_versions`.
    pub(crate) doc_versions: FastHashMap<u32, u64>,
    /// #1293: `docs:replace` no-op suppression side cache. Sparse `doc-id →
    /// field-name → FxHash content checksum` of the last value actually
    /// *written* to a `text`/`vector` field via `docs:replace` — the only
    /// two field types with no cheap forward-value accessor to compare
    /// against directly (see `Engine::replace_value_unchanged`). Populated
    /// and read only by the replace path; the `/index` merge path never
    /// touches it. In-memory only, same as `cell_versions`/`doc_versions` —
    /// a missing entry after a restart just means the next replace of that
    /// field always applies (safe: writing is never wrong, only silently
    /// skipping would be).
    pub(crate) field_checksums: FastHashMap<u32, FastHashMap<String, u64>>,
}

pub(crate) type FieldDirtySnapshot = BTreeMap<String, BTreeMap<String, u64>>;

impl Collection {
    pub(crate) fn new(schema: BTreeMap<String, FieldSpec>) -> Result<Self> {
        let mut fields = FastHashMap::default();
        for (name, spec) in &schema {
            fields.insert(name.clone(), FieldIndex::from_spec(spec)?);
        }
        Ok(Self {
            collection_generation: 0,
            data_version: 1,
            checkpoint_origin: None,
            checkpoint_lineage: None,
            checkpoint_lineage_schema: None,
            field_dirty: BTreeMap::new(),
            next_field_dirty_revision: 0,
            change_journal: crate::ingest::domain::change_journal::ChangeJournal::new(),
            requires_full_checkpoint: false,
            journal_complete_since_empty: true,
            version: 1,
            schema,
            fields,
            interner: Interner::default(),
            eid_fields: FastHashMap::default(),
            seen_requests: VecDeque::new(),
            deleted_at: None,
            last_indexed_at: None,
            search_cache: RwLock::new(FastHashMap::default()),
            cell_versions: FastHashMap::default(),
            doc_versions: FastHashMap::default(),
            field_checksums: FastHashMap::default(),
        })
    }

    /// Reclaim at most `document_budget` detached documents. This method is
    /// called only after the collection has left `EngineState`, so it cannot
    /// change API-visible data. It deliberately works from the end of the
    /// interner: popping the owned external id lets vector indexes release
    /// their string-keyed state without cloning every id first.
    ///
    /// The bound is on external-id items only. A value can contain many text
    /// tokens or set members, and the collection's residual maps and segment
    /// handles still need a final non-incremental destructor.
    pub(crate) fn retire_document_batch(&mut self, document_budget: usize) -> usize {
        debug_assert!(document_budget > 0);
        let mut retired_documents = 0;
        for _ in 0..document_budget {
            let Some((id, external_id)) = self.interner.take_last_for_retirement() else {
                break;
            };
            let fields = self
                .eid_fields
                .remove(&id)
                .map(|coverage| coverage.names)
                .unwrap_or_default();
            for field in fields {
                if let Some(index) = self.fields.get_mut(&field) {
                    index.drop_eid(id, &external_id);
                }
            }
            self.cell_versions.remove(&id);
            self.doc_versions.remove(&id);
            self.field_checksums.remove(&id);
            retired_documents += 1;
        }
        retired_documents
    }

    pub(crate) fn check_live(&self, collection_id: &str) -> Result<()> {
        if self.deleted_at.is_some() {
            return Err(StorageError::Gone(collection_id.to_string()).into());
        }
        Ok(())
    }

    pub(crate) fn fields_count(&self) -> u32 {
        self.schema.len() as u32
    }

    pub(crate) fn check_request_id(&mut self, request_id: Option<&str>) -> bool {
        // Returns true if the request should be skipped (duplicate).
        self.gc_requests();
        let Some(id) = request_id else {
            return false;
        };
        if self.seen_requests.iter().any(|(k, _)| k == id) {
            return true;
        }
        self.seen_requests
            .push_back((id.to_string(), Instant::now()));
        false
    }

    pub(crate) fn gc_requests(&mut self) {
        let now = Instant::now();
        while let Some((_, t)) = self.seen_requests.front() {
            if now.duration_since(*t) > IDEMPOTENCY_TTL {
                self.seen_requests.pop_front();
            } else {
                break;
            }
        }
    }

    pub(crate) fn clear_number_filter_caches(&mut self) {
        for fi in self.fields.values_mut() {
            if let FieldIndex::Number(n) = fi {
                n.clear_keyword_range_cache();
            }
        }
    }

    pub(crate) fn clear_search_cache(&mut self) {
        // Conservative invalidation precedes mutation. Even a partially failed
        // batch must never reuse its earlier immutable checkpoint bytes.
        self.data_version = self.data_version.saturating_add(1);
        self.checkpoint_origin = None;
        if let Ok(mut cache) = self.search_cache.write() {
            cache.clear();
        }
    }

    pub(crate) fn cached_search_response(&self, key: &str) -> Option<SearchResponse> {
        self.search_cache
            .read()
            .ok()
            .and_then(|cache| cache.get(key).cloned())
    }

    pub(crate) fn cache_search_response(&self, key: String, response: &SearchResponse) {
        let Ok(mut cache) = self.search_cache.write() else {
            return;
        };
        if cache.len() >= SEARCH_RESULT_CACHE_MAX && !cache.contains_key(&key) {
            cache.clear();
        }
        cache.insert(key, response.clone());
    }

    pub(crate) fn clear_text_rank_caches(&self) {
        for fi in self.fields.values() {
            if let FieldIndex::Text { idx, .. } = fi {
                idx.clear_match_rank_cache();
            }
        }
    }
}
