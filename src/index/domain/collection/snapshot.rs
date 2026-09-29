//! A collection to and from its CBOR snapshot form, with each doc id resolved
//! to its external id so the snapshot does not depend on the interner.

use std::collections::{BTreeMap, VecDeque};
use std::sync::RwLock;

use anyhow::Result;

use crate::index::domain::collection::Collection;
use crate::index::domain::fast_hash::FastHashMap;
use crate::index::domain::field_coverage::FieldCoverage;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::interner::Interner;
use crate::storage::{CollectionSnapshot, FieldIndexSnapshot};

impl Collection {
    pub(crate) fn to_snapshot(&self) -> Result<CollectionSnapshot> {
        // Which ids carry which field, in ONE walk of the coverage map.
        // `eid_fields` is the document census — `documents_indexed` counts it,
        // and a full delete removes the entry — so an id absent from it names
        // no live document and every per-field accessor answers `None` for it.
        // The forward gathers below used to sweep `0..interner.to_eid.len()`
        // and probe every id for every field: on a sealed field each probe
        // decodes out of the segment, so a ten-field collection paid ten
        // segment decodes per interned id, including for every id whose
        // document had since been deleted.
        let mut ids_by_field: BTreeMap<&str, Vec<u32>> = self
            .fields
            .keys()
            .map(|name| (name.as_str(), Vec::new()))
            .collect();
        for (id, cov) in &self.eid_fields {
            for name in cov.iter() {
                if let Some(ids) = ids_by_field.get_mut(name.as_str()) {
                    ids.push(*id);
                }
            }
        }
        let mut fields: BTreeMap<String, FieldIndexSnapshot> = BTreeMap::new();
        for (name, fi) in &self.fields {
            let field_ids = ids_by_field
                .get(name.as_str())
                .map_or(&[][..], Vec::as_slice);
            fields.insert(name.clone(), fi.to_snapshot(&self.interner, field_ids)?);
        }
        // The on-disk snapshot is String-keyed: resolve the dense doc-ids out
        // so the format is interner-independent and the e2e round-trips.
        let eid_fields = self
            .eid_fields
            .iter()
            .map(|(id, set)| (self.interner.resolve(*id).to_string(), set.to_btree_set()))
            .collect();
        Ok(CollectionSnapshot {
            schema: self.schema.clone(),
            version: self.version,
            eid_fields,
            fields,
        })
    }

    pub(crate) fn from_snapshot(snap: CollectionSnapshot) -> Result<Self> {
        // Re-intern every external_id (eid_fields covers all indexed docs) so
        // the field postings below resolve to the same dense ids.
        let mut interner = Interner::default();
        let mut eid_fields: FastHashMap<u32, FieldCoverage> = FastHashMap::default();
        for (eid, set) in snap.eid_fields {
            let id = interner.intern(&eid);
            eid_fields.insert(id, FieldCoverage::from_btree_set(set));
        }
        let mut fields: FastHashMap<String, FieldIndex> = FastHashMap::default();
        for (name, fi_snap) in snap.fields {
            fields.insert(name, FieldIndex::from_snapshot(fi_snap, &mut interner)?);
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
            journal_complete_since_empty: false,
            version: snap.version,
            schema: snap.schema,
            fields,
            interner,
            eid_fields,
            seen_requests: VecDeque::new(),
            deleted_at: None,
            last_indexed_at: None,
            search_cache: RwLock::new(FastHashMap::default()),
            cell_versions: FastHashMap::default(),
            doc_versions: FastHashMap::default(),
            field_checksums: FastHashMap::default(),
        })
    }
}
