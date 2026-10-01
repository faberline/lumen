//! The snapshot document's wire types: the versioned, serde-serialisable image
//! of each collection's schema, document coverage and field indexes that the
//! RDB file, `/admin/restore`, a reshard delta and a Raft catch-up carry, and
//! the reindex audit asked of a document instead of a restored engine.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::index::domain::collection::coverage::{
    FieldAudit, FieldNotAudited, ReindexNeeded, TEXT_UNAUDITABLE,
};
use crate::index::domain::vector::quantize::ScalarCodebook;
use crate::shared_kernel::types::schema::{Analyzer, FieldSpec, VectorSpec};

// ---------------------------------------------------------------------------
// Snapshot wire types
// ---------------------------------------------------------------------------

/// The format this build WRITES. Readers accept `1..=SNAPSHOT_VERSION`; see
/// [`SnapshotV1::version`].
///
/// 2 dropped the CONTENTS of the `terms` / `elements` inverted maps from the
/// Keyword and Set arms. Reading forward is unaffected — a format-1 document's
/// populated map is dropped on arrival and `forward` restores the field, which
/// `tests/it/snapshot_ships_only_the_forward_column.rs` pins.
///
/// Reading BACKWARD needed care, because 0.4.29 is released and a version gate
/// does not work the way it looks like it does. `version` is a field of the
/// same struct being deserialised; there is no point at which it is read
/// first. So an 0.4.29 build, whose `terms` is a required field with no
/// `#[serde(default)]`, would fail inside serde on the missing key before its
/// own `!= 1` check ever ran — reporting a missing field on a file that is
/// perfectly intact. The sharp case is a ROLLBACK: `rdb.rs` writes a
/// format-2 snapshot to the data directory in CBOR, the operator rolls the
/// node back to 0.4.29 mid-incident, and 0.4.29 fails to decode its own data
/// directory with a message that reads like corruption.
///
/// [`LegacyInvertedIndex`] is why that does not happen: the two keys stay on
/// the wire as empty maps, so a released 0.4.29 parses the document and
/// refuses it by version, in its own words, with the remedy (upgrade the
/// binary) named. The payload saving is unchanged — what cost bytes was the
/// dictionary, not the key.
///
/// The same applies to every other boundary these documents cross:
/// `/admin/restore`, a reshard delta between shards, and a Raft catch-up into
/// a peer that has not been upgraded yet. All of them now refuse by version.
pub(in crate::index) const SNAPSHOT_VERSION: u32 = 2;

/// Top-level snapshot document. JSON-serialisable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotV1 {
    /// Format version. Bump when the wire layout changes
    /// incompatibly so old snapshots can be detected at restore.
    pub version: u32,
    pub collections: BTreeMap<String, CollectionSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionSnapshot {
    pub schema: BTreeMap<String, FieldSpec>,
    pub version: u32,
    pub eid_fields: HashMap<String, BTreeSet<String>>,
    pub fields: BTreeMap<String, FieldIndexSnapshot>,
}

impl SnapshotV1 {
    /// The reindex audit, asked of the DOCUMENT instead of a restored engine.
    ///
    /// This must answer exactly what [`Engine::reindex_needed`] answers for the
    /// same bytes, and `tests/it/reopen_names_the_fields_that_need_reindexing.rs`
    /// runs both over one document and requires the same rows — the differential
    /// is what keeps the two from drifting.
    ///
    /// It exists because the audience is an operator holding a backup file,
    /// deciding whether to import it at all. Restoring into a throwaway
    /// `Engine` to ask would materialise every interner, roaring bitmap and
    /// forward map of the entire backup in RAM — a multiple of the file size,
    /// on the machine that most needs the answer — while every fact the audit
    /// reads is already a plain field of the parsed document.
    ///
    /// [`Engine::reindex_needed`]: crate::index::application::engine::Engine::reindex_needed
    pub fn reindex_needed(&self) -> Vec<ReindexNeeded> {
        struct Tally<'a> {
            field: &'a FieldIndexSnapshot,
            covered: u64,
            holds: bool,
            probe: bool,
        }
        let mut out = Vec::new();
        // `collections` and `fields` are both `BTreeMap`, so the rows come out
        // ordered by collection then field with no sort — the same order
        // `Engine::reindex_needed` sorts into.
        for (collection, coll) in &self.collections {
            let mut tally: BTreeMap<&str, Tally<'_>> = coll
                .fields
                .iter()
                .filter_map(|(name, field)| {
                    let (holds, probe) = match field.audit_kind() {
                        FieldAudit::PerId => (false, true),
                        FieldAudit::WholeIndex(populated) => (populated, false),
                        FieldAudit::Unauditable(_) => return None,
                    };
                    Some((
                        name.as_str(),
                        Tally {
                            field,
                            covered: 0,
                            holds,
                            probe,
                        },
                    ))
                })
                .collect();
            for (eid, cov) in &coll.eid_fields {
                for name in cov {
                    if let Some(t) = tally.get_mut(name.as_str()) {
                        t.covered += 1;
                        if t.probe && !t.holds {
                            t.holds = t.field.holds(eid);
                        }
                    }
                }
            }
            out.extend(
                tally
                    .into_iter()
                    .filter(|(_, t)| t.covered > 0 && !t.holds)
                    .map(|(field, t)| ReindexNeeded {
                        collection: collection.clone(),
                        field: field.to_string(),
                        documents_covered: t.covered,
                    }),
            );
        }
        out
    }

    /// Every field [`SnapshotV1::reindex_needed`] did not examine. See
    /// [`FieldNotAudited`].
    pub fn fields_not_audited(&self) -> Vec<FieldNotAudited> {
        self.collections
            .iter()
            .flat_map(|(collection, coll)| {
                coll.fields
                    .iter()
                    .filter_map(move |(field, index)| match index.audit_kind() {
                        FieldAudit::Unauditable(reason) => Some(FieldNotAudited {
                            collection: collection.clone(),
                            field: field.clone(),
                            reason: reason.to_string(),
                        }),
                        _ => None,
                    })
            })
            .collect()
    }

    /// Per collection, how many live documents this document's census carries —
    /// the same figure `stats` reports as `documents_indexed`. It travels with
    /// the verdict so "nothing is damaged" reads differently from "nothing was
    /// read".
    pub fn documents_scanned(&self) -> BTreeMap<String, u64> {
        self.collections
            .iter()
            .map(|(id, coll)| (id.clone(), coll.eid_fields.len() as u64))
            .collect()
    }
}

/// A field that exists only so a format-1 READER can parse a format-2
/// document and reach its own version check.
///
/// 0.4.29 is released. Its `FieldIndexSnapshot::Keyword` requires a `terms`
/// key and its `Set` requires `elements`, neither carrying `#[serde(default)]`
/// — so a 0.4.29 build fails inside serde on the missing key BEFORE
/// `Engine::restore` ever compares versions. The operator then reads
/// `missing field \`terms\`` (or, from `rdb.rs`'s CBOR, something less legible
/// still) off a file that is not damaged, on a rollback where the only thing
/// wrong is that the binary is too old to say so. Emitting `{}` here costs two
/// bytes per field and hands 0.4.29 back the version error it already knows
/// how to print.
///
/// It is written and never read: `skip_deserializing` drops a format-1
/// document's populated map on arrival, which is what
/// `FieldIndexSnapshot::from_snapshot` wants anyway — the inverted index is
/// rebuilt from `forward` in both formats, so the map on the wire never had a
/// reader.
///
/// Delete this, and bump the format again, once no supported release still
/// requires the key. `tests/it/snapshot_ships_only_the_forward_column.rs` pins that
/// it stays empty; nothing else may put a value in it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LegacyInvertedIndex;

impl Serialize for LegacyInvertedIndex {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        serializer.serialize_map(Some(0))?.end()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FieldIndexSnapshot {
    Text {
        analyzer: Analyzer,
        tokens: BTreeMap<String, BTreeMap<String, u32>>,
        forward: HashMap<String, (BTreeSet<String>, u32)>,
        doc_count: u64,
        total_doc_len: u64,
        bytes: u64,
    },
    /// Only the forward column travels. `from_snapshot` rebuilds `terms` from
    /// it — it has to, because a snapshot whose persisted inverted index
    /// disagreed with its own forward column would otherwise restore into an
    /// index that answers queries no document satisfies. Once the reader
    /// derives one representation from the other, serialising both is writing
    /// a value nobody reads: a `terms` field on the wire is decoded out of the
    /// segment at snapshot time, written to disk, shipped to a Raft follower
    /// mid-catch-up, and dropped on arrival.
    ///
    /// A format-1 document still carries a populated one; `terms` below drops
    /// it on arrival, and `forward` — always complete, even there — is what
    /// restores. The key itself stays on the wire, empty, so a released 0.4.29
    /// can still parse this document and refuse it by version rather than by
    /// serde; see [`LegacyInvertedIndex`].
    Keyword {
        #[serde(default, skip_deserializing)]
        terms: LegacyInvertedIndex,
        forward: HashMap<String, String>,
        bytes: u64,
    },
    Number {
        /// Stored as `f64` on the wire; `SortableF64` is re-derived on
        /// restore.
        forward: HashMap<String, f64>,
        bytes: u64,
    },
    /// Forward column only, for the reason the Keyword arm above states, with
    /// the same empty [`LegacyInvertedIndex`] under the key 0.4.29 requires.
    Set {
        #[serde(default, skip_deserializing)]
        elements: LegacyInvertedIndex,
        forward: HashMap<String, BTreeSet<String>>,
        bytes: u64,
    },
    /// Vector snapshot.
    ///
    /// HNSW graphs are not serialized directly — on restore the
    /// vectors are bulk-reinserted into a fresh graph, which is fast
    /// enough (millions per second on CPU) and avoids tying us to the
    /// upstream graph format. The codebook is carried verbatim when
    /// SQ is enabled so decoding reproduces the exact same f32 values
    /// that were originally indexed.
    Vector {
        spec: VectorSpec,
        vectors: Vec<(String, Vec<f32>)>,
        codebook: Option<ScalarCodebook>,
        bytes: u64,
    },
    Hash {
        /// external_id → 64-bit hash.
        forward: HashMap<String, u64>,
        bytes: u64,
    },
}

impl FieldIndexSnapshot {
    /// What the reindex audit can learn about this arm, in the same three
    /// shapes [`FieldIndex::audit_kind`] answers in. The two must agree arm for
    /// arm: `SnapshotV1::reindex_needed` and `Engine::reindex_needed` are one
    /// verdict asked of the document and of the restored engine.
    ///
    /// [`FieldIndex::audit_kind`]: crate::index::domain::field_index::FieldIndex::audit_kind
    fn audit_kind(&self) -> FieldAudit {
        match self {
            FieldIndexSnapshot::Text { .. } => FieldAudit::Unauditable(TEXT_UNAUDITABLE),
            FieldIndexSnapshot::Keyword { .. }
            | FieldIndexSnapshot::Number { .. }
            | FieldIndexSnapshot::Set { .. }
            | FieldIndexSnapshot::Hash { .. } => FieldAudit::PerId,
            FieldIndexSnapshot::Vector { vectors, .. } => {
                FieldAudit::WholeIndex(!vectors.is_empty())
            }
        }
    }

    /// Whether the forward column carries `eid`.
    ///
    /// This is the document-side twin of [`FieldIndex::holds`], and it reads the
    /// exact column `from_snapshot` restores the index from — so "the document
    /// holds it" and "the restored index answers for it" cannot come apart
    /// without a `from_snapshot` bug, which is a different failure from the one
    /// this audit is looking for.
    ///
    /// [`FieldIndex::holds`]: crate::index::domain::field_index::FieldIndex::holds
    fn holds(&self, eid: &str) -> bool {
        match self {
            FieldIndexSnapshot::Keyword { forward, .. } => forward.contains_key(eid),
            FieldIndexSnapshot::Number { forward, .. } => forward.contains_key(eid),
            FieldIndexSnapshot::Set { forward, .. } => forward.contains_key(eid),
            FieldIndexSnapshot::Hash { forward, .. } => forward.contains_key(eid),
            FieldIndexSnapshot::Text { .. } | FieldIndexSnapshot::Vector { .. } => true,
        }
    }
}
