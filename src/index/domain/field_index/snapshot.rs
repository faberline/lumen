//! A field index to and from its CBOR snapshot form: the forward values
//! gathered by external id, and the index rebuilt from them against the
//! collection's interner.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Mutex, RwLock};

use anyhow::{anyhow, Result};
use roaring::RoaringBitmap;

use crate::index::domain::fast_hash::{FastHashMap, FastHashSet};
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::hash_index::HashIndex;
use crate::index::domain::interner::Interner;
use crate::index::domain::keyword_index::{dup_values_of, KeywordIndex};
use crate::index::domain::number_index::NumberIndex;
use crate::index::domain::postings::Postings;
use crate::index::domain::set_index::SetIndex;
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::domain::text_index::TextIndex;
use crate::index::domain::token_set::TokenSet;
use crate::index::domain::vector::flat_cpu_index::FlatCpuIndex;
use crate::index::domain::vector::hnsw_cpu_index::HnswCpuIndex;
use crate::index::domain::vector::VectorIndex;
use crate::storage::{FieldIndexSnapshot, LegacyInvertedIndex};

impl FieldIndex {
    /// `field_ids` are the ids the collection's coverage map says carry THIS
    /// field — the only ids a forward gather can find anything under.
    pub(crate) fn to_snapshot(
        &self,
        interner: &Interner,
        field_ids: &[u32],
    ) -> Result<FieldIndexSnapshot> {
        let eid = |id: u32| interner.resolve(id).to_string();
        Ok(match self {
            FieldIndex::Text { analyzer, idx } => {
                // Snapshot the same composed source used by queries: sealed
                // base postings minus tombstones, union live overlays.
                // `field_ids` as a membership set: the posting filter below asks
                // "does this id carry the field" once per posting entry, which
                // is a lookup rather than the sweep the forward gathers do.
                let field_live: FastHashSet<u32> = field_ids.iter().copied().collect();
                let mut token_names = BTreeSet::new();
                if let Some(seg) = &idx.segment {
                    let entries = seg
                        .text_tokens_all()
                        .ok_or_else(|| anyhow!("text segment postings torn on snapshot"))?;
                    token_names.extend(entries.into_iter().map(|(tok, _, _)| tok));
                }
                token_names.extend(idx.tokens.keys().cloned());
                for row in idx.staged_rows.values() {
                    let reader = row.reader();
                    let count = reader
                        .keyword_ordinal_count()
                        .ok_or_else(|| anyhow!("staged dictionary missing"))?;
                    for ordinal in 0..count {
                        token_names.insert(
                            reader
                                .keyword_term_at_ordinal(ordinal)
                                .ok_or_else(|| anyhow!("staged dictionary torn"))?,
                        );
                    }
                }
                let active: BTreeMap<String, (Vec<u32>, Vec<u32>)> = token_names
                    .into_iter()
                    .filter_map(|tok| {
                        let postings = idx.tok_postings(&tok)?;
                        let mut docids = Vec::new();
                        let mut tfs = Vec::new();
                        for (&id, &tf) in postings.docids().iter().zip(postings.tfs()) {
                            if field_live.contains(&id) {
                                docids.push(id);
                                tfs.push(tf);
                            }
                        }
                        (!docids.is_empty()).then_some((tok, (docids, tfs)))
                    })
                    .collect();
                let tokens = active
                    .iter()
                    .map(|(tok, (docids, tfs))| {
                        (
                            tok.clone(),
                            docids
                                .iter()
                                .zip(tfs)
                                .map(|(id, tf)| (eid(*id), *tf))
                                .collect(),
                        )
                    })
                    .collect();
                let mut forward: HashMap<String, (BTreeSet<String>, u32)> = HashMap::new();
                for (tok, (docids, _)) in &active {
                    for &id in docids {
                        forward
                            .entry(eid(id))
                            .or_insert_with(|| (BTreeSet::new(), idx.doc_len(id)))
                            .0
                            .insert(tok.clone());
                    }
                }
                // Preserve explicit empty values. The collection coverage map is
                // the authority for field presence when a sealed DocLen is zero.
                for &id in field_ids {
                    forward
                        .entry(eid(id))
                        .or_insert_with(|| (BTreeSet::new(), idx.doc_len(id)))
                        .1 = idx.doc_len(id);
                }
                FieldIndexSnapshot::Text {
                    analyzer: *analyzer,
                    tokens,
                    forward,
                    doc_count: idx.doc_count,
                    total_doc_len: idx.total_doc_len,
                    bytes: idx.bytes,
                }
            }
            // The three forward-only arms below read the same segment-aware
            // accessors they always did — after a seal the raw fields hold only
            // the post-seal tail — but over the ids that carry the field rather
            // than over every id the interner has ever issued.
            FieldIndex::Keyword(k) => FieldIndexSnapshot::Keyword {
                terms: LegacyInvertedIndex,
                forward: field_ids
                    .iter()
                    .filter_map(|&id| k.keyword_at(id).map(|value| (eid(id), value)))
                    .collect(),
                bytes: k.bytes,
            },
            FieldIndex::Number(n) => FieldIndexSnapshot::Number {
                forward: field_ids
                    .iter()
                    .filter_map(|&id| n.live_number_at(id).map(|key| (eid(id), key.to_f64())))
                    .collect(),
                bytes: n.bytes,
            },
            FieldIndex::Set(s) => FieldIndexSnapshot::Set {
                elements: LegacyInvertedIndex,
                forward: field_ids
                    .iter()
                    .filter_map(|&id| s.live_set_members(id).map(|members| (eid(id), members)))
                    .collect(),
                bytes: s.bytes,
            },
            FieldIndex::Vector { spec, idx, bytes } => {
                let (vectors, codebook) = idx.dump_for_snapshot()?;
                FieldIndexSnapshot::Vector {
                    spec: *spec,
                    vectors,
                    codebook,
                    bytes: *bytes,
                }
            }
            FieldIndex::Hash(h) => FieldIndexSnapshot::Hash {
                forward: field_ids
                    .iter()
                    .filter_map(|&id| h.hash_at(id).map(|value| (eid(id), value)))
                    .collect(),
                bytes: h.bytes,
            },
        })
    }
}

impl FieldIndex {
    pub(crate) fn from_snapshot(snap: FieldIndexSnapshot, interner: &mut Interner) -> Result<Self> {
        Ok(match snap {
            FieldIndexSnapshot::Text {
                analyzer,
                tokens,
                forward,
                doc_count,
                total_doc_len,
                bytes,
            } => {
                let mut t: BTreeMap<String, Postings> = BTreeMap::new();
                for (tok, m) in tokens {
                    // Re-intern (assigns fresh ids in arbitrary order), then sort
                    // by docid so the flat postings are ascending.
                    let mut pairs: Vec<(u32, u32)> = m
                        .into_iter()
                        .map(|(eid, tf)| (interner.intern(&eid), tf))
                        .collect();
                    pairs.sort_unstable_by_key(|(id, _)| *id);
                    let mut p = Postings::default();
                    for (id, tf) in pairs {
                        p.docids.push(id);
                        p.tfs.push(tf);
                    }
                    t.insert(tok, p);
                }
                let mut lens: Vec<u32> = Vec::new();
                let mut distinct: Vec<Option<TokenSet>> = Vec::new();
                for (eid, (set, doc_len)) in forward {
                    let id = interner.intern(&eid);
                    if lens.len() <= id as usize {
                        lens.resize(id as usize + 1, 0);
                    }
                    lens[id as usize] = doc_len;
                    if distinct.len() <= id as usize {
                        distinct.resize_with(id as usize + 1, || None);
                    }
                    distinct[id as usize] = Some(TokenSet::from_btree_set(set));
                }
                FieldIndex::Text {
                    analyzer,
                    idx: TextIndex {
                        staged_rows: BTreeMap::new(),
                        live_term_cache: Mutex::new(None),
                        tokens: t,
                        lens,
                        distinct,
                        delta_docs: FastHashMap::default(),
                        doc_count,
                        total_doc_len,
                        bytes,
                        // A rehydrated snapshot has no sealed segment yet;
                        // sealing is a runtime disk-tier action, not restore.
                        segment: None,
                        match_rank_cache: RwLock::new(FastHashMap::default()),
                        // No segment → no sealed-base deletes pending; empty tombstone.
                        tombstones: RoaringBitmap::new(),
                    },
                }
            }
            // The inverted index is rebuilt from `forward`, the way the Number
            // arm below rebuilds `values` from its own. `forward` was always
            // complete — even in a format-1 snapshot written before this fix —
            // so an on-disk snapshot whose persisted `terms` had been bricked
            // self-heals on restore without a re-index. As of format 2 that map
            // is no longer written at all; a format-1 document still carries
            // it, and serde drops it here as an unknown field.
            FieldIndexSnapshot::Keyword { forward, bytes, .. } => {
                let mut fwd: FastHashMap<u32, String> = FastHashMap::default();
                let mut t: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
                for (eid, v) in forward {
                    let id = interner.intern(&eid);
                    t.entry(v.clone()).or_default().insert(id);
                    fwd.insert(id, v);
                }
                FieldIndex::Keyword(KeywordIndex {
                    dup_values: dup_values_of(&t),
                    terms: t,
                    dense_forward: Vec::new(),
                    forward: fwd,
                    bytes,
                    // A rehydrated snapshot has no sealed segment yet.
                    segment: None,
                    // No segment → no sealed-base deletes pending; empty tombstone.
                    tombstones: RoaringBitmap::new(),
                })
            }
            FieldIndexSnapshot::Number { forward, bytes } => {
                let mut idx = NumberIndex {
                    bytes,
                    ..NumberIndex::default()
                };
                for (eid, raw) in forward {
                    let id = interner.intern(&eid);
                    let key = SortableF64::new(raw)?;
                    idx.values.entry(key).or_default().insert(id);
                    idx.set_number(id, key);
                }
                idx.dup_values = dup_values_of(&idx.values);
                FieldIndex::Number(idx)
            }
            // Rebuilt from `forward`, for the reason the Keyword arm above
            // states. Unlike Keyword, a Set snapshot written before the
            // segment-aware gather landed cannot self-heal here — the seal
            // emptied BOTH halves before the old `to_snapshot` ever read them,
            // so `forward` in such a document is empty too, and the field
            // restores empty. Nothing at this layer can invent the data back;
            // finding which fields are in that state is a separate audit.
            FieldIndexSnapshot::Set { forward, bytes, .. } => {
                let mut fwd: FastHashMap<u32, BTreeSet<String>> = FastHashMap::default();
                let mut e: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
                for (eid, set) in forward {
                    let id = interner.intern(&eid);
                    for el in &set {
                        e.entry(el.clone()).or_default().insert(id);
                    }
                    fwd.insert(id, set);
                }
                FieldIndex::Set(SetIndex {
                    dup_values: dup_values_of(&e),
                    elements: e,
                    forward: fwd,
                    bytes,
                    // A rehydrated snapshot has no sealed segment yet.
                    segment: None,
                    // No segment → no sealed-base deletes pending; empty tombstone.
                    tombstones: RoaringBitmap::new(),
                })
            }
            FieldIndexSnapshot::Vector {
                spec,
                vectors,
                codebook,
                bytes,
            } => {
                // Restore the declared backend. Exact backends (flat-cpu)
                // restore exact; HNSW restores its graph (the graph is
                // rebuildable, the raw vectors persist).
                let idx: Box<dyn VectorIndex> = match spec.backend {
                    crate::shared_kernel::types::schema::VectorBackend::FlatCpu => {
                        Box::new(FlatCpuIndex::restore(spec, vectors, codebook)?)
                    }
                    _ => Box::new(HnswCpuIndex::restore(spec, vectors, codebook)?),
                };
                FieldIndex::Vector { spec, idx, bytes }
            }
            FieldIndexSnapshot::Hash { forward, bytes } => {
                let mut fwd: FastHashMap<u32, u64> = FastHashMap::default();
                for (eid, v) in forward {
                    fwd.insert(interner.intern(&eid), v);
                }
                FieldIndex::Hash(HashIndex {
                    tombstones: RoaringBitmap::new(),
                    forward: fwd,
                    bytes,
                    // A rehydrated snapshot has no sealed segment yet.
                    segment: None,
                })
            }
        })
    }
}
