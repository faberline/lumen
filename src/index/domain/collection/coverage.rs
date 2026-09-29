//! The reindex audit: the fields a collection's document census covers and its
//! index cannot answer for, and the fields the audit cannot examine, with why.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;

/// One field a reopen could not restore: the collection's own document census
/// says `documents_covered` live documents carry it, and the restored index
/// holds a value for none of them.
///
/// That disagreement is the whole signal, and it is what separates this from an
/// empty field. `eid_fields` records a document under a field only because that
/// field was written for it, so a field the census covers and the index cannot
/// answer for is a field whose contents were dropped somewhere between the two
/// — not one nobody used.
///
/// The known producer is a `Set` field sealed by a build predating the
/// segment-aware forward gather: that seal emptied both the inverted `elements`
/// map and the `forward` column before the snapshot writer read either, so the
/// document travels with an intact census and an empty field. Nothing at
/// restore can invent the values back; re-indexing the named field is the only
/// repair, and naming it is what this type is for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReindexNeeded {
    pub collection: String,
    pub field: String,
    pub documents_covered: u64,
}

/// A field the reindex audit did NOT examine, and why.
///
/// This travels beside [`ReindexNeeded`] because the audit's whole value to an
/// operator is the negative answer — "nothing needs re-indexing, import the
/// backup" — and a negative answer is only worth acting on next to the list of
/// what it did not cover. Without this, a `Text` field whose contents were
/// dropped clears the same audit a healthy one does, and the report says so in
/// exactly the same words.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldNotAudited {
    pub collection: String,
    pub field: String,
    pub reason: String,
}

pub(crate) const TEXT_UNAUDITABLE: &str =
    "a Text field writes a forward entry for every covered document, \
     explicit-empty values included, so a field of empty strings and a field whose contents were \
     dropped are the same bytes here";

/// How much of a field's arm the reindex audit can see.
///
/// The three answers are genuinely different, and collapsing them is what made
/// the audit overclaim: `Unauditable` is not a healthy verdict, and
/// `WholeIndex` catches a field emptied outright but not one emptied in part.
pub(crate) enum FieldAudit {
    /// Every covered document can be probed individually. Damage is "the census
    /// covers N documents and not one of them has a value".
    PerId,
    /// The arm has no per-id accessor, so only the index's own population is
    /// observable. `true` = populated.
    WholeIndex(bool),
    /// The arm cannot be audited at all. The payload is the reason, and it is
    /// reported to the caller verbatim.
    Unauditable(&'static str),
}

impl Collection {
    /// Every field this collection's document census covers and its index
    /// cannot answer for. See [`ReindexNeeded`].
    ///
    /// Read live rather than cached, off the same segment-aware accessors
    /// `to_snapshot` gathers through, so the answer is about the collection as
    /// it stands — a field re-indexed after the reopen that reported it stops
    /// being reported, and a field sealed since the reopen does not start.
    pub(crate) fn reindex_needed(&self, collection: &str) -> Vec<ReindexNeeded> {
        // One walk of the coverage map, probing as it goes.
        //
        // The shape this replaces materialised a `Vec<u32>` of every covered id
        // for every field first, then probed. That is documents × fields of
        // allocation — gigabytes on a large collection — on a path that runs at
        // the end of every restore and every segment reopen, to answer a
        // question that early-exits on the first hit. Probing inside the walk
        // keeps the single pass AND the early exit while holding O(fields):
        // once a field has been seen to hold something, the remaining ids only
        // cost the count.
        struct Tally<'a> {
            index: &'a FieldIndex,
            covered: u64,
            holds: bool,
            probe: bool,
        }
        // Unauditable arms are excluded here rather than answered `true`:
        // `fields_not_audited` reports them, and folding them into a clean
        // verdict is what let a dropped Text field clear this audit.
        let mut tally: BTreeMap<&str, Tally<'_>> = self
            .fields
            .iter()
            .filter_map(|(name, index)| {
                let (holds, probe) = match index.audit_kind() {
                    FieldAudit::PerId => (false, true),
                    FieldAudit::WholeIndex(populated) => (populated, false),
                    FieldAudit::Unauditable(_) => return None,
                };
                Some((
                    name.as_str(),
                    Tally {
                        index,
                        covered: 0,
                        holds,
                        probe,
                    },
                ))
            })
            .collect();
        for (id, cov) in &self.eid_fields {
            for name in cov.iter() {
                if let Some(t) = tally.get_mut(name.as_str()) {
                    t.covered += 1;
                    if t.probe && !t.holds {
                        t.holds = t.index.holds(*id);
                    }
                }
            }
        }
        // `BTreeMap` already yields fields in name order, and the report is
        // read by humans and diffed by scripts, so the order is the contract.
        tally
            .into_iter()
            // A field no live document carries is empty on purpose. This is
            // what keeps a sparsely-used field, and every field of an empty
            // collection, out of the report.
            .filter(|(_, t)| t.covered > 0 && !t.holds)
            .map(|(name, t)| ReindexNeeded {
                collection: collection.to_string(),
                field: name.to_string(),
                documents_covered: t.covered,
            })
            .collect()
    }

    /// Every field of this collection the audit above could not examine. See
    /// [`FieldNotAudited`]. O(fields) — no walk of the census is needed,
    /// because the answer is a property of the arm, not of the documents.
    pub(crate) fn fields_not_audited(&self, collection: &str) -> Vec<FieldNotAudited> {
        let mut out: Vec<FieldNotAudited> = self
            .fields
            .iter()
            .filter_map(|(name, index)| match index.audit_kind() {
                FieldAudit::Unauditable(reason) => Some(FieldNotAudited {
                    collection: collection.to_string(),
                    field: name.clone(),
                    reason: reason.to_string(),
                }),
                _ => None,
            })
            .collect();
        out.sort_by(|a, b| a.field.cmp(&b.field));
        out
    }
}
