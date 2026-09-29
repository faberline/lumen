//! Record-owned Text preparation runs before the apply lease. Row keys use
//! the original item ordinal, so duplicate cells retain arrival-order meaning.

mod borrowed;
mod staging;

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::index::application::engine::Engine;
#[cfg(feature = "jieba")]
use crate::index::infrastructure::analysis::jieba_disk_route;
use crate::ingest::infrastructure::wal::fast_index_scanner::{FastIndexScanner, FastIndexValue};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::document::{IndexItem, IndexRequest, MAX_INDEX_BATCH_SIZE};
use crate::shared_kernel::types::schema::Analyzer;
use crate::storage::staged_text_row;

pub(super) const TEXT_SCRATCH_BYTES: usize = 8 * 1024 * 1024;

#[derive(Default)]
pub(crate) struct PreparedTextRows {
    epoch: u64,
    collection: Option<(String, u64, u32)>,
    rows: BTreeMap<usize, BTreeMap<String, Arc<staged_text_row::StagedTextRow>>>,
    /// Reader metadata stays live through apply and the retained pending
    /// charge.  It is distinct from the bounded staging workspace, which the
    /// caller releases before it retains the private Index entry.
    retained_reader_bytes: usize,
    /// Extra workspace above `TEXT_SCRATCH_BYTES` accepted while staging.
    /// This is temporary even though it was charged to the reservation during
    /// preparation, so the caller can shrink it before apply.
    scratch_growth_bytes: usize,
}
impl PreparedTextRows {
    pub(super) fn get(
        &self,
        ordinal: usize,
        field: &str,
    ) -> Option<&Arc<staged_text_row::StagedTextRow>> {
        self.rows.get(&ordinal)?.get(field)
    }
    pub(super) fn matches(&self, engine: &Engine) -> bool {
        if self.epoch != engine.capture_barrier.epoch() {
            return false;
        }
        let Some((name, generation, version)) = &self.collection else {
            return true;
        };
        engine.state.read().ok().is_some_and(|state| {
            state.collections.get(name).is_some_and(|coll| {
                coll.collection_generation == *generation && coll.version == *version
            })
        })
    }
    pub(super) fn retained_reader_bytes(&self) -> usize {
        self.retained_reader_bytes
    }
    pub(super) fn scratch_growth_bytes(&self) -> usize {
        self.scratch_growth_bytes
    }
}

fn collection_id(entry: &RaftLogEntry) -> Option<&str> {
    match entry {
        RaftLogEntry::Index { collection_id, .. }
        | RaftLogEntry::ReplaceDocs { collection_id, .. } => Some(collection_id),
        _ => None,
    }
}
fn visit_text_values(entry: &RaftLogEntry, mut visit: impl FnMut(usize, &str, &str)) {
    match entry {
        RaftLogEntry::Index { req, .. } => {
            for (ordinal, item) in req.items.iter().enumerate() {
                if let FieldValue::String(value) = &item.value {
                    visit(ordinal, &item.field, value);
                }
            }
        }
        RaftLogEntry::ReplaceDocs { req, .. } => {
            for (ordinal, doc) in req.docs.iter().enumerate() {
                for (field, value) in &doc.fields {
                    if let FieldValue::String(value) = value {
                        visit(ordinal, field, value);
                    }
                }
            }
        }
        _ => {}
    }
}

/// Bound the owned shell for a borrowed fast-Index Text command.  The source
/// value bytes stay in the scanner; only identifiers, empty value shells, row
/// map nodes, and the small analyzer lookup may allocate here.
///
/// Callers reserve this plus [`TEXT_SCRATCH_BYTES`] before calling
/// `prepare_borrowed_text_rows`.  The method repeats the assertion before it
/// allocates, so a new caller cannot accidentally build metadata while it
/// holds a state lock without first pricing it.
pub(super) fn borrowed_text_metadata_bound(scanner: &FastIndexScanner<'_>) -> Result<usize> {
    anyhow::ensure!(
        scanner.cost().item_count <= MAX_INDEX_BATCH_SIZE,
        "borrowed Text preparation exceeds Index item limit"
    );
    borrowed_field_metadata_bound(scanner)
}

/// Price borrowed Text metadata after a command-aware replacement planner has
/// validated its document and flattened-field limits. It keeps the same
/// per-item arithmetic and overflow checks as the Index entry point.
pub(super) fn borrowed_field_metadata_bound(scanner: &FastIndexScanner<'_>) -> Result<usize> {
    // BTreeMap has no reserve API.  Price three independent maps (analyzers,
    // outer rows, and inner rows) at a deliberately conservative node bound.
    const BTREE_NODE_BOUND: usize = 256;
    const MAP_POPULATIONS: usize = 3;
    const ITEM_SHELL_BOUND: usize = std::mem::size_of::<IndexItem>() + 128;
    let mut bytes = scanner
        .collection_id()
        .len()
        .checked_add(scanner.request_id().map_or(0, str::len))
        .and_then(|n| n.checked_add(std::mem::size_of::<IndexRequest>()))
        .ok_or_else(|| anyhow!("borrowed Text metadata reservation overflow"))?;
    for item in scanner.items() {
        bytes = bytes
            .checked_add(item.external_id.len())
            .and_then(|n| n.checked_add(item.field.len().checked_mul(4)?))
            .and_then(|n| n.checked_add(ITEM_SHELL_BOUND))
            .and_then(|n| n.checked_add(BTREE_NODE_BOUND.checked_mul(MAP_POPULATIONS)?))
            .ok_or_else(|| anyhow!("borrowed Text metadata reservation overflow"))?;
    }
    Ok(bytes)
}

/// The largest temporary normalized token produced before the row writer can
/// return `RequiredTextRowWorkspace`. Whitespace splitting and punctuation
/// trimming match `index_text::for_whitespace_lower_cow`; no output string is
/// built. Pricing only the largest token preserves valid large inputs made of
/// many small words.
fn lowercase_token_workspace_bound(input: &str) -> Result<usize> {
    let mut largest = 0usize;
    for raw in input.split(char::is_whitespace) {
        let token = raw.trim_matches(|character: char| !character.is_alphanumeric());
        if token.is_empty() {
            continue;
        }
        // `to_lowercase` can expand Unicode. Three source bytes plus a small
        // String allocation allowance is the existing owned-preparation rule,
        // applied to one simultaneous token rather than the whole input.
        let bound = token
            .len()
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(64))
            .ok_or_else(|| anyhow!("borrowed Text lowercase workspace overflow"))?;
        largest = largest.max(bound);
    }
    Ok(largest)
}

/// Long whitespace tokens stream to private files. Only the shorter tokens
/// use an owned lowercase buffer; its bound stays fixed as the row grows.
fn bounded_whitespace_workspace(input: &str) -> Result<usize> {
    let mut largest = 0usize;
    for raw in input.split_whitespace() {
        let token = raw.trim_matches(|c: char| !c.is_alphanumeric());
        if token.is_empty() {
            continue;
        }
        let source = token
            .len()
            .min(crate::storage::large_text_row::LARGE_TOKEN_SOURCE_BYTES);
        let bound = source
            .checked_mul(3)
            .and_then(|n| n.checked_add(64))
            .ok_or_else(|| anyhow!("Text lowercase workspace overflow"))?;
        largest = largest.max(bound);
    }
    Ok(largest)
}

/// The feature-off Jieba stream emits CJK bigrams from stack buffers and sends
/// only each non-CJK run to whitespace_lower. Do not price a CJK run as one
/// lowercase String.
#[cfg(not(feature = "jieba"))]
fn fallback_jieba_token_workspace_bound(input: &str) -> Result<usize> {
    let text = input.trim();
    let mut largest = 0usize;
    let mut non_cjk_start = 0usize;
    let mut in_cjk = false;
    for (offset, character) in text.char_indices() {
        if crate::index::domain::analysis::jieba_fallback_stream::is_cjk_char(character) {
            if !in_cjk {
                largest = largest.max(lowercase_token_workspace_bound(
                    &text[non_cjk_start..offset],
                )?);
                in_cjk = true;
            }
        } else if in_cjk {
            in_cjk = false;
            non_cjk_start = offset;
        }
    }
    if !in_cjk {
        largest = largest.max(lowercase_token_workspace_bound(&text[non_cjk_start..])?);
    }
    Ok(largest)
}

/// Dictionary Jieba lowercases each raw word without whitespace-token
/// punctuation trimming. jieba-rs splits at whitespace, so one emitted word is
/// contained in one non-whitespace input span.
#[cfg(feature = "jieba")]
fn dictionary_jieba_token_workspace_bound(input: &str) -> Result<usize> {
    let mut largest = 0usize;
    for span in input.split(char::is_whitespace) {
        if span.is_empty() {
            continue;
        }
        let bound = span
            .len()
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(64))
            .ok_or_else(|| anyhow!("borrowed Text lowercase workspace overflow"))?;
        largest = largest.max(bound);
    }
    Ok(largest)
}

fn borrowed_text_pre_stage_workspace_bound(
    scanner: &FastIndexScanner<'_>,
    mut analyzer_for: impl FnMut(&str) -> Option<Analyzer>,
    mut selected: impl FnMut(usize) -> bool,
) -> Result<usize> {
    let mut lowercase = 0usize;
    #[cfg(feature = "jieba")]
    let mut dictionary_route = false;
    for (ordinal, item) in scanner.items().enumerate() {
        if !selected(ordinal) {
            continue;
        }
        let FastIndexValue::String(input) = item.value else {
            continue;
        };
        let Some(analyzer) = analyzer_for(item.field) else {
            continue;
        };
        match analyzer {
            Analyzer::WhitespaceLower => {
                lowercase = lowercase.max(bounded_whitespace_workspace(input)?);
            }
            #[cfg(not(feature = "jieba"))]
            Analyzer::Jieba => {
                lowercase = lowercase.max(fallback_jieba_token_workspace_bound(input)?);
            }
            #[cfg(feature = "jieba")]
            Analyzer::Jieba => {
                lowercase = lowercase.max(dictionary_jieba_token_workspace_bound(input)?);
                dictionary_route = true;
            }
            Analyzer::Ngram => {}
        }
    }
    #[cfg(feature = "jieba")]
    {
        if dictionary_route {
            lowercase = lowercase
                .checked_add(jieba_disk_route::ROUTE_CACHE_BYTES)
                .ok_or_else(|| anyhow!("borrowed Text pre-stage workspace overflow"))?;
        }
    }
    Ok(lowercase)
}

#[derive(Debug)]
pub(super) struct RequiredBorrowedTextWorkspace {
    pub(super) required_bytes: usize,
}

impl std::fmt::Display for RequiredBorrowedTextWorkspace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "borrowed Text pre-stage workspace requires {} bytes",
            self.required_bytes
        )
    }
}
impl std::error::Error for RequiredBorrowedTextWorkspace {}

/// The plan captured a Text field, then a restore or schema change replaced
/// it before staging started. This is a retry signal, never a client error.
#[derive(Debug)]
pub(super) struct StaleBorrowedTextPreparation;
impl std::fmt::Display for StaleBorrowedTextPreparation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("borrowed Text preparation became stale")
    }
}
impl std::error::Error for StaleBorrowedTextPreparation {}

#[cfg(test)]
mod borrowed_tests;
