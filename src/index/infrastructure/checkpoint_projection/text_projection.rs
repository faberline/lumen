//! Text checkpoint projections keep prepared rows in their immutable mmaps.
//! The dictionary merge owns one current term per input. Posting payloads are
//! decoded one term at a time, with the same live-version selection as queries.

mod rows;

use crate::index::infrastructure::checkpoint_projection::text_projection::rows::RowsProjection;

use std::sync::Arc;

use anyhow::{anyhow, bail, Result};

use crate::index::application::checkpoint_capture::CheckpointValue;
use crate::index::domain::postings::TokPostings;
use crate::index::domain::text_index::TextIndex;
use crate::persistence::infrastructure::segment::stream::text_projection::{
    write_text_projection, TextStreamView,
};
use crate::persistence::infrastructure::segment::SegmentReader;
use std::borrow::Cow;
#[cfg(test)]
use std::cell::Cell;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

type Terms<'a> = Box<dyn Iterator<Item = Result<Cow<'a, str>>> + 'a>;

struct ReaderTerms<'a> {
    reader: &'a SegmentReader,
    next: u32,
    count: u32,
}
impl<'a> Iterator for ReaderTerms<'a> {
    type Item = Result<Cow<'a, str>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.count {
            return None;
        }
        let ordinal = self.next;
        self.next += 1;
        Some(
            self.reader
                .keyword_term_at_ordinal_cow(ordinal)
                .ok_or_else(|| anyhow!("staged Text dictionary entry is corrupt")),
        )
    }
}
fn reader_terms(reader: &SegmentReader) -> Result<Terms<'_>> {
    let count = reader
        .keyword_ordinal_count()
        .ok_or_else(|| anyhow!("staged Text dictionary is missing"))?;
    Ok(Box::new(ReaderTerms {
        reader,
        next: 0,
        count,
    }))
}

struct TermHead<'a> {
    term: Cow<'a, str>,
    source: usize,
}

impl PartialEq for TermHead<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.term == other.term && self.source == other.source
    }
}

impl Eq for TermHead<'_> {}

impl PartialOrd for TermHead<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TermHead<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` is a max heap. Reverse lexical order makes its top the
        // next dictionary term, while the source tie-break makes output stable.
        other
            .term
            .cmp(&self.term)
            .then_with(|| other.source.cmp(&self.source))
    }
}

struct UnionTerms<'a> {
    sources: Vec<Terms<'a>>,
    heads: BinaryHeap<TermHead<'a>>,
    failed: bool,
}

impl<'a> UnionTerms<'a> {
    fn new(mut sources: Vec<Terms<'a>>) -> Result<Self> {
        let mut heads = BinaryHeap::new();
        for (source, terms) in sources.iter_mut().enumerate() {
            if let Some(term) = terms.next().transpose()? {
                heads.push(TermHead { term, source });
            }
        }
        Ok(Self {
            sources,
            heads,
            failed: false,
        })
    }
}
impl<'a> Iterator for UnionTerms<'a> {
    type Item = Result<Cow<'a, str>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let first = self.heads.pop()?;
        let minimum = first.term;
        let mut matches = vec![first.source];
        while self.heads.peek().is_some_and(|head| head.term == minimum) {
            matches.push(self.heads.pop().expect("heap head checked").source);
        }
        for source in matches {
            match self.sources[source].next().transpose() {
                Ok(Some(term)) => self.heads.push(TermHead { term, source }),
                Ok(None) => {}
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            }
        }
        Some(Ok(minimum))
    }
}

fn live_terms(index: &TextIndex) -> Result<Terms<'_>> {
    let mut sources: Vec<Terms<'_>> = Vec::new();
    if let Some(segment) = &index.segment {
        sources.push(TextStreamView::terms(segment.as_ref())?);
    }
    sources.push(Box::new(
        index
            .tokens
            .keys()
            .map(|term| Ok(Cow::Borrowed(term.as_str()))),
    ));
    for row in index.staged_rows.values() {
        sources.push(reader_terms(row.reader())?);
    }
    Ok(Box::new(UnionTerms::new(sources)?))
}

pub(in crate::index) fn live_term_count(index: &TextIndex) -> Result<u64> {
    let mut count = 0;
    for term in live_terms(index)? {
        if index.tok_postings(term?.as_ref()).is_some() {
            count += 1;
        }
    }
    Ok(count)
}

struct LiveProjection<'a> {
    index: &'a TextIndex,
    n_docs: u32,
    live: &'a dyn Fn(u32) -> bool,
}
impl TextStreamView for LiveProjection<'_> {
    fn n_docs(&self) -> u32 {
        self.n_docs
    }
    fn text_is_present(&self, id: u32) -> bool {
        (self.live)(id)
            && (self.index.staged_rows.contains_key(&id)
                || self.index.distinct_at(id).is_some()
                || self.index.segment.as_ref().is_some_and(|segment| {
                    !self.index.tombstones.contains(id) && segment.text_is_present(id)
                }))
    }
    fn text_doc_len(&self, id: u32) -> u32 {
        if self.text_is_present(id) {
            self.index.doc_len(id)
        } else {
            0
        }
    }
    fn terms<'a>(&'a self) -> Result<Terms<'a>> {
        live_terms(self.index)
    }
    fn text_postings(&self, term: &str) -> Result<Option<Arc<(Vec<u32>, Vec<u32>)>>> {
        let Some(posting) = self.index.tok_postings(term) else {
            return Ok(None);
        };
        let (mut ids, mut tfs) = match posting {
            TokPostings::Live(posting) => (posting.docids.clone(), posting.tfs.clone()),
            TokPostings::Segment(posting) => {
                Arc::try_unwrap(posting).unwrap_or_else(|posting| (*posting).clone())
            }
            TokPostings::Combined { docids, tfs } => (docids, tfs),
            // Only `tok_postings_at` builds a candidate projection (#4246);
            // `tok_postings` always yields the full active posting.
            TokPostings::Sparse(_) => {
                unreachable!("tok_postings never yields a candidate projection")
            }
        };
        let mut write = 0;
        for read in 0..ids.len() {
            if self.text_is_present(ids[read]) {
                ids[write] = ids[read];
                tfs[write] = tfs[read];
                write += 1;
            }
        }
        ids.truncate(write);
        tfs.truncate(write);
        if ids.is_empty() {
            return Ok(None);
        }
        Ok(Some(Arc::new((ids, tfs))))
    }
}

pub(in crate::index) fn write_live(
    path: &std::path::Path,
    seq: u64,
    index: &TextIndex,
    n_docs: u32,
    live: &dyn Fn(u32) -> bool,
) -> Result<()> {
    write_text_projection(
        path,
        seq,
        &LiveProjection {
            index,
            n_docs,
            live,
        },
    )
}

pub(crate) fn write_checkpoint_rows(
    path: &std::path::Path,
    seq: u64,
    rows: &[(
        String,
        Option<crate::ingest::domain::change_journal::SharedValue<CheckpointValue>>,
    )],
) -> Result<()> {
    u32::try_from(rows.len()).map_err(|_| anyhow!("Text delta rows exceed u32"))?;
    for (_, value) in rows {
        if !matches!(
            value.as_deref(),
            None | Some(CheckpointValue::Text { .. } | CheckpointValue::StagedText(_))
        ) {
            bail!("text delta value mismatch");
        }
    }
    write_text_projection(
        path,
        seq,
        &RowsProjection {
            rows,
            current: RefCell::new(None),
            #[cfg(test)]
            rows_examined: Cell::new(0),
        },
    )
}

#[cfg(test)]
mod tests;
