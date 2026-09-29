//! The checkpoint rows as the Text writer sees them: one term cursor per
//! ordinary or staged row, merged in term order, with the current term's
//! posting decoded once and shared by the point lookups that follow.

use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};

use crate::index::application::checkpoint_capture::CheckpointValue;
use crate::index::infrastructure::checkpoint_projection::text_projection::Terms;
use crate::persistence::infrastructure::segment::stream::text_projection::TextStreamView;
use crate::persistence::infrastructure::segment::SegmentReader;

#[cfg(test)]
use std::cell::Cell;

pub(super) struct RowsProjection<'a> {
    pub(super) rows: &'a [(
        String,
        Option<crate::ingest::domain::change_journal::SharedValue<CheckpointValue>>,
    )],
    pub(super) current: RefCell<Option<CurrentPosting<'a>>>,
    #[cfg(test)]
    pub(super) rows_examined: Cell<usize>,
}

pub(super) struct CurrentPosting<'a> {
    term: Cow<'a, str>,
    posting: Arc<(Vec<u32>, Vec<u32>)>,
}

enum RowTermCursor<'a> {
    Ordinary {
        source: usize,
        row: u32,
        terms: std::collections::btree_map::Iter<'a, String, u32>,
    },
    Staged {
        source: usize,
        row: u32,
        reader: &'a SegmentReader,
        next: u32,
        count: u32,
    },
}

impl<'a> RowTermCursor<'a> {
    fn next(&mut self) -> Result<Option<RowTermHead<'a>>> {
        match self {
            Self::Ordinary { source, row, terms } => {
                Ok(terms.next().map(|(term, tf)| RowTermHead {
                    term: Cow::Borrowed(term.as_str()),
                    source: *source,
                    row: *row,
                    tf: *tf,
                }))
            }
            Self::Staged {
                source,
                row,
                reader,
                next,
                count,
            } => {
                if *next == *count {
                    return Ok(None);
                }
                let ordinal = *next;
                *next += 1;
                let term = reader
                    .keyword_term_at_ordinal_cow(ordinal)
                    .ok_or_else(|| anyhow!("staged Text dictionary entry is corrupt"))?;
                let posting = reader
                    .text_postings_arc(term.as_ref())
                    .ok_or_else(|| anyhow!("staged Text posting is corrupt"))?;
                if posting.0.len() != 1 || posting.1.len() != 1 || posting.0[0] != 0 {
                    bail!("staged Text row posting is not a single local row");
                }
                Ok(Some(RowTermHead {
                    term,
                    source: *source,
                    row: *row,
                    tf: posting.1[0],
                }))
            }
        }
    }
}

struct RowTermHead<'a> {
    term: Cow<'a, str>,
    source: usize,
    row: u32,
    tf: u32,
}

impl PartialEq for RowTermHead<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.term == other.term && self.source == other.source
    }
}

impl Eq for RowTermHead<'_> {}

impl PartialOrd for RowTermHead<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowTermHead<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .term
            .cmp(&self.term)
            .then_with(|| other.source.cmp(&self.source))
    }
}

/// One dictionary pass. It retains one `(term, tf)` head for each present row
/// and produces the posting for the returned term before advancing that row.
struct RowsTerms<'projection, 'rows> {
    cursors: Vec<RowTermCursor<'rows>>,
    heads: BinaryHeap<RowTermHead<'rows>>,
    current: &'projection RefCell<Option<CurrentPosting<'rows>>>,
    failed: bool,
}

impl<'projection, 'rows> RowsTerms<'projection, 'rows> {
    fn new(projection: &'projection RowsProjection<'rows>) -> Result<Self> {
        projection.current.replace(None);
        let mut cursors = Vec::new();
        for (id, (_, value)) in projection.rows.iter().enumerate() {
            let row = u32::try_from(id).map_err(|_| anyhow!("Text row id exceeds u32"))?;
            let source = cursors.len();
            match value.as_deref() {
                Some(CheckpointValue::Text { tokens, .. }) => {
                    cursors.push(RowTermCursor::Ordinary {
                        source,
                        row,
                        terms: tokens.iter(),
                    })
                }
                Some(CheckpointValue::StagedText(staged)) => {
                    let reader = staged.reader();
                    let count = reader
                        .keyword_ordinal_count()
                        .ok_or_else(|| anyhow!("staged Text dictionary is missing"))?;
                    cursors.push(RowTermCursor::Staged {
                        source,
                        row,
                        reader,
                        next: 0,
                        count,
                    });
                }
                None => {}
                Some(_) => bail!("text checkpoint value mismatch"),
            }
        }
        let mut heads = BinaryHeap::new();
        for cursor in &mut cursors {
            if let Some(head) = cursor.next()? {
                heads.push(head);
            }
        }
        Ok(Self {
            cursors,
            heads,
            current: &projection.current,
            failed: false,
        })
    }
}

impl<'projection, 'rows> Iterator for RowsTerms<'projection, 'rows> {
    type Item = Result<Cow<'rows, str>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        // The writer has consumed the preceding posting before it asks for the
        // next term. Drop that payload before allocating this term's vectors.
        self.current.replace(None);
        let first = self.heads.pop()?;
        let term = first.term;
        let mut sources = Vec::new();
        let mut ids = Vec::new();
        let mut tfs = Vec::new();
        sources.push(first.source);
        ids.push(first.row);
        tfs.push(first.tf);
        while self.heads.peek().is_some_and(|head| head.term == term) {
            let head = self.heads.pop().expect("heap head checked");
            sources.push(head.source);
            ids.push(head.row);
            tfs.push(head.tf);
        }
        for source in sources {
            let cursor = &mut self.cursors[source];
            match cursor.next() {
                Ok(Some(next)) => self.heads.push(next),
                Ok(None) => {}
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            }
        }
        self.current.replace(Some(CurrentPosting {
            term: term.clone(),
            posting: Arc::new((ids, tfs)),
        }));
        Some(Ok(term))
    }
}

#[cfg(test)]
impl RowsProjection<'_> {
    pub(super) fn rows_examined(&self) -> usize {
        self.rows_examined.get()
    }
}
impl TextStreamView for RowsProjection<'_> {
    fn n_docs(&self) -> u32 {
        self.rows.len() as u32
    }
    fn text_is_present(&self, id: u32) -> bool {
        self.rows[id as usize].1.is_some()
    }
    fn text_doc_len(&self, id: u32) -> u32 {
        match self.rows[id as usize].1.as_deref() {
            Some(CheckpointValue::Text { doc_len, .. }) => *doc_len,
            Some(CheckpointValue::StagedText(row)) => row.doc_len(),
            _ => 0,
        }
    }
    fn terms<'a>(&'a self) -> Result<Terms<'a>> {
        // The cache keeps the row lifetime. Narrow only each yielded Cow to
        // this iterator borrow; RefCell itself is invariant in that lifetime.
        Ok(Box::new(
            RowsTerms::new(self)?.map(|term| term.map(|value| -> Cow<'a, str> { value })),
        ))
    }
    fn text_postings(&self, term: &str) -> Result<Option<Arc<(Vec<u32>, Vec<u32>)>>> {
        if let Some(posting) = self
            .current
            .borrow()
            .as_ref()
            .filter(|posting| posting.term.as_ref() == term)
            .map(|posting| posting.posting.clone())
        {
            return Ok(Some(posting));
        }
        let mut out = Vec::new();
        for (id, (_, value)) in self.rows.iter().enumerate() {
            #[cfg(test)]
            self.rows_examined.set(self.rows_examined.get() + 1);
            let tf = match value.as_deref() {
                Some(CheckpointValue::Text { tokens, .. }) => tokens.get(term).copied(),
                Some(CheckpointValue::StagedText(row)) => {
                    match row.reader().text_postings_arc(term) {
                        Some(posting)
                            if posting.0.len() == 1
                                && posting.1.len() == 1
                                && posting.0[0] == 0 =>
                        {
                            Some(posting.1[0])
                        }
                        Some(_) => bail!("staged Text row posting is not a single local row"),
                        None => None,
                    }
                }
                _ => None,
            };
            if let Some(tf) = tf {
                out.push((id as u32, tf));
            }
        }
        Ok((!out.is_empty()).then(|| Arc::new(out.into_iter().unzip())))
    }
}
