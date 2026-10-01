//! The dictionary cursors the projection merges: each staged reader's terms or
//! number keys, one head per reader, ordered so the smallest term or key comes
//! out first.

use std::borrow::Cow;
use std::cmp::Ordering;

use anyhow::{anyhow, Result};

use crate::persistence::infrastructure::segment::SegmentReader;

pub(super) struct ReaderTerms<'a> {
    reader: &'a SegmentReader,
    next: u32,
    count: u32,
}
impl<'a> ReaderTerms<'a> {
    pub(super) fn new(reader: &'a SegmentReader) -> Result<Self> {
        Ok(Self {
            reader,
            next: 0,
            count: reader
                .keyword_ordinal_count()
                .ok_or_else(|| anyhow!("staged scalar dictionary is missing"))?,
        })
    }
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
                .ok_or_else(|| anyhow!("staged scalar dictionary entry is corrupt")),
        )
    }
}
pub(super) struct TermHead<'a> {
    pub(super) term: Cow<'a, str>,
    pub(super) source: usize,
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
        other
            .term
            .cmp(&self.term)
            .then_with(|| other.source.cmp(&self.source))
    }
}

pub(super) struct NumberReaderKeys<'a> {
    reader: &'a SegmentReader,
    next: u32,
    count: u64,
}
impl<'a> NumberReaderKeys<'a> {
    pub(super) fn new(reader: &'a SegmentReader) -> Result<Self> {
        Ok(Self {
            reader,
            next: 0,
            count: reader.number_distinct_count(),
        })
    }
}
impl Iterator for NumberReaderKeys<'_> {
    type Item = Result<u64>;
    fn next(&mut self) -> Option<Self::Item> {
        if u64::from(self.next) == self.count {
            return None;
        }
        let index = self.next;
        self.next += 1;
        Some(
            self.reader
                .number_sorted_bits_at(index)
                .ok_or_else(|| anyhow!("staged scalar number key is corrupt")),
        )
    }
}
pub(super) struct NumberHead {
    pub(super) key: u64,
    pub(super) source: usize,
}
impl PartialEq for NumberHead {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source == other.source
    }
}
impl Eq for NumberHead {}
impl PartialOrd for NumberHead {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for NumberHead {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.source.cmp(&self.source))
    }
}
