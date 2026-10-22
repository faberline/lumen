//! The staged row as the Text writer sees it: the short tokens' one-row segment
//! and the mapped long tokens, grouped by term, merged into one sorted term
//! stream with each term's frequency.

use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::index::infrastructure::staging::large_text_row::MappedToken;
use crate::persistence::infrastructure::segment::stream::text_projection::TextStreamView;
use crate::persistence::infrastructure::segment::SegmentReader;

#[derive(Clone, Copy)]
pub(super) struct Group {
    pub(super) first: usize,
    pub(super) tf: u32,
}

pub(super) struct OneRow<'a> {
    pub(super) small: &'a SegmentReader,
    pub(super) long: &'a [MappedToken],
    pub(super) groups: &'a [Group],
    pub(super) doc_len: u32,
}
impl OneRow<'_> {
    fn long_tf(&self, term: &str) -> Option<u32> {
        self.groups
            .binary_search_by(|group| self.long[group.first].as_str().cmp(term))
            .ok()
            .map(|index| self.groups[index].tf)
    }
}
impl TextStreamView for OneRow<'_> {
    fn n_docs(&self) -> u32 {
        1
    }
    fn text_is_present(&self, id: u32) -> bool {
        id == 0
    }
    fn text_doc_len(&self, id: u32) -> u32 {
        if id == 0 {
            self.doc_len
        } else {
            0
        }
    }
    fn terms<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<Cow<'a, str>>> + 'a>> {
        Ok(Box::new(OneTerms::new(self)?))
    }
    fn text_postings(&self, term: &str) -> Result<Option<Arc<(Vec<u32>, Vec<u32>)>>> {
        let long = self.long_tf(term).unwrap_or(0);
        let small = self
            .small
            .text_postings_arc(term)
            .map(|posting| {
                if posting.0.as_slice() != [0] || posting.1.len() != 1 {
                    return Err(anyhow!("small Text posting is not local"));
                }
                Ok(posting.1[0])
            })
            .transpose()?
            .unwrap_or(0);
        let tf = long
            .checked_add(small)
            .ok_or_else(|| anyhow!("Text term frequency exceeds u32"))?;
        Ok((tf != 0).then(|| Arc::new((vec![0], vec![tf]))))
    }
}

struct OneTerms<'a> {
    view: &'a OneRow<'a>,
    next_small: u32,
    small_count: u32,
    small: Option<Cow<'a, str>>,
    group: usize,
}
impl<'a> OneTerms<'a> {
    fn new(view: &'a OneRow<'a>) -> Result<Self> {
        let small_count = view
            .small
            .keyword_ordinal_count()
            .ok_or_else(|| anyhow!("small Text dictionary is missing"))?;
        let mut result = Self {
            view,
            next_small: 0,
            small_count,
            small: None,
            group: 0,
        };
        result.advance_small()?;
        Ok(result)
    }
    fn advance_small(&mut self) -> Result<()> {
        self.small = if self.next_small == self.small_count {
            None
        } else {
            let ordinal = self.next_small;
            self.next_small += 1;
            Some(
                self.view
                    .small
                    .keyword_term_at_ordinal_cow(ordinal)
                    .ok_or_else(|| anyhow!("small Text dictionary is corrupt"))?,
            )
        };
        Ok(())
    }
}
impl<'a> Iterator for OneTerms<'a> {
    type Item = Result<Cow<'a, str>>;
    fn next(&mut self) -> Option<Self::Item> {
        let long = self
            .view
            .groups
            .get(self.group)
            .map(|group| self.view.long[group.first].as_str());
        match (self.small.as_ref(), long) {
            (None, None) => None,
            (None, Some(term)) => {
                self.group += 1;
                Some(Ok(Cow::Borrowed(term)))
            }
            (Some(_), None) => {
                let term = self.small.take().unwrap();
                match self.advance_small() {
                    Ok(()) => Some(Ok(term)),
                    Err(error) => Some(Err(error)),
                }
            }
            (Some(small), Some(term)) if small.as_ref() < term => {
                let term = self.small.take().unwrap();
                match self.advance_small() {
                    Ok(()) => Some(Ok(term)),
                    Err(error) => Some(Err(error)),
                }
            }
            (Some(small), Some(term)) if small.as_ref() > term => {
                self.group += 1;
                Some(Ok(Cow::Borrowed(term)))
            }
            (Some(_), Some(term)) => {
                self.group += 1;
                match self.advance_small() {
                    Ok(()) => Some(Ok(Cow::Borrowed(term))),
                    Err(error) => Some(Err(error)),
                }
            }
        }
    }
}
