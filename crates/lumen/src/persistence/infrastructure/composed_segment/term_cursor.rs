//! Dictionary walks across layers. The string and number cursors keep one head
//! per segment and never materialize the complete dictionary.

use crate::persistence::infrastructure::composed_segment::{
    note_text_posting_clones, ComposedSegmentReader,
};
use crate::persistence::infrastructure::segment::SegmentReader;
use anyhow::{anyhow, Result};
use roaring::RoaringBitmap;
use std::borrow::Cow;

impl ComposedSegmentReader {
    pub(crate) fn string_terms(&self, descending: bool) -> Result<StringTermCursor<'_>> {
        StringTermCursor::new(self.readers(), descending)
    }
    pub(crate) fn number_keys(
        &self,
        low: Option<(u64, bool)>,
        high: Option<(u64, bool)>,
        descending: bool,
    ) -> Result<NumberKeyCursor<'_>> {
        NumberKeyCursor::new(self.readers(), low, high, descending)
    }
    fn readers(&self) -> Vec<&SegmentReader> {
        std::iter::once(self.base.as_ref())
            .chain(self.layers.iter().map(|layer| layer.reader.as_ref()))
            .collect()
    }
    pub(crate) fn keyword_terms_all(&self) -> Option<Vec<(String, RoaringBitmap)>> {
        if self.dense_base_only() {
            return self.base.keyword_terms_all();
        }
        let mut cursor = self.string_terms(false).ok()?;
        let mut out = Vec::new();
        while let Some((term, ordinals)) = cursor.next_with_ordinals().ok()? {
            let mut matches = ordinals.into_iter().peekable();
            let local = if matches.peek().is_some_and(|(source, _)| *source == 0) {
                self.base.keyword_postings_at_ordinal(matches.next()?.1)?
            } else {
                RoaringBitmap::new()
            };
            let mut posting = if let Some(map) = &self.base_map {
                let mut global = RoaringBitmap::new();
                for id in local {
                    global.insert(*map.ids.get(id as usize)?);
                }
                global
            } else {
                local
            };
            for (index, layer) in self.layers.iter().enumerate() {
                // Even a layer without this term masks all rows it replaced.
                posting -= &layer.coverage;
                if matches
                    .peek()
                    .is_some_and(|(source, _)| *source == index + 1)
                {
                    let local = layer
                        .reader
                        .keyword_postings_at_ordinal(matches.next()?.1)?;
                    for id in local {
                        posting.insert(*layer.ids.get(id as usize)?);
                    }
                }
            }
            if !posting.is_empty() {
                out.push((term, posting));
            }
        }
        Some(out)
    }
    pub(crate) fn set_elements_all(&self) -> Option<Vec<(String, RoaringBitmap)>> {
        if self.dense_base_only() {
            return self.base.set_elements_all();
        }
        let mut cursor = self.string_terms(false).ok()?;
        let mut out = Vec::new();
        while let Some(term) = cursor.next().ok()? {
            if let Some(posting) = self.set_postings(&term) {
                out.push((term, posting));
            }
        }
        Some(out)
    }
    pub(crate) fn number_values_all(&self) -> Option<Vec<(u64, RoaringBitmap)>> {
        if self.dense_base_only() {
            return self.base.number_values_all();
        }
        let mut cursor = self.number_keys(None, None, false).ok()?;
        let mut out = Vec::new();
        while let Some(key) = cursor.next().ok()? {
            if let Some(posting) = self.number_value_postings(key) {
                out.push((key, posting));
            }
        }
        Some(out)
    }
    pub(crate) fn text_tokens_all(&self) -> Option<Vec<(String, Vec<u32>, Vec<u32>)>> {
        if self.dense_base_only() {
            let all = self.base.text_tokens_all()?;
            note_text_posting_clones(all.len() as u64);
            return Some(all);
        }
        let mut cursor = self.string_terms(false).ok()?;
        let mut out = Vec::new();
        while let Some(term) = cursor.next().ok()? {
            if let Some(posting) = self.text_postings_arc(&term) {
                note_text_posting_clones(1);
                out.push((term, posting.0.clone(), posting.1.clone()));
            }
        }
        Some(out)
    }
}

struct StringSource<'a> {
    reader: &'a SegmentReader,
    next_ordinal: Option<u32>,
    count: u32,
    head: Option<Cow<'a, str>>,
}

pub(crate) struct StringTermCursor<'a> {
    sources: Vec<StringSource<'a>>,
    descending: bool,
}

impl<'a> StringTermCursor<'a> {
    fn new(readers: Vec<&'a SegmentReader>, descending: bool) -> Result<Self> {
        let mut sources = Vec::new();
        for reader in readers {
            let count = reader
                .keyword_ordinal_count()
                .ok_or_else(|| anyhow!("missing string dictionary"))?;
            let ordinal = if descending {
                count.checked_sub(1)
            } else {
                (count > 0).then_some(0)
            };
            let head = ordinal
                .map(|i| {
                    reader
                        .keyword_term_at_ordinal_cow(i)
                        .ok_or_else(|| anyhow!("invalid dictionary ordinal"))
                })
                .transpose()?;
            sources.push(StringSource {
                reader,
                next_ordinal: ordinal,
                count,
                head,
            });
        }
        Ok(Self {
            sources,
            descending,
        })
    }
    pub(crate) fn next(&mut self) -> Result<Option<String>> {
        Ok(self.next_cow()?.map(Cow::into_owned))
    }
    /// Advance one dictionary term without copying raw mmap bytes.
    pub(crate) fn next_cow(&mut self) -> Result<Option<Cow<'a, str>>> {
        Ok(self.next_entry_cow(false)?.map(|(term, _)| term))
    }
    /// The selected dictionary ordinal is already known for each matching
    /// layer. Preserve it before advancing so callers need no point lookup.
    fn next_with_ordinals(&mut self) -> Result<Option<(String, Vec<(usize, u32)>)>> {
        Ok(self
            .next_entry_cow(true)?
            .map(|(term, ordinals)| (term.into_owned(), ordinals)))
    }
    pub(super) fn next_entry_cow(
        &mut self,
        include_ordinals: bool,
    ) -> Result<Option<(Cow<'a, str>, Vec<(usize, u32)>)>> {
        let mut selected: Option<usize> = None;
        for (index, source) in self.sources.iter().enumerate() {
            let Some(head) = source.head.as_ref() else {
                continue;
            };
            let replace = match selected {
                None => true,
                Some(old) => {
                    let old_head = self.sources[old].head.as_ref().expect("selected head");
                    if self.descending {
                        head > old_head
                    } else {
                        head < old_head
                    }
                }
            };
            if replace {
                selected = Some(index);
            }
        }
        let Some(selected) = selected else {
            return Ok(None);
        };
        // Move the selected head. This transfers an LZ4 owned fallback and
        // keeps raw mmap bytes borrowed. No selected head is cloned.
        let term = self.sources[selected].head.take().expect("selected head");
        let mut ordinals = Vec::new();
        for (source_index, source) in self.sources.iter_mut().enumerate() {
            if source_index == selected
                || source
                    .head
                    .as_ref()
                    .is_some_and(|head| head.as_ref() == term.as_ref())
            {
                if include_ordinals {
                    ordinals.push((
                        source_index,
                        source.next_ordinal.expect("a head has an ordinal"),
                    ));
                }
                source.next_ordinal = source.next_ordinal.and_then(|i| {
                    if self.descending {
                        i.checked_sub(1)
                    } else {
                        i.checked_add(1).filter(|&j| j < source.count)
                    }
                });
                source.head = source
                    .next_ordinal
                    .map(|i| {
                        source
                            .reader
                            .keyword_term_at_ordinal_cow(i)
                            .ok_or_else(|| anyhow!("invalid dictionary ordinal"))
                    })
                    .transpose()?;
            }
        }
        Ok(Some((term, ordinals)))
    }
}

struct NumberSource<'a> {
    reader: &'a SegmentReader,
    next_ordinal: Option<u32>,
    low: u32,
    high: u32,
    head: Option<u64>,
}

pub(crate) struct NumberKeyCursor<'a> {
    sources: Vec<NumberSource<'a>>,
    descending: bool,
}

impl<'a> NumberKeyCursor<'a> {
    fn new(
        readers: Vec<&'a SegmentReader>,
        low: Option<(u64, bool)>,
        high: Option<(u64, bool)>,
        descending: bool,
    ) -> Result<Self> {
        let mut sources = Vec::new();
        for reader in readers {
            let (first, end) = reader
                .number_range_index_window(low, high)
                .ok_or_else(|| anyhow!("missing numeric dictionary"))?;
            let ordinal = if first >= end {
                None
            } else if descending {
                Some(end - 1)
            } else {
                Some(first)
            };
            let head = ordinal
                .map(|i| {
                    reader
                        .number_sorted_bits_at(i)
                        .ok_or_else(|| anyhow!("invalid numeric ordinal"))
                })
                .transpose()?;
            sources.push(NumberSource {
                reader,
                next_ordinal: ordinal,
                low: first,
                high: end,
                head,
            });
        }
        Ok(Self {
            sources,
            descending,
        })
    }
    pub(crate) fn next(&mut self) -> Result<Option<u64>> {
        let heads = self.sources.iter().filter_map(|source| source.head);
        let selected = if self.descending {
            heads.max()
        } else {
            heads.min()
        };
        let Some(key) = selected else {
            return Ok(None);
        };
        for source in &mut self.sources {
            if source.head == Some(key) {
                source.next_ordinal = source.next_ordinal.and_then(|i| {
                    if self.descending {
                        i.checked_sub(1).filter(|&j| j >= source.low)
                    } else {
                        i.checked_add(1).filter(|&j| j < source.high)
                    }
                });
                source.head = source
                    .next_ordinal
                    .map(|i| {
                        source
                            .reader
                            .number_sorted_bits_at(i)
                            .ok_or_else(|| anyhow!("invalid numeric ordinal"))
                    })
                    .transpose()?;
            }
        }
        Ok(Some(key))
    }
}
