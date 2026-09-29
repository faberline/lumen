use crate::persistence::infrastructure::segment::stream::scalar_projection::KeywordStreamProjection;
use crate::persistence::infrastructure::segment::stream::text_projection::TextStreamView;
use crate::persistence::infrastructure::segment::{
    keyword_writer::write_keyword_segment, set_writer::write_set_segment,
    text_writer::write_text_segment, SegmentReader,
};
use crate::storage::Postings;
use anyhow::Result;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

struct BorrowedKeywordRows<'a>(&'a [Option<&'a str>]);

impl KeywordStreamProjection for BorrowedKeywordRows<'_> {
    fn n_docs(&self) -> u32 {
        self.0.len() as u32
    }

    fn keyword_row(
        &self,
        row: u32,
        emit: &mut dyn FnMut(Option<&str>) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        emit(self.0[row as usize])
    }

    fn keyword_terms(
        &self,
        emit: &mut dyn FnMut(&str) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        // Test-only ordering keeps references, never copies value bytes.
        let terms: std::collections::BTreeSet<&str> = self.0.iter().flatten().copied().collect();
        for term in terms {
            emit(term)?;
        }
        Ok(())
    }

    fn keyword_posting(
        &self,
        term: &str,
        emit: &mut dyn FnMut(u32) -> anyhow::Result<()>,
    ) -> anyhow::Result<bool> {
        let mut found = false;
        for (row, value) in self.0.iter().enumerate() {
            if *value == Some(term) {
                emit(row as u32)?;
                found = true;
            }
        }
        Ok(found)
    }
}

fn source(path: &Path, rows: &[Option<&[(&str, u32)]>]) -> Arc<SegmentReader> {
    let mut tokens = BTreeMap::<String, Postings>::new();
    let mut lens = Vec::new();
    for (id, row) in rows.iter().enumerate() {
        let mut len = 0;
        for &(term, tf) in row.unwrap_or_default() {
            tokens
                .entry(term.to_owned())
                .or_default()
                .upsert(id as u32, tf);
            len += tf;
        }
        lens.push(len);
    }
    let present: Vec<_> = rows.iter().map(Option::is_some).collect();
    write_text_segment(
        path,
        4,
        &tokens,
        &lens,
        &present,
        present.iter().filter(|&&p| p).count() as u64,
        lens.iter().map(|&len| len as u64).sum(),
    )
    .unwrap();
    Arc::new(SegmentReader::open(path).unwrap())
}

fn keyword_source(path: &Path, values: &[Option<&str>]) -> Arc<SegmentReader> {
    let mut postings = BTreeMap::new();
    for (id, value) in values.iter().enumerate() {
        if let Some(value) = value {
            postings
                .entry((*value).to_owned())
                .or_insert_with(roaring::RoaringBitmap::new)
                .insert(id as u32);
        }
    }
    write_keyword_segment(path, 4, values, &postings).unwrap();
    Arc::new(SegmentReader::open(path).unwrap())
}

fn set_source(path: &Path, rows: &[Option<Vec<String>>]) -> Arc<SegmentReader> {
    let mut postings = BTreeMap::new();
    for (id, row) in rows.iter().enumerate() {
        for member in row.as_deref().unwrap_or_default() {
            postings
                .entry(member.clone())
                .or_insert_with(roaring::RoaringBitmap::new)
                .insert(id as u32);
        }
    }
    let refs: Vec<Option<&[String]>> = rows.iter().map(Option::as_deref).collect();
    write_set_segment(path, 4, &refs, &postings).unwrap();
    Arc::new(SegmentReader::open(path).unwrap())
}

struct Projection {
    rows: Vec<Option<u32>>,
    terms: Vec<(String, Arc<(Vec<u32>, Vec<u32>)>)>,
}

impl TextStreamView for Projection {
    fn n_docs(&self) -> u32 {
        self.rows.len() as u32
    }

    fn text_is_present(&self, id: u32) -> bool {
        self.rows.get(id as usize).is_some_and(Option::is_some)
    }

    fn text_doc_len(&self, id: u32) -> u32 {
        self.rows.get(id as usize).and_then(|row| *row).unwrap_or(0)
    }

    fn terms<'a>(&'a self) -> Result<Box<dyn Iterator<Item = Result<Cow<'a, str>>> + 'a>> {
        Ok(Box::new(
            self.terms
                .iter()
                .map(|(term, _)| Ok(Cow::Borrowed(term.as_str()))),
        ))
    }

    fn text_postings(&self, term: &str) -> Result<Option<Arc<(Vec<u32>, Vec<u32>)>>> {
        Ok(self
            .terms
            .iter()
            .find(|(candidate, _)| candidate == term)
            .map(|(_, posting)| posting.clone()))
    }
}

mod raw_dictionary;

mod scalar_projection;

mod scalar_stream;

mod text;
