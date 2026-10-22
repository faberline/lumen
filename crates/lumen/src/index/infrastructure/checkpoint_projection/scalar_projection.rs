//! Scalar checkpoint rows can lend values from immutable temporary segments.
//!
//! This projection never rehydrates a staged field.  Its reader sources keep
//! only a map from staged row number to output row number.  A dictionary pass
//! holds one term and one decoded posting per reader; owned checkpoint strings
//! are borrowed through a small `BTreeMap<&str, Vec<u32>>`.

pub(super) mod merge;

use crate::index::infrastructure::checkpoint_projection::scalar_projection::merge::{
    NumberHead, NumberReaderKeys, ReaderTerms, TermHead,
};

use crate::index::application::checkpoint_capture::CheckpointValue;
use crate::index::domain::sortable_f64::SortableF64;
use crate::persistence::infrastructure::segment::stream::{
    keyword::write_keyword_projection,
    number::write_number_projection,
    scalar_projection::{
        KeywordStreamProjection, NumberStreamProjection, ScalarProjectionScratch,
        SetStreamProjection,
    },
    set::write_set_projection,
};
use crate::persistence::infrastructure::segment::{ScalarPayloadKind, SegmentReader};
use crate::shared_kernel::types::schema::FieldType;
use anyhow::{anyhow, bail, Result};
use std::borrow::Cow;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::sync::Arc;

pub(crate) fn write_checkpoint_rows(
    path: &std::path::Path,
    seq: u64,
    field_type: FieldType,
    rows: &[(
        String,
        Option<crate::ingest::domain::change_journal::SharedValue<CheckpointValue>>,
    )],
) -> Result<()> {
    let projection = RowsProjection::new(field_type, rows)?;
    // The stream writer's temporary posting is the only newly owned field
    // payload.  Do not invent a checkpoint value limit here.
    let scratch = ScalarProjectionScratch::new(usize::MAX).raw_scalar_dictionary();
    match field_type {
        FieldType::Keyword => write_keyword_projection(path, seq, &projection, scratch),
        FieldType::Number => write_number_projection(path, seq, &projection, scratch),
        FieldType::Set => write_set_projection(path, seq, &projection, scratch),
        _ => bail!("scalar checkpoint projection needs keyword, number, or set field"),
    }
}

/// One immutable reader, deduplicated by its Arc allocation.  `selected` is
/// deliberately indexed by source row: dictionary postings remain on disk and
/// only selected dirty rows can enter the new local posting.
struct ReaderSource {
    reader: Arc<SegmentReader>,
    selected: HashMap<u32, u32>,
}

struct RowsProjection<'a> {
    field_type: FieldType,
    rows: &'a [(
        String,
        Option<crate::ingest::domain::change_journal::SharedValue<CheckpointValue>>,
    )],
    readers: Vec<ReaderSource>,
    /// This owns no strings.  The bounded tail is already held by the frozen
    /// checkpoint; only its term-to-local-row index is new.
    owned: BTreeMap<&'a str, Vec<u32>>,
    owned_numbers: BTreeMap<u64, Vec<u32>>,
}

impl<'a> RowsProjection<'a> {
    fn new(
        field_type: FieldType,
        rows: &'a [(
            String,
            Option<crate::ingest::domain::change_journal::SharedValue<CheckpointValue>>,
        )],
    ) -> Result<Self> {
        let mut readers = Vec::new();
        let mut reader_by_ptr = HashMap::<usize, usize>::new();
        let mut owned = BTreeMap::<&str, Vec<u32>>::new();
        let mut owned_numbers = BTreeMap::<u64, Vec<u32>>::new();
        for (local, (_, value)) in rows.iter().enumerate() {
            let local =
                u32::try_from(local).map_err(|_| anyhow!("scalar delta rows exceed u32"))?;
            match (field_type, value.as_deref()) {
                (_, None) => {}
                (FieldType::Keyword, Some(CheckpointValue::Keyword(value))) => {
                    owned.entry(value).or_default().push(local);
                }
                (FieldType::Set, Some(CheckpointValue::Set(values))) => {
                    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
                        bail!("owned Set delta values must be sorted and unique");
                    }
                    for value in values {
                        owned.entry(value).or_default().push(local);
                    }
                }
                (FieldType::Number, Some(CheckpointValue::Number(value))) => {
                    owned_numbers
                        .entry(Self::number_key(*value)?)
                        .or_default()
                        .push(local);
                }
                (_, Some(CheckpointValue::StagedScalar { reader, row })) => {
                    if *row >= reader.n_docs() {
                        bail!("staged scalar row is outside its segment");
                    }
                    let expected = match field_type {
                        FieldType::Keyword => ScalarPayloadKind::Keyword,
                        FieldType::Number => ScalarPayloadKind::Number,
                        FieldType::Set => ScalarPayloadKind::Set,
                        _ => bail!("scalar checkpoint projection field mismatch"),
                    };
                    if reader.scalar_payload_kind() != Some(expected) {
                        bail!("staged scalar payload kind does not match field");
                    }
                    let key = Arc::as_ptr(reader) as usize;
                    let source = match reader_by_ptr.get(&key) {
                        Some(source) => *source,
                        None => {
                            let source = readers.len();
                            readers.push(ReaderSource {
                                reader: reader.clone(),
                                selected: HashMap::new(),
                            });
                            reader_by_ptr.insert(key, source);
                            source
                        }
                    };
                    if readers[source].selected.insert(*row, local).is_some() {
                        bail!("staged scalar row selected more than once");
                    }
                }
                (FieldType::Keyword, _) => bail!("keyword delta value mismatch"),
                (FieldType::Number, _) => bail!("number delta value mismatch"),
                (FieldType::Set, _) => bail!("set delta value mismatch"),
                _ => bail!("scalar checkpoint projection field mismatch"),
            }
        }
        Ok(Self {
            field_type,
            rows,
            readers,
            owned,
            owned_numbers,
        })
    }

    fn staged_keyword<'r>(
        &self,
        row: u32,
        reader: &'r SegmentReader,
    ) -> Result<Option<Cow<'r, str>>> {
        Ok(reader.keyword_at_cow(row))
    }

    fn staged_set(
        &self,
        row: u32,
        reader: &SegmentReader,
        emit: &mut dyn FnMut(&str) -> Result<()>,
    ) -> Result<bool> {
        let (present, count) = reader
            .set_row_member_count(row)
            .ok_or_else(|| anyhow!("staged Set row is corrupt"))?;
        if !present {
            return Ok(false);
        }
        for ordinal in 0..count {
            let value = reader
                .set_member_at_cow(row, ordinal)
                .ok_or_else(|| anyhow!("staged Set member is corrupt"))?;
            emit(&value)?;
        }
        Ok(true)
    }

    fn staged_posting(&self, source: &ReaderSource, term: &str) -> Result<Vec<u32>> {
        let posting = match self.field_type {
            FieldType::Keyword => source.reader.keyword_postings(term),
            FieldType::Set => source.reader.set_postings(term),
            _ => None,
        };
        // A term from one reader is normally absent from every other reader.
        // `None` is therefore an empty source contribution here.
        let Some(posting) = posting else {
            return Ok(Vec::new());
        };
        // The bitmap and `out` are scoped to this one term and one reader.
        let mut out = Vec::new();
        for staged_row in posting.iter() {
            if let Some(local) = source.selected.get(&staged_row) {
                out.push(*local);
            }
        }
        Ok(out)
    }

    fn posting_for(&self, term: &str) -> Result<Vec<u32>> {
        let mut out = self.owned.get(term).cloned().unwrap_or_default();
        for source in &self.readers {
            out.extend(self.staged_posting(source, term)?);
        }
        out.sort_unstable();
        if out.windows(2).any(|pair| pair[0] == pair[1]) {
            bail!("scalar checkpoint term has duplicate local row");
        }
        Ok(out)
    }

    fn number_key(value: f64) -> Result<u64> {
        Ok(SortableF64::new(value)?.bits())
    }

    fn number_posting_for(&self, key: u64) -> Result<Vec<u32>> {
        let mut out = self.owned_numbers.get(&key).cloned().unwrap_or_default();
        for source in &self.readers {
            let Some(posting) = source.reader.number_value_postings(key) else {
                continue;
            };
            for staged_row in posting.iter() {
                if let Some(local) = source.selected.get(&staged_row) {
                    out.push(*local);
                }
            }
        }
        out.sort_unstable();
        if out.windows(2).any(|pair| pair[0] == pair[1]) {
            bail!("scalar checkpoint number has duplicate local row");
        }
        Ok(out)
    }

    fn number_keys_impl(&self, emit: &mut dyn FnMut(u64) -> Result<()>) -> Result<()> {
        let mut cursors: Vec<Box<dyn Iterator<Item = Result<u64>> + '_>> = Vec::new();
        cursors.push(Box::new(self.owned_numbers.keys().copied().map(Ok)));
        for source in &self.readers {
            cursors.push(Box::new(NumberReaderKeys::new(&source.reader)?));
        }
        let mut heap = BinaryHeap::new();
        for (source, cursor) in cursors.iter_mut().enumerate() {
            if let Some(key) = cursor.next().transpose()? {
                heap.push(NumberHead { key, source });
            }
        }
        while let Some(first) = heap.pop() {
            let key = first.key;
            let mut advance = vec![first.source];
            while heap.peek().is_some_and(|head: &NumberHead| head.key == key) {
                advance.push(heap.pop().expect("heap head checked").source);
            }
            for source in advance {
                if let Some(next) = cursors[source].next().transpose()? {
                    heap.push(NumberHead { key: next, source });
                }
            }
            if !self.number_posting_for(key)?.is_empty() {
                emit(key)?;
            }
        }
        Ok(())
    }

    fn number_posting_impl(
        &self,
        key: u64,
        emit: &mut dyn FnMut(u32) -> Result<()>,
    ) -> Result<bool> {
        let posting = self.number_posting_for(key)?;
        for row in &posting {
            emit(*row)?;
        }
        Ok(!posting.is_empty())
    }

    fn emit_terms(&self, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<()> {
        let mut cursors: Vec<Box<dyn Iterator<Item = Result<Cow<'_, str>>> + '_>> = Vec::new();
        cursors.push(Box::new(
            self.owned.keys().map(|term| Ok(Cow::Borrowed(*term))),
        ));
        for source in &self.readers {
            cursors.push(Box::new(ReaderTerms::new(&source.reader)?));
        }
        let mut heap = BinaryHeap::new();
        for (source, cursor) in cursors.iter_mut().enumerate() {
            if let Some(term) = cursor.next().transpose()? {
                heap.push(TermHead { term, source });
            }
        }
        while let Some(first) = heap.pop() {
            let term = first.term;
            let mut advance = vec![first.source];
            while heap.peek().is_some_and(|head: &TermHead| head.term == term) {
                advance.push(heap.pop().expect("heap head checked").source);
            }
            for source in advance {
                if let Some(next) = cursors[source].next().transpose()? {
                    heap.push(TermHead { term: next, source });
                }
            }
            let posting = self.posting_for(&term)?;
            if !posting.is_empty() {
                emit(&term)?;
            }
        }
        Ok(())
    }
}

impl KeywordStreamProjection for RowsProjection<'_> {
    fn n_docs(&self) -> u32 {
        self.rows.len() as u32
    }
    fn keyword_row(
        &self,
        row: u32,
        emit: &mut dyn FnMut(Option<&str>) -> Result<()>,
    ) -> Result<()> {
        match self.rows[row as usize].1.as_deref() {
            Some(CheckpointValue::Keyword(value)) => emit(Some(value)),
            Some(CheckpointValue::StagedScalar { reader, row }) => {
                let value = self.staged_keyword(*row, reader)?;
                emit(value.as_deref())
            }
            None => emit(None),
            _ => bail!("keyword delta value mismatch"),
        }
    }
    fn keyword_terms(&self, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<()> {
        self.emit_terms(emit)
    }
    fn keyword_posting(&self, term: &str, emit: &mut dyn FnMut(u32) -> Result<()>) -> Result<bool> {
        let posting = self.posting_for(term)?;
        for row in &posting {
            emit(*row)?;
        }
        Ok(!posting.is_empty())
    }
}
impl SetStreamProjection for RowsProjection<'_> {
    fn n_docs(&self) -> u32 {
        self.rows.len() as u32
    }
    fn set_row(&self, row: u32, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<bool> {
        match self.rows[row as usize].1.as_deref() {
            Some(CheckpointValue::Set(values)) => {
                for value in values {
                    emit(value)?;
                }
                Ok(true)
            }
            Some(CheckpointValue::StagedScalar { reader, row }) => {
                self.staged_set(*row, reader, emit)
            }
            None => Ok(false),
            _ => bail!("set delta value mismatch"),
        }
    }
    fn set_terms(&self, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<()> {
        self.emit_terms(emit)
    }
    fn set_posting(&self, term: &str, emit: &mut dyn FnMut(u32) -> Result<()>) -> Result<bool> {
        let posting = self.posting_for(term)?;
        for row in &posting {
            emit(*row)?;
        }
        Ok(!posting.is_empty())
    }
}
impl NumberStreamProjection for RowsProjection<'_> {
    fn n_docs(&self) -> u32 {
        self.rows.len() as u32
    }
    fn number_row(&self, row: u32, emit: &mut dyn FnMut(Option<f64>) -> Result<()>) -> Result<()> {
        match self.rows[row as usize].1.as_deref() {
            Some(CheckpointValue::Number(value)) => emit(Some(*value)),
            Some(CheckpointValue::StagedScalar { reader, row }) => emit(reader.number_at(*row)),
            None => emit(None),
            _ => bail!("number delta value mismatch"),
        }
    }
    fn number_keys(&self, emit: &mut dyn FnMut(u64) -> Result<()>) -> Result<()> {
        self.number_keys_impl(emit)
    }
    fn number_posting(&self, key: u64, emit: &mut dyn FnMut(u32) -> Result<()>) -> Result<bool> {
        self.number_posting_impl(key, emit)
    }
}

#[cfg(test)]
mod tests;
