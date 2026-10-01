//! The winning items of one field, lent to the segment writer as keyword, set
//! or number rows: terms stream in sorted passes over the borrowed input, so no
//! whole dictionary is ever resident.

use anyhow::Result;

use crate::index::domain::sortable_f64::SortableF64;
use crate::ingest::infrastructure::wal::fast_index_scanner::{FastIndexItem, FastIndexValue};
use crate::persistence::infrastructure::segment::stream;

pub(super) struct Selected<'view, 'source> {
    pub(super) items: &'view [FastIndexItem<'source>],
    pub(super) winning: &'view [usize],
}
impl<'source> Selected<'_, 'source> {
    fn item(&self, row: u32) -> &FastIndexItem<'source> {
        &self.items[self.winning[row as usize]]
    }
    fn terms(&self, emit: &mut dyn FnMut(&str) -> Result<()>, set: bool) -> Result<()> {
        // Each pass keeps two borrowed terms. Set cardinality does not cause a
        // resident whole-dictionary allocation. Input is immutable across passes.
        let mut previous = None;
        loop {
            let mut next: Option<&str> = None;
            for &ordinal in self.winning {
                let mut consider = |value: &'source str| {
                    if previous.is_none_or(|old| value > old) && next.is_none_or(|old| value < old)
                    {
                        next = Some(value);
                    }
                };
                match &self.items[ordinal].value {
                    FastIndexValue::String(value) if !set => consider(value),
                    FastIndexValue::StringList(list) if set => {
                        for value in list.values() {
                            consider(value);
                        }
                    }
                    _ => unreachable!("validated scalar value"),
                }
            }
            match next {
                Some(value) => {
                    emit(value)?;
                    previous = Some(value);
                }
                None => return Ok(()),
            }
        }
    }
}
impl stream::scalar_projection::KeywordStreamProjection for Selected<'_, '_> {
    fn n_docs(&self) -> u32 {
        self.winning.len() as u32
    }
    fn keyword_row(
        &self,
        row: u32,
        emit: &mut dyn FnMut(Option<&str>) -> Result<()>,
    ) -> Result<()> {
        let FastIndexValue::String(value) = self.item(row).value else {
            unreachable!()
        };
        emit(Some(value))
    }
    fn keyword_terms(&self, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<()> {
        self.terms(emit, false)
    }
    fn keyword_posting(&self, term: &str, emit: &mut dyn FnMut(u32) -> Result<()>) -> Result<bool> {
        let mut found = false;
        for (row, &ordinal) in self.winning.iter().enumerate() {
            if matches!(self.items[ordinal].value, FastIndexValue::String(value) if value == term) {
                emit(row as u32)?;
                found = true;
            }
        }
        Ok(found)
    }
}
impl stream::scalar_projection::SetStreamProjection for Selected<'_, '_> {
    fn n_docs(&self) -> u32 {
        self.winning.len() as u32
    }
    fn set_row(&self, row: u32, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<bool> {
        let FastIndexValue::StringList(list) = self.item(row).value else {
            unreachable!()
        };
        let mut previous = None;
        loop {
            let next = list
                .values()
                .filter(|value| previous.is_none_or(|old| *value > old))
                .min();
            match next {
                Some(value) => {
                    emit(value)?;
                    previous = Some(value);
                }
                None => return Ok(true),
            }
        }
    }
    fn set_terms(&self, emit: &mut dyn FnMut(&str) -> Result<()>) -> Result<()> {
        self.terms(emit, true)
    }
    fn set_posting(&self, term: &str, emit: &mut dyn FnMut(u32) -> Result<()>) -> Result<bool> {
        let mut found = false;
        for (row, &ordinal) in self.winning.iter().enumerate() {
            let FastIndexValue::StringList(list) = self.items[ordinal].value else {
                unreachable!()
            };
            if list.values().any(|value| value == term) {
                emit(row as u32)?;
                found = true;
            }
        }
        Ok(found)
    }
}
impl stream::scalar_projection::NumberStreamProjection for Selected<'_, '_> {
    fn n_docs(&self) -> u32 {
        self.winning.len() as u32
    }
    fn number_row(&self, row: u32, emit: &mut dyn FnMut(Option<f64>) -> Result<()>) -> Result<()> {
        let FastIndexValue::Number(value) = self.item(row).value else {
            unreachable!()
        };
        emit(Some(value))
    }
    fn number_keys(&self, emit: &mut dyn FnMut(u64) -> Result<()>) -> Result<()> {
        let mut previous = None;
        loop {
            let mut next = None;
            for &ordinal in self.winning {
                let FastIndexValue::Number(value) = self.items[ordinal].value else {
                    unreachable!()
                };
                let key = SortableF64::new(value)?.bits();
                if previous.is_none_or(|old| key > old) && next.is_none_or(|old| key < old) {
                    next = Some(key);
                }
            }
            match next {
                Some(key) => {
                    emit(key)?;
                    previous = Some(key);
                }
                None => return Ok(()),
            }
        }
    }
    fn number_posting(&self, key: u64, emit: &mut dyn FnMut(u32) -> Result<()>) -> Result<bool> {
        let mut found = false;
        for (row, &ordinal) in self.winning.iter().enumerate() {
            let FastIndexValue::Number(value) = self.items[ordinal].value else {
                unreachable!()
            };
            if SortableF64::new(value)?.bits() == key {
                emit(row as u32)?;
                found = true;
            }
        }
        Ok(found)
    }
}
