use crate::persistence::infrastructure::segment::{
    keyword_writer::write_keyword_segment, text_writer::write_text_segment, SegmentReader,
};
use crate::storage::Postings;
use roaring::RoaringBitmap;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

fn keyword(path: &Path, values: &[Option<&str>]) -> Arc<SegmentReader> {
    let mut postings = BTreeMap::<String, RoaringBitmap>::new();
    for (id, value) in values.iter().enumerate() {
        if let Some(value) = value {
            postings
                .entry((*value).to_owned())
                .or_default()
                .insert(id as u32);
        }
    }
    write_keyword_segment(path, 7, values, &postings).unwrap();
    Arc::new(SegmentReader::open(path).unwrap())
}

fn text(path: &Path, rows: &[Option<&[(&str, u32)]>]) -> Arc<SegmentReader> {
    let mut postings = BTreeMap::<String, Postings>::new();
    let mut lens = Vec::new();
    for (id, row) in rows.iter().enumerate() {
        let mut len = 0;
        for &(term, tf) in row.unwrap_or_default() {
            postings
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
        7,
        &postings,
        &lens,
        &present,
        present.iter().filter(|&&v| v).count() as u64,
        lens.iter().map(|&v| v as u64).sum(),
    )
    .unwrap();
    Arc::new(SegmentReader::open(path).unwrap())
}

fn bits(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits >> 63 == 1 {
        !bits
    } else {
        bits ^ (1 << 63)
    }
}

mod mapped_base;
mod reads;
mod replacement;
mod retain_uncovered;
mod text_merge;
