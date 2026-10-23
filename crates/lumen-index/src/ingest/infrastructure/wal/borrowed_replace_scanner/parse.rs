//! Parsing one `ReplaceDocs` entry into descriptors: the entry and request
//! framing, each document's fields and values, and the metadata budget every
//! descriptor vector grows against.

use std::mem;

use anyhow::{anyhow, bail, ensure, Result};

use crate::ingest::infrastructure::wal::borrowed_replace_scanner::cursor::Cursor;
use crate::ingest::infrastructure::wal::borrowed_replace_scanner::{
    ArrayRange, BorrowedReplaceScanner, ByteRange, DescriptorValue, DocDescriptor, FieldDescriptor,
};

pub(super) fn parse_entry<'a>(
    cursor: &mut Cursor<'a>,
    reserve: &mut impl FnMut(usize) -> Result<()>,
    accounting: &mut MetadataAccounting,
) -> Result<Option<BorrowedReplaceScanner<'a>>> {
    let mut replace = None;
    let mut variants = 0usize;
    cursor.map_each(|key, cursor| {
        variants = variants
            .checked_add(1)
            .ok_or_else(|| anyhow!("entry variant count overflow"))?;
        if key == "ReplaceDocs" {
            ensure!(replace.is_none(), "duplicate ReplaceDocs entry");
            replace = Some(parse_replace_docs(cursor, reserve, accounting)?);
        } else {
            cursor.skip()?;
        }
        Ok(())
    })?;
    ensure!(
        variants == 1,
        "externally tagged entry must contain exactly one variant"
    );
    Ok(replace)
}

fn parse_replace_docs<'a>(
    cursor: &mut Cursor<'a>,
    reserve: &mut impl FnMut(usize) -> Result<()>,
    accounting: &mut MetadataAccounting,
) -> Result<BorrowedReplaceScanner<'a>> {
    let mut collection_id = None;
    let mut docs = None;
    cursor.map_each(|key, cursor| {
        match key {
            "collection_id" => {
                ensure!(collection_id.is_none(), "duplicate collection_id");
                collection_id = Some(cursor.text()?);
            }
            "req" => {
                ensure!(docs.is_none(), "duplicate ReplaceDocs req");
                docs = Some(parse_request(cursor, reserve, accounting)?);
            }
            _ => cursor.skip()?,
        }
        Ok(())
    })?;
    Ok(BorrowedReplaceScanner {
        bytes: cursor.bytes,
        collection_id: collection_id.ok_or_else(|| anyhow!("ReplaceDocs misses collection_id"))?,
        docs: docs.ok_or_else(|| anyhow!("ReplaceDocs misses req"))?,
        consumed_bytes: 0,
        retained_metadata_bytes: 0,
    })
}

fn parse_request(
    cursor: &mut Cursor<'_>,
    reserve: &mut impl FnMut(usize) -> Result<()>,
    accounting: &mut MetadataAccounting,
) -> Result<Vec<DocDescriptor>> {
    let mut docs = None;
    cursor.map_each(|key, cursor| {
        if key == "docs" {
            ensure!(docs.is_none(), "duplicate docs");
            docs = Some(parse_docs(cursor, reserve, accounting)?);
        } else {
            cursor.skip()?;
        }
        Ok(())
    })?;
    docs.ok_or_else(|| anyhow!("ReplaceDocs request misses docs"))
}

fn parse_docs(
    cursor: &mut Cursor<'_>,
    reserve: &mut impl FnMut(usize) -> Result<()>,
    accounting: &mut MetadataAccounting,
) -> Result<Vec<DocDescriptor>> {
    let range = cursor.array()?;
    let mut docs = Vec::new();
    while cursor.pos < range.end {
        reserve_growth(&mut docs, reserve, accounting)?;
        docs.push(parse_doc(cursor, reserve, accounting)?);
    }
    cursor.finish(range)?;
    Ok(docs)
}

fn parse_doc(
    cursor: &mut Cursor<'_>,
    reserve: &mut impl FnMut(usize) -> Result<()>,
    accounting: &mut MetadataAccounting,
) -> Result<DocDescriptor> {
    let mut external_id = None;
    let mut version = None;
    let mut fields = None;
    cursor.map_each(|key, cursor| {
        match key {
            "external_id" => {
                ensure!(external_id.is_none(), "duplicate external_id");
                external_id = Some(cursor.text()?);
            }
            "version" => {
                ensure!(version.is_none(), "duplicate version");
                version = Some(cursor.option_unsigned()?);
            }
            "fields" => {
                ensure!(fields.is_none(), "duplicate fields");
                fields = Some(parse_fields(cursor, reserve, accounting)?);
            }
            _ => cursor.skip()?,
        }
        Ok(())
    })?;
    Ok(DocDescriptor {
        external_id: external_id.ok_or_else(|| anyhow!("replace doc misses external_id"))?,
        version: version.unwrap_or(None),
        fields: fields.ok_or_else(|| anyhow!("replace doc misses fields"))?,
    })
}

fn parse_fields(
    cursor: &mut Cursor<'_>,
    reserve: &mut impl FnMut(usize) -> Result<()>,
    accounting: &mut MetadataAccounting,
) -> Result<Vec<FieldDescriptor>> {
    let mut fields = Vec::<FieldDescriptor>::new();
    cursor.map_each(|key, cursor| {
        let value = parse_value(cursor)?;
        let start = key.as_ptr() as usize - cursor.bytes.as_ptr() as usize;
        let name = ByteRange {
            start,
            end: start + key.len(),
        };
        if let Some(position) = fields
            .iter()
            .position(|field| field.name.as_str(cursor.bytes) == key)
        {
            fields[position].value = value; // serde BTreeMap keeps the last duplicate key.
        } else {
            reserve_growth(&mut fields, reserve, accounting)?;
            fields.push(FieldDescriptor { name, value });
        }
        Ok(())
    })?;
    fields.sort_unstable_by(|left, right| {
        left.name
            .as_str(cursor.bytes)
            .cmp(right.name.as_str(cursor.bytes))
    });
    Ok(fields)
}

fn parse_value(cursor: &mut Cursor<'_>) -> Result<DescriptorValue> {
    let head = cursor.peek_tagged()?;
    match head.major {
        0 | 1 | 7 => Ok(DescriptorValue::Number(cursor.number()?)),
        3 => Ok(DescriptorValue::String(cursor.text()?)),
        4 => {
            let container = cursor.array()?;
            let range = ArrayRange {
                start: container.start,
                end: container.end,
                len: container.len,
            };
            let mut kind = None;
            while cursor.pos < range.end {
                let item = cursor.peek_tagged()?;
                let next = match item.major {
                    0 | 1 | 7 => {
                        cursor.number()?;
                        0
                    }
                    3 => {
                        cursor.text()?;
                        1
                    }
                    _ => bail!("field array has unsupported item"),
                };
                if let Some(previous) = kind {
                    ensure!(previous == next, "field array mixes strings and numbers");
                }
                kind = Some(next);
            }
            cursor.finish(container)?;
            match kind {
                Some(0) | None => Ok(DescriptorValue::Vector(range)),
                Some(1) => Ok(DescriptorValue::StringList(range)),
                _ => unreachable!(),
            }
        }
        _ => bail!("unsupported field value"),
    }
}

#[derive(Default)]
pub(super) struct MetadataAccounting {
    pub(super) retained_bytes: usize,
}

fn reserve_growth<T>(
    items: &mut Vec<T>,
    reserve: &mut impl FnMut(usize) -> Result<()>,
    accounting: &mut MetadataAccounting,
) -> Result<()> {
    if items.len() == items.capacity() {
        let next = items
            .capacity()
            .max(1)
            .checked_mul(2)
            .ok_or_else(|| anyhow!("descriptor capacity overflow"))?;
        let old = items
            .capacity()
            .checked_mul(mem::size_of::<T>())
            .ok_or_else(|| anyhow!("descriptor cost overflow"))?;
        let new = next
            .checked_mul(mem::size_of::<T>())
            .ok_or_else(|| anyhow!("descriptor cost overflow"))?;
        ensure!(
            accounting.retained_bytes >= old,
            "metadata accounting lost vector capacity"
        );
        // Vec can retain the old allocation while it acquires the new one.
        // Reserve the complete aggregate peak before that allocation starts.
        reserve(
            accounting
                .retained_bytes
                .checked_add(new)
                .ok_or_else(|| anyhow!("metadata peak overflow"))?,
        )?;
        items.try_reserve_exact(next - items.len())?;
        accounting.retained_bytes = accounting
            .retained_bytes
            .checked_sub(old)
            .ok_or_else(|| anyhow!("metadata accounting underflow"))?
            .checked_add(new)
            .ok_or_else(|| anyhow!("metadata accounting overflow"))?;
    }
    Ok(())
}
