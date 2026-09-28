//! Borrowed scanner for the CBOR v1 `ReplaceDocs` WAL entry.
//!
//! The scanner records only byte ranges and fixed-size descriptors.  It never
//! creates owned field strings, string-list members, or vector payloads.  The
//! ranges are valid only while the immutable source frame remains pinned.
//!
//! Indefinite-length text is deliberately `NotHandled`: CBOR permits it, but
//! lending it as one `str` needs a staged concatenation file.  The caller can
//! retain the existing owned decoder for that encoding until that slice exists.

pub(crate) mod cursor;
pub(crate) mod parse;

use std::{error::Error, fmt};

#[cfg(test)]
use std::cell::Cell;

use anyhow::{anyhow, ensure, Result};

use crate::ingest::infrastructure::wal::borrowed_replace_scanner::cursor::Cursor;
use crate::ingest::infrastructure::wal::borrowed_replace_scanner::parse::{
    parse_entry, MetadataAccounting,
};

#[cfg(test)]
thread_local! {
    static UTF8_VALIDATION_BYTES: Cell<usize> = const { Cell::new(0) };
}

fn validate_utf8(bytes: &[u8]) -> Result<&str> {
    #[cfg(test)]
    UTF8_VALIDATION_BYTES.with(|total| {
        total.set(
            total
                .get()
                .checked_add(bytes.len())
                .expect("UTF-8 validation counter overflow"),
        )
    });
    std::str::from_utf8(bytes).map_err(|_| anyhow!("invalid UTF-8 in CBOR text"))
}

#[cfg(test)]
fn reset_utf8_validation_bytes_for_test() {
    UTF8_VALIDATION_BYTES.with(|total| total.set(0));
}
#[cfg(test)]
fn utf8_validation_bytes_for_test() -> usize {
    UTF8_VALIDATION_BYTES.with(Cell::get)
}

#[derive(Debug)]
struct NotHandled(&'static str);

impl fmt::Display for NotHandled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "borrowed ReplaceDocs scanner does not handle {}", self.0)
    }
}

impl Error for NotHandled {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ByteRange {
    start: usize,
    end: usize,
}

impl ByteRange {
    fn as_str<'a>(self, bytes: &'a [u8]) -> &'a str {
        // Every ByteRange is built by `Cursor::text`, which validates this
        // exact immutable byte span before storing it.
        unsafe { std::str::from_utf8_unchecked(&bytes[self.start..self.end]) }
    }
}

#[derive(Clone, Copy, Debug)]
struct ArrayRange {
    start: usize,
    end: usize,
    len: usize,
}

#[derive(Debug)]
enum DescriptorValue {
    String(ByteRange),
    Number(f64),
    Vector(ArrayRange),
    StringList(ArrayRange),
}

#[derive(Debug)]
struct FieldDescriptor {
    name: ByteRange,
    value: DescriptorValue,
}

#[derive(Debug)]
struct DocDescriptor {
    external_id: ByteRange,
    version: Option<u64>,
    fields: Vec<FieldDescriptor>,
}

/// A parsed v1 generic `ReplaceDocs` record borrowing its original WAL bytes.
#[derive(Debug)]
pub(crate) struct BorrowedReplaceScanner<'a> {
    bytes: &'a [u8],
    collection_id: ByteRange,
    docs: Vec<DocDescriptor>,
    consumed_bytes: usize,
    retained_metadata_bytes: usize,
}

/// The field-value shape exposed by a document view.
pub(crate) enum BorrowedReplaceValue<'a> {
    String(&'a str),
    Number(f64),
    Vector(BorrowedVectorIter<'a>),
    StringList(BorrowedStringListIter<'a>),
}

/// One document in source order.
pub(crate) struct BorrowedReplaceDoc<'a> {
    bytes: &'a [u8],
    descriptor: &'a DocDescriptor,
}

/// One field in BTreeMap (lexical key) order.
pub(crate) struct BorrowedReplaceField<'a> {
    bytes: &'a [u8],
    descriptor: &'a FieldDescriptor,
}

impl<'a> BorrowedReplaceScanner<'a> {
    /// Returns `Ok(None)` for a non-CBOR record or an encoding that needs an
    /// owned/staged fallback.  `reserve` is called before each descriptor
    /// vector grows, with the complete next capacity cost.
    pub(crate) fn scan(
        bytes: &'a [u8],
        mut reserve: impl FnMut(usize) -> Result<()>,
    ) -> Result<Option<Self>> {
        // Match the owned generic route before lending any range.  This
        // bounded preflight checks recursive CBOR, malformed breaks/chunks,
        // all UTF-8, and the decoder's 256-level nesting boundary.
        crate::ingest::infrastructure::wire_cost::scan_workspace_bound(bytes)?;
        if bytes
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            .is_some_and(|byte| matches!(byte, b'{' | b'['))
        {
            return Ok(None); // legacy JSON remains on the existing decoder.
        }
        let mut accounting = MetadataAccounting::default();
        match Self::scan_cbor(bytes, &mut reserve, &mut accounting) {
            Ok(scanner) => Ok(Some(scanner)),
            Err(error) if error.downcast_ref::<NotHandled>().is_some() => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn scan_cbor(
        bytes: &'a [u8],
        reserve: &mut impl FnMut(usize) -> Result<()>,
        accounting: &mut MetadataAccounting,
    ) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let mut version = None;
        let mut entry = None;
        // This visits every value once.  In particular, a schema-invalid
        // field may later be a storage error, but malformed CBOR anywhere in
        // this record must still fail before an adapter can apply a prefix.
        cursor.map_each(|key, cursor| {
            match key {
                "version" => {
                    ensure!(version.is_none(), "duplicate generic WAL version");
                    version = Some(cursor.unsigned()?);
                }
                "entry" => {
                    ensure!(entry.is_none(), "duplicate generic WAL entry");
                    entry = Some(parse_entry(cursor, reserve, accounting)?);
                }
                _ => cursor.skip()?,
            }
            Ok(())
        })?;
        let version = version.ok_or_else(|| anyhow!("generic WAL record misses version"))?;
        if version != 1 {
            return Err(NotHandled("generic WAL version").into());
        }
        let mut scanner = entry
            .ok_or_else(|| anyhow!("generic WAL record misses entry"))?
            .ok_or_else(|| NotHandled("generic WAL entry"))?;
        scanner.consumed_bytes = cursor.pos;
        scanner.retained_metadata_bytes = accounting.retained_bytes;
        Ok(scanner)
    }

    pub(crate) fn collection_id(&self) -> &str {
        self.collection_id.as_str(self.bytes)
    }

    /// Byte length of the one CBOR item that was validated.  Generic replay
    /// may ignore a suffix like ciborium does; a private framed caller can
    /// require this to equal its decoded CBOR payload length.
    pub(crate) fn consumed_bytes(&self) -> usize {
        self.consumed_bytes
    }

    pub(crate) fn docs(&self) -> impl Iterator<Item = BorrowedReplaceDoc<'_>> {
        self.docs.iter().map(|descriptor| BorrowedReplaceDoc {
            bytes: self.bytes,
            descriptor,
        })
    }

    pub(crate) fn retained_metadata_bytes(&self) -> usize {
        self.retained_metadata_bytes
    }
}

impl<'a> BorrowedReplaceDoc<'a> {
    pub(crate) fn external_id(&self) -> &'a str {
        self.descriptor.external_id.as_str(self.bytes)
    }
    pub(crate) fn version(&self) -> Option<u64> {
        self.descriptor.version
    }
    pub(crate) fn fields(&self) -> impl Iterator<Item = BorrowedReplaceField<'a>> {
        self.descriptor
            .fields
            .iter()
            .map(|descriptor| BorrowedReplaceField {
                bytes: self.bytes,
                descriptor,
            })
    }
}

impl<'a> BorrowedReplaceField<'a> {
    pub(crate) fn name(&self) -> &'a str {
        self.descriptor.name.as_str(self.bytes)
    }
    pub(crate) fn value(&self) -> BorrowedReplaceValue<'a> {
        match self.descriptor.value {
            DescriptorValue::String(range) => {
                BorrowedReplaceValue::String(range.as_str(self.bytes))
            }
            DescriptorValue::Number(number) => BorrowedReplaceValue::Number(number),
            DescriptorValue::Vector(range) => {
                BorrowedReplaceValue::Vector(BorrowedVectorIter::new(self.bytes, range))
            }
            DescriptorValue::StringList(range) => {
                BorrowedReplaceValue::StringList(BorrowedStringListIter::new(self.bytes, range))
            }
        }
    }
}

/// Re-parses one validated vector range with constant-size state.
#[derive(Clone)]
pub(crate) struct BorrowedVectorIter<'a> {
    cursor: Cursor<'a>,
    end: usize,
    len: usize,
}
impl<'a> BorrowedVectorIter<'a> {
    fn new(bytes: &'a [u8], range: ArrayRange) -> Self {
        Self {
            cursor: Cursor {
                bytes,
                pos: range.start,
            },
            end: range.end,
            len: range.len,
        }
    }
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}
impl Iterator for BorrowedVectorIter<'_> {
    type Item = Result<f32>;
    fn next(&mut self) -> Option<Self::Item> {
        (self.cursor.pos < self.end).then(|| self.cursor.number_f32())
    }
}

/// Re-parses one validated string-list range with constant-size state.
#[derive(Clone)]
pub(crate) struct BorrowedStringListIter<'a> {
    cursor: Cursor<'a>,
    end: usize,
    len: usize,
}
impl<'a> BorrowedStringListIter<'a> {
    fn new(bytes: &'a [u8], range: ArrayRange) -> Self {
        Self {
            cursor: Cursor {
                bytes,
                pos: range.start,
            },
            end: range.end,
            len: range.len,
        }
    }
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}
impl<'a> Iterator for BorrowedStringListIter<'a> {
    type Item = Result<&'a str>;
    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.cursor.bytes;
        // Parse-time and token preflight already validated this span.  A
        // rewind must not rescan a giant list member as UTF-8.
        (self.cursor.pos < self.end)
            .then(|| self.cursor.trusted_text().map(|range| range.as_str(bytes)))
    }
}

#[cfg(test)]
mod numeric_parity_tests;

#[cfg(test)]
mod tests;
