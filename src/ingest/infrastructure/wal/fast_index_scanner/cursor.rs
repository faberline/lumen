//! The byte cursor a fast-Index command is read with: bounds-checked integers,
//! UTF-8 ranges, and each field value shape.

use anyhow::{anyhow, Result};

use crate::ingest::infrastructure::wal::fast_index_scanner::{
    validate_utf8, ByteRange, FastIndexValue, FastStringList,
};
use crate::ingest::infrastructure::wal::{
    WAL_VALUE_NUMBER, WAL_VALUE_STRING, WAL_VALUE_STRING_LIST, WAL_VALUE_VECTOR,
};

pub(super) struct Cursor<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) pos: usize,
}

impl<'a> Cursor<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub(super) fn expect_magic(&mut self, magic: &[u8]) -> Result<()> {
        anyhow::ensure!(
            self.read_exact(magic.len())? == magic,
            "invalid WAL fast magic"
        );
        Ok(())
    }

    pub(super) fn expect_eof(&self) -> Result<()> {
        anyhow::ensure!(
            self.pos == self.bytes.len(),
            "trailing bytes in WAL fast record"
        );
        Ok(())
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| anyhow!("WAL fast cursor overflow"))?;
        anyhow::ensure!(end <= self.bytes.len(), "truncated WAL fast record");
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    pub(super) fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    pub(super) fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?.try_into().expect("four bytes"),
        ))
    }

    pub(super) fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("eight bytes"),
        ))
    }

    fn read_f64(&mut self) -> Result<f64> {
        Ok(f64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("eight bytes"),
        ))
    }

    pub(super) fn read_utf8_range(&mut self) -> Result<ByteRange> {
        let len = self.read_u32()? as usize;
        let start = self.pos;
        let bytes = self.read_exact(len)?;
        validate_utf8(bytes)?;
        Ok(ByteRange {
            start,
            end: self.pos,
        })
    }

    /// Advance a cursor over a range parse already validated. This is private
    /// to iterator paths that borrow an existing `FastIndexScanner` only.
    pub(super) fn read_utf8_range_validated(&mut self) -> Result<ByteRange> {
        let len = self.read_u32()? as usize;
        let start = self.pos;
        self.read_exact(len)?;
        Ok(ByteRange {
            start,
            end: self.pos,
        })
    }

    /// Validate a value without constructing any user value.
    pub(super) fn scan_value(&mut self) -> Result<usize> {
        let start = self.pos;
        match self.read_u8()? {
            WAL_VALUE_STRING => {
                self.read_utf8_range()?;
            }
            WAL_VALUE_NUMBER => {
                self.read_exact(8)?;
            }
            WAL_VALUE_VECTOR => {
                let len = self.read_u32()? as usize;
                let bytes = len
                    .checked_mul(std::mem::size_of::<f32>())
                    .ok_or_else(|| anyhow!("WAL fast vector byte length overflow"))?;
                self.read_exact(bytes)?;
            }
            WAL_VALUE_STRING_LIST => {
                let count = self.read_u32()? as usize;
                for _ in 0..count {
                    self.read_utf8_range()?;
                }
            }
            other => return Err(anyhow!("invalid WAL fast field value tag {other}")),
        }
        self.pos
            .checked_sub(start)
            .ok_or_else(|| anyhow!("WAL fast cursor overflow"))
    }

    pub(super) fn read_value_validated(&mut self) -> Result<FastIndexValue<'a>> {
        match self.read_u8()? {
            WAL_VALUE_STRING => Ok(FastIndexValue::String(
                self.read_utf8_range_validated()?.validated_str(self.bytes),
            )),
            WAL_VALUE_NUMBER => Ok(FastIndexValue::Number(self.read_f64()?)),
            WAL_VALUE_VECTOR => {
                let len = self.read_u32()? as usize;
                let bytes = len
                    .checked_mul(std::mem::size_of::<f32>())
                    .ok_or_else(|| anyhow!("WAL fast vector byte length overflow"))?;
                Ok(FastIndexValue::Vector {
                    values: self.read_exact(bytes)?,
                    len,
                })
            }
            WAL_VALUE_STRING_LIST => {
                let count = self.read_u32()? as usize;
                let start = self.pos;
                for _ in 0..count {
                    self.read_utf8_range_validated()?;
                }
                Ok(FastIndexValue::StringList(FastStringList {
                    bytes: &self.bytes[start..self.pos],
                    count,
                }))
            }
            other => Err(anyhow!("invalid WAL fast field value tag {other}")),
        }
    }
}
