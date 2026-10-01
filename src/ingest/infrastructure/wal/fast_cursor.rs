//! `FastCursor`: a bounds-checked reader over one fast-format record.

use anyhow::{anyhow, Result};

use crate::ingest::infrastructure::wal::{
    WAL_VALUE_NUMBER, WAL_VALUE_STRING, WAL_VALUE_STRING_LIST, WAL_VALUE_VECTOR,
};
use crate::shared_kernel::types::document::FieldValue;

pub(super) struct FastCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> FastCursor<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub(super) fn expect_magic(&mut self, magic: &[u8]) -> Result<()> {
        let got = self.read_exact(magic.len())?;
        anyhow::ensure!(got == magic, "invalid WAL fast magic");
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
        if end > self.bytes.len() {
            return Err(anyhow!("truncated WAL fast record"));
        }
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    pub(super) fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    pub(super) fn read_u32(&mut self) -> Result<u32> {
        let mut raw = [0u8; 4];
        raw.copy_from_slice(self.read_exact(4)?);
        Ok(u32::from_le_bytes(raw))
    }

    fn read_f32(&mut self) -> Result<f32> {
        let mut raw = [0u8; 4];
        raw.copy_from_slice(self.read_exact(4)?);
        Ok(f32::from_le_bytes(raw))
    }

    fn read_f64(&mut self) -> Result<f64> {
        let mut raw = [0u8; 8];
        raw.copy_from_slice(self.read_exact(8)?);
        Ok(f64::from_le_bytes(raw))
    }

    pub(super) fn read_u64(&mut self) -> Result<u64> {
        let mut raw = [0u8; 8];
        raw.copy_from_slice(self.read_exact(8)?);
        Ok(u64::from_le_bytes(raw))
    }

    pub(super) fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|e| anyhow!("invalid WAL fast utf8: {e}"))
    }

    /// Shared `FieldValue` decode for both fast-Index tags — identical wire
    /// shape in each, only the item prefix (per-item version) differs.
    pub(super) fn read_field_value(&mut self) -> Result<FieldValue> {
        match self.read_u8()? {
            WAL_VALUE_STRING => Ok(FieldValue::String(self.read_string()?)),
            WAL_VALUE_NUMBER => Ok(FieldValue::Number(self.read_f64()?)),
            WAL_VALUE_VECTOR => {
                let len = self.read_u32()? as usize;
                let mut v = Vec::with_capacity(len);
                for _ in 0..len {
                    v.push(self.read_f32()?);
                }
                Ok(FieldValue::Vector(v))
            }
            WAL_VALUE_STRING_LIST => {
                let len = self.read_u32()? as usize;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_string()?);
                }
                Ok(FieldValue::StringList(values))
            }
            other => Err(anyhow!("invalid WAL fast field value tag {other}")),
        }
    }
}
