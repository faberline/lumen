//! The CBOR cursor the scanner parses with: item heads, definite and indefinite
//! containers, and half-precision float widening.

use anyhow::{anyhow, bail, ensure, Result};

use crate::ingest::infrastructure::wal::borrowed_replace_scanner::{
    validate_utf8, ByteRange, NotHandled,
};

#[derive(Clone, Copy)]
pub(super) struct Head {
    pub(super) major: u8,
    ai: u8,
    argument: Option<u64>,
}

#[derive(Clone, Copy)]
pub(super) struct Container {
    pub(super) start: usize,
    pub(super) end: usize,
    after_end: usize,
    pub(super) len: usize,
}

#[derive(Clone, Copy)]
pub(super) struct Cursor<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) pos: usize,
}

impl<'a> Cursor<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    fn byte(&mut self) -> Result<u8> {
        let byte = *self
            .bytes
            .get(self.pos)
            .ok_or_else(|| anyhow!("truncated CBOR"))?;
        self.pos += 1;
        Ok(byte)
    }
    pub(super) fn peek_tagged(&mut self) -> Result<Head> {
        let pos = self.pos;
        let head = self.tagged()?;
        self.pos = pos;
        Ok(head)
    }
    fn head(&mut self) -> Result<Head> {
        let initial = self.byte()?;
        let major = initial >> 5;
        let ai = initial & 0x1f;
        let argument = match ai {
            0..=23 => Some(ai as u64),
            24 => Some(self.byte()? as u64),
            25 => Some(u16::from_be_bytes([self.byte()?, self.byte()?]) as u64),
            26 => Some(
                u32::from_be_bytes([self.byte()?, self.byte()?, self.byte()?, self.byte()?]) as u64,
            ),
            27 => Some(u64::from_be_bytes([
                self.byte()?,
                self.byte()?,
                self.byte()?,
                self.byte()?,
                self.byte()?,
                self.byte()?,
                self.byte()?,
                self.byte()?,
            ])),
            31 => None,
            _ => bail!("reserved CBOR additional information"),
        };
        Ok(Head {
            major,
            ai,
            argument,
        })
    }
    fn tagged(&mut self) -> Result<Head> {
        loop {
            let head = self.head()?;
            if head.major != 6 {
                return Ok(head);
            }
        }
    }
    pub(super) fn text(&mut self) -> Result<ByteRange> {
        let head = self.tagged()?;
        ensure!(head.major == 3, "expected CBOR text");
        let len = usize::try_from(
            head.argument
                .ok_or_else(|| NotHandled("fragmented CBOR text"))?,
        )
        .map_err(|_| anyhow!("CBOR text length exceeds usize"))?;
        let start = self.pos;
        let end = start
            .checked_add(len)
            .ok_or_else(|| anyhow!("CBOR text length overflow"))?;
        let text = self
            .bytes
            .get(start..end)
            .ok_or_else(|| anyhow!("truncated CBOR text"))?;
        validate_utf8(text)?;
        self.pos = end;
        Ok(ByteRange { start, end })
    }
    pub(super) fn trusted_text(&mut self) -> Result<ByteRange> {
        let head = self.tagged()?;
        ensure!(head.major == 3, "expected CBOR text");
        let len = usize::try_from(
            head.argument
                .ok_or_else(|| NotHandled("fragmented CBOR text"))?,
        )
        .map_err(|_| anyhow!("CBOR text length exceeds usize"))?;
        let start = self.pos;
        let end = start
            .checked_add(len)
            .ok_or_else(|| anyhow!("CBOR text length overflow"))?;
        ensure!(end <= self.bytes.len(), "truncated CBOR text");
        self.pos = end;
        Ok(ByteRange { start, end })
    }
    pub(super) fn unsigned(&mut self) -> Result<u64> {
        let head = self.tagged()?;
        ensure!(
            head.major == 0 && head.argument.is_some(),
            "expected unsigned integer"
        );
        Ok(head.argument.unwrap())
    }
    pub(super) fn option_unsigned(&mut self) -> Result<Option<u64>> {
        let head = self.peek_tagged()?;
        if head.major == 7 && head.ai == 22 {
            self.tagged()?;
            Ok(None)
        } else {
            self.unsigned().map(Some)
        }
    }
    pub(super) fn number(&mut self) -> Result<f64> {
        let head = self.tagged()?;
        match (head.major, head.ai, head.argument) {
            (0, _, Some(value)) => Ok(value as f64),
            // ciborium supplies negative integers through serde's i64 path;
            // values below i64::MIN are not accepted by WireFieldValue.
            (1, _, Some(value)) if value <= i64::MAX as u64 => Ok((-1i64 - value as i64) as f64),
            (7, 25, Some(bits)) => Ok(f16_to_f64(bits as u16)),
            (7, 26, Some(bits)) => Ok(f32::from_bits(bits as u32) as f64),
            (7, 27, Some(bits)) => Ok(f64::from_bits(bits)),
            _ => bail!("expected CBOR number"),
        }
    }
    pub(super) fn number_f32(&mut self) -> Result<f32> {
        let head = self.tagged()?;
        match (head.major, head.ai, head.argument) {
            // Match serde's integer-to-f32 conversion directly. Going through
            // f64 first can round twice for a valid large integer component.
            (0, _, Some(value)) => Ok(value as f32),
            (1, _, Some(value)) if value <= i64::MAX as u64 => Ok((-1i64 - value as i64) as f32),
            (7, 25, Some(bits)) => Ok(f16_to_f64(bits as u16) as f32),
            (7, 26, Some(bits)) => Ok(f32::from_bits(bits as u32)),
            (7, 27, Some(bits)) => Ok(f64::from_bits(bits) as f32),
            _ => bail!("expected CBOR number"),
        }
    }
    pub(super) fn array(&mut self) -> Result<Container> {
        let head = self.tagged()?;
        ensure!(head.major == 4, "expected CBOR array");
        let start = self.pos;
        match head.argument {
            Some(count) => {
                let len = usize::try_from(count)
                    .map_err(|_| anyhow!("CBOR array length exceeds usize"))?;
                for _ in 0..count {
                    self.skip()?;
                }
                Ok(Container {
                    start,
                    end: self.pos,
                    after_end: self.pos,
                    len,
                })
            }
            None => {
                let mut len = 0usize;
                while self.bytes.get(self.pos) != Some(&0xff) {
                    self.skip()?;
                    len = len
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("CBOR array length overflow"))?;
                }
                let end = self.pos;
                self.pos += 1;
                Ok(Container {
                    start,
                    end,
                    after_end: self.pos,
                    len,
                })
            }
        }
        .map(|container| {
            self.pos = start;
            container
        })
    }
    pub(super) fn finish(&mut self, container: Container) -> Result<()> {
        ensure!(
            self.pos == container.end,
            "CBOR container content was not fully consumed"
        );
        self.pos = container.after_end;
        Ok(())
    }
    pub(super) fn map_each(
        &mut self,
        mut visit: impl FnMut(&str, &mut Self) -> Result<()>,
    ) -> Result<()> {
        let head = self.tagged()?;
        ensure!(head.major == 5, "expected CBOR map");
        let mut remaining = head.argument;
        while remaining.map_or(self.bytes.get(self.pos) != Some(&0xff), |count| count > 0) {
            let key = self.text()?;
            let key = key.as_str(self.bytes);
            visit(key, self)?;
            remaining = remaining.map(|count| count - 1);
        }
        if head.argument.is_none() {
            self.pos += 1;
        }
        Ok(())
    }
    fn map_find<T>(
        &mut self,
        wanted: &str,
        mut read: impl FnMut(&mut Self) -> Result<T>,
    ) -> Result<Option<T>> {
        let mut found = None;
        self.map_each(|key, cursor| {
            if key == wanted {
                ensure!(found.is_none(), "duplicate {wanted}");
                found = Some(read(cursor)?);
            } else {
                cursor.skip()?;
            }
            Ok(())
        })?;
        Ok(found)
    }
    pub(super) fn skip(&mut self) -> Result<()> {
        let head = self.head()?;
        match head.major {
            0 | 1 | 7 => Ok(()),
            2 | 3 => match head.argument {
                Some(len) => {
                    let len =
                        usize::try_from(len).map_err(|_| anyhow!("CBOR length exceeds usize"))?;
                    self.pos = self
                        .pos
                        .checked_add(len)
                        .ok_or_else(|| anyhow!("CBOR length overflow"))?;
                    ensure!(self.pos <= self.bytes.len(), "truncated CBOR bytes");
                    Ok(())
                }
                None => {
                    while self.bytes.get(self.pos) != Some(&0xff) {
                        self.skip()?;
                    }
                    self.pos += 1;
                    Ok(())
                }
            },
            4 => match head.argument {
                Some(count) => {
                    for _ in 0..count {
                        self.skip()?;
                    }
                    Ok(())
                }
                None => {
                    while self.bytes.get(self.pos) != Some(&0xff) {
                        self.skip()?;
                    }
                    self.pos += 1;
                    Ok(())
                }
            },
            5 => match head.argument {
                Some(count) => {
                    for _ in 0..count {
                        self.skip()?;
                        self.skip()?;
                    }
                    Ok(())
                }
                None => {
                    while self.bytes.get(self.pos) != Some(&0xff) {
                        self.skip()?;
                        self.skip()?;
                    }
                    self.pos += 1;
                    Ok(())
                }
            },
            6 => self.skip(),
            _ => bail!("invalid CBOR major type"),
        }
    }
}

fn f16_to_f64(bits: u16) -> f64 {
    let sign = if bits & 0x8000 == 0 { 1.0 } else { -1.0 };
    let exponent = ((bits >> 10) & 0x1f) as i32;
    let fraction = (bits & 0x03ff) as f64;
    match exponent {
        0 => sign * fraction * 2f64.powi(-24),
        31 if fraction == 0.0 => sign * f64::INFINITY,
        31 => f64::NAN,
        _ => sign * (1.0 + fraction / 1024.0) * 2f64.powi(exponent - 15),
    }
}
