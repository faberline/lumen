//! Borrowing generic WAL token preflight.
//!
//! This scans bytes only. It allocates no payload String, Vec, map, or value
//! tree. Its call stack is bounded by the matching decoder recursion limits.
//! `scan` chooses JSON for a leading non-whitespace `{` or `[`, then otherwise
//! follows the generic decoder's CBOR-first and JSON-fallback order. CBOR scans
//! one item and ignores trailing bytes, matching `ciborium::de::from_reader`.

pub(crate) mod json;

use anyhow::{anyhow, bail, ensure, Result};

use crate::ingest::infrastructure::wire_cost::token_stats::json::scan_json;

const CBOR_DEPTH: usize = 256;
const JSON_DEPTH: usize = 128;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TokenStats {
    pub text_bytes: usize,
    pub byte_string_bytes: usize,
    pub token_count: usize,
    pub largest_token_bytes: usize,
}

impl TokenStats {
    fn token(&mut self, bytes: usize, text: bool) -> Result<()> {
        self.token_count = self
            .token_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("token count overflow"))?;
        if text {
            self.text_bytes = self
                .text_bytes
                .checked_add(bytes)
                .ok_or_else(|| anyhow!("text byte count overflow"))?;
        } else {
            self.byte_string_bytes = self
                .byte_string_bytes
                .checked_add(bytes)
                .ok_or_else(|| anyhow!("byte-string byte count overflow"))?;
        }
        self.largest_token_bytes = self.largest_token_bytes.max(bytes);
        Ok(())
    }
}

pub fn scan(bytes: &[u8]) -> Result<TokenStats> {
    if matches!(
        bytes
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace()),
        Some(b'{' | b'[')
    ) {
        return scan_json(bytes);
    }
    match scan_cbor(bytes) {
        Ok(stats) => Ok(stats),
        Err(cbor) => scan_json(bytes).map_err(|json| {
            anyhow!("generic token preflight as cbor ({cbor}) or legacy json ({json})")
        }),
    }
}

pub fn scan_cbor(bytes: &[u8]) -> Result<TokenStats> {
    let mut p = Cbor {
        bytes,
        pos: 0,
        stats: TokenStats::default(),
    };
    p.item(0)?;
    Ok(p.stats)
}

/// Scan exactly one CBOR item. Unlike [`scan_cbor`], this rejects a suffix.
/// Private durable receipts use this form because their payload length is part
/// of the integrity proof. Public generic WAL decoding keeps `scan_cbor`'s
/// historical one-item, suffix-tolerant behavior.
pub fn scan_exact_cbor(bytes: &[u8]) -> Result<TokenStats> {
    let mut p = Cbor {
        bytes,
        pos: 0,
        stats: TokenStats::default(),
    };
    p.item(0)?;
    ensure!(p.pos == bytes.len(), "trailing CBOR bytes");
    Ok(p.stats)
}

struct Cbor<'a> {
    bytes: &'a [u8],
    pos: usize,
    stats: TokenStats,
}
impl<'a> Cbor<'a> {
    fn byte(&mut self) -> Result<u8> {
        let b = *self
            .bytes
            .get(self.pos)
            .ok_or_else(|| anyhow!("truncated CBOR"))?;
        self.pos += 1;
        Ok(b)
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| anyhow!("CBOR offset overflow"))?;
        let out = self
            .bytes
            .get(self.pos..end)
            .ok_or_else(|| anyhow!("truncated CBOR"))?;
        self.pos = end;
        Ok(out)
    }
    fn arg(&mut self, ai: u8) -> Result<Option<u64>> {
        let n = match ai {
            0..=23 => return Ok(Some(ai as u64)),
            24 => self.byte()? as u64,
            25 => u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as u64,
            26 => u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as u64,
            27 => u64::from_be_bytes(self.take(8)?.try_into().unwrap()),
            31 => return Ok(None),
            _ => bail!("reserved CBOR additional information"),
        };
        Ok(Some(n))
    }
    fn len(n: u64) -> Result<usize> {
        usize::try_from(n).map_err(|_| anyhow!("CBOR length does not fit usize"))
    }
    fn item(&mut self, depth: usize) -> Result<()> {
        // `ciborium::de::from_reader` starts with `recurse: 256`. Permit
        // 256 container entries and their final scalar; reject the 257th.
        ensure!(depth <= CBOR_DEPTH, "CBOR recursion limit exceeded");
        let first = self.byte()?;
        let major = first >> 5;
        let arg = self.arg(first & 31)?;
        match major {
            0 | 1 => ensure!(arg.is_some(), "indefinite CBOR integer"),
            2 | 3 => self.string(major == 3, arg)?,
            4 => self.array(arg, depth)?,
            5 => self.map(arg, depth)?,
            6 => {
                ensure!(arg.is_some(), "indefinite CBOR tag");
                self.item(depth + 1)?;
            }
            7 => match arg {
                Some(_) => {}
                None => bail!("unexpected CBOR break"),
            },
            _ => unreachable!(),
        };
        Ok(())
    }
    fn string(&mut self, text: bool, arg: Option<u64>) -> Result<()> {
        let mut total = 0usize;
        match arg {
            Some(n) => {
                let n = Self::len(n)?;
                let b = self.take(n)?;
                if text {
                    std::str::from_utf8(b).map_err(|_| anyhow!("invalid CBOR UTF-8"))?;
                }
                total = n;
            }
            None => loop {
                let head = self.byte()?;
                if head == 0xff {
                    break;
                }
                ensure!(
                    head >> 5 == if text { 3 } else { 2 } && head & 31 != 31,
                    "invalid indefinite CBOR string chunk"
                );
                let n = Self::len(self.arg(head & 31)?.unwrap())?;
                let b = self.take(n)?;
                if text {
                    std::str::from_utf8(b).map_err(|_| anyhow!("invalid CBOR UTF-8"))?;
                }
                total = total
                    .checked_add(n)
                    .ok_or_else(|| anyhow!("CBOR coalesced string overflow"))?;
            },
        }
        self.stats.token(total, text)
    }
    fn array(&mut self, arg: Option<u64>, depth: usize) -> Result<()> {
        match arg {
            Some(n) => {
                let mut n = n;
                while n > 0 {
                    self.item(depth + 1)?;
                    n -= 1;
                }
            }
            None => loop {
                if self.bytes.get(self.pos) == Some(&0xff) {
                    self.pos += 1;
                    break;
                }
                self.item(depth + 1)?;
            },
        };
        Ok(())
    }
    fn map(&mut self, arg: Option<u64>, depth: usize) -> Result<()> {
        match arg {
            Some(n) => {
                let mut n = n;
                while n > 0 {
                    self.item(depth + 1)?;
                    self.item(depth + 1)?;
                    n -= 1;
                }
            }
            None => loop {
                if self.bytes.get(self.pos) == Some(&0xff) {
                    self.pos += 1;
                    break;
                }
                self.item(depth + 1)?;
                self.item(depth + 1)?;
            },
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests;
