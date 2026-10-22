//! The JSON token scan: whitespace, escaped strings, numbers, arrays and maps,
//! counted without building a value.

use anyhow::{anyhow, bail, ensure, Result};

use crate::ingest::infrastructure::wire_cost::token_stats::{TokenStats, JSON_DEPTH};

pub fn scan_json(bytes: &[u8]) -> Result<TokenStats> {
    let mut p = Json {
        bytes,
        pos: 0,
        stats: TokenStats::default(),
    };
    p.ws();
    p.value(0)?;
    p.ws();
    ensure!(p.pos == bytes.len(), "trailing JSON bytes");
    Ok(p.stats)
}
struct Json<'a> {
    bytes: &'a [u8],
    pos: usize,
    stats: TokenStats,
}
impl<'a> Json<'a> {
    fn ws(&mut self) {
        while matches!(self.bytes.get(self.pos), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.pos += 1
        }
    }
    fn peek(&self) -> Result<u8> {
        self.bytes
            .get(self.pos)
            .copied()
            .ok_or_else(|| anyhow!("truncated JSON"))
    }
    fn eat(&mut self, b: u8) -> Result<()> {
        ensure!(self.peek()? == b, "invalid JSON syntax");
        self.pos += 1;
        Ok(())
    }
    fn value(&mut self, depth: usize) -> Result<()> {
        ensure!(depth < JSON_DEPTH, "JSON recursion limit exceeded");
        self.ws();
        match self.peek()? {
            b'"' => self.string(),
            b'[' => self.array(depth + 1),
            b'{' => self.map(depth + 1),
            b't' => self.word(b"true"),
            b'f' => self.word(b"false"),
            b'n' => self.word(b"null"),
            b'-' | b'0'..=b'9' => self.number(),
            _ => bail!("invalid JSON value"),
        }
    }
    fn word(&mut self, w: &[u8]) -> Result<()> {
        ensure!(
            self.bytes.get(self.pos..self.pos + w.len()) == Some(w),
            "invalid JSON literal"
        );
        self.pos += w.len();
        Ok(())
    }
    fn string(&mut self) -> Result<()> {
        self.eat(b'"')?;
        let mut decoded = 0usize;
        loop {
            let b = self.peek()?;
            self.pos += 1;
            match b {
                b'"' => break,
                b'\\' => {
                    let e = self.peek()?;
                    self.pos += 1;
                    match e {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {
                            decoded = decoded
                                .checked_add(1)
                                .ok_or_else(|| anyhow!("JSON string overflow"))?
                        }
                        b'u' => {
                            let a = self.hex4()?;
                            let cp = if (0xd800..=0xdbff).contains(&a) {
                                self.eat(b'\\')?;
                                self.eat(b'u')?;
                                let b = self.hex4()?;
                                ensure!(
                                    (0xdc00..=0xdfff).contains(&b),
                                    "invalid JSON surrogate pair"
                                );
                                0x10000 + (((a - 0xd800) as usize) << 10) + (b - 0xdc00) as usize
                            } else {
                                ensure!(
                                    !(0xdc00..=0xdfff).contains(&a),
                                    "unpaired JSON low surrogate"
                                );
                                a as usize
                            };
                            decoded = decoded
                                .checked_add(char::from_u32(cp as u32).unwrap().len_utf8())
                                .ok_or_else(|| anyhow!("JSON string overflow"))?
                        }
                        _ => bail!("invalid JSON escape"),
                    }
                }
                0x00..=0x1f => bail!("JSON control in string"),
                0x80..=0xff => {
                    let start = self.pos - 1;
                    let width = match b {
                        0xc2..=0xdf => 2,
                        0xe0..=0xef => 3,
                        0xf0..=0xf4 => 4,
                        _ => bail!("invalid JSON UTF-8"),
                    };
                    let end = start
                        .checked_add(width)
                        .ok_or_else(|| anyhow!("JSON offset overflow"))?;
                    let s = std::str::from_utf8(
                        self.bytes
                            .get(start..end)
                            .ok_or_else(|| anyhow!("truncated JSON UTF-8"))?,
                    )
                    .map_err(|_| anyhow!("invalid JSON UTF-8"))?;
                    ensure!(s.chars().count() == 1, "invalid JSON UTF-8");
                    self.pos = end;
                    decoded = decoded
                        .checked_add(width)
                        .ok_or_else(|| anyhow!("JSON string overflow"))?
                }
                _ => {
                    decoded = decoded
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("JSON string overflow"))?
                }
            }
        }
        self.stats.token(decoded, true)
    }
    fn hex4(&mut self) -> Result<u16> {
        let b = self
            .bytes
            .get(
                self.pos
                    ..self
                        .pos
                        .checked_add(4)
                        .ok_or_else(|| anyhow!("JSON offset overflow"))?,
            )
            .ok_or_else(|| anyhow!("truncated JSON unicode escape"))?;
        self.pos += 4;
        let mut n = 0u16;
        for x in b {
            n = n
                .checked_mul(16)
                .ok_or_else(|| anyhow!("JSON hex overflow"))?;
            n = n
                .checked_add(match x {
                    b'0'..=b'9' => (x - b'0') as u16,
                    b'a'..=b'f' => (x - b'a' + 10) as u16,
                    b'A'..=b'F' => (x - b'A' + 10) as u16,
                    _ => bail!("invalid JSON unicode escape"),
                })
                .ok_or_else(|| anyhow!("JSON hex overflow"))?;
        }
        Ok(n)
    }
    fn array(&mut self, d: usize) -> Result<()> {
        self.eat(b'[')?;
        self.ws();
        if self.bytes.get(self.pos) == Some(&b']') {
            self.pos += 1;
            return Ok(());
        }
        loop {
            self.value(d)?;
            self.ws();
            match self.peek()? {
                b']' => {
                    self.pos += 1;
                    return Ok(());
                }
                b',' => self.pos += 1,
                _ => bail!("invalid JSON array"),
            }
        }
    }
    fn map(&mut self, d: usize) -> Result<()> {
        self.eat(b'{')?;
        self.ws();
        if self.bytes.get(self.pos) == Some(&b'}') {
            self.pos += 1;
            return Ok(());
        }
        loop {
            self.string()?;
            self.ws();
            self.eat(b':')?;
            self.value(d)?;
            self.ws();
            match self.peek()? {
                b'}' => {
                    self.pos += 1;
                    return Ok(());
                }
                b',' => {
                    self.pos += 1;
                    self.ws()
                }
                _ => bail!("invalid JSON object"),
            }
        }
    }
    fn number(&mut self) -> Result<()> {
        let start = self.pos;
        if self.bytes.get(self.pos) == Some(&b'-') {
            self.pos += 1
        }
        match self.peek()? {
            b'0' => self.pos += 1,
            b'1'..=b'9' => {
                self.pos += 1;
                while matches!(self.bytes.get(self.pos), Some(b'0'..=b'9')) {
                    self.pos += 1
                }
            }
            _ => bail!("invalid JSON number"),
        };
        if self.bytes.get(self.pos) == Some(&b'.') {
            self.pos += 1;
            let before = self.pos;
            while matches!(self.bytes.get(self.pos), Some(b'0'..=b'9')) {
                self.pos += 1
            }
            ensure!(self.pos > before, "invalid JSON fraction")
        };
        if matches!(self.bytes.get(self.pos), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.bytes.get(self.pos), Some(b'+' | b'-')) {
                self.pos += 1
            }
            let before = self.pos;
            while matches!(self.bytes.get(self.pos), Some(b'0'..=b'9')) {
                self.pos += 1
            }
            ensure!(self.pos > before, "invalid JSON exponent")
        };
        ensure!(self.pos > start, "invalid JSON number");
        Ok(())
    }
}
