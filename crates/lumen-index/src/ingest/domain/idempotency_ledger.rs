//! Payload-bound idempotency ledger (not wired into any write path yet).
//!
//! The ledger keeps, per idempotency key, the SHA-256 digest of the request
//! payload and, once the write completes, the original response bytes. It
//! holds digests and response bytes only, never the request payload.
//!
//! Time is an argument: every method takes `now` as a `Duration` since the
//! Unix epoch, so the module never reads a clock.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

/// Retention of every entry, counted from the `now` of the `begin` that reserved it.
pub const RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
/// Default entry cap of `IdempotencyLedger::new()`.
pub const DEFAULT_MAX_ENTRIES: usize = 100_000;

const MAGIC: &[u8; 4] = b"LIDG";
const VERSION: u8 = 1;
const STATE_RESERVED: u8 = 0;
const STATE_COMPLETE: u8 = 1;

pub type PayloadDigest = [u8; 32];

/// SHA-256 over the caller-supplied payload bytes.
pub fn payload_digest(payload: &[u8]) -> PayloadDigest {
    Sha256::digest(payload).into()
}

#[derive(Clone, PartialEq, Eq)]
pub enum Begin {
    /// Key reserved by this call.
    First,
    /// Same key and digest, already completed: the original response bytes.
    Replay(Vec<u8>),
    /// Same key and digest, reserved but not completed.
    InFlight,
    /// Same key, different digest.
    Conflict,
}

impl fmt::Debug for Begin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Begin::First => f.write_str("First"),
            Begin::Replay(bytes) => write!(f, "Replay(<{} bytes>)", bytes.len()),
            Begin::InFlight => f.write_str("InFlight"),
            Begin::Conflict => f.write_str("Conflict"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LedgerError {
    #[error("idempotency ledger is full ({max_entries} unexpired entries)")]
    Full { max_entries: usize },
    #[error("idempotency key is not reserved")]
    NotReserved,
    #[error("idempotency ledger bytes are truncated")]
    Truncated,
    #[error("idempotency ledger bytes have a bad magic")]
    BadMagic,
    #[error("unknown idempotency ledger version {0}")]
    UnknownVersion(u8),
    #[error("idempotency ledger bytes are corrupt")]
    Corrupt,
}

/// One key's state. No `Debug` derive: it would print response bytes.
struct Entry {
    digest: PayloadDigest,
    stored_at: Duration,
    /// `None` means reserved (in flight); `Some` means completed.
    response: Option<Vec<u8>>,
}

impl Entry {
    /// Live while `now < stored_at + RETENTION`.
    fn is_live(&self, now: Duration) -> bool {
        match self.stored_at.checked_add(RETENTION) {
            Some(end) => now < end,
            None => true,
        }
    }
}

pub struct IdempotencyLedger {
    max_entries: usize,
    entries: BTreeMap<String, Entry>,
}

impl IdempotencyLedger {
    pub fn new() -> Self {
        Self::with_max_entries(DEFAULT_MAX_ENTRIES)
    }

    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            max_entries,
            entries: BTreeMap::new(),
        }
    }

    /// Stored entries, expired or not.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn begin(
        &mut self,
        key: &str,
        digest: PayloadDigest,
        now: Duration,
    ) -> Result<Begin, LedgerError> {
        if let Some(entry) = self.entries.get(key) {
            if entry.is_live(now) {
                if entry.digest != digest {
                    return Ok(Begin::Conflict);
                }
                return Ok(match &entry.response {
                    Some(response) => Begin::Replay(response.clone()),
                    None => Begin::InFlight,
                });
            }
        }
        if !self.entries.contains_key(key) && self.entries.len() >= self.max_entries {
            self.gc(now);
            if self.entries.len() >= self.max_entries {
                return Err(LedgerError::Full {
                    max_entries: self.max_entries,
                });
            }
        }
        self.entries.insert(
            key.to_string(),
            Entry {
                digest,
                stored_at: now,
                response: None,
            },
        );
        Ok(Begin::First)
    }

    pub fn complete(
        &mut self,
        key: &str,
        response: Vec<u8>,
        now: Duration,
    ) -> Result<(), LedgerError> {
        match self.entries.get_mut(key) {
            Some(entry) if entry.is_live(now) && entry.response.is_none() => {
                entry.response = Some(response);
                Ok(())
            }
            _ => Err(LedgerError::NotReserved),
        }
    }

    /// Releases a reservation. Returns `false` (and changes nothing) when the
    /// key is missing or already completed.
    pub fn abort(&mut self, key: &str) -> bool {
        if self
            .entries
            .get(key)
            .is_some_and(|entry| entry.response.is_none())
        {
            self.entries.remove(key);
            true
        } else {
            false
        }
    }

    /// Drops exactly the expired entries and returns how many were dropped.
    pub fn gc(&mut self, now: Duration) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, entry| entry.is_live(now));
        before - self.entries.len()
    }

    /// Deterministic byte form, including expired entries not yet gc'd.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.push(VERSION);
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (key, entry) in &self.entries {
            out.extend_from_slice(&(key.len() as u32).to_le_bytes());
            out.extend_from_slice(key.as_bytes());
            out.extend_from_slice(&entry.digest);
            out.extend_from_slice(&entry.stored_at.as_secs().to_le_bytes());
            out.extend_from_slice(&entry.stored_at.subsec_nanos().to_le_bytes());
            match &entry.response {
                None => out.push(STATE_RESERVED),
                Some(response) => {
                    out.push(STATE_COMPLETE);
                    out.extend_from_slice(&(response.len() as u32).to_le_bytes());
                    out.extend_from_slice(response);
                }
            }
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, LedgerError> {
        let mut reader = Reader { bytes };
        if reader.take(4)? != MAGIC {
            return Err(LedgerError::BadMagic);
        }
        let version = reader.take(1)?[0];
        if version != VERSION {
            return Err(LedgerError::UnknownVersion(version));
        }
        let count = reader.u32()? as usize;
        if count > DEFAULT_MAX_ENTRIES {
            return Err(LedgerError::Full {
                max_entries: DEFAULT_MAX_ENTRIES,
            });
        }
        let mut ledger = Self::new();
        for _ in 0..count {
            let key_len = reader.u32()? as usize;
            let key =
                std::str::from_utf8(reader.take(key_len)?).map_err(|_| LedgerError::Corrupt)?;
            let digest: PayloadDigest = reader
                .take(32)?
                .try_into()
                .map_err(|_| LedgerError::Truncated)?;
            let secs = reader.u64()?;
            let nanos = reader.u32()?;
            if nanos >= 1_000_000_000 {
                return Err(LedgerError::Corrupt);
            }
            let stored_at = Duration::new(secs, nanos);
            let response = match reader.take(1)?[0] {
                STATE_RESERVED => None,
                STATE_COMPLETE => {
                    let len = reader.u32()? as usize;
                    Some(reader.take(len)?.to_vec())
                }
                _ => return Err(LedgerError::Corrupt),
            };
            ledger.entries.insert(
                key.to_string(),
                Entry {
                    digest,
                    stored_at,
                    response,
                },
            );
        }
        if !reader.bytes.is_empty() {
            return Err(LedgerError::Corrupt);
        }
        Ok(ledger)
    }
}

impl Default for IdempotencyLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for IdempotencyLedger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdempotencyLedger")
            .field("len", &self.entries.len())
            .field("max_entries", &self.max_entries)
            .finish()
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], LedgerError> {
        if self.bytes.len() < n {
            return Err(LedgerError::Truncated);
        }
        let (head, tail) = self.bytes.split_at(n);
        self.bytes = tail;
        Ok(head)
    }

    fn u32(&mut self) -> Result<u32, LedgerError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| LedgerError::Truncated)?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, LedgerError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| LedgerError::Truncated)?;
        Ok(u64::from_le_bytes(bytes))
    }
}

#[cfg(test)]
mod tests;
