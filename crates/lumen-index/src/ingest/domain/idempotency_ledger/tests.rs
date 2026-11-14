use super::*;
use std::sync::{Arc, Mutex};

fn decode_err(bytes: &[u8]) -> Option<LedgerError> {
    IdempotencyLedger::decode(bytes).err()
}

fn d(s: &str) -> PayloadDigest {
    payload_digest(s.as_bytes())
}

const T0: Duration = Duration::from_secs(1_000);

#[test]
fn payload_digest_is_sha256() {
    let hex: String = payload_digest(b"abc")
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(
        hex,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn first_then_in_flight_then_replay() {
    let mut l = IdempotencyLedger::new();
    assert_eq!(l.begin("k", d("a"), T0).unwrap(), Begin::First);
    assert_eq!(l.begin("k", d("a"), T0).unwrap(), Begin::InFlight);
    assert_eq!(l.complete("k", b"resp".to_vec(), T0), Ok(()));
    assert_eq!(
        l.begin("k", d("a"), T0).unwrap(),
        Begin::Replay(b"resp".to_vec())
    );
}

// Negative control: if `begin` compared only the key and ignored the digest,
// both assertions below would fail (the author ran that mutation and reverted it).
#[test]
fn different_payload_is_conflict() {
    let mut l = IdempotencyLedger::new();
    assert_eq!(l.begin("k", d("a"), T0).unwrap(), Begin::First);
    assert_eq!(l.begin("k", d("b"), T0).unwrap(), Begin::Conflict);
    assert_eq!(l.complete("k", b"resp".to_vec(), T0), Ok(()));
    assert_eq!(l.begin("k", d("b"), T0).unwrap(), Begin::Conflict);
}

#[test]
fn abort_releases_a_reservation() {
    let mut l = IdempotencyLedger::new();
    assert_eq!(l.begin("k", d("a"), T0).unwrap(), Begin::First);
    assert!(l.abort("k"));
    assert_eq!(l.begin("k", d("a"), T0).unwrap(), Begin::First);
    assert_eq!(l.complete("k", b"r".to_vec(), T0), Ok(()));
    assert!(!l.abort("k"));
    assert_eq!(
        l.begin("k", d("a"), T0).unwrap(),
        Begin::Replay(b"r".to_vec())
    );
    assert!(!l.abort("missing"));
}

#[test]
fn complete_requires_a_live_reservation() {
    let mut l = IdempotencyLedger::new();
    assert_eq!(
        l.complete("missing", b"x".to_vec(), T0),
        Err(LedgerError::NotReserved)
    );
    assert_eq!(l.begin("k", d("a"), T0).unwrap(), Begin::First);
    assert_eq!(l.complete("k", b"first".to_vec(), T0), Ok(()));
    assert_eq!(
        l.complete("k", b"second".to_vec(), T0),
        Err(LedgerError::NotReserved)
    );
    assert_eq!(
        l.begin("k", d("a"), T0).unwrap(),
        Begin::Replay(b"first".to_vec())
    );
    assert_eq!(l.begin("e", d("a"), T0).unwrap(), Begin::First);
    assert_eq!(
        l.complete("e", b"x".to_vec(), T0 + RETENTION),
        Err(LedgerError::NotReserved)
    );
}

#[test]
fn expiry_boundary_is_exactly_24_hours() {
    let mut l = IdempotencyLedger::new();
    assert_eq!(l.begin("k", d("a"), T0).unwrap(), Begin::First);
    assert_eq!(l.complete("k", b"resp".to_vec(), T0), Ok(()));
    assert_eq!(
        l.begin("k", d("a"), T0 + RETENTION - Duration::from_nanos(1))
            .unwrap(),
        Begin::Replay(b"resp".to_vec())
    );
    assert_eq!(l.begin("k", d("a"), T0 + RETENTION).unwrap(), Begin::First);
}

#[test]
fn gc_drops_only_expired_entries() {
    let mut l = IdempotencyLedger::new();
    assert_eq!(l.begin("old", d("a"), T0).unwrap(), Begin::First);
    assert_eq!(
        l.begin("new", d("a"), T0 + RETENTION).unwrap(),
        Begin::First
    );
    assert_eq!(l.gc(T0 + RETENTION), 1);
    assert_eq!(l.len(), 1);
    assert_eq!(
        l.begin("new", d("a"), T0 + RETENTION).unwrap(),
        Begin::InFlight
    );
}

#[test]
fn cap_rejects_instead_of_evicting_a_live_entry() {
    let mut l = IdempotencyLedger::with_max_entries(2);
    let later = T0 + Duration::from_secs(1);
    assert_eq!(l.begin("a", d("a"), T0).unwrap(), Begin::First);
    assert_eq!(l.begin("b", d("b"), T0).unwrap(), Begin::First);
    assert_eq!(
        l.begin("c", d("c"), later),
        Err(LedgerError::Full { max_entries: 2 })
    );
    assert_eq!(l.begin("a", d("a"), later).unwrap(), Begin::InFlight);
    assert_eq!(l.begin("b", d("b"), later).unwrap(), Begin::InFlight);
    assert_eq!(l.begin("c", d("c"), T0 + RETENTION).unwrap(), Begin::First);
    assert_eq!(l.len(), 1);
}

#[test]
fn cap_does_not_block_a_known_key() {
    let mut l = IdempotencyLedger::with_max_entries(1);
    assert_eq!(l.begin("a", d("a"), T0).unwrap(), Begin::First);
    assert_eq!(l.begin("a", d("a"), T0).unwrap(), Begin::InFlight);
}

#[test]
fn encode_decode_round_trips_every_state() {
    let mut l = IdempotencyLedger::new();
    assert_eq!(l.begin("r", d("r"), T0).unwrap(), Begin::First);
    assert_eq!(l.begin("c", d("c"), T0).unwrap(), Begin::First);
    assert_eq!(l.complete("c", b"x".to_vec(), T0), Ok(()));
    assert_eq!(l.begin("e", d("e"), T0).unwrap(), Begin::First);
    assert_eq!(l.complete("e", b"e".to_vec(), T0), Ok(()));
    // "e" is stored at T0 and becomes expired at T0 + RETENTION. Nothing is gc'd.

    let bytes = l.encode();
    let mut decoded = IdempotencyLedger::decode(&bytes).unwrap();
    assert_eq!(decoded.encode(), bytes);
    assert_eq!(decoded.len(), 3);
    let later = T0 + Duration::from_secs(1);
    assert_eq!(decoded.begin("r", d("r"), later).unwrap(), Begin::InFlight);
    assert_eq!(
        decoded.begin("c", d("c"), later).unwrap(),
        Begin::Replay(b"x".to_vec())
    );
    // The expired-but-not-gc'd entry survives decode and is still expired.
    assert_eq!(
        decoded.begin("e", d("e"), T0 + RETENTION).unwrap(),
        Begin::First
    );
}

#[test]
fn encode_is_deterministic() {
    let mut forward = IdempotencyLedger::new();
    let mut backward = IdempotencyLedger::new();
    for key in ["a", "b", "c"] {
        assert_eq!(forward.begin(key, d(key), T0).unwrap(), Begin::First);
    }
    for key in ["c", "b", "a"] {
        assert_eq!(backward.begin(key, d(key), T0).unwrap(), Begin::First);
    }
    assert_eq!(forward.encode(), backward.encode());
}

#[test]
fn decode_rejects_bad_input() {
    let mut l = IdempotencyLedger::new();
    assert_eq!(l.begin("k", d("a"), T0).unwrap(), Begin::First);
    assert_eq!(l.complete("k", b"resp".to_vec(), T0), Ok(()));
    let valid = l.encode();

    for i in 0..valid.len() {
        assert_eq!(
            decode_err(&valid[..i]),
            Some(LedgerError::Truncated),
            "prefix of {i} bytes must be Truncated"
        );
    }

    let mut bad_magic = valid.clone();
    bad_magic[0] ^= 0xff;
    assert_eq!(decode_err(&bad_magic), Some(LedgerError::BadMagic));

    let mut bad_version = valid.clone();
    bad_version[4] = 9;
    assert_eq!(
        decode_err(&bad_version),
        Some(LedgerError::UnknownVersion(9))
    );

    let mut trailing = valid.clone();
    trailing.push(0);
    assert_eq!(decode_err(&trailing), Some(LedgerError::Corrupt));

    // Header 9 bytes, key length 4 + key 1, digest 32, stored_at 12, then state.
    let state_at = 9 + 4 + 1 + 32 + 12;
    assert_eq!(valid[state_at], 1);
    let mut bad_state = valid.clone();
    bad_state[state_at] = 7;
    assert_eq!(decode_err(&bad_state), Some(LedgerError::Corrupt));

    let mut big = b"LIDG".to_vec();
    big.push(1);
    big.extend_from_slice(&(DEFAULT_MAX_ENTRIES as u32 + 1).to_le_bytes());
    assert_eq!(
        decode_err(&big),
        Some(LedgerError::Full {
            max_entries: DEFAULT_MAX_ENTRIES
        })
    );
}

#[test]
fn concurrent_first_use_yields_one_first() {
    let ledger = Arc::new(Mutex::new(IdempotencyLedger::new()));
    let handles: Vec<_> = (0..16)
        .map(|_| {
            let ledger = Arc::clone(&ledger);
            std::thread::spawn(move || {
                let mut guard = ledger.lock().unwrap();
                guard.begin("same", d("p"), T0).unwrap()
            })
        })
        .collect();
    let results: Vec<Begin> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|b| **b == Begin::First).count(), 1);
    assert_eq!(
        results.iter().filter(|b| **b == Begin::InFlight).count(),
        15
    );
}

#[test]
fn debug_redacts_key_and_response() {
    let mut l = IdempotencyLedger::new();
    assert_eq!(l.begin("secret-key-123", d("a"), T0).unwrap(), Begin::First);
    assert_eq!(
        l.complete("secret-key-123", b"secret-response".to_vec(), T0),
        Ok(())
    );
    let ledger_debug = format!("{l:?}");
    assert!(ledger_debug.contains("len"));
    assert!(!ledger_debug.contains("secret-key-123"));
    assert!(!ledger_debug.contains("secret-response"));
    let begin_debug = format!("{:?}", Begin::Replay(b"secret-response".to_vec()));
    assert!(!begin_debug.contains("secret-response"));
}

#[test]
fn ledger_holds_digest_not_payload() {
    let mut l = IdempotencyLedger::new();
    assert_eq!(
        l.begin("k", d("payload-body-xyz"), T0).unwrap(),
        Begin::First
    );
    let needle = b"payload-body-xyz";
    let encoded = l.encode();
    assert!(!encoded.windows(needle.len()).any(|w| w == needle));
}
