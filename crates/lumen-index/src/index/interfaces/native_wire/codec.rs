//! The frame layouts both ends share: a length-prefixed CBOR frame for any
//! request, the fixed binary fast frames for term and range lookups and their
//! response, and reading one frame off a stream.

use std::io::Cursor;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::index::interfaces::native_wire::{NativeSearchRequest, NativeSearchResponse};
use crate::shared_kernel::types::search::{SearchHit, SearchRequest, SearchResponse};

const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const FAST_MAGIC: &[u8; 3] = b"LMN";
const FAST_VER: u8 = 1;
pub(super) const FAST_TERM: u8 = 1;
pub(super) const FAST_RANGE: u8 = 2;
pub(super) const FAST_TERM_RANGE: u8 = 3;
const FAST_RESPONSE: u8 = 0x80;
const FAST_OK: u8 = 0;

/// Encode a prepared search frame that can be written repeatedly on a persistent
/// connection. This is the native analogue of pg's prepared statement path in
/// the competitive gate.
pub fn encode_search_frame(collection_id: &str, request: &SearchRequest) -> Result<Vec<u8>> {
    encode_frame(&NativeSearchRequest {
        collection_id: collection_id.to_string(),
        request: request.clone(),
    })
}

pub fn encode_term_frame(
    collection_id: &str,
    field: &str,
    value: &str,
    limit: u32,
) -> Result<Vec<u8>> {
    let mut payload = fast_header(FAST_TERM);
    put_str(&mut payload, collection_id)?;
    put_str(&mut payload, field)?;
    put_str(&mut payload, value)?;
    payload.extend_from_slice(&limit.to_be_bytes());
    frame_payload(payload)
}

pub fn encode_range_frame(
    collection_id: &str,
    field: &str,
    gte: Option<f64>,
    lt: Option<f64>,
    limit: u32,
) -> Result<Vec<u8>> {
    let mut payload = fast_header(FAST_RANGE);
    put_str(&mut payload, collection_id)?;
    put_str(&mut payload, field)?;
    put_bound(&mut payload, gte);
    put_bound(&mut payload, lt);
    payload.extend_from_slice(&limit.to_be_bytes());
    frame_payload(payload)
}

pub fn encode_term_range_frame(
    collection_id: &str,
    term_field: &str,
    term_value: &str,
    range_field: &str,
    gte: Option<f64>,
    lt: Option<f64>,
    limit: u32,
) -> Result<Vec<u8>> {
    let mut payload = fast_header(FAST_TERM_RANGE);
    put_str(&mut payload, collection_id)?;
    put_str(&mut payload, term_field)?;
    put_str(&mut payload, term_value)?;
    put_str(&mut payload, range_field)?;
    put_bound(&mut payload, gte);
    put_bound(&mut payload, lt);
    payload.extend_from_slice(&limit.to_be_bytes());
    frame_payload(payload)
}

pub(super) fn encode_frame<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    ciborium::ser::into_writer(value, &mut payload).context("encode native frame")?;
    frame_payload(payload)
}

fn frame_payload(payload: Vec<u8>) -> Result<Vec<u8>> {
    if payload.len() > MAX_FRAME_BYTES {
        bail!(
            "native frame too large: {} > {}",
            payload.len(),
            MAX_FRAME_BYTES
        );
    }
    let len = payload.len() as u32;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn fast_header(op: u8) -> Vec<u8> {
    let mut payload = Vec::with_capacity(128);
    payload.extend_from_slice(FAST_MAGIC);
    payload.push(FAST_VER);
    payload.push(op);
    payload
}

fn put_str(out: &mut Vec<u8>, s: &str) -> Result<()> {
    let len: u32 = s
        .len()
        .try_into()
        .map_err(|_| anyhow!("native string too long"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(s.as_bytes());
    Ok(())
}

fn put_bound(out: &mut Vec<u8>, value: Option<f64>) {
    match value {
        Some(v) => {
            out.push(1);
            out.extend_from_slice(&v.to_bits().to_be_bytes());
        }
        None => out.push(0),
    }
}

pub(super) fn is_fast_request(frame: &[u8]) -> bool {
    frame.len() >= 5
        && &frame[0..3] == FAST_MAGIC
        && frame[3] == FAST_VER
        && frame[4] < FAST_RESPONSE
}

pub(super) fn is_fast_response(frame: &[u8]) -> bool {
    frame.len() >= 6
        && &frame[0..3] == FAST_MAGIC
        && frame[3] == FAST_VER
        && frame[4] == FAST_RESPONSE
}

pub(super) fn encode_fast_response(response: &SearchResponse) -> Result<Vec<u8>> {
    let mut payload = fast_header(FAST_RESPONSE);
    payload.push(FAST_OK);
    payload.extend_from_slice(&response.took_us.to_be_bytes());
    payload.extend_from_slice(&response.total.to_be_bytes());
    payload.extend_from_slice(&(response.hits.len() as u32).to_be_bytes());
    for hit in &response.hits {
        put_str(&mut payload, &hit.external_id)?;
        payload.extend_from_slice(&hit.score.to_bits().to_be_bytes());
    }
    frame_payload(payload)
}

pub(super) fn decode_fast_response(frame: &[u8]) -> Result<SearchResponse> {
    let mut pos = 5usize;
    let status = *frame
        .get(pos)
        .ok_or_else(|| anyhow!("missing native response status"))?;
    pos += 1;
    if status != FAST_OK {
        bail!("native fast response status {status}");
    }
    let took_us = take_u64(frame, &mut pos)?;
    let total = take_u64(frame, &mut pos)?;
    let count = take_u32(frame, &mut pos)? as usize;
    let mut hits = Vec::with_capacity(count);
    for _ in 0..count {
        let external_id = take_str(frame, &mut pos)?;
        let score = f32::from_bits(take_u32(frame, &mut pos)?);
        hits.push(SearchHit { external_id, score });
    }
    if pos != frame.len() {
        bail!(
            "native fast response has {} trailing bytes",
            frame.len() - pos
        );
    }
    Ok(SearchResponse {
        hits,
        total,
        cursor: None,
        took_ms: took_us / 1000,
        took_us,
    })
}

fn take<'a>(frame: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
    let end = pos
        .checked_add(n)
        .ok_or_else(|| anyhow!("native frame offset overflow"))?;
    let bytes = frame
        .get(*pos..end)
        .ok_or_else(|| anyhow!("native frame truncated"))?;
    *pos = end;
    Ok(bytes)
}

pub(super) fn take_u32(frame: &[u8], pos: &mut usize) -> Result<u32> {
    let bytes: [u8; 4] = take(frame, pos, 4)?.try_into().unwrap();
    Ok(u32::from_be_bytes(bytes))
}

fn take_u64(frame: &[u8], pos: &mut usize) -> Result<u64> {
    let bytes: [u8; 8] = take(frame, pos, 8)?.try_into().unwrap();
    Ok(u64::from_be_bytes(bytes))
}

pub(super) fn take_bound(frame: &[u8], pos: &mut usize) -> Result<Option<f64>> {
    match *take(frame, pos, 1)?
        .first()
        .ok_or_else(|| anyhow!("missing native bound flag"))?
    {
        0 => Ok(None),
        1 => Ok(Some(f64::from_bits(take_u64(frame, pos)?))),
        tag => bail!("unknown native bound flag {tag}"),
    }
}

pub(super) fn take_str(frame: &[u8], pos: &mut usize) -> Result<String> {
    let len = take_u32(frame, pos)? as usize;
    let bytes = take(frame, pos, len)?;
    String::from_utf8(bytes.to_vec()).context("native string is not utf-8")
}

pub(super) fn take_str_ref<'a>(frame: &'a [u8], pos: &mut usize) -> Result<&'a str> {
    let len = take_u32(frame, pos)? as usize;
    let bytes = take(frame, pos, len)?;
    std::str::from_utf8(bytes).context("native string is not utf-8")
}

pub(super) fn decode_response(bytes: &[u8]) -> Result<NativeSearchResponse> {
    ciborium::de::from_reader(Cursor::new(bytes)).context("decode native search response")
}

pub(super) async fn read_frame<S>(stream: &mut S) -> Result<Option<Vec<u8>>>
where
    S: AsyncRead + Unpin,
{
    let mut hdr = [0u8; 4];
    match stream.read_exact(&mut hdr).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err).context("read native frame header"),
    }
    let len = u32::from_be_bytes(hdr) as usize;
    if len > MAX_FRAME_BYTES {
        bail!("native frame too large: {len} > {MAX_FRAME_BYTES}");
    }
    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .context("read native frame payload")?;
    Ok(Some(payload))
}
