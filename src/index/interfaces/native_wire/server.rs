//! Serving the native wire: the TCP and Unix listeners, one connection's
//! request loop, and answering a fast frame from the Engine.

use std::io::Cursor;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;

use crate::index::application::engine::Engine;
use crate::index::interfaces::native_wire::codec::{
    encode_fast_response, encode_frame, is_fast_request, read_frame, take_bound, take_str,
    take_str_ref, take_u32, FAST_RANGE, FAST_TERM, FAST_TERM_RANGE,
};
use crate::index::interfaces::native_wire::{NativeSearchRequest, NativeSearchResponse};
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::{QueryNode, RangeBound, RangeQuery, TermQuery};
use crate::shared_kernel::types::search::SearchRequest;

/// Serve native binary search on an already-bound listener.
pub async fn serve_search(listener: TcpListener, engine: Arc<Engine>) -> Result<()> {
    loop {
        let (stream, _) = listener.accept().await.context("native accept")?;
        let engine = engine.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_conn(stream, engine).await {
                tracing::debug!(error = %err, "native search connection closed");
            }
        });
    }
}

#[cfg(unix)]
pub async fn serve_unix_search(listener: UnixListener, engine: Arc<Engine>) -> Result<()> {
    loop {
        let (stream, _) = listener.accept().await.context("native unix accept")?;
        let engine = engine.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_conn(stream, engine).await {
                tracing::debug!(error = %err, "native unix search connection closed");
            }
        });
    }
}

async fn handle_conn<S>(mut stream: S, engine: Arc<Engine>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    while let Some(frame) = read_frame(&mut stream).await? {
        let out = if is_fast_request(&frame) {
            match handle_fast_frame(&engine, &frame) {
                Ok(out) => out,
                Err(err) => encode_frame(&NativeSearchResponse::Err {
                    message: err.to_string(),
                })?,
            }
        } else {
            let req: NativeSearchRequest = ciborium::de::from_reader(Cursor::new(&frame))
                .context("decode native search request")?;
            let resp = match engine.search(&req.collection_id, req.request) {
                Ok(response) => NativeSearchResponse::Ok { response },
                Err(err) => NativeSearchResponse::Err {
                    message: err.to_string(),
                },
            };
            encode_frame(&resp)?
        };
        stream
            .write_all(&out)
            .await
            .context("native write response")?;
    }
    Ok(())
}

fn handle_fast_frame(engine: &Engine, frame: &[u8]) -> Result<Vec<u8>> {
    let mut pos = 5usize;
    let op = *frame.get(4).ok_or_else(|| anyhow!("missing native op"))?;
    if op == FAST_TERM {
        let collection_id = take_str_ref(frame, &mut pos)?;
        let field = take_str_ref(frame, &mut pos)?;
        let value = take_str_ref(frame, &mut pos)?;
        let limit = take_u32(frame, &mut pos)?;
        if pos != frame.len() {
            bail!("native frame has {} trailing bytes", frame.len() - pos);
        }
        let response = engine.search_fast_string_term(collection_id, field, value, limit)?;
        return encode_fast_response(&response);
    }

    let (collection_id, request) = match op {
        FAST_RANGE => {
            let collection_id = take_str(frame, &mut pos)?;
            let field = take_str(frame, &mut pos)?;
            // FAST_RANGE is a numeric-only binary fast path (#1307 keyword-range
            // widening is JSON-API-only); wrap the raw f64 bound into `RangeBound`.
            let gte = take_bound(frame, &mut pos)?.map(RangeBound::Number);
            let lt = take_bound(frame, &mut pos)?.map(RangeBound::Number);
            let limit = take_u32(frame, &mut pos)?;
            (
                collection_id,
                SearchRequest {
                    query: QueryNode::Range(RangeQuery {
                        field,
                        gt: None,
                        gte,
                        lt,
                        lte: None,
                    }),
                    limit,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: None,
                    track_total: true,
                    collapse: None,
                },
            )
        }
        FAST_TERM_RANGE => {
            let collection_id = take_str(frame, &mut pos)?;
            let term_field = take_str(frame, &mut pos)?;
            let term_value = take_str(frame, &mut pos)?;
            let range_field = take_str(frame, &mut pos)?;
            let gte = take_bound(frame, &mut pos)?.map(RangeBound::Number);
            let lt = take_bound(frame, &mut pos)?.map(RangeBound::Number);
            let limit = take_u32(frame, &mut pos)?;
            (
                collection_id,
                SearchRequest {
                    query: QueryNode::And(vec![
                        QueryNode::Term(TermQuery {
                            field: term_field,
                            value: FieldValue::String(term_value),
                        }),
                        QueryNode::Range(RangeQuery {
                            field: range_field,
                            gt: None,
                            gte,
                            lt,
                            lte: None,
                        }),
                    ]),
                    limit,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: None,
                    track_total: true,
                    collapse: None,
                },
            )
        }
        _ => bail!("unknown native op {op}"),
    };
    if pos != frame.len() {
        bail!("native frame has {} trailing bytes", frame.len() - pos);
    }
    let response = engine.search(&collection_id, request)?;
    encode_fast_response(&response)
}
