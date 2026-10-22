//! Native binary search wire.
//!
//! The public HTTP/JSON API remains lumen's integration surface. This module is
//! a small length-prefixed CBOR transport for Rust/native clients that need the
//! same engine over a lower fixed-cost wire, especially for sub-100us predicate
//! lookups where HTTP framing dominates the index work.

pub(crate) mod codec;
pub(crate) mod server;

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::index::interfaces::native_wire::codec::{
    decode_fast_response, decode_response, is_fast_response, read_frame,
};
use crate::shared_kernel::types::search::{SearchRequest, SearchResponse};

#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use tokio::net::TcpListener;

#[cfg(test)]
use crate::index::application::engine::Engine;
#[cfg(test)]
use crate::index::interfaces::native_wire::codec::{encode_search_frame, encode_term_frame};
#[cfg(test)]
use crate::index::interfaces::native_wire::server::serve_search;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeSearchRequest {
    pub collection_id: String,
    pub request: SearchRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum NativeSearchResponse {
    Ok { response: SearchResponse },
    Err { message: String },
}

/// Send one already-encoded native search request and decode its response.
pub async fn search_prepared<S>(stream: &mut S, frame: &[u8]) -> Result<SearchResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream
        .write_all(frame)
        .await
        .context("native write request")?;
    let Some(resp_frame) = read_frame(stream).await.context("native read response")? else {
        bail!("native connection closed before response");
    };
    if is_fast_response(&resp_frame) {
        return decode_fast_response(&resp_frame);
    }
    match decode_response(&resp_frame)? {
        NativeSearchResponse::Ok { response } => Ok(response),
        NativeSearchResponse::Err { message } => Err(anyhow!(message)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_kernel::types::query::{QueryNode, TermQuery};
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn native_search_round_trips_on_persistent_conn() {
        let engine = Arc::new(Engine::new());
        engine
            .create_collection(
                "docs",
                crate::shared_kernel::types::schema::CreateCollectionRequest {
                    fields: [(
                        "city".to_string(),
                        crate::shared_kernel::types::schema::FieldSpec {
                            field_type: crate::shared_kernel::types::schema::FieldType::Keyword,
                            analyzer: None,
                            multi: None,
                            dim: None,
                            metric: None,
                            backend: None,
                            quantize: None,
                        },
                    )]
                    .into_iter()
                    .collect(),
                },
            )
            .unwrap();
        engine
            .index(
                "docs",
                crate::shared_kernel::types::document::IndexRequest {
                    items: vec![crate::shared_kernel::types::document::IndexItem {
                        external_id: "a".to_string(),
                        field: "city".to_string(),
                        value: crate::shared_kernel::types::document::FieldValue::String(
                            "taipei".to_string(),
                        ),
                        version: None,
                    }],
                    request_id: None,
                },
            )
            .unwrap();

        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::AddrNotAvailable
                ) =>
            {
                return;
            }
            Err(err) => panic!("bind native wire test listener: {err}"),
        };
        let addr = listener.local_addr().unwrap();
        let serve_engine = engine.clone();
        tokio::spawn(async move {
            let _ = serve_search(listener, serve_engine).await;
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let frame = encode_search_frame(
            "docs",
            &SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "city".to_string(),
                    value: crate::shared_kernel::types::document::FieldValue::String(
                        "taipei".to_string(),
                    ),
                }),
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap();
        let first = search_prepared(&mut stream, &frame).await.unwrap();
        let second = search_prepared(&mut stream, &frame).await.unwrap();
        let fast = search_prepared(
            &mut stream,
            &encode_term_frame("docs", "city", "taipei", 10).unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(first.total, 1);
        assert_eq!(second.hits[0].external_id, "a");
        assert_eq!(fast.hits[0].external_id, "a");
    }
}
