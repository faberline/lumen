//! One hop to the owning shard's pod: send a request over its h2c pool and read
//! the answer back.

use anyhow::{Context, Result};
use axum::http::HeaderMap;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::sharding::domain::forward_error::{ShardForwardRemoteError, ShardForwardUnavailable};
use crate::sharding::infrastructure::routed_router::{
    RoutedRouter, FORWARDED_HEADER, MAP_VERSION_HEADER, READ_CONSISTENCY_HEADER,
};

impl RoutedRouter {
    /// Sends one forwarded request and returns the raw response — success or
    /// not, decoding is the caller's job (`forward_json`/`forward_empty`)
    /// since a `DELETE` success carries no body while every other verb
    /// here returns one.
    async fn send<Req: Serialize>(
        &self,
        shard: u32,
        method: reqwest::Method,
        path: &str,
        body: Option<Req>,
        headers: &HeaderMap,
    ) -> Result<reqwest::Response> {
        let remote = self
            .remotes
            .get(shard as usize)
            .and_then(|r| r.as_ref())
            .with_context(|| format!("shard {shard} has no remote configured"))?;
        let url = format!("{}{}", remote.base_url, path);
        // Plain string header names + raw bytes, not typed `HeaderValue`s:
        // axum's and reqwest's `http` crate types aren't guaranteed to be
        // the same version, but both accept `&str`/`&[u8]` at the
        // `IntoHeaderName`/`TryInto<HeaderValue>` boundary regardless.
        let mut builder = remote
            .pool
            .client()
            .request(method.clone(), &url)
            .header(FORWARDED_HEADER, "1")
            .header(MAP_VERSION_HEADER, self.shard_map.version().to_string());
        if let Some(v) = headers.get(axum::http::header::AUTHORIZATION) {
            builder = builder.header("authorization", v.as_bytes());
        }
        if let Some(v) = headers.get(READ_CONSISTENCY_HEADER) {
            builder = builder.header(READ_CONSISTENCY_HEADER, v.as_bytes());
        }
        if let Some(b) = &body {
            builder = builder.json(b);
        }
        builder.send().await.map_err(|e| {
            anyhow::Error::new(ShardForwardUnavailable(format!("{method} {url}: {e}")))
        })
    }

    fn remote_error(status: reqwest::StatusCode, body_text: String) -> anyhow::Error {
        let message = serde_json::from_str::<service_http::ErrorEnvelope>(&body_text)
            .map(|env| format!("{}: {}", env.error, env.message))
            .unwrap_or(body_text);
        anyhow::Error::new(ShardForwardRemoteError {
            status: status.as_u16(),
            message,
        })
    }

    pub(super) async fn forward_json<Req, Resp>(
        &self,
        shard: u32,
        method: reqwest::Method,
        path: &str,
        body: Option<Req>,
        headers: &HeaderMap,
    ) -> Result<Resp>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let resp = self.send(shard, method, path, body, headers).await?;
        let status = resp.status();
        if status.is_success() {
            resp.json::<Resp>().await.map_err(|e| {
                anyhow::Error::new(ShardForwardUnavailable(format!(
                    "decode response from shard {shard} {path}: {e}"
                )))
            })
        } else {
            let body_text = resp.text().await.unwrap_or_default();
            Err(Self::remote_error(status, body_text))
        }
    }

    pub(super) async fn forward_empty(
        &self,
        shard: u32,
        method: reqwest::Method,
        path: &str,
        headers: &HeaderMap,
    ) -> Result<()> {
        let resp = self.send::<()>(shard, method, path, None, headers).await?;
        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let body_text = resp.text().await.unwrap_or_default();
            Err(Self::remote_error(status, body_text))
        }
    }

    /// Send a JSON request whose successful response is intentionally empty.
    /// `docs:unindex` is the only current write with that shape; keeping this
    /// separate from `forward_json` prevents a 204 from being decoded as JSON.
    pub(super) async fn forward_json_empty<Req>(
        &self,
        shard: u32,
        method: reqwest::Method,
        path: &str,
        body: Req,
        headers: &HeaderMap,
    ) -> Result<()>
    where
        Req: Serialize,
    {
        let resp = self.send(shard, method, path, Some(body), headers).await?;
        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let body_text = resp.text().await.unwrap_or_default();
            Err(Self::remote_error(status, body_text))
        }
    }

    /// #2496: like [`Self::forward_empty`], but a remote `404` is a
    /// legitimate [`DropOutcome::NotFound`] rather than a hard error — the
    /// `DELETE /collections/{id}` route conveys its outcome purely through
    /// status code (202/204/404), so this is the one forward primitive that
    /// must return the raw status instead of collapsing it to success/error.
    ///
    /// [`DropOutcome::NotFound`]: crate::index::application::engine::collections::DropOutcome::NotFound
    pub(super) async fn forward_drop_status(
        &self,
        shard: u32,
        path: &str,
        headers: &HeaderMap,
    ) -> Result<reqwest::StatusCode> {
        let resp = self
            .send::<()>(shard, reqwest::Method::DELETE, path, None, headers)
            .await?;
        let status = resp.status();
        if status.is_success() || status == reqwest::StatusCode::NOT_FOUND {
            Ok(status)
        } else {
            let body_text = resp.text().await.unwrap_or_default();
            Err(Self::remote_error(status, body_text))
        }
    }
}
