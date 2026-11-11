//! A small blocking JSON client for the lumen HTTP API, used by the functional phases.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::Value;

pub(super) struct HttpApi {
    base: String,
    client: reqwest::Client,
    rt: tokio::runtime::Runtime,
}

impl HttpApi {
    pub(super) fn new(base: &str) -> Result<Self> {
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            rt: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
        })
    }

    /// One request. Returns the status and the parsed JSON body (`Null` when the body is
    /// empty or not JSON).
    pub(super) fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<(u16, Value)> {
        let method = reqwest::Method::from_bytes(method.as_bytes())
            .with_context(|| format!("invalid HTTP method `{method}`"))?;
        let mut req = self.client.request(method, format!("{}{path}", self.base));
        if let Some(body) = body {
            req = req.json(body);
        }
        let (status, text) = self.rt.block_on(async {
            let response = req.send().await?;
            let status = response.status().as_u16();
            let text = response.text().await?;
            Ok::<_, anyhow::Error>((status, text))
        })?;
        let value = serde_json::from_str(&text).unwrap_or(Value::Null);
        Ok((status, value))
    }

    /// Like `call`, but fails unless the status is one of `want`. Returns the body.
    pub(super) fn expect(
        &self,
        what: &str,
        want: &[u16],
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value> {
        let (status, value) = self.call(method, path, body)?;
        if !want.contains(&status) {
            bail!("{what}: {method} {path} returned {status}, wanted {want:?}: {value}");
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum_test::{TestServer, TestServerConfig, Transport};
    use lumen::api::{router, AppState};
    use lumen::storage::Engine;

    use super::HttpApi;

    fn open_server() -> TestServer {
        let config = TestServerConfig {
            transport: Some(Transport::HttpRandomPort),
            ..Default::default()
        };
        TestServer::new_with_config(router(AppState::open(Arc::new(Engine::new()))), config)
            .expect("server")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expect_reports_unexpected_status() {
        let server = open_server();
        let url = server.server_address().expect("address").to_string();
        let result = tokio::task::spawn_blocking(move || {
            HttpApi::new(&url)?.expect("probe", &[200], "GET", "/collections/missing/stats", None)
        })
        .await
        .expect("join");
        let err = result.expect_err("404 is not wanted");
        assert!(format!("{err:#}").contains("returned 404"), "{err:#}");
    }
}
