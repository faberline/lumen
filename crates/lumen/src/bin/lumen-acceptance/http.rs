//! A small blocking JSON client for the lumen HTTP API. The functional phases use it.

use std::time::Duration;

use anyhow::{bail, Result};
use serde_json::Value;

pub(super) struct HttpApi {
    base: String,
    client: reqwest::Client,
    rt: tokio::runtime::Runtime,
}

impl HttpApi {
    pub(super) fn new(base: &str) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            client,
            rt,
        })
    }

    /// Sends one request and returns its status and parsed JSON body. An empty or
    /// non-JSON body comes back as `Value::Null`.
    pub(super) fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<(u16, Value)> {
        let method = reqwest::Method::from_bytes(method.as_bytes())?;
        let mut req = self
            .client
            .request(method, format!("{}{}", self.base, path));
        if let Some(body) = body {
            req = req.json(body);
        }
        let (status, text) = self.rt.block_on(async {
            let resp = req.send().await?;
            let status = resp.status().as_u16();
            let text = resp.text().await?;
            Ok::<_, anyhow::Error>((status, text))
        })?;
        let value = if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or(Value::Null)
        };
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
pub(super) mod tests {
    use std::sync::Arc;

    use axum_test::TestServer;
    use lumen::api::{router, AppState};
    use lumen::storage::Engine;

    use super::HttpApi;

    /// A real HTTP server on a random port, so the blocking client can reach it.
    pub(crate) fn serve_for_test() -> (TestServer, String) {
        let app = router(AppState::open(Arc::new(Engine::new())));
        let server = TestServer::builder()
            .http_transport()
            .build(app)
            .expect("open server");
        let url = server.server_address().expect("address").to_string();
        (server, url)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expect_reports_unexpected_status() {
        let (_server, url) = serve_for_test();
        let err = tokio::task::spawn_blocking(move || {
            HttpApi::new(&url).expect("client").expect(
                "probe",
                &[200],
                "GET",
                "/collections/missing/stats",
                None,
            )
        })
        .await
        .expect("join")
        .expect_err("a 404 is not an expected status");
        assert!(format!("{err:#}").contains("returned 404"), "{err:#}");
    }
}
