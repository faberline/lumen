//! #11: the peer plane fails closed on the other planes' credentials, and
//! the refusal paths log no key material. Offline only; the live GKE proof
//! is owned by the GKE acceptance.

use std::io;
use std::sync::{Arc, Mutex};

use peer_tls::install_default_crypto_provider;
use tracing_subscriber::fmt::MakeWriter;

use super::peer_mtls::{authority, material, peer_listener};

#[derive(Clone, Default)]
struct SharedWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

struct SharedWriterGuard {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for SharedWriterGuard {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes.lock().unwrap().extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for SharedWriter {
    type Writer = SharedWriterGuard;

    fn make_writer(&'writer self) -> Self::Writer {
        SharedWriterGuard {
            bytes: Arc::clone(&self.bytes),
        }
    }
}

impl SharedWriter {
    fn output(&self) -> String {
        String::from_utf8(self.bytes.lock().unwrap().clone()).unwrap()
    }
}

/// Captures every log line on this thread while the guard lives.
fn capture() -> (SharedWriter, tracing::subscriber::DefaultGuard) {
    let writer = SharedWriter::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(writer.clone())
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (writer, guard)
}

/// Fails if the captured output holds a PEM marker or a line of the key body.
fn assert_no_key_material(writer: &SharedWriter, key_path: &std::path::Path) {
    let output = writer.output();
    assert!(
        !output.contains("-----BEGIN"),
        "a refusal logged a PEM marker: {output}"
    );
    let key = std::fs::read_to_string(key_path).expect("read key");
    let body = key.lines().nth(1).expect("a PEM key has a body line");
    assert!(
        !output.contains(body),
        "a refusal logged a line of the private key body"
    );
}

#[tokio::test]
async fn peer_leaf_is_refused_where_only_the_serving_ca_is_trusted() {
    install_default_crypto_provider();
    let serving_ca = authority("serving test CA");
    let peer_ca = authority("peer test CA");
    let (writer, _guard) = capture();

    // Bad case: a peer-identity leaf offered as the serving certificate, to
    // a client that trusts only the serving CA.
    let server = material(&peer_ca, &peer_ca, &["localhost"]);
    let listener = peer_listener(&server.config).await;
    let client = material(&serving_ca, &serving_ca, &["localhost"]);
    let refused = client
        .config
        .peer_transport()
        .unwrap()
        .http_client()
        .post(format!("https://localhost:{}/raft/append", listener.port))
        .send()
        .await;
    assert!(
        refused.is_err(),
        "a peer leaf must not pass as the serving certificate"
    );
    assert!(
        !listener.reached(),
        "a refused serving certificate must not reach the Raft router"
    );
    listener.stop().await;
    assert_no_key_material(&writer, &server.config.key);

    // Control: the serving leaf on the same client succeeds.
    let server = material(&serving_ca, &serving_ca, &["localhost"]);
    let listener = peer_listener(&server.config).await;
    let response = client
        .config
        .peer_transport()
        .unwrap()
        .http_client()
        .post(format!("https://localhost:{}/raft/append", listener.port))
        .send()
        .await
        .expect("the serving leaf must be accepted by the serving CA client");
    assert!(response.status().is_success(), "{:?}", response.status());
    assert!(listener.reached(), "the control must reach the Raft router");
    listener.stop().await;
}

#[tokio::test]
async fn serving_ca_leaf_as_peer_client_certificate_never_reaches_the_raft_router() {
    install_default_crypto_provider();
    let serving_ca = authority("serving test CA");
    let peer_ca = authority("peer test CA");
    let (writer, _guard) = capture();

    // Bad case: a serving-CA leaf as the client certificate. The impostor
    // trusts the peer CA, so only the client-certificate direction can fail.
    let server = material(&peer_ca, &peer_ca, &["localhost"]);
    let impostor = material(&serving_ca, &peer_ca, &["localhost"]);
    let listener = peer_listener(&server.config).await;
    let refused = impostor
        .config
        .peer_transport()
        .unwrap()
        .http_client()
        .post(format!("https://localhost:{}/raft/append", listener.port))
        .send()
        .await;
    assert!(
        refused.is_err(),
        "a serving-CA client certificate must be rejected"
    );
    assert!(
        !listener.reached(),
        "a serving-CA client certificate must not reach the Raft router"
    );
    listener.stop().await;
    assert_no_key_material(&writer, &server.config.key);

    // Control: a peer-CA client certificate on the same listener succeeds.
    let peer = material(&peer_ca, &peer_ca, &["localhost"]);
    let listener = peer_listener(&server.config).await;
    let response = peer
        .config
        .peer_transport()
        .unwrap()
        .http_client()
        .post(format!("https://localhost:{}/raft/append", listener.port))
        .send()
        .await
        .expect("a peer-CA client certificate must be accepted");
    assert!(response.status().is_success(), "{:?}", response.status());
    assert!(listener.reached(), "the control must reach the Raft router");
    listener.stop().await;
}
