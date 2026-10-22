//! #2890 R5: the peer listener, exercised rather than described.
//!
//! The four tests below dial the real `PeerTransport::serve` /
//! `connect` / `accept` seams the serving binary hands the Raft router to.
//! The negative ones assert on a router that records whether it was ever
//! reached, because "the connection was refused eventually" and "the
//! replication endpoint never saw the frame" are different facts, and only
//! the second one is the security property.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use peer_tls::install_default_crypto_provider;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair,
};

use crate::access::infrastructure::tls::PeerTlsConfig;

/// A throwaway certificate authority. Two of these is what "a different
/// trust domain" means in these tests.
struct Authority {
    cert: Certificate,
    key: KeyPair,
}

fn authority(name: &str) -> Authority {
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params.distinguished_name.push(DnType::CommonName, name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let key = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    Authority { cert, key }
}

/// Peer material laid out exactly as `spec.peerTlsSecret` projects it —
/// `tls.crt`, `tls.key`, `ca.crt` — so what these tests load is what a pod
/// mounts.
struct Material {
    _dir: tempfile::TempDir,
    config: PeerTlsConfig,
}

/// `identity_ca` signs the leaf; `trust_ca` is who the holder verifies its
/// counterpart against. Separate arguments on purpose: an attacker holds a
/// certificate nobody trusts *while* trusting the real CA, which is the
/// only configuration in which the client-certificate direction is the one
/// under test.
fn material(identity_ca: &Authority, trust_ca: &Authority, dns: &[&str]) -> Material {
    let mut params =
        CertificateParams::new(dns.iter().map(|n| (*n).to_string()).collect::<Vec<_>>()).unwrap();
    params.distinguished_name.push(DnType::CommonName, dns[0]);
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let key = KeyPair::generate().unwrap();
    let cert = params
        .signed_by(&key, &identity_ca.cert, &identity_ca.key)
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("tls.crt"), cert.pem()).unwrap();
    std::fs::write(dir.path().join("tls.key"), key.serialize_pem()).unwrap();
    std::fs::write(dir.path().join("ca.crt"), trust_ca.cert.pem()).unwrap();
    Material {
        config: PeerTlsConfig {
            cert: dir.path().join("tls.crt"),
            key: dir.path().join("tls.key"),
            ca: dir.path().join("ca.crt"),
            required: true,
        },
        _dir: dir,
    }
}

/// A running peer listener plus the one bit that matters: did anything get
/// past the handshake and reach the router?
struct PeerListener {
    port: u16,
    router_reached: Arc<AtomicBool>,
    shutdown: tokio::sync::oneshot::Sender<()>,
    serve: tokio::task::JoinHandle<anyhow::Result<()>>,
}

async fn peer_listener(config: &PeerTlsConfig) -> PeerListener {
    let router_reached = Arc::new(AtomicBool::new(false));
    let flag = router_reached.clone();
    // Stands in for the Raft router: same position in `serve`, and it can
    // testify about itself.
    let router = axum::Router::new().route(
        "/raft/append",
        axum::routing::post(move || {
            let flag = flag.clone();
            async move {
                flag.store(true, Ordering::SeqCst);
                "appended"
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let transport = config.peer_transport().unwrap();
    let (shutdown, rx) = tokio::sync::oneshot::channel();
    let serve = tokio::spawn(async move {
        transport
            .serve(listener, router, async move {
                let _ = rx.await;
            })
            .await
    });
    PeerListener {
        port,
        router_reached,
        shutdown,
        serve,
    }
}

impl PeerListener {
    fn reached(&self) -> bool {
        self.router_reached.load(Ordering::SeqCst)
    }

    async fn stop(self) {
        let _ = self.shutdown.send(());
        self.serve.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn trusted_peer_pair_exchanges_a_raft_request_over_mtls() {
    install_default_crypto_provider();
    let ca = authority("lumen peer test CA");
    let server = material(&ca, &ca, &["localhost"]);
    let peer = material(&ca, &ca, &["localhost"]);
    let listener = peer_listener(&server.config).await;

    let response = peer
        .config
        .peer_transport()
        .unwrap()
        .http_client()
        .post(format!("https://localhost:{}/raft/append", listener.port))
        .send()
        .await
        .expect("a peer holding instance-scoped material must complete the exchange");

    assert!(response.status().is_success(), "{:?}", response.status());
    assert!(
        listener.reached(),
        "the trusted exchange must actually reach the Raft router"
    );
    listener.stop().await;
}

#[tokio::test]
async fn plaintext_peer_dial_never_reaches_the_raft_router() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    install_default_crypto_provider();
    let ca = authority("lumen peer test CA");
    let server = material(&ca, &ca, &["localhost"]);
    let listener = peer_listener(&server.config).await;

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", listener.port))
        .await
        .unwrap();
    // The HTTP/2 preface with no TLS underneath — exactly what the removed
    // h2c fallback used to speak on this port (#2890 R3).
    stream
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read_buf(&mut buf))
        .await
        .expect("a plaintext dial must be terminated, not left hanging");

    assert!(
        !buf.windows(8).any(|w| w == b"appended"),
        "plaintext peer got a router response back: {read:?} {buf:?}"
    );
    assert!(
        !listener.reached(),
        "a plaintext dial must die in the handshake, before the Raft router"
    );
    listener.stop().await;
}

#[tokio::test]
async fn unrelated_ca_peer_certificate_never_reaches_the_raft_router() {
    install_default_crypto_provider();
    let lumen_ca = authority("lumen peer test CA");
    let unrelated_ca = authority("unrelated CA");
    let server = material(&lumen_ca, &lumen_ca, &["localhost"]);
    // The impostor trusts Lumen's real CA, so the server's identity checks
    // out from its side; the only direction left to fail is the client
    // certificate it presents.
    let impostor = material(&unrelated_ca, &lumen_ca, &["localhost"]);
    let listener = peer_listener(&server.config).await;

    let error = impostor
        .config
        .peer_transport()
        .unwrap()
        .http_client()
        .post(format!("https://localhost:{}/raft/append", listener.port))
        .send()
        .await
        .expect_err("a certificate from another trust domain must be rejected");

    assert!(
        !listener.reached(),
        "an untrusted client certificate must be rejected before the Raft \
             router sees a message: {error}"
    );
    listener.stop().await;
}

#[tokio::test]
async fn peer_client_rejects_a_trusted_certificate_for_the_wrong_dns_name() {
    install_default_crypto_provider();
    let ca = authority("lumen peer test CA");
    // Chain-valid — signed by the instance's own CA. What is wrong is who
    // it claims to be: a member of a different headless Service. Verifying
    // the chain alone would accept it.
    let impostor = material(
        &ca,
        &ca,
        &["lumen-0.other-headless.lumen.svc.cluster.local"],
    );
    let dialer = material(
        &ca,
        &ca,
        &["lumen-1.lumen-headless.lumen.svc.cluster.local"],
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_transport = impostor.config.peer_transport().unwrap();
    let client_transport = dialer.config.peer_transport().unwrap();

    let (_server, client_result) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            async {
                let (stream, _) = listener.accept().await.unwrap();
                server_transport.accept(stream).await
            },
            async {
                let stream = tokio::net::TcpStream::connect(address).await.unwrap();
                client_transport
                    .connect(stream, "lumen-0.lumen-headless.lumen.svc.cluster.local")
                    .await
            }
        )
    })
    .await
    .expect("the wrong-identity handshake must terminate");

    let error =
        client_result.expect_err("a trusted certificate for another DNS name must not pass");
    assert!(
        error.to_string().contains("peer TLS client handshake"),
        "unexpected rejection: {error:#}"
    );
}
