use std::sync::Mutex;

use crate::access::infrastructure::tls::{PeerTlsConfig, ServingTlsConfig};

const TEST_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIC5zCCAc+gAwIBAgIJAPl6HZTX5LElMA0GCSqGSIb3DQEBCwUAMBUxEzARBgNV
BAMMCmx1bWVuLXBlZXIwHhcNMjYwNjE4MTQwODA4WhcNMjYwNzE4MTQwODA4WjAV
MRMwEQYDVQQDDApsdW1lbi1wZWVyMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
CgKCAQEAuwEAFs8xYsY9GDDbefwoV4FMiD9f49hs7iLVijVhUM7z5W0Xj9mXFCFS
Sn/DNb/bF9UtUoGJ0cdpjlevd6BjaXbjm2gMIDod1yKBZ2BXwT/elwRzjEIcTgR5
+GTu355VsWqugBYob8cYn2kGAMvVFUZeRBbC1IO02xbp9zABNaBHOWVdRTXODxiU
jbtB4gioNJOG1A71sto61lMmLMp4IL02k+BbuwekhCkkRGGNuqMHVAehkJwTmmxF
aPHK3LMifWgUXn51JWEhU2OiWe3Ja8/XQU5LZDvbc3vmMaJSuIMheOIkM5AXHyo4
LX62YgtuUpouYYOHkOqNRWRQfLrvywIDAQABozowODAVBgNVHREEDjAMggpsdW1l
bi1wZWVyMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgKkMA0GCSqGSIb3
DQEBCwUAA4IBAQAavsvsmN/zKL0TVx7FLEnDRbD6L4KNg3ndPrZDKncl0Df1W5kl
4jZTujiZ2CqH7CQakra3kV51EIUuKSbc0kQBsvsCIw0Fxb/JUmsui/z9uCqrqhrT
ODlcV6pETppce5JozMAZCKUyx9460/+flP7VTqHnLt1oMrM/mmaKeZ0ImSBnx8xF
0JpJN0HyX+vlbrT/9J3xxe53v7glRPZIgBlOT1eTaroXjIk6ZzOBS8bCBpNYVec5
wN93qI3ZQWwNUMB3TXJ7IBpgIrtD+z/ZhliDnk6NOLqKPXJrch0cVwlljT0Uu+DP
Qd9/aITxkqX7P0phj2cYmALL/aBJJaWRuAfw
-----END CERTIFICATE-----
"#;

const TEST_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC7AQAWzzFixj0Y
MNt5/ChXgUyIP1/j2GzuItWKNWFQzvPlbReP2ZcUIVJKf8M1v9sX1S1SgYnRx2mO
V693oGNpduObaAwgOh3XIoFnYFfBP96XBHOMQhxOBHn4ZO7fnlWxaq6AFihvxxif
aQYAy9UVRl5EFsLUg7TbFun3MAE1oEc5ZV1FNc4PGJSNu0HiCKg0k4bUDvWy2jrW
UyYsynggvTaT4Fu7B6SEKSREYY26owdUB6GQnBOabEVo8crcsyJ9aBRefnUlYSFT
Y6JZ7clrz9dBTktkO9tze+YxolK4gyF44iQzkBcfKjgtfrZiC25Smi5hg4eQ6o1F
ZFB8uu/LAgMBAAECggEAF23pp/HvmxOBRg2hAeiQ2V3Oy+c8yVwtUay1mmpTtf8n
2Z/Qaup1HkWKfOEDATH3bkX8NrEaJllYpUfhKRjEO8t0et0PX95ILNMa6WvNst2g
ssURAQqrZy7yZSeoMgYxcFgQYuXjzRVhxV8wLFtdaBv35YoAgQW7XBPD3n96N1CV
nRpk/tPeIaqmGnC6xhtrd9zaRy1qZ3aX5Np27ZrwsMghmJyLNI6OfsS0FRK3dKQx
dkx5L5iMD0wmqC5FsR4nc+pkFsSgdv2uxtS95JDX2jOHuJj4qm5moh0Z6eXQ8lCD
Nhr+JN1TQXHVAL696tQPnQNJtdnQYshNpsC+R2Sg+QKBgQDjsyACww2OLLIO/QBJ
rzbuAgx0n4cRR7mSCZhrgO3xX+sKU1yGPNRtwj0R/dQsVClld0KB2Oa9brR2dzcE
QWSLGcRhAmpjgmYLFn6T2Odbyb5YfTMVF6ka53w4CELKQPm7cm5QsXimTqSYPwZb
Jth2e7bkEdVemzDS8C33WYio3QKBgQDSPweWZNLC+EtHlNHg4fguww6wjkUPcoxG
C8prGovcSEMuIXUnrJmRkWdKTxHud9ofvhfauB86Daf4tkaklGPuJ5CepxnMXyos
I8fSEnIyTPD6sYC37GNUMDhMU3iyxV+CsH077TwSGpjw4cntf8pqYklP8zjctjnq
wAPG6O2cxwKBgQCTEQnW3tatgo7LAXwjG2k2FtqmpLbfYV0pRstMfCyzHwm3VJpJ
FZb7AV7idPiKXR2TrJCnP0nhBlTGwz8kn3vqIA1nvuCqPvnbpX7BzXG5Jjer/cl1
kR+nAeaIZkWFTqw99q3rroTHnbnPn71iOFfNRyCcdCxE+6VwSLLXtNuAfQKBgQDA
05QW6FOxA96vQRuY4EcqRDXV0jYeq9VhbPDyeD9sAk6zIXZ8s72JF82fBpQQnZXN
ZSAltpbVPK8g2bRCv+JDC8CE8gckPOfF4e8jiU15Or4NfvzqMwEKtsr7ndbmR0WI
7Gt/qd5dUE2TJ9J2Y6z3Ezvf+tfc/bhyyDbumLVNAwKBgHj74ZKxCKE21mv1azYk
EF1sOEisJVtdtSq2PZN7hiGgvaMTSfKegRgM+12lGDvabf93LSoYX1pEHY7qIs2f
pky/zqjmfLFtvyP+vQvAL3F+5B/1XpFj2dRnAOJaWpq62Ebe2L9k4ff7EYNTL7oq
LkjT2UdpFBDZGWHwqDRhXX8k
-----END PRIVATE KEY-----
"#;

// env vars are process-global, so the three scenarios share a
// mutex to keep them from racing under `cargo test`'s default
// parallel runner.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn clear_env() {
    unsafe {
        std::env::remove_var("LUMEN_PEER_TLS_CERT");
        std::env::remove_var("LUMEN_PEER_TLS_KEY");
        std::env::remove_var("LUMEN_PEER_TLS_CA");
        std::env::remove_var("LUMEN_PEER_MTLS");
        std::env::remove_var("LUMEN_TLS");
        std::env::remove_var("LUMEN_TLS_CERT");
        std::env::remove_var("LUMEN_TLS_KEY");
        std::env::remove_var("LUMEN_TLS_CA");
        std::env::remove_var("LUMEN_TLS_SERVER_NAMES");
    }
}

// ---- #3113 R1: the serving port's env contract ------------------------
//
// Three states, and the two that are neither "TLS" nor "h2c" are errors.
// The interesting one is the last: material projected with the switch left
// off is the shape in which a deployment believes it is serving TLS and
// is not, and nothing downstream would say otherwise.

#[test]
fn serving_tls_from_env_is_none_when_the_deployment_asked_for_h2c() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    assert!(ServingTlsConfig::from_env().unwrap().is_none());
}

#[test]
fn serving_tls_from_env_reads_the_paths_and_the_service_names() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    unsafe {
        std::env::set_var("LUMEN_TLS", "on");
        std::env::set_var("LUMEN_TLS_CERT", "/var/run/secrets/lumen-serving/tls.crt");
        std::env::set_var("LUMEN_TLS_KEY", "/var/run/secrets/lumen-serving/tls.key");
        std::env::set_var("LUMEN_TLS_CA", "/var/run/secrets/lumen-serving/ca.crt");
        std::env::set_var(
            "LUMEN_TLS_SERVER_NAMES",
            "search.acme.svc, search.acme.svc.cluster.local",
        );
    }
    let cfg = ServingTlsConfig::from_env().unwrap().expect("Some");
    assert_eq!(
        cfg.cert.to_string_lossy(),
        "/var/run/secrets/lumen-serving/tls.crt"
    );
    assert_eq!(
        cfg.dns_names,
        vec![
            "search.acme.svc".to_string(),
            "search.acme.svc.cluster.local".to_string()
        ],
        "both Service DNS forms, whitespace-tolerant"
    );
    clear_env();
}

#[test]
fn serving_tls_on_without_material_fails_startup() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    unsafe {
        std::env::set_var("LUMEN_TLS", "on");
    }
    let err = ServingTlsConfig::from_env().unwrap_err().to_string();
    assert!(err.contains("LUMEN_TLS_CERT"), "{err}");
    clear_env();
}

#[test]
fn projected_material_with_the_switch_off_fails_instead_of_serving_cleartext() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    unsafe {
        std::env::set_var("LUMEN_TLS_CERT", "/var/run/secrets/lumen-serving/tls.crt");
    }
    let err = ServingTlsConfig::from_env().unwrap_err().to_string();
    assert!(
        err.contains("cleartext"),
        "the error must name what would otherwise happen: {err}"
    );
    clear_env();
}

/// R3, read from the type: the serving configuration has no mutual half to
/// enable. Callers prove themselves with a ServiceAccount token; a client
/// certificate on this port would be a second, unrelated answer to a
/// question the token already answers.
#[test]
fn the_serving_profile_never_requires_a_client_certificate() {
    let profile = peer_tls::TlsRuntimeProfile::serving(["search.acme.svc".to_string()]);
    assert!(!profile.mutual);
    assert_eq!(
        profile.alpn_protocols,
        vec![b"h2".to_vec(), b"http/1.1".to_vec()],
        "the client port offers both HTTP/2 and HTTP/1.1"
    );
    assert!(peer_tls::TlsRuntimeProfile::peer(["lumen-0".to_string()], std::iter::empty()).mutual);
}

fn write_tls_fixture(name: &str) -> PeerTlsConfig {
    let dir = std::env::temp_dir().join(format!("lumen-tls-rustls-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("cert.pem"), TEST_CERT).unwrap();
    std::fs::write(dir.join("key.pem"), TEST_KEY).unwrap();
    std::fs::write(dir.join("ca.pem"), TEST_CERT).unwrap();
    PeerTlsConfig {
        cert: dir.join("cert.pem"),
        key: dir.join("key.pem"),
        ca: dir.join("ca.pem"),
        required: true,
    }
}

#[test]
fn reloadable_refuses_to_start_on_material_that_is_no_longer_valid() {
    // The fixture leaf's validity window closed in July 2026, so this is
    // exactly the shape of a projected Secret nobody renewed. Startup must
    // refuse it (#3112 R7) rather than join the group with an identity
    // peers would reject — and the refusal must come from the shared seam,
    // which is the point of the adapter being three lines long.
    let cfg = write_tls_fixture("reloadable");
    let err = cfg
        .reloadable(["lumen-peer".to_string()], std::iter::empty())
        .expect_err("expired material must not activate");
    let message = err.to_string();
    assert!(
        message.contains("expired"),
        "the refusal should name the reason: {message}"
    );
    assert!(
        !message.contains("PRIVATE KEY") && !message.contains("cert.pem"),
        "a refusal must not carry key material or projection paths: {message}"
    );
}

#[test]
fn from_env_returns_none_when_nothing_set() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    let cfg = PeerTlsConfig::from_env().unwrap();
    assert!(cfg.is_none());
}

#[test]
fn from_env_loads_when_all_set() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    let dir = std::env::temp_dir().join(format!("lumen-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    for name in ["cert.pem", "key.pem", "ca.pem"] {
        use std::io::Write;
        let mut f = std::fs::File::create(dir.join(name)).unwrap();
        f.write_all(b"DUMMY").unwrap();
    }
    unsafe {
        std::env::set_var("LUMEN_PEER_TLS_CERT", dir.join("cert.pem"));
        std::env::set_var("LUMEN_PEER_TLS_KEY", dir.join("key.pem"));
        std::env::set_var("LUMEN_PEER_TLS_CA", dir.join("ca.pem"));
        std::env::set_var("LUMEN_PEER_MTLS", "on");
    }
    let cfg = PeerTlsConfig::from_env().unwrap().expect("Some");
    assert!(cfg.required);
    std::fs::remove_dir_all(&dir).ok();
    clear_env();
}

#[test]
fn from_env_errors_on_partial_config() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_env();
    unsafe {
        std::env::set_var("LUMEN_PEER_TLS_CERT", "/tmp/dummy-cert");
    }
    let err = PeerTlsConfig::from_env().unwrap_err();
    assert!(err.to_string().contains("must all be set together"));
    clear_env();
}

#[test]
fn builds_rustls_peer_configs_from_pem_material() {
    let cfg = write_tls_fixture("builder");
    cfg.rustls_server_config()
        .expect("server config should build");
    cfg.rustls_client_config()
        .expect("client config should build");
    assert_eq!(cfg.peer_transport().unwrap().generation(), 1);
    std::fs::remove_dir_all(cfg.cert.parent().unwrap()).ok();
}

mod peer_mtls;
