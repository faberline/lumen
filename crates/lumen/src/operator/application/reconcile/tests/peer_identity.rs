use crate::operator::application::reconcile::plan_verdicts::check_peer_identity;
use crate::operator::application::reconcile::tests::hpa::hpa_test_lumen;
use crate::operator::application::render;
use crate::operator::domain::lumen_spec::Lumen;

// ---- peer TLS Secret check (#2890 R4) ----------------------------------

/// A replicated instance naming `secret`, in namespace `acme`.
fn peer_test_lumen(secret: Option<&str>) -> Lumen {
    let mut lumen = hpa_test_lumen("search", "acme", 1, 3);
    lumen.spec.peer_tls_secret = secret.map(str::to_string);
    lumen
}

#[test]
fn a_single_replica_instance_is_never_asked_for_peer_material() {
    let mut lumen = peer_test_lumen(None);
    lumen.spec.replicas_per_shard = 1;
    assert_eq!(check_peer_identity(&lumen), None);
}

#[test]
fn a_replicated_instance_with_no_secret_named_says_which_keys_it_needs() {
    let lumen = peer_test_lumen(None);

    let error = check_peer_identity(&lumen).expect("a replicated instance owes peer identity");

    assert!(error.contains("spec.peerTlsSecret"), "got: {error}");
    for key in render::PEER_TLS_KEYS {
        assert!(error.contains(key), "the message must name {key}: {error}");
    }
}

#[test]
fn complete_peer_material_is_no_finding_at_all() {
    let lumen = peer_test_lumen(Some("search-peer-tls"));
    assert_eq!(check_peer_identity(&lumen), None);
}

#[test]
fn check_peer_identity_message_pins_spec_field_required_keys_and_replica_count() {
    let lumen = peer_test_lumen(None);
    let error =
        check_peer_identity(&lumen).expect("replicated CR without peerTlsSecret must return error");
    assert!(
        error.contains("spec.peerTlsSecret"),
        "message must name spec field: {error}"
    );
    assert!(
        error.contains(&format!(
            "replicasPerShard={}",
            lumen.spec.replicas_per_shard
        )),
        "message must state replicasPerShard value: {error}"
    );
    for key in render::PEER_TLS_KEYS {
        assert!(
            error.contains(key),
            "message must name required key {key}: {error}"
        );
    }
    assert!(
        error.contains("replicated Raft traffic has no plaintext fallback"),
        "message must state tail rationale: {error}"
    );

    // Single-replica instance owes no peer identity
    let single = hpa_test_lumen("search", "acme", 1, 1);
    assert_eq!(check_peer_identity(&single), None);

    // Replicated instance naming spec.peerTlsSecret returns None
    let configured = peer_test_lumen(Some("search-peer-tls"));
    assert_eq!(check_peer_identity(&configured), None);
}
