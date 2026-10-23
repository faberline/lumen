use serde_json::{json, Value};

use crate::domain::lumen_fleet::plan::{apply_object, plan, seed_object};
use crate::domain::lumen_fleet::{
    FleetInstance, LumenFleet, LumenFleetSpec, PlanOutcome, PlannedInstance, PrunePolicy,
    FLEET_LABEL,
};
use crate::domain::lumen_spec::LumenSpec;

fn defaults() -> LumenSpec {
    serde_json::from_value(json!({
        "image": "ghcr.io/acme/lumen:0.4.27",
        "serving": { "cpu": "2", "memory": "8Gi", "raftStorage": "50Gi" },
        "placement": { "nodeSelector": { "cloud.google.com/gke-nodepool": "lumen-ssd" } },
    }))
    .expect("defaults parse")
}

fn fleet(instances: Vec<FleetInstance>) -> LumenFleet {
    let mut fleet = LumenFleet::new(
        "search",
        LumenFleetSpec {
            defaults: defaults(),
            instances,
            prune_policy: PrunePolicy::default(),
        },
    );
    fleet.metadata.generation = Some(1);
    fleet
}

fn instance(namespace: &str, spec: Option<Value>) -> FleetInstance {
    FleetInstance {
        namespace: namespace.to_string(),
        name: None,
        spec,
    }
}

fn ready(planned: &PlannedInstance) -> &Value {
    match &planned.outcome {
        PlanOutcome::Ready(spec) => spec,
        PlanOutcome::Rejected(reason) => panic!("expected a ready instance, got: {reason}"),
    }
}

fn rejection(planned: &PlannedInstance) -> &str {
    match &planned.outcome {
        PlanOutcome::Rejected(reason) => reason,
        PlanOutcome::Ready(_) => panic!("expected a rejection"),
    }
}

/// The whole point of `defaults`: a tenant that names nothing still gets
/// the platform team's image, node pool, and disk.
#[test]
fn an_entry_inherits_every_default_it_does_not_name() {
    let planned = plan(&fleet(vec![instance("team-a", None)]));
    let spec = ready(&planned[0]);
    assert_eq!(spec["image"], json!("ghcr.io/acme/lumen:0.4.27"));
    assert_eq!(spec["serving"]["cpu"], json!("2"));
    assert_eq!(
        spec["placement"]["nodeSelector"]["cloud.google.com/gke-nodepool"],
        json!("lumen-ssd")
    );
    assert_eq!(planned[0].namespace, "team-a");
    assert_eq!(planned[0].name, "search", "name defaults to the fleet's");
}

/// A merge patch, not a replacement: raising one tenant's CPU must not
/// silently drop the memory and disk it never mentioned.
#[test]
fn an_override_replaces_only_the_field_it_names() {
    let planned = plan(&fleet(vec![instance(
        "team-b",
        Some(json!({ "serving": { "cpu": "8" } })),
    )]));
    let spec = ready(&planned[0]);
    assert_eq!(spec["serving"]["cpu"], json!("8"));
    assert_eq!(spec["serving"]["memory"], json!("8Gi"), "inherited");
    assert_eq!(spec["serving"]["raftStorage"], json!("50Gi"), "inherited");
}

/// RFC 7386's `null`: the only way to *unset* something the defaults set.
#[test]
fn a_null_override_clears_an_inherited_field() {
    let with_log_level = LumenFleet::new(
        "search",
        LumenFleetSpec {
            defaults: serde_json::from_value(json!({
                "image": "img", "logLevel": "warn",
            }))
            .unwrap(),
            instances: vec![instance("team-c", Some(json!({ "logLevel": null })))],
            prune_policy: PrunePolicy::default(),
        },
    );
    let planned = plan(&with_log_level);
    assert!(ready(&planned[0]).get("logLevel").is_none());
}

/// The failure mode a free-form override has to be defended against: a
/// misspelled key is accepted by serde, ignored, and the tenant quietly
/// runs on the default it thought it had changed.
#[test]
fn a_misspelled_override_is_rejected_rather_than_ignored() {
    let planned = plan(&fleet(vec![instance(
        "team-d",
        Some(json!({ "serving": { "cpuu": "8" } })),
    )]));
    let reason = rejection(&planned[0]);
    assert!(reason.contains("serving.cpuu"), "{reason}");
}

/// Empty collections round-trip away because `LumenSpec` skips
/// serializing them; the typo check must not read that as a typo.
#[test]
fn an_empty_collection_is_not_mistaken_for_an_unknown_field() {
    let planned = plan(&fleet(vec![instance(
        "team-e",
        Some(json!({
            "shardMap": { "assignments": [] },
            "placement": { "tolerations": [] },
        })),
    )]));
    ready(&planned[0]);
}

/// A wrong *value* is caught by the same pass, with the field named.
#[test]
fn a_value_the_schema_rejects_names_the_field() {
    let planned = plan(&fleet(vec![instance(
        "team-f",
        // `off` is the env spelling; the CRD spells it `disabled`.
        Some(json!({ "auth": "off" })),
    )]));
    assert!(rejection(&planned[0]).contains("auth"));
}

/// The retired registry fields (#2872) must not survive as a fleet
/// override either. The fleet is the one path that takes free-form spec
/// JSON, so a platform team that kept the old grants in their defaults
/// would otherwise get them merged in and dropped without a word — the
/// exact silent no-op the retirement exists to prevent.
#[test]
fn a_retired_registry_override_is_rejected_at_the_fleet_too() {
    for retired in [
        json!({ "tokensSecret": "lumen-tokens" }),
        json!({ "identities": { "svc@proj.iam.gserviceaccount.com": { "subject": "team-g" } } }),
        json!({ "identityAudiences": ["https://lumen.example.com"] }),
    ] {
        let key = retired.as_object().unwrap().keys().next().unwrap().clone();
        let planned = plan(&fleet(vec![instance("team-g", Some(retired))]));
        let reason = rejection(&planned[0]);
        assert!(reason.contains(&key), "{reason}");
        assert!(
            reason.contains("a Lumen does not have"),
            "a retired field is now an unknown field, not a validate() rule: {reason}"
        );
    }
}

/// One tenant's bad edit must not stop every other tenant from converging.
#[test]
fn one_rejected_entry_does_not_stop_the_others() {
    let planned = plan(&fleet(vec![
        instance("team-h", Some(json!({ "serving": { "nope": 1 } }))),
        instance("team-i", None),
    ]));
    assert!(matches!(planned[0].outcome, PlanOutcome::Rejected(_)));
    assert!(matches!(planned[1].outcome, PlanOutcome::Ready(_)));
}

/// Two entries writing one object would each revert the other every pass,
/// so which settings a tenant ends up with would depend on list order.
#[test]
fn two_entries_targeting_the_same_object_are_rejected() {
    let mut second = instance("team-j", None);
    second.name = Some("search".to_string());
    let planned = plan(&fleet(vec![instance("team-j", None), second]));
    assert!(matches!(planned[0].outcome, PlanOutcome::Ready(_)));
    assert!(rejection(&planned[1]).contains("already targets"));
}

/// ★ The one that protects live data. The reshard driver writes
/// `shardCount`, `shardMap`, and `reshardPolicy.workflow` at runtime; a
/// fleet apply that listed them would revert a finished split on its next
/// pass — resetting the map version and re-migrating data that already
/// moved.
#[test]
fn the_steady_state_apply_never_claims_a_field_the_reshard_driver_owns() {
    let planned = plan(&fleet(vec![instance("team-k", None)]));
    let spec = ready(&planned[0]);
    let applied = apply_object("search", &planned[0], spec);

    assert!(applied["spec"].get("shardCount").is_none(), "{applied}");
    assert!(applied["spec"].get("shardMap").is_none(), "{applied}");
    assert!(
        applied["spec"]["reshardPolicy"].get("workflow").is_none(),
        "{applied}"
    );
    // The rest of `reshardPolicy` is fleet-declared policy and must stay.
    assert!(applied["spec"]["reshardPolicy"]["prepareAtPercent"].is_number());
    assert_eq!(applied["spec"]["image"], json!("ghcr.io/acme/lumen:0.4.27"));
}

/// The create is the one moment the initial topology can be declared, so
/// it carries what the steady-state apply drops.
#[test]
fn the_seed_create_carries_the_initial_topology() {
    let planned = plan(&fleet(vec![instance(
        "team-l",
        Some(json!({ "shardCount": 4 })),
    )]));
    let seed = seed_object("search", &planned[0], ready(&planned[0]));
    assert_eq!(seed["spec"]["shardCount"], json!(4));
    assert_eq!(seed["metadata"]["namespace"], json!("team-l"));
    assert_eq!(seed["metadata"]["labels"][FLEET_LABEL], json!("search"));
}

/// Ownership is a label, not an `ownerReference` — deleting the fleet must
/// not cascade-delete every tenant's index and PVCs.
#[test]
fn a_materialized_instance_carries_no_owner_reference() {
    let planned = plan(&fleet(vec![instance("team-m", None)]));
    let seed = seed_object("search", &planned[0], ready(&planned[0]));
    assert!(seed["metadata"].get("ownerReferences").is_none(), "{seed}");
    assert_eq!(seed["metadata"]["labels"][FLEET_LABEL], json!("search"));
}

#[test]
fn hash_stability_and_sensitivity() {
    let spec1 = json!({
        "image": "lumen:0.4.27",
        "serving": { "cpu": "2", "memory": "8Gi" }
    });
    let spec2 = json!({
        "image": "lumen:0.4.27",
        "serving": { "cpu": "2", "memory": "8Gi" }
    });
    let spec3 = json!({
        "image": "lumen:0.4.27",
        "serving": { "cpu": "4", "memory": "8Gi" }
    });

    let hash1 = crate::domain::lumen_fleet::desired_hash(&spec1);
    let hash2 = crate::domain::lumen_fleet::desired_hash(&spec2);
    let hash3 = crate::domain::lumen_fleet::desired_hash(&spec3);

    // Stable: same fleet spec gives the exact same hash
    assert_eq!(hash1, hash2);
    assert_eq!(hash1.len(), 16);

    // Sensitive: any override change gives a different hash
    assert_ne!(hash1, hash3);
}

#[test]
fn phase_since_kept_across_passes_with_the_same_phase() {
    let t1 = "2026-10-24T00:00:00Z";
    let t2 = "2026-10-24T00:01:00Z";

    // Pass 1: Stalled at t1
    let pass1 =
        crate::domain::lumen_fleet::derive_rollout_status(None, "NamespaceMissing", None, None, t1);
    assert_eq!(
        pass1.phase,
        crate::domain::lumen_fleet::FleetEntryPhase::Stalled
    );
    assert_eq!(pass1.phase_since, t1);

    let prev = crate::domain::lumen_fleet::FleetEntryStatus {
        namespace: "team-a".to_string(),
        name: "search".to_string(),
        state: "NamespaceMissing".to_string(),
        message: "missing".to_string(),
        phase: Some(pass1.phase),
        phase_since: Some(pass1.phase_since),
        stall_reason: pass1.stall_reason,
        last_stall: pass1.last_stall,
        ..Default::default()
    };

    // Pass 2: Still Stalled at t2
    let pass2 = crate::domain::lumen_fleet::derive_rollout_status(
        Some(&prev),
        "NamespaceMissing",
        None,
        None,
        t2,
    );
    assert_eq!(
        pass2.phase,
        crate::domain::lumen_fleet::FleetEntryPhase::Stalled
    );
    // phaseSince must be kept from t1
    assert_eq!(pass2.phase_since, t1);
}

#[test]
fn negative_control_phase_since_changes_when_phase_changes() {
    let t1 = "2026-10-24T00:00:00Z";
    let t2 = "2026-10-24T00:01:00Z";

    // Start in Stalled at t1
    let prev = crate::domain::lumen_fleet::FleetEntryStatus {
        namespace: "team-a".to_string(),
        name: "search".to_string(),
        state: "ApplyFailed".to_string(),
        message: "error".to_string(),
        phase: Some(crate::domain::lumen_fleet::FleetEntryPhase::Stalled),
        phase_since: Some(t1.to_string()),
        stall_reason: Some("ApplyFailed".to_string()),
        last_stall: None,
        ..Default::default()
    };

    // Transition to Healthy at t2 (generation caught up)
    let pass2 = crate::domain::lumen_fleet::derive_rollout_status(
        Some(&prev),
        "Applied",
        Some(1),
        Some(1),
        t2,
    );
    assert_eq!(
        pass2.phase,
        crate::domain::lumen_fleet::FleetEntryPhase::Healthy
    );
    // Phase changed, so phaseSince MUST change to t2
    assert_eq!(pass2.phase_since, t2);
    assert_ne!(pass2.phase_since, t1);
}

#[test]
fn each_stall_token_produced_correctly() {
    let t = "2026-10-24T00:00:00Z";

    // 1. Rejected
    let r1 = crate::domain::lumen_fleet::derive_rollout_status(None, "Rejected", None, None, t);
    assert_eq!(r1.stall_reason.as_deref(), Some("Rejected"));
    assert_eq!(
        r1.phase,
        crate::domain::lumen_fleet::FleetEntryPhase::Stalled
    );

    // 2. NamespaceMissing
    let r2 =
        crate::domain::lumen_fleet::derive_rollout_status(None, "NamespaceMissing", None, None, t);
    assert_eq!(r2.stall_reason.as_deref(), Some("NamespaceMissing"));
    assert_eq!(
        r2.phase,
        crate::domain::lumen_fleet::FleetEntryPhase::Stalled
    );

    // 3. NotAdopted
    let r3 =
        crate::domain::lumen_fleet::derive_rollout_status(None, "NotAdopted", Some(1), Some(1), t);
    assert_eq!(r3.stall_reason.as_deref(), Some("NotAdopted"));
    assert_eq!(
        r3.phase,
        crate::domain::lumen_fleet::FleetEntryPhase::Stalled
    );

    // 4. ApplyFailed
    let r4 =
        crate::domain::lumen_fleet::derive_rollout_status(None, "ApplyFailed", Some(1), Some(1), t);
    assert_eq!(r4.stall_reason.as_deref(), Some("ApplyFailed"));
    assert_eq!(
        r4.phase,
        crate::domain::lumen_fleet::FleetEntryPhase::Stalled
    );

    // 5. Child that has not observed its latest generation
    let r5a =
        crate::domain::lumen_fleet::derive_rollout_status(None, "Applied", Some(2), Some(1), t);
    assert_eq!(r5a.stall_reason.as_deref(), Some("UnobservedGeneration"));
    assert_eq!(
        r5a.phase,
        crate::domain::lumen_fleet::FleetEntryPhase::Stalled
    );

    let r5b = crate::domain::lumen_fleet::derive_rollout_status(None, "Created", Some(1), None, t);
    assert_eq!(r5b.stall_reason.as_deref(), Some("UnobservedGeneration"));
    assert_eq!(
        r5b.phase,
        crate::domain::lumen_fleet::FleetEntryPhase::Stalled
    );
}

#[test]
fn clear_on_recovery_with_last_stall_kept() {
    let t1 = "2026-10-24T00:00:00Z";
    let t2 = "2026-10-24T00:01:00Z";
    let t3 = "2026-10-24T00:02:00Z";

    // Pass 1: Stalled with ApplyFailed at t1
    let s1 = crate::domain::lumen_fleet::derive_rollout_status(
        None,
        "ApplyFailed",
        Some(1),
        Some(0),
        t1,
    );
    let prev1 = crate::domain::lumen_fleet::FleetEntryStatus {
        namespace: "team-a".to_string(),
        name: "search".to_string(),
        state: "ApplyFailed".to_string(),
        message: "timeout".to_string(),
        phase: Some(s1.phase),
        phase_since: Some(s1.phase_since),
        stall_reason: s1.stall_reason,
        last_stall: s1.last_stall,
        ..Default::default()
    };

    // Pass 2: Recovers at t2 (observed generation matches)
    let s2 = crate::domain::lumen_fleet::derive_rollout_status(
        Some(&prev1),
        "Applied",
        Some(1),
        Some(1),
        t2,
    );
    assert_eq!(
        s2.phase,
        crate::domain::lumen_fleet::FleetEntryPhase::Healthy
    );
    assert_eq!(
        s2.stall_reason, None,
        "stallReason must be cleared on recovery"
    );
    assert_eq!(s2.phase_since, t2);
    assert_eq!(
        s2.last_stall,
        Some(crate::domain::lumen_fleet::FleetEntryLastStall {
            reason: "ApplyFailed".to_string(),
            time: t1.to_string(),
        }),
        "previous stall (reason and start time) is kept in lastStall"
    );

    let prev2 = crate::domain::lumen_fleet::FleetEntryStatus {
        namespace: "team-a".to_string(),
        name: "search".to_string(),
        state: "Applied".to_string(),
        message: "".to_string(),
        phase: Some(s2.phase),
        phase_since: Some(s2.phase_since),
        stall_reason: s2.stall_reason,
        last_stall: s2.last_stall,
        ..Default::default()
    };

    // Pass 3: Remains healthy at t3 -> lastStall is kept
    let s3 = crate::domain::lumen_fleet::derive_rollout_status(
        Some(&prev2),
        "Applied",
        Some(1),
        Some(1),
        t3,
    );
    assert_eq!(
        s3.phase,
        crate::domain::lumen_fleet::FleetEntryPhase::Healthy
    );
    assert_eq!(s3.stall_reason, None);
    assert_eq!(
        s3.phase_since, t2,
        "phaseSince kept when phase remains Healthy"
    );
    assert_eq!(
        s3.last_stall,
        Some(crate::domain::lumen_fleet::FleetEntryLastStall {
            reason: "ApplyFailed".to_string(),
            time: t1.to_string(),
        }),
        "lastStall is preserved on subsequent healthy passes"
    );
}

#[test]
fn old_shape_entry_as_previous_state() {
    let old_json = json!({
        "namespace": "team-z",
        "name": "search",
        "state": "Applied",
        "message": ""
    });
    // Old shape deserializes cleanly (forward compatibility)
    let old_entry: crate::domain::lumen_fleet::FleetEntryStatus =
        serde_json::from_value(old_json).expect("old shape deserializes");
    assert!(old_entry.phase.is_none());
    assert!(old_entry.observed_generation.is_none());
    assert!(old_entry.desired_hash.is_none());
    assert!(old_entry.phase_since.is_none());
    assert!(old_entry.stall_reason.is_none());
    assert!(old_entry.last_stall.is_none());

    let now = "2026-10-24T00:00:00Z";
    let status = crate::domain::lumen_fleet::derive_rollout_status(
        Some(&old_entry),
        "Applied",
        Some(1),
        Some(1),
        now,
    );
    assert_eq!(
        status.phase,
        crate::domain::lumen_fleet::FleetEntryPhase::Healthy
    );
    assert_eq!(status.phase_since, now);
    assert_eq!(status.stall_reason, None);
    assert_eq!(status.last_stall, None);
}
