use serde_json::{json, Value};

use crate::domain::lumen_fleet::plan::{apply_object, plan, seed_object};
use crate::domain::lumen_fleet::status::{canonical_json, project_status, spec_hash};
use crate::domain::lumen_fleet::{
    FleetEntryStatus, FleetInstance, LumenFleet, LumenFleetSpec, LumenFleetStatus, PlanOutcome,
    PlannedInstance, PrunePolicy, FLEET_LABEL,
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

/// An entry for `team-x`/`search` in the given state, as one pass writes it.
fn status_entry(state: &str, message: &str) -> FleetEntryStatus {
    FleetEntryStatus {
        namespace: "team-x".to_string(),
        name: "search".to_string(),
        state: state.to_string(),
        message: message.to_string(),
        spec_hash: String::new(),
    }
}

/// The counts and the summary come from the entry list, not from a
/// `desired - applied` shortcut: one of each state, each counted once.
#[test]
fn counts_match_a_mixed_entry_list() {
    let planned = plan(&fleet(vec![
        instance("team-a", None),
        instance("team-b", None),
        instance("team-c", None),
        instance("team-d", None),
    ]));
    let entries = vec![
        status_entry("Created", ""),
        status_entry("Applied", ""),
        status_entry("Rejected", "bad field"),
        status_entry("Orphaned", "retained"),
        status_entry("Pruned", ""),
        status_entry("NamespaceMissing", "no namespace"),
    ];
    let status = project_status(&planned, entries, 3);
    assert_eq!(status.desired_instances, 4);
    assert_eq!(status.materialized_instances, 2);
    assert_eq!(status.applied_instances, 2);
    assert_eq!(status.rejected_instances, 1);
    assert_eq!(status.orphaned_instances, 1);
    assert_eq!(status.pruned_instances, 1);
    assert_eq!(status.observed_generation, 3);
    assert_eq!(
        status.message,
        "2/4 instances converged, 1 rejected, 1 orphaned, 1 pruned"
    );
}

/// A zero count is left out of the summary rather than printed as `0 rejected`.
#[test]
fn a_zero_part_is_left_out_of_the_message() {
    let planned = plan(&fleet(vec![instance("team-x", None)]));
    let status = project_status(&planned, vec![status_entry("Applied", "")], 1);
    assert_eq!(status.message, "1/1 instances converged");
}

#[test]
fn spec_hash_is_stable_for_equal_specs() {
    let a = json!({"image": "img", "serving": {"cpu": "2"}});
    let b = json!({"image": "img", "serving": {"cpu": "2"}});
    let hash = spec_hash(&a);
    assert_eq!(hash, spec_hash(&b));
    assert_eq!(hash.len(), 64);
    assert!(hash.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')));
    assert_eq!(
        spec_hash(&json!({})),
        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
        "SHA-256 of the two bytes {{}}"
    );
}

#[test]
fn spec_hash_changes_with_an_override() {
    let plain = plan(&fleet(vec![instance("team-a", None)]));
    let overridden = plan(&fleet(vec![instance(
        "team-a",
        Some(json!({ "serving": { "cpu": "4" } })),
    )]));
    assert_ne!(
        spec_hash(ready(&plain[0])),
        spec_hash(ready(&overridden[0]))
    );
}

#[test]
fn spec_hash_ignores_json_key_order() {
    let a: Value = serde_json::from_str(r#"{"a":1,"b":{"x":1,"y":2}}"#).unwrap();
    let b: Value = serde_json::from_str(r#"{"b":{"y":2,"x":1},"a":1}"#).unwrap();
    assert_eq!(spec_hash(&a), spec_hash(&b));
    assert_eq!(canonical_json(&a), r#"{"a":1,"b":{"x":1,"y":2}}"#);
    assert_eq!(canonical_json(&b), r#"{"a":1,"b":{"x":1,"y":2}}"#);
}

/// A status written before these fields existed must still deserialize.
#[test]
fn an_old_status_without_the_new_fields_reads_as_zero() {
    let old: LumenFleetStatus = serde_json::from_value(json!({
        "observedGeneration": 1,
        "desiredInstances": 2,
        "appliedInstances": 1,
        "message": "x",
    }))
    .expect("old status parses");
    assert_eq!(old.materialized_instances, 0);
    assert_eq!(old.rejected_instances, 0);
    assert_eq!(old.orphaned_instances, 0);
    assert_eq!(old.pruned_instances, 0);

    let entry: FleetEntryStatus = serde_json::from_value(json!({
        "namespace": "team-a",
        "name": "search",
        "state": "Applied",
    }))
    .expect("old entry parses");
    assert_eq!(entry.spec_hash, "");
}

#[test]
fn spec_hash_is_omitted_for_states_without_one() {
    let entry = status_entry("Rejected", "bad");
    let json = serde_json::to_value(&entry).unwrap();
    assert!(json.get("specHash").is_none(), "{json}");
}

/// Pass 1 fails the entry; pass 2 repairs it. The second status must not
/// carry the old failure anywhere in its serialized patch.
#[test]
fn a_repaired_entry_leaves_no_stale_failure() {
    let planned = plan(&fleet(vec![instance("team-x", None)]));

    let pass1 = project_status(&planned, vec![status_entry("ApplyFailed", "boom")], 1);
    assert_eq!(pass1.materialized_instances, 0);
    assert_eq!(pass1.message, "0/1 instances converged");

    let repaired = FleetEntryStatus {
        spec_hash: spec_hash(ready(&planned[0])),
        ..status_entry("Applied", "")
    };
    let pass2 = project_status(&planned, vec![repaired], 2);
    assert_eq!(pass2.materialized_instances, 1);
    assert_eq!(pass2.message, "1/1 instances converged");

    let patch = json!({ "status": serde_json::to_value(&pass2).unwrap() });
    let text = serde_json::to_string(&patch).unwrap();
    assert!(!text.contains("boom"), "stale failure in the patch: {text}");
    assert_eq!(patch["status"]["entries"][0]["state"], json!("Applied"));
    assert!(patch["status"]["entries"][0]["specHash"].is_string());
}

/// Negative control: a rejected entry is not a materialized one.
#[test]
fn a_rejected_entry_alone_is_not_materialized() {
    let planned = plan(&fleet(vec![instance(
        "team-r",
        Some(json!({ "serving": { "cpuu": "8" } })),
    )]));
    assert!(matches!(planned[0].outcome, PlanOutcome::Rejected(_)));
    let entry = FleetEntryStatus {
        namespace: "team-r".to_string(),
        ..status_entry("Rejected", "bad field")
    };
    let status = project_status(&planned, vec![entry], 1);
    assert_eq!(status.materialized_instances, 0);
    assert_eq!(status.rejected_instances, 1);
}
