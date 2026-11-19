use std::collections::BTreeSet;

use crate::domain::lumen_fleet::children::*;
use serde_json::{json, Value};

use crate::domain::lumen_fleet::plan::{apply_object, plan, seed_object};
use crate::domain::lumen_fleet::{
    FleetInstance, LumenFleet, LumenFleetSpec, PlanOutcome, PlannedInstance, PrunePolicy,
    FLEET_LABEL,
};
use crate::domain::lumen_spec::access::{AccessSpec, ServiceAccountSubject};
use crate::domain::lumen_spec::runtime::RuntimeSettings;
use crate::domain::lumen_spec::LumenSpec;

use crate::domain::lumen_fleet::status::{canonical_json, project_status, spec_hash, status_patch};
use crate::domain::lumen_fleet::{FleetEntryStatus, LumenFleetStatus};

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
        runtime: None,
        spec,
        access: None,
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

fn fleet_with_runtime_defaults(
    defaults_runtime: RuntimeSettings,
    instances: Vec<FleetInstance>,
) -> LumenFleet {
    let mut defaults_spec = defaults();
    defaults_spec.runtime = Some(defaults_runtime);
    LumenFleet::new(
        "search",
        LumenFleetSpec {
            defaults: defaults_spec,
            instances,
            prune_policy: PrunePolicy::default(),
        },
    )
}

/// A runtime value on an instance replaces the same field of the defaults;
/// a field the instance leaves unset keeps the default.
#[test]
fn runtime_replaces_the_default_field_by_field() {
    let mut tenant = instance("team-a", None);
    tenant.runtime = Some(RuntimeSettings {
        snapshot_secs: Some(60),
        ..Default::default()
    });
    let planned = plan(&fleet_with_runtime_defaults(
        RuntimeSettings {
            snapshot_secs: Some(300),
            grace_secs: Some(30),
            ..Default::default()
        },
        vec![tenant],
    ));
    assert_eq!(
        ready(&planned[0])["runtime"],
        json!({"snapshotSecs": 60, "graceSecs": 30})
    );
}

/// Negative control: without the overlay in `plan()`, `team-a` keeps the
/// default 300 and this test fails.
#[test]
fn two_namespaces_get_different_runtime_and_a_third_gets_the_defaults() {
    let mut team_a = instance("team-a", None);
    team_a.runtime = Some(RuntimeSettings {
        snapshot_secs: Some(10),
        ..Default::default()
    });
    let mut team_b = instance("team-b", None);
    team_b.runtime = Some(RuntimeSettings {
        snapshot_secs: Some(20),
        ..Default::default()
    });
    let team_c = instance("team-c", None);
    let planned = plan(&fleet_with_runtime_defaults(
        RuntimeSettings {
            snapshot_secs: Some(300),
            ..Default::default()
        },
        vec![team_a, team_b, team_c],
    ));
    assert_eq!(ready(&planned[0])["runtime"]["snapshotSecs"], json!(10));
    assert_eq!(ready(&planned[1])["runtime"]["snapshotSecs"], json!(20));
    assert_eq!(ready(&planned[2])["runtime"]["snapshotSecs"], json!(300));
}

/// A manifest written before `runtime` existed still reads and plans as before.
#[test]
fn a_manifest_without_runtime_plans_the_same_spec_as_before() {
    let tenant: FleetInstance =
        serde_json::from_value(json!({ "namespace": "team-a" })).expect("instance parses");
    assert!(tenant.runtime.is_none());
    let planned = plan(&fleet(vec![tenant]));
    let spec = ready(&planned[0]);
    assert!(spec.get("runtime").is_none(), "{spec}");
    assert_eq!(spec["image"], json!("ghcr.io/acme/lumen:0.4.27"));
    assert_eq!(spec["serving"]["cpu"], json!("2"));
}

/// The RFC 7386 `spec` patch keeps its behaviour when `runtime` is present:
/// `null` still clears, and an unknown key is still rejected.
#[test]
fn the_spec_patch_still_merges_as_before_with_runtime_present() {
    let mut tenant = instance(
        "team-d",
        Some(json!({ "logLevel": "debug", "serving": null })),
    );
    tenant.runtime = Some(RuntimeSettings {
        snapshot_secs: Some(5),
        ..Default::default()
    });
    let planned = plan(&fleet(vec![tenant]));
    let spec = ready(&planned[0]);
    assert_eq!(spec["logLevel"], json!("debug"));
    assert!(spec.get("serving").is_none(), "{spec}");
    assert_eq!(spec["runtime"]["snapshotSecs"], json!(5));

    let mut misspelled = instance("team-e", Some(json!({ "nope": 1 })));
    misspelled.runtime = Some(RuntimeSettings {
        snapshot_secs: Some(5),
        ..Default::default()
    });
    let planned = plan(&fleet(vec![misspelled]));
    assert!(
        rejection(&planned[0]).contains("nope"),
        "{}",
        rejection(&planned[0])
    );
}

#[test]
fn child_key_is_namespace_and_name() {
    assert_eq!(child_key("a", "b"), ("a".to_string(), "b".to_string()));
}

#[test]
fn duplicate_targets_collapse_to_one_declared_key() {
    let mut second = instance("team-j", None);
    second.name = Some("search".to_string());
    let planned = plan(&fleet(vec![instance("team-j", None), second]));
    let keys: BTreeSet<ChildKey> = planned
        .iter()
        .map(|p| child_key(&p.namespace, &p.name))
        .collect();
    assert_eq!(
        keys.len(),
        1,
        "two entries for one object must share one key"
    );
}

#[test]
fn a_foreign_owner_label_is_not_adopted() {
    match adoption(Some(Some("other")), "search") {
        Adoption::NotAdopted { message } => {
            assert!(message.contains("found other"), "{message}");
            assert!(message.contains("lumen.dev/fleet=search"), "{message}");
        }
        other => panic!("expected NotAdopted, got {other:?}"),
    }
}

#[test]
fn a_missing_label_is_not_adopted() {
    match adoption(Some(None), "search") {
        Adoption::NotAdopted { message } => {
            assert!(message.contains("found no label"), "{message}");
        }
        other => panic!("expected NotAdopted, got {other:?}"),
    }
}

#[test]
fn an_owned_child_is_applied_and_an_absent_one_is_created() {
    assert_eq!(adoption(Some(Some("search")), "search"), Adoption::Apply);
    assert_eq!(adoption(None, "search"), Adoption::Create);
}

#[test]
fn ownership_distinguishes_owned_and_foreign() {
    assert_eq!(ownership(Some("search"), "search"), Ownership::Owned);
    assert_eq!(
        ownership(Some("x"), "search"),
        Ownership::Foreign {
            found: Some("x".into())
        }
    );
    assert_eq!(
        ownership(None, "search"),
        Ownership::Foreign { found: None }
    );
}

#[test]
fn a_retained_orphan_is_reported_not_deleted() {
    let declared: BTreeSet<ChildKey> = [child_key("a", "x")].into_iter().collect();
    assert_eq!(
        prune_decision(&child_key("a", "y"), &declared, PrunePolicy::Retain),
        PruneDecision::Orphaned
    );
}

#[test]
fn a_deleted_orphan_under_delete_policy() {
    let declared: BTreeSet<ChildKey> = [child_key("a", "x")].into_iter().collect();
    assert_eq!(
        prune_decision(&child_key("a", "y"), &declared, PrunePolicy::Delete),
        PruneDecision::Pruned
    );
}

#[test]
fn a_declared_child_is_kept_under_either_policy() {
    let declared: BTreeSet<ChildKey> = [child_key("a", "x")].into_iter().collect();
    for policy in [PrunePolicy::Retain, PrunePolicy::Delete] {
        assert_eq!(
            prune_decision(&child_key("a", "x"), &declared, policy),
            PruneDecision::Keep
        );
    }
}

/// FNV-1a 64-bit over raw bytes, with the constants the spec names, written out
/// here so the test does not call the code it checks.
fn fnv1a_hex(bytes: &[u8]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

#[test]
fn the_desired_hash_ignores_key_order_and_sees_value_changes() {
    let hash = desired_hash(&json!({"a": 1, "b": {"c": 2, "d": 3}}));
    assert_eq!(hash, desired_hash(&json!({"b": {"d": 3, "c": 2}, "a": 1})));
    assert_ne!(hash, desired_hash(&json!({"a": 1, "b": {"c": 2, "d": 4}})));
    assert_eq!(hash.len(), 16);
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(desired_hash(&json!({})), fnv1a_hex(b"{}"));
}

#[test]
fn the_desired_hash_is_sensitive_to_array_order() {
    assert_ne!(desired_hash(&json!([1, 2])), desired_hash(&json!([2, 1])));
}

#[test]
fn a_foreign_labelled_child_is_never_applicable_or_prunable() {
    for label in [Some("other"), None] {
        let decision = adoption(Some(label), "search");
        assert!(
            !matches!(decision, Adoption::Apply | Adoption::Create),
            "a child labelled {label:?} must not be applied or created: {decision:?}"
        );
    }
}

#[test]
fn decisions_depend_only_on_live_state_not_process_memory() {
    let run = || {
        let declared: BTreeSet<ChildKey> = [child_key("a", "x"), child_key("b", "x")]
            .into_iter()
            .collect();
        (
            adoption(Some(Some("search")), "search"),
            adoption(Some(Some("other")), "search"),
            prune_decision(&child_key("a", "y"), &declared, PrunePolicy::Retain),
            desired_hash(&json!({"serving": {"cpu": "2"}, "image": "x"})),
        )
    };
    assert_eq!(run(), run());
}

#[test]
fn a_changed_override_changes_only_its_own_instance_and_plan_has_no_memory() {
    let before = fleet(vec![instance("team-a", None), instance("team-b", None)]);
    let after = fleet(vec![
        instance("team-a", None),
        instance("team-b", Some(json!({ "serving": { "cpu": "4" } }))),
    ]);
    let planned_before = plan(&before);
    let planned_after = plan(&after);

    assert_eq!(
        ready(&planned_before[0]),
        ready(&planned_after[0]),
        "team-a has no override, so its plan must not change"
    );
    assert_eq!(ready(&planned_before[1])["serving"]["cpu"], json!("2"));
    assert_eq!(ready(&planned_after[1])["serving"]["cpu"], json!("4"));

    // Planning is a pure function of the fleet: an earlier pass leaves no trace.
    assert_eq!(plan(&before), planned_before);
    assert_eq!(plan(&after), planned_after);
}

fn sa(namespace: &str, name: &str) -> ServiceAccountSubject {
    ServiceAccountSubject {
        namespace: namespace.to_string(),
        name: name.to_string(),
    }
}

fn access(subjects: Vec<ServiceAccountSubject>) -> AccessSpec {
    AccessSpec {
        allowed_service_accounts: subjects,
    }
}

fn with_access(namespace: &str, subjects: Option<Vec<ServiceAccountSubject>>) -> FleetInstance {
    let mut entry = instance(namespace, None);
    entry.access = subjects.map(access);
    entry
}

fn fleet_with_default_access(
    instances: Vec<FleetInstance>,
    subjects: Vec<ServiceAccountSubject>,
) -> LumenFleet {
    let mut fleet = fleet(instances);
    fleet.spec.defaults.access = Some(access(subjects));
    fleet
}

fn list(planned: &PlannedInstance) -> &Value {
    &ready(planned)["access"]["allowedServiceAccounts"]
}

#[test]
fn fleet_access_accepts_exact_namespace_and_name() {
    let f = fleet_with_default_access(
        vec![with_access("team-a", Some(vec![sa("app-b", "search.v2")]))],
        vec![sa("app-a", "client")],
    );
    let planned = plan(&f);
    assert_eq!(
        list(&planned[0]),
        &json!([{"namespace": "app-b", "name": "search.v2"}])
    );
}

#[test]
fn an_instance_without_access_inherits_the_default_list() {
    let f = fleet_with_default_access(
        vec![with_access("team-a", None)],
        vec![sa("app-a", "client")],
    );
    let planned = plan(&f);
    assert_eq!(
        list(&planned[0]),
        &json!([{"namespace": "app-a", "name": "client"}])
    );
}

/// Negative control for the main failure mode: a union of the default and
/// the instance list would keep `app-a` and `app-c`.
#[test]
fn an_instance_access_list_replaces_the_default_and_is_not_a_union() {
    let f = fleet_with_default_access(
        vec![with_access("team-a", Some(vec![sa("app-b", "search")]))],
        vec![sa("app-a", "client"), sa("app-c", "other")],
    );
    let planned = plan(&f);
    assert_eq!(
        list(&planned[0]),
        &json!([{"namespace": "app-b", "name": "search"}])
    );
    let text = list(&planned[0]).to_string();
    assert!(!text.contains("app-a"), "{text}");
    assert!(!text.contains("app-c"), "{text}");
}

#[test]
fn an_explicit_empty_instance_list_is_deny_all() {
    let f = fleet_with_default_access(
        vec![with_access("team-a", Some(Vec::new()))],
        vec![sa("app-a", "client")],
    );
    let planned = plan(&f);
    assert_eq!(list(&planned[0]), &json!([]));
}

#[test]
fn fleet_access_rejects_wildcards_groups_and_usernames() {
    let cases = [
        ("*", "client", "namespace"),
        ("system:serviceaccounts", "client", "namespace"),
        ("app-a", "*", "name"),
        ("app-a", "system:serviceaccount:app-a:client", "name"),
    ];
    for (namespace, name, field) in cases {
        let value = if field == "namespace" {
            namespace
        } else {
            name
        };
        let f = fleet(vec![with_access("team-a", Some(vec![sa(namespace, name)]))]);
        let planned = plan(&f);
        let expected = format!(
            "allowedServiceAccounts subject ({namespace}/{name}): {field} \"{value}\" must be an exact DNS-1123 name"
        );
        assert!(
            rejection(&planned[0]).contains(&expected),
            "{}",
            rejection(&planned[0])
        );
    }

    let f = fleet_with_default_access(
        vec![with_access("team-a", None), with_access("team-b", None)],
        vec![sa("*", "client")],
    );
    let planned = plan(&f);
    assert!(matches!(planned[0].outcome, PlanOutcome::Rejected(_)));
    assert!(matches!(planned[1].outcome, PlanOutcome::Rejected(_)));
}

#[test]
fn instances_spec_access_is_rejected() {
    let spec =
        json!({"access": {"allowedServiceAccounts": [{"namespace": "app-a", "name": "client"}]}});
    let planned = plan(&fleet(vec![instance("team-a", Some(spec.clone()))]));
    assert!(
        rejection(&planned[0]).contains("instances[].spec.access"),
        "{}",
        rejection(&planned[0])
    );

    let mut both = instance("team-a", Some(spec));
    both.access = Some(access(vec![sa("app-b", "search")]));
    let planned = plan(&fleet(vec![both]));
    assert!(
        rejection(&planned[0]).contains("instances[].spec.access"),
        "{}",
        rejection(&planned[0])
    );
}

#[test]
fn a_rejected_access_entry_does_not_stop_the_others() {
    let f = fleet(vec![
        with_access("team-a", Some(vec![sa("*", "client")])),
        with_access("team-b", Some(vec![sa("app-b", "search")])),
    ]);
    let planned = plan(&f);
    assert!(matches!(planned[0].outcome, PlanOutcome::Rejected(_)));
    assert!(matches!(planned[1].outcome, PlanOutcome::Ready(_)));
}

#[test]
fn the_steady_state_apply_keeps_the_access_list() {
    let f = fleet_with_default_access(
        vec![with_access("team-a", Some(vec![sa("app-b", "search")]))],
        vec![sa("app-a", "client"), sa("app-c", "other")],
    );
    let planned = plan(&f);
    let body = apply_object("search", &planned[0], ready(&planned[0]));
    assert_eq!(
        body["spec"]["access"]["allowedServiceAccounts"],
        json!([{"namespace": "app-b", "name": "search"}])
    );
}

/// A status entry for `team-x`/`search` in `state` with `message`. Its
/// `spec_hash` is empty; a test that needs one sets it.
fn status_entry(state: &str, message: &str) -> FleetEntryStatus {
    FleetEntryStatus {
        namespace: "team-x".to_string(),
        name: "search".to_string(),
        state: state.to_string(),
        message: message.to_string(),
        spec_hash: String::new(),
    }
}

#[test]
fn counts_match_a_mixed_entry_list() {
    let f = fleet(vec![
        instance("team-a", None),
        instance("team-b", None),
        instance("team-c", None),
        instance("team-d", None),
    ]);
    let planned = plan(&f);
    let entries = vec![
        status_entry("Created", ""),
        status_entry("Applied", ""),
        status_entry("Rejected", "bad override"),
        status_entry("Orphaned", "retained"),
        status_entry("Pruned", ""),
        status_entry("NamespaceMissing", "no namespace"),
    ];
    let status = project_status(&planned, entries, 1);
    assert_eq!(status.desired_instances, 4);
    assert_eq!(status.materialized_instances, 2);
    assert_eq!(status.applied_instances, 2);
    assert_eq!(status.rejected_instances, 1);
    assert_eq!(status.orphaned_instances, 1);
    assert_eq!(status.pruned_instances, 1);
    assert_eq!(
        status.message,
        "2/4 instances converged, 1 rejected, 1 orphaned, 1 pruned"
    );
}

#[test]
fn a_zero_part_is_left_out_of_the_message() {
    let planned = plan(&fleet(vec![instance("team-a", None)]));
    let status = project_status(&planned, vec![status_entry("Applied", "")], 1);
    assert_eq!(status.message, "1/1 instances converged");
}

#[test]
fn spec_hash_is_stable_for_equal_specs() {
    let a = spec_hash(&json!({"image": "ghcr.io/acme/lumen:0.4.27", "serving": {"cpu": "2"}}));
    let b = spec_hash(&json!({"image": "ghcr.io/acme/lumen:0.4.27", "serving": {"cpu": "2"}}));
    assert_eq!(a, b);
    assert_eq!(a.len(), 64);
    assert!(a
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    // SHA-256 of the canonical JSON of an empty object, `{}`.
    assert_eq!(
        spec_hash(&json!({})),
        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    );
}

#[test]
fn spec_hash_changes_with_an_override() {
    let plain = plan(&fleet(vec![instance("team-a", None)]));
    let overridden = plan(&fleet(vec![instance(
        "team-a",
        Some(json!({"serving": {"cpu": "4"}})),
    )]));
    assert_ne!(
        spec_hash(ready(&plain[0])),
        spec_hash(ready(&overridden[0]))
    );
}

#[test]
fn spec_hash_ignores_json_key_order() {
    let a: Value = serde_json::from_str(r#"{"a":1,"b":{"x":1,"y":2}}"#).expect("parse a");
    let b: Value = serde_json::from_str(r#"{"b":{"y":2,"x":1},"a":1}"#).expect("parse b");
    assert_eq!(spec_hash(&a), spec_hash(&b));
    assert_eq!(canonical_json(&a), r#"{"a":1,"b":{"x":1,"y":2}}"#);
    assert_eq!(canonical_json(&b), r#"{"a":1,"b":{"x":1,"y":2}}"#);
}

#[test]
fn an_old_status_without_the_new_fields_reads_as_zero() {
    let status: LumenFleetStatus = serde_json::from_value(json!({
        "observedGeneration": 1,
        "desiredInstances": 2,
        "appliedInstances": 1,
        "message": "x"
    }))
    .expect("old status parses");
    assert_eq!(status.materialized_instances, 0);
    assert_eq!(status.rejected_instances, 0);
    assert_eq!(status.orphaned_instances, 0);
    assert_eq!(status.pruned_instances, 0);

    let entry: FleetEntryStatus = serde_json::from_value(json!({
        "namespace": "team-x",
        "name": "search",
        "state": "Applied"
    }))
    .expect("old entry parses");
    assert_eq!(entry.spec_hash, "");
}

#[test]
fn spec_hash_is_omitted_for_states_without_one() {
    let value = serde_json::to_value(status_entry("ApplyFailed", "boom")).expect("serialize");
    assert!(value.get("specHash").is_none());
}

#[test]
fn a_repaired_entry_leaves_no_stale_failure() {
    let planned = plan(&fleet(vec![instance("team-x", None)]));

    // Pass 1: the entry failed to apply.
    let pass1 = project_status(&planned, vec![status_entry("ApplyFailed", "boom")], 1);
    assert_eq!(pass1.materialized_instances, 0);
    assert!(pass1.message.starts_with("0/1 instances converged"));

    // Pass 2: the same entry is applied, with its hash.
    let applied = FleetEntryStatus {
        spec_hash: "abc".to_string(),
        ..status_entry("Applied", "")
    };
    let pass2 = project_status(&planned, vec![applied], 2);
    assert_eq!(pass2.materialized_instances, 1);
    assert_eq!(pass2.message, "1/1 instances converged");

    // The merge patch replaces `entries` as a whole: the failed row is gone.
    let patch = status_patch(&pass2);
    let rows = patch["status"]["entries"].to_string();
    assert!(!rows.contains("boom"), "stale failure in patch: {rows}");
}

/// A `Rejected` entry is a rejected instance, not a materialized one. A
/// `desired - applied` shortcut would count it as materialized.
#[test]
fn a_rejected_entry_alone_is_not_materialized() {
    let planned = vec![PlannedInstance {
        namespace: "team-x".to_string(),
        name: "search".to_string(),
        outcome: PlanOutcome::Rejected("bogus".to_string()),
    }];
    let status = project_status(&planned, vec![status_entry("Rejected", "bogus")], 1);
    assert_eq!(status.materialized_instances, 0);
    assert_eq!(status.rejected_instances, 1);
}

#[test]
fn an_empty_pass_clears_the_old_entries() {
    let planned = plan(&fleet(vec![]));
    let status = project_status(&planned, vec![], 2);
    let patch = status_patch(&status);
    assert_eq!(patch["status"]["entries"], json!([]));
    assert_eq!(patch["status"]["message"], json!("0/0 instances converged"));
}

// A typed runtime field that the spec patch also writes is refused for that instance
// (issue #40). The patch would otherwise win silently.

#[test]
fn a_typed_runtime_field_also_in_the_patch_is_rejected() {
    let mut tenant = instance("team-a", Some(json!({"runtime": {"snapshotSecs": 5}})));
    tenant.runtime = Some(RuntimeSettings {
        snapshot_secs: Some(60),
        ..Default::default()
    });
    let planned = plan(&fleet(vec![tenant]));
    let reason = rejection(&planned[0]);
    assert!(reason.contains("team-a/search"), "{reason}");
    assert!(reason.contains("snapshotSecs"), "{reason}");
    assert!(reason.contains("instances[].runtime"), "{reason}");
    assert!(
        reason.contains("instances[].spec.runtime.snapshotSecs"),
        "{reason}"
    );
}

#[test]
fn a_typed_runtime_field_also_in_extra_env_is_rejected() {
    let mut tenant = instance(
        "team-a",
        Some(
            json!({"extraEnv": [{"name": "LUMEN_SNAPSHOT_SECS", "value": "SENTINEL-OVERLAP-4412"}]}),
        ),
    );
    tenant.runtime = Some(RuntimeSettings {
        snapshot_secs: Some(60),
        ..Default::default()
    });
    let planned = plan(&fleet(vec![tenant]));
    let reason = rejection(&planned[0]);
    assert!(reason.contains("snapshotSecs"), "{reason}");
    assert!(reason.contains("instances[].spec.extraEnv"), "{reason}");
    assert!(!reason.contains("SENTINEL-OVERLAP-4412"), "{reason}");
}

#[test]
fn a_typed_runtime_field_also_in_extra_args_is_rejected() {
    let mut tenant = instance(
        "team-a",
        Some(json!({"extraArgs": ["--snapshot-secs=SENTINEL-OVERLAP-4413"]})),
    );
    tenant.runtime = Some(RuntimeSettings {
        snapshot_secs: Some(60),
        ..Default::default()
    });
    let planned = plan(&fleet(vec![tenant]));
    let reason = rejection(&planned[0]);
    assert!(reason.contains("snapshotSecs"), "{reason}");
    assert!(reason.contains("instances[].spec.extraArgs"), "{reason}");
    assert!(!reason.contains("SENTINEL-OVERLAP-4413"), "{reason}");
}

#[test]
fn a_conflict_rejects_only_that_instance() {
    let mut team_c = instance("team-c", Some(json!({"runtime": {"snapshotSecs": 5}})));
    team_c.runtime = Some(RuntimeSettings {
        snapshot_secs: Some(60),
        ..Default::default()
    });
    let planned = plan(&fleet(vec![
        instance("team-a", None),
        instance("team-b", None),
        team_c,
    ]));
    ready(&planned[0]);
    ready(&planned[1]);
    rejection(&planned[2]);
}

/// Negative control: when `typed_patch_overlap` is always `None`, the three rejection
/// tests above fail.
#[test]
fn different_options_in_runtime_and_patch_do_not_conflict() {
    let mut tenant = instance("team-a", Some(json!({"runtime": {"graceSecs": 9}})));
    tenant.runtime = Some(RuntimeSettings {
        snapshot_secs: Some(60),
        ..Default::default()
    });
    let planned = plan(&fleet(vec![tenant]));
    assert_eq!(
        ready(&planned[0])["runtime"],
        json!({"snapshotSecs": 60, "graceSecs": 9})
    );
}

#[test]
fn a_reserved_flag_in_the_patch_is_rejected_for_that_instance_only() {
    let planned = plan(&fleet(vec![
        instance(
            "team-a",
            Some(json!({"extraArgs": ["--port=SENTINEL-FLAG-4414"]})),
        ),
        instance("team-b", None),
    ]));
    let reason = rejection(&planned[0]);
    assert!(reason.contains("--port"), "{reason}");
    assert!(!reason.contains("SENTINEL-FLAG-4414"), "{reason}");
    ready(&planned[1]);
}
