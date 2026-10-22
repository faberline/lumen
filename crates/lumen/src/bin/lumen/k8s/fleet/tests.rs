use crate::cli::k8s::{K8sFleetProfile, K8sFleetRenderArgs};
use crate::k8s::fleet::render_fleet_yaml;

/// Every handed-out `LumenFleet` must actually materialize. A rendered
/// template is the first thing a deployer applies, so a duplicated
/// `serving:` key (last wins, the CPU/memory request silently gone) or a
/// `defaults`/`instances` pair that merges into two conflicting shard
/// topologies would ship as a cluster that comes up wrong — or not at all —
/// with the
/// mistake in our YAML rather than theirs.
#[cfg(feature = "operator")]
#[test]
fn every_rendered_fleet_profile_materializes_its_instances() {
    use lumen::operator::fleet::{plan, PlanOutcome};

    for profile in [
        K8sFleetProfile::Dev,
        K8sFleetProfile::Prod,
        K8sFleetProfile::Template,
    ] {
        let yaml = render_fleet_yaml(&K8sFleetRenderArgs {
            profile,
            name: None,
            image: None,
            out: None,
        });
        // The template is meant to be edited before it applies, so its
        // required-decision placeholders are filled in here rather than
        // pretending an unedited skeleton is deployable. Everything else
        // about it — key structure, the defaults/instances merge, the
        // per-instance topology pairing — is exactly what ships.
        let yaml = yaml
            .replace("REPLACE_ME__SHARD_COUNT", "1")
            .replace("REPLACE_ME__REPLICAS_PER_SHARD", "1")
            .replace("REPLACE_ME__VOTER_COUNT", "1");
        // Parsing is itself the duplicate-key check: serde_yaml rejects a
        // mapping that names one key twice, which is how a second
        // `serving:` block silently eating the CPU/memory request gets
        // caught rather than shipped.
        let fleet: lumen::operator::LumenFleet = serde_yaml::from_str(&yaml)
            .unwrap_or_else(|err| panic!("profile does not parse: {err}\n{yaml}"));

        let planned = plan(&fleet);
        assert!(
            !planned.is_empty(),
            "a fleet that declares no data plane is not a usable starting point\n{yaml}"
        );
        for instance in &planned {
            if let PlanOutcome::Rejected(reason) = &instance.outcome {
                panic!(
                    "namespace {} would be rejected: {reason}\n{yaml}",
                    instance.namespace
                );
            }
        }
    }
}

/// The template exists to name the knobs a deployer owns; a knob that
/// silently drops out of it is a knob nobody knows to set.
#[test]
fn the_fleet_template_names_every_deployer_owned_knob() {
    let yaml = render_fleet_yaml(&K8sFleetRenderArgs {
        profile: K8sFleetProfile::Template,
        name: None,
        image: None,
        out: None,
    });
    for knob in [
        "nodeSelector",       // which node pool
        "raftStorageClass",   // SSD vs standard disk
        "serviceAccountName", // the KSA the data plane runs as
        "cpu:",               // request — what triggers shard autoscaling
        "memory:",
        "raftStorage:", // per-tenant disk size
        "prunePolicy",
    ] {
        assert!(yaml.contains(knob), "template lost `{knob}`\n{yaml}");
    }
}
