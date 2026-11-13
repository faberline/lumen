//! The fleet materialization loop: leader-gated on its own Lease, it seeds,
//! applies and prunes the `Lumen` objects every fleet declares.

use std::collections::BTreeSet;
use std::time::Duration;

use serde_json::json;

use crate::domain::lumen_fleet::children::{
    adoption, child_key, prune_decision, Adoption, ChildKey, PruneDecision, ORPHAN_RETAINED_MESSAGE,
};
use crate::domain::lumen_fleet::plan::{apply_object, plan, seed_object};
use crate::domain::lumen_fleet::{
    FleetEntryStatus, LumenFleet, PlanOutcome, PlannedInstance, FLEET_LABEL, FLEET_MANAGER,
    FLEET_SEED_MANAGER,
};
use crate::domain::lumen_spec::Lumen;

/// Leader-election Lease for the fleet loop, independent of the main
/// controller's so each can fail over on its own.
const FLEET_LEASE_NAME: &str = "lumen-fleet";

/// How often the fleet re-materializes its instances.
const FLEET_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Run the fleet materialization loop alongside the main controller.
///
/// A poll loop rather than a `kube` `Controller`: the fleet is one
/// cluster-scoped object edited by a human, its children are `Lumen` CRs that
/// have their own controller, and a 30s convergence pass is far below the
/// latency anyone can perceive on a deploy. Leader-gated on its own Lease so a
/// failover of the main controller does not stall it and vice versa.
pub fn spawn_fleet_loop(client: kube::Client) {
    // Same identity/namespace resolution as every other independently
    // leader-gated loop in this operator.
    let identity = std::env::var("POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| FLEET_LEASE_NAME.to_string());
    let namespace =
        std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "lumen-operator-system".to_string());
    let election = crate::infrastructure::lease::Election::new(identity);
    crate::infrastructure::lease::spawn(
        client.clone(),
        namespace,
        FLEET_LEASE_NAME.to_string(),
        election.clone(),
    );
    tokio::spawn(async move {
        let fleets: kube::Api<LumenFleet> = kube::Api::all(client.clone());
        loop {
            if election
                .is_leader
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                match fleets.list(&Default::default()).await {
                    Ok(list) => {
                        for fleet in list.items {
                            if let Err(err) = converge(&client, &fleets, &fleet).await {
                                tracing::warn!(
                                    fleet = %fleet.metadata.name.clone().unwrap_or_default(),
                                    error = %err,
                                    "fleet convergence pass failed"
                                );
                            }
                        }
                    }
                    Err(err) => tracing::warn!(error = %err, "fleet: list LumenFleet failed"),
                }
            }
            tokio::time::sleep(FLEET_POLL_INTERVAL).await;
        }
    });
}

/// One convergence pass over a single fleet.
async fn converge(
    client: &kube::Client,
    fleets: &kube::Api<LumenFleet>,
    fleet: &LumenFleet,
) -> anyhow::Result<()> {
    use kube::ResourceExt;

    let fleet_name = fleet.name_any();
    let planned = plan(fleet);
    let mut entries = Vec::new();
    let mut applied = 0;
    let mut declared: BTreeSet<ChildKey> = BTreeSet::new();

    for instance in &planned {
        let spec = match &instance.outcome {
            PlanOutcome::Ready(spec) => spec,
            PlanOutcome::Rejected(reason) => {
                entries.push(entry(instance, "Rejected", reason));
                continue;
            }
        };
        declared.insert(child_key(&instance.namespace, &instance.name));

        if !namespace_exists(client, &instance.namespace).await? {
            entries.push(entry(
                instance,
                "NamespaceMissing",
                "the namespace does not exist; the fleet does not create namespaces",
            ));
            continue;
        }

        let lumens: kube::Api<Lumen> = kube::Api::namespaced(client.clone(), &instance.namespace);
        let live = lumens.get_opt(&instance.name).await?;
        let owner = live
            .as_ref()
            .map(|object| object.labels().get(FLEET_LABEL).map(String::as_str));
        match adoption(owner, &fleet_name) {
            Adoption::Create => {
                let body: Lumen = serde_json::from_value(seed_object(&fleet_name, instance, spec))?;
                let params = kube::api::PostParams {
                    field_manager: Some(FLEET_SEED_MANAGER.to_string()),
                    ..Default::default()
                };
                match lumens.create(&params, &body).await {
                    Ok(_) => {
                        applied += 1;
                        entries.push(entry(instance, "Created", ""));
                    }
                    Err(err) => {
                        entries.push(entry(instance, "ApplyFailed", &err.to_string()));
                    }
                }
            }
            // A `Lumen` this fleet did not create is not this fleet's to
            // overwrite: a hand-authored instance at the same name would
            // otherwise be silently replaced by the fleet's defaults.
            Adoption::NotAdopted { message } => {
                entries.push(entry(instance, "NotAdopted", &message));
                continue;
            }
            Adoption::Apply => {
                let body = apply_object(&fleet_name, instance, spec);
                match lumens
                    .patch(
                        &instance.name,
                        &kube::api::PatchParams::apply(FLEET_MANAGER).force(),
                        &kube::api::Patch::Apply(&body),
                    )
                    .await
                {
                    Ok(_) => {
                        applied += 1;
                        entries.push(entry(instance, "Applied", ""));
                    }
                    Err(err) => entries.push(entry(instance, "ApplyFailed", &err.to_string())),
                }
            }
        }
    }

    entries.extend(prune(client, fleet, &declared).await?);

    let status = json!({
        "status": {
            "observedGeneration": fleet.metadata.generation.unwrap_or(0),
            "desiredInstances": planned.len() as i32,
            "appliedInstances": applied,
            "entries": entries,
            "message": format!("{applied}/{} instances converged", planned.len()),
        }
    });
    fleets
        .patch_status(
            &fleet_name,
            &kube::api::PatchParams::default(),
            &kube::api::Patch::Merge(&status),
        )
        .await?;
    Ok(())
}

/// Report — and, only under [`crate::domain::lumen_fleet::PrunePolicy::Delete`], remove — instances this
/// fleet created that its spec no longer declares.
async fn prune(
    client: &kube::Client,
    fleet: &LumenFleet,
    declared: &BTreeSet<ChildKey>,
) -> anyhow::Result<Vec<FleetEntryStatus>> {
    use kube::ResourceExt;

    let fleet_name = fleet.name_any();
    let lumens: kube::Api<Lumen> = kube::Api::all(client.clone());
    let params = kube::api::ListParams::default().labels(&format!("{FLEET_LABEL}={fleet_name}"));
    let mut out = Vec::new();
    for live in lumens.list(&params).await?.items {
        let namespace = live.namespace().unwrap_or_default();
        let name = live.name_any();
        let orphan = PlannedInstance {
            namespace: namespace.clone(),
            name: name.clone(),
            outcome: PlanOutcome::Rejected(String::new()),
        };
        match prune_decision(
            &child_key(&namespace, &name),
            declared,
            fleet.spec.prune_policy,
        ) {
            PruneDecision::Keep => continue,
            PruneDecision::Orphaned => {
                out.push(entry(&orphan, "Orphaned", ORPHAN_RETAINED_MESSAGE));
            }
            PruneDecision::Pruned => {
                let scoped: kube::Api<Lumen> = kube::Api::namespaced(client.clone(), &namespace);
                match scoped.delete(&name, &Default::default()).await {
                    Ok(_) => out.push(entry(&orphan, "Pruned", "")),
                    Err(err) => out.push(entry(&orphan, "ApplyFailed", &err.to_string())),
                }
            }
        }
    }
    Ok(out)
}

async fn namespace_exists(client: &kube::Client, namespace: &str) -> anyhow::Result<bool> {
    let api: kube::Api<k8s_openapi::api::core::v1::Namespace> = kube::Api::all(client.clone());
    Ok(api.get_opt(namespace).await?.is_some())
}

pub(crate) fn entry(instance: &PlannedInstance, state: &str, message: &str) -> FleetEntryStatus {
    FleetEntryStatus {
        namespace: instance.namespace.clone(),
        name: instance.name.clone(),
        state: state.to_string(),
        message: message.to_string(),
    }
}
