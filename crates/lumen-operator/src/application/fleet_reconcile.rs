//! The fleet materialization loop: leader-gated on its own Lease, it seeds,
//! applies and prunes the `Lumen` objects every fleet declares.

use std::collections::BTreeSet;
use std::time::Duration;

use serde_json::json;

use crate::domain::lumen_fleet::plan::{apply_object, plan, seed_object};
use crate::domain::lumen_fleet::{
    FleetEntryStatus, LumenFleet, PlanOutcome, PlannedInstance, PrunePolicy, FLEET_LABEL,
};

pub mod control;

#[cfg(test)]
mod tests;

use control::FleetControl;

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
        let control = crate::infrastructure::kube_fleet_control::KubeFleetControl::new(client);
        loop {
            if election
                .is_leader
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                match fleets.list(&Default::default()).await {
                    Ok(list) => {
                        for fleet in list.items {
                            if let Err(err) = converge(&control, &fleet).await {
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

/// One convergence pass over a single fleet. A failure is recorded on the
/// entry it belongs to, and the pass goes on; only a failed status patch
/// returns `Err`.
async fn converge(control: &dyn FleetControl, fleet: &LumenFleet) -> anyhow::Result<()> {
    use kube::ResourceExt;

    let fleet_name = fleet.name_any();
    let planned = plan(fleet);
    let mut entries = Vec::new();
    let mut applied = 0;
    let mut declared: BTreeSet<(String, String)> = BTreeSet::new();

    for instance in &planned {
        let spec = match &instance.outcome {
            PlanOutcome::Ready(spec) => spec,
            PlanOutcome::Rejected(reason) => {
                entries.push(entry(instance, "Rejected", reason));
                continue;
            }
        };
        // Declared before any cluster call: an entry whose read fails stays
        // declared, so `PrunePolicy::Delete` never deletes it.
        declared.insert((instance.namespace.clone(), instance.name.clone()));

        match control.namespace_exists(&instance.namespace).await {
            Ok(true) => {}
            Ok(false) => {
                entries.push(entry(
                    instance,
                    "NamespaceMissing",
                    "the namespace does not exist; the fleet does not create namespaces",
                ));
                continue;
            }
            Err(err) => {
                entries.push(entry(
                    instance,
                    "ObserveFailed",
                    &format!("could not check the namespace: {err}"),
                ));
                continue;
            }
        }

        match control
            .lumen_fleet_label(&instance.namespace, &instance.name)
            .await
        {
            Err(err) => entries.push(entry(
                instance,
                "ObserveFailed",
                &format!("could not read the Lumen: {err}"),
            )),
            Ok(None) => match control
                .create_lumen(seed_object(&fleet_name, instance, spec))
                .await
            {
                Ok(()) => {
                    applied += 1;
                    entries.push(entry(instance, "Created", ""));
                }
                Err(err) => entries.push(entry(instance, "ApplyFailed", &err.to_string())),
            },
            Ok(Some(owner)) => {
                // A `Lumen` this fleet did not create is not this fleet's to
                // overwrite: a hand-authored instance at the same name would
                // otherwise be silently replaced by the fleet's defaults.
                if owner.as_deref() != Some(fleet_name.as_str()) {
                    entries.push(entry(
                        instance,
                        "NotAdopted",
                        &format!(
                            "a Lumen already exists here and is not labelled {FLEET_LABEL}={fleet_name} \
                             (found {}); left untouched",
                            owner.as_deref().unwrap_or("no label")
                        ),
                    ));
                    continue;
                }
                let body = apply_object(&fleet_name, instance, spec);
                match control
                    .apply_lumen(&instance.namespace, &instance.name, body)
                    .await
                {
                    Ok(()) => {
                        applied += 1;
                        entries.push(entry(instance, "Applied", ""));
                    }
                    Err(err) => entries.push(entry(instance, "ApplyFailed", &err.to_string())),
                }
            }
        }
    }

    // A failed list prunes nothing and keeps the entries already computed. The
    // status says so, and the next pass retries.
    let (pruned, prune_note) = match prune(control, fleet, &declared).await {
        Ok(pruned) => (pruned, None),
        Err(err) => (
            Vec::new(),
            Some(format!(
                "prune skipped: could not list the fleet's Lumens: {err}"
            )),
        ),
    };
    entries.extend(pruned);

    let mut message = format!("{applied}/{} instances converged", planned.len());
    if let Some(note) = prune_note {
        message.push_str("; ");
        message.push_str(&note);
    }
    let status = json!({
        "status": {
            "observedGeneration": fleet.metadata.generation.unwrap_or(0),
            "desiredInstances": planned.len() as i32,
            "appliedInstances": applied,
            "entries": entries,
            "message": message,
        }
    });
    control.patch_fleet_status(&fleet_name, status).await
}

/// Report — and, only under [`PrunePolicy::Delete`], remove — instances this
/// fleet created that its spec no longer declares. A list failure is returned
/// to the caller, which skips pruning for the pass.
async fn prune(
    control: &dyn FleetControl,
    fleet: &LumenFleet,
    declared: &BTreeSet<(String, String)>,
) -> anyhow::Result<Vec<FleetEntryStatus>> {
    use kube::ResourceExt;

    let fleet_name = fleet.name_any();
    let mut out = Vec::new();
    for (namespace, name) in control.list_fleet_lumens(&fleet_name).await? {
        if declared.contains(&(namespace.clone(), name.clone())) {
            continue;
        }
        let orphan = PlannedInstance {
            namespace: namespace.clone(),
            name: name.clone(),
            outcome: PlanOutcome::Rejected(String::new()),
        };
        match fleet.spec.prune_policy {
            PrunePolicy::Retain => out.push(entry(
                &orphan,
                "Orphaned",
                "no longer declared by this fleet; retained (set prunePolicy: Delete to remove)",
            )),
            PrunePolicy::Delete => match control.delete_lumen(&namespace, &name).await {
                Ok(()) => out.push(entry(&orphan, "Pruned", "")),
                Err(err) => out.push(entry(&orphan, "ApplyFailed", &err.to_string())),
            },
        }
    }
    Ok(out)
}

pub(crate) fn entry(instance: &PlannedInstance, state: &str, message: &str) -> FleetEntryStatus {
    FleetEntryStatus {
        namespace: instance.namespace.clone(),
        name: instance.name.clone(),
        state: state.to_string(),
        message: message.to_string(),
    }
}
