//! The fleet materialization loop: leader-gated on its own Lease, it seeds,
//! applies and prunes the `Lumen` objects every fleet declares.

use std::collections::BTreeSet;
use std::time::Duration;

use futures::StreamExt;
use kube::runtime::{watcher, WatchStreamExt};

use crate::domain::lumen_fleet::aggregate::{aggregate, live_child_from_json, DeclaredEntry};
use crate::domain::lumen_fleet::children::{
    adoption, child_key, prune_decision, Adoption, ChildKey, PruneDecision, ORPHAN_RETAINED_MESSAGE,
};
use crate::domain::lumen_fleet::plan::{apply_object, plan, seed_object};
use crate::domain::lumen_fleet::status::{project_status, spec_hash, status_patch};
use crate::domain::lumen_fleet::{
    FleetEntryStatus, LumenFleet, PlanOutcome, PlannedInstance, FLEET_LABEL,
};
use crate::domain::lumen_spec::Lumen;

pub mod control;
pub mod schedule;
use control::FleetControl;
use schedule::{FleetEvent, Scheduler, FLEET_REPAIR_INTERVAL};
#[cfg(test)]
mod converge_tests;
#[cfg(test)]
mod tests;

/// Leader-election Lease for the fleet loop, independent of the main
/// controller's so each can fail over on its own.
pub(crate) const FLEET_LEASE_NAME: &str = "lumen-fleet";

/// How often the loop wakes to check leadership and the repair schedule.
/// Each wake does no cluster read unless work is due.
const FLEET_TICK: Duration = Duration::from_secs(1);

/// Run the fleet materialization loop alongside the main controller.
///
/// Driven by watch events on `LumenFleet` and on the `Lumen` children that
/// carry the fleet label, with one low-frequency full repair pass to catch a
/// missed event. The decisions live in [`schedule::Scheduler`]. Leader-gated on
/// its own Lease so a failover of the main controller does not stall it and
/// vice versa.
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
        let lumens: kube::Api<Lumen> = kube::Api::all(client.clone());
        let control =
            crate::infrastructure::kube_fleet_control::KubeFleetControl::new(client.clone());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<FleetEvent>();

        let fleet_watch = fleets.clone();
        let fleet_tx = tx.clone();
        tokio::spawn(async move {
            use kube::ResourceExt;
            let stream =
                kube::runtime::watcher(fleet_watch, watcher::Config::default()).default_backoff();
            let mut stream = std::pin::pin!(stream);
            while let Some(event) = stream.next().await {
                let mapped = match event {
                    Ok(watcher::Event::Apply(o)) | Ok(watcher::Event::InitApply(o)) => {
                        FleetEvent::FleetChanged(o.name_any())
                    }
                    Ok(watcher::Event::Delete(o)) => FleetEvent::FleetDeleted(o.name_any()),
                    Ok(watcher::Event::Init) | Ok(watcher::Event::InitDone) => continue,
                    Err(err) => {
                        tracing::warn!(error = %err, "fleet watch error");
                        continue;
                    }
                };
                if fleet_tx.send(mapped).is_err() {
                    break;
                }
            }
        });

        let child_watch = lumens.clone();
        let child_tx = tx.clone();
        tokio::spawn(async move {
            use kube::ResourceExt;
            let config = watcher::Config::default().labels(FLEET_LABEL);
            let stream = kube::runtime::watcher(child_watch, config).default_backoff();
            let mut stream = std::pin::pin!(stream);
            while let Some(event) = stream.next().await {
                let mapped = match event {
                    Ok(watcher::Event::Apply(o)) | Ok(watcher::Event::InitApply(o)) => {
                        FleetEvent::ChildChanged(o.labels().get(FLEET_LABEL).cloned())
                    }
                    Ok(watcher::Event::Delete(o)) => {
                        FleetEvent::ChildDeleted(o.labels().get(FLEET_LABEL).cloned())
                    }
                    Ok(watcher::Event::Init) | Ok(watcher::Event::InitDone) => continue,
                    Err(err) => {
                        tracing::warn!(error = %err, "fleet child watch error");
                        continue;
                    }
                };
                if child_tx.send(mapped).is_err() {
                    break;
                }
            }
        });

        let start = std::time::Instant::now();
        let mut sched = Scheduler::new(Some(FLEET_REPAIR_INTERVAL));
        let mut tick = tokio::time::interval(FLEET_TICK);
        loop {
            let leader = election
                .is_leader
                .load(std::sync::atomic::Ordering::Relaxed);
            sched.set_leader(leader, start.elapsed());
            tokio::select! {
                Some(event) = rx.recv() => sched.observe(event),
                _ = tick.tick() => {}
            }
            let batch = sched.poll(start.elapsed());
            if batch.is_empty() {
                continue;
            }
            if batch.full_repair {
                match fleets.list(&Default::default()).await {
                    Ok(list) => {
                        for fleet in &list.items {
                            converge_logged(&control, fleet).await;
                        }
                    }
                    Err(err) => tracing::warn!(error = %err, "fleet: list LumenFleet failed"),
                }
            } else {
                for name in &batch.fleets {
                    match fleets.get_opt(name).await {
                        Ok(Some(fleet)) => converge_logged(&control, &fleet).await,
                        // The fleet is gone; its children stay (prune is per pass).
                        Ok(None) => {}
                        Err(err) => {
                            tracing::warn!(fleet = %name, error = %err, "fleet: get LumenFleet failed")
                        }
                    }
                }
            }
        }
    });
}

async fn converge_logged(control: &dyn FleetControl, fleet: &LumenFleet) {
    if let Err(err) = converge(control, fleet).await {
        tracing::warn!(
            fleet = %fleet.metadata.name.clone().unwrap_or_default(),
            error = %err,
            "fleet convergence pass failed"
        );
    }
}

/// One convergence pass over a single fleet.
///
/// A failed cluster call sets only its own entry (`ObserveFailed` or
/// `ApplyFailed`) and the pass goes on. Only a failed status write ends the
/// pass with an error.
async fn converge(control: &dyn FleetControl, fleet: &LumenFleet) -> anyhow::Result<()> {
    use kube::ResourceExt;

    let fleet_name = fleet.name_any();
    let planned = plan(fleet);
    let mut entries = Vec::new();
    let mut declared: BTreeSet<ChildKey> = BTreeSet::new();

    for instance in &planned {
        let spec = match &instance.outcome {
            PlanOutcome::Ready(spec) => spec,
            PlanOutcome::Rejected(reason) => {
                entries.push(entry(instance, "Rejected", reason));
                continue;
            }
        };
        // Declared before the first cluster call: an `ObserveFailed` entry
        // must stay declared so that `Delete` never prunes it.
        declared.insert(child_key(&instance.namespace, &instance.name));

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
                entries.push(entry(instance, "ObserveFailed", &err.to_string()));
                continue;
            }
        }

        let live = match control
            .lumen_fleet_label(&instance.namespace, &instance.name)
            .await
        {
            Ok(live) => live,
            Err(err) => {
                entries.push(entry(instance, "ObserveFailed", &err.to_string()));
                continue;
            }
        };
        match adoption(live.as_ref().map(|label| label.as_deref()), &fleet_name) {
            Adoption::Create => {
                let body = seed_object(&fleet_name, instance, spec);
                match control.create_lumen(body).await {
                    Ok(()) => entries.push(entry_with_hash(instance, "Created", spec)),
                    Err(err) => entries.push(entry(instance, "ApplyFailed", &err.to_string())),
                }
            }
            // A `Lumen` this fleet did not create is not this fleet's to
            // overwrite: a hand-authored instance at the same name would
            // otherwise be silently replaced by the fleet's defaults.
            Adoption::NotAdopted { message } => {
                entries.push(entry(instance, "NotAdopted", &message));
            }
            Adoption::Apply => {
                let body = apply_object(&fleet_name, instance, spec);
                match control
                    .apply_lumen(&instance.namespace, &instance.name, body)
                    .await
                {
                    Ok(()) => entries.push(entry_with_hash(instance, "Applied", spec)),
                    Err(err) => entries.push(entry(instance, "ApplyFailed", &err.to_string())),
                }
            }
        }
    }

    let mut prune_error = None;
    match prune(control, fleet, &declared).await {
        Ok(extra) => entries.extend(extra),
        Err(err) => prune_error = Some(err.to_string()),
    }

    // Conditions and counts: read each declared child once, then fold them.
    let mut declared_entries: Vec<DeclaredEntry> = Vec::new();
    for e in &entries {
        if e.state == "Orphaned" || e.state == "Pruned" {
            continue;
        }
        let keep = e.state == "Rejected" || declared.contains(&child_key(&e.namespace, &e.name));
        if !keep {
            continue;
        }
        let child = if e.state == "Created" || e.state == "Applied" {
            match control.lumen_live(&e.namespace, &e.name).await {
                Ok(Some(live)) => Some(live_child_from_json(&live)),
                _ => None,
            }
        } else {
            None
        };
        declared_entries.push(DeclaredEntry {
            namespace: e.namespace.clone(),
            name: e.name.clone(),
            state: e.state.clone(),
            child,
        });
    }
    let orphaned = entries.iter().filter(|e| e.state == "Orphaned").count() as i32;
    let agg = aggregate(&declared_entries, orphaned);

    let mut projected = project_status(&planned, entries, fleet.metadata.generation.unwrap_or(0));
    projected.ready_instances = agg.ready_instances;
    projected.degraded_instances = agg.degraded_instances;
    projected.not_ready_instances = agg.not_ready_instances;
    projected.conditions = agg.conditions;
    if let Some(err) = prune_error {
        projected
            .message
            .push_str(&format!("; prune skipped: {err}"));
    }
    control
        .patch_fleet_status(&fleet_name, status_patch(&projected))
        .await?;
    Ok(())
}

/// Report — and, only under [`PrunePolicy::Delete`](crate::domain::lumen_fleet::PrunePolicy::Delete), remove — instances this
/// fleet created that its spec no longer declares. Only the list call can
/// fail the whole prune; a failed delete stays on its own entry.
async fn prune(
    control: &dyn FleetControl,
    fleet: &LumenFleet,
    declared: &BTreeSet<ChildKey>,
) -> anyhow::Result<Vec<FleetEntryStatus>> {
    use kube::ResourceExt;

    let fleet_name = fleet.name_any();
    let mut out = Vec::new();
    for (namespace, name) in control.list_fleet_lumens(&fleet_name).await? {
        let key = child_key(&namespace, &name);
        let orphan = PlannedInstance {
            namespace: namespace.clone(),
            name: name.clone(),
            outcome: PlanOutcome::Rejected(String::new()),
        };
        match prune_decision(&key, declared, fleet.spec.prune_policy) {
            PruneDecision::Keep => continue,
            PruneDecision::Orphaned => {
                out.push(entry(&orphan, "Orphaned", ORPHAN_RETAINED_MESSAGE));
            }
            PruneDecision::Pruned => match control.delete_lumen(&namespace, &name).await {
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
        spec_hash: String::new(),
    }
}

/// An entry that carries the hash of the merged spec the fleet wrote for it.
/// The hash is of `spec` (the merged `Lumen` spec), not of the apply body.
pub(crate) fn entry_with_hash(
    instance: &PlannedInstance,
    state: &str,
    spec: &serde_json::Value,
) -> FleetEntryStatus {
    FleetEntryStatus {
        spec_hash: spec_hash(spec),
        ..entry(instance, state, "")
    }
}
