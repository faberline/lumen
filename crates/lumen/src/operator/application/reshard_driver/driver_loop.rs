//! The reshard driver loop, leader-gated on its own Lease.

use std::collections::BTreeSet;
use std::time::Duration;

use kube::{Client, ResourceExt};

use crate::operator::application::reshard_driver::convergence_stall::prune_convergence_stall_cache;
use crate::operator::application::reshard_driver::oversize::prune_oversize_cache;
use crate::operator::application::reshard_driver::{
    drive_tick, DriveOutcome, DRIVER_LEASE_NAME, DRIVER_POLL_INTERVAL,
};
use crate::operator::domain::lumen_spec::Lumen;
use crate::operator::infrastructure::kube_cluster_control::KubeClusterControl;
use crate::operator::infrastructure::lease::{self, Election};

/// Background loop: every [`DRIVER_POLL_INTERVAL`], list every `Lumen` CR
/// cluster-wide and [`drive_tick`] it. Independently leader-gated (its own
/// [`DRIVER_LEASE_NAME`] Lease) from the shared `libs/service-k8s` apply loop —
/// either loop's leader may or may not be this replica, and both are safe to
/// run concurrently since every driver action is an idempotent-or-checkpointed
/// spec patch / additive data-plane call.
pub fn spawn_reshard_driver_loop(client: Client) {
    // Mirrors `libs/service-k8s::controller`'s own `identity`/`lease_namespace`
    // helpers (private to that crate, so duplicated here) so both
    // independently-leader-gated loops resolve the same pod identity and
    // Lease namespace from the same env vars.
    let identity = std::env::var("POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| DRIVER_LEASE_NAME.to_string());
    let namespace =
        std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "lumen-operator-system".to_string());
    let election = Election::new(identity);
    lease::spawn(
        client.clone(),
        namespace,
        DRIVER_LEASE_NAME.to_string(),
        election.clone(),
    );
    let control = KubeClusterControl::new(client.clone());
    tokio::spawn(async move {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let api: kube::Api<Lumen> = kube::Api::all(client);
        loop {
            if election
                .is_leader
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                match api.list(&Default::default()).await {
                    Ok(list) => {
                        // #1458 R4: this list is already the authoritative
                        // live-CR set, so pruning stale oversize-cache
                        // entries here needs no extra k8s API call.
                        let live_uids: BTreeSet<String> =
                            list.items.iter().filter_map(|l| l.uid()).collect();
                        prune_oversize_cache(&live_uids);
                        // #1467 R7: same already-listed live-CR set bounds
                        // the convergence-stall cache too.
                        prune_convergence_stall_cache(&live_uids);
                        for lumen in list.items {
                            let outcome = drive_tick(&control, &http, &lumen).await;
                            if !matches!(outcome, DriveOutcome::NoOp(_)) {
                                tracing::info!(
                                    lumen = lumen.name_any(),
                                    namespace = lumen.namespace(),
                                    ?outcome,
                                    "reshard driver tick"
                                );
                            }
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "reshard driver: list Lumen failed");
                    }
                }
            }
            tokio::time::sleep(DRIVER_POLL_INTERVAL).await;
        }
    });
}
