//! The leader-gated background loops for the HPA handoff and the auth-delegator
//! sweep, each on its own Lease.

use std::time::Duration;

use kube::Client;

use crate::operator::application::reconcile::auth_delegator::sweep_stale_auth_delegator_bindings;
use crate::operator::application::reconcile::hpa::prune_stale_hpa;
use crate::operator::domain::lumen_spec::Lumen;
use crate::operator::infrastructure::kube_auth_delegator_control::KubeAuthDelegatorControl;
use crate::operator::infrastructure::kube_hpa_control::KubeHpaControl;

/// Poll interval for [`spawn_hpa_handoff_loop`] (#1385). Faster than
/// [`SHARD_USAGE_POLL_INTERVAL`] since a lingering stale HPA actively starves
/// the reshard driver's `PrepareSplit` gate — the sooner it's pruned, the
/// sooner that gate can converge.
///
/// [`SHARD_USAGE_POLL_INTERVAL`]: crate::operator::application::reconcile::shard_usage::SHARD_USAGE_POLL_INTERVAL
const HPA_HANDOFF_POLL_INTERVAL: Duration = Duration::from_secs(15);

/// Leader-election Lease name for [`spawn_hpa_handoff_loop`] (#1385) —
/// distinct from both `libs/service-k8s`'s own `S::MANAGER`-named apply-loop
/// Lease and `reshard_driver::DRIVER_LEASE_NAME`, so none of the three
/// independently leader-gated loops contend on one Lease object (mirrors the
/// same duplicated `identity`/`lease_namespace` resolution
/// `reshard_driver::spawn_reshard_driver_loop` already uses for the same
/// reason).
const HPA_HANDOFF_LEASE_NAME: &str = "lumen-hpa-handoff";

/// Poll interval for [`spawn_auth_delegator_sweep_loop`] (#2876). A leftover
/// binding is a standing grant of delegated authentication review to a
/// ServiceAccount whose instance no longer exists, so it is swept on the same
/// brisk cadence as the HPA handoff rather than the slower usage scrape.
const AUTH_DELEGATOR_SWEEP_POLL_INTERVAL: Duration = Duration::from_secs(15);

/// Leader-election Lease name for [`spawn_auth_delegator_sweep_loop`] (#2876),
/// distinct from every other independently leader-gated loop's Lease for the
/// same reason [`HPA_HANDOFF_LEASE_NAME`] is.
const AUTH_DELEGATOR_SWEEP_LEASE_NAME: &str = "lumen-auth-delegator-sweep";

/// Background loop (#1385): every [`HPA_HANDOFF_POLL_INTERVAL`], while
/// holding the [`HPA_HANDOFF_LEASE_NAME`] Lease, list every `Lumen` CR
/// cluster-wide and run [`prune_stale_hpa`] against each. Independently
/// leader-gated (like [`crate::operator::reshard_driver::
/// spawn_reshard_driver_loop`]) since deletion is a cluster write, unlike
/// [`spawn_shard_usage_loop`]'s read-only cache population.
///
/// [`spawn_shard_usage_loop`]: crate::operator::application::reconcile::shard_usage::spawn_shard_usage_loop
pub(super) fn spawn_hpa_handoff_loop(client: Client) {
    // Mirrors `libs/service-k8s::controller`'s own `identity`/`lease_namespace`
    // helpers (private to that crate, so duplicated here, same as
    // `reshard_driver::spawn_reshard_driver_loop` already does) so every
    // independently-leader-gated loop resolves the same pod identity and
    // Lease namespace from the same env vars.
    let identity = std::env::var("POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| HPA_HANDOFF_LEASE_NAME.to_string());
    let namespace =
        std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "lumen-operator-system".to_string());
    let election = crate::operator::infrastructure::lease::Election::new(identity);
    crate::operator::infrastructure::lease::spawn(
        client.clone(),
        namespace,
        HPA_HANDOFF_LEASE_NAME.to_string(),
        election.clone(),
    );
    let control = KubeHpaControl {
        client: client.clone(),
    };
    tokio::spawn(async move {
        let api: kube::Api<Lumen> = kube::Api::all(client);
        loop {
            if election
                .is_leader
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                match api.list(&Default::default()).await {
                    Ok(list) => {
                        for lumen in list.items {
                            prune_stale_hpa(&control, &lumen).await;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "HPA handoff: list Lumen failed");
                    }
                }
            }
            tokio::time::sleep(HPA_HANDOFF_POLL_INTERVAL).await;
        }
    });
}

/// Background loop (#2876): every [`AUTH_DELEGATOR_SWEEP_POLL_INTERVAL`],
/// while holding the [`AUTH_DELEGATOR_SWEEP_LEASE_NAME`] Lease, list every
/// `Lumen` cluster-wide and hand the list to
/// [`sweep_stale_auth_delegator_bindings`]. Independently leader-gated for the
/// same reason [`spawn_hpa_handoff_loop`] is: it performs cluster writes.
pub(super) fn spawn_auth_delegator_sweep_loop(client: Client) {
    let identity = std::env::var("POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| AUTH_DELEGATOR_SWEEP_LEASE_NAME.to_string());
    let namespace =
        std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "lumen-operator-system".to_string());
    let election = crate::operator::infrastructure::lease::Election::new(identity);
    crate::operator::infrastructure::lease::spawn(
        client.clone(),
        namespace,
        AUTH_DELEGATOR_SWEEP_LEASE_NAME.to_string(),
        election.clone(),
    );
    let control = KubeAuthDelegatorControl {
        client: client.clone(),
    };
    tokio::spawn(async move {
        let api: kube::Api<Lumen> = kube::Api::all(client);
        loop {
            if election
                .is_leader
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                match api.list(&Default::default()).await {
                    Ok(list) => {
                        sweep_stale_auth_delegator_bindings(&control, &list.items).await;
                    }
                    Err(err) => {
                        // Deliberately no sweep on a failed list: an empty
                        // `live` set would read as "no instance wants any
                        // binding" and delete every one of them.
                        tracing::warn!(error = %err, "auth delegation sweep: list Lumen failed");
                    }
                }
            }
            tokio::time::sleep(AUTH_DELEGATOR_SWEEP_POLL_INTERVAL).await;
        }
    });
}
