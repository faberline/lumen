//! One reconcile's observed facts about a `Lumen` (#2601), the single source
//! both status surfaces project from.

use kube::ResourceExt;
use service_k8s::{ConditionFact, ConditionStatus, ReadyFacts};

use crate::application::reconcile::shard_usage::{cache_key, shard_usage_cache};
use crate::domain::lumen_spec::status::LumenReshardStatus;
use crate::domain::lumen_spec::topology::ReshardPhase;
use crate::domain::lumen_spec::Lumen;

/// The reshard blocking conditions that mean *wedged*, as opposed to the policy
/// states [`crate::domain::lumen_spec::LumenSpec::reshard_status`] reports at plain
/// defaults (#2601).
///
/// `maxShardBytesUnset` is present on every CR that has not opted into
/// auto-splitting, and `maxShardsReached` is a configured ceiling rather than a
/// fault; gating `Ready` on the raw `blockingConditions` list would therefore
/// report every default install as permanently not-ready.
///
/// A wedge feeds `Writable` and `Degraded`. It never changes `Ready`, which
/// means only "searchable service is available".
const RESHARD_WEDGE_CONDITIONS: [&str; 2] =
    ["reshardOversizedDocument", "topologyConvergenceStalled"];

/// One reconcile's worth of observed facts about a `Lumen` (#2601).
///
/// Both status surfaces — the flat legacy fields and `status.conditions[]` —
/// project from this single computation rather than each re-deriving the
/// reshard state, so they cannot drift apart.
pub(super) struct Observation {
    pub(super) serving_ready: i32,
    pub(super) desired: i32,
    pub(super) reshard: LumenReshardStatus,
    /// The post-cutover write-pause fence is still armed (#1458 R1).
    awaiting_convergence: bool,
    pub(super) phase: &'static str,
    /// Why this reconcile could not apply the serving ServiceAccount's
    /// `system:auth-delegator` binding, if it could not (#2876). Filled from
    /// the reconcile context, never from `observe` — the apply is I/O and
    /// `observe` is not allowed to do any.
    pub(super) auth_delegation: Option<String>,
    /// Why this replicated instance has no usable Raft peer identity, if it
    /// has none (#2890). Same provenance as `auth_delegation`, and empty for
    /// every single-replica instance.
    pub(super) peer_identity: Option<String>,
    /// Whether this instance runs a replicated Raft group at all — the one
    /// piece of the peer-identity story `observe` *can* derive, since it is
    /// pure spec.
    peer_identity_required: bool,
    /// Why the spec fails `validate()`, if it does. Pure, so `observe` computes it.
    config_error: Option<String>,
    /// Why the capacity profile could not be resolved, if it could not. Same
    /// provenance as `peer_identity`: filled from the reconcile context.
    pub(super) capacity_error: Option<String>,
}

impl Observation {
    /// Is a reshard workflow actually in flight? Either a non-`Complete` phase,
    /// or the post-cutover fence still waiting to clear — the latter happens
    /// *at* phase `Complete`, which is why it is a separate disjunct.
    fn reshard_active(&self) -> bool {
        self.reshard.phase != ReshardPhase::Complete.as_str() || self.awaiting_convergence
    }

    /// The clock-free condition facts for this observation, in printed order.
    pub(super) fn conditions(&self) -> Vec<ConditionFact> {
        let replicas = format!("{}/{} serving pods ready", self.serving_ready, self.desired);
        let replicas_ready = self.serving_ready >= self.desired;
        let wedge = self
            .reshard
            .blocking_conditions
            .iter()
            .find(|c| RESHARD_WEDGE_CONDITIONS.contains(&c.as_str()));

        let ready = match (
            self.auth_delegation
                .as_deref()
                .map(|error| ("AuthDelegationNotGranted", error))
                // #2890 R4 AC2: outranks everything below for the same reason
                // the auth-delegation verdict does — a replicated group with no
                // peer identity has no authenticated way to replicate, and its
                // pods refuse to start rather than fall back to plaintext.
                // Ordered after it only because a broken TokenReview grant
                // fails every request, while this one fails replication.
                .or_else(|| {
                    self.peer_identity
                        .as_deref()
                        .map(|error| ("PeerIdentityNotConfigured", error))
                }),
            replicas_ready,
        ) {
            // #2876 AC4. This outranks everything below it: without the
            // `system:auth-delegator` binding the serving pods cannot run a
            // TokenReview, so every request fails authentication no matter how
            // many pods are Ready or how healthy the shard map is. Reporting
            // Ready here would be reporting on a data plane that answers 401
            // to its own operator.
            (Some((reason, error)), _) => {
                ConditionFact::new("Ready", ConditionStatus::False, reason, error.to_string())
            }
            (None, true) => ConditionFact::new(
                "Ready",
                ConditionStatus::True,
                "AllReplicasReady",
                replicas.clone(),
            ),
            (None, false) => ConditionFact::new(
                "Ready",
                ConditionStatus::False,
                "ReplicasNotReady",
                replicas.clone(),
            ),
        };

        // #2876 AC4: a condition of its own, not just a Ready reason. A
        // watcher that only sees `Ready=False/AuthDelegationNotGranted`
        // learns nothing once something else takes over the Ready slot; this
        // one keeps reporting the RBAC state on its own terms, and names the
        // exact operation that was refused.
        let auth_delegation = match &self.auth_delegation {
            Some(error) => ConditionFact::new(
                "AuthDelegationReady",
                ConditionStatus::False,
                "ClusterRoleBindingFailed",
                error.clone(),
            ),
            None => ConditionFact::new(
                "AuthDelegationReady",
                ConditionStatus::True,
                "AuthDelegatorBound",
                "serving ServiceAccount is bound to system:auth-delegator".to_string(),
            ),
        };

        // #2890 R4 AC2: its own condition for the same reason
        // `AuthDelegationReady` is one — a watcher that only reads `Ready`
        // learns nothing about peer identity once something else takes the
        // Ready slot, and this is the condition that names the spec field and required keys.
        let peer_identity = match &self.peer_identity {
            Some(error) => ConditionFact::new(
                "PeerIdentityReady",
                ConditionStatus::False,
                "PeerTlsSecretNotNamed",
                error.clone(),
            ),
            // True for a single-replica instance too, with a reason that says
            // why rather than implying material was found: there is no peer to
            // authenticate, so nothing is outstanding.
            None if !self.peer_identity_required => ConditionFact::new(
                "PeerIdentityReady",
                ConditionStatus::True,
                "NoReplicatedPeers",
                "single-member shard: no Raft peer transport to authenticate".to_string(),
            ),
            None => ConditionFact::new(
                "PeerIdentityReady",
                ConditionStatus::True,
                "PeerTlsSecretProjected",
                "spec.peerTlsSecret is configured; peer TLS material is required at member startup"
                    .to_string(),
            ),
        };

        // Each plane reads its own fact. `Ready` above never reads them, and
        // `Degraded` below never reads `Ready`.
        let writable = match wedge {
            Some(wedge) => ConditionFact::new(
                "Writable",
                ConditionStatus::False,
                "ReshardWedged",
                format!("{wedge}: {}", self.reshard.message),
            ),
            None if self.awaiting_convergence => ConditionFact::new(
                "Writable",
                ConditionStatus::False,
                "WritePauseFenceArmed",
                self.reshard.message.clone(),
            ),
            None => ConditionFact::new(
                "Writable",
                ConditionStatus::True,
                "WritesAccepted",
                "no reshard wedge or write-pause fence".to_string(),
            ),
        };
        let replication = match &self.peer_identity {
            Some(error) => ConditionFact::new(
                "ReplicationReady",
                ConditionStatus::False,
                "PeerIdentityNotConfigured",
                error.clone(),
            ),
            None => ConditionFact::new(
                "ReplicationReady",
                ConditionStatus::True,
                "ReplicationAuthenticated",
                "no replication identity is missing".to_string(),
            ),
        };
        let config = match &self.config_error {
            Some(error) => ConditionFact::new(
                "ConfigReady",
                ConditionStatus::False,
                "SpecInvalid",
                error.clone(),
            ),
            None => ConditionFact::new(
                "ConfigReady",
                ConditionStatus::True,
                "SpecValid",
                "spec passed validation".to_string(),
            ),
        };
        let capacity = match &self.capacity_error {
            Some(error) => ConditionFact::new(
                "CapacityReady",
                ConditionStatus::False,
                "CapacityRefused",
                error.clone(),
            ),
            None => ConditionFact::new(
                "CapacityReady",
                ConditionStatus::True,
                "CapacityResolved",
                "capacity profile resolved".to_string(),
            ),
        };
        // `Degraded` names the first plane, in this order, that is False.
        let degraded = match [&writable, &replication, &config, &capacity]
            .into_iter()
            .find(|plane| plane.status == ConditionStatus::False)
        {
            Some(first) => ConditionFact::new(
                "Degraded",
                ConditionStatus::True,
                format!("{}NotReady", first.type_),
                format!("{}: {}", first.reason, first.message),
            ),
            None => ConditionFact::new(
                "Degraded",
                ConditionStatus::False,
                "AllPlanesHealthy",
                "writable, replication, config and capacity are healthy".to_string(),
            ),
        };

        let progressing = if !replicas_ready {
            ConditionFact::new(
                "Progressing",
                ConditionStatus::True,
                "ReplicasConverging",
                replicas,
            )
        } else if self.reshard_active() {
            ConditionFact::new(
                "Progressing",
                ConditionStatus::True,
                "ReshardInFlight",
                self.reshard.message.clone(),
            )
        } else {
            ConditionFact::new(
                "Progressing",
                ConditionStatus::False,
                "Converged",
                "spec is fully reconciled".to_string(),
            )
        };

        let reshard = if self.reshard_active() {
            // Reason tracks the workflow's own vocabulary so a watcher can read
            // the phase straight off the condition; the fence-only case has no
            // phase of its own to report.
            let reason = if self.reshard.phase == ReshardPhase::Complete.as_str() {
                "AwaitingTopologyConvergence".to_string()
            } else {
                self.reshard.phase.clone()
            };
            ConditionFact::new(
                "ReshardInProgress",
                ConditionStatus::True,
                reason,
                self.reshard.message.clone(),
            )
        } else {
            ConditionFact::new(
                "ReshardInProgress",
                ConditionStatus::False,
                "Complete",
                self.reshard.message.clone(),
            )
        };

        vec![
            ready,
            progressing,
            reshard,
            auth_delegation,
            peer_identity,
            writable,
            replication,
            config,
            capacity,
            degraded,
        ]
    }
}

impl Lumen {
    /// Compute this reconcile's [`Observation`] — the single source both status
    /// surfaces project from (#2601). Synchronous and I/O-free, per the module
    /// doc's contract; the live usage read is a cache lookup, not a scrape.
    pub(super) fn observe(&self, ready: &ReadyFacts) -> Observation {
        let name = self.name_any();
        let serving_ready = ready.ready.get(&name).copied().unwrap_or(0) as i32;
        let desired = self.spec.storage_pod_count();
        let usage = shard_usage_cache()
            .lock()
            .ok()
            .and_then(|cache| cache.get(&cache_key(self)).cloned());
        let mut reshard = match usage {
            Some(snapshot) if !snapshot.usage.is_empty() => self
                .spec
                .reshard_status_with_usage(&snapshot.usage, snapshot.measured_at_map_version),
            _ => self.spec.reshard_status(),
        };
        // #1444 R2: an oversized single-document batch the reshard driver
        // cannot apply is a distinct, named blocking condition (not the
        // generic threshold/policy conditions `reshard_status*` already
        // computes above) with its own remediation text — layered on here
        // rather than inside `LumenSpec::reshard_status*` because it comes
        // from the driver's own live apply attempts, not from spec/usage.
        let namespace = self.namespace().unwrap_or_else(|| "default".to_string());
        let uid = self.uid().unwrap_or_default();
        if let Some(block) = crate::application::reshard_driver::oversize::oversize_block_condition(
            &namespace, &name, &uid,
        ) {
            reshard
                .blocking_conditions
                .push("reshardOversizedDocument".to_string());
            reshard.message = block.to_string();
        }
        // #1458 R1: post-cutover topology-convergence pending — derived
        // purely from persisted spec state (`shardMap.version` vs
        // `workflow.convergedShardMapVersion`), the same check
        // `reshard_driver::advance_convergence` runs each tick, so this
        // needs no driver-side cache read. Only sets the message if a more
        // severe oversize wedge did not already claim it above.
        //
        // #1467 R7: also require `workflow.lastCutoverShardMapVersion ==
        // shardMap.version` — the same gate `advance_convergence` itself
        // uses to decide whether to engage the fence loop at all. Without
        // this, a manually-authored/edited `shardMap.version` (one the
        // driver never cut over to, so it never arms a fence or advances
        // convergence) would report `awaitingTopologyConvergence` forever,
        // even though nothing is actually blocking writes.
        let map_version = self.spec.shard_map.version;
        let workflow = &self.spec.reshard_policy.workflow;
        let awaiting_convergence = map_version > 0
            && workflow.converged_shard_map_version != Some(map_version)
            && workflow.last_cutover_shard_map_version == Some(map_version);
        if awaiting_convergence {
            reshard
                .blocking_conditions
                .push("awaitingTopologyConvergence".to_string());
            if !reshard
                .blocking_conditions
                .contains(&"reshardOversizedDocument".to_string())
            {
                reshard.message = format!(
                    "waiting for every serving pod to become Ready on shardMap version \
                     {map_version} before the post-cutover write-pause fence is cleared"
                );
            }
        }
        // #1467 R7: distinct, named condition once the wait above has
        // exceeded the stall budget — the driver keeps re-arming the fence
        // (never silently drops it), but operators need a visible signal
        // that convergence has been pending unusually long, not just that
        // it's pending.
        //
        // #1485 R2: computed purely from the persisted `workflow.
        // convergenceWaitStartedAt` checkpoint (the same field `advance_
        // convergence` itself stamps and reads), not the driver's
        // process-local stall-tracking cache — so this condition, and the
        // budget it is derived from, survive an operator restart mid-wait
        // rather than resetting to "not stalled" until the cache re-fills.
        if awaiting_convergence
            && crate::application::reshard_driver::convergence_stall::convergence_stall_condition(
                workflow.convergence_wait_started_at,
            )
        {
            reshard
                .blocking_conditions
                .push("topologyConvergenceStalled".to_string());
            // #1485 R1: surface the bounded remediation restart's own
            // re-trigger count/timestamp alongside the condition, so
            // operators can see the self-heal fired without reading driver
            // logs.
            reshard.convergence_remediation_restart_count =
                workflow.convergence_remediation_restart_count;
            reshard.convergence_remediation_restarted_at =
                workflow.convergence_remediation_restarted_at;
            if !reshard
                .blocking_conditions
                .contains(&"reshardOversizedDocument".to_string())
            {
                reshard.message = format!(
                    "topology convergence on shardMap version {map_version} has not been \
                     confirmed after an extended wait; the write-pause fence remains armed \
                     and is being kept re-armed"
                );
            }
        }
        let phase = if serving_ready >= desired {
            "Ready"
        } else if serving_ready > 0 {
            "Reconciling"
        } else {
            "Pending"
        };
        Observation {
            serving_ready,
            desired,
            reshard,
            awaiting_convergence,
            phase,
            // Not knowable here: applying the binding is I/O, and this
            // function is synchronous and I/O-free by the module's contract.
            // `conditions` fills it from the reconcile context (#2876).
            auth_delegation: None,
            // Same for the Secret read behind this one (#2890) — but whether
            // the instance owes peer identity at all is pure spec, so that half
            // is derivable here.
            peer_identity: None,
            peer_identity_required: self.spec.peer_identity_required(),
            // Pure: the same `validate()` the reconcile runs before any apply.
            config_error: self.spec.validate().err(),
            // Filled from the reconcile context, like the two verdicts above.
            capacity_error: None,
        }
    }
}
