//! What the operator does with a `Lumen`: render its child objects, reconcile
//! them onto the cluster, drive reshards, materialize fleets, and issue its
//! certificates.

pub mod capacity_catalog;
pub mod certificate;
pub mod fleet_reconcile;
pub mod reconcile;
pub mod render;
pub mod reshard_driver;
pub mod resize;

#[cfg(test)]
mod lease_tests {
    use std::collections::BTreeSet;

    #[test]
    fn every_independently_leader_gated_loop_has_its_own_lease_name() {
        let names = [
            crate::application::fleet_reconcile::FLEET_LEASE_NAME,
            crate::application::reconcile::loops::HPA_HANDOFF_LEASE_NAME,
            crate::application::reconcile::loops::AUTH_DELEGATOR_SWEEP_LEASE_NAME,
            crate::application::reshard_driver::DRIVER_LEASE_NAME,
        ];
        let distinct: BTreeSet<&str> = names.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            names.len(),
            "lease names collide: {names:?}"
        );
        assert_eq!(
            names,
            [
                "lumen-fleet",
                "lumen-hpa-handoff",
                "lumen-auth-delegator-sweep",
                "lumen-reshard-driver",
            ]
        );
    }
}
