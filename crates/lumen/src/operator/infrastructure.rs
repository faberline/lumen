//! Kubernetes adapters behind the operator's control seams, the leader-election
//! Lease, and the CustomResourceDefinitions it installs.

pub(crate) mod crd_manifest;
pub(crate) mod kube_auth_delegator_control;
pub(crate) mod kube_cluster_control;
pub(crate) mod kube_hpa_control;
pub mod lease;
