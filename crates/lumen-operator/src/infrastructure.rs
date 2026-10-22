//! Kubernetes adapters behind the operator's control seams, the leader-election
//! Lease, and the CustomResourceDefinitions it installs.

pub mod crd_manifest;
pub mod kube_auth_delegator_control;
pub mod kube_cluster_control;
pub mod kube_hpa_control;
pub mod lease;
