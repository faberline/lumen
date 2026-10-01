# Operator

This context owns the Lumen and LumenFleet declarations, desired-state plans, Kubernetes resource rendering, reconciliation and status.

Source: `src/operator`.
Layers: domain, application and infrastructure.

The operator consumes externally provisioned TLS Secrets. It uses service-k8s for shared Kubernetes mechanisms. Feature-gated runtime work stays behind operator.

[GKE](../gke.md) and the [operator runbook](../runbooks/operator-control-plane.md) define the related product contract.
`ddd.toml` records the current dependency rules and P1 exceptions.
[The context index](README.md) maps the rest of the service.
