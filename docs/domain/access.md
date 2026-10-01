# Access

This context owns caller identity, collection and instance permissions, token handling, and TLS material.

Source: `src/access`.
Layers: domain, application, infrastructure and interfaces.

Kubernetes TokenReview identifies a ServiceAccount. SubjectAccessReview checks its permission. Lumen owns the mapping from its operations to Kubernetes resource attributes. Shared libraries own the review transport and TLS loading mechanisms.

[Authentication](../authentication.md) defines the related product contract.
`ddd.toml` records the current dependency rules and P1 exceptions.
[The context index](README.md) maps the rest of the service.
