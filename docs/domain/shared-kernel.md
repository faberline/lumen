# Shared kernel

This context owns the wire types, committed-mutation log vocabulary, apply/capture barrier and cross-crate ports shared by the contexts. A port is a trait one crate implements and another reads, so neither crate depends on the other: `ports::cluster_status` lets `lumen-index` read the cluster state that `lumen-replication` owns.

Source: `crates/lumen-shared-kernel/src`.
Layers: shared modules.

The shared kernel has a small common vocabulary. It does not own configuration, HTTP wiring or the command line. Those belong to app and the binary modules.

[Protocol](../protocol.md) and [indexing](../indexing.md) define the related product contract.
`ddd.toml` records the current dependency rules and P1 exceptions.
[The context index](README.md) maps the rest of the service.
