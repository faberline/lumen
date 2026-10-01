# Shared kernel

This context owns the wire types, committed-mutation log vocabulary and apply/capture barrier shared by the contexts.

Source: `src/shared_kernel`.
Layers: shared modules.

The shared kernel has a small common vocabulary. It does not own configuration, HTTP wiring or the command line. Those belong to app and the binary modules.

[Protocol](../protocol.md) and [indexing](../indexing.md) define the related product contract.
`ddd.toml` records the current dependency rules and P1 exceptions.
[The context index](README.md) maps the rest of the service.
