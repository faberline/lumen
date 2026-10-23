# Storage

This context owns the data lumen keeps on disk: the columnar segment file format, its writers, its zero-copy reader, and the composed reader that reads one base segment through its ordered sparse deltas.

Source: `crates/lumen-storage/src`.
Layers: infrastructure.

The crate depends on no other lumen crate. The text segment writer reads each token's posting list through the `TokenPostings` trait, which the index's `Postings` implements. [Index](index.md) seals and reopens each field's segments through this crate. [Persistence](persistence.md) publishes them as checkpoint generations.

[Indexing](../indexing.md) defines the related product contract.
`ddd.toml` records the current dependency rules and P1 exceptions.
[The context index](README.md) maps the rest of the service.
