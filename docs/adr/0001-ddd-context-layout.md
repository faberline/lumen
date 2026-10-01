# 0001: Organize Lumen by context within one crate

Status: accepted for the P1 source-layout migration.

## Context

Lumen is a standalone Cargo package.
Large source modules combine separate areas of responsibility.
Callers still use the public module paths that existed before the split.

## Decision

Keep one crate and organize the source by context.
Use the contexts and layers declared in `ddd.toml`.
Keep `src/app` as the composition root.
Keep `src/compat` as the bridge for existing public module paths.
Keep command-line modules beside each binary's `main.rs`.
Use `name.rs` with a `name/` directory for child modules.

P1 moves and splits files.
It preserves function bodies, feature conditions and public behavior.
New files aim for 400 lines or fewer.
A long function can remain whole when extraction would exceed P1's scope.
The architecture checker reports those files for later work.

Keep the two save-gate instances and their two declared path exceptions.
Record existing layer violations and dependency cycles in `ddd.toml`.
Keep process state in its owner's infrastructure layer.
Do not split the service into separate crates during P1.

P2 requires a separate decision for each change.
It can extract long functions, define ports, remove dependency cycles,
unify save gates and retire compatibility paths after callers migrate.

## Consequences

The directory names show who owns each module.
Existing callers keep their public paths.
File moves require source-reading tests and relative includes to move with them.
The checked-in OpenAPI and CRD output must stay byte-identical.
Existing architecture exceptions remain visible to the checker.

## References

- [Architecture](../architecture.md)
- [Contexts](../domain/README.md)
- [Contributor workflow](../../CONTRIBUTING.md)
- [Architecture configuration](../../ddd.toml)
