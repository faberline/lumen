# Compatibility

## Promise

From `1.0.0` the project follows semantic versioning. No breaking change ships before `2.0.0`. Additive changes may ship in a minor release.

## Versioned surfaces

| Surface | Contract source | Check |
|---|---|---|
| HTTP API | `clients/openapi.json` (`lumen spec --format openapi`) | test `openapi_compat` |
| Native wire format | `docs/protocol.md` | not yet checked |
| On-disk segment format | `docs/indexing.md` | not yet checked |
| CRD | `crates/lumen-operator` | not yet checked |

## What is a breaking change

- Removing or renaming an operation (method, path or `operationId`).
- Removing a documented status code.
- Removing a request or response field.
- Changing a field type.

The permanent `POST` twins of the `QUERY` operations stay (`docs/protocol.md`).

## What is not promised

- Published npm, PyPI or crates.io packages.
- Behavior listed as `Not supported` or `Limited` in `STATUS.md`.
- Anything before `1.0.0`.

## How the HTTP surface is checked

The test file is `crates/lumen/tests/it/openapi_compat.rs`. It compares the live `lumen spec --format openapi` output with the baseline file `crates/lumen/tests/openapi-operations-baseline.txt`.

An intended breaking change needs a major version and a change of the baseline in the same pull request.
