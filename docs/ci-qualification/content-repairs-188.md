# Lumen development pipeline content repairs

PR #189 keeps #188 as its sole closing issue. Issues #192–#201 record the
bounded content repairs. A successful subset does not qualify the PR.

## Source contracts (#198)

The source oracle now reads complete active Rust tokens. Literal contents,
numbers, punctuation and order remain exact. Comments and strings cannot
supply required code. The existing negative mutations remain required.
The complete durable oracle is compared with the original implementation,
rendered by Rustfmt 1.99. This changes the reference representation only.

The original GKE workflow already used immutable N4 runtime
`6eb7ebcdf38343e0d4c77f8674b6bc30976d86bb`. The stale oracle required `main`.
The oracle now requires the fixed workflow and matching `aw-ref`. Its full
byte pin remains enforced. The GKE workflow and product Kustomize did not change.

## Generated clients (#199, #200)

The existing Python, TypeScript and Rust CRUD assertions must all run.
Missing tools or failed commands fail the test. Python uses the generated
manifest's interpreter and Pydantic dependency through the existing `uv`
runner. The original untargeted client bytes remain the code under test.
The existing explicit Python 3.11, TypeScript 5.0 and Rust 2021 targets emit
separate `.toolchain` references through the public manifest writer. These
references provide tool requirements and do not replace any original client
file. Receipts identify this legacy execution mode and the reference source.
CI installs TypeScript 5.9.3 from the official npm artifact and checks
its fixed SHA-512 integrity. Installation is local to the hosted runner.

Each completed language writes one fresh receipt after all CRUD assertions.
The integration driver requires all three distinct language receipts and
binds their bytes to the current target, profile, head, base and run evidence.
Missing, duplicate, failed or incomplete proof fails qualification.

CLI output is captured before checking its status. Child output cannot enter
a libtest progress row. The strict complete-roster parser is unchanged.
Isolated Rust harness controls reproduce and reject the previous corruption.
They also require a failing child to retain both diagnostic streams.

## Scale proof setup (#201)

The pending-change limit remains 256 MiB. Setup keeps all original per-document
writes. It persists and syncs an actual snapshot after bounded batches, then
restores that snapshot into full in-memory indexes. This uses the public restore
lifecycle to publish pending changes. It does not bypass admission or pre-seal
the RAM reference into mmap segments.

Temporary snapshot state is dropped before measurement. Unused allocator pages
are released before both the full-RAM reference and the post-drop baseline.
This prevents setup allocations from padding the denominator or hiding growth.
The original 50,000/200,000 documents, 96/192 dimensions, query assertions,
65% bounds, page-fault checks and measurement formulas remain required.
The full measurements run only in hosted CI.

## Incomplete full-graph answer (#202)

Run 41317557070 at `e5544c6f2c9d987dc3ed10d0c1ddb57a14a03833`
failed the existing cache-rejection unit in the no-default profile. A rebuilt
graph omitted `id-13` from the original 32-row/query/top-ten result. The search
requested every allocated graph ID, but accepted ten other hits before checking
that the full graph inventory was present.

An incomplete full-graph request now uses the existing authoritative live-store
fallback. Normal bounded and complete graph answers keep their existing path.
A deterministic in-memory regression omits that same graph node while retaining
all 32 live rows. It fails with the original exact ID/score difference before
the fix, then requires the complete top ten, exact score bits, one bounded
traversal and a real fallback. The original cache rejection, metadata mutations
and strict recovery assertions remain unchanged. This failed historical run
grants no final-head acceptance.

## Acceptance

Keep all 31 unit/static cases, four unit batches, their original shared
30-minute per-case budgets, 13 integration profiles, eight prior cloud jobs
and both native contract jobs. The full 32-job run must pass at the current
head and base. Partial results and source-fixture controls do not replace
actual product execution. The three original source gaps have proposed
decisions and source-bound evidence in `content-decisions-188.json`. That
record preserves the failed historical run and every required final runtime
check. It grants no acceptance.
Independent Codex review, protected cutover and actual merge/issue closure
remain separate required steps. No Claude review or cloud merge job is added.
