# Lumen development pipeline content repairs

PR #189 keeps #188 as its sole closing issue. Issues #192–#204 record the
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

## Complete original Rig workloads (review R1 / #197)

Independent Codex review rejected head `16350a3` after its 32 outer jobs passed.
The old rig-load directory also required search_qps and data_table_browse.
Those two cases had no actual receipt. The repaired owner executes all three
load cases. A separate original-contract inventory and native driver require
all eight original required Rig scenarios, complete steps/captures and measured
load profiles. A green outer test cannot replace this nested execution proof.

Keep search at 200 QPS, eight workers, 30 seconds with five seconds warmup,
p99 <=25 ms, error_rate <=0.01 and ratchet0.8. Delegate floors and ratchets to
the pinned Rig gate. Read existing repository-root .rig baselines by scenario,
metric and OS/architecture. Never create a baseline for a pass. An absent
baseline remains explicit NoBaseline/Info; original RIG_STRICT=1 makes it High.
NoBaseline is not recorded as an evaluated or successful ratchet.

Keep browse's 1000 documents, both original fields, page size50, full HTTP
cursor equality and deep <=max(shallow*20,50ms). All2000 original field items
retain their IDs, values and order in legal batches. Use the scenario's owned
service. The one-shot oracle must also contain every original document.
Pure mocked-transport controls retain the original corpus/query/page work and
refuse cursor corruption, seed failure and either breached latency term.
The new final head still needs all current CI and independent review.

## Shared capacity publication (#203)

Run 41339468620 at `92d5e72d8ed2ad09bfde877f9694064da05f481d`
failed the original default-profile HTTP capacity contract. Both writes and
WAL/apply counts succeeded, but completed checkpoints were four instead of
the required two. The original HTTP test, deadlines, 256 MiB limit, exact
counters, IDs and cold-reopen assertions remain unchanged.

The periodic driver and native owner now share completed request progress
within one publication owner. A pending request can reuse a successful
publication instead of saving it twice. Coalescing happens before save,
attempt counters and diagnostic phases. It does not fabricate a checkpoint.
The relay still owns merge and request consumption. Failed publication cannot
mark relief complete. Fresh demand gets a successor revision. Owner replacement
gets separate progress and retains its publication fence.

The admission lookup uses a separate weak progress reference. It does not
lock endpoint requests or owner registration. Existing replacement-owner and
fallback tests retain their assertions. New regressions cover real held file
IO, duplicate publication, new work, stale consumption, failure/retry, owner
replacement and honest counters/diagnostics. The real duplicate-publication
regression failed before the fix. At `7492a10`, all 230 affected unit executions
passed across default and no-default profiles with zero ignored tests. Fresh complete hosted
CI and independent Codex review remain required for the final head.

The first combined repair run 41353187079 failed the new failed-publication
regression under parallel no-default unit execution. The old new-test fixture
used process-shared accounting. Another owner legitimately advanced its global
request revision from 115175 to 115177. Only the new coalescing fixture now owns
an independent real ChangeBudget at the same default 256 MiB limit. All four
regression bodies and exact assertions are byte-identical. Production accounting,
original shared-budget tests and normal CI parallelism remain unchanged. Debug
ignored cases still require the original complete release execution in CI.

Run 41356621438 at `ddebf98` still failed the original `feature:release`
HTTP case: completed checkpoints were three instead of two. Other successful
profiles did not qualify that failed run. An opt-in diagnostic copy, kept
outside the committed suite, captured the remaining path: the periodic driver
selected `successor` for the old high active prefix after its same capacity
request had already received a real publication. That diagnostic run happened
to read the counter before the extra save completed. Its passing assertion
was not proof that the race was fixed.

A deterministic scheduler regression uses the actual observed owner states.
The old implementation returns `Successor` when the served episode requires
no second publication. The repair records the first owner work boundary of a
capacity demand under the same accounting lock as its request revision. The
request clock can run ahead of owner work, so the two clocks are not guessed
from each other. Successful publication carries that exact boundary to the
scheduler. Only the served old prefix loses the duplicate successor.

Fresh work after the request, a newer real refusal without new work, ordinary
deadline saves, a drained owner's next crossing, unknown boundaries, failed
publication, native fallback and replacement fences keep their progress rules.
New positive and negative controls cover those cases. All original test
bodies remain unchanged. The new source
still requires complete current-head CI and independent review.

Controlled local schedules at parent `2dca1b8` found two further cases, without
claiming a hosted failure. A periodic task resumed after both waiting writes
had committed and mistook them for work during its completed IO. A controlled
six-seed capture also left a smaller active prefix; the same resumed writes
crossed the trigger and wrongly earned the drained owner's next successor.
Both copies retained the original exact HTTP counter and failed three versus
two. Their raw negative evidence is kept outside the committed suite.

Publication now records its owner snapshot during acknowledgement while the
capture barrier still excludes later apply. Request progress remains locked
across that accounting acknowledgement, so newly woken refusals cannot be
credited to an earlier release. Cleanup and async resumption use this recorded
snapshot. A failed publication supplies no completed owner snapshot.

Local admissions carry their original capacity episode through the existing
reservation. All bytes, charges and the ordinary work clock remain unchanged.
Only pressure scheduling excludes work resumed by an already-served episode.
Independent work stays eligible even when interleaved with that cohort. New
unserved episodes stay eligible. Served episode metadata is pruned, and late
members do not accumulate entries. Repricing, transport splitting and source
retention preserve the apply origin. No reservation or budget is bypassed.

Five small accounting controls and two real snapshot controls supplement the
earlier regressions. Two permanent HTTP interleaving cases call the original
two-writer body with all its assertions. The original HTTP file gains only one
module declaration; every original byte remains after removing that addition.
The integration target now has eight required cases, including the original
six. Both schedules delay observation after the writes, never relax admission
deadlines, and require their control hooks to run. Their added cases must run
under every original integration profile. All complete new-head CI and the
independent review still remain required.

## Committed boundary in manual checkpoints (#204)

A separate local no-default run at `7492a10` failed the unchanged
`http_checkpoint_releases_pending_capacity_relay` cold-reopen assertion. The
HTTP checkpoint returned OK and the committed record became visible live, but
CURRENT omitted `capacity-large-010`. This local failure is retained separately
from hosted CI. A default-profile subset did not qualify the source.

On-demand checkpoints now capture one fixed committed WAL boundary from the
existing coordinator. The initial save still runs before waiting for apply,
because blocked apply may need that save to release capacity. When CURRENT
does not yet cover the fixed boundary, the request waits for apply and performs
at most one follow-up save. It verifies the actual CURRENT sequence before
success. New traffic does not move the target. Waits hold no save, apply or
restore permit. The wait reuses the existing submit timeout and propagates
failure. Other write sinks retain their explicit applied-state default.

A real WAL delivery is held across the first actual publication in a new small
regression. The old path returns premature success. The repaired path waits,
then reopens the durable generation and finds the exact committed data. A later
committed delivery stays blocked to prove the fixed target cannot drift. Other
new controls refuse an unavailable boundary and a failed apply. The original
six-case HTTP target, cold-reopen IDs, capacity and deadlines are unchanged.
All 320 focused unit executions pass across default and no-default settings,
including the original checkpoint, capacity, budget, schedule and coordinator
tests. The final source still requires complete hosted CI and independent review.

## Large AOF startup capacity progress (#205)

Head `e2d7f47` failed the original complete large-codec replay case in the
default and backup integration jobs of run `41373917194`. Both children did
not reach readiness within the unchanged 30-second startup bound. The same
unmodified case failed locally. Keep its three authentic 60 MiB values, fast
WAL/CBOR/legacy JSON codecs, CRC frames, exact live and cold values, CURRENT/AOF
retention, 64 MiB frame maximum and 256 MiB pending limit unchanged.

An opt-in trace shows that the first checkpoint captured only an older small
cut. About 192 MiB (201 MB) of later active work remained when the capacity request was
consumed. The blocked reservation still could not fit, but its condition wait
did not request another publication. The normal 300-second snapshot period
could not satisfy the startup deadline. A small real-accounting regression
reproduces this missing request before the fix without leaking its waiter.

Blocking reservation and growth now request existing checkpointable work only
after their actual capacity predicate still fails. Consuming an actual request
wakes those predicates. A new monotonic revision is issued when necessary;
spurious wakes share the pending revision. Admitted work, oversized inputs,
uncommitted reservations alone, raw work clocks, byte charges, owner retirement
and stale-consumption fences keep their existing rules. Another owner's work
can relieve a global wait without assigning work to an empty owner. Five small
positive/negative accounting controls supplement all original assertions.

The original large-codec test then completed locally with all live and cold
assertions intact. This focused local pass grants no hosted qualification.
The final commit still needs the complete original profiles and current-head
CI, independent Codex review and actual protected operational acceptance.

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
