#!/usr/bin/env python3
"""Take the durable-performance receipts out of GKE test evidence.

Each cell of .github/workflows/gke-perf.yml is one call of the aw gke-test
workflow, which uploads the artifact gke-test-evidence-<run>-<attempt>-<cell>.
Its file logs-lumen-perf-cell-<id>.txt is the client pod log, written by
`kubectl logs --all-containers --prefix --timestamps`, so every line reads
`[pod/<pod>/<container>] <RFC 3339 time> <line>`. scripts/gke-perf/cell.sh
prints the receipt between LUMEN-PERF-RECEIPT-BEGIN/END markers as base64, and
the failure evidence between LUMEN-PERF-FAILURE-BEGIN/END.

This program fails closed: a cell passes only with exactly one receipt block,
no failure block, the cell name of the call, and the byte count and sha256
that the block declares. It writes <output-dir>/<cell>.json for each passing
cell and <failures-dir>/<cell>-failure.tar.gz for each failure block, then
exits non-zero if any cell did not pass. It never reads the receipt fields;
scripts/verify-durable-perf.py does.
"""
import argparse
import base64
import binascii
import hashlib
import os
import pathlib
import re
import stat
import sys
from typing import NoReturn


CELL_RE = re.compile(r"^(index|unindex|replace)-[1-9][0-9]*-(flat|hnsw)-cpu$")
POSITIVE_RE = re.compile(r"^[1-9][0-9]*$")
LOG_RE = re.compile(r"^logs-lumen-perf-cell-[a-z0-9-]+\.txt$")
LINE_RE = re.compile(r"^\[pod/(?P<pod>[^/\]]+)/(?P<container>[^\]]+)\] (?P<time>\S+)(?: (?P<text>.*))?$")
BEGIN_RE = re.compile(r"^LUMEN-PERF-(?P<kind>RECEIPT|FAILURE)-BEGIN cell=(?P<cell>\S+) sha256=(?P<sha256>[0-9a-f]{64}) bytes=(?P<bytes>0|[1-9][0-9]*)$")
END_RE = re.compile(r"^LUMEN-PERF-(?P<kind>RECEIPT|FAILURE)-END cell=(?P<cell>\S+)$")
BASE64_RE = re.compile(r"^[A-Za-z0-9+/=]{1,76}$")
CONTAINER = "cell"


def fail(message) -> NoReturn:
    raise ValueError(message)


def real_dir(path, name):
    try:
        info = os.lstat(path)
    except OSError as error:
        fail(f"cannot inspect {name}: {error}")
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        fail(f"{name} must be a real directory")


def client_log(evidence_dir, run_id, run_attempt, cell):
    artifact = evidence_dir / f"gke-test-evidence-{run_id}-{run_attempt}-{cell}"
    real_dir(artifact, f"artifact {artifact.name}")
    logs = []
    for entry in os.scandir(artifact):
        if entry.name.endswith("-previous.txt"):
            fail(f"{artifact.name}: the client container restarted ({entry.name})")
        if LOG_RE.match(entry.name):
            if not entry.is_file(follow_symlinks=False):
                fail(f"{artifact.name}/{entry.name} is not a regular file")
            logs.append(pathlib.Path(entry.path))
    if len(logs) != 1:
        fail(f"{artifact.name}: expected one client pod log, found {sorted(p.name for p in logs)!r}")
    return logs[0]


def blocks(log, cell):
    """Return [(kind, payload)] for every marked block in the client log."""
    found = []
    current = None
    for number, raw in enumerate(log.read_text(encoding="utf-8").splitlines(), 1):
        match = LINE_RE.match(raw)
        if match is None or match["container"] != CONTAINER:
            if current is not None:
                fail(f"{log.name}:{number}: unprefixed or foreign line inside a {current['kind']} block")
            continue
        text = match["text"] or ""
        begin = BEGIN_RE.match(text)
        end = END_RE.match(text)
        if current is None:
            if end is not None:
                fail(f"{log.name}:{number}: END without BEGIN")
            if begin is None:
                if text.startswith("LUMEN-PERF-"):
                    fail(f"{log.name}:{number}: malformed marker {text!r}")
                continue
            if begin["cell"] != cell:
                fail(f"{log.name}:{number}: block for cell {begin['cell']!r}, expected {cell!r}")
            current = {"kind": begin["kind"], "sha256": begin["sha256"],
                       "bytes": int(begin["bytes"]), "lines": [], "pod": match["pod"]}
            continue
        if begin is not None:
            fail(f"{log.name}:{number}: BEGIN inside a {current['kind']} block")
        if end is not None:
            if end["kind"] != current["kind"] or end["cell"] != cell:
                fail(f"{log.name}:{number}: END {end['kind']} cell={end['cell']} does not close {current['kind']} cell={cell}")
            if match["pod"] != current["pod"]:
                fail(f"{log.name}:{number}: block spans two pods")
            try:
                payload = base64.b64decode("".join(current["lines"]), validate=True)
            except binascii.Error as error:
                fail(f"{log.name}:{number}: invalid base64: {error}")
            if len(payload) != current["bytes"]:
                fail(f"{log.name}:{number}: {current['kind']} has {len(payload)} bytes, declared {current['bytes']}")
            if hashlib.sha256(payload).hexdigest() != current["sha256"]:
                fail(f"{log.name}:{number}: {current['kind']} sha256 differs from the declared one")
            found.append((current["kind"], payload))
            current = None
            continue
        if not BASE64_RE.match(text) or match["pod"] != current["pod"]:
            fail(f"{log.name}:{number}: not a base64 line of this block")
        current["lines"].append(text)
    if current is not None:
        fail(f"{log.name}: {current['kind']} block is not closed")
    return found


def create(path, payload):
    with open(path, "xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def extract(evidence_dir, run_id, run_attempt, cells, output_dir, failures_dir):
    real_dir(evidence_dir, "evidence directory")
    real_dir(output_dir, "output directory")
    real_dir(failures_dir, "failures directory")
    failed = []
    for cell in cells:
        try:
            found = blocks(client_log(evidence_dir, run_id, run_attempt, cell), cell)
        except (OSError, UnicodeDecodeError, ValueError) as error:
            print(f"refused: {cell}: {error}", file=sys.stderr)
            failed.append(cell)
            continue
        receipts = [payload for kind, payload in found if kind == "RECEIPT"]
        failures = [payload for kind, payload in found if kind == "FAILURE"]
        if len(failures) > 1:
            print(f"refused: {cell}: {len(failures)} failure blocks", file=sys.stderr)
        elif failures:
            create(failures_dir / f"{cell}-failure.tar.gz", failures[0])
        if failures or len(receipts) != 1:
            print(f"refused: {cell}: {len(receipts)} receipt blocks and {len(failures)} failure blocks",
                  file=sys.stderr)
            failed.append(cell)
            continue
        create(output_dir / f"{cell}.json", receipts[0])
        print(f"RECEIPT {cell} sha256={hashlib.sha256(receipts[0]).hexdigest()} bytes={len(receipts[0])}")
    if failed:
        print(f"refused: {len(failed)} of {len(cells)} cells have no receipt: {' '.join(failed)}", file=sys.stderr)
        return 1
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Take the durable-performance receipts out of GKE test evidence.")
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", required=True)
    parser.add_argument("--cells", required=True, help="space- or comma-separated cell names")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--failures-dir", required=True)
    args = parser.parse_args(argv)
    try:
        for value, name in ((args.run_id, "--run-id"), (args.run_attempt, "--run-attempt")):
            if not POSITIVE_RE.match(value):
                fail(f"{name} must be a positive integer")
        cells = [cell for cell in re.split(r"[\s,]+", args.cells) if cell]
        if not cells:
            fail("--cells is empty")
        if len(set(cells)) != len(cells):
            fail("--cells repeats a cell")
        for cell in cells:
            if not CELL_RE.match(cell):
                fail(f"unknown cell {cell!r}")
        return extract(pathlib.Path(args.evidence_dir), args.run_id, args.run_attempt, cells,
                       pathlib.Path(args.output_dir), pathlib.Path(args.failures_dir))
    except (OSError, ValueError) as error:
        print(f"refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
