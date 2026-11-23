#!/usr/bin/env python3
"""List the files below 100 percent line coverage for the `coverage-list` check run.

Reads the `cargo llvm-cov --json --summary-only` file of a run and the
`[standards.coverage]` excludes of `aw.toml`, and writes the JSON body of one
check run (`POST repos/<repo>/check-runs`) to `--payload`. The body names the
commit, the line coverage, the number of files below 100 percent, and a table
of the least covered files with their line percent and uncovered-line count.

The numbers follow `faberline/aw` `.github/actions/coverage/check.sh`: the same
excludes, the same root cut from file names and the same two-decimal floor.
With `--report` (the `coverage-report.json` of that action) the totals must
equal the report; a difference prints one line per value to stderr, exits 1
and writes no payload.
"""

import argparse
import json
import math
import os
import re
import sys
import tomllib

NAME = "coverage-list"
DEFAULT_LIMIT = 20
MIN_LIMIT = 20
TEXT_BUDGET = 60000  # GitHub accepts 65535 characters in a check run output.


def floor2(value):
    """Cut a number down to two decimals, as the jq `floor2` of check.sh does."""
    return math.floor(value * 100) / 100


def load_excludes(config_path):
    """Return the `path` regexes of `[[standards.coverage.exclude]]`, or []."""
    if not os.path.exists(config_path):
        return []
    with open(config_path, "rb") as f:
        data = tomllib.load(f)
    section = data.get("standards", {}).get("coverage", {})
    return [entry["path"] for entry in section.get("exclude", []) if entry.get("path")]


def load_files(summary, root, excludes):
    """Return the files that are not excluded, with their line counts."""
    prefix = root.rstrip("/") + "/"
    files = []
    for entry in summary["data"][0]["files"]:
        name = entry["filename"]
        if name.startswith(prefix):
            name = name[len(prefix):]
        if any(re.search(pattern, name) for pattern in excludes):
            continue
        lines = entry["summary"]["lines"]
        count = lines["count"]
        covered = lines["covered"]
        files.append(
            {
                "file": name,
                "count": count,
                "covered": covered,
                "percent": floor2(lines["percent"]),
                "uncovered": count - covered,
            }
        )
    return files


def totals(files):
    """Return the measured percent and the sums that the report also holds."""
    lines = sum(f["count"] for f in files)
    covered = sum(f["covered"] for f in files)
    measured = 100 if lines == 0 else floor2(covered / lines * 100)
    return {"measured": measured, "covered": covered, "lines": lines, "files": len(files)}


def check_report(files, report):
    """Return one message per value where the report differs from this list."""
    ours = totals(files)
    problems = []
    for key in ("measured", "covered", "lines", "files"):
        if report.get(key) != ours[key]:
            problems.append(f"{key}: report {report.get(key)}, list {ours[key]}")
    return problems


def sorted_below(files):
    """Files with uncovered lines: least covered first, then most uncovered, then name."""
    below = [f for f in files if f["uncovered"] > 0]
    return sorted(below, key=lambda f: (f["percent"], -f["uncovered"], f["file"]))


def render(files, total, sha, shown):
    """Return the summary and the table text for the first `shown` files."""
    below = sorted_below(files)
    n = len(below)
    summary_lines = [
        f"commit: {sha}",
        f"line coverage: {total['measured']}% "
        f"({total['covered']} of {total['lines']} lines in {total['files']} files)",
        f"files below 100 percent: {n}",
        f"listed: {shown} least covered files",
    ]
    if shown < n:
        summary_lines.append(f"not listed: {n - shown} more files")
    table = ["| File | Line % | Uncovered lines |", "|---|---:|---:|"]
    for f in below[:shown]:
        table.append(f"| `{f['file']}` | {f['percent']} | {f['uncovered']} |")
    return "\n".join(summary_lines), "\n".join(table)


def build_payload(files, sha, limit):
    """Return the check run body. The table keeps the `limit` least covered files."""
    total = totals(files)
    n = len(sorted_below(files))
    shown = min(n, limit)
    floor_shown = min(n, MIN_LIMIT)
    summary, text = render(files, total, sha, shown)
    while len(text) > TEXT_BUDGET and shown > floor_shown:
        shown -= 1
        summary, text = render(files, total, sha, shown)
    title = f"Coverage {total['measured']}% ({n} files below 100%) at {sha[:12]}"
    return {
        "name": NAME,
        "head_sha": sha,
        "status": "completed",
        "conclusion": "neutral",
        "output": {"title": title, "summary": summary, "text": text},
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--summary", required=True, help="cargo llvm-cov --json --summary-only file")
    parser.add_argument("--root", default=".", help="directory cut from file names (default: cwd)")
    parser.add_argument("--sha", required=True, help="full commit SHA of the run")
    parser.add_argument("--config", default="aw.toml", help="aw.toml with [standards.coverage]")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="files to list (default 20)")
    parser.add_argument("--payload", required=True, help="output file for the check run body")
    parser.add_argument("--report", help="coverage-report.json of the aw coverage action")
    args = parser.parse_args(argv)
    if args.limit < 0:
        parser.error("--limit must not be negative")

    root = os.path.abspath(args.root)
    with open(args.summary, encoding="utf-8") as f:
        summary = json.load(f)
    files = load_files(summary, root, load_excludes(args.config))

    if args.report:
        with open(args.report, encoding="utf-8") as f:
            report = json.load(f)
        problems = check_report(files, report)
        if problems:
            for problem in problems:
                print(problem, file=sys.stderr)
            return 1

    payload = build_payload(files, args.sha, args.limit)
    with open(args.payload, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
