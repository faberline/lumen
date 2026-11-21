#!/usr/bin/env python3
"""Choose the CI groups that a pull request needs from its changed paths.

Reads changed paths from stdin, one per line, and the reviewed map
(`.github/ci-paths.json`). Prints exactly two lines:

    tests=true|false
    image=true|false

Classes: `image` runs everything, `unit` runs the test jobs only, `docs` runs
neither. A path takes the highest class of every rule it matches (image over
unit over docs). A path that matches no rule counts as `image`. No input, the
`--all` flag, a missing map or an invalid map runs everything; the reason goes
to stderr and the exit code stays 0.
"""

import argparse
import json
import re
import sys

CLASSES = ("image", "unit", "docs")


def glob_to_regex(glob):
    """`**` matches any characters, `*` any except `/`, `?` one except `/`."""
    out = []
    i = 0
    while i < len(glob):
        if glob.startswith("**", i):
            out.append(".*")
            i += 2
        elif glob[i] == "*":
            out.append("[^/]*")
            i += 1
        elif glob[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(glob[i]))
            i += 1
    return re.compile("".join(out) + r"\Z")


def load_map(path):
    """Return {class: [compiled regex]}; raise ValueError when the map is not usable."""
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    rules = data.get("rules") if isinstance(data, dict) else None
    if not isinstance(rules, list):
        raise ValueError("the map has no rules list")
    compiled = {name: [] for name in CLASSES}
    for rule in rules:
        if not isinstance(rule, dict) or rule.get("class") not in compiled:
            raise ValueError("the map has a rule with an unknown class")
        for glob in rule.get("paths", []):
            compiled[rule["class"]].append(glob_to_regex(glob))
    return compiled


def path_class(path, compiled):
    """Return the highest class that matches `path`; an unmatched path is `image`."""
    for name in CLASSES:
        if any(regex.match(path) for regex in compiled[name]):
            return name
    return "image"


def select(paths, compiled):
    """Return (tests, image) for the highest class of all paths."""
    if not paths:
        return True, True
    classes = {path_class(path, compiled) for path in paths}
    if "image" in classes:
        return True, True
    if "unit" in classes:
        return True, False
    return False, False


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--map", required=True, help="path to ci-paths.json")
    parser.add_argument("--all", action="store_true", help="run everything")
    args = parser.parse_args()

    paths = [line.rstrip("\r\n") for line in sys.stdin if line.strip()]

    if args.all:
        tests, image = True, True
    else:
        try:
            compiled = load_map(args.map)
        except (OSError, ValueError) as error:
            print(f"select_paths: running everything: {error}", file=sys.stderr)
            tests, image = True, True
        else:
            tests, image = select(paths, compiled)

    print(f"tests={'true' if tests else 'false'}")
    print(f"image={'true' if image else 'false'}")


if __name__ == "__main__":
    main()
