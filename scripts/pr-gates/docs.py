#!/usr/bin/env python3
"""Offline milestone contract checks. Document text is never executed."""
import argparse
import hashlib
import json
from pathlib import Path
import re

LINK = re.compile(r'\[Milestone #(\d+)\]\(([^)]+)\)')
HEADING = re.compile(r'^#{1,6} (.+)$')
SUFFIX = re.compile(r'\s*\(Milestone #(\d+)\)\s*$')


def fixture(raw, expected_sha256):
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError('validator policy changed: fixture SHA-256 mismatch')
    policy = json.loads(raw)
    if policy.get('version') != 1 or policy.get('repository') != 'faberline/lumen':
        raise ValueError('unknown milestone contract')
    milestones = policy.get('milestones', [])
    links = policy.get('links', [])
    if not milestones or not links:
        raise ValueError('missing or empty milestone inventory')
    native = {m['number']: m for m in milestones}
    if len(native) != len(milestones):
        raise ValueError('duplicate milestone identity')
    result = {}
    for item in links:
        path = Path(item['file'])
        if path.is_absolute() or '..' in path.parts or path.suffix != '.md':
            raise ValueError('unsafe document path')
        key = (item['file'], item['section'])
        if key in result or not key[1]:
            raise ValueError('missing or duplicate outcome section')
        number = item['number']
        if (number not in native or native[number]['title'] != item['milestone_title']
                or native[number]['html_url'] != f'https://github.com/faberline/lumen/milestone/{number}'):
            raise ValueError('milestone identity/title mismatch')
        result[key] = number
    return result


def verify(root, expected):
    root = Path(root).resolve()
    actual = {}
    errors = []
    paths = [root / name for name in ('README.md', 'STATUS.md', 'ROADMAP.md')]
    paths += sorted((root / 'docs').rglob('*.md'))
    for path in paths:
        if not path.is_file():
            raise ValueError(f'missing document inventory: {path.name}')
        if not path.resolve().is_relative_to(root):
            raise ValueError('document symlink leaves checkout')
        name = str(path.relative_to(root))
        section, heading_number, fence = '', None, None
        for line_number, line in enumerate(path.read_text().splitlines(), 1):
            marker = re.match(r'^\s*(`{3,}|~{3,})', line)
            if marker:
                token = marker[1][0]
                if fence is None:
                    fence = token
                elif fence == token:
                    fence = None
                continue
            if fence:
                continue
            heading = HEADING.match(line)
            if heading:
                suffix = SUFFIX.search(heading[1])
                heading_number = int(suffix[1]) if suffix else None
                section = SUFFIX.sub('', heading[1]).strip()
            matches = list(LINK.finditer(line))
            if ('/milestone/' in line or '[Milestone #' in line) and not matches and not heading:
                errors.append(f'{name}:{line_number}: malformed milestone link')
            for match in matches:
                key = (name, section)
                number = int(match[1])
                url = f'https://github.com/faberline/lumen/milestone/{number}'
                if match[2] != url:
                    errors.append(f'{name}:{line_number}: link text and URL disagree')
                if key not in expected:
                    errors.append(f'{name}:{line_number}: unknown outcome applicability')
                elif expected[key] != number:
                    errors.append(f'{name}:{line_number}: expected milestone {expected[key]}, found {number}')
                if heading_number is not None and heading_number != number:
                    errors.append(f'{name}:{line_number}: heading and tracking milestone disagree')
                if key in actual:
                    errors.append(f'{name}:{line_number}: duplicate tracking link')
                actual[key] = number
    missing = sorted(set(expected) - set(actual))
    errors += [f'{name}: missing outcome section/link {section}' for name, section in missing]
    if errors or not actual:
        raise ValueError('\n'.join(errors or ['zero milestone assertions']))
    return len(actual)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--fixture-sha256', required=True)
    args = parser.parse_args()
    expected = fixture(args.fixture.read_bytes(), args.fixture_sha256)
    count = verify(args.root, expected)
    print(json.dumps({'milestone_assertions': count, 'qualification': 'not evaluated'}))


if __name__ == '__main__':
    main()
