#!/usr/bin/env python3
"""Lumen-owned unit batches; every manifest row keeps both original commands."""
import argparse
import hashlib
import importlib.util
import json
import os
import signal
from pathlib import Path
import subprocess
import sys
import time

spec = importlib.util.spec_from_file_location('lumen_batch_plan', Path(__file__).with_name('plan.py'))
unit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(unit)
CASE_SECONDS = 30 * 60


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def settings(value):
    if (not isinstance(value, dict) or set(value) != {'version', 'unit_batches'}
            or type(value['version']) is not int or value['version'] != 1
            or type(value['unit_batches']) is not int or not 1 <= value['unit_batches'] <= 256):
        raise ValueError('invalid Lumen batch configuration')
    return value['unit_batches']


def build(rows, count):
    if type(count) is not int or not 1 <= count <= min(len(rows), 256):
        raise ValueError('unknown or zero-case batch count')
    if not all(isinstance(row, dict) and set(row) == {'package', 'profile'}
               and all(isinstance(row[k], str) and row[k] for k in row) for row in rows):
        raise ValueError('invalid complete unit row')
    identities = [(row['package'], row['profile']) for row in rows]
    if len(identities) != len(set(identities)):
        raise ValueError('duplicate complete unit row')
    for row in rows:
        unit.feature_args(row['profile'])
    # Keep default rows spread across workers; retain all later feature rows.
    ordered = sorted(rows, key=lambda r: (r['profile'] != 'default', r['package'], r['profile']))
    batches = []
    for index in range(count):
        cases = [dict(row, case_id=digest([row['package'], row['profile']])) for row in ordered[index::count]]
        batches.append(dict(batch=f'{index+1:02}/{count:02}', cases=cases))
    result = dict(version=1, batches=batches)
    result['sha256'] = digest(result)
    return result


def invoke_command(argv, cwd, timeout):
    # The Python gate starts Cargo children. Kill its whole isolated process
    # group on timeout so a previous row cannot keep running behind the next.
    child = subprocess.Popen(argv, cwd=cwd, start_new_session=True)
    try:
        return subprocess.CompletedProcess(argv, child.wait(timeout=timeout))
    except subprocess.TimeoutExpired:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait()
        raise


def execute(plan, batch, root, baseline, head, base, invoke=invoke_command, clock=time.monotonic,
            verify=unit.verify_identity):
    selected = [item for item in plan['batches'] if item['batch'] == batch]
    if len(selected) != 1 or not selected[0]['cases']:
        raise ValueError('unknown or zero-case batch')
    cases = selected[0]['cases']
    expected = [row['case_id'] for row in cases]
    if len(expected) != len(set(expected)):
        raise ValueError('duplicate selected case')
    results = []
    common = ['--root', str(root), '--baseline-root', str(baseline),
              '--expected-head', head, '--expected-base', base]
    for row in cases:
        print('::group::' + row['case_id'] + ' ' + row['package'] + ' ' + row['profile'], flush=True)
        errors = []
        commands = []
        deadline = clock() + CASE_SECONDS
        try:
            verify(root, head, base, baseline)
            for kind in ('unit', 'clippy'):
                argv = [sys.executable, str(root/'scripts/pr-gates/plan.py'), 'run', *common,
                        '--package', row['package'], '--profile', row['profile'], '--kind', kind]
                remaining = deadline - clock()
                if remaining <= 0:
                    errors.append(dict(kind=kind, reason='shared 30-minute case budget exhausted'))
                    continue
                print(json.dumps(dict(case_id=row['case_id'], kind=kind, command=argv)), flush=True)
                try:
                    result = invoke(argv, cwd=root, timeout=remaining)
                    commands.append(dict(kind=kind, exit_code=result.returncode))
                    if result.returncode:
                        errors.append(dict(kind=kind, exit_code=result.returncode))
                except (OSError, subprocess.TimeoutExpired) as error:
                    errors.append(dict(kind=kind, reason=str(error)))
        except (ValueError, OSError) as error:
            errors.append(dict(reason=str(error)))
        finally:
            try:
                verify(root, head, base, baseline)
            except (ValueError, OSError) as error:
                errors.append(dict(reason=str(error)))
        record = dict(case_id=row['case_id'], package=row['package'], profile=row['profile'],
                      commands=commands, errors=errors, result='fail' if errors else 'pass')
        results.append(record)
        print('LUMEN_UNIT_CASE_V1 ' + json.dumps(record, sort_keys=True), flush=True)
        print('::endgroup::', flush=True)
    if [row['case_id'] for row in results] != expected:
        raise ValueError('batch execution differs from complete case roster')
    summary = dict(schema='LUMEN_UNIT_BATCH_V1', head=head, base=base, batch=batch,
                   plan_sha256=plan['sha256'], expected=expected, results=results,
                   run_id=os.environ.get('GITHUB_RUN_ID'), attempt=os.environ.get('GITHUB_RUN_ATTEMPT'))
    print('LUMEN_UNIT_BATCH_V1 ' + json.dumps(summary, sort_keys=True), flush=True)
    if any(row['result'] != 'pass' for row in results):
        raise ValueError('one or more complete unit rows failed')
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['matrix', 'run'])
    for name in ('root', 'baseline-root'):
        parser.add_argument('--'+name, type=Path, required=True)
    for name in ('expected-head', 'expected-base'):
        parser.add_argument('--'+name, required=True)
    parser.add_argument('--batch')
    args = parser.parse_args(argv)
    root, baseline = args.root.resolve(), args.baseline_root.resolve()
    unit.verify_identity(root, args.expected_head, args.expected_base, baseline)
    packages, prior = unit.load(root), unit.load(baseline)
    unit.compare_inventory(prior, packages)
    count = settings(json.loads((root/'.github/development-test-batches.json').read_text()))
    plan = build(unit.matrix_rows(packages), count)
    unit.verify_identity(root, args.expected_head, args.expected_base, baseline)
    if args.action == 'matrix':
        print(json.dumps(dict(plan_sha256=plan['sha256'], cases=sum(len(b['cases']) for b in plan['batches']))), file=sys.stderr)
        print(json.dumps({'include': [dict(batch=b['batch']) for b in plan['batches']]}, separators=(',', ':')))
    else:
        execute(plan, args.batch, root, baseline, args.expected_head, args.expected_base)


if __name__ == '__main__':
    main()
