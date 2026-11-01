#!/usr/bin/env python3
"""Run each named hosted contract with exact source and nonzero harness proof."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile


def module(name):
    spec = importlib.util.spec_from_file_location('lumen_external_' + name, Path(__file__).with_name(name + '.py'))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


contracts = module('contracts')
measure = module('measure')
unit = measure.unit
JOBS = {'functional': 'native functional contracts', 'performance': 'native resource and performance contracts'}


def invoke(command, cwd=None, env=None, timeout=1800):
    child = subprocess.Popen(command, cwd=cwd, env=env, text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             start_new_session=True)
    try:
        stdout, stderr = child.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        stdout, stderr = child.communicate()
        raise subprocess.TimeoutExpired(command, timeout, output=stdout, stderr=stderr)
    return subprocess.CompletedProcess(command, child.returncode, stdout, stderr)


def test_command(binary, target, name):
    command = [binary, name, '--exact', '--include-ignored', '--test-threads', '1', '--format', 'pretty', '--color', 'never']
    if target != 'it':
        command.append('--show-output')
    return command


def validate_direct_output(target, stdout, stderr):
    if target == 'disk_scale_proof' and 'SKIPPING' in stdout + stderr:
        raise ValueError('disk RSS workload skipped its actual measurement')


def built_binaries(root, suite):
    selectors = ['--test', 'it']
    if suite == 'functional':
        selectors += ['--bin', 'lumen']
    else:
        selectors += ['--test', 'disk_scale_proof', '--test', 'perf_gate_vs_db']
    command = ['cargo', 'test', '--release', '--locked', '-p', 'lumen', '--features', 'release', *selectors, '--no-run', '--message-format=json']
    result = subprocess.run(command, cwd=root, text=True, capture_output=True)
    print(result.stderr, flush=True)
    if result.returncode:
        print(result.stdout, flush=True)
        raise ValueError('hosted contract build failed: ' + str(result.returncode))
    binaries = {}
    for line in result.stdout.splitlines():
        record = json.loads(line)
        if record.get('reason') != 'compiler-artifact' or not record.get('executable') or record['profile']['test'] is not True:
            continue
        name = record['target']['name']
        if name in binaries and binaries[name] != record['executable']:
            raise ValueError('ambiguous compiled test executable')
        binaries[name] = record['executable']
    needed = {'it', 'lumen'} if suite == 'functional' else {'it', 'disk_scale_proof', 'perf_gate_vs_db'}
    if set(binaries) != needed:
        raise ValueError('missing or extra compiled test executable: ' + str(sorted(binaries)))
    return binaries


def discover(binary, selected=None):
    command = [binary, '--list', '--format', 'pretty', '--color', 'never']
    if selected is not None:
        command[1:1] = [selected, '--exact']
    result = invoke(command, timeout=60)
    if result.returncode:
        raise ValueError('compiled test discovery failed')
    return unit.listed_unit_tests(result.stdout)


def selected(root, suite):
    rows = contracts.ignored_contracts(root)
    names = {(row['target'], row['name']) for row in rows if row['normal_pr'] == 'external' and row['owner'] == JOBS[suite]}
    if not names:
        raise ValueError('zero selected hosted contracts')
    return names


def run(root, suite, provenance):
    overrides = [key for key in os.environ if key.startswith(('RIG_VAR_', 'CARGO_PROFILE_', 'LUMEN_GATE_', 'LUMEN_SCALE_'))]
    if overrides:
        raise ValueError('ambient workload overrides are forbidden: '+str(sorted(overrides)))
    names = selected(root, suite)
    binaries = built_binaries(root, suite)
    environment = os.environ.copy()
    for key, name in [('LUMEN_CONTRACT_CLI_TEST_BIN', 'lumen'), ('LUMEN_CONTRACT_SCALE_TEST_BIN', 'disk_scale_proof'), ('LUMEN_CONTRACT_COMPETITIVE_TEST_BIN', 'perf_gate_vs_db')]:
        if name in binaries:
            environment[key] = binaries[name]
    full = {target: discover(binary) for target, binary in binaries.items()}
    for row in contracts.ignored_contracts(root):
        if row['target'] in full and row['name'] not in full[row['target']]:
            raise ValueError('recorded ignored test disappeared from full harness: '+row['name'])
    errors = []
    for target, name in sorted(names):
        record = dict(schema='LUMEN_HOSTED_CONTRACT_V1', provenance=provenance, suite=suite, target=target, name=name,
                      binary_sha256=hashlib.sha256(Path(binaries[target]).read_bytes()).hexdigest())
        try:
            row = contracts.ignored_contract(root, target, name)
            if row is None or row['owner'] != JOBS[suite]:
                raise ValueError('unreviewed execution owner')
            selection = discover(binaries[target], name)
            if selection != {name} or not selection <= full[target]:
                raise ValueError('exact selected contract is missing or ambiguous')
            command = test_command(binaries[target], target, name)
            with tempfile.TemporaryDirectory(prefix='lumen-native-receipt-') as directory:
                receipt = Path(directory)/'measurements.jsonl'
                invocation_env = dict(environment, LUMEN_NATIVE_CONTRACT_RECEIPT=str(receipt))
                result = invoke(command, cwd=root, env=invocation_env, timeout=1800)
                record['measurements'] = [json.loads(line) for line in receipt.read_text().splitlines()] if receipt.exists() else []
            print(result.stdout, flush=True)
            print(result.stderr, flush=True)
            record.update(command=command, exit_code=result.returncode,
                          stdout_sha256=hashlib.sha256(result.stdout.encode()).hexdigest(),
                          stderr_sha256=hashlib.sha256(result.stderr.encode()).hexdigest())
            if result.returncode:
                raise ValueError('hosted contract failed: ' + str(result.returncode))
            validate_direct_output(target, result.stdout, result.stderr)
            if name.startswith(('behavior_', 'stability_', 'efficiency_', 'benchmark_')) and not record['measurements']:
                raise ValueError('native contract has no actual measurement receipt')
            record['tests'] = measure.selected_evidence(result.stdout, full[target], selection)
            if discover(binaries[target]) != full[target] or discover(binaries[target], name) != selection:
                raise ValueError('hosted contract roster changed during execution')
            record.update(result='pass')
        except (ValueError, OSError, subprocess.TimeoutExpired) as error:
            errors.append(name + ': ' + str(error))
            record.update(result='fail', error=str(error))
            if isinstance(error, subprocess.TimeoutExpired):
                print(error.output or '', flush=True)
                print(error.stderr or '', flush=True)
                print('LUMEN_HOSTED_CONTRACT_V1 ' + json.dumps(record, sort_keys=True), flush=True)
                # A timed-out Docker harness cannot execute its Rust cleanup.
                # Fail this isolated hosted job now so its runner is retired;
                # never let unknown leftover services affect a later case.
                raise ValueError('timed-out contract; remaining work is not executed or passed') from error
        print('LUMEN_HOSTED_CONTRACT_V1 ' + json.dumps(record, sort_keys=True), flush=True)
    if errors:
        raise ValueError('\n'.join(errors))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    for key in ('root', 'baseline-root'):
        parser.add_argument('--' + key, type=Path, required=True)
    for key in ('expected-head', 'expected-base'):
        parser.add_argument('--' + key, required=True)
    parser.add_argument('--suite', choices=JOBS, required=True)
    args = parser.parse_args(argv)
    root, baseline = args.root.resolve(), args.baseline_root.resolve()
    provenance = unit.verify_identity(root, args.expected_head, args.expected_base, baseline)
    provenance.update(repo='faberline/lumen', run_id=os.environ.get('GITHUB_RUN_ID'), run_attempt=os.environ.get('GITHUB_RUN_ATTEMPT'),
                      driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), features='release')
    try:
        run(root, args.suite, provenance)
    finally:
        unit.verify_identity(root, args.expected_head, args.expected_base, baseline)


if __name__ == '__main__':
    main()
