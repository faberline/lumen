"""Affected handoff wiring tests: fixtures only, no network or product suites."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / 'scripts/issue-ci'


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


ci = module('handoff', SCRIPTS / 'handoff.py')
policy = module('check_policy', SCRIPTS / 'check-policy.py')
CONFIG = json.loads((ROOT / '.github/issue-ci-required-gates.json').read_text())
GATES = CONFIG['workflows']
SHA = 'a' * 40


def evidence():
    result = {}
    for i, workflow in enumerate(GATES, 1):
        run = dict(id=i, run_number=i, run_attempt=1, status='completed', conclusion='success', html_url=f'https://github.com/o/r/actions/runs/{i}')
        jobs = [dict(name=name, status='completed', conclusion='success', html_url=f'{run["html_url"]}/job/{j}') for j, name in enumerate(workflow['jobs'])]
        result[workflow['path']] = run, jobs
    return result


class HandoffTests(unittest.TestCase):
    def test_controller_matches_verified_aw_merge(self):
        self.assertEqual(hashlib.sha256((SCRIPTS / 'handoff.py').read_bytes()).hexdigest(), '1abb5570032eaf294a50c5fa0bfb9ae7edd1db7e3f0a98a1f56601720916caf1')
        self.assertIn('84e9b72b4a63cc30b47449a2e321981525fa9c34', CONFIG['controller_source'])

    def test_exact_complete_inventory(self):
        inventory = json.loads((SCRIPTS / 'tests/fixtures/gate-inventory.json').read_text())
        self.assertEqual(GATES, [dict(path=inventory['path'], jobs=inventory['jobs'])])
        self.assertEqual(len(GATES[0]['jobs']), 8)
        source = (ROOT / inventory['path']).read_text()
        self.assertIn('name: ' + inventory['workflow_name'], source)
        self.assertIn('  pull_request:\n    paths:', source)
        self.assertIn('name: resolve image identity', source)
        self.assertEqual(sum(' / deploy + test on shared GKE' in name for name in inventory['jobs']), 2)
        self.assertEqual(sum(' / build (' in name for name in inventory['jobs']), 2)

    def test_no_vacuous_done_empty_or_invalid_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'gates.json'
            for gates in ([], [dict(path=GATES[0]['path'], jobs=[])], GATES * 2):
                path.write_text(json.dumps(dict(workflows=gates)))
                with self.assertRaises(RuntimeError):
                    ci.load_config(path)

    def test_blocked_rollout_cannot_invoke_controller(self):
        with patch.object(ci, 'api', side_effect=AssertionError('must not call GitHub')):
            with self.assertRaisesRegex(RuntimeError, 'rollout blocked'):
                policy.check(ROOT / '.github/issue-ci-required-gates.json')
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'policy.json'
            for config in ({'workflows': GATES}, {'workflows': [], 'qualification_blockers': []}, CONFIG):
                path.write_text(json.dumps(config))
                with self.assertRaises(RuntimeError):
                    policy.check(path)
            path.write_text(json.dumps({'workflows': GATES, 'qualification_blockers': []}))
            policy.check(path)

    def test_every_job_is_required(self):
        self.assertEqual(ci.evaluate(GATES, evidence())[0], 'done')
        for index in range(len(GATES[0]['jobs'])):
            with self.subTest(missing=GATES[0]['jobs'][index]):
                snapshots = evidence()
                snapshots[GATES[0]['path']][1].pop(index)
                self.assertEqual(ci.evaluate(GATES, snapshots)[0], 'pending')
        self.assertEqual(ci.evaluate(GATES, {})[0], 'pending')

    def test_skip_cancel_neutral_pending_and_failure(self):
        for status in ('skipped', 'cancelled', 'neutral', None):
            snapshots = evidence()
            snapshots[GATES[0]['path']][1][0]['conclusion'] = status
            self.assertEqual(ci.evaluate(GATES, snapshots)[0], 'pending')
        snapshots = evidence()
        snapshots[GATES[0]['path']][0]['status'] = 'in_progress'
        self.assertEqual(ci.evaluate(GATES, snapshots)[0], 'pending')
        snapshots = evidence()
        job = snapshots[GATES[0]['path']][1][-1]
        job['conclusion'] = 'failure'
        state, reasons, _ = ci.evaluate(GATES, snapshots)
        self.assertEqual(state, 'todo')
        self.assertIn(job['html_url'], '\n'.join(reasons))

    def test_stale_foreign_and_old_attempt_cannot_supply_gates(self):
        good = dict(id=3, run_number=2, run_attempt=2, status='completed', conclusion='success', html_url='https://github.com/o/r/actions/runs/3', head_sha=SHA, event='pull_request', head_repository={'full_name': 'o/r'}, path=GATES[0]['path'])
        old = dict(good, id=2, run_attempt=1)
        stale = dict(good, id=100, head_sha='b' * 40)
        foreign = dict(good, id=101, head_repository={'full_name': 'fork/r'})
        with patch.object(ci, 'pages', side_effect=[[old, good, stale, foreign], []]) as pages:
            snapshots = ci.snapshot('o/r', SHA, CONFIG)
        self.assertEqual(snapshots[GATES[0]['path']][0]['id'], 3)
        self.assertIn('/runs/3/attempts/2/jobs', pages.call_args_list[1].args[0])
        self.assertEqual(ci.evaluate(GATES, snapshots)[0], 'pending')

    def test_moved_pr_head_never_mutates(self):
        args = argparse.Namespace(repo='o/r', pr=7, head=SHA, apply=True, run_id=1)
        pr = dict(state='open', head=dict(sha='b' * 40, repo={'full_name': 'o/r'}))
        with patch.object(ci, 'api', return_value=pr) as api:
            with self.assertRaisesRegex(RuntimeError, 'stale PR head'):
                ci.process_head(args, CONFIG)
        self.assertEqual(len(api.call_args_list), 1)
        self.assertEqual(api.call_args_list[0].args, ('repos/o/r/pulls/7',))

    def test_trusted_workflow_guard_precedes_controller(self):
        workflow = (ROOT / '.github/workflows/issue-ci.yml').read_text()
        self.assertIn('ref: ${{ github.event.repository.default_branch }}', workflow)
        self.assertIn('persist-credentials: false', workflow)
        self.assertLess(workflow.index('python3 scripts/issue-ci/check-policy.py'), workflow.index('python3 scripts/issue-ci/handoff.py'))
        self.assertIn('--reconcile', workflow)
        self.assertIn('group: issue-ci-handoff', workflow)
        self.assertIn('workflows: ["gke-test"]', workflow)
        self.assertNotIn('pull_request_target', workflow.replace('# This is a trusted default-branch workflow, NOT pull_request_target execution.', ''))
        for forbidden in ('pull_request.head', 'download-artifact', 'workflow_dispatch:', 'id-token:', 'packages:'):
            self.assertNotIn(forbidden, workflow)


if __name__ == '__main__':
    unittest.main()
