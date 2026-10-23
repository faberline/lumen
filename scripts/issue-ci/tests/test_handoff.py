"""Affected handoff wiring tests: fixtures only, no network or product suites."""
import argparse
import contextlib
import io
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
        self.assertEqual(hashlib.sha256((SCRIPTS / 'handoff.py').read_bytes()).hexdigest(), 'c67533b39d45ad62d407af3748856be396941647c349b63464424fa6aff1c198')
        self.assertIn('67af8d020df3ca2ec4b064289408cfb0c2f8b299', CONFIG['controller_source'])

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

    def test_guard_requires_explicit_failure_only_policy_and_new_controller(self):
        policy.check(ROOT / '.github/issue-ci-required-gates.json')
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'policy.json'
            for bad in ({'workflows': GATES}, dict(CONFIG, lifecycle_mode='full'),
                        dict(CONFIG, qualification_blockers=None), dict(CONFIG, qualification_blockers=[]),
                        dict(CONFIG, qualification_blockers=['']),
                        dict(CONFIG, workflows=[])):
                path.write_text(json.dumps(bad))
                with self.assertRaises((RuntimeError, KeyError)):
                    policy.check(path)
            path.write_text(json.dumps(CONFIG))
            with patch.object(policy, 'load_controller', return_value=type('OldController', (), {'load_config': ci.load_config})()):
                with self.assertRaises(RuntimeError):
                    policy.check(path)

    def test_failure_only_success_missing_and_skipped_stay_pending(self):
        self.assertEqual(ci.evaluate_policy(CONFIG, evidence())[0], 'pending')
        self.assertEqual(ci.evaluate_policy(dict(CONFIG, qualification_blockers=[]), evidence())[0], 'pending')
        self.assertEqual(ci.evaluate_policy(CONFIG, {})[0], 'pending')
        snapshots = evidence()
        snapshots[GATES[0]['path']][1][0]['conclusion'] = 'skipped'
        self.assertEqual(ci.evaluate_policy(CONFIG, snapshots)[0], 'pending')
        snapshots[GATES[0]['path']][1][1]['conclusion'] = 'failure'
        state, reasons, _ = ci.evaluate_policy(CONFIG, snapshots)
        self.assertEqual(state, 'todo')
        self.assertIn(snapshots[GATES[0]['path']][1][1]['html_url'], '\n'.join(reasons))

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

class FailureOnlyApplyTests(unittest.TestCase):
    def setUp(self):
        self.pr = dict(state='open', body='Closes #7', head=dict(sha=SHA, ref='issue/7', repo={'full_name': 'o/r'}))
        self.labels = {'doing', 'priority:p1'}
        self.comments, self.writes = [], []
        self.failed = evidence()
        self.failed[GATES[0]['path']][1][0]['conclusion'] = 'failure'

    def api(self, path, method='GET', data=None):
        if method == 'GET':
            if '/pulls/' in path:
                return self.pr
            return dict(state='open', labels=[dict(name=n) for n in self.labels])
        self.writes.append((path, method, data))
        if method == 'POST' and path.endswith('/comments'):
            c = dict(id=len(self.comments)+1, body=data['body'], user={'login': 'github-actions[bot]'})
            self.comments.append(c)
            return c
        if method == 'PATCH':
            self.comments[int(path.rsplit('/',1)[1])-1]['body'] = data['body']
        if method == 'POST' and path.endswith('/labels'):
            self.labels.update(data['labels'])
        if method == 'DELETE':
            self.labels.discard(path.rsplit('/',1)[1])
        return {}

    def invoke(self, snapshots=None):
        args = argparse.Namespace(repo='o/r', pr=8, head=SHA, apply=True, run_id=1)
        with patch.object(ci, 'api', side_effect=self.api), patch.object(ci, 'pages', side_effect=lambda *a: self.comments), patch.object(ci, 'snapshot', return_value=self.failed if snapshots is None else snapshots), contextlib.redirect_stdout(io.StringIO()):
            return ci.process_head(args, CONFIG, check_event=False)

    def test_success_and_missing_have_no_writes(self):
        self.invoke(evidence())
        self.invoke({})
        self.assertEqual(self.writes, [])

    def test_only_doing_returns_todo_and_preserves_other_labels(self):
        self.invoke()
        self.assertEqual(self.labels, {'todo', 'priority:p1'})
        self.assertIn(self.failed[GATES[0]['path']][1][0]['html_url'], self.comments[0]['body'])
        self.assertFalse(any('/labels/done' in x[0] for x in self.writes))

    def test_done_todo_and_mixed_issues_cannot_be_mutated(self):
        for labels in ({'done'}, {'done','doing'}, {'todo'}, {'todo','doing'}):
            self.labels = labels.copy()
            self.invoke()
            self.assertEqual(self.writes, [])

    def test_duplicate_failure_preserves_later_agy_claim(self):
        self.invoke()
        self.labels = {'doing', 'priority:p1'}
        self.writes.clear()
        self.invoke()
        self.assertEqual(self.writes, [])
        self.assertEqual(self.labels, {'doing', 'priority:p1'})

    def test_ready_recovery_does_not_readd_consumed_todo(self):
        self.invoke()
        self.comments[0]['body'] = self.comments[0]['body'].replace(':applied -->', ':ready -->')
        self.labels = {'doing', 'priority:p1'}
        self.writes.clear()
        self.invoke()
        self.assertEqual(self.labels, {'doing', 'priority:p1'})
        self.assertTrue(self.writes)
        self.assertTrue(all('/issues/comments/' in w[0] for w in self.writes))


if __name__ == '__main__':
    unittest.main()
