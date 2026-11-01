"""Protect the PR unit-before-cloud boundary and non-PR entry points."""
import pathlib
import os
import re
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[3]


def job(source, name):
    match = re.search(rf'^  {re.escape(name)}:\n(.*?)(?=^  [a-z][a-z-]*:\n|\Z)',
                      source, re.MULTILINE | re.DOTALL)
    if not match:
        raise AssertionError(f'missing workflow job: {name}')
    return match.group(1)


class WorkflowSequence(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.unit = (ROOT / '.github/workflows/pr-unit-static.yml').read_text()
        cls.cloud = (ROOT / '.github/workflows/gke-test.yml').read_text()

    def test_unit_workflow_is_available_for_pr_and_as_dependency(self):
        triggers = self.unit.split('permissions:', 1)[0]
        self.assertRegex(triggers, r'(?m)^  pull_request:$')
        self.assertIn('types: [opened, reopened, synchronize, ready_for_review, edited]', triggers)
        self.assertIn('lumen-development-${{ github.ref }}', self.unit)
        self.assertNotRegex(self.cloud.split('\npermissions:',1)[0], r'(?m)^  pull_request:$')
        self.assertRegex(self.cloud, r'(?m)^  workflow_call:$')
        self.assertNotIn('uses: ./.github/workflows/pr-unit-static.yml', self.cloud)

    def test_no_cloud_entry_starts_before_pr_unit_success(self):
        barrier = 'needs: [inventory, workspace-format, workspace-unit-static, milestone-docs]'
        for name in ('image', 'test-client','workspace-contracts','measured-performance'):
            with self.subTest(job=name):
                block = job(self.unit, name)
                self.assertIn(barrier,block)
                self.assertNotRegex(block,r'(?m)^    if:')
        self.assertIn('needs: [inventory, image]',job(self.unit,'resolve'))
        self.assertIn('needs: resolve', job(self.unit, 'standalone-container'))
        self.assertIn('needs: [resolve, test-client]', job(self.unit, 'gke-test'))

    def test_every_static_consumer_waits_for_complete_inventory_and_impact(self):
        for name in ('workspace-format','workspace-unit-static','milestone-docs'):
            with self.subTest(job=name):self.assertRegex(job(self.unit,name),r'(?m)^    needs: inventory$')
        inventory=job(self.unit,'inventory')
        self.assertIn('fetch-depth: 0',inventory)
        self.assertIn('plan.py impact',inventory)
        self.assertIn('batches.py matrix',inventory)

    def test_matrix_builder_failure_fails_inventory_instead_of_exporting_empty_matrix(self):
        # Execute only the metadata step with a fake Python command. The
        # fixture never invokes Cargo, a product binary, or a remote service.
        section=job(self.unit,'inventory').split('      - name: Inventory every library',1)[1]
        section=section.split('      - name:',1)[0]
        block=section.split('        run: |\n',1)[1]
        script='\n'.join(line[10:] for line in block.splitlines() if line.startswith('          '))
        with tempfile.TemporaryDirectory() as directory:
            root=pathlib.Path(directory);python=root/'python3';output=root/'outputs'
            python.write_text('#!/bin/sh\ncase "$*" in *"batches.py matrix"*) exit 23;; esac\nexit 0\n')
            python.chmod(0o700)
            env=dict(os.environ,PATH=str(root)+os.pathsep+os.environ['PATH'],
                     EXPECTED_HEAD='a'*40,EXPECTED_BASE='b'*40,GITHUB_OUTPUT=str(output))
            result=subprocess.run(['bash','--noprofile','--norc','-c',script],env=env,capture_output=True,text=True)
            self.assertEqual(result.returncode,23,result.stderr)
            self.assertFalse(output.exists())

    def test_dispatch_and_release_workflow_inputs_remain_available(self):
        triggers = self.cloud.split('\npermissions:', 1)[0]
        self.assertNotRegex(triggers, r'(?m)^  pull_request:$')
        self.assertRegex(triggers, r'(?m)^  workflow_dispatch:$')
        self.assertRegex(triggers, r'(?m)^  workflow_call:$')
        self.assertIn("inputs.image == ''", job(self.cloud, 'image'))
        self.assertIn("needs.image.result == 'skipped'", job(self.cloud, 'resolve'))


if __name__ == '__main__':
    unittest.main()
