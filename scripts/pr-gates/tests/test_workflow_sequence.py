"""Protect the PR unit-before-cloud boundary and non-PR entry points."""
import pathlib
import re
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
        self.assertRegex(triggers, r'(?m)^  workflow_call:$')
        self.assertIn('pr-unit-static-${{ github.workflow }}-${{ github.ref }}', self.unit)
        unit = job(self.cloud, 'unit-static')
        self.assertIn("if: github.event_name == 'pull_request'", unit)
        self.assertIn('uses: ./.github/workflows/pr-unit-static.yml', unit)

    def test_no_cloud_entry_starts_before_pr_unit_success(self):
        condition = "(github.event_name != 'pull_request' || needs.unit-static.result == 'success')"
        for name in ('image', 'resolve', 'test-client'):
            with self.subTest(job=name):
                block = job(self.cloud, name)
                self.assertRegex(block, r'(?m)^    needs: (?:unit-static|\[unit-static, image\])$')
                self.assertIn(condition, block)
                self.assertIn('!cancelled()', block)
        self.assertIn('needs: resolve', job(self.cloud, 'standalone-container'))
        self.assertIn('needs: [resolve, test-client]', job(self.cloud, 'gke-test'))

    def test_dispatch_and_release_workflow_inputs_remain_available(self):
        triggers = self.cloud.split('\npermissions:', 1)[0]
        self.assertRegex(triggers, r'(?m)^  pull_request:$')
        self.assertRegex(triggers, r'(?m)^  workflow_dispatch:$')
        self.assertRegex(triggers, r'(?m)^  workflow_call:$')
        self.assertIn("inputs.image == ''", job(self.cloud, 'image'))
        self.assertIn("needs.image.result == 'skipped'", job(self.cloud, 'resolve'))


if __name__ == '__main__':
    unittest.main()
