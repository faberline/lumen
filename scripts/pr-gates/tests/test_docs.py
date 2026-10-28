import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location('docs', Path(__file__).parents[1] / 'docs.py')
docs = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(docs)


class DocumentControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in ('README.md', 'STATUS.md', 'ROADMAP.md'):
            (self.root / name).write_text('# Document\n')
        (self.root / 'docs').mkdir()
        self.path = self.root / 'ROADMAP.md'
        self.good = '### Durable writes\n- Outcome: acknowledged writes survive restart.\n- Tracking: [Milestone #22](https://github.com/faberline/lumen/milestone/22).\n'
        self.path.write_text(self.good)
        self.expected = {('ROADMAP.md', 'Durable writes'): 22}

    def test_nonempty_valid_contract(self):
        self.assertEqual(1, docs.verify(self.root, self.expected))

    def test_prose_reference_is_not_a_tracking_link(self):
        self.path.write_text('Milestone #22 describes this outcome.\n' + self.good)
        self.assertEqual(1, docs.verify(self.root, self.expected))
        self.path.write_text('### Durable writes\nMilestone #22 describes this outcome.\n')
        with self.assertRaisesRegex(ValueError, 'missing outcome'):
            docs.verify(self.root, self.expected)

    def test_wrong_number_wrong_repo_and_broken_link_refuse(self):
        for text in (self.good.replace('#22]', '#23]'),
                     self.good.replace('/22)', '/23)'),
                     self.good.replace('faberline/lumen', 'faberline/axiom'),
                     self.good.replace('/22)', '/22')):
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.path.write_text(text)
                docs.verify(self.root, self.expected)

    def test_missing_or_renamed_outcome_refuses(self):
        for text in ('', self.good.replace('Durable writes', 'Unknown outcome')):
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.path.write_text(text)
                docs.verify(self.root, self.expected)

    def test_tracking_link_requires_nonempty_outcome_field(self):
        for text in (self.good.replace('- Outcome: acknowledged writes survive restart.\n', ''),
                     self.good.replace('acknowledged writes survive restart.', '')):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, 'outcome/status linkage'):
                self.path.write_text(text)
                docs.verify(self.root, self.expected)

    def test_heading_number_must_match_tracking(self):
        self.path.write_text(self.good.replace('Durable writes\n', 'Durable writes (Milestone #9)\n'))
        with self.assertRaisesRegex(ValueError, 'heading'):
            docs.verify(self.root, self.expected)

    def test_new_document_with_unmapped_milestone_refuses(self):
        (self.root / 'docs/new.md').write_text(self.good)
        with self.assertRaisesRegex(ValueError, 'unknown outcome'):
            docs.verify(self.root, self.expected)

    def test_fenced_shell_is_data_not_an_assertion(self):
        self.path.write_text('```sh\n' + self.good + '```\n')
        with self.assertRaisesRegex(ValueError, 'missing outcome'):
            docs.verify(self.root, self.expected)

    def test_missing_file_and_duplicate_tracking_refuse(self):
        self.path.write_text(self.good + self.good.splitlines()[-1] + '\n')
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            docs.verify(self.root, self.expected)
        (self.root / 'STATUS.md').unlink()
        with self.assertRaisesRegex(ValueError, 'missing document'):
            docs.verify(self.root, self.expected)

    def test_fixture_change_and_empty_inventory_refuse(self):
        raw = json.dumps({'version': 1, 'repository': 'faberline/lumen', 'milestones': [], 'links': []}).encode()
        with self.assertRaisesRegex(ValueError, 'changed'):
            docs.fixture(raw, '0' * 64)
        with self.assertRaisesRegex(ValueError, 'empty'):
            docs.fixture(raw, hashlib.sha256(raw).hexdigest())

    def test_boolean_contract_version_refuses(self):
        raw = json.dumps({'version': True, 'repository': 'faberline/lumen'}).encode()
        with self.assertRaisesRegex(ValueError, 'unknown'):
            docs.fixture(raw, hashlib.sha256(raw).hexdigest())


if __name__ == '__main__':
    unittest.main()
