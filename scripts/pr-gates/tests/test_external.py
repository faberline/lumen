"""External execution must bind complete real rosters to named owners."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('external_controls',ROOT/'scripts/pr-gates/external.py')
external=importlib.util.module_from_spec(spec);spec.loader.exec_module(external)


class ExternalTests(unittest.TestCase):
    def test_current_named_owners_are_disjoint_and_nonzero(self):
        functional=external.selected(ROOT,'functional')
        performance=external.selected(ROOT,'performance')
        self.assertTrue(functional and performance)
        self.assertFalse(functional & performance)
        self.assertIn(('perf_gate_vs_db','competitive_perf_gate'),performance)
        self.assertIn(('disk_scale_proof','scale_proof_reopen_rss_is_bounded'),performance)
        self.assertEqual(sum(name.startswith('docker_named_volume::')for _,name in functional),2)

    def test_ambient_workload_reduction_refuses_before_build(self):
        for key in ('RIG_VAR_KEYSPACE','CARGO_PROFILE_RELEASE_OPT_LEVEL','LUMEN_GATE_N','LUMEN_SCALE_N'):
            with self.subTest(key=key),patch.dict(external.os.environ,{key:'1'}),patch.object(external,'built_binaries')as build:
                with self.assertRaisesRegex(ValueError,'overrides'):external.run(ROOT,'performance',{})
                build.assert_not_called()

    def test_missing_owner_cannot_be_empty_success(self):
        with patch.object(external.contracts,'ignored_contracts',return_value=[]):
            with self.assertRaisesRegex(ValueError,'zero selected'):external.selected(ROOT,'functional')


if __name__=='__main__':unittest.main()
