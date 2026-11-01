"""Missing test content and wrong-runtime execution must fail closed."""
import importlib.util
from pathlib import Path
import unittest
import json
import tempfile
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('lumen_contract_controls',ROOT/'scripts/pr-gates/contracts.py')
c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)


def output(passed,ignored=()):
    return f'running {len(passed)+len(ignored)} tests\n'+''.join(f'test {name} ... ok\n'for name in passed)+''.join(f'test {name} ... ignored\n'for name in ignored)+f'\ntest result: ok. {len(passed)} passed; 0 failed; {len(ignored)} ignored; 0 measured; 0 filtered out; finished in 0.01s\n'


class ContractsTests(unittest.TestCase):
    def test_complete_nonzero_roster_matches_and_unknown_extra_refuses(self):
        value=c.validate_execution(output(['a','b']),{'a','b'},'it')
        self.assertEqual(value['local_executed'],['a','b']);self.assertFalse(value['external_required'])
        with self.assertRaisesRegex(ValueError,'discovered roster'):c.validate_execution(output(['a','b']),{'a'},'it')

    def test_zero_tests_cannot_supply_functional_coverage(self):
        with self.assertRaises(ValueError):c.validate_execution(output([]),set(),'it')
        with self.assertRaisesRegex(ValueError,'zero executed'):c.validate_execution(output([],['e2e::overlay::journey']),{'e2e::overlay::journey'},'it')

    def test_known_external_test_is_required_and_never_counted_as_local_pass(self):
        name='e2e::overlay::journey';value=c.validate_execution(output(['local'],[name]),{'local',name},'it')
        self.assertEqual(value['external_required'],{name:'GKE e2e (overlay) / deploy + test on shared GKE'})
        self.assertEqual(value['local_executed'],['local']);self.assertFalse(value['external_tests_counted_as_local_pass'])

    def test_unknown_ignored_content_and_release_workload_remain_failures(self):
        # An explicit source-bound owner/applicability record is necessary.
        # Missing records cannot silently waive old release or EC work.
        for name in ('new_unknown','perf_gate::durable_workload::approved_30_minute_durable_workload','stability_lumen_long_running_stability_resource_leak::lumen_long_running_stability_resource_leak'):
            with self.subTest(name=name),patch.object(c,'ignored_contract',return_value=None),self.assertRaisesRegex(ValueError,'not passed'):
                c.validate_execution(output(['local'],[name]),{'local',name},'it')

    def test_preserved_full_workload_is_outside_normal_pr_and_never_passed(self):
        name='perf_gate::durable_workload::approved_30_minute_durable_workload'
        value=c.validate_execution(output(['local'],[name]),{'local',name},'it')
        self.assertFalse(value['external_required'])
        self.assertFalse(value['non_applicable_tests_counted_as_pass'])
        self.assertIn('not_executed_not_passed',value['outside_normal_pr'][name]['result'])
        self.assertIn('sixteen',value['outside_normal_pr'][name]['basis'])

    def test_retired_ec_contract_requires_actual_current_hosted_owner(self):
        name='stability_lumen_long_running_stability_resource_leak::lumen_long_running_stability_resource_leak'
        value=c.validate_execution(output(['local'],[name]),{'local',name},'it')
        self.assertEqual(value['external_required'][name],'native resource and performance contracts')
        self.assertEqual(value['local_executed'],['local'])
        self.assertFalse(value['external_tests_counted_as_local_pass'])

    def test_source_changed_duplicate_or_unsafe_applicability_refuses(self):
        source=json.loads((ROOT/'docs/ci-qualification/normal-pr-ignored-tests.json').read_text())
        for mutation in ('hash','duplicate','path'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                row=dict(source['tests'][0]);path=root/row['source'];path.parent.mkdir(parents=True);path.write_bytes((ROOT/row['source']).read_bytes())
                rows=[row]
                if mutation=='hash':row['source_sha256']='0'*64
                elif mutation=='duplicate':rows.append(dict(row))
                else:row['source']='../outside.rs'
                config=root/'docs/ci-qualification/normal-pr-ignored-tests.json';config.parent.mkdir(parents=True);config.write_text(json.dumps(dict(version=1,tests=rows)))
                with self.assertRaises(ValueError):c.ignored_contracts(root)

    def test_external_names_cannot_be_forged_on_an_unrelated_target(self):
        name='e2e::overlay::journey'
        with self.assertRaisesRegex(ValueError,'not passed'):c.validate_execution(output(['local'],[name]),{'local',name},'another-target')

    def test_default_contract_command_cannot_start_ignored_release_or_cloud_work(self):
        target=dict(name='it',**{'required-features':[]});package=dict(name='lumen',features={},integration_targets=[target])
        argv=c.command(package,'default',target)
        self.assertEqual(argv[:5],['cargo','test','--release','--locked','-p'])
        self.assertNotIn('--include-ignored',argv);self.assertNotIn('--ignored',argv)

    def test_duplicate_ignored_and_changed_roster_are_rejected(self):
        with self.assertRaises(ValueError):c.validate_execution(output(['a'],['a']),{'a'},'it')
        with self.assertRaises(ValueError):c.validate_execution(output(['a']),{'a','lost'},'it')


if __name__=='__main__':unittest.main()
