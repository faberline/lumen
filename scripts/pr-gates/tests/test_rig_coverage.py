"""A green outer harness cannot replace the original nested required workloads."""
import copy
import importlib.util
import os
from pathlib import Path
import tempfile
import tomllib
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('rig_coverage_controls',ROOT/'scripts/pr-gates/rig_coverage.py')
coverage=importlib.util.module_from_spec(spec);spec.loader.exec_module(coverage)


def complete_receipts():
    cases,pins=coverage.inventory(ROOT)
    result=[]
    for name,case in cases.items():
        captures={key:1 for step in case['steps']for key in step.get('capture',{})}
        captures.update({s['name']+'_recovered_secs':1 for s in case['steps']if s['type']=='wait_until'})
        result.append(dict(kind='scenario',name=name,values=dict(steps=len(case['steps']),expected_steps=len(case['steps']),captures=captures)))
        if not case.get('load'):continue
        profile=case['load'];scenario='/'.join((case['record']['suite'],case['record']['dimension'],case['record']['case']))
        metrics=dict(p99_ms=1.0,error_rate=0.0)
        checked=[]
        for pin in pins:
            if scenario!=pin['scenario']and not scenario.endswith('/'+pin['scenario']):continue
            ratchet=pin.get('ratchet')
            checked.append(dict(issue=pin['issue'],pin_scenario=pin['scenario'],scenario=scenario,metric=pin['metric'],
                value=metrics[pin['metric']],floor=pin.get('floor'),ratchet=ratchet,strict=False,permitted=True,
                host_fingerprint='fixture-host',baseline=None,ratchet_evaluated=False,
                outcome='no_baseline'if ratchet is not None else'pass',severity='info'if ratchet is not None else'none'))
        result.append(dict(kind='load',name=name,values=dict(**metrics,profile=copy.deepcopy(profile),
            achieved_qps=profile['target_qps'],total=profile['target_qps']*(profile['duration_secs']-profile['warmup_secs']),
            failed=0,pins_checked=len(checked),pins=checked)))
    return result


class RigCoverageTests(unittest.TestCase):
    def setUp(self):
        self.environment=patch.dict(os.environ,{'RIG_STRICT':''});self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_original_eight_required_cases_and_all_real_measurements_are_required(self):
        result=coverage.validate(ROOT,complete_receipts())
        self.assertEqual(len(result['required']),8)
        self.assertEqual(result['executed'],result['required'])
        self.assertIn('load/data_table_browse',result['executed'])
        self.assertIn('load/search_qps',result['executed'])

    def test_old_six_case_gap_and_each_missing_case_are_rejected(self):
        receipts=complete_receipts()
        old=[r for r in receipts if r['name']not in('load/search_qps','load/data_table_browse')]
        with self.assertRaisesRegex(ValueError,'missing required Rig scenarios'):
            coverage.validate(ROOT,old)
        for name in coverage.inventory(ROOT)[0]:
            with self.subTest(name=name),self.assertRaisesRegex(ValueError,'missing required Rig scenarios'):
                coverage.validate(ROOT,[r for r in receipts if r['name']!=name])

    def test_incomplete_steps_captures_load_windows_and_schedule_cannot_pass(self):
        mutations=[('scenario','load/data_table_browse',lambda v:v.update(steps=0)),
            ('scenario','load/data_table_browse',lambda v:v.update(captures={})),
            ('load','load/search_qps',lambda v:v['profile'].update(target_qps=199)),
            ('load','load/search_qps',lambda v:v['profile'].update(workers=1)),
            ('load','load/search_qps',lambda v:v['profile'].update(duration_secs=10)),
            ('load','load/search_qps',lambda v:v.update(total=4999)),
            ('load','load/search_qps',lambda v:v.update(achieved_qps=189)),
            ('load','load/search_qps',lambda v:v.update(p99_ms=float('nan')))]
        for kind,name,mutate in mutations:
            with self.subTest(kind=kind,name=name):
                rows=complete_receipts();mutate(next(r['values']for r in rows if r['kind']==kind and r['name']==name))
                with self.assertRaises(ValueError):coverage.validate(ROOT,rows)

    def test_missing_baseline_is_info_not_a_claimed_ratchet_pass(self):
        rows=complete_receipts();coverage.validate(ROOT,rows)
        pin=next(r for r in rows if r['kind']=='load'and r['name']=='load/search_qps')['values']['pins'][0]
        self.assertEqual(pin['outcome'],'no_baseline');self.assertFalse(pin['ratchet_evaluated'])
        pin.update(outcome='pass',severity='none',ratchet_evaluated=True)
        with self.assertRaisesRegex(ValueError,'NoBaseline'):coverage.validate(ROOT,rows)

    def test_existing_ratchet_is_enforced_and_strict_missing_baseline_cannot_pass(self):
        rows=complete_receipts()
        pin=next(r for r in rows if r['kind']=='load'and r['name']=='load/search_qps')['values']['pins'][0]
        pin.update(baseline=1.0,outcome='pass',severity='none',ratchet_evaluated=True)
        coverage.validate(ROOT,rows)
        pin['baseline']=0.01
        with self.assertRaisesRegex(ValueError,'ratchet breached'):coverage.validate(ROOT,rows)
        with patch.dict(os.environ,{'RIG_STRICT':'1'}):
            with self.assertRaises(ValueError):coverage.validate(ROOT,complete_receipts())

    def test_required_flags_and_original_floor_cannot_be_removed(self):
        for changed in ('required','floor'):
            with self.subTest(changed=changed),tempfile.TemporaryDirectory()as directory:
                root=Path(directory)
                for source in (ROOT/'crates/lumen/tests/rig').rglob('*.toml'):
                    target=root/source.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(source.read_bytes())
                target=root/'docs/ci-qualification/rig-original-contracts.json';target.parent.mkdir(parents=True)
                target.write_bytes((ROOT/'docs/ci-qualification/rig-original-contracts.json').read_bytes())
                path=root/('crates/lumen/tests/rig/cases/load/search_qps.toml'if changed=='required'else'crates/lumen/tests/rig/config/pins/search_p99.toml')
                path.write_text(path.read_text().replace('required = true','required = false')if changed=='required'else path.read_text().replace('floor = 25.0','floor = 250.0'))
                with self.assertRaises(ValueError):coverage.inventory(root)


if __name__=='__main__':unittest.main()
