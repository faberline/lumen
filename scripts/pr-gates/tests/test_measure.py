"""Performance rosters and original exclusive bounds cannot be weakened."""
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('lumen_measure_controls',ROOT/'scripts/pr-gates/measure.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def output(names,filtered):
    return f'running {len(names)} tests\n'+''.join(f'test {name} ... ok\n'for name in names)+f'\ntest result: ok. {len(names)} passed; 0 failed; 0 ignored; 0 measured; {filtered} filtered out; finished in 0.01s\n'


class MeasureTests(unittest.TestCase):
    def test_native_libtest_selection_counts_match_real_harness(self):
        with tempfile.TemporaryDirectory(prefix='lumen-libtest-control-')as directory:
            executable=Path(directory)/'rosters'
            subprocess.run(['rustc','--test','--edition=2021',str(ROOT/'scripts/pr-gates/tests/fixtures/selected-rosters.rs'),'-o',str(executable)],check=True,capture_output=True,text=True)
            def invoke(*args):return subprocess.run([str(executable),*args],check=True,capture_output=True,text=True).stdout
            full=m.unit.listed_unit_tests(invoke('--list','--format','pretty'))
            selected=m.unit.listed_unit_tests(invoke('perf_gate::','--ignored','--skip','durable_workload','--list','--format','pretty'))
            actual=invoke('perf_gate::','--ignored','--skip','durable_workload','--test-threads','1','--format','pretty','--nocapture')
            result=m.selected_evidence(actual,full,selected)
            self.assertEqual(result['passed'],2);self.assertEqual(result['filtered'],3)
            self.assertEqual(result['roster'],['perf_gate::first','perf_gate::second'])

    def test_selected_and_full_inventory_are_both_checked(self):
        value=m.selected_evidence(output(['a'],2),{'a','b','c'},{'a'})
        self.assertEqual(value['roster'],['a']);self.assertEqual(value['unselected_not_counted_as_pass'],2)
        for bad in (output(['a'],1),output(['a','b'],1),output([],3)):
            with self.subTest(stdout=bad),self.assertRaises(ValueError):m.selected_evidence(bad,{'a','b','c'},{'a'})

    def test_no_ignored_duplicate_or_multiple_harness_success(self):
        good=output(['a'],0)
        for bad in (good.replace('0 ignored','1 ignored'),good+good,output(['a','a'],0)):
            with self.subTest(stdout=bad),self.assertRaises(ValueError):m.selected_evidence(bad,{'a'},{'a'})

    def test_original_500k_latency_exclusive_limit(self):
        case=m.CASES[1];value=m.measurement(case,'total=49.99ms')
        self.assertEqual(value['milliseconds_exclusive_limit'],50)
        for bad in ('total=50ms','total=0ms','total=51ms','','total=1ms total=2ms'):
            with self.subTest(value=bad),self.assertRaises(ValueError):m.measurement(case,bad)

    def test_original_pagination_exclusive_20x_limit(self):
        case=m.CASES[2];value=m.measurement(case,'page#1 2us | keyset@50k 39us | offset@50k 100us')
        self.assertEqual(value['ratio_exclusive_limit'],20)
        for deep in (40,41):
            with self.subTest(deep=deep),self.assertRaises(ValueError):m.measurement(case,f'page#1 2us | keyset@50k {deep}us | offset@50k 100us')

    def test_existing_coarse_filter_and_release_mode_are_retained(self):
        argv=m.command(m.CASES[0]);self.assertEqual(argv[:8],['cargo','test','--release','--locked','-p','lumen','--test','it'])
        self.assertEqual(argv[9:14],['perf_gate::','--ignored','--skip','durable_workload','--test-threads'])
        self.assertEqual(len(m.CASES[0]['names']),5)


if __name__=='__main__':unittest.main()
