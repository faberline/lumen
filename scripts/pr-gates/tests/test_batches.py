"""Pure batch controls. No Cargo, product, container or network execution."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import signal
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('lumen_batches_test', Path(__file__).parents[1]/'batches.py')
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)
HEAD, BASE = 'a'*40, 'b'*40


def rows(count=2):
    return [dict(package='crate-'+str(i), profile='default') for i in range(count)]


class LayoutTests(unittest.TestCase):
    def test_all_original_rows_remain_once_in_four_lumen_batches(self):
        fixture=json.loads((Path(__file__).parent/'fixtures/lumen187-unit-rows.json').read_text())
        self.assertTrue(fixture['definition_only'])
        original=fixture['rows'];self.assertEqual(len(original),31)
        plan=b.build(original,4)
        actual=[row for batch in plan['batches'] for row in batch['cases']]
        self.assertEqual(len(actual),31)
        self.assertEqual({(r['package'],r['profile']) for r in actual},
                         {(r['package'],r['profile']) for r in original})
        self.assertEqual(len({r['case_id'] for r in actual}),31)
        self.assertTrue(all(batch['cases'] for batch in plan['batches']))
        self.assertEqual(plan,b.build(list(reversed(original)),4))

    def test_logical_rows_can_exceed_physical_matrix_limit_without_loss(self):
        plan=b.build(rows(257),4)
        self.assertEqual(sum(len(batch['cases']) for batch in plan['batches']),257)
        self.assertEqual(len(plan['batches']),4)

    def test_unknown_profiles_duplicate_rows_and_empty_batches_refuse(self):
        for values,count in [([],1),(rows(),0),(rows(),3),(rows(),True),
                             ([rows()[0],rows()[0]],1),
                             ([dict(package='a',profile='unknown')],1)]:
            with self.subTest(values=values,count=count),self.assertRaises(ValueError):
                b.build(values,count)

    def test_batch_setting_is_app_owned_and_rejects_unknown_values(self):
        for value in [0,True,257,'4',-1]:
            with self.subTest(value=value),self.assertRaises(ValueError):
                b.settings(dict(version=1,unit_batches=value))
        self.assertEqual(b.settings(dict(version=1,unit_batches=4)),4)
        with self.assertRaises(ValueError):b.settings(dict(version=True,unit_batches=4))
        with self.assertRaises(ValueError):b.settings(dict(version=1,unit_batches=4,extra=True))


class ExecutionTests(unittest.TestCase):
    def run_batch(self,values=None,invoke=None,clock=lambda:0,verify=lambda *args:None):
        plan=b.build(rows() if values is None else values,1)
        output=io.StringIO()
        with contextlib.redirect_stdout(output):
            try:
                result=b.execute(plan,'01/01',Path('/candidate'),Path('/baseline'),HEAD,BASE,
                                 invoke=invoke or (lambda *args,**kwargs:SimpleNamespace(returncode=0)),
                                 clock=clock,verify=verify)
            except ValueError:
                result=None
        receipts=[json.loads(line.split('LUMEN_UNIT_BATCH_V1 ',1)[1])
                  for line in output.getvalue().splitlines() if line.startswith('LUMEN_UNIT_BATCH_V1 ')]
        return result,receipts[0]

    def test_unit_failure_keeps_clippy_and_every_later_case(self):
        calls=[]
        def invoke(argv,**kwargs):
            calls.append(argv);return SimpleNamespace(returncode=1 if len(calls)==1 else 0)
        result,receipt=self.run_batch(invoke=invoke)
        self.assertIsNone(result)
        self.assertEqual([argv[-1] for argv in calls],['unit','clippy','unit','clippy'])
        self.assertEqual([r['result'] for r in receipt['results']],['fail','pass'])
        self.assertEqual(receipt['expected'],[r['case_id'] for r in receipt['results']])
        self.assertEqual((receipt['head'],receipt['base']),(HEAD,BASE))

    def test_both_failure_causes_are_retained_for_the_same_case(self):
        result,receipt=self.run_batch(values=rows(1),invoke=lambda *args,**kwargs:SimpleNamespace(returncode=7))
        self.assertIsNone(result)
        self.assertEqual([r['kind'] for r in receipt['results'][0]['errors']],['unit','clippy'])

    def test_unit_and_clippy_share_the_original_case_time_budget(self):
        deadlines=[];clock=iter([0,300,800])
        def invoke(argv,**kwargs):
            deadlines.append(kwargs['timeout']);return SimpleNamespace(returncode=0)
        result,_=self.run_batch(values=rows(1),invoke=invoke,clock=lambda:next(clock))
        self.assertIsNotNone(result)
        self.assertEqual(deadlines,[1500,1000])

    def test_timed_out_case_fails_without_stopping_other_rows(self):
        calls=[];clock=iter([0,0,1801,1802,1802,1803])
        def invoke(argv,**kwargs):
            calls.append(argv)
            if len(calls)==1:raise subprocess.TimeoutExpired(argv,kwargs['timeout'])
            return SimpleNamespace(returncode=0)
        result,receipt=self.run_batch(invoke=invoke,clock=lambda:next(clock))
        self.assertIsNone(result);self.assertEqual(len(calls),3)
        self.assertEqual([r['result'] for r in receipt['results']],['fail','pass'])
        self.assertIn('budget exhausted',receipt['results'][0]['errors'][1]['reason'])

    def test_source_drift_after_passing_commands_is_still_failure(self):
        with patch.object(b.unit,'verify_identity'):
            verify=iter([None,ValueError('source changed')])
            def check(*args):
                value=next(verify)
                if value:raise value
            result,receipt=self.run_batch(values=rows(1),verify=check)
        self.assertIsNone(result)
        self.assertEqual(receipt['results'][0]['errors'],[dict(reason='source changed')])

    def test_invalid_source_does_not_launch_any_product_command(self):
        calls=[]
        def check(*args):raise ValueError('wrong baseline')
        _,receipt=self.run_batch(verify=check,invoke=lambda *args,**kwargs:calls.append(args))
        self.assertEqual(calls,[])
        self.assertTrue(all(row['result']=='fail' for row in receipt['results']))

    def test_timeout_terminates_the_isolated_cargo_process_group(self):
        child=SimpleNamespace(pid=123,wait=unittest.mock.Mock(side_effect=[subprocess.TimeoutExpired(['python'],1),-9]))
        with patch.object(b.subprocess,'Popen',return_value=child) as start,patch.object(b.os,'killpg') as kill:
            with self.assertRaises(subprocess.TimeoutExpired):b.invoke_command(['python'],Path('/tmp'),1)
        self.assertTrue(start.call_args.kwargs['start_new_session'])
        kill.assert_called_once_with(123,signal.SIGKILL)


if __name__=='__main__':unittest.main()
