"""Valid HTTP transport must retain every original fixture item and failure."""
import json
from pathlib import Path
import sys
import time
import tomllib
import unittest
import urllib.request
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[3]
FIXTURE=json.loads((ROOT/'scripts/pr-gates/tests/fixtures/native-seed-workloads.json').read_text())


def probe(code,status=200):
    requests=[]
    class Response:
        def __enter__(self):return self
        def __exit__(self,*args):pass
    Response.status=status
    def send(request,timeout):
        if timeout!=30:raise AssertionError('original HTTP deadline changed')
        requests.append(json.loads(request.data)['items'])
        return Response()
    exit_code=None
    with patch.object(sys,'argv',['fixture','127.0.0.1:7373','users','5000']),patch.object(time,'time_ns',return_value=42),patch.object(urllib.request,'urlopen',side_effect=send):
        try:exec(compile(code,'<isolated-seed-fixture>','exec'),{})
        except SystemExit as error:exit_code=error.code
    return requests,exit_code


def current(row):
    scenario=tomllib.loads((ROOT/row['path']).read_text())
    return next(step['cmd'][2]for step in scenario['steps']if step['name']==row['step'])


class NativeSeedTests(unittest.TestCase):
    def test_transport_retains_exact_all_original_items_order_and_workload(self):
        self.assertEqual(len(FIXTURE['steps']),10)
        for row in FIXTURE['steps']:
            with self.subTest(path=row['path'],step=row['step']):
                before,old_exit=probe(row['original_code']);after,new_exit=probe(current(row))
                self.assertIn(old_exit,(None,0));self.assertIn(new_exit,(None,0))
                self.assertEqual([len(batch)for batch in before],row['original_batches'])
                self.assertEqual(sum(map(len,after)),row['items'])
                self.assertTrue(all(0<len(batch)<=1000 for batch in after))
                self.assertEqual([item for batch in before for item in batch],[item for batch in after for item in batch])

    def test_failed_batch_is_not_swallowed_and_later_batches_do_not_run(self):
        for row in FIXTURE['steps']:
            with self.subTest(path=row['path'],step=row['step']):
                requests,exit_code=probe(current(row),status=503)
                self.assertEqual(exit_code,2);self.assertEqual(len(requests),1)


if __name__=='__main__':unittest.main()
