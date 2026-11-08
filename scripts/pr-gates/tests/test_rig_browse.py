"""Run only the isolated HTTP-script fixture with a mocked transport and clock."""
import contextlib
import io
import json
from pathlib import Path
import sys
import time
import tomllib
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[3]
PATH=ROOT/'crates/lumen/tests/rig/cases/load/data_table_browse.toml'


def probe(code,base,broken=None,shallow=0.001,deep=0.001):
    requests=[];ids=['doc-%06d'%i for i in range(1000)]
    timings=iter(value for i in range(20)for value in(0.0,deep if i>=17 else shallow))
    class Response:
        def __init__(self,value):self.value=value
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def read(self):return json.dumps(self.value).encode()
    def send(request,timeout):
        if timeout!=30 or not request.full_url.startswith(base+'/collections/browse_docs_42000'):
            raise AssertionError('original timeout or owned endpoint changed')
        data=json.loads(request.data);requests.append((request.method,request.full_url.removeprefix(base),data))
        if request.method=='PUT':return Response({})
        if request.full_url.endswith('/index'):
            if broken=='seed':raise urllib.error.HTTPError(request.full_url,503,'fixture',{},io.BytesIO(b'failed seed'))
            return Response({})
        if data['query']!={'range':{'field':'age','gte':0}}or data['sort']!=[{'field':'age','order':'asc'}]or data['track_total']is not False:
            raise AssertionError('original HTTP query changed')
        if data['limit']==1000:return Response({'hits':[{'external_id':i}for i in(ids[:-1]if broken=='oracle'else ids)]})
        if data['limit']!=50:raise AssertionError('original page size changed')
        start=int(data.get('cursor','0'));page=ids[start:start+50]
        if broken=='order'and start==50:page=list(reversed(page))
        return Response({'hits':[{'external_id':i}for i in page],'cursor':str(start+50)if start+50<1000 else None})
    output=io.StringIO();error=io.StringIO()
    with patch.object(sys,'argv',['fixture','127.0.0.1:55881']),patch.object(time,'time',return_value=42),patch.object(time,'perf_counter',side_effect=lambda:next(timings)),patch.object(urllib.request,'urlopen',side_effect=send),contextlib.redirect_stdout(output),contextlib.redirect_stderr(error):
        try:exec(compile(code,'<isolated-browse-fixture>','exec'),{})
        except BaseException as failure:return requests,output.getvalue(),failure
    return requests,output.getvalue(),None


class RigBrowseTests(unittest.TestCase):
    def code(self):return tomllib.loads(PATH.read_text())['steps'][0]['cmd'][2]

    def test_all_original_rows_fields_order_query_and_page_work_are_retained(self):
        original=json.loads((ROOT/'scripts/pr-gates/tests/fixtures/rig-browse-original.json').read_text())['code']
        old,_,old_error=probe(original,'http://127.0.0.1:7373')
        new,output,new_error=probe(self.code(),'http://127.0.0.1:55881')
        self.assertIsNone(old_error);self.assertIsNone(new_error);self.assertGreaterEqual(float(output),0)
        seed=lambda rows:[item for method,path,payload in rows if path.endswith('/index')for item in payload['items']]
        self.assertEqual(seed(old),seed(new));self.assertEqual(len(seed(new)),2000)
        self.assertEqual([len(p['items'])for _,path,p in new if path.endswith('/index')],[1000,1000])
        self.assertEqual([(m,p,v)for m,p,v in old if not p.endswith('/index')],[(m,p,v)for m,p,v in new if not p.endswith('/index')])
        self.assertEqual(sum(path.endswith('/search')for _,path,_ in new),21)

    def test_missing_oracle_rows_and_changed_cursor_order_fail(self):
        for broken in ('oracle','order'):
            with self.subTest(broken=broken):
                self.assertIsInstance(probe(self.code(),'http://127.0.0.1:55881',broken)[2],SystemExit)

    def test_both_original_depth_bound_terms_remain_enforced(self):
        for shallow,deep,passes in ((0.001,0.049,True),(0.001,0.051,False),(0.004,0.079,True),(0.004,0.081,False)):
            with self.subTest(shallow=shallow,deep=deep):
                error=probe(self.code(),'http://127.0.0.1:55881',shallow=shallow,deep=deep)[2]
                self.assertEqual(error is None,passes)

    def test_seed_failure_stops_before_later_batches_or_search(self):
        requests,_,failure=probe(self.code(),'http://127.0.0.1:55881','seed')
        self.assertIsInstance(failure,urllib.error.HTTPError)
        self.assertEqual(sum(path.endswith('/index')for _,path,_ in requests),1)
        self.assertFalse(any(path.endswith('/search')for _,path,_ in requests))


if __name__=='__main__':unittest.main()
