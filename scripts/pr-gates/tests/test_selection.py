"""Initial policy proposals run all tests and grant no adoption authority."""
import copy
import importlib.util
import json
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[3]
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
s=load('lumen_selection_controls',ROOT/'scripts/pr-gates/selection.py')
shared=load('lumen_pinned_test_plan',ROOT/'scripts/issue-ci/test_plan.py')
development=load('lumen_pinned_development',ROOT/'scripts/issue-ci/development_pipeline.py')
config=json.loads((ROOT/'.github/issue-ci-required-gates.json').read_text())
prior=json.loads((ROOT/'scripts/issue-ci/tests/fixtures/pre-local-policy.json').read_text())
issue=json.loads((ROOT/'scripts/pr-gates/tests/fixtures/development-issue-188.json').read_text())
HEAD='a'*40;BASE='b'*40;REPO='faberline/lumen'


def pull():return dict(number=189,title='Adopt pipeline',body='Closes #188\n',draft=False,state='open',head=dict(sha=HEAD,repo=dict(full_name=REPO)),base=dict(sha=BASE,repo=dict(full_name=REPO)),changed_files=1)


class SelectionTests(unittest.TestCase):
    def invoke(self,p=None,old=None,new=None,target=None,files=None):
        return s.bootstrap(shared,prior if old is None else old,config if new is None else new,REPO,HEAD,BASE,pull()if p is None else p,[dict(filename='scripts/pr-gates/selection.py',status='modified')]if files is None else files,issue if target is None else target,development)

    def test_bootstrap_plans_all_cases_but_does_not_qualify(self):
        value=self.invoke()
        self.assertEqual(len(value['test_plan']['selected']),61)
        self.assertEqual(set(value['test_plan']['selected']),set(config['qualification']['checks']))
        self.assertEqual(value['qualification'],'not_evaluated');self.assertTrue(value['bootstrap_execution_only'])
        self.assertTrue(value['pending_protected_review'])
        self.assertIn('gate infrastructure change requires reviewed policy update: scripts/pr-gates/selection.py',value['pending_applicability'])
        unresolved=copy.deepcopy(config)
        unresolved['qualification']['contracts']['188']['unresolved']=['unreviewed new content requirement']
        pending=self.invoke(new=unresolved)
        self.assertIn('linked issue has unresolved required gates',pending['pending_applicability'])
        self.assertEqual(pending['test_plan']['selected'],value['test_plan']['selected'])
        self.assertTrue(pending['bootstrap_execution_only']);self.assertEqual(pending['qualification'],'not_evaluated')

    def test_bootstrap_preserves_prior_cloud_gate_and_refuses_new_unknown_old_gate(self):
        old=copy.deepcopy(prior);old['workflows'][0]['jobs'].append('unaccounted required old workload')
        with self.assertRaisesRegex(ValueError,'lost existing'):self.invoke(old=old)

    def test_cannot_rebootstrap_an_already_reviewed_policy(self):
        with self.assertRaisesRegex(ValueError,'existing reviewed'):self.invoke(old=config)

    def test_wrong_contract_stale_head_base_draft_and_foreign_pr_refuse(self):
        bad=copy.deepcopy(issue);bad['body']+=' changed'
        with self.assertRaises(ValueError):self.invoke(target=bad)
        for field,value in [('head','c'*40),('base','c'*40)]:
            p=pull();p[field]['sha']=value
            with self.subTest(field=field),self.assertRaisesRegex(ValueError,'PR moved'):self.invoke(p=p)
        p=pull();p['draft']=True
        with self.assertRaisesRegex(ValueError,'draft'):self.invoke(p=p)
        p=pull();p['head']['repo']['full_name']='fork/lumen'
        with self.assertRaisesRegex(ValueError,'foreign'):self.invoke(p=p)

    def test_incomplete_or_capped_changed_paths_refuse(self):
        with self.assertRaises(ValueError):self.invoke(files=[])
        p=pull();p['changed_files']=3000
        with self.assertRaises(ValueError):self.invoke(p=p)

    def test_new_unit_case_or_wrong_batch_cannot_escape_policy(self):
        name=next(name for name,c in config['qualification']['checks'].items()if c['kind']=='unit')
        job=config['qualification']['checks'][name]['job'];batch=job.removeprefix('unit-static batch (').removesuffix(')')
        with self.assertRaisesRegex(ValueError,'unit cases'):s.inventory_contract(config,dict(batches=[dict(batch=batch,cases=[dict(case_id=name)])]),[])

    def test_unregistered_issue_runs_full_roster_but_remains_unqualified(self):
        p=pull();p['body']='Closes #7\n';p['changed_files']=2
        target=dict(number=7,state='open',title='Fleet change',body='Keep all behavior',labels=[],
                    url=f'https://api.github.com/repos/{REPO}/issues/7',
                    repository_url=f'https://api.github.com/repos/{REPO}')
        changed=[dict(filename='Cargo.lock',status='modified'),dict(filename='crates/lumen-operator/src/lib.rs',status='modified')]
        defs={(path,ref):{'sha':item['definition_sha']} for path,item in config['qualification']['trusted_workflows'].items() for ref in (HEAD,BASE)}
        value=s.build(shared,config,'diff',REPO,HEAD,BASE,p,changed,target,config,fallback=shared,definitions=defs)
        self.assertEqual(set(value['test_plan']['selected']),set(config['qualification']['checks']))
        self.assertEqual(value['qualification'],'not_evaluated')
        self.assertIn('linked issue has no reviewed contract',value['pending_applicability'])
        self.assertIn('gate infrastructure change requires reviewed policy update: Cargo.lock',value['pending_applicability'])
        with self.assertRaisesRegex(ValueError,'no reviewed contract'):
            shared.select(config,'diff',REPO,HEAD,BASE,pull=p,files=changed,issue=target)
        defs[next(iter(defs))]={'sha':'f'*40}
        with self.assertRaisesRegex(ValueError,'untrusted head/base'):
            s.build(shared,config,'diff',REPO,HEAD,BASE,p,changed,target,config,fallback=shared,definitions=defs)

    def test_execution_planner_is_exact_reviewed_aw_source(self):
        loaded=s.execution_planner(ROOT)
        self.assertTrue(callable(loaded.execute_unregistered_issue))


if __name__=='__main__':unittest.main()
