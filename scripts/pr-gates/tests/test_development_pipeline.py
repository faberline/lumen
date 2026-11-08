"""Consumer admission, real DAG, every expanded failure, and writer boundaries."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[3]
def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
d=module('lumen_development',ROOT/'scripts/issue-ci/development_pipeline.py')
h=module('lumen_handoff',ROOT/'scripts/issue-ci/handoff.py')
config=json.loads((ROOT/'.github/issue-ci-required-gates.json').read_text());workflow=config['development_pipeline']['workflow']


class DevelopmentTests(unittest.TestCase):
    def test_actual_workflow_obeys_all_stage_barriers(self):
        result=d.workflow_contract((ROOT/workflow).read_text(),config)
        self.assertEqual(result['stages']['preflight'],['development-preflight'])
        self.assertEqual(result['stages']['impact'],['inventory'])

    def test_each_later_entry_refuses_missing_unit_dependency(self):
        import yaml
        original=d.yaml_document((ROOT/workflow).read_text())
        for name in ('image','test-client','workspace-contracts','measured-performance','native-contracts'):
            value=copy.deepcopy(original);value['jobs'][name]['needs']=['inventory']
            with self.subTest(job=name),self.assertRaisesRegex(RuntimeError,'earlier stage'):
                d.workflow_contract(yaml.safe_dump(value),config)

    def test_every_expanded_job_is_required_and_every_case_retained(self):
        policy=config['qualification'];self.assertEqual(len(policy['checks']),61)
        self.assertEqual(len(config['workflows'][0]['jobs']),32)
        legacy=json.loads((ROOT/'scripts/issue-ci/tests/fixtures/pre-local-policy.json').read_text())
        self.assertTrue(d.retains_workflows(legacy,config))
        for name in legacy['workflows'][0]['jobs']:self.assertIn(name,config['workflows'][0]['jobs'])
        self.assertEqual(sum(c['kind']=='unit'for c in policy['checks'].values()),31)
        self.assertEqual(sum(c['kind']=='functional_e2e'for c in policy['checks'].values()),17)
        self.assertEqual(sum(c['kind']=='performance'for c in policy['checks'].values()),4)
        for name in config['workflows'][0]['jobs']:
            with self.subTest(job=name):self.assertTrue(any(c['job']==name for c in policy['checks'].values()))

    def test_each_expanded_failure_gets_one_distinct_diagnostic_even_without_logs(self):
        repo='faberline/lumen';head='a'*40;base='b'*40;run=dict(id=7,run_attempt=2,html_url=f'https://github.com/{repo}/actions/runs/7')
        names=config['workflows'][0]['jobs'];comments=[];writes=[]
        jobs=[dict(id=100+i,name=name,status='completed',conclusion='failure',steps=[],html_url=run['html_url']+f'/job/{100+i}')for i,name in enumerate(names)]
        records=d.failure_records(repo,189,head,base,{workflow:(run,jobs)})
        self.assertEqual(len(records),len(names))
        def write(path,method,data):
            self.assertEqual(method,'POST');self.assertEqual(path,f'repos/{repo}/issues/188/comments')
            self.assertIn('No failed-step detail',data['body']);self.assertIn(head,data['body']);self.assertIn(base,data['body'])
            result=dict(id=len(comments)+1,body=data['body'],user=dict(login='github-actions[bot]'));comments.append(result);writes.append(result);return result
        d.report_failures(repo,188,records,comments,write,lambda:None)
        self.assertEqual(len(writes),32)
        d.report_failures(repo,188,records,comments,write,lambda:None)
        self.assertEqual(len(writes),32)

    def test_setup_timeout_and_missing_runner_receipt_keep_metadata_reasons(self):
        repo='faberline/lumen';run=dict(id=7,run_attempt=1,html_url=f'https://github.com/{repo}/actions/runs/7')
        for conclusion in ('failure','timed_out','startup_failure','action_required'):
            job=dict(id=9,name='unit-static batch (01/04)',status='completed',conclusion=conclusion,steps=[dict(name='Check out exact head',conclusion=conclusion)],html_url=run['html_url']+'/job/9')
            row=d.failure_records(repo,189,'a'*40,'b'*40,{workflow:(run,[job])})[0]
            self.assertEqual(row['steps'][0]['name'],'Check out exact head');self.assertEqual(row['conclusion'],conclusion)

    def test_nonpassing_or_missing_jobs_never_supply_success(self):
        names=config['workflows'][0]['jobs'];run=dict(id=1,run_attempt=1,status='completed',conclusion='success',html_url='https://github.com/faberline/lumen/actions/runs/1')
        complete=[dict(name=name,status='completed',conclusion='success',html_url=run['html_url']+f'/job/{i+1}')for i,name in enumerate(names)]
        self.assertEqual(h.evaluate(config['workflows'],{workflow:(run,complete)})[0],'done')
        for index in range(len(complete)):
            for result in ('skipped','cancelled','neutral',None):
                jobs=copy.deepcopy(complete);jobs[index]['conclusion']=result
                with self.subTest(job=names[index],result=result):self.assertEqual(h.evaluate(config['workflows'],{workflow:(run,jobs)})[0],'pending')
            jobs=copy.deepcopy(complete);jobs.pop(index)
            self.assertEqual(h.evaluate(config['workflows'],{workflow:(run,jobs)})[0],'pending')
        self.assertEqual(h.evaluate_policy(config,{workflow:(run,complete)})[0],'pending')

    def test_new_qualification_keeps_unresolved_product_content_blocking(self):
        case=config['qualification']['contracts']['188']
        self.assertEqual(set(case['applicability']),{'unit','functional_e2e','performance'})
        self.assertTrue(all(v['status']=='required'and v['checks']for v in case['applicability'].values()))
        self.assertFalse(any(rule['exempt']for rule in config['qualification']['rules']))
        q=module('lumen_content_contract',ROOT/'scripts/issue-ci/qualification.py')
        policy=copy.deepcopy(config['qualification']);bound=policy['contracts']['188']
        issue=dict(number=188,title='Fixture goal',body='Fixture acceptance',state='open',labels=[dict(name='todo')],
                   url='https://api.github.com/repos/faberline/lumen/issues/188',repository_url='https://api.github.com/repos/faberline/lumen')
        for key in ('title','body'):bound[key+'_sha256']=hashlib.sha256(issue[key].encode()).hexdigest()
        pr=dict(title='Fixture change',body='Closes #188',head=dict(repo=dict(full_name='faberline/lumen')),base=dict(repo=dict(full_name='faberline/lumen')))
        expected={name for v in bound['applicability'].values()for name in v['checks']}
        context,issues,selected=q.issue_contract(policy,pr,issue)
        self.assertEqual(issues,[]);self.assertEqual(selected,expected);self.assertEqual(context['unresolved_count'],0)
        # Exercise actual admission with each unknown, preserving the rejection
        # and the complete required check set after today's source repairs.
        for reason in ('missing real unit coverage','unknown external owner','unknown milestone mapping'):
            bound['unresolved']=[reason]
            context,issues,selected=q.issue_contract(policy,pr,issue)
            self.assertIn('linked issue has unresolved required gates',issues)
            self.assertEqual(context['unresolved_count'],1);self.assertEqual(selected,expected)

    def test_source_decisions_preserve_all_current_runtime_and_review_gates(self):
        proof=json.loads((ROOT/'docs/ci-qualification/content-decisions-188.json').read_text())
        self.assertEqual(proof['schema'],'LUMEN188_SOURCE_DECISIONS_V1')
        self.assertEqual(len(proof['resolved_source_gaps']),3)
        self.assertFalse(proof['recorded_run_overall_acceptance'])
        self.assertFalse(proof['source_proposal_grants_qualification'])
        self.assertFalse(proof['release_or_milestone_acceptance'])
        self.assertTrue(proof['current_head_runtime_checks_remain_required'])
        self.assertEqual((proof['required_jobs'],proof['required_checks'],proof['unit_cases'],proof['unit_batches'],proof['shared_case_budget_seconds'],proof['integration_profiles']),(32,61,31,4,1800,13))
        expected=hashlib.sha256(json.dumps(config['qualification']['checks'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
        self.assertEqual(proof['checks_sha256'],expected)
        self.assertEqual(config['qualification']['contracts']['188']['unresolved'],[])
        self.assertEqual(config['ci_handoff_policy']['qualification'],config['qualification'])
        inventory=ROOT/proof['resolved_source_gaps'][1]['inventory']
        self.assertEqual(hashlib.sha256(inventory.read_bytes()).hexdigest(),proof['resolved_source_gaps'][1]['inventory_sha256'])
        self.assertIn('complete_current_head_and_base_CI',proof['required_final_acceptance'])
        self.assertIn('independent_Codex_review',proof['required_final_acceptance'])

    def test_frozen_workflow_identity_is_real_git_blob(self):
        raw=(ROOT/workflow).read_bytes();expected=hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()
        self.assertEqual(config['qualification']['trusted_workflows'][workflow],dict(id=399074736,definition_sha=expected))

    def test_one_label_writer_has_no_cloud_completion_authority(self):
        writer=d.yaml_document((ROOT/'.github/workflows/issue-ci.yml').read_text())
        self.assertEqual(writer['permissions'],{})
        self.assertEqual(set(writer['jobs']),{'source-pr-links','report'})
        label=writer['jobs']['report'];helper=writer['jobs']['source-pr-links']
        self.assertEqual(label['permissions'],{'actions':'read','contents':'read','pull-requests':'read','issues':'write'})
        self.assertEqual(helper['permissions'],{'actions':'read','contents':'read','pull-requests':'write','issues':'read'})
        self.assertEqual(label['needs'],'source-pr-links');self.assertEqual(label['if'],'${{ always() }}')
        for job in (label,helper):self.assertEqual(job['steps'],[{'uses':'faberline/aw/.github/actions/issue-ci@4223a4534f0197e1ea23004b9388ffd1eea34639','with':{'operation':'development-report'}}])
        self.assertNotIn('pull_request_target',writer['on']);self.assertNotIn('schedule',writer['on'])
        self.assertFalse((ROOT/'.github/workflows/claude-final-review.yml').exists())
        for field in ('native_auto_merge','claude_final_review','issue_recovery','controller_source'):self.assertNotIn(field,config)


if __name__=='__main__':unittest.main()
