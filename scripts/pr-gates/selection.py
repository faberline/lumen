"""Lumen test execution plan from the pinned AW selector. No lifecycle writes."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess

AW_EXECUTION_SOURCE = 'b4660d6bb0ebe65f5d9c0bae536d83ae96994970'
AW_EXECUTION_SHA256 = '023f387b675076c4293a8d1374b991e24fa6051e6db54e537d554ed7e0e310d9'
AW_QUALIFICATION_SHA256 = '67412fbcd054c6a14efe1c196059933a6309d657fe8b3e07eeccc6ffb208401d'


def api(path):
    return json.loads(subprocess.check_output(['gh','api',path],text=True))


def files(repo, number):
    rows=[]
    for page in range(1,31):
        batch=api(f'repos/{repo}/pulls/{number}/files?per_page=100&page={page}')
        if not isinstance(batch,list):raise ValueError('incomplete PR files')
        rows.extend(batch)
        if len(batch)<100:return rows
    raise ValueError('capped PR files cannot define coverage')


def planner(path):
    spec=importlib.util.spec_from_file_location('lumen_pinned_aw_test_plan',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def execution_planner(root):
    directory = root / 'scripts/issue-ci'
    for name, expected in (('test_plan.py', AW_EXECUTION_SHA256), ('qualification.py', AW_QUALIFICATION_SHA256)):
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != expected:
            raise ValueError('execution-only planner differs from reviewed AW source ' + AW_EXECUTION_SOURCE)
    return planner(directory / 'test_plan.py')


def workflow_definitions(root, policy, head, base):
    result = {}
    for path in policy['qualification']['trusted_workflows']:
        for ref in (head, base):
            sha = subprocess.check_output(['git', '-C', str(root), 'rev-parse', f'{ref}:{path}'], text=True).strip()
            if not re.fullmatch('[0-9a-f]{40}', sha):
                raise ValueError('workflow blob identity unavailable')
            result[(path, ref)] = {'sha':sha}
    return result


def pull_identity(pull):
    return dict(number=pull.get('number'),state=pull.get('state'),draft=pull.get('draft'),
                title=pull.get('title'),body=pull.get('body'),
                **{side:dict(sha=pull.get(side,{}).get('sha'),ref=pull.get(side,{}).get('ref'),
                            repo=pull.get(side,{}).get('repo',{}).get('full_name')) for side in ('head','base')})


def issue_identity(issue):
    labels = issue.get('labels')
    if not isinstance(labels, list) or any(not isinstance(row, dict) or not isinstance(row.get('name'), str) for row in labels):
        raise ValueError('incomplete issue labels')
    return dict(number=issue.get('number'), state=issue.get('state'), title=issue.get('title'),
                body=issue.get('body'), url=issue.get('url'), repository_url=issue.get('repository_url'),
                updated_at=issue.get('updated_at'), labels=sorted(row['name'] for row in labels))


def contract_proposal(shared, policy, proposed, pull, issue):
    if not isinstance(proposed,dict):raise ValueError('current issue contract needs a protected proposal')
    q=shared.qualification();candidate=q.validate(proposed);prior=q.validate(policy)
    if (candidate['checks']!=prior['checks'] or candidate['always']!=prior['always']
            or candidate['rules']!=prior['rules']):
        raise ValueError('contract proposal cannot change existing gate selection or case definitions')
    _,issues,_=q.issue_contract(candidate,pull,issue)
    if issues:raise ValueError('; '.join(issues))
    number=str(q.linked_issue(pull));case=candidate['contracts'][number];old=prior['contracts'].get(number)
    if old is not None:
        semantics=lambda value:{k:v for k,v in value.items() if k not in ('title_sha256','body_sha256')}
        if semantics(case)!=semantics(old):raise ValueError('contract rebind cannot remove unresolved or required gates')
    else:
        for kind in q.CATEGORIES:
            decision=case['applicability'][kind]
            expected={name for name,check in prior['checks'].items() if check['kind']==kind}
            if decision['status']!='required' or set(decision['checks'])!=expected:
                raise ValueError('new contract proposal must retain every existing test category and case')
    return 'source issue contract proposal requires independent protected review'


def build(shared, policy, mode, repo, head, base, pull=None, changed=None, issue=None, proposed=None,
          fallback=None, definitions=None):
    pending=[]
    pending_applicability=[]
    if mode=='diff':
        if (not isinstance(pull,dict) or pull.get('state')!='open' or pull.get('draft') is not False
                or pull.get('head',{}).get('sha')!=head or pull.get('base',{}).get('sha')!=base
                or any(pull.get(side,{}).get('repo',{}).get('full_name')!=repo for side in ('head','base'))):
            raise ValueError('PR moved or is foreign, closed or draft during planning')
        scope=shared.qualification().select(policy,pull,changed,issue)
        # A protected policy proposal must run every old gate before independent
        # review. This receipt only plans execution and cannot approve adoption.
        pending=[reason for reason in scope['issues'] if reason.startswith('gate infrastructure change requires reviewed policy update:')]
        other=[reason for reason in scope['issues'] if reason not in pending]
        rebinding={'linked issue title differs from reviewed contract','linked issue body differs from reviewed contract',
                   'linked issue has no reviewed contract'}
        if other == ['linked issue has no reviewed contract'] and proposed == policy and fallback is not None:
            execution = fallback.execute_unregistered_issue(policy, repo, head, base, pull=pull,
                                                              files=changed, issue=issue, definitions=definitions)
            pending_applicability = execution['unresolved']
            result = execution['plan']
            other = []
        elif other and set(other)<=rebinding:
            pending.append(contract_proposal(shared,policy,proposed,pull,issue))
            other=[]
        if other:raise ValueError('; '.join(other))
        if pending_applicability:
            pass
        elif pending:
            result=shared.select(policy,'all',repo,head,base)
        else:
            result=shared.select(policy,'diff',repo,head,base,pull=pull,files=changed,issue=issue)
    elif mode=='all':
        result=shared.select(policy,'all',repo,head,base)
    else:
        raise ValueError('unknown selection mode')
    # Current reviewed Lumen policy requires every gate for every changed path.
    # Keep the complete batches until a separate reviewed policy changes that.
    if set(result['selected'])!=set(policy['qualification']['checks']):
        raise ValueError('Lumen full fallback policy and complete batch roster differ')
    return dict(schema='LUMEN_TEST_SELECTION_V1',requested_mode=mode,test_plan=result,
                pending_protected_review=pending,pending_applicability=pending_applicability,
                qualification='not_evaluated')


def bootstrap(shared, baseline, proposed, repo, head, base, pull, changed, issue, development):
    # An initial candidate may plan every proposed test, but it cannot grant
    # itself trusted policy, done, review eligibility or merge authority.
    if (pull.get('state')!='open' or pull.get('draft') is not False
            or pull.get('head',{}).get('sha')!=head or pull.get('base',{}).get('sha')!=base
            or any(pull.get(side,{}).get('repo',{}).get('full_name')!=repo for side in ('head','base'))):
        raise ValueError('PR moved or is foreign, closed or draft during bootstrap planning')
    q=shared.qualification();q.validate(proposed)
    if 'qualification' in baseline:raise ValueError('bootstrap cannot replace an existing reviewed policy')
    if not development.retains_workflows(baseline,proposed):raise ValueError('bootstrap lost existing required workflows/jobs')
    scope=q.select(proposed,pull,changed,issue)
    allowed=lambda reason: reason.startswith('gate infrastructure change requires reviewed policy update:') or reason=='linked issue has unresolved required gates'
    unknown=[reason for reason in scope['issues']if not allowed(reason)]
    if unknown:raise ValueError('; '.join(unknown))
    result=shared.select(proposed,'all',repo,head,base)
    return dict(schema='LUMEN_TEST_SELECTION_V1',requested_mode='diff',test_plan=result,changed_paths=scope['paths'],pending_protected_review=['initial protected policy requires independent source review'],pending_applicability=scope['issues'],qualification='not_evaluated',bootstrap_execution_only=True)


def inventory_contract(config,unit_plan,functional_rows):
    checks=config['qualification']['checks']
    actual={row['case_id']:'unit-static batch ('+batch['batch']+')'for batch in unit_plan['batches']for row in batch['cases']}
    expected={name:c['job']for name,c in checks.items()if c['kind']=='unit'}
    if not actual or actual!=expected:raise ValueError('manifest unit cases or batch identities differ from proposed reviewed checks')
    actual={f'integration contracts ({row["package"]}, {row["profile"]})'for row in functional_rows}
    expected={c['job']for c in checks.values()if c['job'].startswith('integration contracts (')}
    if not actual or actual!=expected:raise ValueError('manifest integration rows differ from proposed reviewed checks')


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__,allow_abbrev=False)
    for key in ('root','baseline-root','output'):parser.add_argument('--'+key,type=Path,required=True)
    for key in ('expected-head','expected-base'):parser.add_argument('--'+key,required=True)
    args=parser.parse_args(argv)
    if any(not re.fullmatch('[0-9a-f]{40}',value) for value in (args.expected_head,args.expected_base)):
        raise ValueError('exact frozen commits required')
    # Reuse Lumen's existing clean checkout/ancestry enforcer.
    spec=importlib.util.spec_from_file_location('lumen_selection_identity',Path(__file__).with_name('plan.py'))
    unit=importlib.util.module_from_spec(spec);spec.loader.exec_module(unit)
    unit.verify_identity(args.root.resolve(),args.expected_head,args.expected_base,args.baseline_root.resolve())
    supplied=Path(os.environ['AW_ASSERTION_LIBRARY']).resolve()
    if supplied.name!='assertion_record.py':raise ValueError('pinned AW action library required')
    shared=planner(supplied.with_name('test_plan.py'))
    policy=json.loads((args.baseline_root/'.github/issue-ci-required-gates.json').read_text())
    repo=os.environ['GITHUB_REPOSITORY']
    if repo!='faberline/lumen':raise ValueError('foreign Lumen test-plan repository')
    event=os.environ['GITHUB_EVENT_NAME'];pull=changed=issue=None
    if event=='pull_request':
        payload=json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text())
        number=payload['number'];pull=api(f'repos/{repo}/pulls/{number}')
        if type(number) is not int or pull.get('number') != number:
            raise ValueError('event and live PR identities differ')
        changed=files(repo,number)
        linked=shared.qualification().linked_issue(pull)
        if linked is None:raise ValueError('canonical source issue required')
        issue=api(f'repos/{repo}/issues/{linked}');mode='diff'
    else:
        raise ValueError('only a normal development PR can plan tests')
    proposed=json.loads((args.root/'.github/issue-ci-required-gates.json').read_text())
    batches=planner(Path(__file__).with_name('batches.py'))
    contracts=planner(Path(__file__).with_name('contracts.py'))
    packages=unit.load(args.root)
    count=batches.settings(json.loads((args.root/'.github/development-test-batches.json').read_text()))
    inventory_contract(proposed,batches.build(unit.matrix_rows(packages),count),contracts.matrix(contracts.load(args.root))['include'])
    if 'qualification' not in policy:
        development=planner(supplied.with_name('development_pipeline.py'))
        result=bootstrap(shared,policy,proposed,repo,args.expected_head,args.expected_base,pull,changed,issue,development)
    else:
        scope = shared.qualification().select(policy, pull, changed, issue)
        needs_fallback = ('linked issue has no reviewed contract' in scope['issues'] and proposed == policy)
        fallback = execution_planner(args.root) if needs_fallback else None
        definitions = workflow_definitions(args.root, policy, args.expected_head, args.expected_base) if needs_fallback else None
        result=build(shared,policy,mode,repo,args.expected_head,args.expected_base,pull,changed,issue,proposed,
                     fallback=fallback,definitions=definitions)
    if pull is not None:
        fresh=api(f'repos/{repo}/pulls/{pull["number"]}')
        if pull_identity(fresh)!=pull_identity(pull):
            raise ValueError('PR changed during complete selection')
        if issue_identity(api(f'repos/{repo}/issues/{issue["number"]}')) != issue_identity(issue):
            raise ValueError('issue changed during complete selection')
    unit.verify_identity(args.root.resolve(),args.expected_head,args.expected_base,args.baseline_root.resolve())
    encoded=json.dumps(result,sort_keys=True)
    args.output.write_text(encoded+'\n')
    print('LUMEN_TEST_SELECTION_V1 '+encoded)


if __name__=='__main__':main()
