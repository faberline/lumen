#!/usr/bin/env python3
"""Run complete local integration rosters; never excuse missing external work."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import tomllib

spec=importlib.util.spec_from_file_location('lumen_contract_units',Path(__file__).with_name('plan.py'))
unit=importlib.util.module_from_spec(spec);spec.loader.exec_module(unit)


def inventory(root,metadata):
    packages=unit.inventory(root,metadata)
    by={p['name']:p for p in metadata['packages']if p['id']in metadata['workspace_members']}
    for package in packages:
        source=by[package['name']]
        declared={t['name']:t for t in tomllib.loads(Path(source['manifest_path']).read_text()).get('test',[])}
        targets=[]
        for target in source['targets']:
            if target['kind']!=['test']:continue
            if target.get('test',True)is not True or declared.get(target['name'],{}).get('harness',True)is not True:
                raise ValueError('disabled integration harness requires reviewed applicability: '+package['name'])
            targets.append(dict(name=target['name'],kind=['test'],**{'required-features':target.get('required-features',[])},harness=True,test=True))
        package['integration_targets']=sorted(targets,key=lambda t:t['name'])
    return packages


def load(root):
    return inventory(root,json.loads(subprocess.check_output(['cargo','metadata','--locked','--no-deps','--format-version','1'],cwd=root,text=True)))


def active_targets(package,profile):
    if profile not in unit.profiles(package):raise ValueError('unknown manifest-derived profile')
    enabled=unit.enabled_features(package,profile)
    return [t for t in package['integration_targets']if set(t['required-features'])<=enabled]


def matrix(packages):
    rows=[]
    for package in packages:
        reached=set()
        for profile in unit.profiles(package):
            targets=active_targets(package,profile);reached.update(t['name']for t in targets)
            if targets:rows.append(dict(package=package['name'],profile=profile))
        if reached!={t['name']for t in package['integration_targets']}:raise ValueError('unreachable integration target')
    if not rows or len(rows)>256:raise ValueError('empty or oversized integration matrix')
    return dict(include=sorted(rows,key=lambda r:(r['profile']!='default',r['package'],r['profile'])))


def compare_inventory(before,after):
    unit.compare_inventory(before,after);current={p['name']:p for p in after}
    for package in before:
        successor=current[package['name']]
        if not {t['name']for t in package['integration_targets']}<={t['name']for t in successor['integration_targets']}:
            raise ValueError('integration target removal requires review')
        for profile in unit.profiles(package):
            if not {t['name']for t in active_targets(package,profile)}<={t['name']for t in active_targets(successor,profile)}:
                raise ValueError('formerly enabled integration target lost')


def command(package,profile,target):
    if target not in active_targets(package,profile):raise ValueError('target outside complete manifest row')
    # External ignored tests must run in their declared GKE/performance job.
    # Unknown ignored work remains a failed applicability gap. Never launch
    # the sixteen-cell release workload on a general PR runner by accident.
    return ['cargo','test','--release','--locked','-p',package['name'],'--test',target['name'],*unit.feature_args(profile),'--','--format','pretty','--color','never','--test-threads','1']


def discover(root,package,profile,target):
    argv=command(package,profile,target);argv[argv.index('--')+1:]=['--list','--format','pretty','--color','never']
    result=unit._run_product(argv,root)
    if result.returncode:raise ValueError('integration discovery failed with exit '+str(result.returncode))
    return unit.listed_unit_tests(result.stdout)


def external_coverage(target,name):
    """Only named existing suites have an external owner; no wildcard waiver."""
    if target!='it':return None
    owned={
        'e2e::overlay::journey':'GKE e2e (overlay) / deploy + test on shared GKE',
        'e2e::operator::journey':'GKE e2e (operator) / deploy + test on shared GKE',
        'e2e::standalone_container::bind':'standalone container e2e',
        'e2e::standalone_container::durable':'standalone container e2e',
    }
    for test in ('index_throughput_floor','match_query_latency_floor','term_query_latency_floor','truncate_docs_cost_is_constant_from_10_to_100k_documents','number_read_costs_on_100k_documents'):
        owned['perf_gate::'+test]='measured performance'
    return owned.get(name)


def ignored_contracts(root):
    path = Path(root)/'docs/ci-qualification/normal-pr-ignored-tests.json'
    data = json.loads(path.read_text())
    if data.get('version') != 1 or not isinstance(data.get('tests'), list):
        raise ValueError('invalid ignored contract inventory')
    seen = set()
    for row in data['tests']:
        key = (row['target'], row['name'])
        if key in seen or row['normal_pr'] not in ('external', 'outside_normal_pr') or not row.get('basis') or not row.get('owner'):
            raise ValueError('duplicate or invalid ignored contract identity')
        seen.add(key)
        source = Path(row['source'])
        if source.is_absolute() or '..' in source.parts or not str(source).startswith('crates/lumen/tests/'):
            raise ValueError('unsafe ignored contract source')
        if hashlib.sha256((Path(root)/source).read_bytes()).hexdigest() != row['source_sha256']:
            raise ValueError('ignored contract source changed; requires independent applicability review: '+row['name'])
    return data['tests']


def ignored_contract(root,target,name):
    return next((row for row in ignored_contracts(root) if row['target']==target and row['name']==name),None)


def validate_execution(stdout,expected,target,root=None):
    blocks=unit._libtest_blocks(stdout,1,pending_ignored=True)
    executed={name for b in blocks for name in b['roster']}
    ignored={name for b in blocks for name in b['ignored_roster']}
    if not executed:raise ValueError('zero executed integration tests')
    if executed & ignored or executed|ignored!=set(expected):raise ValueError('integration execution differs from complete discovered roster')
    root = Path(root) if root is not None else Path(__file__).resolve().parents[2]
    deferred={}
    outside={}
    missing=[]
    for name in sorted(ignored):
        row=ignored_contract(root,target,name)
        if row is None:
            missing.append(name)
        elif row['normal_pr']=='external':
            deferred[name]=row['owner']
        else:
            outside[name]=dict(owner=row['owner'],basis=row['basis'],source=row['source'],source_sha256=row['source_sha256'],result='not_applicable_to_normal_pr_not_executed_not_passed')
    if missing:raise ValueError('ignored tests require reviewed runtime/applicability and actual evidence; not passed: '+json.dumps(missing))
    return dict(tests=blocks,discovered_roster=sorted(expected),local_executed=sorted(executed),external_required=deferred,outside_normal_pr=outside,external_tests_counted_as_local_pass=False,non_applicable_tests_counted_as_pass=False)


def run(root,package,profile,provenance):
    targets=active_targets(package,profile)
    if not targets:raise ValueError('empty integration row')
    errors=[]
    for target in targets:
        record=dict(schema='LUMEN_CONTRACT_EVIDENCE_V1',provenance=provenance,package=package['name'],profile=profile,target=target['name'])
        try:
            before=discover(root,package,profile,target);argv=command(package,profile,target)
            started=time.monotonic();result=unit._run_product(argv,root)
            record.update(command=argv,elapsed_seconds=time.monotonic()-started,exit_code=result.returncode,stdout_sha256=hashlib.sha256(result.stdout.encode()).hexdigest(),stderr_sha256=hashlib.sha256(result.stderr.encode()).hexdigest())
            if result.returncode:raise ValueError('integration command failed with exit '+str(result.returncode))
            proof=validate_execution(result.stdout,before,target['name'],root)
            after=discover(root,package,profile,target)
            if before!=after:raise ValueError('integration discovery changed during execution')
            record.update(proof,result='pass_local_only_external_jobs_still_required'if proof['external_required']else'pass')
        except (ValueError,OSError,subprocess.CalledProcessError)as error:
            errors.append(target['name']+': '+str(error));record.update(result='fail',error=str(error))
        print('LUMEN_CONTRACT_EVIDENCE_V1 '+json.dumps(record,sort_keys=True),flush=True)
    if errors:raise ValueError('\n'.join(errors))


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__,allow_abbrev=False)
    parser.add_argument('action',choices=['matrix','run'])
    for key in ('root','baseline-root'):parser.add_argument('--'+key,type=Path,required=True)
    for key in ('expected-head','expected-base'):parser.add_argument('--'+key,required=True)
    for key in ('package','profile'):parser.add_argument('--'+key)
    args=parser.parse_args(argv);root=args.root.resolve();baseline=args.baseline_root.resolve()
    provenance=unit.verify_identity(root,args.expected_head,args.expected_base,baseline)
    packages=load(root);prior=load(baseline);compare_inventory(prior,packages);planned=matrix(packages)
    unit.verify_identity(root,args.expected_head,args.expected_base,baseline)
    if args.action=='matrix':print(json.dumps(planned,separators=(',',':')));return
    if dict(package=args.package,profile=args.profile)not in planned['include']:raise ValueError('unknown complete integration row')
    provenance.update(repository='faberline/lumen',run_id=os.environ.get('GITHUB_RUN_ID'),run_attempt=os.environ.get('GITHUB_RUN_ATTEMPT'),driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    try:run(root,next(p for p in packages if p['name']==args.package),args.profile,provenance)
    finally:unit.verify_identity(root,args.expected_head,args.expected_base,baseline)


if __name__=='__main__':main()
