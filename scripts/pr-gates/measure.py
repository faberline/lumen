#!/usr/bin/env python3
"""Existing Lumen timing workloads with exact selected and full harness rosters."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re

spec=importlib.util.spec_from_file_location('lumen_measure_units',Path(__file__).with_name('plan.py'))
unit=importlib.util.module_from_spec(spec);spec.loader.exec_module(unit)
COARSE={'perf_gate::'+name for name in ('index_throughput_floor','match_query_latency_floor','term_query_latency_floor','truncate_docs_cost_is_constant_from_10_to_100k_documents','number_read_costs_on_100k_documents')}
CASES=(
    dict(id='coarse',package='lumen',selector=['--test','it'],filters=['perf_gate::','--ignored','--skip','durable_workload'],names=COARSE),
    dict(id='index-latency',package='lumen-index',selector=['--lib'],filters=['timing_and_match_500k_all_docs_match','--ignored'],suffix='::timing_and_match_500k_all_docs_match'),
    dict(id='pagination',package='lumen-index',selector=['--lib'],filters=['deep_pagination_depth_invariance','--ignored'],suffix='::deep_pagination_depth_invariance'),
)


def command(case,mode='run'):
    base=['cargo','test','--release','--locked','-p',case['package'],*case['selector'],'--']
    if mode=='all':return base+['--list','--format','pretty','--color','never']
    if mode=='selected':return base+case['filters']+['--list','--format','pretty','--color','never']
    if mode!='run':raise ValueError('unknown measured command mode')
    return base+case['filters']+['--test-threads','1','--format','pretty','--color','never','--nocapture']


def discovery(root,case,mode):
    result=unit._run_product(command(case,mode),root)
    if result.returncode:raise ValueError('performance discovery failed with exit '+str(result.returncode))
    return unit.listed_unit_tests(result.stdout)


def selected_evidence(stdout,full,selected):
    if not selected or not selected<=full:raise ValueError('empty or foreign selected performance roster')
    starts=[];summaries=[];names=[]
    for line in stdout.splitlines():
        start=unit.RUNNING.fullmatch(line);row=unit.TEST_ROW.fullmatch(line);summary=unit.SUMMARY.match(line)
        if start:starts.append(int(start[1]))
        if row:
            if row[2]!='ok' or row[1]in names:raise ValueError('nonpassing or duplicate performance test')
            names.append(row[1])
        if summary:
            values=dict(zip(('passed','failed','ignored','measured','filtered'),map(int,summary.groups()[1:])))
            if summary[1]!='ok' or any(values[k]for k in ('failed','ignored','measured')):raise ValueError('nonpassing measured counts')
            summaries.append(values)
    if starts!=[len(selected)]or len(summaries)!=1 or set(names)!=selected:raise ValueError('performance execution differs from independently selected roster')
    result=summaries[0]
    if result['passed']!=len(selected)or result['filtered']!=len(full)-len(selected):raise ValueError('incomplete full/selected performance counts')
    return dict(**result,running=len(selected),roster=sorted(selected),unselected_not_counted_as_pass=len(full)-len(selected))


def measurement(case,text):
    if case['id']=='coarse':return dict(workload='five original coarse release gates',thresholds='unchanged Rust assertions')
    if case['id']=='index-latency':
        matches=re.findall(r'total=([0-9.]+)(ns|µs|μs|us|ms|s)',text)
        if len(matches)!=1:raise ValueError('missing or ambiguous 500k latency measurement')
        value,scale=matches[0];milliseconds=float(value)*{'ns':1e-6,'µs':1e-3,'μs':1e-3,'us':1e-3,'ms':1,'s':1000}[scale]
        if not 0<milliseconds<50:raise ValueError('500k latency does not meet original exclusive 50ms bound')
        return dict(documents=500000,milliseconds=milliseconds,milliseconds_exclusive_limit=50)
    matches=re.findall(r'page#1 (\d+)us \| keyset@50k (\d+)us \| offset@50k (\d+)us',text)
    if len(matches)!=1:raise ValueError('missing or ambiguous pagination measurement')
    first,deep,offset=map(int,matches[0])
    if deep>=max(first,1)*20:raise ValueError('deep pagination does not meet original exclusive 20x bound')
    return dict(first_us=first,deep_us=deep,offset_us=offset,ratio_exclusive_limit=20)


def run(root,provenance):
    errors=[]
    for case in CASES:
        record=dict(schema='LUMEN_PERFORMANCE_EVIDENCE_V1',case_id=case['id'],provenance=provenance)
        try:
            full=discovery(root,case,'all');selected=discovery(root,case,'selected')
            expected=case.get('names')or{name for name in full if name.endswith(case['suffix'])}
            if selected!=expected or len(selected)!=(5 if case['id']=='coarse'else 1):raise ValueError('missing or added workload requires reviewed plan')
            argv=command(case);result=unit._run_product(argv,root)
            record.update(command=argv,exit_code=result.returncode,stdout_sha256=hashlib.sha256(result.stdout.encode()).hexdigest(),stderr_sha256=hashlib.sha256(result.stderr.encode()).hexdigest())
            if result.returncode:raise ValueError('original measured workload failed with exit '+str(result.returncode))
            record['tests']=selected_evidence(result.stdout,full,selected)
            record['measurement']=measurement(case,result.stdout+'\n'+result.stderr)
            if discovery(root,case,'all')!=full or discovery(root,case,'selected')!=selected:raise ValueError('measured roster changed during execution')
            record.update(result='pass')
        except (ValueError,OSError)as error:
            errors.append(case['id']+': '+str(error));record.update(result='fail',error=str(error))
        print('LUMEN_PERFORMANCE_EVIDENCE_V1 '+json.dumps(record,sort_keys=True),flush=True)
    if errors:raise ValueError('\n'.join(errors))


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__,allow_abbrev=False)
    for key in ('root','baseline-root'):parser.add_argument('--'+key,type=Path,required=True)
    for key in ('expected-head','expected-base'):parser.add_argument('--'+key,required=True)
    args=parser.parse_args(argv);root=args.root.resolve();baseline=args.baseline_root.resolve()
    identity=unit.verify_identity(root,args.expected_head,args.expected_base,baseline)
    identity.update(repo='faberline/lumen',run_id=os.environ.get('GITHUB_RUN_ID'),run_attempt=os.environ.get('GITHUB_RUN_ATTEMPT'),driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    try:run(root,identity)
    finally:unit.verify_identity(root,args.expected_head,args.expected_base,baseline)


if __name__=='__main__':main()
