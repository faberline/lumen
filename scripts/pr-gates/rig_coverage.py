"""Retain every original required Rig workload and require real native receipts."""
import hashlib
import json
import math
import os
from pathlib import Path
import tomllib


def inventory(root):
    root=Path(root)
    frozen=json.loads((root/'docs/ci-qualification/rig-original-contracts.json').read_text())
    if frozen.get('schema')!='LUMEN_ORIGINAL_REQUIRED_RIG_V1':
        raise ValueError('invalid original Rig workload inventory')
    directory=root/'crates/lumen/tests/rig'
    cases={}
    for path in sorted((directory/'cases').rglob('*.toml')):
        value=tomllib.loads(path.read_text())
        if value.get('record',{}).get('required')is not True:continue
        name=path.relative_to(directory/'cases').with_suffix('').as_posix()
        record=value['record']
        if record.get('expected')!='pass'or name!=record['dimension']+'/'+record['case']:
            raise ValueError('invalid required Rig scenario identity: '+name)
        cases[name]=dict(value,path=str(path.relative_to(root)),source_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    for name,prior in frozen['cases'].items():
        current=cases.get(name)
        if (current is None or current['record']!=prior['record']
                or [s['name']for s in current['steps']]!=prior['steps']
                or current.get('load')!=prior['load']or current.get('limits',{})!=prior['limits']):
            raise ValueError('original required Rig workload changed: '+name)
    pins=[]
    for path in sorted((directory/'config/pins').rglob('*.toml')):
        pins+=tomllib.loads(path.read_text()).get('pins',[])
    for pin in frozen['pins']:
        if pin not in pins:raise ValueError('original Rig pin changed: '+str(pin))
    return cases,pins


def validate(root,measurements):
    cases,pins=inventory(root)
    scenarios=[r for r in measurements if r.get('kind')=='scenario']
    observed={r['name']for r in scenarios}
    missing=set(cases)-observed
    if missing:raise ValueError('missing required Rig scenarios: '+', '.join(sorted(missing)))
    if observed-set(cases):raise ValueError('unknown Rig scenario receipt')
    loads=[r for r in measurements if r.get('kind')=='load']
    if {r['name']for r in loads}!={name for name,c in cases.items()if c.get('load')}:
        raise ValueError('required Rig load measurement inventory differs')
    for row in scenarios:
        case=cases[row['name']];value=row['values']
        if value['steps']!=value['expected_steps']or value['steps']!=len(case['steps']):
            raise ValueError('incomplete Rig step roster: '+row['name'])
        expected_captures={key for step in case['steps']for key in step.get('capture',{})}
        expected_captures|={s['name']+'_recovered_secs'for s in case['steps']if s['type']=='wait_until'}
        if set(value['captures'])!=expected_captures:
            raise ValueError('incomplete Rig captures: '+row['name'])
    strict=os.environ.get('RIG_STRICT')=='1'
    for row in loads:
        name=row['name'];case=cases[name];value=row['values'];profile=case['load']
        for key in ('target_qps','workers','duration_secs','warmup_secs'):
            if value['profile'][key]!=profile[key]:raise ValueError('Rig load profile changed: '+name+'/'+key)
        expected=profile['target_qps']*(profile['duration_secs']-profile['warmup_secs'])
        if type(value['total'])is not int or value['total']!=expected or expected<=0:
            raise ValueError('incomplete Rig offered schedule: '+name)
        for metric in ('achieved_qps','p99_ms','error_rate'):
            if type(value[metric])not in(int,float)or not math.isfinite(value[metric])or value[metric]<0:
                raise ValueError('invalid Rig measurement: '+name+'/'+metric)
        if value['achieved_qps']<profile['target_qps']*0.95:
            raise ValueError('original Rig 95% load honesty floor failed: '+name)
        scenario='/'.join((case['record']['suite'],case['record']['dimension'],case['record']['case']))
        expected_pins=[p for p in pins if scenario==p['scenario']or scenario.endswith('/'+p['scenario'])]
        actual=value.get('pins',[])
        if not expected_pins or value['pins_checked']!=len(expected_pins)or len(actual)!=len(expected_pins):
            raise ValueError('missing original Rig pin evidence: '+name)
        for pin in expected_pins:
            matching=[p for p in actual if p['issue']==pin['issue']and p['pin_scenario']==pin['scenario']and p['metric']==pin['metric']]
            if len(matching)!=1:raise ValueError('ambiguous original Rig pin evidence: '+name)
            result=matching[0];measured=value[pin['metric']]
            if (result['scenario']!=scenario or result['value']!=measured or result['floor']!=pin.get('floor')
                    or result['ratchet']!=pin.get('ratchet')or result['strict']is not strict
                    or result['permitted']is not True or not result['host_fingerprint']):
                raise ValueError('stale or changed Rig pin evidence: '+name)
            if pin.get('floor')is not None and measured>pin['floor']:
                raise ValueError('original Rig floor breached: '+name)
            if pin.get('ratchet')is not None and result['baseline']is None:
                if strict or result['outcome']!='no_baseline'or result['severity']!='info'or result['ratchet_evaluated']is not False:
                    raise ValueError('original explicit NoBaseline/strict result lost: '+name)
            else:
                if result['outcome']!='pass'or result['severity']!='none':
                    raise ValueError('Rig pin did not pass: '+name)
                if pin.get('ratchet')is not None:
                    baseline=result['baseline']
                    if type(baseline)not in(int,float)or not math.isfinite(baseline)or measured>baseline/max(0.01,min(pin['ratchet'],1.0)):
                        raise ValueError('original Rig ratchet breached: '+name)
                    if result['ratchet_evaluated']is not True:raise ValueError('Rig ratchet not evaluated: '+name)
    return dict(required=sorted(cases),executed=sorted(observed),missing=[],
                source_sha256={name:c['source_sha256']for name,c in cases.items()},
                original_ratchet_and_NoBaseline_semantics_preserved=True)
