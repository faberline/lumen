"""Shared diff/all selection and execution receipts. No commands or provider writes."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import re


def require(ok, reason):
    if not ok:
        raise ValueError('test plan: ' + reason)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=False).encode()).hexdigest()


def qualification():
    spec = importlib.util.spec_from_file_location('aw_test_qualification', Path(__file__).with_name('qualification.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def select(config, mode, repo, head, base, *, pull=None, files=None, issue=None):
    require(mode in ('diff', 'all'), 'mode must be diff or all')
    require(isinstance(repo, str) and re.fullmatch(r'[\w.-]+/[\w.-]+', repo), 'invalid repository')
    require(all(isinstance(s, str) and re.fullmatch('[0-9a-f]{40}', s) for s in (head, base)), 'exact head/base required')
    q = qualification()
    policy = q.validate(config)
    require(config.get('lifecycle_mode') == 'full' and not config.get('qualification_blockers'),
            'unresolved qualification blockers; selection cannot approve adoption')
    if mode == 'diff':
        require(isinstance(pull, dict) and pull.get('head', {}).get('sha') == head
                and pull.get('base', {}).get('sha') == base
                and pull.get('head', {}).get('repo', {}).get('full_name') == repo
                and pull.get('base', {}).get('repo', {}).get('full_name') == repo,
                'diff needs the exact same-repository PR')
        require(isinstance(files, list), 'complete changed-file inventory required')
        scope = q.select(config, pull, files, issue)
        require(not scope['issues'], '; '.join(scope['issues']))
        selected, paths = scope['selected'], scope['paths']
    else:
        # all never reuses a diff-selected subset or a caller-supplied matrix.
        selected, paths = sorted(policy['checks']), []
        require(q.CATEGORIES <= {c['kind'] for c in policy['checks'].values()},
                'full inventory lacks a required test category')
    require(bool(selected) and len(selected) == len(set(selected)), 'empty or duplicate selection')
    checks = {name: copy.deepcopy(policy['checks'][name]) for name in sorted(selected)}
    result = dict(schema='AW_TEST_PLAN_V1', repo=repo, mode=mode, head=head, base=base,
                  policy_sha256=digest(config), selected=sorted(checks), checks=checks, paths=paths)
    result['sha256'] = digest(result)
    return result


def validate_plan(plan):
    require(isinstance(plan, dict) and plan.get('schema') == 'AW_TEST_PLAN_V1', 'invalid plan schema')
    value = {k: v for k, v in plan.items() if k != 'sha256'}
    require(plan.get('sha256') == digest(value), 'plan digest differs')
    require(isinstance(plan.get('checks'), dict) and bool(plan['checks'])
            and plan.get('selected') == sorted(plan['checks']), 'invalid complete check roster')
    return plan


def verify_execution(plan, receipts):
    """Verify a product runner's per-case roster; this is not CI authorization."""
    validate_plan(plan)
    require(isinstance(receipts, list), 'execution receipts must be an array')
    ids = [r.get('check') for r in receipts if isinstance(r, dict)]
    require(len(ids) == len(receipts) and len(ids) == len(set(ids))
            and set(ids) == set(plan['selected']), 'missing, duplicate or unexpected executed case')
    for row in receipts:
        name = row['check']
        require(all(row.get(k) == plan[k] for k in ('repo', 'head', 'base'))
                and row.get('plan_sha256') == plan['sha256'], 'stale or foreign case receipt: ' + name)
        require(row.get('conclusion') == 'success', 'case did not pass: ' + name)
        expected, executed = row.get('expected'), row.get('executed')
        require(isinstance(expected, list) and isinstance(executed, list)
                and all(isinstance(x, str) and x.strip() for x in expected + executed)
                and len(expected) == len(set(expected)) and len(executed) == len(set(executed))
                and set(expected) == set(executed), 'test names differ: ' + name)
        require(bool(expected) or plan['checks'][name]['kind'] == 'policy', 'zero tests: ' + name)
        require(row.get('failures') == [] and row.get('skipped') == [], 'failed or skipped tests: ' + name)
    return dict(schema='AW_TEST_EXECUTION_V1', plan_sha256=plan['sha256'],
                cases=sorted(ids), status='success', qualification='not_evaluated')
