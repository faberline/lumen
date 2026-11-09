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


def execute_unregistered_issue(config, repo, head, base, *, pull, files, issue, definitions):
    """Plan the full trusted roster without granting an issue qualification."""
    q = qualification()
    policy = q.validate(config)
    require(policy['version'] == 2, 'unregistered execution requires reviewed v2 policy')
    require(isinstance(pull, dict) and pull.get('state') == 'open' and pull.get('draft') is False
            and pull.get('head', {}).get('sha') == head and pull.get('base', {}).get('sha') == base
            and all(pull.get(side, {}).get('repo', {}).get('full_name') == repo for side in ('head', 'base')),
            'execution needs the exact open same-repository PR')
    number = q.linked_issue(pull)
    require(type(number) is int and isinstance(issue, dict) and issue.get('number') == number
            and type(issue.get('number')) is int and issue.get('state') == 'open'
            and issue.get('url') == f'https://api.github.com/repos/{repo}/issues/{number}'
            and issue.get('repository_url') == f'https://api.github.com/repos/{repo}'
            and 'pull_request' not in issue, 'linked issue identity is not verified')
    require(all(isinstance(issue.get(field), str) and issue[field].strip() for field in ('title', 'body')),
            'linked issue title or body is missing')
    labels = issue.get('labels')
    require(isinstance(labels, list) and all(isinstance(x, dict) and isinstance(x.get('name'), str) for x in labels)
            and not any(x['name'].lower() in ('blocked', 'agy:blocked') for x in labels), 'linked issue is blocked or labels unknown')
    require(isinstance(files, list), 'complete changed-file inventory required')
    scope = q.select(config, pull, files, issue)
    protected = [reason for reason in scope['issues']
                 if reason.startswith('gate infrastructure change requires reviewed policy update: ')]
    remaining = [reason for reason in scope['issues'] if reason not in protected]
    require(remaining == ['linked issue has no reviewed contract'],
            'execution-only fallback requires an unregistered contract and no other blockers')
    require(isinstance(definitions, dict), 'trusted workflow definitions required')
    for path, identity in policy['trusted_workflows'].items():
        require(all(definitions.get((path, ref), {}).get('sha') == identity['definition_sha']
                    for ref in (head, base)), 'untrusted head/base workflow definition: ' + path)
    plan = select(config, 'all', repo, head, base)
    return dict(schema='AW_TEST_EXECUTION_ONLY_V1', plan=plan, changed_paths=scope['paths'],
                qualification='not_evaluated', unresolved=protected + remaining)


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
