"""Trusted, versioned PR applicability policy. No shell, artifacts or PR config."""
import copy
import hashlib
import json
import re

CATEGORIES = {'unit', 'functional_e2e', 'performance'}
KINDS = CATEGORIES | {'policy'}
STATUSES = {'added', 'removed', 'modified', 'renamed', 'copied', 'changed', 'unchanged'}


def require(condition, message):
    if not condition:
        raise RuntimeError('qualification policy: ' + message)


def strings(value):
    return isinstance(value, list) and all(isinstance(x, str) and x.strip() for x in value)


def path_valid(path):
    return (isinstance(path, str) and bool(path) and not path.startswith('/')
            and '\\' not in path and '\x00' not in path
            and not any(part in ('', '.', '..') for part in path.split('/')))


def matches(path, pattern):
    # * and ? stay in a directory; ** crosses directories. **/ includes root.
    parts = []
    index = 0
    while index < len(pattern):
        if pattern[index:index + 3] == '**/':
            parts.append('(?:.*/)?')
            index += 3
        elif pattern[index:index + 2] == '**':
            parts.append('.*')
            index += 2
        elif pattern[index] == '*':
            parts.append('[^/]*')
            index += 1
        elif pattern[index] == '?':
            parts.append('[^/]')
            index += 1
        else:
            parts.append(re.escape(pattern[index]))
            index += 1
    return re.fullmatch(''.join(parts), path) is not None


def validate(config):
    policy = config['qualification']
    require(isinstance(policy, dict) and policy.get('version') in (1, 2)
            and type(policy.get('version')) is int, 'version must be 1 or 2')
    checks = policy.get('checks')
    require(isinstance(checks, dict) and bool(checks), 'checks must be nonempty')
    allowed = {w['path']: set(w['jobs']) for w in config['workflows']}
    for name, check in checks.items():
        require(isinstance(name, str) and re.fullmatch(r'[A-Za-z0-9_.-]+', name)
                and isinstance(check, dict), 'invalid check ID or object')
        require(check.get('kind') in KINDS, 'unknown check category')
        require(check.get('workflow') in allowed and isinstance(check.get('job'), str)
                and check['job'] in allowed[check['workflow']], 'check outside workflow/job allowlist')
    identities = policy.get('trusted_workflows')
    require(isinstance(identities, dict) and set(identities) == set(allowed),
            'every allowlisted workflow needs a trusted identity')
    for identity in identities.values():
        require(isinstance(identity, dict) and type(identity.get('id')) is int
                and identity['id'] > 0 and isinstance(identity.get('definition_sha'), str)
                and re.fullmatch('[0-9a-f]{40}', identity['definition_sha']),
                'workflow identity needs numeric ID and reviewed definition blob SHA')
    always = policy.get('always')
    require(strings(always) and bool(always) and len(set(always)) == len(always)
            and all(name in checks and checks[name]['kind'] == 'policy' for name in always),
            'always must name real policy checks; zero-check qualification is forbidden')
    protected = policy.get('protected_paths')
    require(strings(protected) and bool(protected) and all(path_valid(p) for p in protected),
            'declare gate infrastructure protected_paths')
    rules = policy.get('rules')
    require(isinstance(rules, list) and bool(rules), 'rules must be nonempty')
    for rule in rules:
        require(isinstance(rule, dict), 'rule must be an object')
        require(strings(rule.get('paths')) and bool(rule['paths'])
                and all(path_valid(p) for p in rule['paths']), 'rule needs safe path globs')
        required, exempt = rule.get('require'), rule.get('exempt')
        require(strings(required) and len(set(required)) == len(required)
                and all(name in checks for name in required), 'rule references unknown checks')
        require(isinstance(exempt, dict) and set(exempt) <= CATEGORIES
                and all(isinstance(reason, str) and reason.strip() for reason in exempt.values()),
                'exemptions need category and reviewed reason')
        kinds = {checks[name]['kind'] for name in required}
        require(not kinds & set(exempt), 'same rule cannot require and exempt a category')
    if policy['version'] == 2:
        validate_contracts(policy, checks)
    return policy


def validate_contracts(policy, checks):
    contracts = policy.get('contracts')
    require(isinstance(contracts, dict) and bool(contracts), 'v2 requires a nonempty issue contract registry')
    for number, case in contracts.items():
        require(isinstance(number, str) and re.fullmatch(r'[1-9][0-9]*', number), 'invalid issue contract number')
        require(isinstance(case, dict) and set(case) == {'title_sha256', 'body_sha256', 'applicability', 'additional', 'unresolved'}, 'invalid issue contract fields')
        for name in ('title_sha256', 'body_sha256'):
            require(isinstance(case[name], str) and re.fullmatch('[0-9a-f]{64}', case[name]), 'invalid issue contract hash')
        decisions = case['applicability']
        require(isinstance(decisions, dict) and set(decisions) == CATEGORIES, 'issue contract needs every category')
        for kind, decision in decisions.items():
            require(isinstance(decision, dict) and set(decision) == {'status', 'checks', 'reason'}, 'invalid issue applicability fields')
            status, ids, reason = decision['status'], decision['checks'], decision['reason']
            require(status in ('required', 'not_applicable', 'unknown') and isinstance(reason, str) and bool(reason.strip()), 'issue applicability needs status and reason')
            require(strings(ids) and len(ids) == len(set(ids)) and all(name in checks and checks[name]['kind'] == kind for name in ids), 'issue applicability has invalid checks')
            require(bool(ids) if status == 'required' else not ids, 'issue applicability status and checks disagree')
        ids = case['additional']
        require(strings(ids) and len(ids) == len(set(ids)) and all(name in checks for name in ids), 'issue contract references unknown additional checks')
        require(strings(case['unresolved']), 'unresolved issue gates must be strings')


def linked_issue(pr):
    # Match the native merge controller's intentionally conservative contract.
    body = pr.get('body') or ''
    if not isinstance(body, str) or not isinstance(pr.get('title') or '', str):
        return None
    lines = re.findall(r'(?m)^Closes #([1-9][0-9]*)[ \t\r]*$', body)
    if len(lines) != 1:
        return None
    rest = re.sub(r'(?m)^Closes #[1-9][0-9]*[ \t\r]*$', '', body)
    if re.search(r'(?i)\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\b[ \t\r\n]*:?'
                 r'[ \t\r\n]*(?:#[0-9]+|[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+#[0-9]+|https?://)',
                 (pr.get('title') or '') + '\n' + rest):
        return None
    return int(lines[0])


def issue_contract(policy, pr, issue):
    """Bind trusted issue metadata; absence or uncertainty cannot authorize review."""
    number = linked_issue(pr)
    issues, selected, context = [], set(), {'issue': number}
    if number is None:
        return context, ['one canonical linked issue is required'], selected
    case = policy['contracts'].get(str(number))
    if case is None:
        return context, ['linked issue has no reviewed contract'], selected
    repo = pr.get('base', {}).get('repo', {}).get('full_name')
    head_repo = pr.get('head', {}).get('repo', {}).get('full_name')
    if (not isinstance(repo, str) or not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo)
            or repo != head_repo or not isinstance(issue, dict)
            or issue.get('number') != number or type(issue.get('number')) is not int
            or issue.get('url') != f'https://api.github.com/repos/{repo}/issues/{number}'
            or issue.get('repository_url') != f'https://api.github.com/repos/{repo}'
            or issue.get('state') != 'open' or 'pull_request' in issue):
        return context, ['linked issue identity, state or repository cannot be verified'], selected
    labels = issue.get('labels')
    if (not isinstance(labels, list) or any(not isinstance(x, dict) or not isinstance(x.get('name'), str) for x in labels)
            or any(x['name'].lower() in ('blocked', 'agy:blocked') for x in labels)):
        issues.append('linked issue labels are missing, malformed or blocked')
    for name in ('title', 'body'):
        value = issue.get(name)
        if not isinstance(value, str) or not value.strip():
            issues.append('linked issue ' + name + ' is missing')
            continue
        digest = hashlib.sha256(value.encode('utf-8')).hexdigest()
        context[name + '_sha256'] = digest
        if digest != case[name + '_sha256']:
            issues.append('linked issue ' + name + ' differs from reviewed contract')
    context['contract_sha256'] = hashlib.sha256(json.dumps(case, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')).hexdigest()
    for kind, decision in case['applicability'].items():
        if decision['status'] == 'unknown':
            issues.append('linked issue has unknown ' + kind + ' applicability')
        elif decision['status'] == 'required':
            selected.update(decision['checks'])
    if case['unresolved']:
        issues.append('linked issue has unresolved required gates')
    selected.update(case['additional'])
    context['applicability'] = copy.deepcopy(case['applicability'])
    context['unresolved_count'] = len(case['unresolved'])
    return context, issues, selected


def changed_paths(pr, files):
    """GitHub truncates this endpoint at 3000 files. Unknown is never empty."""
    count = pr.get('changed_files')
    issues, paths, names = [], [], set()
    if type(count) is not int or count <= 0 or count >= 3000 or len(files) != count:
        issues.append('incomplete, empty or capped PR files inventory')
    for file in files:
        name = file.get('filename')
        if not path_valid(name) or name in names or file.get('status') not in STATUSES:
            issues.append('invalid, duplicate or unknown PR file metadata')
            continue
        names.add(name)
        paths.append(name)
        if file['status'] in ('renamed', 'copied'):
            previous = file.get('previous_filename')
            if not path_valid(previous):
                issues.append('rename/copy missing previous filename')
            else:
                paths.append(previous)
    return sorted(set(paths)), issues


def select(config, pr, files, issue=None):
    policy = validate(config)
    paths, issues = changed_paths(pr, files)
    selected = set(policy['always'])
    exemptions = {}
    for path in paths:
        if (path.startswith(('.github/workflows/', '.github/actions/'))
                or any(matches(path, p) for p in policy['protected_paths'])):
            issues.append('gate infrastructure change requires reviewed policy update: ' + path)
        rules = [r for r in policy['rules'] if any(matches(path, p) for p in r['paths'])]
        if not rules:
            issues.append('unmapped changed path: ' + path)
            continue
        required = {name for rule in rules for name in rule['require']}
        selected.update(required)
        covered = {policy['checks'][name]['kind'] for name in required}
        exempt = {kind: reason for rule in rules for kind, reason in rule['exempt'].items()}
        # Every path must independently cover every category. Required wins
        # across overlapping rules; a docs exemption cannot cover another path.
        missing = CATEGORIES - covered - set(exempt)
        if missing:
            issues.append(path + ': no applicability decision for ' + ', '.join(sorted(missing)))
        exemptions[path] = {kind: reason for kind, reason in exempt.items() if kind not in covered}
    contract = None
    if policy['version'] == 2:
        contract, contract_issues, additional = issue_contract(policy, pr, issue)
        issues.extend(contract_issues)
        selected.update(additional)
    wanted = {}
    for name in sorted(selected):
        check = policy['checks'][name]
        wanted.setdefault(check['workflow'], set()).add(check['job'])
    workflows = [dict(path=path, jobs=sorted(jobs)) for path, jobs in sorted(wanted.items())]
    base = pr.get('base', {}).get('sha')
    if not isinstance(base, str) or not re.fullmatch('[0-9a-f]{40}', base):
        issues.append('missing frozen PR base SHA')
    return dict(workflows=workflows, selected=sorted(selected), paths=paths,
                base=base, exemptions=exemptions, issues=sorted(set(issues)), contract=contract)


def trusted_run(run, config):
    context = config['_qualification_context']
    identity = config['qualification']['trusted_workflows'][run['path']]
    if (run.get('workflow_id') != identity['id'] or type(run.get('run_attempt')) is not int
            or run['run_attempt'] < 1):
        return False
    associations=run.get('pull_requests')
    if not isinstance(associations,list) or not all(isinstance(pr,dict) for pr in associations):
        return False
    return any(pr.get('number') == context['pr']
               and pr.get('head', {}).get('sha') == context['head']
               and pr.get('base', {}).get('sha') == context['base']
               for pr in associations)


def scoped_config(config, pr, files, definitions, issue=None):
    """definitions maps (workflow path, immutable ref) to GitHub blob metadata."""
    plan = select(config, pr, files, issue)
    head = pr['head']['sha']
    untrusted = []
    for workflow in plan['workflows']:
        path = workflow['path']
        expected = config['qualification']['trusted_workflows'][path]['definition_sha']
        if any(definitions.get((path, ref), {}).get('sha') != expected
               for ref in (head, plan['base'])):
            untrusted.append(path)
            plan['issues'].append('untrusted head/base workflow definition: ' + path)
    result = copy.deepcopy(config)
    result['workflows'] = plan['workflows']
    result['_qualification_context'] = dict(plan, head=head, pr=pr['number'],
                                             untrusted_workflows=untrusted)
    return result
