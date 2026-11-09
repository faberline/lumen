"""DEV-PIPELINE-1 metadata and DAG checks. Never executes candidate code."""
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import quote

STAGES = ('preflight', 'impact', 'unit', 'functional', 'performance')
FAILURES = {'failure', 'timed_out', 'action_required', 'startup_failure'}
REVIEW_TITLE = 'AW Claude review PR ${{ github.event.workflow_run.pull_requests[0].number }} head ${{ github.event.workflow_run.head_sha }}'


def need(condition, reason):
    if not condition:
        raise RuntimeError('development pipeline: ' + reason)


def sibling(name):
    spec = importlib.util.spec_from_file_location('aw_development_' + name,
                                                Path(__file__).with_name(name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def sha(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{40}', value) is not None


def integer(value):
    return type(value) is int and 0 < value < 2**53


def validate(config):
    """Opt in explicitly; legacy consumers keep their current behavior."""
    policy = config.get('development_pipeline')
    if policy is None:
        return None
    need(isinstance(policy, dict) and {'version', 'workflow', 'stages'} <= set(policy)
         and set(policy) <= {'version', 'workflow', 'stages', 'replaces', 'grouped_checks'}
         and type(policy['version']) is int and policy['version'] == 1,
         'invalid version or fields')
    need(policy['workflow'] in {w['path'] for w in config['workflows']},
         'test workflow must remain in required workflows')
    replacements = policy.get('replaces', {})
    need(isinstance(replacements, dict) and len(replacements) <= 1
         and all(isinstance(old, str) and re.fullmatch(r'\.github/workflows/[\w.-]+\.ya?ml', old)
                 and new == policy['workflow'] and old != new for old,new in replacements.items()),
         'workflow replacement must be explicit and one to one')
    stages = policy['stages']
    need(isinstance(stages, dict) and set(stages) == set(STAGES), 'every stage is required')
    ids = []
    for stage in STAGES:
        jobs = stages[stage]
        need(isinstance(jobs, list) and jobs and all(isinstance(x, str)
             and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]*', x) for x in jobs),
             'stage needs explicit job IDs: ' + stage)
        ids.extend(jobs)
    need(len(ids) <= 256 and len(ids) == len(set(ids)), 'duplicate or excessive stage job IDs')
    grouped = policy.get('grouped_checks', [])
    checks = config.get('qualification', {}).get('checks', {})
    need(isinstance(grouped, list) and all(isinstance(x, str) and x in checks for x in grouped)
         and len(grouped) == len(set(grouped)), 'invalid grouped check IDs')
    labels = set()
    for name in grouped:
        check = checks[name]
        need(check.get('case_id') == name and isinstance(check.get('case_label'), str)
             and bool(check['case_label'].strip()) and check.get('workflow') == policy['workflow'],
             'grouped checks require stable case identity and original label')
        need(check['case_label'] not in labels, 'duplicate grouped case label')
        labels.add(check['case_label'])
    for name, check in checks.items():
        if 'case_id' in check or 'case_label' in check:
            need(name in grouped, 'case mapping must be explicit')
    for workflow in config['workflows']:
        for job in workflow['jobs']:
            members = [name for name, check in checks.items()
                       if check.get('workflow') == workflow['path'] and check.get('job') == job]
            if set(members) & set(grouped):
                need(len(members) > 1 and set(members) <= set(grouped),
                     'every check in a batch needs its own case mapping')
    return policy


def retains_workflows(prior, candidate):
    """A reviewed regrouping must retain every old check and case identity."""
    policy = validate(candidate)
    replacements = policy.get('replaces', {}) if policy else {}
    historical = prior.get('development_pipeline', {}).get('replaces', {})
    old = {w['path']:set(w['jobs']) for w in prior['workflows']}
    new = {w['path']:set(w['jobs']) for w in candidate['workflows']}
    grouped = set(policy.get('grouped_checks', [])) if policy else set()
    old_checks = prior.get('qualification', {}).get('checks', {})
    new_checks = candidate.get('qualification', {}).get('checks', {})
    if grouped:
        for name, before in old_checks.items():
            after = new_checks.get(name)
            if not isinstance(after, dict):
                return False
            fields = lambda check: {k:v for k,v in check.items()
                                   if k not in ('workflow','job','case_id','case_label')}
            target = before['workflow'] if before['workflow'] in new else replacements.get(before['workflow'])
            if fields(before) != fields(after) or after.get('workflow') != target:
                return False
            if after.get('job') != before['job'] or name in grouped:
                if (name not in grouped or after['case_id'] != name
                        or after['case_label'] != before.get('case_label', before['job'])):
                    return False
    for path,target in replacements.items():
        if path not in old:
            if historical.get(path) != target:
                return False
        elif path in new or target in old:
            return False  # No merging two old producers into one name set.
    for path,jobs in old.items():
        target = path if path in new else replacements.get(path)
        if target not in new:
            return False
        for job in jobs:
            if job in new[target]:
                continue
            members = {name:check for name,check in old_checks.items()
                       if check.get('workflow') == path and check.get('job') == job}
            if not members or not set(members) <= grouped:
                return False
            for name, check in members.items():
                after = new_checks[name]
                before_fields = {k:v for k,v in check.items()
                                 if k not in ('workflow','job','case_id','case_label')}
                after_fields = {k:v for k,v in after.items()
                                if k not in ('workflow','job','case_id','case_label')}
                if (before_fields != after_fields or after['case_id'] != name
                        or after['case_label'] != check.get('case_label', job)
                        or after['workflow'] != target or after['job'] not in new[target]):
                    return False
    return True


def yaml_document(raw):
    import yaml
    class Unique(yaml.BaseLoader):
        def construct_mapping(self, node, deep=False):
            result = {}
            for key, value in node.value:
                name = self.construct_object(key, deep=deep)
                need(name not in result, 'duplicate YAML key')
                result[name] = self.construct_object(value, deep=deep)
            return result
    need(isinstance(raw, str), 'workflow source must be text')
    for event in yaml.parse(raw):
        need(not isinstance(event, yaml.events.AliasEvent) and not getattr(event, 'anchor', None),
             'workflow aliases and anchors are unsupported')
    result = yaml.load(raw, Loader=Unique)
    need(isinstance(result, dict), 'workflow must be an object')
    return result


def workflow_contract(raw, config):
    """Validate actual transitive needs, rather than trusting a stage label."""
    policy = validate(config)
    need(policy is not None, 'explicit adoption is required')
    workflow = yaml_document(raw)
    jobs = workflow.get('jobs')
    assigned = {j for group in policy['stages'].values() for j in group}
    need(isinstance(jobs, dict) and set(jobs) == assigned,
         'every development test job must be assigned exactly once')
    dependencies = {}
    for name, job in jobs.items():
        need(isinstance(job, dict), 'invalid job')
        need(job.get('continue-on-error', 'false') == 'false', 'job cannot ignore failure')
        for step in job.get('steps', []):
            need(isinstance(step, dict) and step.get('continue-on-error', 'false') == 'false',
                 'step cannot ignore failure')
        if 'matrix' in job.get('strategy', {}):
            need(job['strategy'].get('fail-fast') == 'false', 'matrix must retain all required job results')
        condition = job.get('if')
        need(condition in (None, "github.event_name == 'pull_request'",
                           "${{ github.event_name == 'pull_request' }}"),
             'stage job cannot bypass dependency success')
        required = job.get('needs', [])
        if isinstance(required, str):
            required = [required]
        need(isinstance(required, list) and len(required) == len(set(required))
             and all(x in jobs and x != name for x in required), 'invalid needs')
        dependencies[name] = set(required)
    cache = {}
    def ancestors(name, visiting):
        need(name not in visiting, 'cyclic stage dependencies')
        if name in cache:
            return cache[name]
        result = set(dependencies[name])
        for parent in dependencies[name]:
            result.update(ancestors(parent, visiting | {name}))
        cache[name] = result
        return result
    groups = policy['stages']
    ranks = {'preflight':0, 'impact':1, 'unit':2, 'functional':3, 'performance':3}
    rank = {job:ranks[stage] for stage, names in groups.items() for job in names}
    for job in jobs:
        need(all(rank[parent] <= rank[job] for parent in ancestors(job, set())),
             'earlier stage cannot depend on a later stage')
    for stage in STAGES[1:]:
        prior = {'preflight'} if stage == 'impact' else {'preflight', 'impact'}
        if stage in ('functional', 'performance'):
            prior.add('unit')
        required = {j for group in prior for j in groups[group]}
        for job in groups[stage]:
            need(required <= ancestors(job, set()), 'missing earlier stage dependency: ' + job)
    return dict(version=1, workflow=policy['workflow'], stages=groups, sha256=digest(workflow))


def canonical_issue_target(repo, number, source):
    """A GitHub issues endpoint can also return a PR. Verify the real target."""
    return (integer(number) and isinstance(source, dict) and integer(source.get('number'))
            and source['number'] == number
            and source.get('state') == 'open' and 'pull_request' not in source
            and source.get('repository_url') == f'https://api.github.com/repos/{repo}'
            and source.get('url') == f'https://api.github.com/repos/{repo}/issues/{number}')


def read_issue_target(api, repo, number):
    """Only an exact gh GET 404 proves the linked target is unavailable."""
    need(isinstance(repo, str) and re.fullmatch(r'[\w.-]+/[\w.-]+', repo)
         and integer(number), 'invalid linked issue identity')
    path = f'repos/{repo}/issues/{number}'
    try:
        return api.api(path)
    except subprocess.CalledProcessError as error:
        if (error.returncode == 1 and error.cmd == ['gh', 'api', '--method', 'GET', path]
                and isinstance(error.output, str)):
            try:
                response = sibling('json_data').loads(error.output)
            except (ValueError, TypeError):
                response = None
            if (isinstance(response, dict) and response.get('status') == '404'
                    and response.get('message') == 'Not Found'):
                return None
        raise


def preflight(api, repo, number, head, expected_base=None):
    """Read one explicit comparison; a query error never becomes a pass."""
    need(isinstance(repo, str) and re.fullmatch(r'[\w.-]+/[\w.-]+', repo)
         and integer(number) and sha(head), 'invalid PR identity')
    root = f'repos/{repo}'
    meta = api.api(root)
    default = meta['default_branch']
    base = api.api(f'{root}/commits/{quote(default, safe="")}')['sha']
    need(sha(base), 'invalid default commit')
    pull = api.api(f'{root}/pulls/{number}')
    need(pull.get('state') == 'open' and pull.get('head', {}).get('sha') == head
         and (pull.get('head', {}).get('repo') or {}).get('full_name') == repo
         and pull.get('base', {}).get('ref') == default, 'closed, foreign, stale or wrong-target PR')
    comparison = api.api(f'{root}/compare/{base}...{head}')
    behind = comparison.get('behind_by')
    need(type(behind) is int and behind >= 0 and sha(comparison.get('merge_base_commit', {}).get('sha')),
         'unreadable ancestry comparison')
    current = behind == 0 and comparison['merge_base_commit']['sha'] == base
    checks = [dict(check='includes_current_main', passed=current,
                   reason='PR includes current main' if current else 'PR is behind current main'),
              dict(check='no_merge_conflict', passed=pull.get('mergeable') is True,
                   reason='PR merges without conflicts' if pull.get('mergeable') is True else
                   'PR has merge conflicts' if pull.get('mergeable') is False else 'mergeability is not known')]
    if expected_base is not None:
        need(sha(expected_base), 'invalid frozen base')
        if expected_base != base:
            checks[0].update(passed=False, reason='main changed after the run froze its base')
    issue = sibling('qualification').linked_issue(pull)
    source = None
    if issue is not None:
        source = read_issue_target(api, repo, issue)
    valid = canonical_issue_target(repo, issue, source)
    checks.append(dict(check='one_canonical_issue', passed=valid,
                       reason='One canonical open issue is linked' if valid else
                       'PR requires exactly one canonical open issue'))
    need(api.api(f'{root}/pulls/{number}').get('head', {}).get('sha') == head
         and api.api(f'{root}/commits/{quote(default, safe="")}')['sha'] == base,
         'PR or main changed during preflight')
    reasons = [check['reason'] for check in checks if not check['passed']]
    if pull.get('draft'):
        reasons.append('PR is still a draft')
    return dict(version=1, repo=repo, pr=number, head=head, base=base, issue=issue if valid else None,
                issue_sha256=digest({k:source.get(k) for k in ('title', 'body')}) if valid else None,
                status='failure' if reasons else 'success', reasons=reasons, checks=checks)


def failure_records(repo, number, head, base, snapshots, plan=None):
    """One metadata-only record per failed expanded job; no artifact execution."""
    need(sha(head) and sha(base), 'failure needs exact head and base')
    records = []
    for path, (run, jobs) in sorted(snapshots.items()):
        need(integer(run.get('id')) and integer(run.get('run_attempt')), 'invalid failure run identity')
        for job in jobs:
            if job.get('status') != 'completed' or job.get('conclusion') not in FAILURES:
                continue
            need(integer(job.get('id')) and isinstance(job.get('name'), str), 'invalid failure job identity')
            expected = f'https://github.com/{repo}/actions/runs/{run["id"]}'
            need(run.get('html_url') == expected and job.get('html_url') == expected + f'/job/{job["id"]}',
                 'failure URL identity mismatch')
            steps = [dict(name=s.get('name'), conclusion=s.get('conclusion'))
                     for s in job.get('steps', []) if s.get('conclusion') in FAILURES]
            value = dict(version=1, repo=repo, pr=number, head=head, base=base,
                         workflow=path, run=run['id'], attempt=run['run_attempt'], job=job['id'],
                         name=job['name'], conclusion=job['conclusion'], steps=steps,
                         run_url=expected, job_url=job['html_url'], plan_sha256=digest(plan) if plan else None)
            value['id'] = digest([repo, number, head, path, run['id'], run['run_attempt'], job['id']])
            records.append(value)
    need(len({r['id'] for r in records}) == len(records), 'duplicate failure job identity')
    return records


def text(value):
    """Render metadata as text, without mentions or executable log excerpts."""
    return str(value).replace('@', '＠').replace('`', "'").replace('<', '&lt;').replace('>', '&gt;')[:1000]


def report_failures(repo, issue, records, comments, write, revalidate):
    """Comments carry no label authority. Existing handoff owns the one reset."""
    comments = list(comments)
    result = []
    for record in records:
        marker = '<!-- aw-job-failure:' + record['id'] + ' -->'
        prior = [c for c in comments if c.get('body', '').startswith(marker + '\n')
                 and c.get('user', {}).get('login') == 'github-actions[bot]']
        need(len(prior) <= 1, 'ambiguous failure receipt')
        if prior:
            continue
        revalidate()
        failed = '; '.join(text(s['name']) + ' (' + text(s['conclusion']) + ')' for s in record['steps'])
        reason = failed or 'No failed-step detail is available from GitHub job metadata'
        body = (f'{marker}\nCI job failed for PR #{record["pr"]}.\n\n'
                f'- Job: {text(record["name"])}\n- Observed result: {text(record["conclusion"])}; required: success.\n'
                f'- Failed step: {reason}\n- Head: `{record["head"]}`; base: `{record["base"]}`.\n'
                f'- Workflow: {text(record["workflow"])}; run {record["run"]}, attempt {record["attempt"]}, job {record["job"]}.\n'
                f'- Evidence: [job]({record["job_url"]}), [run]({record["run_url"]}).\n\n'
                'Repair this failure in the existing PR. The lifecycle handoff returns eligible current work to todo once. '
                'It must verify ownership and lifecycle state first; otherwise that handoff remains incomplete. '
                'later failures do not reset an agent that already claimed it. This is an observed job failure, not a verified root-cause claim.')
        response = write(f'repos/{repo}/issues/{issue}/comments', 'POST', {'body':body})
        need(isinstance(response, dict) and integer(response.get('id')), 'failure comment write outcome unknown')
        comments.append(response)
        result.append(record['id'])
    return result


def report_missing_issue(api, repo, pull, before_mutation=None):
    """There is no issue to return. Diagnose the exact PR without guessing."""
    root = f'repos/{repo}'
    number, head = pull['number'], pull['head']['sha']
    need(integer(number) and sha(head), 'invalid unlinked PR identity')
    def fresh():
        current = api.api(f'{root}/pulls/{number}')
        need(current.get('state') == 'open' and current.get('head', {}).get('sha') == head
             and (current.get('head', {}).get('repo') or {}).get('full_name') == repo
             and current.get('base', {}).get('ref') == api.api(root)['default_branch'],
             'unlinked PR changed before diagnostic')
        issue = sibling('qualification').linked_issue(current)
        target = read_issue_target(api, repo, issue) if issue is not None else None
        need(not canonical_issue_target(repo, issue, target), 'PR now has a canonical open issue')
        return current
    current = fresh()
    key = digest([repo, number, head, current.get('title'), current.get('body')])
    marker = '<!-- aw-development-issue-link:' + key + ' -->'
    comments = api.pages(f'{root}/issues/{number}/comments')
    if any(c.get('body', '').startswith(marker + '\n')
           and c.get('user', {}).get('login') == 'github-actions[bot]' for c in comments):
        return False
    need(fresh() == current, 'PR linking metadata changed')
    body = (marker + '\nDevelopment preflight failed: this PR requires exactly one canonical open issue.\n\n'
            f'Head: `{head}`. Use one `Closes #N` link for the source issue and remove ambiguous closing links. '
            'No issue was selected or changed. Later stages and merge remain blocked.')
    if before_mutation is not None:
        before_mutation()
    response = api.api(f'{root}/issues/{number}/comments', method='POST', data={'body':body})
    need(isinstance(response, dict) and integer(response.get('id')), 'PR diagnostic write outcome unknown')
    return True


def review_failure_snapshot(api, repo, number, head, config):
    """Authenticate failed review setup jobs without inventing a Claude decision."""
    producer = config.get('claude_final_review', {}).get('producer')
    if not producer:
        return {}
    review = sibling('claude_review')
    index = sibling('native_review_index')
    runs = api.pages(f'repos/{repo}/actions/workflows/{producer["id"]}/runs', 'workflow_runs')
    # The existing index validates policy source metadata as well as title and
    # run ordering. Bind the current immutable default revision before use.
    default = api.api(f'repos/{repo}')['default_branch']
    source = api.api(f'repos/{repo}/commits/{quote(default, safe="")}')['sha']
    document = dict(config=config, source_commit=source)
    latest = index.attempts(api, repo, document, runs, {number:head}, default).get(number)
    if latest is None:
        return {}
    need(not latest.get('_native_index_error'), 'review failure identity unknown')
    identity = review.producer_identity(api, repo, document, latest['id'], False, producer)
    raw = review.blob(api, repo, producer['path'], identity['head'], producer['definition_sha'])
    need(yaml_document(raw).get('run-name') == REVIEW_TITLE, 'review target title is not trusted')
    current = api.api(f'repos/{repo}/actions/runs/{latest["id"]}')
    need(all(current.get(k) == latest.get(k) for k in ('id', 'run_attempt', 'display_title')),
         'review attempt changed')
    jobs = api.pages(f'repos/{repo}/actions/runs/{latest["id"]}/attempts/{latest["run_attempt"]}/jobs', 'jobs')
    return {producer['path']:(current, jobs)}
