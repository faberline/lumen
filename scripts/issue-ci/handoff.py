#!/usr/bin/env python3
"""Trusted CI controller. Reads GitHub metadata, never executes PR code/artifacts."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import quote

_json_spec = importlib.util.spec_from_file_location('issue_ci_json_data', Path(__file__).with_name('json_data.py'))
_json_data = importlib.util.module_from_spec(_json_spec)
_json_spec.loader.exec_module(_json_data)
strict_json_loads = _json_data.loads


def api(path, method='GET', data=None):
    command = ['gh', 'api', '--method', method, path]
    kwargs = dict(text=True)
    if data is not None:
        command += ['--input', '-']
        kwargs['input'] = json.dumps(data)
    output = subprocess.check_output(command, **kwargs)
    return strict_json_loads(output) if output.strip() else None


def pages(path, key=None):
    items = []
    for page in range(1, 1001):
        value = api(f'{path}{"&" if "?" in path else "?"}per_page=100&page={page}')
        batch = value[key] if key else value
        items.extend(batch)
        if len(batch) < 100:
            return items
    raise RuntimeError('pagination limit reached; refuse partial evidence')


def load_config(path):
    return validate_config(strict_json_loads(Path(path).read_text()))


def validate_config(config):
    if not isinstance(config, dict):
        raise RuntimeError('gate config must be an object')
    if '_qualification_context' in config:
        raise RuntimeError('computed qualification context cannot be supplied by config')
    if 'completion_owner' in config:
        if config['completion_owner'] != 'local':
            raise RuntimeError('completion_owner must be local when explicitly configured')
        retired = {'claude_final_review', 'native_auto_merge', 'issue_recovery',
                   'issue_guard_rollout', 'issue_guard_cutover'}
        if retired.intersection(config):
            raise RuntimeError('local completion cannot retain cloud review or completion producers')
    mode = config.get('lifecycle_mode', 'full')
    if mode not in ('full', 'failure-only'):
        raise RuntimeError('lifecycle_mode must be full or failure-only')
    blockers = config.get('qualification_blockers', [])
    if not isinstance(blockers, list) or not all(isinstance(b, str) and b.strip() for b in blockers):
        raise RuntimeError('qualification_blockers must be a list of nonempty strings')
    workflows = config.get('workflows')
    if not isinstance(workflows, list) or not workflows:
        raise RuntimeError('required workflows must be a nonempty list')
    paths = []
    for w in workflows:
        if not isinstance(w, dict):
            raise RuntimeError('each required workflow must be an object')
        path, jobs = w.get('path'), w.get('jobs')
        if (not isinstance(path, str) or not re.fullmatch(r'\.github/workflows/[\w.-]+\.ya?ml', path)
                or not isinstance(jobs, list) or not jobs
                or not all(isinstance(j, str) and j.strip() for j in jobs)
                or len(set(jobs)) != len(jobs)):
            raise RuntimeError('each workflow needs an explicit nonempty unique job-name list')
        paths.append(path)
    if len(set(paths)) != len(paths):
        raise RuntimeError('required workflow paths must be unique')
    if 'qualification' in config:
        qualification_module().validate(config)
    development_module().validate(config)
    return config


def qualification_module():
    # Always load the sibling of this pinned controller, never a PR module.
    path = Path(__file__).with_name('qualification.py')
    spec = importlib.util.spec_from_file_location('issue_ci_qualification', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def development_module():
    spec = importlib.util.spec_from_file_location('issue_ci_development',
            Path(__file__).with_name('development_pipeline.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_adoption(config):
    """Shared-action rollout cannot enable legacy full without applicability."""
    if config.get('lifecycle_mode') not in ('failure-only', 'full'):
        raise RuntimeError('shared adoption requires an explicit lifecycle_mode')
    if not isinstance(config.get('qualification_blockers'), list):
        raise RuntimeError('shared adoption requires explicit qualification_blockers')
    if config['lifecycle_mode'] == 'full' and 'qualification' not in config:
        raise RuntimeError('shared full adoption requires qualification contract v1')
    return config


def qualify_config(repo, pr, config, files=None):
    return _qualify_config(repo, pr, config, files)


def _merged_pull(repo, number, head, base):
    """Authenticate a cleared CI association, not permission to close an issue.

    The close layer must still prove merge ancestry and unchanged ownership.
    This read-only path cannot qualify an open, unmerged or foreign PR.
    """
    if (not isinstance(repo,str) or not re.fullmatch(r'[\w.-]+/[\w.-]+',repo)
            or type(number) is not int or number<=0
            or any(not isinstance(x,str) or not re.fullmatch('[0-9a-f]{40}',x) for x in (head,base))):
        raise RuntimeError('merged CI target identity')
    pull=api(f'repos/{repo}/pulls/{number}')
    meta=api(f'repos/{repo}')
    if not (pull.get('number')==number and pull.get('state')=='closed' and pull.get('merged') is True
            and pull.get('draft') is False and isinstance(pull.get('merged_at'),str)
            and isinstance(pull.get('merge_commit_sha'),str) and re.fullmatch('[0-9a-f]{40}',pull['merge_commit_sha'])
            and pull.get('head',{}).get('sha')==head and pull.get('base',{}).get('sha')==base
            and pull.get('head',{}).get('repo',{}).get('full_name')==repo
            and pull.get('base',{}).get('repo',{}).get('full_name')==repo
            and meta.get('full_name')==repo and isinstance(meta.get('default_branch'),str)
            and pull['base'].get('ref')==meta['default_branch']):
        raise RuntimeError('merged CI target identity')
    return {k:pull[k] for k in ('number','state','merged','merge_commit_sha','head','base')}


def qualify_merged_config(repo, pr, config, files=None):
    _merged_pull(repo,pr['number'],pr['head']['sha'],pr['base']['sha'])
    return _qualify_config(repo,pr,config,files,merged=True)


def _qualify_config(repo, pr, config, files=None, merged=False):
    module = qualification_module()
    if files is None:
        files = pages(f'repos/{repo}/pulls/{pr["number"]}/files')
    issue = None
    if config['qualification'].get('version') == 2:
        number = module.linked_issue(pr)
        if number is not None and str(number) in config['qualification'].get('contracts', {}):
            if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo):
                raise RuntimeError('invalid issue repository')
            issue = api(f'repos/{repo}/issues/{number}')
            if merged and issue.get('state')=='closed':
                # Reconstruct historical applicability only after actual
                # merged-PR authentication. Never change the provider issue.
                issue=dict(issue,state='open')
    plan = module.select(config, pr, files, issue)
    definitions = {}
    for workflow in plan['workflows']:
        for ref in (pr['head']['sha'], plan['base']):
            if isinstance(ref, str) and re.fullmatch('[0-9a-f]{40}', ref):
                definitions[workflow['path'], ref] = api(
                    f'repos/{repo}/contents/{workflow["path"]}?ref={ref}')
    scoped = module.scoped_config(config, pr, files, definitions, issue)
    development = development_module()
    policy = development.validate(config)
    if policy is not None:
        data = definitions.get((policy['workflow'], pr['head']['sha']))
        trusted = config['qualification']['trusted_workflows'][policy['workflow']]['definition_sha']
        if data is not None and data.get('sha') == trusted:
            import base64
            raw = base64.b64decode(''.join(data['content'].split()), validate=True)
            if (len(raw) > 262144 or hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest() != trusted):
                raise RuntimeError('development workflow content identity mismatch')
            development.workflow_contract(raw.decode('utf-8'), config)
    return scoped


def evaluate_evidence(workflows, snapshots):
    """Aggregate latest runs of ALL configured workflows; no vacuous all-green."""
    pending, failures, evidence = [], [], []
    for workflow in workflows:
        path = workflow['path']
        snapshot = snapshots.get(path)
        if not snapshot:
            pending.append(f'{path}: missing run')
            continue
        run, jobs = snapshot
        evidence.append(f'{run["id"]}:{run.get("run_attempt", 1)}')
        if run['status'] != 'completed':
            pending.append(f'{path}: {run["status"]}')
        elif run.get('conclusion') in ('failure', 'timed_out', 'action_required', 'startup_failure'):
            failures.append(f'{path}: {run["conclusion"]} {run["html_url"]}')
        elif run.get('conclusion') != 'success':
            pending.append(f'{path}: {run.get("conclusion")} {run["html_url"]}')
        for name in workflow['jobs']:
            matching = [j for j in jobs if j['name'] == name]
            if not matching:
                pending.append(f'{path} / {name}: missing job')
            for job in matching:
                link = job['html_url']
                if job['status'] != 'completed':
                    pending.append(f'{path} / {name}: {job["status"]} {link}')
                elif job.get('conclusion') in ('failure', 'timed_out', 'action_required', 'startup_failure'):
                    failures.append(f'{path} / {name}: {job["conclusion"]} {link}')
                elif job.get('conclusion') != 'success':
                    # Skips need a separate future policy; never assume justification.
                    pending.append(f'{path} / {name}: {job.get("conclusion")} {link}')
    return dict(failures=failures, pending=pending, evidence=sorted(evidence))


def evaluate(workflows, snapshots):
    # Legacy lifecycle adapter; evidence collection itself has no label authority.
    result = evaluate_evidence(workflows, snapshots)
    state = 'todo' if result['failures'] else 'pending' if result['pending'] else 'done'
    return state, result['failures'] + result['pending'], result['evidence']


def evaluate_policy(config, snapshots):
    """Missing qualification can never produce done, even in full mode."""
    state, reasons, evidence = evaluate(config['workflows'], snapshots)
    if state == 'done':
        if 'qualification' in config and '_qualification_context' not in config:
            state = 'pending'
            reasons.append('qualification applicability has not been computed')
        if config.get('lifecycle_mode', 'full') == 'failure-only':
            state = 'pending'
            reasons.append('failure-only mode does not authorize done')
        if config.get('qualification_blockers', []):
            state = 'pending'
            reasons.extend(f'qualification blocked: {b}' for b in config['qualification_blockers'])
        issues = config.get('_qualification_context', {}).get('issues', [])
        if issues:
            state = 'pending'
            reasons.extend(issues)
    return state, reasons, evidence


def snapshot(repo, sha, config, diagnostics=None):
    return _snapshot(repo,sha,config,diagnostics)


def merged_snapshot(repo, sha, config, number, base, diagnostics=None):
    before=_merged_pull(repo,number,sha,base)
    context=config.get('_qualification_context')
    if context is not None and (context.get('pr'),context.get('head'),context.get('base'))!=(number,sha,base):
        raise RuntimeError('merged CI qualification context')
    result=_snapshot(repo,sha,config,diagnostics,allow_cleared=True)
    if _merged_pull(repo,number,sha,base)!=before:
        raise RuntimeError('merged CI target changed')
    return result


def _snapshot(repo, sha, config, diagnostics=None, allow_cleared=False):
    root = f'repos/{repo}'
    snapshots = {}
    for workflow in config['workflows']:
        if workflow['path'] in config.get('_qualification_context', {}).get('untrusted_workflows', []):
            if diagnostics is not None:
                diagnostics[workflow['path']] = 'untrusted_workflow'
            continue
        runs = pages(f'{root}/actions/workflows/{quote(workflow["path"].split("/")[-1], safe="")}/runs?head_sha={sha}&event=pull_request', 'workflow_runs')
        runs = [r for r in runs if r['head_sha'] == sha and r.get('event') == 'pull_request' and r.get('head_repository', {}).get('full_name') == repo and r.get('path') == workflow['path']]
        if not runs:
            continue
        latest = max(runs, key=lambda r: (r['run_number'], r.get('run_attempt', 1)))
        trusted=True
        if '_qualification_context' in config:
            trusted=qualification_module().trusted_run(latest,config)
            if not trusted and allow_cleared and latest.get('pull_requests')==[]:
                identity=config['qualification']['trusted_workflows'][workflow['path']]
                trusted=(latest.get('workflow_id')==identity['id']
                         and type(latest.get('run_attempt')) is int and latest['run_attempt']>0
                         and latest.get('repository',{}).get('full_name')==repo)
        if not trusted:
            if diagnostics is not None:
                diagnostics[workflow['path']] = 'untrusted_latest_run'
            # Never fall back to an older green run when the newest identity
            # or PR/base binding is unknown or untrusted.
            continue
        # Run-attempt endpoint prevents jobs from a previous attempt filling holes.
        jobs = pages(f'{root}/actions/runs/{latest["id"]}/attempts/{latest.get("run_attempt", 1)}/jobs', 'jobs')
        snapshots[workflow['path']] = latest, jobs
    return snapshots


def untrusted_failure_snapshots(repo, sha, config):
    """Collect exact-run failures without authorizing changed CI as a pass.

    A protected workflow edit needs reviewed adoption for positive evidence.
    Its real failed jobs can still return the associated issue to todo. A
    missing, skipped, cancelled, successful or unbound run is never a failure.
    """
    context = config.get('_qualification_context', {})
    paths = context.get('untrusted_workflows', [])
    if not paths:
        return {}
    root = f'repos/{repo}'
    results = {}
    for workflow in config['workflows']:
        path = workflow['path']
        if path not in paths:
            continue
        runs = pages(f'{root}/actions/workflows/{quote(path.split("/")[-1], safe="")}/runs?head_sha={sha}&event=pull_request', 'workflow_runs')
        runs = [r for r in runs if r.get('head_sha') == sha and r.get('event') == 'pull_request'
                and r.get('head_repository', {}).get('full_name') == repo and r.get('path') == path]
        if not runs:
            continue
        if not all(type(r.get('id')) is int and r['id'] > 0
                   and type(r.get('run_number')) is int and r['run_number'] > 0
                   and type(r.get('run_attempt')) is int and r['run_attempt'] > 0 for r in runs):
            continue  # An unknown latest attempt must not revive older evidence.
        latest = max(runs, key=lambda r: (r['run_number'], r['run_attempt']))
        associations = latest.get('pull_requests')
        if not isinstance(associations, list) or len(associations) != 1 \
                or not qualification_module().trusted_run(latest, config):
            continue
        jobs = pages(f'{root}/actions/runs/{latest["id"]}/attempts/{latest["run_attempt"]}/jobs', 'jobs')
        evidence = evaluate_evidence([workflow], {path: (latest, jobs)})
        if evidence['failures']:
            results[path] = (latest, jobs)
    return results


def process_head(args, config, check_event=True, before_mutation=None, development_config=None):
    source_config = config
    review_config = development_config if development_config is not None else config
    if development_config is not None:
        validate_config(development_config)
        if any(development_config.get(key) != config.get(key) for key in
               ('workflows', 'qualification', 'lifecycle_mode', 'qualification_blockers', 'development_pipeline')):
            raise RuntimeError('development handoff semantics differ from trusted CI policy')
    root = f'repos/{args.repo}'
    pr_path = f'{root}/pulls/{args.pr}'
    def write(path, method, data=None):
        # Call before the provider request: a lost response is still a write.
        if before_mutation is not None:
            before_mutation()
        return api(path, method, data)
    pr = api(pr_path)
    if pr['state'] != 'open' or pr['head']['sha'] != args.head or pr['head']['repo']['full_name'] != args.repo:
        raise RuntimeError('closed, foreign, or stale PR head; no mutation')
    development = development_module()
    development_policy = development.validate(source_config)
    match = re.fullmatch(r'(?:issue/|agy/issue-)([0-9]+)', pr['head']['ref'])
    if development_policy is not None:
        issue = qualification_module().linked_issue(pr)
        if issue is None:
            raise RuntimeError('PR requires exactly one canonical issue association')
    else:
        if not match:
            raise RuntimeError('PR branch has no supported issue association')
        issue = int(match[1])
    if not re.search(rf'(?im)^Closes #{issue}\s*$', pr.get('body') or ''):
        raise RuntimeError('PR body must explicitly link its branch issue with Closes #N')
    if 'qualification' in config:
        config = qualify_config(args.repo, pr, config)
    snapshots = snapshot(args.repo, args.head, config)
    state, reasons, evidence = evaluate_policy(config, snapshots)
    negative_snapshots = untrusted_failure_snapshots(args.repo, args.head, config)
    if negative_snapshots:
        negative = evaluate_evidence(config['workflows'], negative_snapshots)
        if negative['failures']:
            state = 'todo'
            reasons = negative['failures'] + reasons
            evidence = sorted(set(evidence + negative['evidence']))
    development_snapshots, failed_jobs, preflight = {}, [], None
    if development_policy is not None:
        from types import SimpleNamespace
        adapter = SimpleNamespace(api=api, pages=pages)
        preflight = development.preflight(adapter, args.repo, args.pr, args.head)
        if preflight.get('issue') != issue:
            raise RuntimeError('PR has no verified canonical open issue; no issue mutation')
        development_snapshots = development.review_failure_snapshot(adapter, args.repo, args.pr, args.head, review_config)
        failed_jobs = development.failure_records(args.repo, args.pr, args.head, preflight['base'],
            {**snapshots, **negative_snapshots, **development_snapshots}, config.get('_qualification_context'))
        decisive_preflight = (not preflight['checks'][0]['passed']
                             or preflight['checks'][1]['reason'] == 'PR has merge conflicts') if preflight['checks'] else False
        if failed_jobs or decisive_preflight:
            state = 'todo'
            reasons = preflight['reasons'] + [f'{r["name"]}: {r["conclusion"]} {r["job_url"]}' for r in failed_jobs] + reasons
    if not args.apply and state == 'done' and 'qualification' in source_config:
        if snapshot(args.repo, args.head, config) != snapshots:
            raise RuntimeError('CI runs changed during read-only qualification')
        current_pr = api(pr_path)
        if qualify_config(args.repo, current_pr, source_config) != config:
            raise RuntimeError('PR applicability changed during read-only qualification')
        final_pr = api(pr_path)
        if (final_pr['state'] != 'open' or final_pr['head']['sha'] != args.head
                or final_pr['head']['repo']['full_name'] != args.repo
                or final_pr['head']['ref'] != pr['head']['ref']
                or final_pr.get('base', {}).get('sha') != config['_qualification_context']['base']
                or not re.search(rf'(?im)^Closes #{issue}\s*$', final_pr.get('body') or '')):
            raise RuntimeError('PR moved during read-only qualification')
    print(json.dumps(dict(state=state, head=args.head, evidence=evidence, reasons=reasons)))
    if not args.apply:
        return 0 if state == 'done' else 2
    if check_event:
        if args.run_id is None:
            raise RuntimeError('--apply requires workflow_run event --run-id')
        event_run = api(f'{root}/actions/runs/{args.run_id}')
        if event_run['head_sha'] != args.head or event_run.get('event') != 'pull_request' or str(event_run['id']) not in {e.split(':')[0] for e in evidence}:
            raise RuntimeError('stale/unconfigured triggering run; no mutation')
    if state == 'pending':
        return 0
    issue_path = f'{root}/issues/{issue}'
    current = api(issue_path)
    labels = {l['name'] for l in current['labels']}
    if ('pull_request' in current or (development_policy is not None
            and not development.canonical_issue_target(args.repo, issue, current))):
        raise RuntimeError('target is not a verified canonical open issue; no mutation')
    if current['state'] != 'open' or 'blocked' in labels:
        raise RuntimeError('issue is closed or blocked; no mutation')
    failure_only = config.get('lifecycle_mode', 'full') == 'failure-only'
    if failure_only and 'done' in labels:
        return 0  # This mode has no authority over already done issues.
    comments = pages(f'{issue_path}/comments')

    def fresh_labels(require_managed=True):
        fresh = api(pr_path)
        if (fresh['state'] != 'open' or fresh['head']['sha'] != args.head
                or fresh['head']['repo']['full_name'] != args.repo
                or fresh['head']['ref'] != pr['head']['ref']
                or not re.search(rf'(?im)^Closes #{issue}\s*$', fresh.get('body') or '')):
            raise RuntimeError('PR moved during handoff; no further mutation')
        context = config.get('_qualification_context')
        expected_base = context['base'] if context else preflight['base'] if preflight else None
        if expected_base is not None and fresh.get('base', {}).get('sha') != expected_base:
            raise RuntimeError('PR base changed during handoff; no further mutation')
        current = api(issue_path)
        labels = {l['name'] for l in current['labels']}
        if ('pull_request' in current or (development_policy is not None
                and not development.canonical_issue_target(args.repo, issue, current))):
            raise RuntimeError('issue is no longer the verified canonical target; no further mutation')
        if current['state'] != 'open' or 'blocked' in labels:
            raise RuntimeError('issue lifecycle changed during handoff')
        if require_managed and not labels & {'todo', 'doing', 'done'}:
            raise RuntimeError('issue is unmanaged; diagnostics recorded; lifecycle handoff remains incomplete')
        if failure_only and 'done' in labels:
            raise RuntimeError('failure-only mode cannot touch a done issue')
        return labels

    if failed_jobs:
        # Diagnostics have no ownership authority. An open canonical issue
        # can receive them even when its lifecycle labels need reconciliation.
        development.report_failures(args.repo, issue, failed_jobs, comments, write,
                                    lambda: fresh_labels(require_managed=False))
    if not labels & {'todo', 'doing', 'done'}:
        raise RuntimeError('issue is unmanaged; diagnostics recorded; lifecycle handoff remains incomplete')
    policy = hashlib.sha256(json.dumps(source_config, sort_keys=True).encode()).hexdigest()
    # A failure hands a head back once per coding epoch, regardless of how
    # many more workflows fail while AGY is fixing it. A successfully APPLIED
    # done receipt begins a new epoch, allowing a later done -> todo regression.
    def applied(outcome, current_policy=True):
        return [c for c in comments
                if c['user']['login'] == 'github-actions[bot]'
                and c['body'].startswith('<!-- issue-ci:')
                and c['body'].splitlines()[0].endswith(':applied -->')
                and f'\nCI: {outcome} for PR #{args.pr} head `{args.head}`.\n' in c['body']
                and (f'\nPolicy: {policy}\n' in c['body'] if current_policy
                     else re.search(r'(?m)^Policy: [0-9a-f]{64}$', c['body']))]
    qualified = applied('done')
    # Old policy receipts establish a handoff, never current qualification.
    # An upgraded runtime cannot grant itself a worker's existing ownership.
    local_qualified, local_returned = applied('done', False), applied('todo', False)

    def local_hold(labels):
        if source_config.get('completion_owner') != 'local':
            return None
        lifecycle = labels & {'todo', 'doing', 'done'}
        if state == 'done' and 'done' not in lifecycle and local_qualified:
            return 'local completion hold: this head was already handed to local review; preserve its returned state'
        if lifecycle == {'doing'} and (local_qualified or local_returned):
            return 'local completion hold: returned work was claimed; a CI rerun cannot replace that owner'
        return None
    epoch = qualified[-1]['id'] if qualified else 0
    receipt_key = evidence if state == 'done' else ['coding-epoch', epoch]
    # A success receipt binds the current base/files/applicability fingerprint.
    # Failure keeps the stable source policy/coding epoch, so a changed base
    # cannot undo an AGY claim after an already applied failure handoff.
    receipt_config = config if state == 'done' else source_config
    digest = hashlib.sha256(json.dumps([args.head, receipt_key, receipt_config, state], sort_keys=True).encode()).hexdigest()
    prefix = f'<!-- issue-ci:{digest}:'
    journals = [c for c in comments if c['body'].startswith(prefix) and c['user']['login'] == 'github-actions[bot]']
    journal = journals[-1] if journals else None
    phase = journal['body'].split(prefix, 1)[1].split(' -->', 1)[0] if journal else 'pending'
    if failure_only and journal is None and labels & {'todo', 'doing', 'done'} != {'doing'}:
        return 0  # Only doing -> todo; existing journals may finish recovery.
    if phase not in ('pending', 'ready', 'applied'):
        raise RuntimeError('invalid handoff journal phase')
    detail = '\n'.join(reasons) if reasons else 'All explicitly required workflow/job gates passed.'
    urls = '\n'.join(s[0]['html_url'] for s in snapshots.values())

    def body(phase):
        return f'{prefix}{phase} -->\nCI: {state} for PR #{args.pr} head `{args.head}`.\nPolicy: {policy}\n{detail}\n{urls}'

    def writable_labels():
        labels = fresh_labels()
        hold = local_hold(labels)
        if hold:
            raise RuntimeError(hold)
        return labels

    # Diagnostics still reach the issue. Only the lifecycle handoff is held.
    # Pending/ready journals are not completed receipts and retain recovery.
    if source_config.get('completion_owner') == 'local':
        hold = local_hold(fresh_labels())
        if hold:
            print(hold)
            return 0
    if phase == 'applied':
        print('duplicate applied head/run handoff; no lifecycle mutation')
        return 0
    labels = writable_labels()
    if journal is None:
        phase = 'ready' if state in labels else 'pending'
        journal = write(f'{issue_path}/comments', 'POST', {'body': body(phase)})
    comment_path = f'{root}/issues/comments/{journal["id"]}'
    # A pending intent never suppresses a retry after head/snapshot/API failures.
    labels = writable_labels()
    if snapshot(args.repo, args.head, config) != snapshots:
        raise RuntimeError('CI runs changed during evaluation; pending intent is retryable')
    if untrusted_failure_snapshots(args.repo, args.head, config) != negative_snapshots:
        raise RuntimeError('untrusted CI failure changed during evaluation; no label mutation')
    if development_policy is not None:
        from types import SimpleNamespace
        adapter = SimpleNamespace(api=api, pages=pages)
        if (development.preflight(adapter, args.repo, args.pr, args.head) != preflight
                or development.review_failure_snapshot(adapter, args.repo, args.pr, args.head, review_config) != development_snapshots):
            raise RuntimeError('development input or failed review attempt changed; no label mutation')
    if 'qualification' in source_config:
        current_pr = api(pr_path)
        if (current_pr['state'] != 'open' or current_pr['head']['sha'] != args.head
                or qualify_config(args.repo, current_pr, source_config) != config):
            raise RuntimeError('PR applicability changed during evaluation; no label mutation')
    if phase == 'pending':
        # Snapshot pagination can outlast a lifecycle transition. Recheck after
        # that read, immediately before adding the target label.
        labels = writable_labels()
        if state not in labels:
            write(f'{issue_path}/labels', 'POST', {'labels': [state]})
        # Durable proof that the desired label was added BEFORE deleting old
        # labels. Workers refuse mixed states, so they cannot claim between
        # the add and this checkpoint, even if the controller crashes.
        write(comment_path, 'PATCH', {'body': body('ready')})
    labels = writable_labels()
    if state in labels:
        for old in sorted((labels & {'todo', 'doing', 'done'}) - {state}):
            live = writable_labels()
            if state not in live:
                break
            if old in live:
                write(f'{issue_path}/labels/{old}', 'DELETE')
    # ready + desired label absent means a later actor already consumed it
    # (e.g. AGY claimed todo -> doing). Never add it again or undo that claim.
    write(comment_path, 'PATCH', {'body': body('applied')})
    return 0


def reconcile(args, config):
    """Every wake scans all managed open PRs: a replaced pending wake loses none."""
    root = f'repos/{args.repo}'
    event = api(f'{root}/actions/runs/{args.run_id}')
    if (event.get('event') != 'pull_request'
            or event.get('head_repository', {}).get('full_name') != args.repo
            or event.get('path') not in {w['path'] for w in config['workflows']}):
        raise RuntimeError('unconfigured/foreign reconciliation wake')
    errors = []
    for pr in pages(f'{root}/pulls?state=open'):
        match = re.fullmatch(r'(?:issue/|agy/issue-)([0-9]+)', pr['head']['ref'])
        issue = qualification_module().linked_issue(pr) if development_module().validate(config) else (int(match[1]) if match else None)
        if (issue is None or (pr['head'].get('repo') or {}).get('full_name') != args.repo
                or not re.search(rf'(?im)^Closes #{issue}\s*$', pr.get('body') or '')):
            continue
        current = argparse.Namespace(**vars(args))
        current.pr, current.head, current.apply = pr['number'], pr['head']['sha'], True
        try:
            # Event metadata only wakes us. All decisions use fresh current-head
            # snapshots, including PRs unrelated to the triggering run.
            process_head(current, config, check_event=False)
        except (RuntimeError, subprocess.CalledProcessError, OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(f'PR #{current.pr}: {exc}')
    if errors:
        raise RuntimeError('; '.join(errors))
    return 0


def development_report(repo, document, evaluator, run_id):
    """Default-branch CI label writer. Has no review, merge or close operation."""
    from types import SimpleNamespace
    import base64
    import os
    if (not isinstance(repo,str) or not re.fullmatch(r'[\w.-]+/[\w.-]+',repo)
            or type(run_id)is not int or run_id<=0
            or not isinstance(evaluator,str) or not re.fullmatch('[0-9a-f]{40}',evaluator)):
        raise RuntimeError('invalid development reporter input')
    config=validate_adoption(validate_config(document['config']))
    if config.get('completion_owner')!='local':
        raise RuntimeError('development reporter requires explicit local completion')
    source=document['source_commit'];spec=config.get('development_reporter',{})
    if (not isinstance(spec,dict) or set(spec)!= {'path','id','definition_sha','job','link_job'}
            or type(spec['id'])is not int or spec['id']<=0
            or not isinstance(spec['path'],str) or not re.fullmatch(r'\.github/workflows/[\w.-]+\.ya?ml',spec['path'])
            or not isinstance(spec['definition_sha'],str) or not re.fullmatch('[0-9a-f]{40}',spec['definition_sha'])
            or any(not isinstance(spec[key],str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]*',spec[key])
                   for key in ('job','link_job')) or spec['job']==spec['link_job']):
        raise RuntimeError('development reporter identity is missing or invalid')
    event=os.environ.get('GITHUB_EVENT_NAME');role=os.environ.get('GITHUB_JOB')
    if (event not in ('workflow_run','push','issues','repository_dispatch')
            or os.environ.get('GITHUB_ACTIONS')!='true'
            or os.environ.get('GITHUB_REPOSITORY')!=repo
            or os.environ.get('GITHUB_WORKFLOW_SHA')!=source
            or os.environ.get('AW_ACTION_REF')!=evaluator or role not in (spec['job'],spec['link_job'])):
        raise RuntimeError('development reporter must use its current trusted default source')
    root=f'repos/{repo}';default=api(root)['default_branch']
    run=api(f'{root}/actions/runs/{run_id}')
    if (run.get('id')!=run_id or run.get('workflow_id')!=spec['id'] or run.get('path')!=spec['path']
            or run.get('event')!=event or run.get('head_sha')!=source
            or run.get('head_branch')!=default or run.get('head_repository',{}).get('full_name')!=repo
            or str(run.get('run_attempt'))!=os.environ.get('GITHUB_RUN_ATTEMPT')):
        raise RuntimeError('development reporter run does not match its deployed identity')
    data=api(f'{root}/contents/{spec["path"]}?ref={source}')
    if data.get('type')!='file' or data.get('encoding')!='base64':
        raise RuntimeError('development reporter source is not a file')
    raw=base64.b64decode(''.join(data['content'].split()),validate=True)
    if (len(raw)>262144 or data.get('sha')!=spec['definition_sha']
            or hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()!=spec['definition_sha']):
        raise RuntimeError('development reporter source changed')
    workflow=development_module().yaml_document(raw.decode())
    permissions={'actions':'read','contents':'read','pull-requests':'read','issues':'write'}
    job=workflow.get('jobs',{}).get(spec['job'],{})
    link_job=workflow.get('jobs',{}).get(spec['link_job'],{})
    link_permissions={'actions':'read','contents':'read','pull-requests':'write','issues':'read'}
    links_only=role==spec['link_job']
    expected={'uses':f'faberline/aw/.github/actions/issue-ci@{evaluator}',
              'with':{'operation':'development-report'}}
    if (workflow.get('permissions')!={} or set(workflow.get('jobs',{}))!={spec['job'],spec['link_job']}
            or set(job)!={'runs-on','timeout-minutes','permissions','steps','needs','if'}
            or job.get('permissions')!=permissions or job.get('steps')!=[expected]
            or job.get('needs')!=spec['link_job'] or job.get('if')!='${{ always() }}'
            or set(link_job)!={'runs-on','timeout-minutes','permissions','steps'}
            or link_job.get('permissions')!=link_permissions or link_job.get('steps')!=[expected]):
        raise RuntimeError('development reporter requires separate source-PR-comment and issue-label permissions')
    if workflow.get('concurrency')!={'group':'issue-ci-handoff','cancel-in-progress':'false'}:
        raise RuntimeError('development reporter must retain the single-writer mutex')
    results=[];original_api=globals()['api']
    def current_source():
        if original_api(f'{root}/commits/{quote(default,safe="")}').get('sha')!=source:
            raise RuntimeError('default source moved before failure reporting')
    current_source()
    pulls=pages(f'{root}/pulls?state=open')
    for pull in pulls:
        if ((pull.get('head',{}).get('repo')or{}).get('full_name')!=repo
                or pull.get('base',{}).get('ref')!=default):
            continue
        number,head=pull['number'],pull['head']['sha']
        linking_metadata=(pull.get('title'),pull.get('body'))
        issue=qualification_module().linked_issue(pull)
        def failure_api(path,method='GET',data=None):
            if method!='GET':
                source_diagnostic=(links_only and method=='POST' and path==f'{root}/issues/{number}/comments'
                                   and isinstance(data,dict) and set(data)=={'body'})
                allowed=source_diagnostic
                if not links_only:
                    allowed|=(method=='POST' and path==f'{root}/issues/{issue}/comments'
                              and isinstance(data,dict) and set(data)=={'body'})
                    allowed|=(method=='PATCH' and bool(re.fullmatch(re.escape(root)+r'/issues/comments/[1-9][0-9]*',path))
                              and isinstance(data,dict) and set(data)=={'body'})
                    allowed|=(method=='POST' and path==f'{root}/issues/{issue}/labels' and data=={'labels':['todo']})
                    allowed|=(method=='DELETE' and path in tuple(f'{root}/issues/{issue}/labels/{label}'for label in ('todo','doing','done')) and data is None)
                if not links_only and method=='POST' and path==f'{root}/issues/{issue}/labels' and data=={'labels':['done']}:
                    # done is only a review-ready label. Recheck all necessary
                    # current-input CI before allowing even this label write.
                    current=original_api(f'{root}/pulls/{number}')
                    if (current.get('state')!='open' or current.get('head',{}).get('sha')!=head
                            or qualification_module().linked_issue(current)!=issue):
                        raise RuntimeError('PR moved before the done label handoff')
                    scoped=qualify_config(repo,current,config)
                    if evaluate_policy(scoped,snapshot(repo,head,scoped))[0]!='done':
                        raise RuntimeError('all necessary current-input CI must pass before the done label')
                    allowed=True
                if not allowed:raise RuntimeError('development CI writer cannot merge, close or write unrelated state')
                current=original_api(f'{root}/pulls/{number}')
                if (current.get('state')!='open' or current.get('head',{}).get('sha')!=head
                        or (current.get('head',{}).get('repo')or{}).get('full_name')!=repo
                        or current.get('base',{}).get('ref')!=default):
                    raise RuntimeError('source PR moved before development handoff')
                if source_diagnostic:
                    if (current.get('title'),current.get('body'))!=linking_metadata:
                        raise RuntimeError('source PR linking metadata changed before diagnostic')
                    target=qualification_module().linked_issue(current)
                    if target is not None and development_module().canonical_issue_target(repo,target,
                            development_module().read_issue_target(SimpleNamespace(api=original_api),repo,target)):
                        raise RuntimeError('source PR now has a canonical open issue; no diagnostic')
                else:
                    if (qualification_module().linked_issue(current)!=issue
                            or not development_module().canonical_issue_target(repo,issue,
                                development_module().read_issue_target(SimpleNamespace(api=original_api),repo,issue))):
                        raise RuntimeError('canonical issue changed before development handoff')
                current_source()
            return original_api(path,method,data)
        try:
            globals()['api']=failure_api
            if issue is None or not development_module().canonical_issue_target(repo,issue,
                    development_module().read_issue_target(SimpleNamespace(api=original_api),repo,issue)):
                if links_only:
                    development_module().report_missing_issue(SimpleNamespace(api=failure_api,pages=pages),repo,pull,current_source)
            elif not links_only:
                args=argparse.Namespace(repo=repo,pr=number,head=head,apply=True,run_id=None)
                process_head(args,config,check_event=False,before_mutation=current_source)
            results.append(dict(pr=number,head=head,state='processed'))
        except (RuntimeError,subprocess.CalledProcessError,OSError,ValueError,KeyError,TypeError)as error:
            results.append(dict(pr=number,head=head,state='held',reason=str(error)))
        finally:
            globals()['api']=original_api
    print('AW_DEVELOPMENT_FAILURE_REPORT_V1 '+json.dumps(dict(repo=repo,source=source,run=run_id,role=role,results=results),sort_keys=True))
    return 2 if any(row['state']=='held'for row in results)else 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', required=True)
    parser.add_argument('--config', required=True, help='trusted default-branch config, never from PR')
    parser.add_argument('--pr', type=int)
    parser.add_argument('--head')
    parser.add_argument('--run-id', type=int)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--reconcile', action='store_true', help='trusted wake: reconcile every managed current PR head')
    args = parser.parse_args()
    if not re.fullmatch(r'[\w.-]+/[\w.-]+', args.repo):
        raise RuntimeError('invalid repository')
    config = load_config(args.config)
    if args.reconcile:
        if args.run_id is None or args.pr is not None or args.head is not None:
            raise RuntimeError('--reconcile requires --run-id and scans PRs itself')
        return reconcile(args, config)
    if args.pr is None or not re.fullmatch('[0-9a-f]{40}', args.head or ''):
        raise RuntimeError('--pr and valid --head required')
    return process_head(args, config)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (RuntimeError, subprocess.CalledProcessError, OSError, ValueError, KeyError, TypeError) as exc:
        print(f'refused: {exc}', file=sys.stderr)
        sys.exit(2)
