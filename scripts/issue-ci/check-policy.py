#!/usr/bin/env python3
"""Validate retained legacy policy or the explicit local completion contract."""
import importlib.util
from pathlib import Path
import sys


def load_controller():
    path = Path(__file__).with_name('handoff.py')
    spec = importlib.util.spec_from_file_location('issue_ci_handoff', path)
    controller = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(controller)
    return controller


def check(path):
    controller = load_controller()
    if not all(callable(getattr(controller, name, None)) for name in ('load_config', 'evaluate_policy')):
        raise RuntimeError('failure-only rollout requires the reviewed policy-aware controller')
    policy = controller.load_config(path)
    if policy.get('completion_owner') == 'local':
        controller.validate_adoption(policy)
        if policy.get('lifecycle_mode') != 'full' or controller.development_module().validate(policy) is None:
            raise RuntimeError('local completion needs explicit full qualification and development stages')
    else:
        if policy.get('lifecycle_mode') != 'failure-only':
            raise RuntimeError('legacy rollout requires explicit failure-only mode')
        if not isinstance(policy.get('qualification_blockers'), list) or not policy['qualification_blockers']:
            raise RuntimeError('legacy rollout requires nonempty qualification blockers')
    return policy


if __name__ == '__main__':
    check(sys.argv[1])
