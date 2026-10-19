#!/usr/bin/env python3
"""Replay the changed PR tiers on identical fixed sources; preserve every result."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parent.parent
INTEGRATION = [
    'catalog_tests', 'extension_tests', 'monitoring_tests', 'smoke_tests',
    'resilience_tests', 'scenario_tests', 'workflow_tests', 'property_tests',
]


def commands(allocation, tier):
    if tier.startswith('light-'):
        return [['bash', 'scripts/run_light_e2e_tests.sh', '--shard-count', '3',
                 '--shard-index', tier[-1]]]
    if tier == 'integration':
        base = ['cargo', 'nextest', 'run', '--profile', 'ci', '--retries', '0']
        for target in INTEGRATION:
            base.extend(['--test', target])
        result = [base]
        if allocation == 'after':
            result.append(['bash', 'scripts/run_dvm_integration_tests.sh', 'pg18'])
        return result
    if allocation == 'before':
        result = [
            ['./scripts/run_e2e_tests.sh', '--profile', 'ci', '--retries', '0',
             '--no-fail-fast', '--test', 'e2e_*', '-E',
             'not binary(=e2e_refresh_atomicity_tests)'],
            ['./scripts/run_e2e_tests.sh', '--profile', 'ci', '--retries', '0',
             '--test', 'e2e_refresh_atomicity_tests', '--test-threads', '1'],
        ]
    else:
        result = [['python3', 'scripts/ci_pr_suite.py']]
    return result + [['bash', 'scripts/run_failure_recovery_repetition.sh']]


def output(command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('allocation', choices=['before', 'after'])
    parser.add_argument('tier', choices=['integration', 'light-1', 'light-2', 'light-3', 'full'])
    parser.add_argument('--list', action='store_true')
    args = parser.parse_args()
    selected = commands(args.allocation, args.tier)
    if args.list:
        print(json.dumps(selected, indent=2))
        return 0
    os.environ['NEXTEST_RETRIES'] = '0'
    if (ROOT / 'target/debug').exists():
        raise RuntimeError('comparison requires a cold workspace build directory')
    directory = ROOT / 'target/allocation-comparison'
    directory.mkdir(parents=True, exist_ok=True)
    os.environ['FAILURE_RECOVERY_ARTIFACT_DIR'] = str(directory / 'recovery')
    identity = {
        'allocation': args.allocation, 'tier': args.tier,
        'comparison_sha': output(['git', 'rev-parse', 'HEAD']),
        'sources': output(['git', 'ls-tree', 'HEAD', 'src', 'tests', 'verification',
                           'Cargo.toml', 'Cargo.lock', 'pg_trickle.control']),
        'rustc': output(['rustc', '--version']),
        'nextest': output(['cargo', 'nextest', '--version']),
        'postgresql': output(['pg_config', '--version']),
        'runner_image': os.environ.get('ImageOS'),
        'runner_image_version': os.environ.get('ImageVersion'),
        'package_checksums': (ROOT / 'comparison-inputs/package.sha256').read_text(),
        'image_checksums': (ROOT / 'comparison-inputs/images.sha256').read_text(),
        'build_cache': 'disabled', 'retries': 0,
    }
    (directory / 'identity.json').write_text(json.dumps(identity, indent=2) + '\n')
    results = []
    for index, command in enumerate(selected, 1):
        start = time.monotonic()
        with (directory / f'step-{index}.log').open('w') as log:
            result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        results.append({'command': command, 'exit_code': result.returncode,
                        'seconds': round(time.monotonic() - start, 3)})
        print(json.dumps(results[-1]), flush=True)
        (directory / 'steps.json').write_text(json.dumps(results, indent=2) + '\n')
    return 0 if all(step['exit_code'] == 0 for step in results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
