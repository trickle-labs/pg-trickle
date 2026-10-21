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
SERIAL_CASES = [
    ("e2e_rls_wal_tests", "test_rls_bypassrls_owner_differential_wal_matches_query"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_before_receipt_commit_preserves_committed_changes"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_after_receipt_commit_before_ack_converges_once"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_after_apply_before_ack_does_not_reapply"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_after_slot_advance_finishes_acknowledgement"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_trigger_fallback_received_receipt_requires_rebaseline"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_trigger_fallback_undecoded_commit_requires_rebaseline"),
    ("e2e_wal_receipt_recovery_tests", "test_wal_recovery_semantic_controls_are_detected"),
]


def serial_case_command():
    command = ['./scripts/run_e2e_tests.sh', '--profile', 'ci', '--retries', '0',
               '--no-fail-fast', '--test-threads', '1']
    for binary in dict.fromkeys(binary for binary, _ in SERIAL_CASES):
        command.extend(['--test', binary])
    command.extend(['-E', ' | '.join(f'test(={test})' for _, test in SERIAL_CASES)])
    return command


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
             ' & '.join([
                 'not binary(=e2e_refresh_atomicity_tests)',
                 *(f'not (binary(={binary}) & test(={test}))' for binary, test in SERIAL_CASES),
             ])],
            ['./scripts/run_e2e_tests.sh', '--profile', 'ci', '--retries', '0',
             '--test', 'e2e_refresh_atomicity_tests', '--test-threads', '1'],
            serial_case_command(),
        ]
    else:
        result = [['python3', 'scripts/ci_pr_suite.py', '--no-fail-fast']]
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
    if not os.environ.get('CI_ALLOCATION_CONTAINER', '').startswith('sha256:'):
        raise RuntimeError('comparison requires the frozen toolchain container')
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
        'workload_image': os.environ['CI_ALLOCATION_CONTAINER'],
        'container_os': output(['cat', '/etc/os-release']),
        'host_kernel': output(['uname', '-r']),
        'host_docker': output(['docker', 'version', '--format', '{{.Server.Version}}']),
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
