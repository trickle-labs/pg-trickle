#!/usr/bin/env python3
"""Validate a completed paired replay before reporting any execution-cost difference."""

import argparse
import json
from pathlib import Path
import re

from ci_run_metrics import summarize

TIERS = {'integration', 'light-1', 'light-2', 'light-3', 'full'}
IDENTITY_FIELDS = (
    'comparison_sha', 'sources', 'rustc', 'nextest', 'postgresql',
    'runner_image', 'runner_image_version', 'package_checksums', 'image_checksums',
)


def compare(run, jobs, artifacts):
    if run['status'] != 'completed':
        raise ValueError('wait for the entire comparison workflow to complete')
    failed = [job['name'] for job in jobs if job['conclusion'] not in ('success', 'skipped')]
    report = {'url': run['html_url'], 'sha': run['head_sha'], 'valid': False, 'errors': []}
    if run['conclusion'] != 'success' or failed:
        report['errors'].append(f'workflow result={run["conclusion"]}; failed jobs={failed}')
        return report
    metrics = summarize(run, jobs)
    groups = {'before': [], 'after': []}
    identities = []
    seen = set()
    for row in metrics['jobs']:
        match = re.search(r'Replay \((before|after), ([^)]+)\)$', row['name'])
        if not match:
            continue
        allocation, tier = match.groups()
        if (allocation, tier) in seen or tier not in TIERS:
            report['errors'].append(f'duplicate or unexpected replay: {row["name"]}')
        seen.add((allocation, tier))
        folder = artifacts / f'allocation-{allocation}-{tier}'
        try:
            identity = json.loads((folder / 'identity.json').read_text())
            steps = json.loads((folder / 'steps.json').read_text())
        except (OSError, ValueError) as error:
            report['errors'].append(f'missing/invalid evidence for {allocation}/{tier}: {error}')
            continue
        if identity['allocation'] != allocation or identity['tier'] != tier:
            report['errors'].append(f'wrong artifact identity for {allocation}/{tier}')
        if identity.get('comparison_sha') != run['head_sha']:
            report['errors'].append(f'artifact belongs to a different commit for {allocation}/{tier}')
        if identity['build_cache'] != 'disabled' or identity['retries'] != 0:
            report['errors'].append(f'cache/retry policy mismatch for {allocation}/{tier}')
        if not steps or any(step['exit_code'] != 0 for step in steps):
            report['errors'].append(f'failed or missing steps for {allocation}/{tier}')
        identities.append(identity)
        groups[allocation].append({**row, 'tier': tier,
                                   'workload_minutes': sum(step['seconds'] for step in steps) / 60})
    expected = {(allocation, tier) for allocation in groups for tier in TIERS}
    if seen != expected:
        report['errors'].append(f'missing replays: {sorted(expected - seen)}')
    for field in IDENTITY_FIELDS:
        values = {identity.get(field) for identity in identities}
        if len(values) != 1 or None in values or '' in values:
            report['errors'].append(f'mismatched or absent {field}')
    report['allocations'] = {}
    for allocation, rows in groups.items():
        report['allocations'][allocation] = {
            'jobs': rows, 'job_count': len(rows),
            'execution_minutes': round(sum(row['execution_minutes'] for row in rows), 3),
            'creation_to_start_minutes': round(sum(row['creation_to_start_minutes'] for row in rows), 3),
            'workload_minutes': round(sum(row['workload_minutes'] for row in rows), 3),
        }
    report['shared_jobs'] = [row for row in metrics['jobs'] if not re.search(r'Replay \(', row['name'])]
    report['valid'] = not report['errors']
    if report['valid']:
        before = report['allocations']['before']['execution_minutes']
        after = report['allocations']['after']['execution_minutes']
        report['observed_execution_minutes_saved'] = round(before - after, 3)
        report['observed_execution_percent_saved'] = round(100 * (before - after) / before, 2)
        report['matched_identity'] = {field: identities[0][field] for field in IDENTITY_FIELDS}
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', type=Path)
    parser.add_argument('jobs', type=Path)
    parser.add_argument('artifacts', type=Path)
    args = parser.parse_args()
    pages = json.loads(args.jobs.read_text())
    if isinstance(pages, dict):
        pages = [pages]
    jobs = [job for page in pages for job in page['jobs']]
    report = compare(json.loads(args.run.read_text()), jobs, args.artifacts)
    print(json.dumps(report, indent=2))
    return 0 if report['valid'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
