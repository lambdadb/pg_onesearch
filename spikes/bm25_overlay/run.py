#!/usr/bin/env python3
"""Run the independent Lucene experiment and optionally persist source-bound evidence."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
INPUTS = ('OverlayProbe.java', 'Dockerfile', 'SHA256SUMS', 'run.py')
CASES = {
    'empty_overlay', 'delete_only', 'nonmatching_replacement', 'insert_new_term',
    'replace_frequency_length', 'mixed_changes', 'retained_hard_deletes', 'all_deleted',
    'empty_text_and_unknown_delete', 'removed_only_term', 'no_match', 'outside_old_top_k',
    'partition_and_private_requests', 'bounds_and_failure_cleanup',
}


def output(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if args.report and args.report.exists():
        raise RuntimeError('Refusing to overwrite an evidence report')
    hashes = {name: hashlib.sha256((ROOT/'spikes/bm25_overlay'/name).read_bytes()).hexdigest()
              for name in INPUTS}
    image = output('docker', 'image', 'inspect', 'pg_onesearch:bm25-overlay', '--format', '{{.Id}}')
    actual = output('docker', 'run', '--rm', '--network', 'none', '--entrypoint', 'sha256sum', image, *INPUTS)
    image_hashes = {line.split()[1]: line.split()[0] for line in actual.splitlines()}
    if hashes != image_hashes:
        raise RuntimeError('Image/source inputs differ; rebuild before recording evidence')
    started = datetime.now(timezone.utc).isoformat()
    process = subprocess.run(['docker', 'run', '--rm', '--network', 'none', image],
                             cwd=ROOT, text=True, capture_output=True, timeout=60)
    if process.returncode:
        raise RuntimeError('Lucene experiment failed:\n' + process.stderr + process.stdout)
    tests = [json.loads(line) for line in process.stdout.splitlines()]
    if len(tests) != len(CASES) or {t['case'] for t in tests} != CASES or any(t['status'] != 'passed' for t in tests):
        raise RuntimeError('Incomplete experiment output')
    runtime = subprocess.run(['docker', 'run', '--rm', '--network', 'none', '--entrypoint', 'java', image, '-version'],
                             text=True, capture_output=True, check=True)
    report = {
        'status': 'passed', 'scope': 'independent local Lucene experiment; no LambdaDB or PostgreSQL execution',
        'started_at': started, 'finished_at': datetime.now(timezone.utc).isoformat(),
        'source_revision': output('git', 'rev-parse', 'HEAD'),
        'worktree_dirty': bool(output('git', 'status', '--porcelain')),
        'image_id': image, 'files_sha256': hashes, 'java': runtime.stderr.strip(),
        'lucene_version': '10.4.0', 'score_absolute_tolerance': 0.000001,
        'max_base_physical_documents': 64, 'max_effective_documents': 64,
        'tests': tests,
    }
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive creation prevents an accidental replacement of previous evidence.
        with args.report.open('x') as file:
            file.write(json.dumps(report, indent=2) + '\n')
    for test in tests:
        print('PASS:', test['case'])
    print(f'PASS: {len(tests)} Lucene overlay cases; image inputs match source')


if __name__ == '__main__':
    main()
