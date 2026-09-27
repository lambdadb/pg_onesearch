#!/usr/bin/env python3
"""Opt-in live Custom Scan probe. Creates two owned fixtures and removes them afterward."""
import argparse
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'spikes/live_compatibility'))
from client import Client, ProbeError, load_settings, require
from run import Experiment, fixtures, git, save_report, text_query, utc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    require(not args.report.exists(), 'Refusing to overwrite a report')
    settings = load_settings(args.env_file)
    files = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
             for directory in ('remote_read', 'executor')
             for p in sorted((ROOT / 'spikes' / directory).glob('*')) if p.is_file()}
    image_id = subprocess.check_output(['docker', 'image', 'inspect', 'pg_onesearch:executor',
                                        '--format', '{{.Id}}'], text=True).strip()
    # Bind evidence to the built container, not just to the current worktree.
    image_files = json.loads(subprocess.check_output(
        ['docker', 'run', '--rm', '--network', 'none', '--entrypoint', 'python3', image_id, '-c',
         'import hashlib,json,pathlib; print(json.dumps({str(p):hashlib.sha256(p.read_bytes()).hexdigest() '
         'for directory in ("remote_read", "executor") for p in pathlib.Path("spikes",directory).glob("*") if p.is_file() '
         'and p.suffix not in (".o", ".bc", ".so")}))'], text=True))
    require(files == image_files, 'Probe image differs from source; rebuild before live execution')
    report = {'run_id': uuid.uuid4().hex, 'started_at': utc(), 'status': 'running',
              'source_revision': git('rev-parse', 'HEAD'),
              'worktree_dirty': bool(git('status', '--porcelain')),
              'image_id': image_id, 'files_sha256': files,
              'checks': [], 'writes': []}
    experiment = Experiment(Client(settings), report, 360)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    experiment.persist = lambda: save_report(args.report, report)
    experiment.persist()
    container = 'pgos-executor-' + report['run_id'][:16]

    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        cases = []
        for role in ('vector', 'bm25'):
            resource = experiment.create(role)
            rows = [dict(row, id=str(i)) for i, row in enumerate(fixtures().values(), 1)]
            experiment.write(resource, 'upsert', rows)
            ref = experiment.barrier(resource, 'c-read')
            references = {}
            if role == 'bm25':
                for term in ('alpha', 'beta', 'gamma'):
                    _, docs = experiment.query(resource, text_query(term), ref, size=100)
                    references[term] = experiment.scores(docs)
            cases.append({'role': role, 'collection': resource['name'], 'tag': ref['name'],
                          'references': references})
        # Pipe credentials after container creation, not via -e/--env-file/command args.
        result = subprocess.run(['docker', 'run', '--rm', '-i', '--name', container,
                                 '--platform', 'linux/arm64', '--entrypoint', 'python3',
                                 image_id, 'spikes/executor/bootstrap.py'],
                                input=json.dumps({'settings': settings, 'cases': cases}),
                                text=True, capture_output=True, timeout=360)
        lines = [line.removeprefix('PGOS_RESULT=') for line in result.stdout.splitlines()
                 if line.startswith('PGOS_RESULT=')]
        require(bool(lines), 'Container returned no redacted probe result')
        report['executor'] = json.loads(lines[-1])
        require(result.returncode == 0 and report['executor']['status'] == 'passed',
                'Custom Scan comparison failed')
        report['status'] = 'passed'
        print('PASS: Custom Scan vector/BM25 results, prepared plans, and correlated rescans', flush=True)
    except (ProbeError, KeyboardInterrupt) as exc:
        report['status'] = 'failed'
        report['failure'] = str(exc) if isinstance(exc, ProbeError) else 'Interrupted'
    except Exception as exc:
        report['status'] = 'failed'
        report['failure'] = 'Unexpected ' + type(exc).__name__
    finally:
        # Also remove a still-running container after timeout/interruption; no secrets in args.
        try:
            subprocess.run(['docker', 'rm', '-f', container], capture_output=True, timeout=30)
        except (OSError, subprocess.TimeoutExpired):
            report['container_cleanup'] = 'unconfirmed'
            report['status'] = 'failed'
        experiment.cleanup()
        if any(r['cleanup'] != 'confirmed_absent' for r in experiment.resources):
            report['status'] = 'failed'
        report['finished_at'] = utc()
        experiment.persist()
    print('RESULT: ' + report['status'], flush=True)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except ProbeError as exc:
        print('FAIL: ' + str(exc), file=sys.stderr)
        sys.exit(1)
