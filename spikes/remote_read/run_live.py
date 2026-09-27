#!/usr/bin/env python3
"""Opt-in live C read probe. Creates two owned fixtures and removes them afterward."""
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
from run import Experiment, fixtures, git, knn, save_report, text_query, utc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--large-results', action='store_true', help='Also verify C download of 100 large documents')
    args = parser.parse_args()
    require(not args.report.exists(), 'Refusing to overwrite a report')
    settings = load_settings(args.env_file)
    files = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in sorted((ROOT / 'spikes/remote_read').glob('*')) if p.is_file()}
    image_id = subprocess.check_output(['docker', 'image', 'inspect', 'pg_onesearch:remote-read',
                                        '--format', '{{.Id}}'], text=True).strip()
    # Bind evidence to the built container, not just to the current worktree.
    image_files = json.loads(subprocess.check_output(
        ['docker', 'run', '--rm', '--network', 'none', '--entrypoint', 'python3', image_id, '-c',
         'import hashlib,json,pathlib; print(json.dumps({str(p):hashlib.sha256(p.read_bytes()).hexdigest() '
         'for p in pathlib.Path("spikes/remote_read").glob("*") if p.is_file() '
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
    container = 'pgos-read-' + report['run_id'][:16]

    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        cases = []
        for role in ('vector', 'bm25'):
            resource = experiment.create(role)
            experiment.write(resource, 'upsert', list(fixtures().values()))
            ref = experiment.barrier(resource, 'c-read')
            query = knn(k=10) if role == 'vector' else text_query('alpha')
            response, docs = experiment.query(resource, query, ref, size=10, includeVectors=True)
            cases.append({'role': role, 'collection': resource['name'], 'tag': ref['name'],
                          'query': query, 'scores': experiment.scores(docs),
                          'size': 10, 'offloaded': not response['isDocsInline']})
            if role == 'bm25' and args.large_results:
                payload = 'x' * 80000
                rows = [{'id': f'large-{i:03}', 'content': 'payload alpha',
                         'cohort': 'large', 'payload': payload} for i in range(100)]
                experiment.write(resource, 'upsert', rows)
                large_ref = experiment.barrier(resource, 'c-download')
                response, docs = experiment.query(resource, text_query('payload'), large_ref, size=100)
                require(not response['isDocsInline'] and len(docs) == 100,
                        'Large fixture must exercise an offloaded 100-result response')
                require(all(item['doc'].get('payload') == payload for item in docs),
                        'Python reference payload mismatch')
                cases.append({'role': 'bm25-download', 'collection': resource['name'],
                              'tag': large_ref['name'], 'query': text_query('payload'),
                              'scores': experiment.scores(docs), 'size': 100, 'offloaded': True,
                              'payload_length': len(payload),
                              'payload_sha256': hashlib.sha256(payload.encode()).hexdigest()})
        # Pipe credentials after container creation, not via -e/--env-file/command args.
        result = subprocess.run(['docker', 'run', '--rm', '-i', '--name', container,
                                 '--platform', 'linux/arm64', '--entrypoint', 'python3',
                                 image_id, 'spikes/remote_read/bootstrap.py'],
                                input=json.dumps({'settings': settings, 'cases': cases}),
                                text=True, capture_output=True, timeout=360)
        lines = [line.removeprefix('PGOS_RESULT=') for line in result.stdout.splitlines()
                 if line.startswith('PGOS_RESULT=')]
        require(bool(lines), 'Container returned no redacted probe result')
        report['c_read'] = json.loads(lines[-1])
        require(result.returncode == 0 and report['c_read']['status'] == 'passed',
                'C SQL read comparison failed')
        report['status'] = 'passed'
        print('PASS: C SQL vector/BM25 Tag reads and repeated prepared execution', flush=True)
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
