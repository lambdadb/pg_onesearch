#!/usr/bin/env python3
"""Opt-in live snapshot read probe. Creates two owned fixtures and removes them afterward."""
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
from client import Client, HttpFailure, ProbeError, load_settings, require
from run import Experiment, git, save_report, utc


def remove_container(container, report):
    """Remote cleanup is safe only after the daemon confirms this worker is absent."""
    try:
        removed = subprocess.run(['docker', 'rm', '-f', container], capture_output=True, timeout=30)
        report['container_remove_returncode'] = removed.returncode
    except (OSError, subprocess.TimeoutExpired, KeyboardInterrupt) as exc:
        report['container_remove_error'] = type(exc).__name__
    # --rm may already have removed the container. Do not infer absence from a
    # failed inspect/rm (which could instead mean the daemon is unavailable).
    try:
        listed = subprocess.run(['docker', 'container', 'ls', '--all', '--format', '{{.Names}}'],
                                text=True, capture_output=True, timeout=30)
        if listed.returncode == 0 and container not in listed.stdout.splitlines():
            report['container_cleanup'] = 'confirmed_absent'
            return True
        report['container_verification'] = 'container_present' if listed.returncode == 0 else 'list_failed'
    except (OSError, subprocess.TimeoutExpired, KeyboardInterrupt) as exc:
        report['container_verification'] = type(exc).__name__
    report['container_cleanup'] = 'unconfirmed'
    return False


def main(*, executor=False):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    require(not args.report.exists(), 'Refusing to overwrite a report')
    settings = load_settings(args.env_file)
    probe = 'snapshot_executor' if executor else 'snapshot_read'
    directories = ('remote_read', 'index_lifecycle', 'change_capture', 'batch_replay', 'live_compatibility', 'snapshot_read')
    if executor:
        directories += ('snapshot_executor',)
    files = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
             for directory in directories
             for p in sorted((ROOT / 'spikes' / directory).glob('*')) if p.is_file() and p.suffix not in ('.o', '.bc', '.so')}
    image_id = subprocess.check_output(['docker', 'image', 'inspect', 'pg_onesearch:' + probe.replace('_', '-'),
                                        '--format', '{{.Id}}'], text=True).strip()
    # Bind evidence to the built container, not just to the current worktree.
    image_files = json.loads(subprocess.check_output(
        ['docker', 'run', '--rm', '--network', 'none', '--entrypoint', 'python3', image_id, '-c',
         'import hashlib,json,pathlib; print(json.dumps({str(p):hashlib.sha256(p.read_bytes()).hexdigest() '
         f'for directory in {directories!r} for p in pathlib.Path("spikes",directory).glob("*") if p.is_file() '
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
    container = 'pgos-snapshot-' + report['run_id'][:16]
    report['container_name'] = container
    experiment.persist()

    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        cases = []
        for role in ('vector', 'bm25'):
            resource = experiment.create(role)
            cases.append({'role':role, 'collection':resource['name']})
        # Pipe credentials after container creation, not via -e/--env-file/command args.
        result = subprocess.run(['docker', 'run', '--rm', '-i', '--name', container,
                                 '--platform', 'linux/arm64', '--entrypoint', 'python3',
                                 image_id, f'spikes/{probe}/bootstrap.py'],
                                input=json.dumps({'settings': settings, 'cases': cases, 'owner':report['run_id']}),
                                text=True, capture_output=True, timeout=1800)
        lines = [line.removeprefix('PGOS_RESULT=') for line in result.stdout.splitlines()
                 if line.startswith('PGOS_RESULT=')]
        require(bool(lines), 'Container returned no redacted probe result')
        report[probe] = json.loads(lines[-1])
        require(result.returncode == 0 and report[probe]['status'] == 'passed',
                'Snapshot read comparison failed')
        report['status'] = 'passed'
        print('PASS: ' + probe + ' vector overlay and same-corpus BM25 reads', flush=True)
    except (ProbeError, KeyboardInterrupt) as exc:
        report['status'] = 'failed'
        report['failure'] = str(exc) if isinstance(exc, ProbeError) else 'Interrupted'
    except Exception as exc:
        report['status'] = 'failed'
        report['failure'] = 'Unexpected ' + type(exc).__name__
    finally:
        if remove_container(container, report):
            experiment.persist()
            # Discover versions even after a worker dies before returning its attempt journal.
            # Only a collection with this run's ownership marker can be touched.
            for resource in experiment.resources:
                try:
                    info = experiment.client.request('GET', experiment.path(resource))
                    require(info.get('collection', {}).get('tags', {}).get('pgos-run') == report['run_id'],
                            'Ownership marker mismatch; version cleanup refused')
                    tags = experiment.client.request('GET', experiment.path(resource, '/tags'))['tags']
                    branches = experiment.client.request('GET', experiment.path(resource, '/branches'))['branches']
                    require(all(t['name'].startswith('attempt-') for t in tags)
                            and all(b['name']=='main' or b['name'].startswith('attempt-') for b in branches),
                            'Unexpected version in owned fixture; version cleanup refused')
                    resource['tags'] = [t['name'] for t in tags]
                    resource['branches'] = [b['name'] for b in branches if b['name']!='main']
                    experiment.persist()
                    for tag in resource['tags']:
                        experiment.client.request('DELETE', experiment.path(resource, '/tags/'+tag))
                    for branch in resource['branches']:
                        experiment.client.request('DELETE', experiment.path(resource, '/branches/'+branch))
                except HttpFailure as exc:
                    if exc.status != 404:
                        report['status'] = 'failed'
                        resource['version_cleanup_error'] = str(exc)
                except (ProbeError, KeyError, TypeError) as exc:
                    report['status'] = 'failed'
                    resource['version_cleanup_error'] = str(exc) if isinstance(exc,ProbeError) else 'Invalid version list'
            experiment.cleanup()
            if any(r['cleanup'] != 'confirmed_absent' for r in experiment.resources):
                report['status'] = 'failed'
        else:
            report['status'] = 'failed'
            report['remote_cleanup'] = 'deferred_until_container_absence'
            for resource in experiment.resources:
                resource['cleanup'] = 'deferred_container_unconfirmed'
            print('CLEANUP: container absence unconfirmed; remote resources retained', flush=True)
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
