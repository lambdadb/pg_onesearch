"""Credential-free regression tests for the container/remote cleanup boundary."""
from contextlib import ExitStack, redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import run_live as harness

CONTAINER = 'pgos-snapshot-' + 'a' * 16
SENTINEL = 'private-daemon-output'


def result(code=0, output=''):
    return subprocess.CompletedProcess([], code, stdout=output, stderr=SENTINEL)


class CleanupTests(unittest.TestCase):
    def check_removal(self, removal, listing):
        report = {}
        with patch.object(harness.subprocess, 'run', side_effect=[removal, listing]) as run:
            absent = harness.remove_container(CONTAINER, report)
        self.assertEqual(run.call_args_list[0].args[0], ['docker', 'rm', '-f', CONTAINER])
        self.assertEqual(run.call_args_list[1].args[0],
                         ['docker', 'container', 'ls', '--all', '--format', '{{.Names}}'])
        self.assertNotIn(SENTINEL, json.dumps(report))
        return absent, report

    def test_successful_removal_requires_absence_confirmation(self):
        absent, report = self.check_removal(result(), result(output=CONTAINER+'-other\n'))
        self.assertTrue(absent)
        self.assertEqual(report['container_cleanup'], 'confirmed_absent')

    def test_already_absent_is_success_even_when_rm_fails(self):
        absent, report = self.check_removal(result(1), result())
        self.assertTrue(absent)
        self.assertEqual(report['container_remove_returncode'], 1)
        self.assertEqual(report['container_cleanup'], 'confirmed_absent')

    def test_existing_container_blocks_cleanup_regardless_of_rm_exit_code(self):
        for code in (0, 1):
            with self.subTest(code=code):
                absent, report = self.check_removal(result(code), result(output=CONTAINER+'\n'))
                self.assertFalse(absent)
                self.assertEqual(report['container_cleanup'], 'unconfirmed')
                self.assertEqual(report['container_verification'], 'container_present')

    def test_failed_or_timed_out_verification_never_proves_absence(self):
        for listing in (result(1), OSError(SENTINEL),
                        subprocess.TimeoutExpired('docker', 30, stderr=SENTINEL), KeyboardInterrupt()):
            with self.subTest(listing=type(listing).__name__):
                absent, report = self.check_removal(result(), listing)
                self.assertFalse(absent)
                self.assertEqual(report['container_cleanup'], 'unconfirmed')

    def test_removal_exceptions_still_require_independent_absence(self):
        for failure in (OSError(SENTINEL), subprocess.TimeoutExpired('docker', 30, stderr=SENTINEL), KeyboardInterrupt()):
            for listing, expected in ((result(), True), (result(output=CONTAINER+'\n'), False)):
                with self.subTest(error=type(failure).__name__, expected=expected):
                    absent, report = self.check_removal(failure, listing)
                    self.assertEqual(absent, expected)
                    self.assertEqual(report['container_remove_error'], type(failure).__name__)

    def run_harness(self, worker, removal, listing):
        events, deleted = [], set()
        client = Mock()
        def request(method, path, *args, **kwargs):
            events.append(('remote', method, path))
            if path in deleted:
                raise harness.HttpFailure(404)
            if method == 'GET':
                if path.endswith('/tags'):
                    return {'tags': [{'name': 'attempt-fixture'}]}
                if path.endswith('/branches'):
                    return {'branches': [{'name': 'main'}, {'name': 'attempt-fixture'}]}
                return {'collection': {'tags': {'pgos-run': 'a'*32}}}
            deleted.add(path)
            return {}
        client.request.side_effect = request
        def create(experiment, role):
            resource = {'name': 'pgos-live-fixture-'+role, 'role': role, 'tags': [], 'cleanup': 'pending'}
            experiment.resources.append(resource)
            return resource
        def run(command, **kwargs):
            if command[1] == 'run':
                outcome = worker
            elif command[1] == 'rm':
                events.append('remove')
                outcome = removal
            else:
                self.assertEqual(command[1:3], ['container', 'ls'])
                events.append('verify')
                outcome = listing
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        with tempfile.TemporaryDirectory() as temp, ExitStack() as stack:
            report = Path(temp)/'report.json'
            stack.enter_context(patch('sys.argv', ['run_live.py', '--env-file', '/unused', '--report', str(report)]))
            stack.enter_context(patch.object(harness, 'load_settings', return_value={'fixture': 'credential'}))
            stack.enter_context(patch.object(harness, 'Client', return_value=client))
            stack.enter_context(patch.object(harness, 'git', side_effect=['revision', '']))
            stack.enter_context(patch.object(harness.uuid, 'uuid4', return_value=SimpleNamespace(hex='a'*32)))
            stack.enter_context(patch.object(harness.signal, 'signal'))
            # Provenance is outside this regression; avoid reading an image or source tree.
            stack.enter_context(patch.object(harness.Path, 'glob', return_value=[]))
            stack.enter_context(patch.object(harness.subprocess, 'check_output', side_effect=['image-id', '{}']))
            stack.enter_context(patch.object(harness.subprocess, 'run', side_effect=run))
            stack.enter_context(patch.object(harness.Experiment, 'create', create))
            stack.enter_context(redirect_stdout(io.StringIO()))
            code = harness.main()
            saved = json.loads(report.read_text())
            self.assertNotIn(SENTINEL, report.read_text())
            self.assertNotIn('credential', report.read_text())
        return code, saved, events, client

    def test_worker_timeout_with_failed_removal_retains_resources_and_saves_report(self):
        code, report, events, client = self.run_harness(
            subprocess.TimeoutExpired('docker', 1800, stderr=SENTINEL), result(1), result(output=CONTAINER+'\n'))
        self.assertEqual(code, 1)
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['container_name'], CONTAINER)
        self.assertEqual(report['container_cleanup'], 'unconfirmed')
        self.assertEqual(report['remote_cleanup'], 'deferred_until_container_absence')
        self.assertTrue(all(r['cleanup']=='deferred_container_unconfirmed' for r in report['resources']))
        self.assertIn('finished_at', report)
        client.request.assert_not_called()
        self.assertEqual(events, ['remove', 'verify'])

    def test_worker_interruption_with_daemon_failure_preserves_remote_resources(self):
        code, report, events, client = self.run_harness(KeyboardInterrupt(), result(1), result(1))
        self.assertEqual(code, 1)
        self.assertEqual(report['failure'], 'Interrupted')
        self.assertEqual(report['container_cleanup'], 'unconfirmed')
        self.assertIn('finished_at', report)
        client.request.assert_not_called()

    def test_remote_cleanup_runs_only_after_confirmed_absence(self):
        worker = result(output='PGOS_RESULT={"status":"passed"}\n')
        code, report, events, client = self.run_harness(worker, result(1), result())
        self.assertEqual(code, 0)
        self.assertEqual(report['container_cleanup'], 'confirmed_absent')
        self.assertEqual(events[:2], ['remove', 'verify'])
        self.assertTrue(all(event[0]=='remote' for event in events[2:]))
        self.assertTrue(any(e[1]=='DELETE' and '/tags/' in e[2] for e in events[2:]))
        self.assertTrue(any(e[1]=='DELETE' and '/branches/' in e[2] for e in events[2:]))
        self.assertTrue(all(r['cleanup']=='confirmed_absent' for r in report['resources']))


if __name__ == '__main__':
    unittest.main(verbosity=2)
