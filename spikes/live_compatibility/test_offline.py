"""Credential-free safety/contract tests; no remote requests are sent."""
from concurrent.futures import ThreadPoolExecutor
import gzip
import http.client
import json
import threading
import urllib.error
import urllib.parse
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, MagicMock, patch

from client import Client, HttpFailure, ProbeError, batches, decode_json, encoded, load_settings
from run import BRANCH, Experiment, MARKER, main, save_report

SETTINGS = {'LAMBDADB_BASE_URL':'https://example.invalid',
            'LAMBDADB_PROJECT_NAME':'fixture', 'LAMBDADB_PROJECT_API_KEY':'secret-do-not-forward'}


class OfflineTests(unittest.TestCase):
    def test_protocol_errors_are_redacted_on_open_and_read(self):
        for error in (http.client.IncompleteRead(b'sensitive-body'),
                      http.client.BadStatusLine('sensitive-status-line'),
                      http.client.LineTooLong('sensitive-header')):
            for stage in ('open', 'read'):
                with self.subTest(error=type(error).__name__, stage=stage):
                    client = Client(SETTINGS)
                    response = MagicMock()
                    response.__enter__.return_value = response
                    response.status = 200
                    response.read.side_effect = error
                    client.opener.open = Mock(side_effect=error if stage == 'open' else None,
                                              return_value=response)
                    with self.assertRaises(ProbeError) as caught:
                        client.request('GET', '/collections/fixture')
                    self.assertEqual(str(caught.exception),
                                     'Transport failed; mutation outcome may be unknown')
                    self.assertTrue(caught.exception.__suppress_context__)

    def test_cleanup_protocol_failure_continues_and_saves_final_report(self):
        client = Client(SETTINGS)
        state = {'deleted':False, 'calls':[]}
        def setup(experiment):
            state['owner'] = experiment.report['run_id']
            experiment.resources.extend([
                {'name':'healthy','tags':[],'cleanup':'pending'},
                {'name':'broken','tags':[],'cleanup':'pending'},
            ])
            raise ProbeError('Simulated experiment failure')
        def respond(request, **kwargs):
            name = urllib.parse.urlsplit(request.full_url).path.rsplit('/',1)[-1]
            state['calls'].append((request.method, name))
            response = MagicMock()
            response.__enter__.return_value = response
            response.status, response.headers = 200, {}
            if name == 'broken':
                response.read.side_effect = http.client.IncompleteRead(b'sensitive-body')
            elif request.method == 'DELETE':
                state['deleted'] = True
                response.read.return_value = b'{}'
            elif state['deleted']:
                raise urllib.error.HTTPError(request.full_url, 404, 'absent', {}, None)
            else:
                response.read.return_value = encoded({'collection':{'tags':{'pgos-run':state['owner']}}})
            return response
        client.opener.open = Mock(side_effect=respond)
        with tempfile.TemporaryDirectory() as temp:
            report = Path(temp)/'report.json'
            with patch('sys.argv', ['run.py','--report',str(report)]), \
                 patch('run.load_settings', return_value=SETTINGS), \
                 patch('run.Client', return_value=client), \
                 patch('run.Experiment.run', setup), patch('run.signal.signal'):
                self.assertEqual(main(), 1)
            saved = json.loads(report.read_text())
            self.assertEqual(saved['status'], 'failed')
            self.assertIn('finished_at', saved)
            self.assertEqual([r['cleanup'] for r in saved['resources']],
                             ['confirmed_absent','unconfirmed'])
            self.assertEqual(state['calls'], [('GET','broken'),('GET','healthy'),
                                             ('DELETE','healthy'),('GET','healthy')])
            self.assertNotIn('sensitive-body', report.read_text())

    def test_report_saves_preserve_sibling_and_concurrent_reports(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            sibling = root/'live.tmp'
            sibling.write_text('unrelated file')
            reports = [root/'live.json', root/'live.txt']
            barrier = threading.Barrier(2)
            original_replace = Path.replace
            def simultaneous_replace(source, target):
                barrier.wait(timeout=5)
                return original_replace(source, target)
            with patch.object(Path, 'replace', simultaneous_replace), ThreadPoolExecutor(2) as pool:
                futures = [pool.submit(save_report, path, {'run':i}) for i,path in enumerate(reports)]
                for future in futures:
                    future.result(timeout=10)
            self.assertEqual(sibling.read_text(), 'unrelated file')
            self.assertEqual([json.loads(p.read_text()) for p in reports], [{'run':0},{'run':1}])
            self.assertEqual(set(root.iterdir()), {sibling, *reports})

    def test_failed_report_replace_preserves_old_report_and_removes_temp(self):
        with tempfile.TemporaryDirectory() as temp:
            report = Path(temp)/'live.json'
            report.write_text('{"previous":true}')
            with patch.object(Path, 'replace', side_effect=OSError('simulated failure')):
                with self.assertRaises(OSError):
                    save_report(report, {'replacement':True})
            self.assertEqual(json.loads(report.read_text()), {'previous':True})
            self.assertEqual(list(Path(temp).iterdir()), [report])

    def test_batch_budget_preserves_multirow_order(self):
        rows = [{'id':str(i), 'value':'x'*50} for i in range(10)]
        result = list(batches(rows, limit=300))
        self.assertEqual([row for batch in result for row in batch], rows)
        self.assertTrue(all(len(encoded({'docs':b,'branch':'main'})) <= 300 for b in result))
        self.assertTrue(any(len(b) > 1 for b in result))
        with self.assertRaises(ProbeError):
            list(batches([{'id':'huge','value':'x'*500}], limit=100))

    def test_environment_is_not_executed_or_mixed(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'settings'
            path.write_text('\n'.join(key+'='+value for key,value in SETTINGS.items()))
            with patch.dict('os.environ', {'LAMBDADB_PROJECT_NAME':'another'}):
                self.assertEqual(load_settings(path), SETTINGS)
            path.write_text('LAMBDADB_BASE_URL=https://example.invalid\n')
            with self.assertRaises(ProbeError):
                load_settings(path)

    def test_origin_rejects_userinfo_or_project_path(self):
        with patch.dict('os.environ', {**SETTINGS,'LAMBDADB_BASE_URL':'https://user:pass@example.invalid'}):
            with self.assertRaises(ProbeError):
                load_settings(None)
        with patch.dict('os.environ', {**SETTINGS,'LAMBDADB_BASE_URL':'https://example.invalid/projects/other'}):
            with self.assertRaises(ProbeError):
                load_settings(None)

    def test_download_uses_fresh_request_and_array(self):
        client = Client(SETTINGS)
        captured = []
        def transport(req):
            captured.append(req)
            return 200, [{'doc':{'id':'a'}, 'score':1}]
        client._json = transport
        url = 'https://storage.invalid/result?signature=opaque'
        result = client.items({'isDocsInline':False,'docs':[],'docsUrl':url})
        self.assertEqual(result[0]['doc']['id'],'a')
        self.assertEqual(captured[0].full_url,url)
        self.assertEqual(captured[0].header_items(),[])
        self.assertEqual(client.downloads,1)
        client._json = lambda req: (200, {'docs':[]})
        with self.assertRaises(ProbeError):
            client.items({'isDocsInline':False,'docs':[],'docsUrl':url})

    def test_bad_download_url_and_failed_download_are_not_empty_results(self):
        client = Client(SETTINGS)
        with self.assertRaises(ProbeError):
            client.items({'isDocsInline':False,'docs':[],'docsUrl':'http://storage.invalid/r'})
        def failed(req):
            raise HttpFailure(403)
        client._json = failed
        with self.assertRaises(HttpFailure):
            client.items({'isDocsInline':False,'docs':[],'docsUrl':'https://storage.invalid/r'})

    def test_gzip_download_and_expansion_budget(self):
        raw = gzip.compress(b'[{"doc":{"id":"a"}}]')
        for encoding in ('gzip', 'identity'):
            value, compressed = decode_json(raw, encoding)
            self.assertEqual(value[0]['doc']['id'], 'a')
            self.assertTrue(compressed)
        with patch('client.MAX_RESPONSE', 100):
            with self.assertRaises(ProbeError):
                decode_json(gzip.compress(b'x'*101), 'gzip')
        with self.assertRaises(ProbeError):
            decode_json(b'not gzip', 'gzip')

    def test_duplicate_results_rejected(self):
        with self.assertRaises(ProbeError):
            Client(SETTINGS).items({'isDocsInline':True,'docs':[{'doc':{'id':'a'}},{'doc':{'id':'a'}}]})

    def test_marker_requires_new_revision_and_tag_recheck(self):
        client = Client(SETTINGS)
        calls = []
        marker = {'id':MARKER,'cohort':'barrier','barrier_revision':'run-initial'}
        responses = [dict(marker, barrier_revision='stale'), marker, marker]
        def request(method,path,body=None,expected=200):
            calls.append((method,path,body))
            if path.endswith('/docs/upsert'):
                return {}
            if path.endswith('/tags'):
                return {'tag':{'name':'checkpoint-initial','snapshotId':'snapshot'}}
            if path.endswith('/docs/fetch'):
                return {'isDocsInline':True,'docs':[{'doc':responses.pop(0)}]}
            raise AssertionError('unexpected request')
        client.request=request
        report={'run_id':'run','checks':[],'writes':[]}
        exp=Experiment(client,report,10)
        resource={'name':'fixture','role':'vector','tags':[]}
        with patch('run.time.sleep'):
            exp.barrier(resource,'initial')
        fetches=[c[2] for c in calls if c[1].endswith('/docs/fetch')]
        self.assertEqual(fetches[0]['ref'],BRANCH)
        self.assertIs(fetches[0]['consistentRead'],False)
        self.assertIs(fetches[1]['consistentRead'],False)
        self.assertEqual(fetches[2]['ref']['kind'],'tag')
        self.assertNotIn('consistentRead',fetches[2])
        self.assertEqual([c[1].rsplit('/',1)[-1] for c in calls],['upsert','fetch','fetch','tags','fetch'])

    def test_cleanup_refuses_foreign_ownership(self):
        client=Client(SETTINGS)
        methods=[]
        def request(method,path,body=None,expected=200):
            methods.append(method)
            return {'collection':{'tags':{'pgos-run':'someone-else'}}}
        client.request=request
        exp=Experiment(client,{'run_id':'run','checks':[],'writes':[]},10)
        exp.resources=[{'name':'fixture','tags':[],'cleanup':'pending'}]
        exp.cleanup()
        self.assertEqual(methods,['GET'])
        self.assertEqual(exp.resources[0]['cleanup'],'unconfirmed')


if __name__ == '__main__':
    unittest.main()
