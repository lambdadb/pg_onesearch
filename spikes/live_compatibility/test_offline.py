"""Credential-free safety/contract tests; no remote requests are sent."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from client import Client, HttpFailure, ProbeError, batches, encoded, load_settings
from run import BRANCH, Experiment, MARKER

SETTINGS = {'LAMBDADB_BASE_URL':'https://example.invalid',
            'LAMBDADB_PROJECT_NAME':'fixture', 'LAMBDADB_PROJECT_API_KEY':'secret-do-not-forward'}


class OfflineTests(unittest.TestCase):
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
