"""Real PG snapshots and C/libcurl, deterministic local TLS Tag fixture."""
from copy import deepcopy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import ssl
import sys
import threading
import time
import unittest

import psycopg
from psycopg.types.json import Jsonb

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'batch_replay'))
from test import FakeClient, COLLECTION, OWNER
from worker import Remote, replay, P, MARKER

STORES = {}
CALLS = []
FAULT = None


class Store(FakeClient):
    def __init__(self, collection):
        super().__init__()
        self.collection = collection
    def request(self, method, path, body=None, expected=200):
        return super().request(method,path.replace('/collections/'+self.collection,'/collections/'+COLLECTION),body,expected)


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self,*args): pass
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        collection = self.path.split('/')[-2]
        CALLS.append(deepcopy(body))
        docs = STORES[collection].tags[body['ref']['name']]
        if 'knn' in body['query']:
            hits = [{'doc':d,'score':999} for d in docs.values() if 'embedding' in d]
        else:
            term = body['query']['queryString']['query'].lower()
            hits = [{'doc':d,'score':float(d['content'].lower().split().count(term))}
                    for d in docs.values() if term in d.get('content','').lower().split()]
        hits = sorted(hits,key=lambda x:x['score'],reverse=True)
        if FAULT=='missing': hits = hits[1:]
        if FAULT=='wrong_revision' and hits:
            hits = deepcopy(hits)
            hits[0]['doc']['embedding'] = [0,0,1]
        if FAULT=='duplicate' and hits: hits.append(hits[0])
        if callable(FAULT): FAULT()
        data = json.dumps({'isDocsInline':True,'docs':hits,'total':len(hits)}).encode()
        self.send_response(200)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        global FAULT
        FAULT = None
        CALLS.clear()
        STORES.clear()
        self.conn = psycopg.connect(autocommit=True)
        self.addCleanup(lambda:self.conn.close())
        self.conn.execute('DROP SCHEMA IF EXISTS fixture CASCADE')
        self.conn.execute('TRUNCATE pgos_capture_probe.sources CASCADE')
        self.conn.execute('CREATE SCHEMA fixture')
        self.conn.execute('''CREATE TABLE fixture.docs(id bigint PRIMARY KEY,content text,
            embedding onesearch.vector(3),note text) WITH(fillfactor=50)''')
        self.conn.execute("INSERT INTO fixture.docs VALUES(1,'alpha','[1,0,0]','a'),(2,'beta','[0,1,0]','b'),(3,NULL,NULL,NULL)")
        self.gen,self.remote = {},{}
        for mode,field,opclass in [('vec','embedding','vector_ops'),('txt','content','text_ops')]:
            self.conn.execute(f'CREATE INDEX {mode} ON fixture.docs USING pgos_lifecycle({field} pgos_lifecycle_probe.{opclass})')
            self.gen[mode] = self.conn.execute('SELECT pgos_capture_probe.register_index(%s::regclass)',('fixture.'+mode,)).fetchone()[0]
            collection = 'pgos-live-offline-'+mode
            store = Store(collection)
            STORES[collection] = store
            self.remote[mode] = Remote(store)
            self.conn.execute(f'SELECT {P}.bind(%s,%s,%s)',(self.gen[mode],collection,OWNER))
            self.publish(mode)
            self.conn.execute("SELECT pgos_snapshot_probe.set_test_health(%s::regclass,true)",('fixture.'+mode,))

    def publish(self,mode='vec'):
        return replay(self.conn,self.gen[mode],self.remote[mode])
    def view(self,mode='vec',conn=None):
        return (conn or self.conn).execute('SELECT pgos_snapshot_probe.view(%s::regclass)',('fixture.'+mode,)).fetchone()[0]
    def search(self,mode='vec',query=None,conn=None,prepare=None):
        if query is None: query = [1,0,0] if mode=='vec' else 'alpha'
        return (conn or self.conn).execute('SELECT pgos_snapshot_probe.search(%s::regclass,%s)',
                  ('fixture.'+mode,Jsonb(query)),prepare=prepare).fetchone()[0]
    def assert_vector(self,result):
        expected = self.conn.execute("SELECT id,onesearch.cosine_distance(embedding,'[1,0,0]') FROM fixture.docs WHERE embedding IS NOT NULL ORDER BY 2,1").fetchall()
        self.assertEqual([(int(x['id']),x['distance']) for x in result['results']],expected)
    def wait_lock(self,conn):
        until = time.monotonic()+5
        while self.conn.execute('SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s',(conn.info.backend_pid,)).fetchone()[0]!='Lock':
            self.assertLess(time.monotonic(),until)
            time.sleep(.01)

    def test_base_search_both_modes_and_revision_identity(self):
        vector = self.search()
        self.assert_vector(vector)
        self.assertTrue(all(x['version'].startswith(str(self.gen['vec'])+':') for x in vector['results']))
        self.assertEqual([(x['id'],x['score']) for x in self.search('txt')['results']],[('1',1.0)])
        self.assertTrue(all(c['ref']['kind']=='tag' and c['size']==100 for c in CALLS))

    def test_own_writes_pk_delete_insert_null_and_rollback(self):
        before = self.view()
        with self.conn.transaction():
            self.conn.execute('SAVEPOINT edits')
            self.conn.execute("UPDATE fixture.docs SET id=10,embedding='[-1,0,0]' WHERE id=1")
            self.conn.execute('DELETE FROM fixture.docs WHERE id=2')
            self.conn.execute("UPDATE fixture.docs SET embedding='[1,0,0]' WHERE id=3")
            self.conn.execute("INSERT INTO fixture.docs VALUES(4,'new','[0,0,1]',NULL)")
            view = self.view()
            self.assertEqual(view['tag'],before['tag'])
            self.assertTrue(all(x['own'] for x in view['delta'].values()))
            self.assert_vector(self.search())
            with psycopg.connect(autocommit=True) as other:
                self.assertEqual(self.view(conn=other)['effective'],before['effective'])
            self.conn.execute('ROLLBACK TO edits')
            self.assertEqual(self.view()['effective'],before['effective'])
        self.assert_vector(self.search())

    def test_committed_lag_and_uncommitted_aborted_exclusion(self):
        before = self.view()
        self.conn.execute("UPDATE fixture.docs SET embedding='[-1,0,0]' WHERE id=1")
        with psycopg.connect() as other:
            other.execute("UPDATE fixture.docs SET embedding='[0,0,1]' WHERE id=2")
            view = self.view()
            self.assertEqual(view['tag'],before['tag'])
            self.assertEqual(set(view['delta']),{'1'})
            self.assertFalse(view['delta']['1']['own'])
            self.assert_vector(self.search())
            other.rollback()
        self.publish()
        self.assertEqual(self.view()['delta'],{})
        self.assert_vector(self.search())

    def test_old_statement_keeps_tag_delta_heap_through_publication_vacuum(self):
        old_tag = self.view()['tag']
        self.conn.execute("UPDATE fixture.docs SET embedding='[-1,0,0]' WHERE id=1")
        expected = self.view()
        with psycopg.connect(autocommit=True) as reader, psycopg.connect(autocommit=True) as gate:
            reader.execute('SET statement_timeout=10000')
            reader.execute('SET pgOS_snapshot_probe.pause_before_capture=9001')
            gate.execute('SELECT pg_advisory_lock(9001)')
            output,errors = [],[]
            def read():
                try: output.append(self.view(conn=reader))
                except Exception as exc: errors.append(exc)
            thread = threading.Thread(target=read)
            thread.start()
            try:
                self.wait_lock(reader)
                self.publish()
                self.conn.execute("UPDATE fixture.docs SET embedding='[0,0,1]' WHERE id=1")
                self.conn.execute('VACUUM fixture.docs')
                self.conn.execute('VACUUM pgos_capture_probe.outbox')
                self.conn.execute('VACUUM pgos_replay_probe.targets')
            finally:
                gate.execute('SELECT pg_advisory_unlock(9001)')
                thread.join(10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors,[])
            self.assertEqual(output[0],expected)
            self.assertEqual(output[0]['tag'],old_tag)
            reader.execute('RESET pgos_snapshot_probe.pause_before_capture')
            fresh = self.view(conn=reader)
            self.assertNotEqual(fresh['tag'],old_tag)
            self.assertEqual(fresh['effective']['1']['doc']['embedding'],[0,0,1])
        print('MVCC: old statement retained Tag + deleted delta + old heap after publish/VACUUM',flush=True)

    def test_current_health_not_hidden_by_old_snapshot_and_sibling_usable(self):
        with psycopg.connect(autocommit=True) as reader, psycopg.connect(autocommit=True) as gate:
            reader.execute('SET statement_timeout=10000')
            reader.execute('SET pgos_snapshot_probe.pause_after_capture=9002')
            gate.execute('SELECT pg_advisory_lock(9002)')
            errors=[]
            def read():
                try: self.view(conn=reader)
                except Exception as exc: errors.append(exc)
            thread=threading.Thread(target=read)
            thread.start()
            try:
                self.wait_lock(reader)
                self.conn.execute("SELECT pgos_snapshot_probe.set_test_health('fixture.vec',false)")
            finally:
                gate.execute('SELECT pg_advisory_unlock(9002)')
                thread.join(10)
            self.assertEqual(len(errors),1)
            self.assertIsInstance(errors[0],psycopg.errors.ObjectNotInPrerequisiteState)
        self.assertTrue(self.search('txt')['results'])
        self.assertEqual(self.conn.execute('SELECT count(*) FROM fixture.docs').fetchone()[0],3)
        print('HEALTH: fresh committed failure rejected an old statement snapshot',flush=True)

    def test_health_change_during_http_rejected_before_results(self):
        global FAULT
        def degrade():
            with psycopg.connect(autocommit=True) as other:
                other.execute("SELECT pgos_snapshot_probe.set_test_health('fixture.vec',false)")
        FAULT = degrade
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState): self.search()

    def test_prepared_calls_refresh_query_tag_own_writes_and_health(self):
        self.conn.execute('SET plan_cache_mode=force_generic_plan')
        first=self.search(prepare=True)
        with self.conn.transaction():
            self.conn.execute("UPDATE fixture.docs SET embedding='[-1,0,0]' WHERE id=1")
            self.assert_vector(self.search(prepare=True))
        self.publish()
        self.assertNotEqual(self.search(prepare=True)['tag'],first['tag'])
        self.assertEqual(self.search(query=[-1,0,0],prepare=True)['results'][0]['id'],'1')
        self.conn.execute("SELECT pgos_snapshot_probe.set_test_health('fixture.vec',false)")
        count=len(CALLS)
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState): self.search(prepare=True)
        self.assertEqual(len(CALLS),count)

    def test_bm25_changed_corpus_fails_before_network_unchanged_projection_allowed(self):
        with self.conn.transaction():
            self.conn.execute("UPDATE fixture.docs SET note='own note' WHERE id=1")
            self.assertEqual(self.search('txt')['results'][0]['row']['note'],'own note')
            self.conn.execute("UPDATE fixture.docs SET content='beta changed' WHERE id=1")
            before=len(CALLS)
            with self.assertRaises(psycopg.errors.FeatureNotSupported):
                with self.conn.transaction(): self.search('txt')
            self.assertEqual(len(CALLS),before)
        with self.assertRaises(psycopg.errors.FeatureNotSupported): self.search('txt')
        self.publish('txt')
        self.assertEqual(self.search('txt')['results'],[])

    def test_late_smaller_ids_across_batches_and_same_key_latest_revision(self):
        with psycopg.connect() as late:
            late.execute("UPDATE fixture.docs SET embedding='[-1,0,0]' WHERE id=1")
            self.conn.execute("UPDATE fixture.docs SET embedding='[0,0,1]' WHERE id=2")
            self.publish()
            late.commit()
        self.assert_vector(self.search())
        self.publish()
        self.assert_vector(self.search())
        self.conn.execute("UPDATE fixture.docs SET embedding='[0,1,0]' WHERE id=1")
        self.publish()
        self.assert_vector(self.search())

    def test_hot_update_delete_reinsert_and_vacuum_never_cache_tid(self):
        before=self.search()['results'][0]
        self.conn.execute("UPDATE fixture.docs SET note='HOT' WHERE id=1")
        after=self.search()['results'][0]
        self.assertNotEqual(after['tid'],before['tid'])
        self.assertNotEqual(after['version'],before['version'])
        self.assertEqual(after['row']['note'],'HOT')
        self.conn.execute('DELETE FROM fixture.docs WHERE id=1')
        self.conn.execute('VACUUM fixture.docs')
        self.conn.execute("INSERT INTO fixture.docs VALUES(1,'replacement','[-1,0,0]','reused key')")
        result=self.search()
        self.assert_vector(result)
        current=next(x for x in result['results'] if x['id']=='1')
        self.assertEqual(current['row']['note'],'reused key')
        self.assertNotEqual(current['version'],before['version'])

    def test_wrong_remote_revision_missing_and_duplicate_fail_closed(self):
        global FAULT
        for fault,error in [('wrong_revision',psycopg.errors.ObjectNotInPrerequisiteState),
                            ('missing',psycopg.errors.ObjectNotInPrerequisiteState),
                            ('duplicate',psycopg.DataError)]:
            FAULT=fault
            with self.assertRaises(error): self.search()
        FAULT=None
        self.assert_vector(self.search())

    def test_coverage_corruption_size_limits_and_reindex(self):
        self.conn.execute("UPDATE fixture.docs SET embedding='[-1,0,0]' WHERE id=1")
        with self.conn.transaction():
            self.conn.execute('SAVEPOINT corrupt')
            self.conn.execute('DELETE FROM pgos_capture_probe.outbox WHERE generation=%s',(self.gen['vec'],))
            with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
                with self.conn.transaction(): self.view()
            self.conn.execute('ROLLBACK TO corrupt')
        with self.conn.transaction():
            self.conn.execute('SAVEPOINT large')
            self.conn.execute("INSERT INTO fixture.docs SELECT x,'extra','[1,0,0]',NULL FROM generate_series(10,80) x")
            with self.assertRaises(psycopg.errors.ProgramLimitExceeded):
                with self.conn.transaction(): self.view()
            self.conn.execute('ROLLBACK TO large')
        self.conn.execute('REINDEX INDEX fixture.vec')
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState): self.view()

    def test_isolation_permissions_bad_query_and_rls(self):
        with self.conn.transaction():
            self.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            with self.assertRaises(psycopg.errors.FeatureNotSupported):
                with self.conn.transaction(): self.view()
        with self.assertRaises(psycopg.errors.InvalidParameterValue): self.search(query=[1,0])
        self.conn.execute('ALTER TABLE fixture.docs ENABLE ROW LEVEL SECURITY')
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState): self.view()
        self.conn.execute('ALTER TABLE fixture.docs DISABLE ROW LEVEL SECURITY')
        self.conn.execute('CREATE ROLE snapshot_reader')
        try:
            self.conn.execute('SET ROLE snapshot_reader')
            with self.assertRaises(psycopg.errors.InsufficientPrivilege): self.view()
        finally:
            self.conn.execute('RESET ROLE')
            self.conn.execute('DROP ROLE snapshot_reader')


if __name__=='__main__':
    server=ThreadingHTTPServer(('127.0.0.1',18443),Handler)
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain('/tmp/probe.crt','/tmp/probe.key')
    server.socket=context.wrap_socket(server.socket,server_side=True)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    try: unittest.main(verbosity=2)
    finally: server.shutdown()
