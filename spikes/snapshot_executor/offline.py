"""Actual Custom Scan over mutable heap, capture/replay and local immutable TLS Tags."""
import importlib.util
import os
from pathlib import Path
import select
import ssl
import subprocess
import sys
import threading
import unittest

import psycopg

path = Path(__file__).resolve().parents[1] / 'snapshot_read' / 'offline.py'
spec = importlib.util.spec_from_file_location('snapshot_fixture', path)
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)
from worker import name, ProbeError

SCORE = "pgos_snapshot_executor.score('fixture.txt',id)"
BM25 = f"SELECT id,note,{SCORE} AS score FROM fixture.docs WHERE pgos_snapshot_executor.match('fixture.txt',content,%s) ORDER BY score DESC,id"
VECTOR = """SELECT id,note,onesearch.cosine_distance(embedding,%s::onesearch.vector) AS distance
FROM fixture.docs WHERE pgos_snapshot_executor.vector_match('fixture.vec',embedding,%s::onesearch.vector)
ORDER BY distance,id"""


def nodes(plan):
    yield plan
    for child in plan.get('Plans', []):
        yield from nodes(child)


class ExecutorTests(unittest.TestCase):
    setUp = fixture.SnapshotTests.setUp
    publish = fixture.SnapshotTests.publish
    view = fixture.SnapshotTests.view
    wait_lock = fixture.SnapshotTests.wait_lock

    def vector(self, conn=None, query='[1,0,0]', sql=VECTOR, prepare=None):
        return (conn or self.conn).execute(sql,(query,query),prepare=prepare).fetchall()

    def expected(self, conn=None, query='[1,0,0]'):
        return (conn or self.conn).execute('''SELECT id,note,onesearch.cosine_distance(embedding,%s::onesearch.vector)
        FROM fixture.docs WHERE embedding IS NOT NULL ORDER BY 3,1''',(query,)).fetchall()

    def pending_events(self, mode='vec'):
        return self.conn.execute('''SELECT event_id,writer_xid,operation,row_key,document
            FROM pgos_capture_probe.outbox WHERE generation=%s ORDER BY event_id''',
            (self.gen[mode],)).fetchall()

    def assert_replay_blocked(self, mode='vec'):
        before = len(fixture.CALLS)
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            if mode == 'vec':
                self.vector(prepare=True)
            else:
                self.conn.execute(BM25, ('alpha',), prepare=True)
        # The SQL reader and Custom Scan must enforce the same operational gate.
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.view(mode)
        self.assertEqual(len(fixture.CALLS), before)

    def stop_after_verified_tag(self, mode='vec'):
        def stop(phase):
            if phase == 'tag_verified':
                raise RuntimeError('Stopped before PG publication')
        with self.assertRaisesRegex(RuntimeError, 'Stopped before PG publication'):
            fixture.replay(self.conn, self.gen[mode], self.remote[mode], checkpoint=stop)
        barrier = self.remote[mode].barriers[-1]
        snapshot = self.remote[mode].client.snapshots[name(barrier['attempt'])]
        return barrier['attempt'], snapshot['snapshotId'], snapshot['snapshotCommittedAt']

    def test_replay_failure_blocks_both_modes_until_fresh_attempt_publishes(self):
        self.conn.execute('SET plan_cache_mode=force_generic_plan')
        for mode in ('vec', 'txt'):
            for failure in ('/docs/upsert', '/tags', 'wrong_marker'):
                with self.subTest(mode=mode, failure=failure):
                    # Same indexed corpus: BM25 must fail on replay health, not 0A000.
                    self.conn.execute("UPDATE fixture.docs SET note=coalesce(note,'')||'x' WHERE id=1")
                    if mode == 'vec':
                        self.vector(prepare=True)
                    else:
                        self.conn.execute(BM25, ('alpha',), prepare=True)
                    events = self.pending_events(mode)
                    source_rows = self.conn.execute('SELECT * FROM fixture.docs ORDER BY id').fetchall()
                    store = self.remote[mode].client
                    if failure == 'wrong_marker':
                        store.wrong_marker = True
                    else:
                        store.fail_after = failure
                    with self.assertRaises(ProbeError):
                        self.publish(mode)
                    old_attempt = self.conn.execute('''SELECT active_attempt FROM pgos_replay_probe.batches
                        WHERE generation=%s AND state='pending' ''', (self.gen[mode],)).fetchone()[0]
                    self.assertEqual(self.pending_events(mode), events)
                    self.assertEqual(self.conn.execute('SELECT * FROM fixture.docs ORDER BY id').fetchall(), source_rows)
                    self.conn.execute('SELECT pgos_snapshot_probe.set_test_health(%s::regclass,true)', ('fixture.'+mode,))
                    self.assert_replay_blocked(mode)
                    # A failed index must not disable a healthy sibling or ordinary PG reads.
                    if mode == 'vec':
                        self.assertEqual(self.conn.execute(BM25, ('alpha',)).fetchone()[0], 1)
                    else:
                        self.assertEqual(self.vector(), self.expected())
                    store.wrong_marker = False
                    result = self.publish(mode)
                    self.assertNotEqual(result['attempt'], str(old_attempt))
                    self.assertEqual(self.pending_events(mode), [])
                    if mode == 'vec':
                        self.assertEqual(self.vector(prepare=True), self.expected())
                    else:
                        self.assertEqual(self.conn.execute(BM25, ('alpha',), prepare=True).fetchone()[0], 1)

    def test_abandoned_claim_and_attempt_remain_blocked_without_worker_callback(self):
        program = '''
import signal, sys
import psycopg
sys.path.insert(0, 'spikes/batch_replay')
from worker import replay
def pause(phase):
    if phase == sys.argv[2]:
        print('ready', flush=True)
        signal.pause()
with psycopg.connect(autocommit=True) as conn:
    replay(conn, sys.argv[1], None, checkpoint=pause)
'''
        for phase in ('claimed', 'attempt_committed'):
            with self.subTest(phase=phase):
                self.conn.execute("UPDATE fixture.docs SET note=note||'x' WHERE id=1")
                worker = subprocess.Popen([sys.executable, '-c', program, str(self.gen['vec']), phase],
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    self.assertTrue(select.select([worker.stdout], [], [], 5)[0], 'Worker did not reach checkpoint')
                    self.assertEqual(worker.stdout.readline().strip(), 'ready')
                    worker.kill()  # SIGKILL: no worker exception/finally hook can set health.
                    worker.wait(timeout=5)
                finally:
                    if worker.poll() is None:
                        worker.kill()
                    worker.communicate(timeout=5)
                self.assert_replay_blocked()
                self.publish()
                self.assertEqual(self.vector(), self.expected())

    def test_publication_does_not_hide_newer_bm25_lag_or_clear_explicit_veto(self):
        self.conn.execute("UPDATE fixture.docs SET content='alpha first' WHERE id=1")
        receipt = self.stop_after_verified_tag('txt')
        self.conn.execute("UPDATE fixture.docs SET content='alpha second' WHERE id=1")
        self.conn.execute('SELECT pgos_replay_probe.publish(%s,%s,%s)', receipt)
        self.assertTrue(self.pending_events('txt'))
        before = len(fixture.CALLS)
        with self.assertRaises(psycopg.errors.FeatureNotSupported):
            self.conn.execute(BM25, ('alpha',))
        self.assertEqual(len(fixture.CALLS), before)
        self.conn.execute("SELECT pgos_snapshot_probe.set_test_health('fixture.txt',false)")
        self.publish('txt')
        self.assertEqual(self.pending_events('txt'), [])
        self.assert_replay_blocked('txt')
        self.conn.execute("SELECT pgos_snapshot_probe.set_test_health('fixture.txt',true)")
        self.assertEqual(self.conn.execute(BM25, ('alpha',)).fetchall(), [(1, 'a', 1.0)])

    def test_verified_tag_uncommitted_publication_and_stale_receipts_do_not_unblock(self):
        self.conn.execute("UPDATE fixture.docs SET embedding='[-1,0,0]' WHERE id=1")
        events = self.pending_events()
        receipt = self.stop_after_verified_tag()
        self.assert_replay_blocked()
        with psycopg.connect() as publishing:
            publishing.execute('SELECT pgos_replay_probe.publish(%s,%s,%s)', receipt)
            self.assert_replay_blocked()
            publishing.rollback()
        self.assertEqual(self.pending_events(), events)
        self.assert_replay_blocked()
        result = self.publish()
        self.assertNotEqual(result['attempt'], receipt[0])
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute('SELECT pgos_replay_probe.publish(%s,%s,%s)', receipt)
        self.assertEqual(self.vector(), self.expected())
        # An idempotent receipt for an older published batch cannot clear a NEW pending batch.
        committed = self.remote['vec'].client.snapshots[result['tag']]
        self.conn.execute("UPDATE fixture.docs SET note=note||'x' WHERE id=1")
        self.conn.execute('SELECT pgos_replay_probe.claim(%s)', (self.gen['vec'],))
        self.conn.execute('SELECT pgos_replay_probe.publish(%s,%s,%s)',
                          (result['attempt'], committed['snapshotId'], committed['snapshotCommittedAt']))
        self.assert_replay_blocked()
        self.publish()
        self.assertEqual(self.vector(), self.expected())

    def test_pending_replay_survives_pg_crash_with_uncommitted_publication(self):
        self.conn.execute("UPDATE fixture.docs SET embedding='[-1,0,0]' WHERE id=1")
        receipt = self.stop_after_verified_tag()
        events = self.pending_events()
        publishing = psycopg.connect()
        self.addCleanup(publishing.close)
        publishing.execute('SELECT pgos_replay_probe.publish(%s,%s,%s)', receipt)
        self.assert_replay_blocked()
        self.conn.close()
        subprocess.run(['gosu','postgres','pg_ctl','-m','immediate','-w','stop'], check=True, capture_output=True)
        subprocess.run(['gosu','postgres','pg_ctl','-l',os.environ['PGDATA']+'/server.log','-w','start'],
                       check=True, capture_output=True)
        self.conn = psycopg.connect(autocommit=True)
        self.addCleanup(self.conn.close)
        self.assertEqual(self.pending_events(), events)
        self.assert_replay_blocked()
        self.publish()
        self.assertEqual(self.vector(), self.expected())

    def test_lost_publication_response_is_already_healthy(self):
        self.conn.execute("UPDATE fixture.docs SET note=note||'x' WHERE id=1")
        def stop(phase):
            if phase == 'published':
                raise RuntimeError('Publication response lost')
        with self.assertRaisesRegex(RuntimeError, 'Publication response lost'):
            fixture.replay(self.conn, self.gen['vec'], self.remote['vec'], checkpoint=stop)
        self.assertEqual(self.pending_events(), [])
        self.assertEqual(self.vector(), self.expected())
        self.assertIsNone(self.publish())

    def test_claim_during_http_blocks_old_statement_until_recovered(self):
        self.conn.execute("UPDATE fixture.docs SET note=note||'x' WHERE id=1")
        def claim():
            with psycopg.connect(autocommit=True) as worker:
                worker.execute('SELECT pgos_replay_probe.claim(%s)', (self.gen['vec'],))
        fixture.FAULT = claim
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.vector()
        fixture.FAULT = None
        self.assert_replay_blocked()
        self.publish()
        self.assertEqual(self.vector(), self.expected())

    def test_plan_typed_rows_filters_limit_and_no_io_explain(self):
        before=len(fixture.CALLS)
        plan=self.conn.execute('EXPLAIN (FORMAT JSON) '+VECTOR,('[1,0,0]','[1,0,0]')).fetchone()[0][0]['Plan']
        self.assertEqual(len(fixture.CALLS),before)
        self.assertEqual(next(n for n in nodes(plan) if n['Node Type']=='Custom Scan')['Custom Plan Provider'],'OneSearchSnapshot')
        self.assertEqual(self.vector(),self.expected())
        filtered=VECTOR.replace('ORDER BY','AND id=2 ORDER BY')+' LIMIT 1'
        self.assertEqual(self.vector(sql=filtered),[(2,'b',1.0)])
        self.assertEqual(self.vector(sql=VECTOR+' LIMIT 0'),[])
        self.assertEqual(self.conn.execute(BM25,('alpha',)).fetchall(),[(1,'a',1.0)])
        plan=self.conn.execute('EXPLAIN (FORMAT JSON, ANALYZE) '+VECTOR,('[1,0,0]','[1,0,0]')).fetchone()[0][0]['Plan']
        self.assertEqual(next(n for n in nodes(plan) if n['Node Type']=='Custom Scan')['Remote Queries'],1)
        print('SNAPSHOT CUSTOM PLAN:',plan,flush=True)

    def test_own_pk_move_delete_insert_null_savepoint_and_isolation(self):
        with self.conn.transaction():
            self.conn.execute('SAVEPOINT own')
            self.conn.execute("UPDATE fixture.docs SET id=-9223372036854775808,embedding='[-1,0,0]' WHERE id=1")
            self.conn.execute('DELETE FROM fixture.docs WHERE id=2')
            self.conn.execute("INSERT INTO fixture.docs VALUES(9223372036854775807,'alpha','[0,0,1]','own')")
            self.conn.execute("UPDATE fixture.docs SET embedding='[1,0,0]' WHERE id=3")
            self.assertEqual(self.vector(),self.expected())
            with psycopg.connect(autocommit=True) as other:
                self.assertEqual(self.vector(conn=other),[(1,'a',0.0),(2,'b',1.0)])
            self.conn.execute('ROLLBACK TO own')
            self.assertEqual(self.vector(),[(1,'a',0.0),(2,'b',1.0)])

    def test_prepared_refreshes_query_delta_tag_health_and_reindex(self):
        self.conn.execute('SET plan_cache_mode=force_generic_plan')
        self.vector(prepare=True)
        old=fixture.CALLS[-1]['ref']['name']
        self.conn.execute("UPDATE fixture.docs SET embedding='[-1,0,0]' WHERE id=1")
        self.assertEqual(self.vector(prepare=True),self.expected())
        self.assertEqual(fixture.CALLS[-1]['ref']['name'],old)
        self.publish()
        self.assertEqual(self.vector(query='[-1,0,0]',prepare=True),self.expected(query='[-1,0,0]'))
        self.assertNotEqual(fixture.CALLS[-1]['ref']['name'],old)
        self.conn.execute("SELECT pgos_snapshot_probe.set_test_health('fixture.vec',false)")
        before=len(fixture.CALLS)
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState): self.vector(prepare=True)
        self.assertEqual(len(fixture.CALLS),before)
        self.conn.execute("SELECT pgos_snapshot_probe.set_test_health('fixture.vec',true)")
        self.conn.execute('REINDEX INDEX fixture.vec')
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState): self.vector(prepare=True)

    def test_bm25_note_only_allowed_changed_corpus_rejected_before_http(self):
        with self.conn.transaction():
            self.conn.execute("UPDATE fixture.docs SET note='own' WHERE id=1")
            self.assertEqual(self.conn.execute(BM25,('alpha',)).fetchall(),[(1,'own',1.0)])
            self.conn.execute("UPDATE fixture.docs SET content='beta changed' WHERE id=1")
            before=len(fixture.CALLS)
            with self.assertRaises(psycopg.errors.FeatureNotSupported):
                with self.conn.transaction(): self.conn.execute(BM25,('alpha',))
            self.assertEqual(len(fixture.CALLS),before)
        with self.assertRaises(psycopg.errors.FeatureNotSupported): self.conn.execute(BM25,('alpha',))
        self.publish('txt')
        self.assertEqual(self.conn.execute(BM25,('alpha',)).fetchall(),[])

    def test_parameterized_rescan_refreshes_hits_and_score_frame(self):
        sql=f'''SELECT v.term,(SELECT {SCORE} FROM fixture.docs
        WHERE pgos_snapshot_executor.match('fixture.txt',content,v.term) ORDER BY {SCORE} DESC,id LIMIT 1)
        FROM (VALUES ('alpha'),('beta'),('absent')) v(term)'''
        before=len(fixture.CALLS)
        self.assertEqual(self.conn.execute(sql).fetchall(),[('alpha',1.0),('beta',1.0),('absent',None)])
        self.assertEqual(len(fixture.CALLS),before+3)
        plan=self.conn.execute('EXPLAIN (FORMAT JSON, ANALYZE) '+sql).fetchone()[0][0]['Plan']
        scan=next(n for n in nodes(plan) if n['Node Type']=='Custom Scan')
        self.assertEqual(scan['Remote Queries'],3)
        self.assertGreaterEqual(scan['Rescans'],2)

    def test_old_statement_preserves_tag_delta_tuple_during_publish_and_vacuum(self):
        self.conn.execute("UPDATE fixture.docs SET embedding='[-1,0,0]' WHERE id=1")
        old=self.view()['tag']
        expected=self.expected()
        with psycopg.connect(autocommit=True) as reader, psycopg.connect(autocommit=True) as gate:
            reader.execute('SET statement_timeout=10000')
            reader.execute('SET pgos_snapshot_probe.pause_before_capture=9011')
            gate.execute('SELECT pg_advisory_lock(9011)')
            output,errors=[],[]
            def read():
                try: output.append(self.vector(conn=reader))
                except Exception as exc: errors.append(exc)
            thread=threading.Thread(target=read);thread.start()
            try:
                self.wait_lock(reader)
                self.publish()
                self.conn.execute("UPDATE fixture.docs SET embedding='[0,0,1]',note='new' WHERE id=1")
                self.conn.execute('VACUUM fixture.docs')
                self.conn.execute('VACUUM pgos_capture_probe.outbox')
            finally:
                gate.execute('SELECT pg_advisory_unlock(9011)');thread.join(10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors,[])
            self.assertEqual(output,[expected])
            self.assertEqual(fixture.CALLS[-1]['ref']['name'],old)
            reader.execute('RESET pgos_snapshot_probe.pause_before_capture')
            self.assertEqual(self.vector(conn=reader),self.expected())
            self.assertNotEqual(fixture.CALLS[-1]['ref']['name'],old)

    def test_health_committed_during_http_and_projection_rejected(self):
        def degrade():
            with psycopg.connect(autocommit=True) as other:
                other.execute("SELECT pgos_snapshot_probe.set_test_health('fixture.vec',false)")
        fixture.FAULT=degrade
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState): self.vector()
        fixture.FAULT=None
        self.assertEqual(self.conn.execute(BM25,('alpha',)).fetchall(),[(1,'a',1.0)])
        self.conn.execute("SELECT pgos_snapshot_probe.set_test_health('fixture.vec',true)")
        # A projection blocks AFTER the remote materialization, exercising the executor's last guard.
        self.conn.execute('''CREATE FUNCTION fixture.pause(k bigint) RETURNS bigint LANGUAGE plpgsql VOLATILE AS $$
        BEGIN PERFORM pg_advisory_xact_lock(9012); RETURN k; END $$''')
        with psycopg.connect(autocommit=True) as reader, psycopg.connect(autocommit=True) as gate:
            reader.execute('SET statement_timeout=10000')
            gate.execute('SELECT pg_advisory_lock(9012)')
            errors=[]
            def read():
                try: self.vector(conn=reader,sql=VECTOR.replace('SELECT id,','SELECT fixture.pause(id),').replace('ORDER BY distance,id',''))
                except Exception as exc: errors.append(exc)
            thread=threading.Thread(target=read);thread.start()
            try:
                self.wait_lock(reader);degrade()
            finally:
                gate.execute('SELECT pg_advisory_unlock(9012)');thread.join(10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(len(errors),1)
            self.assertIsInstance(errors[0],psycopg.errors.ObjectNotInPrerequisiteState)

    def test_heap_tuple_identity_update_delete_reinsert_vacuum(self):
        self.conn.execute("UPDATE fixture.docs SET note='HOT' WHERE id=1")
        self.assertEqual(self.vector(),self.expected())
        self.conn.execute('DELETE FROM fixture.docs WHERE id=1')
        self.conn.execute('VACUUM fixture.docs')
        self.conn.execute("INSERT INTO fixture.docs VALUES(1,'replacement','[-1,0,0]','new tuple')")
        self.assertEqual(self.vector(),self.expected())
        self.assertEqual(self.vector()[-1],(1,'new tuple',2.0))

    def test_remote_errors_clear_score_context_and_reject_incomplete_base(self):
        for fault,error in [('missing',psycopg.errors.ObjectNotInPrerequisiteState),
                            ('wrong_revision',psycopg.errors.ObjectNotInPrerequisiteState),('duplicate',psycopg.DataError)]:
            fixture.FAULT=fault
            with self.assertRaises(error): self.vector()
        fixture.FAULT=None
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute(f'SELECT {SCORE} FROM fixture.docs')
        self.assertEqual(self.vector(),self.expected())

    def test_alias_index_column_scope_and_nested_score_rejection(self):
        self.conn.execute('''CREATE FUNCTION fixture.nested(k bigint) RETURNS float8 LANGUAGE plpgsql VOLATILE AS $$
        BEGIN RETURN pgos_snapshot_executor.score('fixture.txt',k); END $$''')
        queries=[
            "SELECT id FROM fixture.docs WHERE pgos_snapshot_executor.match('fixture.vec',content,'alpha')",
            "SELECT id FROM fixture.docs WHERE pgos_snapshot_executor.match('fixture.txt',note,'alpha')",
            "SELECT id FROM fixture.docs WHERE pgos_snapshot_executor.match('fixture.txt',content,'alpha') OR id=2",
            "SELECT count(*) FROM fixture.docs WHERE pgos_snapshot_executor.match('fixture.txt',content,'alpha')",
            BM25.replace('%s',"'alpha'").replace(SCORE,"pgos_snapshot_executor.score('fixture.vec',id)"),
            BM25.replace('%s',"'alpha'").replace(SCORE,"pgos_snapshot_executor.score('fixture.txt',id+1)"),
            "SELECT a.id FROM fixture.docs a JOIN fixture.docs b USING(id) WHERE pgos_snapshot_executor.match('fixture.txt',a.content,'alpha')",
        ]
        before=len(fixture.CALLS)
        for sql in queries:
            with self.subTest(sql=sql):
                with self.assertRaises(psycopg.errors.FeatureNotSupported): self.conn.execute(sql)
        self.assertEqual(len(fixture.CALLS),before)
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute("SELECT fixture.nested(id) FROM fixture.docs WHERE pgos_snapshot_executor.match('fixture.txt',content,'alpha')")
        self.assertEqual(self.conn.execute(BM25,('alpha',)).fetchall(),[(1,'a',1.0)])

    def test_nonfirst_key_and_value_columns(self):
        # Planner binding comes from IAM metadata, never fixed fixture attribute numbers.
        self.conn.execute('CREATE TABLE fixture.reordered(note text, embedding onesearch.vector(3), id bigint PRIMARY KEY)')
        self.conn.execute("INSERT INTO fixture.reordered VALUES('reordered','[1,0,0]',-7)")
        self.conn.execute('CREATE INDEX reordered_vec ON fixture.reordered USING pgos_lifecycle(embedding pgos_lifecycle_probe.vector_ops)')
        gen=self.conn.execute("SELECT pgos_capture_probe.register_index('fixture.reordered_vec')").fetchone()[0]
        collection='pgos-live-offline-reordered';store=fixture.Store(collection);fixture.STORES[collection]=store
        self.conn.execute(f'SELECT {fixture.P}.bind(%s,%s,%s)',(gen,collection,fixture.OWNER))
        fixture.replay(self.conn,gen,fixture.Remote(store))
        self.conn.execute("SELECT pgos_snapshot_probe.set_test_health('fixture.reordered_vec',true)")
        rows=self.conn.execute("SELECT id,note FROM fixture.reordered WHERE pgos_snapshot_executor.vector_match('fixture.reordered_vec',embedding,'[1,0,0]')").fetchall()
        self.assertEqual(rows,[(-7,'reordered')])
        with self.assertRaises(psycopg.errors.FeatureNotSupported):
            self.conn.execute("SELECT id FROM fixture.docs WHERE pgos_snapshot_executor.vector_match('fixture.reordered_vec',embedding,'[1,0,0]')")

    def test_null_zero_limit_bounds_rls_and_isolation(self):
        before=len(fixture.CALLS)
        self.assertEqual(self.vector(query=None),[])
        self.assertEqual(self.vector(sql=VECTOR+' LIMIT 0'),[])
        self.assertEqual(len(fixture.CALLS),before)
        with self.assertRaises(psycopg.errors.InvalidParameterValue): self.vector(query='[1,0]')
        with self.conn.transaction():
            self.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            with self.assertRaises(psycopg.errors.FeatureNotSupported):
                with self.conn.transaction(): self.vector()
        self.conn.execute('ALTER TABLE fixture.docs ENABLE ROW LEVEL SECURITY')
        with self.assertRaises(psycopg.errors.FeatureNotSupported): self.vector()
        self.conn.execute('ALTER TABLE fixture.docs DISABLE ROW LEVEL SECURITY')
        self.conn.execute("INSERT INTO fixture.docs SELECT x,'extra','[1,0,0]',NULL FROM generate_series(10,80) x")
        with self.assertRaises(psycopg.errors.ProgramLimitExceeded): self.vector()


if __name__=='__main__':
    server=fixture.ThreadingHTTPServer(('127.0.0.1',18443),fixture.Handler)
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain('/tmp/probe.crt','/tmp/probe.key')
    server.socket=context.wrap_socket(server.socket,server_side=True)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    try: unittest.main(verbosity=2)
    finally: server.shutdown();server.server_close()
