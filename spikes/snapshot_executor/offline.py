"""Actual Custom Scan over mutable heap, capture/replay and local immutable TLS Tags."""
import importlib.util
from pathlib import Path
import ssl
import threading
import unittest

import psycopg

path = Path(__file__).resolve().parents[1] / 'snapshot_read' / 'offline.py'
spec = importlib.util.spec_from_file_location('snapshot_fixture', path)
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)

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
