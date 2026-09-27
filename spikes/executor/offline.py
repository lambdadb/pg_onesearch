"""Actual Custom Scan plans against a frozen PG heap and a local HTTPS service."""
import json
from pathlib import Path
import ssl
import sys
import threading
import unittest

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'remote_read'))
from offline import Handler, Server

CALLS = []
from queries import TABLE, SCORE, BM25, VECTOR, RESCAN, nodes


class FixtureHandler(Handler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        CALLS.append(body)
        if 'knn' in body['query']:
            scores = {str(i): 42 for i in range(1, 6)}  # Deliberately not a cosine score.
        else:
            term = body['query']['queryString']['query']
            if term == 'error':
                self.respond(b'private', 503, {})
                return
            scores = {'alpha': {'1': 1.1, '2': .4, '5': 1.1},
                      'beta': {'1': .5, '2': 1.4, '3': .6, '5': .5},
                      'gamma': {'3': 1}}.get(term, {})
        if body['ref']['name'] == 'checkpoint-second':
            scores = {key: value * 2 for key, value in scores.items()}
        self.respond(json.dumps({'isDocsInline': True, 'docs': [
            {'doc': {'id': key}, 'score': value} for key, value in scores.items()]}).encode(), 200, {})



class ExecutorTests(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg.connect(autocommit=True)
        self.addCleanup(self.conn.close)

    def plan(self, sql, args=None, analyze=False):
        return self.conn.execute(f'EXPLAIN (FORMAT JSON, ANALYZE {analyze}) ' + sql, args).fetchone()[0][0]['Plan']

    def test_explain_without_network_then_actual_plan(self):
        before = len(CALLS)
        plan = self.plan(BM25, ('alpha',))
        self.assertEqual(len(CALLS), before)
        custom = [n for n in nodes(plan) if n['Node Type'] == 'Custom Scan']
        self.assertEqual(len(custom), 1)
        self.assertEqual(custom[0]['Custom Plan Provider'], 'OneSearchProbe')
        plan = self.plan(BM25, ('alpha',), True)
        custom = next(n for n in nodes(plan) if n['Node Type'] == 'Custom Scan')
        self.assertEqual(custom['Remote Queries'], 1)
        self.assertEqual(len(CALLS), before + 1)
        print('PLAN:', json.dumps(plan), flush=True)

    def test_bm25_scores_bound_to_scan(self):
        before = len(CALLS)
        rows = self.conn.execute(BM25, ('alpha',)).fetchall()
        self.assertEqual(rows, [(1, 1.1), (5, 1.1), (2, .4)])
        self.assertEqual(len(CALLS), before + 1)
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute(f'SELECT {SCORE} FROM {TABLE}')

    def test_vector_distance_uses_pg_originals(self):
        before = len(CALLS)
        args = ('[1,0,0]', '[1,0,0]')
        rows = self.conn.execute(VECTOR, args).fetchall()
        reference = self.conn.execute(f'SELECT id,embedding OPERATOR(onesearch.<=>) %s::onesearch.vector AS d FROM {TABLE} ORDER BY d,id', (args[0],)).fetchall()
        self.assertEqual(rows, reference)
        self.assertEqual([row[0] for row in rows], [1, 5, 2, 3, 4])
        self.assertEqual(len(CALLS), before + 1)
        self.assertTrue(all(row[1] != 42 for row in rows))

    def test_generic_plan_new_query_and_health(self):
        self.conn.execute('SET plan_cache_mode=force_generic_plan')
        before = len(CALLS)
        self.assertEqual(self.conn.execute(BM25, ('alpha',), prepare=True).fetchall()[0], (1, 1.1))
        self.assertEqual(self.conn.execute(BM25, ('beta',), prepare=True).fetchall()[0], (2, 1.4))
        self.conn.execute('SET pgos_executor_probe.healthy=off')
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute(BM25, ('alpha',), prepare=True)
        self.assertEqual(len(CALLS), before + 2)
        self.conn.execute('SET pgos_executor_probe.healthy=on')
        self.conn.execute("SET pgos_executor_probe.bm25_tag='checkpoint-second'")
        self.assertEqual(self.conn.execute(BM25, ('alpha',), prepare=True).fetchall()[0], (1, 2.2))
        self.assertEqual(CALLS[-1]['ref']['name'], 'checkpoint-second')

    def test_correlated_subplan_rescans(self):
        sql = RESCAN
        before = len(CALLS)
        self.assertEqual(self.conn.execute(sql).fetchall(), [('alpha', 1.1), ('beta', 1.4), ('gamma', 1)])
        self.assertEqual(len(CALLS), before + 3)
        plan = self.plan(sql, analyze=True)
        custom = next(n for n in nodes(plan) if n['Node Type'] == 'Custom Scan')
        self.assertEqual(custom['Remote Queries'], 3)
        self.assertGreaterEqual(custom['Rescans'], 2)
        print('RESCAN PLAN:', json.dumps(plan), flush=True)

    def test_rescan_checks_health(self):
        self.conn.execute('''CREATE OR REPLACE FUNCTION pgos_executor_probe.health_term(n int)
          RETURNS text LANGUAGE plpgsql VOLATILE AS $$ BEGIN
          PERFORM set_config('pgos_executor_probe.healthy', CASE WHEN n=1 THEN 'on' ELSE 'off' END,true);
          RETURN 'alpha'; END $$''')
        sql = f'''SELECT (SELECT id FROM {TABLE}
          WHERE pgos_executor_probe.match(content,v.term) ORDER BY id LIMIT 1)
          FROM (SELECT pgos_executor_probe.health_term(n) AS term FROM (VALUES (1),(2)) x(n) OFFSET 0) v'''
        before = len(CALLS)
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute(sql)
        self.assertEqual(len(CALLS), before + 1)
        self.assertEqual(self.conn.execute(BM25, ('alpha',)).fetchall()[0], (1, 1.1))

    def test_empty_null_and_limit_zero(self):
        before = len(CALLS)
        self.assertEqual(self.conn.execute(BM25 + ' LIMIT 0', ('alpha',)).fetchall(), [])
        self.assertEqual(self.conn.execute(BM25, (None,)).fetchall(), [])
        self.assertEqual(len(CALLS), before)
        self.assertEqual(self.conn.execute(BM25, ('absent',)).fetchall(), [])
        self.assertEqual(len(CALLS), before + 1)
        with self.assertRaises(psycopg.errors.InvalidParameterValue):
            self.conn.execute(BM25, ('   ',))

    def test_filter_uses_complete_small_candidate_universe(self):
        before = len(CALLS)
        sql = BM25.replace(' ORDER BY', ' AND id=2 ORDER BY') + ' LIMIT 1'
        self.assertEqual(self.conn.execute(sql, ('alpha',)).fetchall(), [(2, .4)])
        self.assertEqual(CALLS[-1]['size'], 100)
        self.assertEqual(len(CALLS), before + 1)

    def test_errors_do_not_leave_score_context(self):
        with self.assertRaises(psycopg.errors.ConnectionFailure):
            self.conn.execute(BM25, ('error',))
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute(f'SELECT {SCORE} FROM {TABLE}')
        self.assertEqual(self.conn.execute(BM25, ('alpha',)).fetchall()[0], (1, 1.1))

    def test_nested_sql_cannot_borrow_outer_score_context(self):
        self.conn.execute('''CREATE OR REPLACE FUNCTION pgos_executor_probe.nested_score(k bigint)
          RETURNS double precision LANGUAGE plpgsql VOLATILE AS $$ BEGIN
          RETURN pgos_executor_probe.score('pgos_executor_probe.documents'::regclass,k); END $$''')
        sql = f'''SELECT pgos_executor_probe.nested_score(id) FROM {TABLE}
          WHERE pgos_executor_probe.match(content,'alpha')'''
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute(sql)
        self.assertEqual(self.conn.execute(BM25, ('alpha',)).fetchall()[0], (1, 1.1))

    def test_unsupported_shapes_and_frozen_writes(self):
        before = len(CALLS)
        queries = [
            f"SELECT id FROM {TABLE} WHERE pgos_executor_probe.match(content,'alpha') OR id=1",
            f"SELECT id FROM {TABLE} WHERE pgos_executor_probe.match(content,'alpha') AND pgos_executor_probe.match(content,'beta')",
            f"SELECT a.id FROM {TABLE} a JOIN {TABLE} b USING(id) WHERE pgos_executor_probe.match(a.content,'alpha')",
            f"SELECT id FROM {TABLE} WHERE pgos_executor_probe.match(content,'alpha') FOR UPDATE",
            f"SELECT (SELECT id FROM {TABLE} WHERE pgos_executor_probe.match(content,'alpha') LIMIT 1), (SELECT id FROM {TABLE} WHERE pgos_executor_probe.match(content,'beta') LIMIT 1)",
            f"SELECT count(*) FROM {TABLE} WHERE pgos_executor_probe.match(content,'alpha')",
            f"SELECT {SCORE.replace(',id)', ',id+1)')} FROM {TABLE} WHERE pgos_executor_probe.match(content,'alpha')",
            f"UPDATE {TABLE} SET content='changed' WHERE id=1",
            f"DELETE FROM {TABLE} WHERE id=1",
            f"TRUNCATE {TABLE}",
        ]
        for sql in queries:
            with self.subTest(sql=sql):
                with self.assertRaises(psycopg.errors.FeatureNotSupported):
                    self.conn.execute(sql)
        self.assertEqual(len(CALLS), before)


if __name__ == '__main__':
    server = Server(('127.0.0.1', 18443), FixtureHandler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain('/tmp/probe.crt', '/tmp/probe.key')
    server.context = context
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        unittest.main(verbosity=2)
    finally:
        server.shutdown()
        server.server_close()
