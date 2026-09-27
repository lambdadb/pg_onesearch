"""PG commit cleanup -> real replay protocol -> durable per-transaction coverage.

Remote writes use the deterministic API fixture; SQL searches use local TLS.
No credentials, external network, or claim of live LambdaDB latency/safety.
"""
import os
from pathlib import Path
import select
import ssl
import subprocess
import sys
import threading
import time
import unittest

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'snapshot_executor'))
import offline as executor
from worker import replay, ProbeError

fixture = executor.fixture
P = 'pgos_completion_probe'


class CompletionTests(unittest.TestCase):
    publish = executor.ExecutorTests.publish
    vector = executor.ExecutorTests.vector
    expected = executor.ExecutorTests.expected
    view = executor.ExecutorTests.view
    assert_replay_blocked = executor.ExecutorTests.assert_replay_blocked
    stop_after_verified_tag = executor.ExecutorTests.stop_after_verified_tag

    def setUp(self):
        with psycopg.connect(autocommit=True) as admin:
            admin.execute(f'SELECT {P}.control(1)')
            self.until(lambda: admin.execute(f'SELECT {P}.worker_status()').fetchone()[0].split(',')[1:] == ['0','0'])
            admin.execute('ALTER TABLE pgos_capture_probe.outbox DISABLE TRIGGER completion')
        try:
            fixture.SnapshotTests.setUp(self)
        finally:
            with psycopg.connect(autocommit=True) as admin:
                admin.execute('ALTER TABLE pgos_capture_probe.outbox ENABLE TRIGGER completion')
        self.conn.execute(f'SELECT {P}.control(0)')

    def until(self, predicate, timeout=6):
        end = time.monotonic() + timeout
        while not predicate():
            self.assertLess(time.monotonic(), end, 'test barrier timed out')
            time.sleep(.01)

    def worker_status(self):
        return tuple(map(int, self.conn.execute(f'SELECT {P}.worker_status()').fetchone()[0].split(',')))

    def ready(self, xid):
        return self.conn.execute(f'SELECT {P}.is_published(%s::xid8)', (xid,)).fetchone()[0]

    def coverage(self, xid):
        return self.conn.execute(f'SELECT * FROM {P}.status(%s::xid8) ORDER BY generation', (xid,)).fetchall()

    def writer(self, sql="UPDATE fixture.docs SET note='committed' WHERE id=1", wait=4000):
        conn = psycopg.connect(autocommit=True)
        self.addCleanup(conn.close)
        conn.execute(f'SET onesearch_probe.wait_ms={wait}')
        conn.execute('BEGIN')
        conn.execute(sql)
        xid = conn.execute('SELECT pg_current_xact_id()::text').fetchone()[0]
        notices = []
        conn.add_notice_handler(lambda d: notices.append((d.sqlstate, d.message_primary)))
        return conn, xid, notices

    def commit(self, conn):
        errors = []
        thread = threading.Thread(target=lambda: self.run_commit(conn, errors))
        thread.start()
        self.addCleanup(lambda: thread.join(11))
        self.until(lambda: self.worker_status()[2] > 0 or not thread.is_alive())
        self.assertTrue(thread.is_alive(), errors)
        return thread, errors

    @staticmethod
    def run_commit(conn, errors):
        try:
            conn.execute('COMMIT')
        except Exception as exc:
            errors.append(exc)

    def finished(self, thread, errors, notices, warning=False):
        thread.join(6)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual([n[0] for n in notices], ['01000'] if warning else [])

    def publish_all(self):
        for mode in ('vec','txt'):
            self.publish(mode)

    def test_two_indexes_all_events_and_batched_rpc_before_commit_response(self):
        writer, xid, notices = self.writer("UPDATE fixture.docs SET content=content||' alpha' WHERE id IN (1,2)")
        self.assertFalse(self.ready(xid))  # Other backends cannot see uncommitted requirements.
        thread, errors = self.commit(writer)
        self.assertEqual([r[1:] for r in self.coverage(xid)], [(2,0),(2,0)])
        self.assert_replay_blocked('vec')
        self.assert_replay_blocked('txt')
        self.publish('vec')
        self.assertFalse(self.ready(xid))
        self.assertTrue(thread.is_alive())
        self.assertEqual(self.vector(), self.expected())  # Completed sibling is readable.
        self.assert_replay_blocked('txt')
        self.publish('txt')
        self.finished(thread, errors, notices)
        self.assertTrue(self.ready(xid))
        self.assertEqual([r[1:] for r in self.coverage(xid)], [(2,2),(2,2)])
        self.assertIn('outcome=published', writer.execute(f'SELECT {P}.last_result()').fetchone()[0])
        for mode in ('vec','txt'):
            self.assertEqual(self.remote[mode].writes[-1]['size'], 2)
        self.assertEqual(len(self.conn.execute(executor.BM25, ('alpha',)).fetchall()), 2)

    def test_no_replay_timeout_preserves_source_and_blocks_before_claim(self):
        writer, xid, notices = self.writer(wait=150)
        thread, errors = self.commit(writer)
        self.finished(thread, errors, notices, warning=True)
        self.assertEqual(self.conn.execute('SELECT note FROM fixture.docs WHERE id=1').fetchone()[0], 'committed')
        self.assertEqual(self.conn.execute('SELECT count(*) FROM pgos_capture_probe.outbox WHERE writer_xid=%s::xid8', (xid,)).fetchone()[0], 2)
        self.assertFalse(self.ready(xid))
        self.assert_replay_blocked('vec')
        self.assert_replay_blocked('txt')
        self.publish_all()
        self.assertTrue(self.ready(xid))
        self.assertEqual(self.vector(), self.expected())
        self.assertEqual(self.conn.execute(executor.BM25, ('alpha',)).fetchone()[0], 1)

    def test_ambiguous_write_and_tag_ack_warn_then_fresh_attempt_recovers(self):
        for failure in ('/docs/upsert','/tags','wrong_marker'):
            with self.subTest(failure=failure):
                writer, xid, notices = self.writer(wait=250)
                thread, errors = self.commit(writer)
                store = self.remote['vec'].client
                if failure == 'wrong_marker':
                    store.wrong_marker = True
                else:
                    store.fail_after = failure
                with self.assertRaises(ProbeError):
                    self.publish('vec')
                old = self.conn.execute("SELECT active_attempt FROM pgos_replay_probe.batches WHERE generation=%s AND state='pending'", (self.gen['vec'],)).fetchone()[0]
                self.publish('txt')
                self.finished(thread, errors, notices, warning=True)
                self.assertFalse(self.ready(xid))
                self.assert_replay_blocked('vec')
                store.wrong_marker = False
                result = self.publish('vec')
                self.assertNotEqual(result['attempt'], str(old))
                self.assertTrue(self.ready(xid))
                self.assertEqual(self.vector(), self.expected())

    def test_verified_tag_and_uncommitted_publication_cannot_finish_wait(self):
        writer, xid, notices = self.writer()
        thread, errors = self.commit(writer)
        self.publish('vec')
        attempt, snapshot, timestamp = self.stop_after_verified_tag('txt')
        with psycopg.connect() as publisher:
            publisher.execute('SELECT pgos_replay_probe.publish(%s,%s,%s)', (attempt,snapshot,timestamp))
            self.assertFalse(self.ready(xid))
            self.assertTrue(thread.is_alive())
            self.assert_replay_blocked('txt')
            publisher.rollback()
        self.assertFalse(self.ready(xid))
        # Retry supersedes the abandoned attempt and commits a verified receipt.
        result = self.publish('txt')
        self.assertNotEqual(result['attempt'], str(attempt))
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute('SELECT pgos_replay_probe.publish(%s,%s,%s)', (attempt,snapshot,timestamp))
        self.finished(thread, errors, notices)

    def test_lost_publication_response_still_completes_from_durable_coverage(self):
        writer, xid, notices = self.writer()
        thread, errors = self.commit(writer)
        self.publish('vec')
        def lose_response(phase):
            if phase == 'published':
                raise RuntimeError('lost publication response')
        with self.assertRaisesRegex(RuntimeError, 'lost publication response'):
            replay(self.conn, self.gen['txt'], self.remote['txt'], checkpoint=lose_response)
        self.finished(thread, errors, notices)
        self.assertTrue(self.ready(xid))
        calls = len(self.remote['txt'].client.calls)
        self.assertIsNone(self.publish('txt'))
        self.assertEqual(len(self.remote['txt'].client.calls), calls)

    def test_observer_death_before_notification_reconciles_durable_publication(self):
        self.conn.execute(f'SELECT {P}.control(4)')
        writer, xid, notices = self.writer(wait=6000)
        thread, errors = self.commit(writer)
        self.publish_all()
        self.until(lambda: self.worker_status()[1] == 3)
        old_pid = self.worker_status()[0]
        self.assertTrue(self.ready(xid))
        self.assertTrue(thread.is_alive())
        self.conn.execute('SELECT pg_terminate_backend(%s)', (old_pid,))
        self.conn.execute(f'SELECT {P}.control(0)')
        self.until(lambda: self.worker_status()[0] not in (0,old_pid))
        self.finished(thread, errors, notices)

    def test_replay_process_killed_after_attempt_then_restarted(self):
        writer, xid, notices = self.writer(wait=1200)
        thread, errors = self.commit(writer)
        code = '''import sys, psycopg
with psycopg.connect(autocommit=True) as c:
    batch=c.execute('SELECT pgos_replay_probe.claim(%s)',(sys.argv[1],)).fetchone()[0]
    attempt=c.execute('SELECT pgos_replay_probe.begin_attempt(%s)',(batch,)).fetchone()[0]
    print(attempt,flush=True)
    sys.stdin.read()
'''
        child = subprocess.Popen([sys.executable,'-u','-c',code,str(self.gen['vec'])], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        try:
            self.assertTrue(select.select([child.stdout],[],[],5)[0])
            old = child.stdout.readline().strip()
            self.assertTrue(old)
        finally:
            child.kill()
            child.wait(5)
            child.stdin.close()
            child.stdout.close()
        self.finished(thread, errors, notices, warning=True)
        self.assertFalse(self.ready(xid))
        self.assert_replay_blocked('vec')
        result = self.publish('vec')
        self.assertNotEqual(result['attempt'], old)
        self.publish('txt')
        self.assertTrue(self.ready(xid))
        self.assertEqual(self.vector(), self.expected())

    def test_late_smaller_event_ids_require_their_own_publication(self):
        a, xa, na = self.writer("UPDATE fixture.docs SET note='a' WHERE id=1")
        b, xb, nb = self.writer("UPDATE fixture.docs SET note='b' WHERE id=2")
        ids_a = [r[0] for r in a.execute('SELECT event_id FROM pgos_capture_probe.outbox WHERE writer_xid=%s::xid8',(xa,))]
        tb, eb = self.commit(b)
        self.publish_all()
        self.finished(tb, eb, nb)
        self.assertFalse(self.ready(xa))
        ta, ea = self.commit(a)
        self.assertTrue(self.ready(xb))
        self.assertFalse(self.ready(xa))
        max_published = self.conn.execute('SELECT max(event_id) FROM pgos_replay_probe.events').fetchone()[0]
        self.assertLess(max(ids_a), max_published)
        self.publish('vec')
        self.assertTrue(ta.is_alive())
        self.publish('txt')
        self.finished(ta, ea, na)
        self.assertTrue(self.ready(xa))

    def test_two_concurrent_waiters_use_frozen_batch_membership(self):
        a, xa, na = self.writer("UPDATE fixture.docs SET note='a' WHERE id=1")
        ta, ea = self.commit(a)
        for mode in ('vec','txt'):
            self.conn.execute('SELECT pgos_replay_probe.claim(%s)', (self.gen[mode],))
        b, xb, nb = self.writer("UPDATE fixture.docs SET note='b' WHERE id=2")
        tb, eb = self.commit(b)
        self.until(lambda: self.worker_status()[2] == 2)
        self.publish_all()  # Frozen batches contain A, not the later B commit.
        self.finished(ta, ea, na)
        self.assertTrue(self.ready(xa))
        self.assertFalse(self.ready(xb))
        self.assertTrue(tb.is_alive())
        self.assert_replay_blocked('vec')
        self.publish_all()
        self.finished(tb, eb, nb)
        self.assertTrue(self.ready(xb))

    def test_observer_timeout_can_warn_after_durable_success(self):
        self.conn.execute(f'SELECT {P}.control(1)')
        self.until(lambda: self.worker_status()[1] == 0)
        writer, xid, notices = self.writer(wait=150)
        thread, errors = self.commit(writer)
        self.publish_all()
        self.finished(thread, errors, notices, warning=True)
        # The bounded callback cannot run SQL. Its conservative warning is not
        # a failed PG commit or proof that durable publication is incomplete.
        self.assertTrue(self.ready(xid))
        self.assertEqual(self.vector(), self.expected())
        self.conn.execute(f'SELECT {P}.control(0)')

    def test_coalesced_events_savepoints_rollback_and_own_write_boundary(self):
        writer, xid, notices = self.writer("UPDATE fixture.docs SET note='first' WHERE id=1")
        writer.execute('SAVEPOINT discarded')
        writer.execute("UPDATE fixture.docs SET id=10 WHERE id=1")
        writer.execute('ROLLBACK TO discarded')
        writer.execute('DELETE FROM fixture.docs WHERE id=1')
        writer.execute("INSERT INTO fixture.docs VALUES(1,'new alpha','[-1,0,0]','replacement')")
        self.assertEqual(self.vector(conn=writer), self.expected(conn=writer))
        with self.assertRaises(psycopg.errors.FeatureNotSupported):
            with writer.transaction():
                writer.execute(executor.BM25, ('alpha',))
        thread, errors = self.commit(writer)
        self.assertEqual([r[1:] for r in self.coverage(xid)], [(3,0),(3,0)])
        self.publish_all()
        self.finished(thread, errors, notices)
        self.assertEqual([r[1:] for r in self.coverage(xid)], [(3,3),(3,3)])
        self.assertEqual(self.remote['vec'].writes[-1]['size'], 1)
        aborted, aborted_xid, aborted_notices = self.writer()
        aborted.execute('ROLLBACK')
        self.assertEqual(self.coverage(aborted_xid), [])
        self.assertFalse(self.ready(aborted_xid))
        self.assertEqual(aborted_notices, [])
        # A savepoint-only write rolls back both requirements and hook marks.
        with psycopg.connect(autocommit=True) as c:
            c.execute('BEGIN'); c.execute('SAVEPOINT only_write')
            c.execute("UPDATE fixture.docs SET note='discarded' WHERE id=2")
            c.execute('ROLLBACK TO only_write'); c.execute('COMMIT')
            self.assertEqual(c.execute(f'SELECT {P}.last_result()').fetchone()[0], 'none')

    def test_prepare_rejected_before_source_commit(self):
        writer, xid, notices = self.writer()
        with self.assertRaises(psycopg.errors.FeatureNotSupported):
            writer.execute("PREPARE TRANSACTION 'unsupported-completion'")
        self.assertEqual(writer.info.transaction_status, psycopg.pq.TransactionStatus.IDLE)
        self.assertEqual(self.coverage(xid), [])
        self.assertEqual(notices, [])
        self.assertEqual(self.conn.execute('SELECT note FROM fixture.docs WHERE id=1').fetchone()[0], 'a')

    def test_postmaster_restart_retains_partial_coverage_and_replay(self):
        writer, xid, notices = self.writer(wait=150)
        thread, errors = self.commit(writer)
        self.finished(thread, errors, notices, warning=True)
        self.publish('vec')
        before = self.coverage(xid)
        writer.close(); self.conn.close()
        subprocess.run(['gosu','postgres','pg_ctl','-D',os.environ['PGDATA'],'-m','immediate','stop'],check=True,stdout=subprocess.DEVNULL)
        subprocess.run(['gosu','postgres','pg_ctl','-D',os.environ['PGDATA'],'-l',os.environ['PGDATA']+'/server.log','-w','start'],check=True,stdout=subprocess.DEVNULL)
        self.conn = psycopg.connect(autocommit=True)
        self.assertEqual(self.coverage(xid), before)
        self.assertFalse(self.ready(xid))
        self.assert_replay_blocked('txt')
        self.conn.execute(f'SELECT {P}.start_worker()')
        self.conn.execute(f'SELECT {P}.control(0)')
        self.publish('txt')
        self.assertTrue(self.ready(xid))
        self.assertEqual(self.vector(), self.expected())

    def test_psql_autocommit_and_psycopg_sync_complete_after_publication(self):
        child = subprocess.Popen(['psql','-X','-v','ON_ERROR_STOP=1','-c',
            "SET onesearch_probe.wait_ms=4000", '-c', "UPDATE fixture.docs SET note='psql' WHERE id=1"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.until(lambda: self.worker_status()[2] == 1)
            self.assertIsNone(child.poll())
            self.publish_all()
            out, err = child.communicate(timeout=6)
            self.assertEqual(child.returncode, 0, err)
            self.assertIn('UPDATE 1',out)
            self.assertNotIn('WARNING',err)
        finally:
            if child.poll() is None:
                child.kill(); child.communicate()
        with psycopg.connect(autocommit=True) as writer:
            writer.execute('SET onesearch_probe.wait_ms=4000')
            notices, errors = [], []
            writer.add_notice_handler(lambda d: notices.append(d.sqlstate))
            def execute():
                try:
                    writer.execute("UPDATE fixture.docs SET note=%s WHERE id=2", ('extended',))
                except Exception as exc:
                    errors.append(exc)
            t = threading.Thread(target=execute); t.start()
            try:
                self.until(lambda: self.worker_status()[2] == 1)
                self.assertTrue(t.is_alive())
                self.publish_all()
            finally:
                t.join(6)
            self.assertFalse(t.is_alive())
            self.assertEqual(errors,[])
            self.assertEqual(notices,[])

    def test_status_and_worker_controls_are_superuser_only(self):
        self.assertFalse(self.ready('0'))
        self.conn.execute('CREATE ROLE completion_unprivileged')
        try:
            self.conn.execute('SET ROLE completion_unprivileged')
            for sql in (f'SELECT {P}.is_published(0::text::xid8)',f'SELECT {P}.start_worker()',
                        f'SELECT * FROM {P}.requirements',f'SELECT {P}.mark()'):
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    self.conn.execute(sql)
        finally:
            self.conn.execute('RESET ROLE')
            self.conn.execute('DROP ROLE completion_unprivileged')


if __name__ == '__main__':
    server = fixture.ThreadingHTTPServer(('127.0.0.1',18443),fixture.Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain('/tmp/probe.crt','/tmp/probe.key')
    server.socket = context.wrap_socket(server.socket,server_side=True)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    with psycopg.connect(autocommit=True) as admin:
        admin.execute(f'SELECT {P}.start_worker()')
    try:
        unittest.main(verbosity=2)
    finally:
        server.shutdown(); server.server_close()
