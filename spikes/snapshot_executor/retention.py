"""Snapshot/worker-aware ref deletion using real PG and local immutable Tags."""
from copy import deepcopy
import os
import ssl
import subprocess
import threading
import unittest

import psycopg
import offline as executor
from cleanup import collect
from client import HttpFailure, ProbeError
from worker import replay, name

fixture = executor.fixture


class CleanupClient:
    """Ref lifecycle fixture wrapped around the existing write/search store."""
    def __init__(self, store):
        self.store = store
        self.deletes = []
        self.owner = fixture.OWNER
        self.tag_identity = {}
        self.fail_after = None
        self.noop_delete = False
        self.conflict = False
        self.bad_inventory = False

    def request(self, method, path, body=None, expected=200):
        suffix = path.removeprefix('/collections/' + self.store.collection)
        if method == 'GET' and suffix == '':
            return {'collection': {'tags': {'pgos-run': self.owner}}}
        if method == 'GET' and suffix == '/tags':
            if self.bad_inventory:
                return {'tags': {}}
            return {'tags': [dict(name=ref, **self.tag_identity.get(ref, self.store.snapshots[ref]))
                             for ref in self.store.tags]}
        if method == 'GET' and suffix == '/branches':
            return {'branches': [{'name': ref, 'headSnapshot': self.store.snapshots[ref]}
                                 for ref in self.store.branches]}
        if method == 'DELETE':
            kind, ref = suffix.strip('/').split('/')
            assert ref.startswith('attempt-')
            if self.conflict:
                raise HttpFailure(409)
            values = self.store.tags if kind == 'tags' else self.store.branches
            if ref not in values:
                raise HttpFailure(404)
            if not self.noop_delete:
                del values[ref]
            self.deletes.append((kind, ref))
            if self.fail_after == kind:
                self.fail_after = None
                raise ProbeError('Lost delete response')
            return {}
        raise AssertionError((method, path))


class RetentionTests(unittest.TestCase):
    setUp = executor.ExecutorTests.setUp
    publish = executor.ExecutorTests.publish
    vector = executor.ExecutorTests.vector
    expected = executor.ExecutorTests.expected
    view = executor.ExecutorTests.view
    wait_lock = executor.ExecutorTests.wait_lock

    def client(self, mode='vec'):
        return CleanupClient(self.remote[mode].client)

    def plan(self, mode='vec', conn=None):
        return (conn or self.conn).execute('SELECT pgos_retention_probe.plan(%s)',
                                          (self.gen[mode],)).fetchone()[0]

    def collect(self, client, mode='vec', conn=None, **kwargs):
        return collect(conn or self.conn, self.gen[mode], client, **kwargs)

    def advance(self, mode='vec'):
        old = self.view(mode)
        self.conn.execute("UPDATE fixture.docs SET note=note||'x' WHERE id=1")
        new = self.publish(mode)
        return old, new

    def job_state(self, batch):
        return self.conn.execute('SELECT state FROM pgos_retention_probe.jobs WHERE batch_id=%s',
                                 (batch,)).fetchone()[0]

    def test_current_base_preserved_old_refs_deleted_history_and_search_survive(self):
        client = self.client()
        self.assertIsNone(self.collect(client))
        old, new = self.advance()
        audit = self.conn.execute('SELECT count(*) FROM pgos_replay_probe.events').fetchone()[0]
        self.assertEqual(self.collect(client), old['batch'])
        self.assertEqual(client.deletes, [('tags', old['tag']), ('branches', old['tag'])])
        self.assertEqual(self.job_state(old['batch']), 'done')
        self.assertIn(new['tag'], client.store.tags)
        self.assertIn(new['tag'], client.store.branches)
        self.assertIn('main', client.store.branches)
        self.assertEqual(self.vector(), self.expected())
        self.assertEqual(self.conn.execute('SELECT count(*) FROM pgos_replay_probe.events').fetchone()[0], audit)
        self.assertIsNone(self.collect(client))
        # Replay still branches from the retained current base after history GC.
        self.advance()
        self.assertEqual(self.vector(), self.expected())

    def test_old_statement_before_capture_pins_tag_through_new_publication(self):
        client = self.client()
        old = self.view()
        expected = self.expected()
        with psycopg.connect(autocommit=True) as reader, psycopg.connect(autocommit=True) as gate:
            reader.execute('SET statement_timeout=10000')
            reader.execute('SET pgos_snapshot_probe.pause_before_capture=9031')
            gate.execute('SELECT pg_advisory_lock(9031)')
            output, errors = [], []
            def read():
                try:
                    output.append(self.vector(conn=reader))
                except Exception as exc:
                    errors.append(exc)
            thread = threading.Thread(target=read)
            thread.start()
            try:
                self.wait_lock(reader)
                self.assertIsNone(self.conn.execute('SELECT backend_xid FROM pg_stat_activity WHERE pid=%s',
                                                   (reader.info.backend_pid,)).fetchone()[0])
                self.advance()
                # The reader has not assembled its view yet; a pin registered
                # only after Tag selection would miss precisely this window.
                self.assertIsNone(self.collect(client))
                self.assertEqual(client.deletes, [])
                self.assertIn(old['tag'], client.store.tags)
            finally:
                gate.execute('SELECT pg_advisory_unlock(9031)')
                thread.join(10)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(output, [expected])
            self.assertEqual(fixture.CALLS[-1]['ref']['name'], old['tag'])
        self.assertEqual(self.collect(client), old['batch'])
        self.assertEqual(self.vector(), self.expected())

    def test_pending_and_abandoned_attempts_pin_parent_even_after_winner_publishes(self):
        client = self.client()
        base = self.view()
        self.conn.execute("UPDATE fixture.docs SET note=note||'x' WHERE id=1")
        batch = self.conn.execute('SELECT pgos_replay_probe.claim(%s)', (self.gen['vec'],)).fetchone()[0]
        abandoned = self.conn.execute('SELECT pgos_replay_probe.begin_attempt(%s)', (batch,)).fetchone()[0]
        self.assertIsNone(self.collect(client))
        winner = self.publish()
        self.assertIsNone(self.collect(client))  # An old HTTP branch-create may still use base.
        self.advance()
        self.assertEqual(self.collect(client), winner['batch'])
        self.assertIn(base['tag'], client.store.branches)
        self.assertNotIn(('branches', name(abandoned)), client.deletes)
        self.assertIsNone(self.collect(client))

    def test_drop_rollback_then_committed_retirement_keeps_pg_rows_and_sibling(self):
        client = self.client()
        old = self.view()
        rows = self.conn.execute('SELECT * FROM fixture.docs ORDER BY id').fetchall()
        with psycopg.connect() as ddl:
            ddl.execute('DROP INDEX fixture.vec')
            ddl.rollback()
        self.assertIsNone(self.collect(client))
        self.conn.execute('DROP INDEX fixture.vec')
        self.assertEqual(self.collect(client), old['batch'])
        self.assertEqual(self.conn.execute('SELECT * FROM fixture.docs ORDER BY id').fetchall(), rows)
        self.assertEqual(self.conn.execute(executor.BM25, ('alpha',)).fetchall(), [(1, 'a', 1.0)])
        self.assertIn('main', client.store.branches)  # Collection removal is intentionally separate.
        self.assertEqual(self.conn.execute("SELECT count(*) FROM pgos_capture_probe.outbox WHERE operation='retire'").fetchone()[0], 1)

    def test_reindex_retirement_does_not_touch_replacement_binding(self):
        client = self.client()
        old = self.view()
        old_generation = self.gen['vec']
        self.conn.execute('REINDEX INDEX fixture.vec')
        replacement = self.conn.execute("SELECT pgos_capture_probe.register_index('fixture.vec')").fetchone()[0]
        collection = 'pgos-live-offline-replacement'
        store = fixture.Store(collection)
        fixture.STORES[collection] = store
        self.conn.execute('SELECT pgos_replay_probe.bind(%s,%s,%s)', (replacement, collection, fixture.OWNER))
        result = replay(self.conn, replacement, fixture.Remote(store))
        self.conn.execute("SELECT pgos_snapshot_probe.set_test_health('fixture.vec',true)")
        self.assertEqual(self.collect(client), old['batch'])
        self.assertIn(result['tag'], store.tags)
        self.assertEqual(self.vector(), self.expected())
        self.assertNotEqual(replacement, old_generation)

    def test_lost_delete_response_and_post_delete_crash_resume_durable_job(self):
        client = self.client()
        old, new = self.advance()
        client.fail_after = 'tags'
        with self.assertRaisesRegex(ProbeError, 'Lost delete response'):
            self.collect(client)
        self.assertEqual(self.job_state(old['batch']), 'pending')
        self.assertNotIn(old['tag'], client.store.tags)
        self.assertIn(old['tag'], client.store.branches)
        # Force a real PG restart; the remote fixture retains the partial deletion.
        self.conn.close()
        subprocess.run(['gosu','postgres','pg_ctl','-m','immediate','-w','stop'], check=True, capture_output=True)
        subprocess.run(['gosu','postgres','pg_ctl','-l',os.environ['PGDATA']+'/server.log','-w','start'],
                       check=True, capture_output=True)
        self.conn = psycopg.connect(autocommit=True)
        self.addCleanup(self.conn.close)
        self.assertEqual(self.job_state(old['batch']), 'pending')
        def stop(phase):
            if phase == 'absent':
                raise RuntimeError('Collector died before finish')
        with self.assertRaisesRegex(RuntimeError, 'Collector died'):
            self.collect(client, checkpoint=stop)
        self.assertEqual(self.job_state(old['batch']), 'pending')
        self.assertEqual(self.collect(client), old['batch'])
        self.assertEqual(self.job_state(old['batch']), 'done')
        self.assertIn(new['tag'], client.store.tags)
        self.assertEqual(self.vector(), self.expected())

    def test_owner_identity_inventory_conflict_and_false_ack_fail_closed(self):
        client = self.client()
        old, new = self.advance()
        client.owner = 'not-this-owner'
        with self.assertRaisesRegex(ProbeError, 'ownership'):
            self.collect(client)
        client.owner = fixture.OWNER
        client.tag_identity[old['tag']] = {'snapshotId': 'foreign', 'snapshotCommittedAt': 1}
        with self.assertRaisesRegex(ProbeError, 'Tag identity'):
            self.collect(client)
        client.tag_identity.clear()
        saved = deepcopy(client.store.snapshots[old['tag']])
        client.store.snapshots[old['tag']] = {'snapshotId': 'foreign', 'snapshotCommittedAt': 1}
        client.tag_identity[old['tag']] = saved
        with self.assertRaisesRegex(ProbeError, 'Branch identity'):
            self.collect(client)
        client.store.snapshots[old['tag']] = saved
        client.tag_identity.clear()
        client.bad_inventory = True
        with self.assertRaisesRegex(ProbeError, 'Malformed'):
            self.collect(client)
        client.bad_inventory = False
        self.assertEqual(client.deletes, [])
        client.conflict = True
        with self.assertRaises(HttpFailure):
            self.collect(client)
        client.conflict = False
        client.noop_delete = True
        with self.assertRaisesRegex(ProbeError, 'still present'):
            self.collect(client)
        self.assertEqual(self.job_state(old['batch']), 'pending')
        client.noop_delete = False
        self.assertEqual(self.collect(client), old['batch'])
        self.assertIn(new['tag'], client.store.tags)

    def test_concurrent_collectors_share_one_plan_and_accept_absent_refs(self):
        client = self.client()
        old, new = self.advance()
        once = []
        def interleave(phase):
            if phase == 'checked' and not once:
                once.append(True)
                with psycopg.connect(autocommit=True) as other:
                    self.assertEqual(self.collect(client, conn=other), old['batch'])
        self.assertEqual(self.collect(client, checkpoint=interleave), old['batch'])
        self.assertEqual(self.job_state(old['batch']), 'done')
        self.assertEqual(client.deletes, [('tags', old['tag']), ('branches', old['tag'])])
        self.assertIn(new['tag'], client.store.tags)

    def test_plan_rollback_committed_protocol_and_permissions(self):
        old, _ = self.advance()
        with psycopg.connect() as planning:
            batch = self.plan(conn=planning)
            self.assertEqual(str(batch), old['batch'])
            with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
                planning.execute('SELECT pgos_retention_probe.finish(%s)', (batch,))
            planning.rollback()
        self.assertEqual(self.conn.execute('SELECT count(*) FROM pgos_retention_probe.jobs').fetchone()[0], 0)
        with psycopg.connect() as transaction:
            with self.assertRaises(ProbeError):
                self.collect(self.client(), conn=transaction)
        self.conn.execute('CREATE ROLE retention_reader')
        try:
            self.conn.execute('SET ROLE retention_reader')
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                self.plan()
        finally:
            self.conn.execute('RESET ROLE')
            self.conn.execute('DROP ROLE retention_reader')
        self.assertEqual(self.collect(self.client()), old['batch'])


if __name__ == '__main__':
    server = fixture.ThreadingHTTPServer(('127.0.0.1',18443), fixture.Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain('/tmp/probe.crt', '/tmp/probe.key')
    server.socket = context.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        unittest.main(verbosity=2)
    finally:
        server.shutdown()
        server.server_close()
