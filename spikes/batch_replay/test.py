"""Real PG transactions plus a deterministic REST fixture for replay failure boundaries."""
from copy import deepcopy
import json
import os
import subprocess
import threading
import unittest
import uuid

import psycopg
from worker import Remote, replay, chunks, encoded, name, MARKER, ProbeError, P

OWNER = 'a' * 32
COLLECTION = 'pgos-live-offline-text'
C = 'pgos_capture_probe'


class FakeClient:
    def __init__(self):
        self.branches = {'main': {}}
        self.snapshots = {'main': None}
        self.tags = {}
        self.calls = []
        self.fail_after = None
        self.wrong_parent = False
        self.wrong_marker = False
        self.snapshot_counter = 0
        self.fail_write_number = None

    def items(self, value):
        return value['docs']

    def request(self, method, path, body=None, expected=200):
        self.calls.append((method,path,deepcopy(body)))
        suffix = path.removeprefix('/collections/' + COLLECTION)
        if method=='GET' and suffix=='':
            return {'collection': {'tags': {'pgos-run': OWNER}}}
        if suffix=='/branches':
            branch = body['branchName']
            assert branch not in self.branches
            source = body['source']['name']
            base = self.snapshots[source]
            if source != 'main':
                assert body['source']['asOf'] == base['snapshotCommittedAt']
            self.branches[branch] = deepcopy(self.branches[source])
            self.snapshots[branch] = deepcopy(base)
            return {'branch': {'name': branch, 'headSnapshot': base,
                               'parentSnapshot': {} if self.wrong_parent else base}}
        if suffix in ('/docs/upsert','/docs/delete'):
            branch = body['branch']
            if suffix.endswith('upsert'):
                for doc in body['docs']:
                    self.branches[branch][doc['id']] = deepcopy(doc)
            else:
                for key in body['ids']:
                    self.branches[branch].pop(key,None)
            self.snapshot_counter += 1
            self.snapshots[branch] = {'snapshotId': 'snapshot-'+str(self.snapshot_counter),
                                      'snapshotCommittedAt': self.snapshot_counter}
            if self.fail_after == suffix or self.fail_write_number == self.snapshot_counter:
                self.fail_after = None
                raise ProbeError('Injected lost write acknowledgement')
            return {}
        if suffix=='/tags':
            branch,tag = body['source']['name'],body['tagName']
            assert tag not in self.tags
            self.tags[tag] = deepcopy(self.branches[branch])
            if self.fail_after == suffix:
                self.fail_after = None
                raise ProbeError('Injected lost Tag acknowledgement')
            return {'tag': {'name': tag, **self.snapshots[branch]}}
        if suffix=='/docs/fetch':
            ref = body['ref']
            if ref['kind']=='branch':
                assert body['consistentRead'] is False
                docs = self.branches[ref['name']]
            else:
                assert 'consistentRead' not in body
                docs = self.tags[ref['name']]
            values = [deepcopy(docs[key]) for key in body['ids'] if key in docs]
            if self.wrong_marker and ref['kind']=='tag':
                values = []
            return {'docs': [{'doc':doc} for doc in values]}
        raise AssertionError((method,path))


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg.connect(autocommit=True)
        self.addCleanup(lambda: self.conn.close())
        self.conn.execute('DROP SCHEMA IF EXISTS fixture CASCADE')
        self.conn.execute(f'TRUNCATE {C}.sources CASCADE')
        self.conn.execute('CREATE SCHEMA fixture')
        self.conn.execute('CREATE TABLE fixture.docs(id bigint PRIMARY KEY,content text)')
        self.conn.execute("INSERT INTO fixture.docs VALUES(1,'alpha'),(2,'beta'),(3,NULL)")
        self.conn.execute('CREATE INDEX txt ON fixture.docs USING pgos_lifecycle(content pgos_lifecycle_probe.text_ops)')
        self.gen = self.conn.execute(f"SELECT {C}.register_index('fixture.txt')").fetchone()[0]
        self.conn.execute(f'SELECT {P}.bind(%s,%s,%s)',(self.gen,COLLECTION,OWNER))
        self.client = FakeClient()
        self.remote = Remote(self.client)

    def claim(self, conn=None):
        return (conn or self.conn).execute(f'SELECT {P}.claim(%s)',(self.gen,)).fetchone()[0]

    def attempt(self, batch):
        return self.conn.execute(f'SELECT {P}.begin_attempt(%s)',(batch,)).fetchone()[0]

    def publish(self, attempt, conn=None):
        (conn or self.conn).execute(f'SELECT {P}.publish(%s,%s,%s)',(attempt,'receipt',123))

    def pending(self):
        return self.conn.execute(f'SELECT event_id,operation,row_key,document FROM {C}.outbox ORDER BY event_id').fetchall()

    def run_replay(self, **kwargs):
        return replay(self.conn,self.gen,self.remote,**kwargs)

    def test_initial_and_delta_coalescing_batched_and_old_tag_immutable(self):
        first = self.run_replay()
        initial = deepcopy(self.client.tags[first['tag']])
        self.assertEqual([x['size'] for x in self.remote.writes],[2])
        with self.conn.transaction():
            self.conn.execute("UPDATE fixture.docs SET content='intermediate' WHERE id=1")
            self.conn.execute("UPDATE fixture.docs SET id=10,content='final' WHERE id=1")
            self.conn.execute('DELETE FROM fixture.docs WHERE id=2')
            self.conn.execute("INSERT INTO fixture.docs VALUES(4,'new'),(5,'also new')")
        before = self.pending()
        second = self.run_replay()
        got = self.client.tags[second['tag']]
        self.assertEqual({k:v for k,v in got.items() if k!=MARKER},
                         {'10':{'id':'10','content':'final'},'4':{'id':'4','content':'new'},'5':{'id':'5','content':'also new'}})
        self.assertEqual(self.client.tags[first['tag']],initial)
        self.assertEqual([(x['operation'],x['size']) for x in self.remote.writes],[('upsert',2),('delete',2),('upsert',3)])
        self.assertEqual(self.pending(),[])
        self.assertEqual(self.conn.execute(f'SELECT count(*) FROM {P}.events WHERE batch_id=%s',(second['batch'],)).fetchone()[0],len(before))
        self.assertIsNone(self.run_replay())

    def test_frozen_claim_new_writes_and_late_smaller_id(self):
        self.run_replay()
        with psycopg.connect() as late:
            late.execute("UPDATE fixture.docs SET content='late smaller' WHERE id=1")
            small = late.execute(f'SELECT max(event_id) FROM {C}.outbox').fetchone()[0]
            self.conn.execute("UPDATE fixture.docs SET content='early larger' WHERE id=2")
            batch = self.claim()
            self.assertTrue(all(r[0]>small for r in self.pending()))
            late.commit()
            self.assertEqual(self.claim(),batch)
            first = self.run_replay()
            self.assertEqual(self.client.tags[first['tag']]['1']['content'],'alpha')
            self.assertEqual([x[0] for x in self.pending()],[small])
            second = self.run_replay()
            self.assertEqual(self.client.tags[second['tag']]['1']['content'],'late smaller')
            self.assertEqual(self.pending(),[])

    def test_same_key_writer_serialization(self):
        self.run_replay()
        errors = []
        with psycopg.connect() as a, psycopg.connect(autocommit=True) as b:
            a.execute("UPDATE fixture.docs SET content='first' WHERE id=1")
            def write():
                try:
                    b.execute("UPDATE fixture.docs SET content='second' WHERE id=1")
                except Exception as exc:
                    errors.append(exc)
            thread = threading.Thread(target=write)
            thread.start()
            # Observe the actual PG lock wait, not just a scheduling sleep.
            import time
            until = time.monotonic()+5
            while True:
                wait = self.conn.execute('SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s',(b.info.backend_pid,)).fetchone()[0]
                if wait=='Lock': break
                self.assertLess(time.monotonic(),until)
                time.sleep(.01)
            self.assertEqual(self.pending(),[])
            a.commit()
            thread.join(5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors,[])
        self.assertEqual([r[3]['content'] for r in self.pending()],['first','second'])
        result = self.run_replay()
        self.assertEqual(self.client.tags[result['tag']]['1']['content'],'second')

    def test_stale_attempt_cannot_publish_or_mutate_winner(self):
        batch = self.claim()
        old = self.attempt(batch)
        new = self.attempt(batch)
        target = (COLLECTION,OWNER)
        old_receipt = self.remote.apply(target,old,None,[],[{'id':'1','content':'stale'}])
        new_receipt = self.remote.apply(target,new,None,[],[{'id':'1','content':'winner'}])
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute(f'SELECT {P}.publish(%s,%s,%s)',(old,*old_receipt))
        self.conn.execute(f'SELECT {P}.publish(%s,%s,%s)',(new,*new_receipt))
        self.client.request('POST','/collections/'+COLLECTION+'/docs/upsert',
                            {'docs':[{'id':'1','content':'late stale'}],'branch':name(old)},expected=202)
        self.assertEqual(self.client.tags[name(new)]['1']['content'],'winner')
        self.assertEqual(self.pending(),[])

    def test_lost_ack_write_and_tag_retry_fresh_branch(self):
        for failure in ('/docs/upsert','/tags'):
            self.conn.execute("UPDATE fixture.docs SET content=content||' changed' WHERE id=1")
            before = self.pending()
            self.client.fail_after = failure
            with self.assertRaises(ProbeError): self.run_replay()
            self.assertEqual(self.pending(),before)
            result = self.run_replay()
            branches = [body['branchName'] for method,path,body in self.client.calls if path.endswith('/branches')]
            self.assertEqual(len(set(branches)),len(branches))
            self.assertEqual(self.pending(),[])
            self.assertEqual(self.client.tags[result['tag']]['1']['content'],self.conn.execute('SELECT content FROM fixture.docs WHERE id=1').fetchone()[0])

    def test_verified_tag_before_publication_failure_is_retryable(self):
        before = self.pending()
        def crash(phase):
            if phase=='tag_verified': raise ProbeError('Injected worker death')
        with self.assertRaises(ProbeError): self.run_replay(checkpoint=crash)
        self.assertEqual(self.pending(),before)
        self.assertEqual(len(self.client.tags),1)
        self.run_replay()
        self.assertEqual(len(self.client.tags),2)
        self.assertEqual(self.pending(),[])

    def test_wrong_base_and_wrong_tag_marker_preserve_outbox(self):
        before = self.pending()
        self.client.wrong_parent = True
        with self.assertRaisesRegex(ProbeError,'parent snapshot'): self.run_replay()
        self.assertFalse(self.remote.writes)
        self.client.wrong_parent = False
        self.client.wrong_marker = True
        with self.assertRaisesRegex(ProbeError,'exact attempt marker'): self.run_replay()
        self.assertEqual(self.pending(),before)
        self.client.wrong_marker = False
        self.run_replay()

    def test_atomic_publication_rollback_and_duplicate_receipt(self):
        batch = self.claim()
        attempt = self.attempt(batch)
        before = self.pending()
        with psycopg.connect() as publishing:
            self.publish(attempt,publishing)
            self.assertEqual(self.pending(),before)
            self.assertIsNone(self.conn.execute(f'SELECT current_batch FROM {P}.targets').fetchone()[0])
            publishing.rollback()
        self.assertEqual(self.pending(),before)
        self.publish(attempt)
        self.publish(attempt)
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute(f'SELECT {P}.publish(%s,%s,%s)',(attempt,'changed',123))
        self.assertEqual(self.pending(),[])

    def test_crash_recovery_pending_and_published(self):
        batch = self.claim()
        attempt = self.attempt(batch)
        before = self.pending()
        self.conn.execute('CHECKPOINT')
        publishing = psycopg.connect()
        self.publish(attempt,publishing)
        def restart():
            for args in [('gosu','postgres','pg_ctl','-m','immediate','-w','stop'),
                         ('gosu','postgres','pg_ctl','-l',os.environ['PGDATA']+'/server.log','-w','start')]:
                subprocess.check_output(args,stderr=subprocess.STDOUT)
            self.conn.close()
            self.conn = psycopg.connect(autocommit=True)
        restart()
        publishing.close()
        self.assertEqual(self.pending(),before)
        self.assertEqual(self.claim(),batch)
        self.publish(attempt)
        restart()
        self.assertEqual(self.pending(),[])
        self.assertEqual(self.conn.execute(f'SELECT current_batch FROM {P}.targets').fetchone()[0],batch)
        self.publish(attempt)

    def test_claim_limits_own_writes_and_uncommitted_protocol(self):
        with self.conn.transaction():
            self.conn.execute("UPDATE fixture.docs SET content='own write' WHERE id=1")
            with self.assertRaises(psycopg.errors.ProgramLimitExceeded):
                with self.conn.transaction(): self.claim()
        with self.conn.transaction():
            batch = self.claim()
            with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
                with self.conn.transaction(): self.attempt(batch)
        with self.conn.transaction():
            attempt = self.attempt(batch)
            with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
                with self.conn.transaction(): self.publish(attempt)
        self.publish(attempt)
        self.conn.execute("INSERT INTO fixture.docs SELECT x,'bulk' FROM generate_series(10,1010) x")
        before = self.pending()
        with self.assertRaises(psycopg.errors.ProgramLimitExceeded): self.claim()
        self.assertEqual(self.pending(),before)
        self.assertEqual(self.conn.execute(f"SELECT count(*) FROM {P}.batches WHERE state='pending'").fetchone()[0],0)

    def test_byte_limit_fails_without_partial_claim(self):
        self.conn.execute("UPDATE fixture.docs SET content=repeat('x',9000000) WHERE id=1")
        with self.assertRaises(psycopg.errors.ProgramLimitExceeded): self.claim()
        self.assertEqual(self.conn.execute(f'SELECT count(*) FROM {P}.events').fetchone()[0],0)

    def test_changed_coverage_and_ddl_fail_closed(self):
        batch = self.claim()
        attempt = self.attempt(batch)
        with self.conn.transaction():
            self.conn.execute('SAVEPOINT corrupt')
            self.conn.execute(f"DELETE FROM {C}.outbox WHERE operation='create'")
            with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
                with self.conn.transaction(): self.publish(attempt)
            self.conn.execute('ROLLBACK TO corrupt')
        self.conn.execute('REINDEX INDEX fixture.txt')
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState): self.publish(attempt)
        self.conn.execute(f"SELECT {C}.register_index('fixture.txt')")
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState): self.publish(attempt)
        self.assertTrue(self.pending())

    def test_binding_and_permissions(self):
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute(f'SELECT {P}.bind(%s,%s,%s)',(self.gen,'pgos-live-different',OWNER))
        self.conn.execute('CREATE ROLE replay_reader')
        try:
            self.conn.execute('SET ROLE replay_reader')
            with self.assertRaises(psycopg.errors.InsufficientPrivilege): self.claim()
        finally:
            self.conn.execute('RESET ROLE')
            self.conn.execute('DROP ROLE replay_reader')

    def test_concurrent_claims_share_one_frozen_batch(self):
        with psycopg.connect() as first, psycopg.connect(autocommit=True) as second:
            batch = self.claim(first)
            second.execute("SET lock_timeout='100ms'")
            with self.assertRaises(psycopg.errors.LockNotAvailable): self.claim(second)
            first.commit()
            self.assertEqual(self.claim(second),batch)
        self.assertEqual(self.conn.execute(f'SELECT count(*) FROM {P}.batches').fetchone()[0],1)

    def test_drop_after_verified_tag_rejects_publication(self):
        def drop(phase):
            if phase=='tag_verified': self.conn.execute('DROP INDEX fixture.txt')
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.run_replay(checkpoint=drop)
        self.assertTrue(any(r[1]=='retire' for r in self.pending()))
        self.assertIsNone(self.conn.execute(f'SELECT current_batch FROM {P}.targets').fetchone()[0])

    def test_failure_after_second_rpc_retries_whole_frozen_batch(self):
        self.conn.execute("INSERT INTO fixture.docs SELECT x,repeat('x',900000) FROM generate_series(10,14) x")
        before = self.pending()
        self.client.fail_write_number = 2
        with self.assertRaises(ProbeError): self.run_replay()
        self.assertEqual(self.pending(),before)
        self.assertGreater(self.remote.writes[0]['size'],1)
        self.client.fail_write_number = None
        result = self.run_replay()
        self.assertEqual(len(self.client.tags[result['tag']]),8)  # seven data documents + marker
        self.assertEqual(self.pending(),[])

    def test_chunk_budget_accounts_for_branch_and_utf8(self):
        branch = name(uuid.uuid4())
        rows = [{'id':str(i),'content':'한글'*20} for i in range(10)]
        batches = list(chunks(rows,'docs',branch,500))
        self.assertEqual(sum(batches,[]),rows)
        self.assertTrue(all(len(encoded({'docs':b,'branch':branch}))<=500 for b in batches))
        self.assertTrue(any(len(b)>1 for b in batches))
        with self.assertRaises(ProbeError): list(chunks(rows,'docs',branch,10))


if __name__=='__main__':
    unittest.main(verbosity=2)
