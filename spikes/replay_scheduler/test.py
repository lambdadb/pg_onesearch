"""Automatic replay discovery/retry with real PG and the offline Tag fixture."""
import importlib.util
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

path = Path(__file__).resolve().parents[1] / 'publication_commit' / 'test.py'
spec = importlib.util.spec_from_file_location('completion_fixture', path)
completion = importlib.util.module_from_spec(spec)
spec.loader.exec_module(completion)
from scheduler import Scheduler, S
from worker import ProbeError

fixture = completion.fixture
P = completion.P
Base = completion.CompletionTests


class SchedulerTests(unittest.TestCase):
    until = Base.until
    writer = Base.writer
    run_commit = staticmethod(Base.run_commit)
    finished = Base.finished
    ready = Base.ready
    coverage = Base.coverage
    worker_status = Base.worker_status
    publish = Base.publish
    vector = Base.vector
    expected = Base.expected
    view = Base.view
    assert_replay_blocked = Base.assert_replay_blocked

    def setUp(self):
        Base.setUp(self)
        self.conn.execute(f'UPDATE {S}.config SET base_delay_ms=100,max_delay_ms=400')

    def commit(self, conn):
        # With automatic replay the observer may finish before the test thread
        # sees its waiter slot. Assert the durable outcome, not that timing race.
        errors=[]
        thread=threading.Thread(target=lambda:self.run_commit(conn,errors))
        thread.start()
        self.addCleanup(lambda:thread.join(11))
        return thread,errors

    def remote_for(self, generation):
        return self.remote[next(k for k,v in self.gen.items() if v == generation)]

    def scheduler(self, remote_for=None, checkpoint=lambda gen,phase: None):
        conn = psycopg.connect(autocommit=True)
        self.addCleanup(conn.close)
        sched = Scheduler(conn, remote_for or self.remote_for, checkpoint)
        self.addCleanup(sched.close)
        return sched

    def loop(self, remote_for=None, checkpoint=lambda gen,phase: None):
        sched = self.scheduler(remote_for, checkpoint)
        stop, errors = threading.Event(), []
        def run():
            try:
                sched.run(stop, poll_seconds=.02)
            except Exception as exc:
                errors.append(exc)
        thread = threading.Thread(target=run)
        thread.start()
        def cleanup():
            stop.set(); thread.join(6)
            self.assertFalse(thread.is_alive(), 'scheduler did not stop')
        self.addCleanup(cleanup)
        self.until(lambda: sched.started or errors)
        self.assertEqual(errors, [])
        return sched, stop, thread, errors

    def job(self, gen):
        return self.conn.execute(f'''SELECT attempts,consecutive_unconfirmed,last_error_kind,last_sqlstate,
            extract(epoch FROM next_attempt_at-clock_timestamp()),last_success_at
            FROM {S}.jobs WHERE generation=%s''',(gen,)).fetchone()

    def test_automatic_two_index_commit_and_batching_without_manual_replay(self):
        sched, stop, thread, errors = self.loop()
        writer, xid, notices = self.writer("UPDATE fixture.docs SET content=content||' alpha' WHERE id IN (1,2)")
        waiting, failures = self.commit(writer)
        self.finished(waiting, failures, notices)
        self.assertTrue(self.ready(xid))
        self.assertEqual([r[1:] for r in self.coverage(xid)],[(2,2),(2,2)])
        for mode in ('vec','txt'):
            self.assertEqual(self.remote[mode].writes[-1]['size'],2)
        self.assertEqual(self.vector(),self.expected())
        self.assertEqual(errors,[])
        self.assertEqual(sched.conn.info.transaction_status, psycopg.pq.TransactionStatus.IDLE)

    def test_ambiguous_ack_automatically_retries_fresh_attempt(self):
        self.remote['vec'].client.fail_after='/tags'
        attempts=[]
        def checkpoint(gen, phase):
            if gen == self.gen['vec'] and phase == 'attempt_committed':
                attempts.append(self.conn.execute("SELECT active_attempt FROM pgos_replay_probe.batches WHERE generation=%s AND state='pending'",(gen,)).fetchone()[0])
        sched, stop, thread, errors = self.loop(checkpoint=checkpoint)
        writer,xid,notices = self.writer()
        waiting, failures = self.commit(writer)
        self.finished(waiting,failures,notices)
        self.assertTrue(self.ready(xid))
        self.assertEqual(len(set(attempts)),2)
        self.assertEqual(self.job(self.gen['vec'])[:4],(2,0,None,None))
        self.assertEqual(errors,[])

    def test_permanent_failure_does_not_starve_healthy_sibling(self):
        self.remote['vec'].client.wrong_marker=True
        sched,stop,thread,errors = self.loop()
        writer,xid,notices = self.writer(wait=700)
        waiting,failures = self.commit(writer)
        self.finished(waiting,failures,notices,warning=True)
        self.assertFalse(self.ready(xid))
        self.assert_replay_blocked('vec')
        self.assertEqual(self.conn.execute(completion.executor.BM25,('alpha',)).fetchone()[0],1)
        job = self.job(self.gen['vec'])
        self.until(lambda:self.job(self.gen['vec'])[0]>=2)
        self.assertEqual(job[2],'remote')
        self.remote['vec'].client.wrong_marker=False
        self.until(lambda:self.ready(xid))
        self.assertEqual(self.vector(),self.expected())
        self.assertEqual(self.job(self.gen['vec'])[1],0)
        self.assertEqual(errors,[])

    def test_backoff_is_durable_across_scheduler_restart_and_capped(self):
        self.conn.execute(f'UPDATE {S}.config SET base_delay_ms=400,max_delay_ms=800')
        writer,xid,notices = self.writer(wait=50)
        waiting,failures = self.commit(writer)
        self.finished(waiting,failures,notices,warning=True)
        class Failure:
            def apply(self,*args):
                raise ProbeError('synthetic private response must not be persisted')
        first = self.scheduler(lambda gen:Failure()).start()
        failed=first.step()['generation']
        row=self.job(failed)
        self.assertEqual(row[:4],(1,1,'remote',None))
        self.assertGreater(row[4],0)
        first.close(); first.conn.close()
        second=self.scheduler(lambda gen:Failure()).start()
        other=second.step()['generation']
        self.assertNotEqual(other,failed)
        self.assertIsNone(second.step())
        self.assertEqual(self.job(failed)[0],1)
        self.until(lambda:self.job(failed)[4]<=0)
        self.assertEqual(second.step()['generation'],failed)
        self.assertEqual(self.job(failed)[:4],(2,2,'remote',None))
        self.assertGreater(self.job(failed)[4],.5)
        self.assertLessEqual(self.job(failed)[4],.8)
        # Advance only the due timestamp as a fixture clock override; capped
        # backoff must remain 800 ms even after many unconfirmed attempts.
        self.conn.execute(f"UPDATE {S}.jobs SET next_attempt_at='-infinity',consecutive_unconfirmed=20 WHERE generation=%s",(failed,))
        self.conn.execute(f"UPDATE {S}.jobs SET next_attempt_at='infinity' WHERE generation=%s",(other,))
        self.assertEqual(second.step()['generation'],failed)
        self.assertGreater(self.job(failed)[4],.5)
        self.assertLessEqual(self.job(failed)[4],.8)

    def test_single_owner_reentrant_rejection_and_session_loss(self):
        first=self.scheduler().start()
        second=self.scheduler()
        with self.assertRaisesRegex(ProbeError,'Another scheduler'):
            second.start()
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            first.conn.execute(f'SELECT {S}.acquire()')
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            second.conn.execute(f'SELECT {S}.next_generation()')
        first.conn.close()  # PostgreSQL releases leadership on backend exit.
        self.until(lambda:self.conn.execute("SELECT NOT EXISTS(SELECT FROM pg_locks WHERE locktype='advisory' AND classid=1885826931 AND objid=1920363641 AND granted)").fetchone()[0])
        second.start()
        self.assertIsNone(second.step())
        second.close()
        with self.assertRaises(ProbeError):
            second.step()
        second.conn.execute('SELECT pg_advisory_lock_shared(1885826931,1920363641)')
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            second.conn.execute(f'SELECT {S}.next_generation()')
        second.conn.execute('SELECT pg_advisory_unlock_shared(1885826931,1920363641)')

    def test_killed_scheduler_keeps_deadline_and_attempt_then_recovers(self):
        self.conn.execute(f'UPDATE {S}.config SET base_delay_ms=500,max_delay_ms=1000')
        writer,xid,notices=self.writer(wait=50)
        waiting,failures=self.commit(writer)
        self.finished(waiting,failures,notices,warning=True)
        code='''import sys,psycopg
sys.path.insert(0,'spikes/replay_scheduler')
from scheduler import Scheduler
with psycopg.connect(autocommit=True) as c:
    def checkpoint(gen,phase):
        if phase=='attempt_committed':
            a=c.execute("SELECT active_attempt FROM pgos_replay_probe.batches WHERE generation=%s AND state='pending'",(gen,)).fetchone()[0]
            print(str(gen)+' '+str(a),flush=True)
            sys.stdin.read()
    s=Scheduler(c,lambda gen:None,checkpoint).start()
    s.step()
'''
        child=subprocess.Popen([sys.executable,'-u','-c',code],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
        try:
            self.assertTrue(select.select([child.stdout],[],[],5)[0])
            gen,old=child.stdout.readline().strip().split()
        finally:
            child.kill();child.wait(5);child.stdin.close();child.stdout.close()
        self.until(lambda:self.conn.execute("SELECT NOT EXISTS(SELECT FROM pg_locks WHERE locktype='advisory' AND classid=1885826931 AND objid=1920363641 AND granted)").fetchone()[0])
        self.assertEqual(self.job(gen)[:4],(1,1,None,None))
        self.assertFalse(self.ready(xid))
        sched,stop,thread,errors=self.loop()
        self.until(lambda:self.ready(xid))
        new=self.conn.execute('SELECT b.active_attempt FROM pgos_replay_probe.targets t JOIN pgos_replay_probe.batches b ON b.id=t.current_batch WHERE t.generation=%s',(gen,)).fetchone()[0]
        self.assertNotEqual(str(new),old)
        self.assertEqual(self.job(gen)[:2],(2,0))
        self.assertEqual(errors,[])

    def test_postgres_restart_preserves_retry_deadlines_and_recovers_automatically(self):
        self.remote['vec'].client.wrong_marker=True
        sched,stop,thread,errors=self.loop()
        writer,xid,notices=self.writer(wait=150)
        waiting,failures=self.commit(writer)
        self.finished(waiting,failures,notices,warning=True)
        self.until(lambda:self.job(self.gen['vec']) is not None)
        stop.set();thread.join(5)
        before=self.job(self.gen['vec'])[:4]
        writer.close();self.conn.close();sched.conn.close()
        subprocess.run(['gosu','postgres','pg_ctl','-D',os.environ['PGDATA'],'-m','immediate','stop'],check=True,stdout=subprocess.DEVNULL)
        subprocess.run(['gosu','postgres','pg_ctl','-D',os.environ['PGDATA'],'-l',os.environ['PGDATA']+'/server.log','-w','start'],check=True,stdout=subprocess.DEVNULL)
        self.conn=psycopg.connect(autocommit=True)
        self.assertEqual(self.job(self.gen['vec'])[:4],before)
        self.assertFalse(self.ready(xid))
        self.assert_replay_blocked('vec')
        self.conn.execute(f'SELECT {P}.start_worker()')
        self.conn.execute(f'SELECT {P}.control(0)')
        self.remote['vec'].client.wrong_marker=False
        resumed,halt,t,failures=self.loop()
        self.until(lambda:self.ready(xid))
        self.assertEqual(self.vector(),self.expected())
        self.assertEqual(errors+failures,[])

    def test_uncommitted_and_rolled_back_writes_are_not_scheduled(self):
        sched=self.scheduler().start()
        writer,xid,notices=self.writer()
        self.assertIsNone(sched.step())
        self.assertFalse(self.ready(xid))
        writer.execute('ROLLBACK')
        self.assertIsNone(sched.step())
        self.assertEqual(self.conn.execute(f'SELECT count(*) FROM {S}.jobs').fetchone()[0],0)

    def test_binding_created_after_start_is_discovered_without_restart(self):
        self.conn.execute('SET onesearch_probe.wait_ms=10')
        sched,stop,thread,errors=self.loop()
        self.conn.execute('CREATE INDEX extra ON fixture.docs USING pgos_lifecycle(content pgos_lifecycle_probe.text_ops)')
        gen=self.conn.execute("SELECT pgos_capture_probe.register_index('fixture.extra')").fetchone()[0]
        self.assertIsNone(self.job(gen))
        self.gen['extra']=gen
        collection='pgos-live-offline-extra'
        store=fixture.Store(collection);fixture.STORES[collection]=store
        self.remote['extra']=fixture.Remote(store)
        self.conn.execute('SELECT pgos_replay_probe.bind(%s,%s,%s)',(gen,collection,fixture.OWNER))
        self.until(lambda:self.job(gen) is not None and self.job(gen)[5] is not None)
        self.assertEqual(errors,[])
        self.assertEqual(self.conn.execute('SELECT count(*) FROM pgos_capture_probe.outbox WHERE generation=%s',(gen,)).fetchone()[0],0)

    def test_retired_generation_is_skipped_without_deleting_its_work(self):
        writer,xid,notices=self.writer(wait=50)
        waiting,failures=self.commit(writer)
        self.finished(waiting,failures,notices,warning=True)
        self.conn.execute('SET onesearch_probe.wait_ms=10')
        self.conn.execute('DROP INDEX fixture.vec')
        sched,stop,thread,errors=self.loop()
        self.until(lambda:self.job(self.gen['txt']) is not None and self.job(self.gen['txt'])[5] is not None)
        self.assertIsNone(self.job(self.gen['vec']))
        self.assertFalse(self.ready(xid))
        self.assertGreater(self.conn.execute('SELECT count(*) FROM pgos_capture_probe.outbox WHERE generation=%s',(self.gen['vec'],)).fetchone()[0],0)
        self.assertEqual(errors,[])

    def test_database_errors_backoff_without_blocking_healthy_generation(self):
        holder={}
        def remote(gen):
            if gen==self.gen['vec']:
                holder['sched'].conn.execute('SELECT 1/0')
            return self.remote_for(gen)
        sched=self.scheduler(remote).start();holder['sched']=sched
        writer,xid,notices=self.writer(wait=50)
        waiting,failures=self.commit(writer)
        self.finished(waiting,failures,notices,warning=True)
        results=[sched.step(),sched.step()]
        self.assertEqual({r['outcome'] for r in results},{'retry','published'})
        self.assertEqual(self.job(self.gen['vec'])[2:4],('database','22012'))
        self.assertIsNone(sched.step())
        self.assert_replay_blocked('vec')
        self.assertEqual(self.conn.execute(completion.executor.BM25,('alpha',)).fetchone()[0],1)

    def test_stop_finishes_inflight_attempt_and_starts_no_next_generation(self):
        reached,release=threading.Event(),threading.Event()
        def checkpoint(gen,phase):
            if phase=='attempt_committed':
                reached.set()
                if not release.wait(5):
                    raise RuntimeError('test barrier timed out')
        sched,stop,thread,errors=self.loop(checkpoint=checkpoint)
        writer,xid,notices=self.writer(wait=150)
        waiting,failures=self.commit(writer)
        self.assertTrue(reached.wait(5))
        self.assertEqual(sched.conn.info.transaction_status,psycopg.pq.TransactionStatus.IDLE)
        self.assertIsNone(self.conn.execute('SELECT xact_start FROM pg_stat_activity WHERE pid=%s',(sched.conn.info.backend_pid,)).fetchone()[0])
        stop.set();release.set();thread.join(5)
        self.finished(waiting,failures,notices,warning=True)
        self.assertEqual(errors,[])
        self.assertFalse(self.ready(xid))
        self.assertEqual(self.conn.execute(f'SELECT sum(attempts) FROM {S}.jobs').fetchone()[0],1)
        resumed,halt,t,failures=self.loop()
        self.until(lambda:self.ready(xid))
        self.assertEqual(failures,[])

    def test_slow_failure_schedules_delay_after_response(self):
        writer,xid,notices=self.writer(wait=50)
        waiting,failures=self.commit(writer)
        self.finished(waiting,failures,notices,warning=True)
        class SlowFailure:
            def apply(self,*args):
                time.sleep(.15)  # Longer than the provisional 100 ms deadline.
                raise ProbeError('slow remote failure')
        sched=self.scheduler(lambda gen:SlowFailure()).start()
        gen=sched.step()['generation']
        self.assertGreater(self.job(gen)[4],.05)
        self.assertLessEqual(self.job(gen)[4],.1)

    def test_lost_pg_session_exits_and_new_scheduler_resumes(self):
        reached,release=threading.Event(),threading.Event()
        def checkpoint(gen,phase):
            if phase=='attempt_committed':
                reached.set()
                if not release.wait(5):
                    raise RuntimeError('test barrier timed out')
        sched,stop,thread,errors=self.loop(checkpoint=checkpoint)
        writer,xid,notices=self.writer(wait=200)
        waiting,failures=self.commit(writer)
        self.assertTrue(reached.wait(5))
        self.conn.execute('SELECT pg_terminate_backend(%s)',(sched.conn.info.backend_pid,))
        release.set();thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors),1)
        self.assertIsInstance(errors[0],psycopg.OperationalError)
        self.finished(waiting,failures,notices,warning=True)
        resumed,halt,t,new_errors=self.loop()
        self.until(lambda:self.ready(xid))
        self.assertEqual(new_errors,[])
        self.assertEqual(self.vector(),self.expected())

    def test_lost_publication_response_does_not_repeat_remote_work(self):
        def checkpoint(gen,phase):
            if phase=='published':
                raise ProbeError('publication response lost')
        sched,stop,thread,errors=self.loop(checkpoint=checkpoint)
        writer,xid,notices=self.writer()
        waiting,failures=self.commit(writer)
        self.finished(waiting,failures,notices)
        stop.set();thread.join(5)
        self.assertTrue(self.ready(xid))
        before=sum(len(r.client.calls) for r in self.remote.values())
        restarted=self.scheduler().start()
        self.assertIsNone(restarted.step())  # Durable publication removed work.
        self.assertEqual(sum(len(r.client.calls) for r in self.remote.values()),before)
        self.assertEqual(errors,[])
        self.assertEqual(self.vector(),self.expected())

    def test_connection_and_privilege_boundaries(self):
        with psycopg.connect() as conn:
            with self.assertRaises(ProbeError):
                Scheduler(conn,self.remote_for).start()
        self.conn.execute('CREATE ROLE scheduler_unprivileged')
        try:
            self.conn.execute('SET ROLE scheduler_unprivileged')
            for sql in (f'SELECT {S}.acquire()',f'SELECT {S}.next_generation()',f'SELECT * FROM {S}.jobs'):
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    self.conn.execute(sql)
        finally:
            self.conn.execute('RESET ROLE');self.conn.execute('DROP ROLE scheduler_unprivileged')


if __name__=='__main__':
    server=fixture.ThreadingHTTPServer(('127.0.0.1',18443),fixture.Handler)
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain('/tmp/probe.crt','/tmp/probe.key')
    server.socket=context.wrap_socket(server.socket,server_side=True)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    with psycopg.connect(autocommit=True) as admin:
        admin.execute(f'SELECT {P}.start_worker()')
    try:
        unittest.main(defaultTest='SchedulerTests',verbosity=2)
    finally:
        server.shutdown();server.server_close()
