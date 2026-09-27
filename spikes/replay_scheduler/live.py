"""Automatic PG -> live LambdaDB publication -> SQL search, with redacted evidence."""
import json
import os
from pathlib import Path
import sys
import threading
import time

import psycopg
from scheduler import Scheduler
from worker import Remote, P, ProbeError, require
from client import Client, load_settings
from run import text_query

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'snapshot_executor'))
from live import VECTOR, BM25, nodes

C = 'pgos_completion_probe'


class LostAckClient:
    """Drop one real successful upsert response locally, after the service ACK."""
    def __init__(self, client, collection, report):
        self.client, self.collection, self.report = client, collection, report
        self.injected = False

    def request(self, method, path, body=None, expected=200):
        response = self.client.request(method, path, body, expected)
        if not self.injected and method == 'POST' and path == '/collections/'+self.collection+'/docs/upsert':
            self.injected = True
            self.report.append({'kind':'locally_discarded_live_upsert_ack','expected_status':expected})
            raise ProbeError('Injected lost live upsert acknowledgement')
        return response

    def items(self, response):
        return self.client.items(response)


class Loop:
    def __init__(self, remote, cases, report):
        self.remote, self.cases, self.report = remote, cases, report
        self.stop = threading.Event()
        self.started = threading.Event()
        self.errors = []
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        try:
            with psycopg.connect(autocommit=True) as conn:
                def checkpoint(gen, phase):
                    role = next(c['role'] for c in self.cases if c['generation']==gen)
                    item = {'role':role, 'phase':phase, 'at_seconds':round(time.monotonic()-self.report['clock_start'],3)}
                    if phase == 'attempt_committed':
                        item['attempt'] = str(conn.execute(f"SELECT active_attempt FROM {P}.batches WHERE generation=%s AND state='pending'",(gen,)).fetchone()[0])
                    self.report['events'].append(item)
                    print('PGOS_PROGRESS='+json.dumps(item),flush=True)
                scheduler=Scheduler(conn,lambda gen:self.remote,checkpoint)
                # start() is performed inside run(); expose launch, not ownership.
                self.started.set()
                scheduler.run(self.stop)
        except Exception as exc:
            self.errors.append(type(exc).__name__)
            self.started.set()

    def start(self):
        self.thread.start()
        require(self.started.wait(5),'Scheduler launch timed out')
        return self

    def check(self):
        require(not self.errors and self.thread.is_alive(),'Live scheduler stopped unexpectedly')

    def close(self):
        self.stop.set()
        self.thread.join(5)
        require(not self.thread.is_alive(),'Live scheduler stop unconfirmed')
        require(not self.errors,'Live scheduler failed')


def main():
    report={'status':'running','checks':[],'events':[],'commits':[],'reads':[],
            'plans':[],'writes':[],'barriers':[],'faults':[],'clock_start':time.monotonic()}
    loops=[]
    settings=load_settings(None)
    reader=Client(settings)
    stage='setup'
    try:
        with psycopg.connect(autocommit=True) as conn:
            conn.execute(f'SELECT {C}.start_worker()')
            conn.execute(f'SELECT {C}.control(0)')
            conn.execute('SET onesearch_probe.wait_ms=10')  # Bootstrap before a target is bound.
            conn.execute('CREATE TABLE docs(id bigint PRIMARY KEY,content text,embedding onesearch.vector(3),note text)')
            conn.execute("INSERT INTO docs VALUES(1,'alpha','[1,0,0]','a'),(2,'beta','[0,1,0]','b'),(3,NULL,NULL,NULL)")
            cases=json.loads(os.environ['PGOS_REPLAY_CASES'])
            for case in cases:
                case['index']='vec' if case['role']=='vector' else 'txt'
                column,opclass=('embedding','vector_ops') if case['role']=='vector' else ('content','text_ops')
                conn.execute(f"CREATE INDEX {case['index']} ON docs USING pgos_lifecycle({column} pgos_lifecycle_probe.{opclass})")
                case['generation']=conn.execute('SELECT pgos_capture_probe.register_index(%s::regclass)',(case['index'],)).fetchone()[0]
                conn.execute(f'SELECT {P}.bind(%s,%s,%s)',(case['generation'],case['collection'],os.environ['PGOS_REPLAY_OWNER']))
                conn.execute('SELECT pgos_snapshot_probe.set_test_health(%s::regclass,true)',(case['index'],))
            conn.execute('SET plan_cache_mode=force_generic_plan')

            def launch(client):
                remote=Remote(client)
                report['remotes'].append(remote)
                loop=Loop(remote,cases,report).start();loops.append(loop)
                return loop

            def pending():
                return conn.execute(f'SELECT count(*) FROM pgos_capture_probe.outbox').fetchone()[0]

            def published(xid):
                return conn.execute(f'SELECT {C}.is_published(%s::xid8)',(xid,)).fetchone()[0]

            def await_ready(loop, label, xid=None, start=None):
                started=time.monotonic() if start is None else start
                until=time.monotonic()+900
                def ready():
                    return published(xid) if xid else pending()==0
                while not ready():
                    loop.check()
                    require(time.monotonic()<until,'Automatic live publication timed out')
                    time.sleep(.2)
                elapsed=round(time.monotonic()-started,3)
                report['checks'].append({'name':label,'publication_seconds':elapsed})
                print('PGOS_PROGRESS='+json.dumps({'phase':label,'publication_seconds':elapsed}),flush=True)
                return elapsed

            def search(case, connection=conn):
                return connection.execute(VECTOR if case['role']=='vector' else BM25,
                    ('[1,0,0]','[1,0,0]') if case['role']=='vector' else ('alpha',),prepare=True).fetchall()

            def verify(label):
                for case in cases:
                    rows=search(case)
                    sql,args=(VECTOR,('[1,0,0]','[1,0,0]')) if case['role']=='vector' else (BM25,('alpha',))
                    plan=conn.execute('EXPLAIN (FORMAT JSON, ANALYZE) '+sql,args).fetchone()[0][0]['Plan']
                    scan=next(n for n in nodes(plan) if n['Node Type']=='Custom Scan')
                    require(scan['Custom Plan Provider']=='OneSearchSnapshot' and scan['Remote Queries']==1,'Expected live Custom Scan missing')
                    batch,attempt=conn.execute(f'SELECT b.id,b.active_attempt FROM {P}.targets t JOIN {P}.batches b ON b.id=t.current_batch WHERE t.generation=%s',(case['generation'],)).fetchone()
                    tag='attempt-'+str(attempt).replace('-','')
                    require(scan['Snapshot Tag']==tag,'SQL chose an unexpected published Tag')
                    if case['role']=='vector':
                        expected=conn.execute("SELECT id,note,onesearch.cosine_distance(embedding,'[1,0,0]') FROM docs WHERE embedding IS NOT NULL ORDER BY 3,1").fetchall()
                        require(rows==expected,'Live SQL vector results differ from PG reference')
                    else:
                        response=reader.request('POST','/collections/'+case['collection']+'/query',
                            {'query':text_query('alpha'),'ref':{'kind':'tag','name':tag},'size':100})
                        expected={int(x['doc']['id']):x['score'] for x in reader.items(response)}
                        require(set(expected)=={r[0] for r in rows},'Live SQL BM25 membership differs from the same Tag')
                        require(all(abs(r[2]-expected[r[0]])<1e-7 for r in rows),'Live SQL BM25 scores differ from the same Tag')
                    report['reads'].append({'phase':label,'role':case['role'],'tag':tag,'batch':str(batch),'rows':rows})
                    report['plans'].append({'phase':label,'role':case['role'],'plan':plan})

            def commit(label, sql, wait_ms):
                with psycopg.connect(autocommit=True) as writer:
                    notices=[]
                    writer.add_notice_handler(lambda d:notices.append(d.sqlstate))
                    writer.execute(f'SET onesearch_probe.wait_ms={wait_ms}')
                    writer.execute('BEGIN')
                    writer.execute(sql)
                    xid=writer.execute('SELECT pg_current_xact_id()::text').fetchone()[0]
                    started=time.monotonic()
                    writer.execute('COMMIT')
                    elapsed=round(time.monotonic()-started,3)
                    outcome=writer.execute(f'SELECT {C}.last_result()').fetchone()[0]
                require(notices in ([],['01000']),'Unexpected live commit notices')
                ready=published(xid)
                if not notices:
                    require(ready and 'outcome=published' in outcome,'Normal commit response lacks durable publication')
                report['commits'].append({'phase':label,'writer_xid':xid,'wait_ms':wait_ms,
                    'response_seconds':elapsed,'notices':notices,'callback':outcome,
                    'published_at_status_check_after_response':ready})
                print('PGOS_PROGRESS='+json.dumps(report['commits'][-1]),flush=True)
                return xid,started,notices

            report['remotes']=[]
            stage='initial_publication'
            loop=launch(Client(settings))
            await_ready(loop,'automatic-bootstrap')
            verify('initial')
            stage='own_writes'
            with conn.transaction():
                conn.execute('SAVEPOINT own')
                conn.execute("UPDATE docs SET content='changed corpus',embedding='[-1,0,0]' WHERE id=1")
                vector=next(c for c in cases if c['role']=='vector')
                require(search(vector)==conn.execute("SELECT id,note,onesearch.cosine_distance(embedding,'[1,0,0]') FROM docs WHERE embedding IS NOT NULL ORDER BY 3,1").fetchall(),'Own vector overlay differs')
                try:
                    with conn.transaction(): search(next(c for c in cases if c['role']=='bm25'))
                except psycopg.errors.FeatureNotSupported:
                    pass
                else:
                    raise ProbeError('Changed own-write BM25 corpus was accepted')
                conn.execute('ROLLBACK TO own')
            report['checks'].append({'name':'own vector overlay and changed-corpus BM25 restriction'})

            stage='automatic_commit'
            xid,started,notices=commit('automatic',"UPDATE docs SET content=content||' alpha',embedding='[0,0,1]',note='automatic' WHERE id IN (1,2)",10000)
            await_ready(loop,'automatic-commit-publication',xid,started)
            verify('automatic')
            require(conn.execute('SELECT count(*) FROM docs WHERE note=%s',('automatic',)).fetchone()[0]==2,'Source mutation missing after commit')
            loop.close()

            stage='paused_timeout'
            xid,started,notices=commit('paused',"UPDATE docs SET content='alpha recovered',embedding='[-1,0,0]',note='recovered' WHERE id IN (1,2)",250)
            require(notices==['01000'] and not published(xid),'Paused scheduler did not preserve a pending committed writer')
            for case in cases:
                try: search(case)
                except psycopg.errors.ObjectNotInPrerequisiteState: pass
                else: raise ProbeError('Pending live publication did not block SQL search')
            require(conn.execute("SELECT count(*) FROM docs WHERE note='recovered'").fetchone()[0]==2,'Timeout lost source rows')
            require(pending()==4,'Timeout did not retain both indexed events for both rows')
            report['checks'].append({'name':'paused scheduler warning preserves source/outbox and blocks both indexes'})

            stage='lost_ack_recovery'
            vector=next(c for c in cases if c['role']=='vector')
            fault=LostAckClient(Client(settings),vector['collection'],report['faults'])
            resumed=launch(fault)
            await_ready(resumed,'restart-and-lost-ack-recovery',xid,started)
            require(fault.injected,'Live acknowledgement fault was not exercised')
            attempts=conn.execute(f'SELECT count(DISTINCT a.id) FROM {P}.attempts a JOIN {P}.batches b ON b.id=a.batch_id JOIN {P}.events e ON e.batch_id=b.id WHERE b.generation=%s AND e.writer_xid=%s::xid8',(vector['generation'],xid)).fetchone()[0]
            require(attempts>=2,'Lost live ACK did not create a fresh replay attempt')
            report['checks'].append({'name':'fresh fenced attempt after locally discarded live ACK','attempts':attempts})
            verify('recovered')
            resumed.close()
            report['coverage']= [{'generation':str(g),'required_events':n,'published_events':done}
                for g,n,done in conn.execute(f'SELECT * FROM {C}.status(%s::xid8)',(xid,))]
            require(all(x['required_events']==x['published_events'] for x in report['coverage']),'Recovery coverage incomplete')
            report['status']='passed'
    except ProbeError as exc:
        report['status'],report['failure']='failed',str(exc)
    except Exception as exc:
        report['status'],report['failure']='failed','Unexpected '+type(exc).__name__
    finally:
        for loop in loops:
            try: loop.close()
            except ProbeError:
                report['status'],report['worker_stop']='failed','unconfirmed'
        for remote in report.pop('remotes',[]):
            report['writes'].extend(remote.writes)
            report['barriers'].extend(remote.barriers)
        report['last_stage']=stage
        report['elapsed_seconds']=round(time.monotonic()-report.pop('clock_start'),3)
    print('PGOS_RESULT='+json.dumps(report),flush=True)
    return 0 if report['status']=='passed' else 1


if __name__=='__main__': sys.exit(main())
