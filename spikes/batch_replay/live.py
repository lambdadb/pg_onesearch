"""Inside the disposable PG container: source DML -> capture -> replay -> verified Tag."""
import json
import os
import sys

import psycopg
from worker import Remote, replay, P, MARKER
from client import Client, load_settings, require, ProbeError


def main():
    report = {'status': 'running', 'checks': [], 'publications': []}
    remote = Remote(Client(load_settings(None)))
    report['writes'], report['barriers'] = remote.writes, remote.barriers
    try:
        with psycopg.connect(autocommit=True) as conn:
            conn.execute('CREATE TABLE docs(id bigint PRIMARY KEY,content text,embedding onesearch.vector(3))')
            conn.execute("INSERT INTO docs VALUES(1,'alpha','[1,0,0]'),(2,'beta','[0,1,0]'),(3,NULL,NULL)")
            conn.execute('CREATE INDEX txt ON docs USING pgos_lifecycle(content pgos_lifecycle_probe.text_ops)')
            conn.execute('CREATE INDEX vec ON docs USING pgos_lifecycle(embedding pgos_lifecycle_probe.vector_ops)')
            cases = json.loads(os.environ['PGOS_REPLAY_CASES'])
            for case in cases:
                idx = 'vec' if case['role']=='vector' else 'txt'
                case['generation'] = conn.execute('SELECT pgos_capture_probe.register_index(%s::regclass)',(idx,)).fetchone()[0]
                conn.execute(f'SELECT {P}.bind(%s,%s,%s)',(case['generation'],case['collection'],os.environ['PGOS_REPLAY_OWNER']))
            def expected(case):
                field = 'embedding' if case['role']=='vector' else 'content'
                mode = 'vector' if case['role']=='vector' else 'text'
                return {doc['id']:doc for (doc,) in conn.execute(f'''SELECT pgos_capture_probe.project(%s,id,to_jsonb({field}))
                    FROM docs WHERE {field} IS NOT NULL ORDER BY id''',(mode,)).fetchall()}
            def verify(case, publication, docs):
                got = remote.fetch(case['collection'], ['1','2','3','4','5','10'], {'kind':'tag','name':publication['tag']})
                require(got==docs, 'Published Tag differs from controlled PG projection')
                require(conn.execute('SELECT count(*) FROM pgos_capture_probe.outbox WHERE generation=%s',
                                     (case['generation'],)).fetchone()[0]==0, 'Covered outbox remains after publication')
                row = conn.execute(f'''SELECT b.id,b.snapshot_id,(SELECT count(*) FROM {P}.events e WHERE e.batch_id=b.id)
                    FROM {P}.targets t JOIN {P}.batches b ON b.id=t.current_batch WHERE t.generation=%s''',
                                   (case['generation'],)).fetchone()
                require(str(row[0])==publication['batch'] and row[1]==publication['snapshot'], 'PG publication differs')
                report['publications'].append({'role':case['role'], **publication, 'covered_events':row[2], 'document_ids':sorted(docs)})
            for case in cases:
                case['initial_docs'] = expected(case)
                case['initial'] = replay(conn,case['generation'],remote)
                verify(case,case['initial'],case['initial_docs'])
            with conn.transaction():
                conn.execute("UPDATE docs SET content='temporary' WHERE id=1")
                conn.execute("UPDATE docs SET id=10,content='alpha changed',embedding='[0,0,1]' WHERE id=1")
                conn.execute('DELETE FROM docs WHERE id=2')
                conn.execute("INSERT INTO docs VALUES(4,'beta new','[1,0,0]'),(5,'gamma new','[0,1,0]')")
            for case in cases:
                result = replay(conn,case['generation'],remote)
                verify(case,result,expected(case))
                old = remote.fetch(case['collection'],['1','2','3','4','5','10'],{'kind':'tag','name':case['initial']['tag']})
                require(old==case['initial_docs'], 'Old Tag changed after delta replay')
                require(replay(conn,case['generation'],remote) is None, 'Empty generation creates another batch')
                report['checks'].append(case['role']+': initial and delta Tag match PG; old Tag immutable; exact outbox coverage')
            require([w['size'] for w in remote.writes]==[2,2,2,3,2,3], 'Data requests were not expected grouped batches')
            report['status'] = 'passed'
    except ProbeError as exc:
        report['status'], report['failure'] = 'failed', str(exc)
    except Exception as exc:
        report['status'], report['failure'] = 'failed', 'Unexpected '+type(exc).__name__
    print('PGOS_RESULT='+json.dumps(report),flush=True)
    return 0 if report['status']=='passed' else 1


if __name__=='__main__':
    sys.exit(main())
