"""Real captured source, verified Tags and SQL C/libcurl snapshot reads; no mock transport."""
import json
import os
from pathlib import Path
import sys

import psycopg
from psycopg.types.json import Jsonb
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'batch_replay'))
from worker import Remote, replay, P
from client import Client, load_settings, require, ProbeError
from run import text_query


def main():
    report={'status':'running','checks':[],'reads':[]}
    remote=Remote(Client(load_settings(None)))
    report['writes'],report['barriers']=remote.writes,remote.barriers
    try:
        with psycopg.connect(autocommit=True) as conn:
            conn.execute('CREATE TABLE docs(id bigint PRIMARY KEY,content text,embedding onesearch.vector(3),note text)')
            conn.execute("INSERT INTO docs VALUES(1,'alpha','[1,0,0]','a'),(2,'beta','[0,1,0]','b'),(3,NULL,NULL,NULL)")
            conn.execute('CREATE INDEX txt ON docs USING pgos_lifecycle(content pgos_lifecycle_probe.text_ops)')
            conn.execute('CREATE INDEX vec ON docs USING pgos_lifecycle(embedding pgos_lifecycle_probe.vector_ops)')
            cases=json.loads(os.environ['PGOS_REPLAY_CASES'])
            for case in cases:
                case['index']='vec' if case['role']=='vector' else 'txt'
                case['generation']=conn.execute('SELECT pgos_capture_probe.register_index(%s::regclass)',(case['index'],)).fetchone()[0]
                conn.execute(f'SELECT {P}.bind(%s,%s,%s)',(case['generation'],case['collection'],os.environ['PGOS_REPLAY_OWNER']))
                case['initial']=replay(conn,case['generation'],remote)
                conn.execute('SELECT pgos_snapshot_probe.set_test_health(%s::regclass,true)',(case['index'],))
            vector=next(c for c in cases if c['role']=='vector')
            text=next(c for c in cases if c['role']=='bm25')
            conn.execute('SET plan_cache_mode=force_generic_plan')
            def search(case):
                query=[1,0,0] if case['role']=='vector' else 'alpha'
                return conn.execute('SELECT pgos_snapshot_probe.search(%s::regclass,%s)',
                                    (case['index'],Jsonb(query)),prepare=True).fetchone()[0]
            def verify(case,label):
                result=search(case)
                if case['role']=='vector':
                    expected=conn.execute("SELECT id,onesearch.cosine_distance(embedding,'[1,0,0]') FROM docs WHERE embedding IS NOT NULL ORDER BY 2,1").fetchall()
                    require([(int(r['id']),r['distance']) for r in result['results']]==expected,'Vector snapshot differs from PG reference')
                else:
                    response=remote.client.request('POST','/collections/'+case['collection']+'/query',
                        {'query':text_query('alpha'),'ref':{'kind':'tag','name':result['tag']},'size':100})
                    reference={x['doc']['id']:x['score'] for x in remote.client.items(response)}
                    require({r['id'] for r in result['results']}==set(reference),'BM25 same-Tag membership differs')
                    require(all(abs(r['score']-reference[r['id']])<1e-7 for r in result['results']),'BM25 same-Tag score differs')
                report['reads'].append({'role':case['role'],'phase':label,**result})
                return result
            def reject_bm25():
                try:
                    with conn.transaction(): search(text)
                except psycopg.errors.FeatureNotSupported:
                    return
                raise ProbeError('Changed BM25 corpus was accepted without coherent statistics')
            for case in cases: verify(case,'initial')
            with conn.transaction():
                conn.execute('SAVEPOINT own')
                conn.execute("UPDATE docs SET id=10,content='changed',embedding='[-1,0,0]' WHERE id=1")
                conn.execute('DELETE FROM docs WHERE id=2')
                conn.execute("INSERT INTO docs VALUES(4,'alpha own','[0,0,1]','own')")
                own=verify(vector,'own-writes')
                require(own['tag']==vector['initial']['tag'],'Own writes switched the published base')
                reject_bm25()
                with psycopg.connect(autocommit=True) as other:
                    visible=other.execute("SELECT pgos_snapshot_probe.search('vec','[1,0,0]')").fetchone()[0]
                    require([r['id'] for r in visible['results']]==['1','2'],'Own writes leaked to another session')
                conn.execute('ROLLBACK TO own')
                verify(vector,'own-rollback')
            report['checks'].append('Own PK move/delete/insert included locally, isolated from other sessions; savepoint rollback restores base')
            with conn.transaction():
                conn.execute("UPDATE docs SET content='alpha alpha',embedding='[-1,0,0]' WHERE id=1")
                conn.execute('DELETE FROM docs WHERE id=2')
                conn.execute("INSERT INTO docs VALUES(4,'beta','[0,0,1]','committed'),(5,'alpha','[0,1,0]','committed')")
            lag=verify(vector,'committed-lag')
            require(lag['tag']==vector['initial']['tag'],'Lag query switched the published base')
            reject_bm25()
            report['checks'].append('Committed vector delta returned against old Tag; changed BM25 corpus rejected explicitly')
            for case in cases:
                case['published']=replay(conn,case['generation'],remote)
                latest=verify(case,'republished')
                require(latest['tag']==case['published']['tag'] and latest['tag']!=case['initial']['tag'],
                        'Prepared query did not refresh publication')
            conn.execute("SELECT pgos_snapshot_probe.set_test_health('vec',false)")
            try: search(vector)
            except psycopg.errors.ObjectNotInPrerequisiteState: pass
            else: raise ProbeError('Degraded prepared index was accepted')
            verify(text,'healthy-sibling')
            require(conn.execute('SELECT count(*) FROM docs').fetchone()[0]==4,'Ordinary source read changed')
            report['checks'].append('Prepared reads refresh Tags; degraded index fails while sibling BM25 and ordinary PG reads remain usable')
            report['status']='passed'
    except ProbeError as exc:
        report['status'],report['failure']='failed',str(exc)
    except Exception as exc:
        report['status'],report['failure']='failed','Unexpected '+type(exc).__name__
    print('PGOS_RESULT='+json.dumps(report),flush=True)
    return 0 if report['status']=='passed' else 1


if __name__=='__main__':
    sys.exit(main())
