"""Actual mutable-source Custom Scan and real LambdaDB Tags, no local HTTP fixture."""
import json
import os
from pathlib import Path
import sys

import psycopg
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'batch_replay'))
from worker import Remote, replay, P
from client import Client, load_settings, require, ProbeError
from run import text_query

VECTOR="""SELECT id,note,onesearch.cosine_distance(embedding,%s::onesearch.vector) AS distance
FROM docs WHERE pgos_snapshot_executor.vector_match('vec',embedding,%s::onesearch.vector)
ORDER BY distance,id"""
BM25="""SELECT id,note,pgos_snapshot_executor.score('txt',id) AS score FROM docs
WHERE pgos_snapshot_executor.match('txt',content,%s) ORDER BY score DESC,id"""


def nodes(plan):
    yield plan
    for child in plan.get('Plans',[]): yield from nodes(child)


def main():
    report={'status':'running','checks':[],'reads':[],'plans':[]}
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
                case['published']=replay(conn,case['generation'],remote)
                conn.execute('SELECT pgos_snapshot_probe.set_test_health(%s::regclass,true)',(case['index'],))
            vector=next(c for c in cases if c['role']=='vector')
            text=next(c for c in cases if c['role']=='bm25')
            conn.execute('SET plan_cache_mode=force_generic_plan')
            def sql_args(case):
                return (VECTOR,('[1,0,0]','[1,0,0]')) if case['role']=='vector' else (BM25,('alpha',))
            def search(case,connection=conn):
                sql,args=sql_args(case)
                return connection.execute(sql,args,prepare=True).fetchall()
            def verify(case,label):
                rows=search(case)
                # No other publishers exist in this owned fixture. Verify the actual
                # executor's chosen Tag in EXPLAIN ANALYZE against that publication.
                sql,args=sql_args(case)
                plan=conn.execute('EXPLAIN (FORMAT JSON, ANALYZE) '+sql,args).fetchone()[0][0]['Plan']
                scan=next(n for n in nodes(plan) if n['Node Type']=='Custom Scan')
                require(scan['Custom Plan Provider']=='OneSearchSnapshot' and scan['Remote Queries']==1,'Actual snapshot Custom Scan missing')
                require(scan['Snapshot Tag']==case['published']['tag'],'Custom Scan selected an unexpected Tag')
                if case['role']=='vector':
                    expected=conn.execute("SELECT id,note,onesearch.cosine_distance(embedding,'[1,0,0]') FROM docs WHERE embedding IS NOT NULL ORDER BY 3,1").fetchall()
                    require(rows==expected,'Vector Custom Scan differs from PG reference')
                    filtered=conn.execute(VECTOR.replace('ORDER BY','AND id<>1 ORDER BY')+' LIMIT 1',args).fetchall()
                    require(filtered==[r for r in expected if r[0]!=1][:1],'SQL filter/LIMIT differs')
                else:
                    response=remote.client.request('POST','/collections/'+case['collection']+'/query',
                        {'query':text_query('alpha'),'ref':{'kind':'tag','name':scan['Snapshot Tag']},'size':100})
                    reference={int(x['doc']['id']):x['score'] for x in remote.client.items(response)}
                    require({r[0] for r in rows}==set(reference),'BM25 same-Tag membership differs')
                    require(all(abs(r[2]-reference[r[0]])<1e-7 for r in rows),'BM25 same-Tag score differs')
                    notes=dict(conn.execute('SELECT id,note FROM docs').fetchall())
                    require(all(r[1]==notes[r[0]] for r in rows),'BM25 did not project current PG originals')
                report['reads'].append({'role':case['role'],'phase':label,'tag':scan['Snapshot Tag'],'rows':rows})
                report['plans'].append({'phase':label,'role':case['role'],'plan':plan})
                return rows
            def reject_bm25():
                try:
                    with conn.transaction(): search(text)
                except psycopg.errors.FeatureNotSupported: return
                raise ProbeError('Changed BM25 corpus was accepted without coherent statistics')
            for case in cases: verify(case,'initial')
            with conn.transaction():
                conn.execute('SAVEPOINT own')
                conn.execute("UPDATE docs SET note='own note' WHERE id=1")
                verify(text,'own-note-only')
                conn.execute("UPDATE docs SET id=-10,content='changed',embedding='[-1,0,0]' WHERE id=1")
                conn.execute('DELETE FROM docs WHERE id=2')
                conn.execute("INSERT INTO docs VALUES(4,'alpha own','[0,0,1]','own')")
                verify(vector,'own-writes')
                reject_bm25()
                with psycopg.connect(autocommit=True) as other:
                    require([r[0] for r in search(vector,other)]==[1,2],'Own writes leaked to another session')
                conn.execute('ROLLBACK TO own')
                verify(vector,'own-rollback')
            report['checks'].append('Own PK move/delete/insert, source note projection, other-session isolation and savepoint rollback')
            with conn.transaction():
                conn.execute("UPDATE docs SET content='alpha alpha',embedding='[-1,0,0]' WHERE id=1")
                conn.execute('DELETE FROM docs WHERE id=2')
                conn.execute("INSERT INTO docs VALUES(4,'beta','[0,0,1]','committed'),(5,'alpha','[0,1,0]','committed')")
            verify(vector,'committed-lag')
            reject_bm25()
            for case in cases:
                previous=case['published']['tag']
                case['published']=replay(conn,case['generation'],remote)
                require(case['published']['tag']!=previous,'Publication did not advance')
                verify(case,'republished')
            conn.execute("SELECT pgos_snapshot_probe.set_test_health('vec',false)")
            try: search(vector)
            except psycopg.errors.ObjectNotInPrerequisiteState: pass
            else: raise ProbeError('Degraded prepared index was accepted')
            verify(text,'healthy-sibling')
            require(conn.execute('SELECT count(*) FROM docs').fetchone()[0]==4,'Ordinary source read changed')
            report['checks'].append('Prepared Custom Scans refresh Tags; vector delta/filter/LIMIT and same-Tag BM25 verified; changed BM25 corpus/degraded vector rejected')
            report['status']='passed'
    except ProbeError as exc:
        report['status'],report['failure']='failed',str(exc)
    except Exception as exc:
        report['status'],report['failure']='failed','Unexpected '+type(exc).__name__
    print('PGOS_RESULT='+json.dumps(report),flush=True)
    return 0 if report['status']=='passed' else 1


if __name__=='__main__': sys.exit(main())
