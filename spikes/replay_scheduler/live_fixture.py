"""Exercise the entire live scenario with local API/TLS fixtures, no credentials."""
from contextlib import redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import ssl
import sys
import threading
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'snapshot_executor'))
import offline as executor
fixture=executor.fixture
path=Path(__file__).with_name('live.py')
spec=importlib.util.spec_from_file_location('scheduler_live_harness',path)
live=importlib.util.module_from_spec(spec)
spec.loader.exec_module(live)


class LocalClient:
    def __init__(self,settings): pass
    def items(self,result): return result['docs']
    def request(self,method,path,body=None,expected=200):
        collection=path.split('/')[2]
        store=fixture.STORES[collection]
        if path.endswith('/query'):
            docs=store.tags[body['ref']['name']]
            term=body['query']['queryString']['query'].lower()
            hits=[{'doc':doc,'score':float(doc['content'].lower().split().count(term))}
                  for doc in docs.values() if term in doc.get('content','').lower().split()]
            return {'docs':hits}
        return store.request(method,path,body,expected)


if __name__=='__main__':
    cases=[{'role':role,'collection':'pgos-live-scheduler-'+role} for role in ('vector','bm25')]
    for case in cases:
        fixture.STORES[case['collection']]=fixture.Store(case['collection'])
    os.environ['PGOS_REPLAY_CASES']=json.dumps(cases)
    os.environ['PGOS_REPLAY_OWNER']=fixture.OWNER
    server=fixture.ThreadingHTTPServer(('127.0.0.1',18443),fixture.Handler)
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain('/tmp/probe.crt','/tmp/probe.key')
    server.socket=context.wrap_socket(server.socket,server_side=True)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    output=io.StringIO()
    try:
        with patch.object(live,'Client',LocalClient),redirect_stdout(output):
            code=live.main()
        report=json.loads(next(line.removeprefix('PGOS_RESULT=') for line in output.getvalue().splitlines() if line.startswith('PGOS_RESULT=')))
        assert code==0,report
        assert report['status']=='passed'
        assert report['commits'][0]['notices']==[],report['commits']
        assert report['commits'][1]['notices']==['01000']
        assert len(report['faults'])==1
        assert len(report['reads'])==6 and len(report['plans'])==6
        assert all(x['required_events']==x['published_events']==2 for x in report['coverage'])
        assert any(x['size']==2 for x in report['writes'])
        print('PASS: complete live scheduler scenario using offline API/TLS fixtures')
    finally:
        server.shutdown();server.server_close()
