"""Live fixture settings via stdin, outside Docker config and SQL literals."""
import json
import os
import sys
payload = json.load(sys.stdin)
os.environ.update(payload['settings'])
os.environ['PGOS_LIVE_CASES'] = json.dumps(payload['cases'])
for case in payload['cases']:
    prefix = 'PGOS_EXECUTOR_' + case['role'].upper()
    os.environ[prefix + '_COLLECTION'] = case['collection']
    os.environ[prefix + '_TAG'] = case['tag']
os.execv('/bin/sh', ['sh', 'spikes/executor/run.sh', 'live'])
