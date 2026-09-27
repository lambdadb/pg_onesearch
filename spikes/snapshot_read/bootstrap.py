"""Credentials arrive on stdin, never in Docker config, SQL, or command arguments."""
import json
import os
import sys
payload = json.load(sys.stdin)
os.environ.update(payload['settings'])
os.environ['PGOS_REPLAY_CASES'] = json.dumps(payload['cases'])
os.environ['PGOS_REPLAY_OWNER'] = payload['owner']
os.execv('/bin/sh', ['sh', 'spikes/snapshot_read/run.sh', 'live'])
