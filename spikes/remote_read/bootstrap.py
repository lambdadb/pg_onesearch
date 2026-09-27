"""Read live settings from stdin, never Docker arguments/config or SQL literals."""
import json
import os
import sys

payload = json.load(sys.stdin)
os.environ.update(payload['settings'])
os.environ['PGOS_LIVE_CASES'] = json.dumps(payload['cases'])
os.execv('/bin/sh', ['sh', 'spikes/remote_read/run.sh', 'live'])
