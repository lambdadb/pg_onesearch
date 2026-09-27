"""Container-side SQL calls. Remote fixture lifecycle is owned by the host runner."""
import json
import hashlib
import math
import os
import sys

import psycopg
from psycopg.types.json import Jsonb


def main():
    checks = []
    with psycopg.connect(autocommit=True) as conn:
        conn.execute('SET pgos_remote_probe.timeout_ms = 45000')
        for case in json.loads(os.environ['PGOS_LIVE_CASES']):
            # Same prepared SQL, different collection/query parameters, repeated execution.
            # This exercises the function only, not an IAM/CustomScan rescan contract.
            for attempt in range(2):
                result = conn.execute('SELECT pgos_remote_probe.query(%s,%s,%s,%s)',
                                      (case['collection'], case['tag'], Jsonb(case['query']), case['size']),
                                      prepare=True).fetchone()[0]
                if (not result['isDocsInline'] or 'docsUrl' in result or
                        result['wasOffloaded'] != case['offloaded']):
                    raise ValueError('Result hydration mismatch')
                docs = result['docs']
                scores = {item['doc']['id']: item['score'] for item in docs}
                if len(scores) != len(docs) or set(scores) != set(case['scores']):
                    raise ValueError('Fixture membership mismatch')
                if any(not math.isclose(score, case['scores'][key], rel_tol=1e-6, abs_tol=1e-7)
                       for key, score in scores.items()):
                    raise ValueError('Fixture score mismatch')
                if 'payload_sha256' in case:
                    for item in docs:
                        payload = item['doc'].get('payload', '')
                        if (len(payload) != case['payload_length'] or
                                hashlib.sha256(payload.encode()).hexdigest() != case['payload_sha256']):
                            raise ValueError('Downloaded payload mismatch')
                checks.append({'role': case['role'], 'execution': attempt + 1,
                               'documents': len(docs), 'scores': scores,
                               'was_offloaded': result['wasOffloaded'],
                               'payload_bytes_verified': case.get('payload_length', 0) * len(docs)})
    print('PGOS_RESULT=' + json.dumps({'status': 'passed', 'checks': checks}), flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # Do not print exception text/tracebacks or query/connection values.
        print('PGOS_RESULT=' + json.dumps({'status': 'failed', 'error_type': type(exc).__name__,
                                         'sqlstate': getattr(exc, 'sqlstate', None)}), flush=True)
        sys.exit(1)
