"""Actual Custom Scan plans and PG-source comparisons against immutable live Tags."""
import json
import math
import os
import sys
import psycopg
from queries import TABLE, BM25, VECTOR, RESCAN, nodes


def require(condition):
    if not condition:
        raise ValueError('Executor fixture comparison failed')


def main():
    checks, plans = [], {}
    cases = {case['role']: case for case in json.loads(os.environ['PGOS_LIVE_CASES'])}
    with psycopg.connect(autocommit=True) as conn:
        conn.execute('SET pgos_remote_probe.timeout_ms=45000')
        conn.execute('SET plan_cache_mode=force_generic_plan')
        for query in ('[1,0,0]', '[0,1,0]', '[1,0,0]'):
            rows = conn.execute(VECTOR, (query, query), prepare=True).fetchall()
            expected = conn.execute(f'SELECT id,embedding OPERATOR(onesearch.<=>) %s::onesearch.vector AS d FROM {TABLE} ORDER BY d,id', (query,)).fetchall()
            require(rows == expected and len(rows) == 5)
            checks.append({'mode': 'vector', 'query': query, 'rows': rows})
        for term in ('alpha', 'beta', 'gamma', 'alpha'):
            rows = conn.execute(BM25, (term,), prepare=True).fetchall()
            reference = cases['bm25']['references'][term]
            expected_ids = sorted(reference, key=lambda key: (-reference[key], int(key)))
            require([str(key) for key, _ in rows] == expected_ids)
            require(all(math.isclose(score, reference[str(key)], rel_tol=1e-6, abs_tol=1e-7) for key, score in rows))
            checks.append({'mode': 'bm25', 'query': term, 'rows': rows})
        rescanned = conn.execute(RESCAN).fetchall()
        for term, score in rescanned:
            require(math.isclose(score, max(cases['bm25']['references'][term].values()), rel_tol=1e-6, abs_tol=1e-7))
        checks.append({'mode': 'correlated-rescan', 'rows': rescanned})
        for label, sql, args, calls in (
                ('vector', VECTOR, ('[1,0,0]', '[1,0,0]'), 1),
                ('bm25', BM25, ('alpha',), 1), ('rescan', RESCAN, None, 3)):
            plan = conn.execute('EXPLAIN (ANALYZE,FORMAT JSON) ' + sql, args).fetchone()[0][0]['Plan']
            custom = next(n for n in nodes(plan) if n['Node Type'] == 'Custom Scan')
            require(custom['Remote Queries'] == calls)
            plans[label] = plan
        conn.execute('SET pgos_executor_probe.healthy=off')
        try:
            conn.execute(BM25, ('alpha',), prepare=True)
        except psycopg.errors.ObjectNotInPrerequisiteState:
            checks.append({'mode': 'cached-plan-health', 'sqlstate': '55000'})
        else:
            raise ValueError('Health guard failed')
        conn.execute('SET pgos_executor_probe.healthy=on')
        require(len(conn.execute(BM25, ('alpha',), prepare=True).fetchall()) == 3)
    print('PGOS_RESULT=' + json.dumps({'status': 'passed', 'checks': checks, 'plans': plans}), flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print('PGOS_RESULT=' + json.dumps({'status': 'failed', 'error_type': type(exc).__name__,
                                         'sqlstate': getattr(exc, 'sqlstate', None)}), flush=True)
        sys.exit(1)
