TABLE = 'pgos_executor_probe.documents'
SCORE = "pgos_executor_probe.score('pgos_executor_probe.documents'::regclass,id)"
BM25 = f'SELECT id,{SCORE} AS score FROM {TABLE} WHERE pgos_executor_probe.match(content,%s) ORDER BY score DESC,id'
VECTOR = f'''SELECT id,embedding OPERATOR(onesearch.<=>) %s::onesearch.vector AS distance
FROM {TABLE} WHERE pgos_executor_probe.vector_match(embedding,%s::onesearch.vector)
ORDER BY distance,id'''
RESCAN = f'''SELECT v.term, (SELECT {SCORE} FROM {TABLE}
WHERE pgos_executor_probe.match(content,v.term) ORDER BY {SCORE} DESC,id LIMIT 1)
FROM (VALUES ('alpha'),('beta'),('gamma')) v(term)'''


def nodes(plan):
    yield plan
    for child in plan.get('Plans', []):
        yield from nodes(child)
