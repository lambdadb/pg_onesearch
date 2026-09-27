"""Delete one durably admitted published attempt's refs; never delete collections.

Only the exclusive-owner, no-external-writer probe contract is supported. Names
are never reused. Abandoned attempts and their possible parents remain retained.
"""
from worker import name, require
from client import HttpFailure


def inventory(client, path, kind):
    rows = client.request('GET', path + '/' + kind).get(kind)
    require(isinstance(rows, list) and all(isinstance(x, dict) and isinstance(x.get('name'), str)
                                         for x in rows), 'Malformed ref inventory')
    require(len({x['name'] for x in rows}) == len(rows), 'Duplicate ref inventory')
    return {x['name']: x for x in rows}


def matches_snapshot(value, expected):
    return (isinstance(value, dict) and isinstance(value.get('snapshotId'), str)
            and type(value.get('snapshotCommittedAt')) is int
            and {key: value.get(key) for key in expected} == expected)


def collect(conn, generation, client, checkpoint=lambda phase: None):
    require(conn.autocommit and conn.info.transaction_status == 0, 'Collector needs an idle autocommit connection')
    batch = conn.execute('SELECT pgos_retention_probe.plan(%s)', (generation,)).fetchone()[0]
    if batch is None:
        return None
    checkpoint('planned')
    collection, owner, attempt, snapshot, committed_at = conn.execute('''
        SELECT t.collection_name,t.owner_run,b.active_attempt,b.snapshot_id,b.snapshot_committed_at
        FROM pgos_retention_probe.jobs j JOIN pgos_replay_probe.batches b ON b.id=j.batch_id
        JOIN pgos_replay_probe.targets t ON t.generation=b.generation
        WHERE j.batch_id=%s AND b.generation=%s''', (batch, generation)).fetchone()
    path = '/collections/' + collection
    ref = name(attempt)
    metadata = client.request('GET', path)
    require(metadata.get('collection', {}).get('tags', {}).get('pgos-run') == owner,
            'Cleanup collection ownership differs')
    # Validate BOTH remaining refs before deleting either one. A partial earlier
    # deletion is allowed, but a same-name ref with different identity is not.
    tags = inventory(client, path, 'tags')
    branches = inventory(client, path, 'branches')
    expected = {'snapshotId': snapshot, 'snapshotCommittedAt': committed_at}
    if ref in tags:
        require(matches_snapshot(tags[ref], expected), 'Cleanup Tag identity differs')
    if ref in branches:
        require(matches_snapshot(branches[ref].get('headSnapshot'), expected), 'Cleanup Branch identity differs')
    checkpoint('checked')
    for kind, refs in (('tags', tags), ('branches', branches)):
        if ref in refs:
            try:
                client.request('DELETE', path + '/' + kind + '/' + ref)
            except HttpFailure as exc:
                if exc.status != 404:
                    raise
        checkpoint(kind + '_deleted')
    # Acknowledgement is not completion: verify both names are absent.
    require(ref not in inventory(client, path, 'tags') and ref not in inventory(client, path, 'branches'),
            'Cleanup refs still present')
    checkpoint('absent')
    conn.execute('SELECT pgos_retention_probe.finish(%s)', (batch,))
    return str(batch)
