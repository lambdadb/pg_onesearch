"""Synchronous test-only replay adapter; each failed attempt is abandoned, never reused."""
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'live_compatibility'))
from client import MAX_REQUEST, ProbeError, encoded, require

P = 'pgos_replay_probe'
MARKER = '__pg_onesearch_barrier'  # Disjoint from signed bigint source IDs.


def name(attempt):
    return 'attempt-' + str(attempt).replace('-', '')


def chunks(rows, field, branch, limit=MAX_REQUEST):
    """Count the actual serialized body including the attempt branch name."""
    batch = []
    size = len(encoded({field: [], 'branch': branch}))
    for row in rows:
        addition = len(encoded(row)) + bool(batch)
        if size + addition > limit and batch:
            yield batch
            batch, size = [], len(encoded({field: [], 'branch': branch}))
            addition = len(encoded(row))
        require(size + addition <= limit, 'One replay document exceeds request budget')
        batch.append(row)
        size += addition
    if batch:
        yield batch


class Remote:
    def __init__(self, client, deadline=360):
        self.client, self.deadline = client, deadline
        self.writes = []
        self.barriers = []

    def fetch(self, collection, ids, ref):
        body = {'ids': ids, 'includeVectors': True, 'ref': ref}
        if ref['kind'] == 'branch':
            body['consistentRead'] = False
        result = self.client.request('POST', '/collections/' + collection + '/docs/fetch', body)
        return {item['doc']['id']: item['doc'] for item in self.client.items(result)}

    def apply(self, target, attempt, base, deletes, upserts):
        collection, owner = target
        path = '/collections/' + collection
        metadata = self.client.request('GET', path)
        require(metadata.get('collection', {}).get('tags', {}).get('pgos-run') == owner,
                'Collection ownership differs')
        branch = name(attempt)
        source = {'kind': 'branch', 'name': name(base['attempt']) if base else 'main'}
        if base:
            source['asOf'] = base['committed_at']
        result = self.client.request('POST', path + '/branches',
                                     {'branchName': branch, 'source': source}, expected=201)
        details = result.get('branch', {})
        require(details.get('name') == branch, 'Branch identity differs')
        expected = {'snapshotId': base['snapshot'], 'snapshotCommittedAt': base['committed_at']} if base else None
        require('parentSnapshot' in details and details['parentSnapshot'] == expected,
                'Branch parent snapshot differs from published base')
        require('headSnapshot' in details and details['headSnapshot'] == expected,
                'Branch head differs from published base')
        for operation, field, rows in [('delete', 'ids', deletes), ('upsert', 'docs', upserts)]:
            for batch in chunks(rows, field, branch):
                self.client.request('POST', path + '/docs/' + operation,
                                    {field: batch, 'branch': branch}, expected=202)
                self.writes.append({'attempt': str(attempt), 'operation': operation, 'size': len(batch)})
        marker = {'id': MARKER, 'cohort': 'barrier', 'barrier_revision': str(attempt)}
        self.client.request('POST', path + '/docs/upsert', {'docs': [marker], 'branch': branch}, expected=202)
        ref = {'kind': 'branch', 'name': branch}
        start = time.monotonic()
        polls = 0
        while True:
            polls += 1
            if self.fetch(collection, [MARKER], ref).get(MARKER) == marker:
                break
            require(time.monotonic() - start < self.deadline, 'Indexed replay marker timed out')
            time.sleep(2)
        tag = self.client.request('POST', path + '/tags',
                                  {'tagName': branch, 'source': ref}, expected=201).get('tag', {})
        require(tag.get('name') == branch and isinstance(tag.get('snapshotId'), str)
                and bool(tag['snapshotId']) and type(tag.get('snapshotCommittedAt')) is int,
                'Tag has no pinned snapshot identity')
        require(self.fetch(collection, [MARKER], {'kind': 'tag', 'name': branch}).get(MARKER) == marker,
                'Tag does not contain the exact attempt marker')
        self.barriers.append({'attempt': str(attempt), 'parent_snapshot': expected,
                              'snapshot_id': tag['snapshotId'], 'polls': polls,
                              'elapsed_seconds': round(time.monotonic()-start, 3)})
        return tag['snapshotId'], tag['snapshotCommittedAt']


def replay(conn, generation, remote, checkpoint=lambda phase: None):
    require(conn.autocommit and conn.info.transaction_status == 0, 'Worker needs an idle autocommit connection')
    batch = conn.execute(f'SELECT {P}.claim(%s)', (generation,)).fetchone()[0]
    if batch is None:
        return None
    checkpoint('claimed')
    attempt = conn.execute(f'SELECT {P}.begin_attempt(%s)', (batch,)).fetchone()[0]
    checkpoint('attempt_committed')
    target = conn.execute(f'SELECT collection_name,owner_run FROM {P}.targets WHERE generation=%s', (generation,)).fetchone()
    parent = conn.execute(f'''SELECT p.active_attempt,p.snapshot_id,p.snapshot_committed_at
        FROM {P}.batches b JOIN {P}.batches p ON p.id=b.parent_batch WHERE b.id=%s''', (batch,)).fetchone()
    base = dict(zip(('attempt','snapshot','committed_at'),parent)) if parent else None
    # In this bounded schema, same-key writes serialize through the immediate PK/row locks.
    # Across disjoint keys sequence order is immaterial; IDs are never a commit cursor.
    rows = conn.execute(f'''SELECT DISTINCT ON(row_key) operation,row_key,document
        FROM {P}.events WHERE batch_id=%s AND row_key IS NOT NULL ORDER BY row_key,event_id DESC''', (batch,)).fetchall()
    deletes = [str(key) for operation,key,doc in rows if operation == 'delete']
    upserts = [doc for operation,key,doc in rows if operation == 'upsert']
    snapshot, committed_at = remote.apply(target, attempt, base, deletes, upserts)
    checkpoint('tag_verified')
    conn.execute(f'SELECT {P}.publish(%s,%s,%s)', (attempt,snapshot,committed_at))
    checkpoint('published')
    return {'batch': str(batch), 'attempt': str(attempt), 'tag': name(attempt), 'snapshot': snapshot}
