#!/usr/bin/env python3
"""Opt-in, bounded live REST experiment. Never part of credential-free CI."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import platform
import signal
import subprocess
import sys
import tempfile
import time
import uuid

from client import Client, HttpFailure, ProbeError, batches, encoded, load_settings, require

BRANCH = {'kind': 'branch', 'name': 'main'}
MARKER = '__pg_onesearch_barrier'
TRANSIENT = (429, 500, 502, 503, 504)
ROOT = Path(__file__).resolve().parents[2]


def utc():
    return datetime.now(timezone.utc).isoformat()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def git(*args):
    return subprocess.check_output(['git', '-C', str(ROOT), *args], text=True).strip()


def text_query(query):
    return {'queryString': {'query': query, 'defaultField': 'content', 'skipSyntax': True}}


def knn(k=10, vector=None):
    return {'knn': {'field': 'embedding', 'queryVector': vector or [1, 0, 0], 'k': k}}


def cosine(a, b):
    return 1 - sum(x*y for x, y in zip(a, b)) / math.sqrt(sum(x*x for x in a) * sum(x*x for x in b))


def bm25_reference(docs, query):
    """ASCII fixture only: Lucene-style k1=1.2, b=.75, no (k1+1) multiplier.

    This is a candidate comparison, not a claim about deployed parameters,
    deleted-document statistics, arbitrary analyzers, or shard scoring.
    """
    terms = {key: Counter(doc['content'].lower().split()) for key, doc in docs.items() if doc.get('content')}
    avg = sum(sum(tokens.values()) for tokens in terms.values()) / len(terms)
    scores = {}
    for key, tokens in terms.items():
        score = 0.0
        for term in query.lower().split():
            tf = tokens[term]
            if tf:
                df = sum(term in other for other in terms.values())
                idf = math.log(1 + (len(terms) - df + .5) / (df + .5))
                score += idf * tf / (tf + 1.2 * (.25 + .75 * sum(tokens.values()) / avg))
        if score:
            scores[key] = score
    return scores


def fixtures():
    rows = [
        ('d1', 'alpha alpha beta', [1, 0, 0]),
        ('d2', 'alpha beta beta beta', [.8, .6, 0]),
        ('d3', 'beta gamma', [0, 1, 0]),
        ('d4', 'delta epsilon', [-1, 0, 0]),
        ('d5', 'alpha alpha beta', [1, 0, 0]),
    ]
    return {key: {'id': key, 'content': content, 'embedding': vector,
                  'cohort': 'small', 'revision': 1} for key, content, vector in rows}


class Experiment:
    def __init__(self, client, report, deadline):
        self.client, self.report, self.deadline = client, report, deadline
        self.resources = []
        self.stage = 'setup'
        self.persist = lambda: None
        self.report['resources'] = self.resources

    def record(self, name, **evidence):
        self.report['checks'].append({'name': name, **evidence})
        print(f'PASS: {name}', flush=True)

    def path(self, resource, suffix=''):
        return '/collections/' + resource['name'] + suffix

    def create(self, role):
        name = f"pgos-live-{self.report['run_id'][:20]}-{role}"
        resource = {'name': name, 'role': role, 'tags': [], 'cleanup': 'pending'}
        # Refuse adoption/collision. Track only after absence has been established.
        try:
            self.client.request('GET', '/collections/' + name)
        except HttpFailure as exc:
            require(exc.status == 404, 'Collection preflight failed')
        else:
            raise ProbeError('Generated collection name already exists')
        self.resources.append(resource)
        self.persist()
        configs = {'cohort': {'type': 'keyword'}}
        if role == 'vector':
            configs['embedding'] = {'type': 'vector', 'dimensions': 3, 'similarity': 'cosine'}
        else:
            configs['content'] = {'type': 'text', 'analyzers': ['standard']}
        self.client.request('POST', '/collections', {
            'collectionName': name, 'indexConfigs': configs,
            'description': 'Temporary pg_onesearch compatibility fixture',
            'tags': {'pgos-run': self.report['run_id']},
        }, expected=201)
        resource['created'] = True
        self.record(f'{role}: create independent collection')
        return resource

    def write(self, resource, operation, rows):
        field = 'ids' if operation == 'delete' else 'docs'
        sizes = []
        # Each request is atomic per owner confirmation. Multiple requests are not.
        # Wait for each acknowledgement before sending the next request.
        for batch in batches(rows, field):
            self.client.request('POST', self.path(resource, '/docs/' + operation),
                                {field: batch, 'branch': 'main'}, expected=202)
            sizes.append(len(batch))
        self.report['writes'].append({'collection': resource['name'], 'operation': operation,
                                      'batch_sizes': sizes, 'ack_status': 202})

    def fetch(self, resource, ids, ref, consistent=False):
        body = {'ids': ids, 'includeVectors': True, 'ref': ref}
        if ref['kind'] == 'branch':
            body['consistentRead'] = consistent
        result = self.client.request('POST', self.path(resource, '/docs/fetch'), body)
        return {item['doc']['id']: item['doc'] for item in self.client.items(result)}

    def barrier(self, resource, label):
        revision = self.report['run_id'] + '-' + label
        marker = {'id': MARKER, 'cohort': 'barrier', 'barrier_revision': revision}
        started = time.monotonic()
        self.write(resource, 'upsert', [marker])
        attempts = 0
        until = time.monotonic() + self.deadline
        while True:
            attempts += 1
            try:
                got = self.fetch(resource, [MARKER], BRANCH, consistent=False)
                if got.get(MARKER) == marker:
                    break
            except HttpFailure as exc:
                if exc.status not in TRANSIENT:
                    raise
            require(time.monotonic() < until, 'Indexed marker visibility timed out')
            time.sleep(2)
        tag = 'checkpoint-' + label
        resource['tags'].append(tag)  # Also track a possibly ambiguous create response.
        self.persist()
        response = self.client.request('POST', self.path(resource, '/tags'),
                                       {'tagName': tag, 'source': BRANCH}, expected=201)
        details = response.get('tag', {})
        require(details.get('name') == tag and details.get('snapshotId'), 'Missing pinned snapshot identity')
        ref = {'kind': 'tag', 'name': tag}
        require(self.fetch(resource, [MARKER], ref).get(MARKER) == marker,
                'Tag does not include the exact indexed marker revision')
        self.record(f"{resource['role']}: indexed marker then verified Tag ({label})",
                    attempts=attempts, elapsed_seconds=round(time.monotonic()-started, 3),
                    snapshot_id=details['snapshotId'], marker_revision=revision)
        return ref

    def verify_docs(self, resource, expected, ref, universe):
        got = {}
        for offset in range(0, len(universe), 100):
            got.update(self.fetch(resource, universe[offset:offset+100], ref))
        require(set(got) == set(expected), 'Snapshot document membership differs from fixture')
        for key, doc in expected.items():
            # Service stores vectors as float32; compare numeric values with tolerance.
            actual = dict(got[key])
            if 'embedding' in doc:
                v = actual.pop('embedding', None)
                require(isinstance(v, list) and len(v) == len(doc['embedding'])
                        and all(math.isclose(a, b, abs_tol=1e-6) for a, b in zip(v, doc['embedding'])),
                        'Snapshot vector payload differs')
            require(actual == {k: v for k, v in doc.items() if k != 'embedding'},
                    'Snapshot source payload differs')

    def query(self, resource, query, ref, size=100, **kwargs):
        response = self.client.request('POST', self.path(resource, '/query'),
                                       {'query': query, 'size': size, 'ref': ref, **kwargs})
        return response, self.client.items(response)

    def scores(self, items):
        require(all(isinstance(item.get('score'), (int, float)) and math.isfinite(item['score']) for item in items),
                'Missing/nonfinite query score')
        scores = {item['doc']['id']: item['score'] for item in items}
        require(list(scores.values()) == sorted(scores.values(), reverse=True), 'Scores not descending')
        return scores

    def small_search(self, resource, expected, ref, label):
        if resource['role'] == 'vector':
            response, items = self.query(resource, knn(k=10), ref, includeVectors=True)
            require(response['isDocsInline'], 'Small vector fixture unexpectedly offloaded')
            scores = self.scores(items)
            require(set(scores) == set(expected), 'Small vector fixture candidate membership differs')
            distances = {key: cosine(doc['embedding'], [1, 0, 0]) for key, doc in expected.items()}
            remote_order = [distances[key] for key in scores]
            require(all(a <= b + 1e-6 for a, b in zip(remote_order, remote_order[1:])),
                    'Remote order differs from exact cosine order on small fixture')
            error = max(abs(scores[key] - (1 - distances[key]/2)) for key in scores)
            self.record(f'vector: bounded cosine membership/order ({label})',
                        scores=scores, local_distances=distances,
                        max_error_vs_one_minus_half_distance=error)
            return scores
        response, items = self.query(resource, text_query('alpha'), ref)
        require(response['isDocsInline'], 'Small BM25 fixture unexpectedly offloaded')
        scores = self.scores(items)
        reference = bm25_reference(expected, 'alpha')
        require(set(scores) == set(reference), 'BM25 term membership differs')
        error = max(abs(scores[key] - reference[key]) for key in scores)
        self.record(f'bm25: alpha membership and score observation ({label})', scores=scores,
                    live_corpus_reference=reference, max_error_vs_reference=error,
                    reference_matches=error < 1e-5)
        return scores

    def rejection(self, resource, body, label):
        try:
            self.client.request('POST', self.path(resource, '/query'), body)
        except HttpFailure as exc:
            require(exc.status == 400, 'Unexpected rejection status')
        else:
            raise ProbeError('Invalid query accepted')
        self.record(label, http_status=400)

    def run(self):
        initial = fixtures()
        self.report['fixture_sha256'] = digest(initial)
        for role in ('vector', 'bm25'):
            self.stage = role + '-small'
            resource = self.create(role)
            self.write(resource, 'upsert', list(initial.values()))
            before = self.barrier(resource, 'initial')
            self.verify_docs(resource, initial, before, list(initial))
            scores_before = self.small_search(resource, initial, before, 'initial')
            if role == 'bm25':
                for query, ids in [('alpha gamma', {'d1','d2','d3','d5'}),
                                   ('ALPHA', {'d1','d2','d5'}), ('!!!', set())]:
                    _, items = self.query(resource, text_query(query), before)
                    require({x['doc']['id'] for x in items} == ids, 'Plain-text analyzer match differs')
                self.record('bm25: OR, case folding, and no-token query')
                _, repeated = self.query(resource, text_query('alpha alpha'), before)
                self.record('bm25: repeated-term score observation', scores=self.scores(repeated))
            else:
                self.rejection(resource, {'query': knn(vector=[1,0]), 'ref': before},
                               'vector: mismatched query dimensions rejected')
                self.rejection(resource, {'query': knn(vector=[0,0,0]), 'ref': before},
                               'vector: zero query rejected')
            self.rejection(resource, {'query': text_query('alpha') if role == 'bm25' else knn(),
                                      'ref': before, 'consistentRead': True},
                           f'{role}: consistentRead on Tag rejected')
            # Two-row update, two-row upsert, two-row delete: never one request per data row.
            updates = [{'id':'d1', 'content':'gamma gamma', 'embedding':[0,1,0], 'revision':2},
                       {'id':'d2', 'content':'alpha alpha alpha', 'embedding':[1,0,0], 'revision':2}]
            inserts = [{'id':'d6','content':'alpha gamma','embedding':[.6,.8,0], 'cohort':'small','revision':2},
                       {'id':'d7','content':'beta gamma','embedding':[-.8,.6,0], 'cohort':'small','revision':2}]
            expected = {key: dict(value) for key, value in initial.items()}
            for row in updates:
                expected[row['id']].update(row)
            expected.update({row['id']: row for row in inserts})
            for key in ('d3','d4'):
                del expected[key]
            self.write(resource, 'update', updates)
            self.write(resource, 'upsert', inserts)
            self.write(resource, 'delete', ['d3','d4'])
            after = self.barrier(resource, 'mutated')
            universe = list(initial) + ['d6','d7']
            self.verify_docs(resource, expected, after, universe)
            self.verify_docs(resource, initial, before, universe)
            self.small_search(resource, expected, after, 'mutated')
            require(self.small_search(resource, initial, before, 'pinned-again') == scores_before,
                    'Old Tag scores changed after branch mutations')
            self.record(f'{role}: batch update/upsert/delete and immutable old Tag')
            if role == 'bm25':
                self.large_results(resource, expected, before, initial)

    def large_results(self, resource, small, old_ref, initial):
        self.stage = 'bm25-large'
        # 8.8 MB of stored, unindexed ASCII payload. Size splitting yields a few batches.
        big = [{'id':f'large-{i:03}', 'content':'payload alpha', 'cohort':'large',
                'payload':'x'*80000, 'revision':3} for i in range(110)]
        self.write(resource, 'upsert', big)
        ref = self.barrier(resource, 'large')
        response, items = self.query(resource, text_query('payload'), ref)
        require(not response['isDocsInline'], 'Large fixture did not exercise result offloading')
        require(len(items) == 100, 'Large query did not return its 100 requested results')
        require(all(item['doc'].get('payload') == 'x'*80000 for item in items), 'Downloaded payload differs')
        candidates = {item['doc']['id'] for item in items}
        self.record('bm25: offloaded query array downloaded without API key', count=len(items),
                    returned_total=response.get('total'), known_matching_corpus=110)
        self.rejection(resource, {'query': text_query('payload'), 'size':101, 'ref':ref},
                       'query: size above 100 rejected')
        # A list cursor can enumerate IDs; it is not a ranked query continuation.
        all_ids, tokens = set(), set()
        token = None
        pages = 0
        while True:
            body = {'size':37, 'ref':ref, 'fields':{'include':['id','cohort']}}
            if token:
                body['pageToken'] = token
            result = self.client.request('POST', self.path(resource, '/docs/list'), body)
            ids = {item['doc']['id'] for item in self.client.items(result)}
            require(not (ids & all_ids), 'Duplicate IDs across immutable list pages')
            all_ids |= ids
            pages += 1
            token = result.get('nextPageToken')
            if not token:
                break
            require(token not in tokens and pages < 20, 'List pagination made no progress')
            tokens.add(token)
        expected_ids = set(small) | {row['id'] for row in big} | {MARKER}
        require(all_ids == expected_ids, 'Immutable list enumeration differs from exact fixture')
        self.record('Tag list pagination enumerates fixture but is not ranked continuation',
                    pages=pages, documents=len(all_ids))
        eligible = {row['id'] for row in big} - candidates
        require(len(eligible) == 10 and not (candidates & eligible), 'Candidate rejection fixture invalid')
        self.record('coverage gap: post-filtering one top-100 page can miss eligible rows',
                    eligible_outside_page=len(eligible), returned_after_rejecting_page=0,
                    ranked_cursor_present='nextPageToken' in response)
        self.verify_docs(resource, initial, old_ref, list(initial) + ['large-000','large-109'])
        self.record('old Tag remains unchanged after large corpus growth')

    def cleanup(self):
        for resource in reversed(self.resources):
            try:
                try:
                    info = self.client.request('GET', self.path(resource))
                except HttpFailure as exc:
                    if exc.status == 404:
                        resource['cleanup'] = 'confirmed_absent'
                        continue
                    raise
                require(info.get('collection', {}).get('tags', {}).get('pgos-run') == self.report['run_id'],
                        'Ownership marker mismatch; cleanup refused')
                for tag in resource['tags']:
                    try:
                        self.client.request('DELETE', self.path(resource, '/tags/' + tag))
                    except HttpFailure as exc:
                        if exc.status != 404:
                            raise
                self.client.request('DELETE', self.path(resource))
                until = time.monotonic() + 60
                while True:
                    try:
                        self.client.request('GET', self.path(resource))
                    except HttpFailure as exc:
                        if exc.status == 404:
                            break
                        raise
                    require(time.monotonic() < until, 'Collection deletion not confirmed')
                    time.sleep(2)
                resource['cleanup'] = 'confirmed_absent'
            except ProbeError as exc:
                resource['cleanup'] = 'unconfirmed'
                resource['cleanup_error'] = str(exc)
            print(f"CLEANUP: {resource['name']}: {resource['cleanup']}", flush=True)
        self.report['resources'] = self.resources


def save_report(path, report):
    # Unique, exclusive creation prevents collisions with siblings or other runs.
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent,
                                         prefix=f'.{path.name}.', suffix='.tmp',
                                         delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(report, stream, indent=2)
            stream.write('\n')
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path, help='Explicit dotenv file; otherwise use process environment')
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--poll-timeout', type=int, default=360)
    args = parser.parse_args()
    require(10 <= args.poll_timeout <= 900, 'Poll timeout must be 10..900 seconds')
    require(not args.report.exists(), 'Refusing to overwrite an existing report')
    settings = load_settings(args.env_file)
    client = Client(settings)
    report = {'run_id':uuid.uuid4().hex, 'started_at':utc(), 'status':'running',
              'source_revision':git('rev-parse','HEAD'), 'worktree_dirty':bool(git('status','--porcelain')),
              'harness_sha256':digest({p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                      for p in sorted(Path(__file__).parent.glob('*.py'))}),
              'python':platform.python_version(), 'target_fingerprint':hashlib.sha256(client.base.encode()).hexdigest(),
              'assumptions':['Owner-confirmed per-request document atomicity',
                             'Owner-confirmed application order follows serial write acknowledgements',
                             'Single writer in fresh unpartitioned temporary collections'],
              'checks':[], 'writes':[]}
    experiment = Experiment(client, report, args.poll_timeout)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    def save():
        save_report(args.report, report)
    experiment.persist = save
    save()
    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        experiment.run()
        report['status'] = 'passed'
    except (ProbeError, KeyboardInterrupt) as exc:
        report['status'] = 'failed'
        report['failure'] = {'stage':experiment.stage, 'reason':str(exc) if isinstance(exc, ProbeError) else 'Interrupted'}
        print('FAIL: ' + report['failure']['reason'], flush=True)
    except Exception as exc:
        # Class name is safe; exception strings/tracebacks may contain sensitive URLs.
        report['status'] = 'failed'
        report['failure'] = {'stage':experiment.stage, 'reason':'Unexpected ' + type(exc).__name__}
        print('FAIL: ' + report['failure']['reason'], flush=True)
    finally:
        experiment.cleanup()
        if any(r['cleanup'] != 'confirmed_absent' for r in experiment.resources):
            report['status'] = 'failed'
        report.update(finished_at=utc(), http_calls=client.calls, result_downloads=client.downloads,
                      compressed_responses=client.compressed_responses,
                      gzip_without_header=client.gzip_without_header)
        save()
    print(f"RESULT: {report['status']}; report: {args.report}", flush=True)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except ProbeError as exc:
        print('FAIL: ' + str(exc), file=sys.stderr)
        sys.exit(1)
