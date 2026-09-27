# Snapshot-aware Custom Scan experiment

This test-only C module connects the bounded [snapshot reader](snapshot-read-spike.md)
to actual PostgreSQL `Custom Scan` execution. It is excluded from the product
extension and evaluation bundle. It supersedes the frozen five-row assumption
for this experiment without changing the earlier isolated executor probe.

## Implemented boundary

Explicit match calls name a registered lifecycle index and its source column.
The planner checks index-to-heap identity, mode, indexed attribute, bigint key
attribute and query argument shape from physical IAM metadata. It binds BM25
score expressions to that index and the same source alias/key. Index relation
invalidation participates in prepared-plan dependencies; runtime metadata and
snapshot generation checks still run on each execution. An index is not chosen
automatically and lifecycle IAM scans remain disabled.

```sql
SELECT id, note,
       onesearch.cosine_distance(embedding, '[1,0,0]') AS distance
FROM documents
WHERE pgos_snapshot_executor.vector_match(
        'documents_embedding_idx', embedding, '[1,0,0]')
  AND id <> 1
ORDER BY distance, id LIMIT 10;

SELECT id, note,
       pgos_snapshot_executor.score('documents_content_idx', id) AS score
FROM documents
WHERE pgos_snapshot_executor.match('documents_content_idx', content, 'alpha')
ORDER BY score DESC, id LIMIT 10;
```

These require separate probe setup, captured/replayed indexes, and injected
healthy state. They are not product SQL APIs. Superuser, READ COMMITTED,
ordinary permanent non-RLS tables, immediate bigint PKs, prior-command own writes,
64 heap rows/base documents, snapshot history/delta budgets and exclusive remote
fixture ownership remain prerequisites. Joins, aggregates, windows, row locking,
multiple markers, OR/nested markers, CTEs and backward/parallel scans are rejected.
A correlated scalar subquery with a single marker may rescan for each parameter.
Full SQL/portal/cursor support and same-command source mutations are not claimed.

## Snapshot, tuple and score scope

The executor passes `estate->es_snapshot` explicitly to read-only SPI. The snapshot
reader captures the Tag, retained audit, visible delta and source rows using that
snapshot. The Custom Scan then scans the actual heap under the **same** snapshot,
returns typed heap slots, and leaves SQL qualification, projection, sorting and
LIMIT to PostgreSQL. AccessShare relation locks allow ordinary DML and VACUUM.

Candidate keys are full signed bigint values, not positions in a fixed fixture.
The result's snapshot-local TID must equal the actual visible slot's TID for that
key. It is a comparison, never a remote physical lookup or a cached TID fetch.
Prepared execution and rescan reconstruct candidates; no TID survives a scan
snapshot. HOT-style updates, delete/reinsert and VACUUM are exercised, but actual
physical TID reuse is not asserted. Scalable row-version lookup remains open.

The vector reader requires complete bounded base coverage, reconciles all visible
delta and computes exact cosine from PG originals. The executor returns every
eligible candidate before local filters/LIMIT; it does not claim ANN acceleration
or a production cost model. BM25 uses remote scores only for an identical indexed
corpus. Note-only source changes are allowed; indexed changes fail with `0A000`
before HTTP. See the separate [BM25 overlay proposal](bm25-overlay-contract.md).

BM25 score state lives in one scan instance. A dynamically scoped projection
frame checks the exact planned score expression, index and current key. Nested
SQL cannot borrow it. Error unwinding restores the prior frame. Fresh operational
health is checked at begin, fetch, rescan, score access and before/after each
`ExecScan` call; the reader also checks before/after HTTP. A failure during a row
projection is detected before that scan call returns. This does not asynchronously
revoke rows already returned or stored by parent nodes such as Sort/Materialize.
Production failure detection/recovery and stronger parent-node revocation remain
unimplemented; the health table is still an explicit fault-injection control.

`EXPLAIN` performs no HTTP; `EXPLAIN ANALYZE` reports actual Custom Scan provider,
mode, successful remote queries, rescans and the last selected Snapshot Tag.
NULL queries and LIMIT 0 do not send HTTP; empty text and invalid nonempty
queries reject. Generic prepared executions refresh query values, delta, publication
and health; REINDEX without recapture/republication fails rather than using old data.

## Validation

```sh
./scripts/test-snapshot-executor.sh
# After building/testing the matching snapshot reader image:
./scripts/test-snapshot-executor.sh --reuse-snapshot-image
```

Twelve actual PostgreSQL/C/libcurl tests use the shared deterministic TLS Tag
fixture. They cover actual plans and typed columns, local filtering and LIMIT,
own PK changes across signed bigint extremes, insert/delete/NULL/savepoint
rollback, another session, generic plan refresh, BM25 boundaries, parameterized
rescans, publication/VACUUM under an old statement, degradation during HTTP and
projection, source tuple changes, malformed/incomplete remote base, score/alias/
index/column binding, nonstandard attribute order, bounds, RLS and isolation.
The existing 13 snapshot tests also pass with the exported shared health guard.
Fixture BM25 scores are synthetic; those checks are not BM25 quality evidence.

Opt-in real LambdaDB test:

```sh
python3 spikes/snapshot_executor/run_live.py \
  --env-file /absolute/path/to/.env.local \
  --report /absolute/path/to/new-report.json
```

It reuses the snapshot harness's source/image hash verification, stdin credentials,
owned resource creation and confirmed-container-absence cleanup gate. Only the
selected module/image/bootstrap change. Nine cleanup regression tests, including
the executor variant, check that remote deletion requires confirmed worker absence. It exercises typed Custom Scan reads,
actual EXPLAIN ANALYZE Tag selection, PG exact-vector references, same-Tag Python
BM25 references, own writes/note-only projection, rollback, committed lag,
republishing and prepared health rejection. Live concurrency/error injection is
not implied by the separate local fixture tests. No server revision is pinned.

Remaining gates: approved/server-implemented coherent BM25 overlay, scalable
source lookup and ranked continuation, product IAM/planner/API integration,
operational recovery/retention, commit-wait, security, isolation and scale.

### Live result, 2026-09-27

The [sanitized evidence](evidence/snapshot-executor-2026-09-27.json) records a passed
run at clean implementation `0c84b607be3da2a4ecfe6eefd0781dae4524b2d0`, from
13:05:11–13:09:43 UTC, with source hashes matched to the pinned image. The
[full CI run](https://github.com/lambdadb/pg_onesearch/actions/runs/36321200098)
also passed for that implementation, including all existing probe/package checks.

Nine recorded typed reads comprise five vector/PG reference comparisons and four
BM25/same-Tag REST comparisons. Each phase also records a real EXPLAIN ANALYZE
Custom Scan and its selected Tag. Note-only own writes returned the current note
with unchanged BM25 scores. Own PK move/delete/insert produced vector IDs `4,-10`,
while another session retained `1,2`; savepoint rollback restored `1,2`.
Committed unpublished changes returned `4,5,1` against the old Tag, and the prepared
query returned the same values against the next published Tag. Changed-corpus BM25
was rejected before publication, then returned IDs `1,5` after replay. Injected
vector degradation rejected prepared execution while BM25 and ordinary PG remained
usable. Additional vector SQL filter/LIMIT comparisons passed in each vector phase.

Four indexed-marker barriers took 106.758, 46.419, 50.387 and 62.590 seconds;
these are fixture observations, not an SLO. Data RPCs grouped operations into
2/2/1/3/1/3 documents or IDs, followed by separate marker writes. The worker
container was confirmed absent before remote cleanup. All four Tags and four
non-main Branches were deleted, and both owned collections were confirmed absent.
The evidence contains synthetic fixture data and no environment connection values.
