# Statement snapshot and own-write read experiment

This isolated C/SQL probe connects the capture/replay state to the C/libcurl
remote reader. It exposes JSON results through explicit SQL functions. It does
not install a product API, activate lifecycle IAM scans, or replace the earlier
Custom Scan experiment. The standalone extension and evaluation bundle remain
unchanged.

## Confirmed requirements and experiment choices

The [canonical design](https://github.com/lambdadb/sbrain/blob/628fdf77aaf36a4544c2083d11579525c95c9931/projects/lambdadb-postgresql-extension.md)
requires a fixed Tag plus a complete snapshot-visible delta, eligible own writes,
exclusion of other uncommitted/aborted versions, PG originals, and an operational
health guard independent of the reader's historical Tag mapping. Healthy bounded
publication lag may use the complete delta. A degraded index must fail rather
than silently return stale results or fall back to a different BM25 implementation.

Selected bounds: superuser, ordinary permanent source table with an immediate
bigint primary key, READ COMMITTED, separate SELECTs after source statements,
64 physical source rows and 64 base documents, at most 64 publication ancestors /
4,096 historical events / 1,000 visible delta events, and a 4 MiB materialized view.
These are experiment admission limits, not product limits or hard peak-memory
bounds. Generation/source validation and RLS/isolation rejection remain active.
The replay worker is still Python; this reader and HTTP transport run inside the
PG backend in C, with read-only SQL assembly and reconciliation.

## One snapshot for data, fresh observation for health

`pgos_snapshot_probe.view(index)` and `search(index, query_json)` register the
caller's active statement snapshot. C calls `SPI_execute_snapshot` with that
explicit snapshot and `read_only=true`. The `assemble` helper is STABLE, so its
publication, audit, outbox and dynamic source-heap SELECTs use the same read view.
No global latest-Tag cache or separate application query fetches these pieces.
The snapshot remains registered throughout remote HTTP and result materialization.

The data view contains:

- The registered source epoch and physical index generation.
- The snapshot-visible target's published batch and immutable Tag.
- Base documents reconstructed from that batch's parent chain. Newer **batches**
  win before event order within a batch; event IDs are not a global commit cursor.
- The last visible outbox operation per key, including the session's prior commands.
  Own-transaction attribution uses `writer_xid` and `pg_current_xact_id_if_assigned`;
  visibility itself comes from PG MVCC, not that comparison.
- Full source rows read under the same snapshot. The indexed projection reconstructed
  from base + delta must exactly match the heap, or the call fails with `55000`.

The source heap and IAM metadata relation locks prevent physical replacement
during a read but allow ordinary DML and VACUUM. Published payload audit records
and remote Tags are retained indefinitely by the earlier probe; this experiment
adds no remote GC. The concurrent test pauses a statement before assembling its
view, publishes and deletes its outbox in another session, changes the source
again, and VACUUMs source/outbox/target. The old statement still reads its old Tag,
its now-deleted delta and its old heap version; the next statement selects the new
Tag and remaining delta. This tests READ COMMITTED statement lifetime, not support
for REPEATABLE READ, exported snapshots, persistent cursors or remote continuation.

Operational health uses a separate logged table and `GetLatestSnapshot()` for
checks before capture and before returning materialized results. A missing or
false flag rejects the index with `55000`, including prepared calls and an old
statement snapshot. A failure committed during HTTP also rejects the response.
The guarantee ends at the last check; this is not asynchronous revocation of
already-returned rows. `set_test_health` is explicitly a fault-injection control.
Automatic failure detection, restart reconstruction, generation-fenced recovery,
and write/recovery races are **not** implemented. A manual healthy flag is not
production evidence of recovery.

The choice follows [PG function snapshot rules](https://www.postgresql.org/docs/18/xfunc-volatility.html)
and the explicit-snapshot/read-only branches in [PG18.6 SPI source](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/spi.c).
The concurrency tests provide executable evidence for this specific implementation.

## Identity and complete bounded vector reads

The reader never retains a remote TID or dereferences a cached `ctid`. It looks up
base membership by the selected Tag plus generation and key, verifies every
remote returned document against its captured base projection, and constructs the
current result from snapshot-visible source rows and the complete delta.

Each result includes `version = generation:event_id`, identifying the latest
visible captured change for that logical document. This is an immutable capture
revision, **not** a physical tuple-version locator or a completed IAM mapping.
`tid` is diagnostic, taken only from the current snapshot heap scan. Tests cover
HOT-style note updates, delete/reinsert of the same primary key, and VACUUM; they
do not establish production TID reuse safety through an index lookup. Actual TID
reuse is not required by those tests, and no reuse observation is claimed.

Vector search requests `k=100,size=100` against a base independently bounded to
64 documents. Every base ID and exact captured payload must appear once; missing,
duplicate, unknown or mismatched revisions fail. With full base coverage proven,
visible delta deletes/upserts replace it and exact cosine is computed from PG
originals. The remote score is deliberately unrelated in the offline fixture.
The result is sorted by distance and bigint key. It scans the bounded source and
retained history on every call: no acceleration or ANN recall claim is made.
Larger corpora still require a continuation/exhaustion protocol and scalable
source-row resolution. Exact payload comparison is deliberately conservative;
broader float serialization interoperability remains to be defined.

## BM25 support boundary

A selected Tag's BM25 scores are returned only when the complete effective indexed
document map equals the base document map. Updates to unrelated source columns
can therefore return current PG originals without changing the scoring corpus.
Any effective indexed insert/update/delete, whether committed lag or own writes,
is rejected with `0A000` **before HTTP**. Replaying/publishing the changes makes
same-corpus BM25 reads available again. Query strings use the existing standard
analyzer / `skipSyntax=true` path and reject empty text.

This does not satisfy the required first-E2E BM25 own-write contract yet. Local
matching, rescoring only returned base hits, or merging separately scored delta
hits cannot prove one coherent corpus or candidate coverage. The next BM25 gate
is a concrete request-scoped overlay/scoring contract: pinned base plus explicit
replacements/deletes, common corpus/term statistics, and adequate candidate
coverage/continuation. No new LambdaDB endpoint or approved server implementation
is assumed here.

## Reproduce and review

```sh
./scripts/test-snapshot-read.sh
# If the C transport image is already built/tested:
./scripts/test-snapshot-read.sh --reuse-transport-image
```

Thirteen tests use actual PG snapshots, capture/replay tables, C/libcurl and a
local TLS server with deterministic immutable Tag fixtures. They cover both base
query modes, own PK move/delete/insert/NULL/savepoint rollback, other transactions,
committed lag, publication/VACUUM concurrency, fresh health under old snapshots and
HTTP, prepared executions, BM25 rejection, late smaller IDs, update/reinsert
identity, missing/wrong/duplicate remote documents, capture corruption, bounds,
REINDEX, isolation, permissions and RLS. Local BM25 fixture scores are synthetic
for transport testing, not an independent scoring implementation.

```sql
SELECT pgos_snapshot_probe.view('documents_embedding_idx'::regclass);
SELECT pgos_snapshot_probe.search('documents_embedding_idx'::regclass, '[1,0,0]'::jsonb);
SELECT pgos_snapshot_probe.search('documents_content_idx'::regclass, '"alpha"'::jsonb);
```

These require the separate probe setup, registered/replayed indexes and injected
healthy state. Results are materialized JSON, not the proposed typed product SQL.
There are no new planner hooks or automatic index paths, and no claim of arbitrary
SQL shape, RLS/tenant security, read-only transactions, pipeline or own writes made
inside the same SQL command. The supported fixture uses ordinary SELECTs in
READ COMMITTED after earlier source commands.

Opt-in live harness:

```sh
python3 spikes/snapshot_read/run_live.py \
  --env-file /absolute/path/to/.env.local \
  --report /absolute/path/to/new-report.json
```

It hashes source against the pinned image, passes credentials on stdin, creates
fresh owned vector/BM25 collections, drives real replay, and exercises SQL reads
before/after own writes, rollback, committed lag, and republishing. C BM25 results
are compared to Python REST results from the exact same Tag; vector results are
compared to local PG cosine. It also checks prepared health failure and a healthy
sibling. Version/collection cleanup is ownership-checked, with final absence
confirmation. Live evidence is recorded separately after execution.

Remaining product gates: coherent BM25 overlay, integration into the actual
Custom Scan/typed result path, source row-version lookup without a full scan,
ranked continuation, production operational health/recovery, remote Tag retention,
commit-wait integration, and broader isolation/security/scale acceptance.
