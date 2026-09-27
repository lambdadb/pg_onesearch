# Vector/BM25 executor experiment

This separate PostgreSQL 18.6 module connects actual `Custom Scan
(OneSearchProbe)` plans to the existing C read transport. It returns original PG
heap rows, projects exact local cosine distance, and binds remote BM25 scores to
one scan execution. It is not an index access method or an accelerated index;
the product installation bundle remains unchanged.

## Confirmed requirements and experimental choices

Both vector and BM25 remain required for the first E2E milestone. PG originals,
snapshot visibility, own writes, and per-index failure handling remain product
requirements. This experiment proves only a bounded executor mechanism.

Custom Scan is the chosen mechanism for this probe. An IAM, opclasses, automatic
vector `ORDER BY` selection, durable index identity, and index lifecycle are
still pending. An explicit probe predicate replaces the relation's paths with
the custom path. Costs 10/20 are placeholders, not a measured cost model.

The fixture contains five immutable rows with IDs 1–5. The harness creates a
separate LambdaDB collection and verified immutable Tag for each modality.
`size=100` and vector `k=100` cover this fixture only. The implementation rejects
unexpected IDs and incomplete vector coverage. It does not infer general ranked
exhaustion or implement continuation.

## Execution and score ownership

- The first tuple request issues one remote query. Plain `EXPLAIN` and `LIMIT 0`
  issue none. `EXPLAIN ANALYZE` reports mode, scope, remote queries, and rescans.
- Local rows are scanned using `estate->es_snapshot`; `ExecScan` applies local
  quals and projection. This deliberately scans the five-row heap, not TIDs
  obtained from a production remote index.
- BM25 hits and scores belong to `CustomScanState`. A temporary projection frame
  is restored on success or ERROR. Exact function-expression identity, table
  identity, and current row key prevent nested SQL from borrowing the outer
  scan's score. There is no global row-ID score cache or `fn_extra` score cache.
- Each rescan clears results and reevaluates the query parameter. Prepared
  statements create fresh execution state; collection/Tag mappings are read at
  execution time. A correlated scalar subplan tests changing `PARAM_EXEC` values.
- A superuser-only health GUC is checked at begin, execution, score projection,
  and rescan. It tests guard placement; it is not durable health or publication.

The trusted setup creates the exact fixture schema and a statement trigger that
blocks INSERT, UPDATE, DELETE, and TRUNCATE. Execution takes a ShareLock. The
module is superuser-only and rejects RLS. Superuser DDL, trigger bypass, and
arbitrary mapping changes are outside this experiment. Reading a PG snapshot
over this frozen fixture does **not** establish mutable-source snapshot/Tag
alignment, strict reads, or own-write support.

## Probe SQL, not product SQL

```sql
SELECT id,
       embedding OPERATOR(onesearch.<=>) '[1,0,0]'::onesearch.vector AS distance
FROM pgos_executor_probe.documents
WHERE pgos_executor_probe.vector_match(embedding, '[1,0,0]'::onesearch.vector)
ORDER BY distance, id;

SELECT id,
       pgos_executor_probe.score('pgos_executor_probe.documents'::regclass, id) AS score
FROM pgos_executor_probe.documents
WHERE pgos_executor_probe.match(content, 'alpha')
ORDER BY score DESC, id;
```

Vector distance is calculated from PG originals. The offline server deliberately
returns an unrelated remote score to verify this. BM25 uses the remote score;
the probe's `regclass` is a **table**, whereas the [product SQL proposal](sql-client-contract.md)
requires an index identity. These functions are installed only by the probe.

The bounded path accepts one search context and one relation per query level,
with a constant or parameter query. A VALUES-driven correlated scalar subplan
is also exercised. Joins, multiple contexts, aggregates, windows, target SRFs,
CTEs, set operations, DISTINCT, row locks, sampling, lateral paths, RLS, and
backward execution are outside the supported scope. Unsupported marker
evaluation fails rather than pretending to provide local BM25. Unsupported
shapes report `0A000`; unmatched score context and injected unhealthy state
report `55000`. Parallel execution is disabled.

## Reproduce and inspect

```sh
./scripts/test-executor.sh
python3 spikes/executor/run_live.py \
  --env-file /absolute/path/to/.env.local \
  --report artifacts/executor-live.json
```

The first command runs the 25-case offline TLS transport suite, builds the
separate executor module, and runs 11 executor tests without external network.
CI reuses the transport image built by its preceding step. The probe alone
requires preload in its disposable cluster; the product extension does not.

The executor tests check real plans/call counts, local vector projections,
scan-bound scores, generic prepared statements, Tag changes, correlated
rescans, health failure during rescan, NULL/empty queries, local filters,
HTTP-error cleanup, nested SQL isolation, and unsupported query shapes.

The opt-in live harness hashes both executor and transport source against the
built image before connecting. It creates two owned collections, writes each
five-document batch atomically, waits for the final indexed marker, creates and
verifies Tags, and removes its collections with absence confirmation. Credentials
are passed on stdin and retained only in the disposable postmaster environment;
they are not emitted in SQL, image metadata, or reports. BM25 is compared with
the Python reference against the same Tag; vector results are compared with
ordinary PG cosine queries. Evidence includes actual plans and scan call counts.

## Observed run — 2026-09-27

The [live evidence](evidence/executor-2026-09-27.json) records a passed run at
10:44:28–10:46:40 UTC from clean commit `ed6e6a9`, with both modules' file hashes
verified against the pinned image. Three prepared vector executions matched all
five PG distances/IDs. Four prepared BM25 executions (alpha, beta, gamma, alpha)
matched same-Tag Python IDs/scores. Correlated rescans returned each term's top
score; their actual plan recorded three remote queries and three rescans.
Individual vector/BM25 plans each recorded one remote query. A cached plan
failed with `55000` under injected unhealthy state and recovered after reset.

Each collection received its five documents in one request, followed by a
separate final-marker request. Marker-to-verified-Tag waits were 68.874 and
56.703 seconds; these are fixture observations, not an SLO. Both collections
and their Tags were removed and collection absence was confirmed. No backend
deployment revision is pinned by this evidence.

## Remaining gates

The next product decisions are IAM/index identity and lifecycle; PK/TID/version
mapping under HOT, VACUUM and reuse; durable outbox and ordered replay; snapshot
and own-write handling; BM25 corpus/overlay statistics; health publication and
recovery races; ranked continuation; and production credential/security policy.
This fixture does not close those gates or establish performance suitability.

PostgreSQL references: [Custom Scan](https://www.postgresql.org/docs/18/custom-scan.html),
[execution callbacks](https://www.postgresql.org/docs/18/custom-scan-execution.html),
and [PG18.6 executor source](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeCustom.c).
