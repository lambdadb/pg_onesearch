# SQL and client contract proposal — 2026-09-26

## Status and authority

This is a review proposal for the first vector + BM25 end-to-end milestone,
not an implemented remote API or a supported-client announcement. The
[reviewed design](https://github.com/lambdadb/sbrain/blob/628fdf77aaf36a4544c2083d11579525c95c9931/projects/lambdadb-postgresql-extension.md)
remains the requirements authority. [Development decisions](development.md)
describe the installed local API. [Client evidence](client-contract-validation.md)
records what actual clients did against the separate mock-publication probe.

| Status | Scope |
| --- | --- |
| Confirmed requirements | Both vector and BM25 in the first E2E; original values in PG; independent remote resources per index; atomic source/outbox commit; healthy completion waits for ready Tag + PG publication; failure after commit warns and preserves replay; affected-index searches fail until recovery; PG snapshots and own writes remain authoritative. |
| Implemented product API | `onesearch.vector(n)`, text/binary I/O, typmod validation, exact local `onesearch.cosine_distance` / `OPERATOR(onesearch.<=>)`. No remote access method, BM25 functions, outbox, or worker. |
| Selected experiment targets | C/PGXS, PG 18.6, Debian 12 arm64; psql 18.6 and synchronous Psycopg 3.3.6 using system libpq. Actual-driver tests use a separate test-only worker and local receipts. |
| Proposed below | SQL names, bounded first query shapes, notice handling, execution-context binding, error states, and initial client support scope. These are engineering proposals subject to review and executable proof. |
| Open gates | Production hook safety, deployed remote capabilities, BM25 corpus/overlay scoring, candidate continuation, executor integration, operational status API, and security/lifecycle semantics. |

## Client completion boundary

Proposed initial scope: ordinary synchronous psql and Psycopg calls, one
statement per autocommit operation or an explicit outer transaction. A successful
statement inside an explicit transaction is not a successful commit. Neither
COPY completion, a nested transaction/savepoint exit, nor extended-protocol
CommandComplete alone establishes commit/publication.

For the tested Psycopg default connection, `execute()` returns with `INTRANS`;
`commit()` is the outer completion boundary. For an autocommit connection,
ordinary synchronous `execute()` includes protocol synchronization. An outer
`conn.transaction()` context entered from IDLE commits on normal exit; a
context entered inside an existing transaction manages a savepoint. In all cases, preceding SQL errors must be handled: after an error
leaves the transaction `INERROR`, `commit()` can return `None` while PG rolls
back. A driver return value or `ReadyForQuery(IDLE)` alone is therefore insufficient.
See the [Psycopg transaction model](https://www.psycopg.org/psycopg3/docs/basic/transactions.html).

The intended healthy path is: PG source + replay commit, verified remote ready
Tag, atomic PG publication mapping, then successful outer completion. The
probe only substitutes a committed local receipt for those remote steps. Its
AFTER_LOCKS wait is a feasibility candidate, not an adopted production hook.

After source commit, synchronization timeout/failure or cancellation during
the wait returns success plus SQLSTATE `01000` WARNING in the tested probe;
source/outbox remain committed. A later rollback cannot undo them. Before
commit, an ordinary cancellation or deferred constraint failure can still abort
and roll back. Client disconnection cannot establish which outcome occurred.
Applications must not blindly retry a mutation after an ambiguous response.
Durable operation identity and a status/reconciliation API remain design gates.

Psycopg delivers the warning through `add_notice_handler()`, not an exception;
the handler must copy diagnostic fields while they are valid. psql prints it
on stderr and exits 0 even with `ON_ERROR_STOP=1`. Ignoring notices can therefore
look like ordinary success while publication is incomplete. The product must
enforce an affected-index health guard in execution, including prepared/cached
plans and rescans; correctness cannot depend on applications noticing warnings.
Notice suppression by client/server settings also requires a status channel.
See [Psycopg server messages](https://www.psycopg.org/psycopg3/docs/advanced/async.html#server-messages).

Pipeline mode is proposed to remain outside the first supported client contract.
Its `execute()` may return before synchronization; the test observes the eventual
warning at `pipeline.sync()`. Raw Execute+Flush can likewise produce
CommandComplete before commit. This PR does not install a product hook or reject
these modes. Before enabling synchronization in the product, define and enforce
its protocol scope or prove equivalent semantics. Async clients, pools/proxies,
ORMs, other drivers, multi-statement autocommit batches, and two-phase commit
need separate acceptance cases. See [pipeline semantics](https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html)
and the [raw-protocol probe](commit-worker-spike.md).

## Proposed first SQL surface

The examples in this section are **not executable with the current extension**.
Start with an ordinary table with a non-null bigint primary key, nullable text,
and a fixed-dimensional vector. Partitioning, RLS/security barriers, joins,
self-joins, parallel plans, and more than one remote search context per SELECT
are outside this first executor experiment. Unsupported shapes must fail
explicitly until their semantics are implemented and tested; they must not
silently select a different snapshot or score universe.

```sql
CREATE TABLE documents (
    id bigint PRIMARY KEY,
    content text,
    embedding onesearch.vector(3)
);

-- Proposed access method, opclasses, and analyzer option; not registered yet.
CREATE INDEX documents_embedding_idx ON documents
    USING onesearch (embedding onesearch.vector_cosine_ops);
CREATE INDEX documents_content_idx ON documents
    USING onesearch (content onesearch.bm25_ops) WITH (analyzer = 'standard');
```

Each index gets its own remote collection/index generation. Use blocking builds
first; build/rebuild/drop, PK changes, restore fencing, and credentials/configuration
need explicit lifecycle contracts before remote DDL is enabled. SQL examples
intentionally do not specify an unreviewed credential storage mechanism.

### Vector

```sql
SELECT id, embedding OPERATOR(onesearch.<=>) $1::onesearch.vector(3) AS distance
FROM documents
WHERE embedding IS NOT NULL
ORDER BY distance, id
LIMIT 10;
```

Keep the already implemented cosine definition and ascending order. Remote ANN
selects candidates; projected SQL distances are recomputed from snapshot-visible
PG vectors using the local operator, so a remote relevance score is not assumed
to equal cosine distance. Exact local sequential evaluation stays available for
ordinary SQL without a remote plan; a selected degraded remote plan must error.
ANN candidate coverage is approximate and needs recall validation. A secondary
key orders ties within the candidate set; it does not prove globally deterministic
candidate membership or exact top-k completeness.

Vectors remain 2–4096 finite float32 elements. Proposed cosine indexes exclude
NULL values and reject zero vectors at build or mutation time before commit.
The base type continues to permit storing zero vectors, with cosine evaluation
raising `22000`; index validation must not change that standalone storage rule.
Nonzero query vectors must match index dimensions. No managed embedding or
query-text-to-vector API is proposed in this first scope.

### BM25

Proposed signatures:

```text
onesearch.match(index regclass, query text) -> onesearch.bm25_query
text OPERATOR(onesearch.@@@) onesearch.bm25_query -> boolean
onesearch.score(index regclass, row_key bigint) -> double precision
```

```sql
SELECT d.id, d.content,
       onesearch.score('documents_content_idx'::regclass, d.id) AS score
FROM documents AS d
WHERE d.content OPERATOR(onesearch.@@@)
      onesearch.match('documents_content_idx'::regclass, $1)
ORDER BY score DESC, d.id
LIMIT 10;
```

Match determines eligibility; score is a separate descending relevance value.
Explicit index identity avoids an ambiguous global `score(id)` map. The executor
must also bind the index, table alias, query value, snapshot, and scan instance
to one execution-local context. Index + row key alone is not sufficient for
multiple queries or aliases: this first proposal permits only one such context
per SELECT. A score without an eligible matching context must error, never
return a fabricated zero. Rescans and prepared executions must refresh or reset
that context according to PG snapshot rules, including own writes and health.

An IAM plus an executor/CustomScan integration is a candidate implementation,
not a proven planner design. Function volatility, parallel safety, costs,
operator strategy registration, and plan recognition remain implementation
questions. No speculative SQL stubs are installed by this PR.

Proposed initial query meaning: plain text analyzed with the index's `standard`
analyzer, optional/OR matching among resulting terms, query syntax disabled
(`skipSyntax=true` in the remote query-string mapping). Reject empty/whitespace
queries with `22023`; a nonempty query producing no tokens matches no rows.
NULL text/query produces SQL NULL, hence no WHERE match. Analyzer/tokenization,
Unicode, repeated terms, and score ordering require deployed-service fixtures.
The public [query-string API](https://docs.lambdadb.ai/guides/search/query-string)
is the mapping reference, not evidence that this extension already implements it.

Strict BM25 requires one coherent corpus/statistics definition across the
snapshot-visible base and delta. Independently scoring an overlay and merging
numbers is not accepted. Proposed reference corpus: snapshot-visible documents
of that index generation, including transaction-local changes. How to obtain
matching remote statistics, how deletions affect them, and the effect of tenant
filters/RLS on corpus visibility are unresolved blockers. Do not claim strict
BM25 or security isolation from ID filtering alone. The final contract must
resolve these before enabling broader security/query shapes.

## Remote compatibility gates

The public [Query API schema](https://docs.lambdadb.ai/reference/api/openapi.json) was inspected
on 2026-09-26; no authenticated remote requests or remote resources were created.
This is published-schema evidence, not a deployed-service compatibility result.

- Query `size` is 1–100. No ranked continuation token is declared. `total` is
  documented as the number returned, not an eligible-corpus count or exhaustion
  proof. Filtering rejected candidates or LIMIT above one page cannot be called
  complete without a continuation/coverage protocol. A bounded fixture may only
  claim completeness when its entire candidate universe is independently proven.
- `isDocsInline=false` returns results through `docsUrl`; support and validate
  the offloaded JSON array as well as inline documents. Do not forward LambdaDB
  API credentials to a result-download URL. Download failures/incomplete data
  must not produce silently partial SQL results.
- Versioned reads must use the verified immutable Tag selected for the PG
  snapshot. The schema allows `consistentRead=true` only with Branch refs;
  it is not an extra consistency switch for immutable Tag reads.
- Validate document identity, dimensions, cosine candidate ordering, BM25 query
  behavior, response score meaning, and publication readiness against the actual
  deployment. The [vector API](https://docs.lambdadb.ai/guides/search/vector)
  documents explicit query vectors; its existence does not prove snapshot
  overlays, continuation, or ready-Tag barriers required by this extension.

## Proposed errors and acceptance gates

| Condition | Proposed behavior |
| --- | --- |
| Invalid vector input/distance | Keep the implemented type's `22000` / `22023` behavior. |
| Invalid BM25 query/options | `22023`, before remote execution. |
| Unsupported query/transaction shape | `0A000`; precise detection must be demonstrated. |
| Missing BM25 execution context or unavailable/degraded selected index | `55000`; no stale or silent local-BM25 fallback. |
| Synchronization failure after source commit | Success + `01000` warning, durable replay, index health guard. This is observed only in the separate probe. |

Next executable gates, before claiming the first E2E complete:

1. Run a bounded deployed-service compatibility fixture for both search paths,
   including inline/offloaded results, immutable Tags, candidate coverage, and
   BM25 score/statistics behavior. Resolve API gaps instead of hiding them in SQL.
2. Prove actual plans and execution-local match/score binding, health checks on
   reused plans/rescans, and local reference comparisons for both modalities.
3. Implement/audit durable identity, fenced replay, remote readiness/publication,
   snapshot overlays, and AFTER_LOCKS cleanup safety. Extend fault tests beyond
   the mock worker and the two tested synchronous clients.

Product timeouts/SLOs, supported client/version matrix, synchronization status
API, SQL naming approval, license, and release compatibility remain open.
The 300/1500/5000 ms waits in the fixture are test controls, not product defaults.
