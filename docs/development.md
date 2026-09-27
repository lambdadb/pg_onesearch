# Initial development decisions — 2026-09-26

## Authority and scope

The [reviewed plan](https://github.com/lambdadb/sbrain/blob/628fdf77aaf36a4544c2083d11579525c95c9931/projects/lambdadb-postgresql-extension.md)
remains the requirements and release-policy authority. The pinned file was read
in full and compared with local sbrain `main`; that file had no differences.
This record covers implementation choices and evidence for the bounded build
spike requested on September 26. It does not close the first end-to-end milestone.

## Accepted stabilization scope — 2026-09-27

The owner accepted stabilizing the existing restricted search paths before
revisiting changed-corpus BM25. This decision takes precedence over the earlier
implementation sequence below; the original full transaction-search milestone
remains incomplete.

- Admit BM25 only when the complete effective indexed document map under the SQL
  snapshot equals the selected Tag's base map. Unrelated source-column changes
  remain eligible, subject to the existing query, health and admission checks.
- Reject effective indexed changes, including own writes and committed replay
  lag, with `0A000` before HTTP. Do not return stale results, silently use local
  BM25, or combine independently scored base/delta top-k results. After replay
  and verified publication, a read is eligible only if its own snapshot passes
  the same map comparison; publication does not refresh an existing PG snapshot.
- Keep the current bounded vector path and the PostgreSQL outbox/replay/verified
  Tag publication flow. These are experiments, not activated product features.
- Defer LambdaDB core changes, a request-scoped overlay API, and temporary
  Branches for source transactions or individual searches. Existing replay
  attempt Branches remain part of the tested publication protocol.
- Preserve the overlay experiment as research evidence. Live-corpus statistics,
  equality with rebuilt-corpus scores, and compatibility with ordinary Tag
  scores are unresolved alternatives, not newly accepted scoring guarantees.

### Stabilization sequence

1. Validate the restricted BM25 boundary through SQL execution: indexed own
   writes and committed lag reject before HTTP; unrelated-column changes remain
   readable; rollback and verified publication restore eligibility when the
   current statement's document map matches. Include prepared execution/rescans.
2. Strengthen synchronization failure and recovery evidence: preserve committed
   PG rows and replay work; block affected-index search; retry without allowing
   an obsolete worker to publish; restore search only after verified publication
   and the applicable generation/health checks. Cover worker termination and
   ambiguous remote acknowledgements at publication boundaries.
3. Stabilize generation retirement and remote resource retention/cleanup without
   deleting resources still needed by admitted snapshots or active attempts.
   Then assess product integration and scalable lookup/continuation within the
   explicit supported scope.

This ordering is the implementation plan; it does not claim new test coverage or
completed recovery behavior. Keep local fixture, live-service, CI and release
evidence separate. Commit-wait/protocol safety, security, scale and release gates
remain open. Revisit a generic temporary search workspace only after use cases
beyond pg_onesearch and the cost of existing Branch/Tag workflows justify it.

The first stabilization increment connects the SQL reader and Custom Scan to
durable replay readiness: a committed pending batch blocks affected-index reads
until verified publication commits, even after worker termination or PG restart.
The [executor recovery tests](snapshot-executor-spike.md#replay-failure-and-recovery-coverage)
cover this bounded behavior with local fixtures. General remote health detection,
automatic retry scheduling, commit-wait, resource cleanup and product integration
remain open; no LambdaDB core change or changed-corpus BM25 path is introduced.

The [retention increment](retention-spike.md) adds durable, retryable cleanup for
eligible published attempt refs after the PG snapshot horizon has passed, while
retaining current bases and possible replay-parent dependencies. It also admits
published refs of committed retired generations. Abandoned attempts, collection
deletion, audit compaction and production scheduling remain deferred; local
fixtures do not establish live service deletion or physical storage reclamation.

The [publication completion increment](publication-commit-spike.md) connects the
bounded commit callback to actual replay publication through durable exact event
requirements across affected generations. A separate observer reconciles
committed coverage after notification loss, and optional reader gating covers
the source-commit/claim gap. Offline integration tests include timeout, ambiguous
ACKs, partial publication, process termination and PG restart. This does not
adopt the hook for production or add automatic replay scheduling; the Python
adapter, live integration, supported lifecycle, scale and release gates remain open.

The [automatic replay increment](replay-scheduler-spike.md) adds a single serial
Python scheduler over that adapter, with database-scoped session ownership,
committed-work discovery, durable capped retry backoff and restart recovery.
Offline tests now complete source commits without explicit per-generation replay
calls. This does not choose the product worker language or add a production
launcher, automatic PG reconnect, parallelism, retirement completion or live
service evidence. The earlier increments' limitations describe their standalone
fixtures; this optional scheduler is an additional integration layer.

The [live automatic replay run](live-scheduler-validation.md) subsequently
verified real LambdaDB publication, exact two-index recovery, SQL vector/BM25
results and owned-fixture cleanup. Its normal mutation returned a warning after
10.003 seconds and finished publication at 120.347 seconds. Thus a functioning
remote service still exceeded the probe's current wait cap: live no-warning
commit completion is unproven. Wait/completion policy and production hook
adoption remain explicit gates; the accepted requirement is not relaxed.

## Confirmed requirements from the handoff

- Extension/repository identifier: `pg_onesearch`; no required pgvector extension.
- PostgreSQL retains original text/vectors; remote type I/O must not write data.
- Both vector and BM25 belong in the first end-to-end PoC. BM25 separates match
  from descending score projection. Each PG index owns independent remote resources.
- Source changes and durable replay capture commit atomically in PostgreSQL.
  Healthy commit responses wait for remote readiness, a verified Tag, and PG
  publication. After PG commit, a remote failure returns success plus a sync
  warning, preserves replay work, and blocks affected-index searches until recovery.
- Strict reads must respect PG snapshots and own writes; no silent stale fallback.
  Snapshot overlays and BM25 corpus/statistics correctness are acceptance gates.
- Feature review into `develop`; release gates before promoting implementation
  to `main`; independent extension versions and immutable release tags.

The transaction requirements above are not implemented by the product skeleton.
The separate [commit/worker probe](commit-worker-spike.md) now provides bounded
feasibility evidence; it is not the production synchronization implementation.

## Selected for this spike

| Choice | Decision and reason |
| --- | --- |
| Language | C with PostgreSQL-native PGXS: direct access to the IAM, transaction and worker APIs needed for the next spikes; fewer initial toolchain layers. This is an engineering selection under the current request, not a prior owner decision. Rust/pgrx was considered and is not selected for this spike. |
| PG target | PostgreSQL 18.6, PGDG `18.6-1.pgdg12+2`; official stable minor release verified in [release notes](https://www.postgresql.org/docs/release/18.6/). No claim of other major-version compatibility. |
| OS / CPU | Debian 12 / Linux arm64, matching the working Docker host architecture. Ubuntu and x86_64 in the plan were examples, not commitments. Native macOS binaries are not validated. |
| Toolchain | GCC 12.2.0 (`12.2.0-14+deb12u1`), GNU Make 4.3, Clang/LLVM 19.1.7 (`1:19.1.7-3~deb12u1`) for PGXS bitcode. Dockerfile pins those packages, PG headers, and the base-image digest. Transitive apt dependencies remain repository-resolved; this is not a bit-reproducible release build. |
| Namespace | Fixed `onesearch` schema; `onesearch.vector(n)`, `onesearch.cosine_distance(a,b)`, `OPERATOR(onesearch.<=>)`. Explicit qualification avoids dependence on application search_path. Development API, subject to review before a release. |
| Development version | `0.1.0-dev` in control/install SQL. No published version, release tag, upgrade SQL, or backward-compatibility promise. |
| Repository | `main` is the release branch; `develop` contains the reviewed skeleton from PR #1. Further work uses feature PRs into `develop`. Public GitHub repository approved; existing organization rulesets govern branches without repository-level rule changes. GitHub Actions runs the same Docker tests and clean-install check on pull requests. License selection remains pending. |

Build conventions follow [PGXS](https://www.postgresql.org/docs/18/extend-pgxs.html)
and the [PG type interface](https://www.postgresql.org/docs/18/xtypes.html).
No pgvector source was copied. The development host had Clang 21, Make, Docker
29.8.0 with a working Linux arm64 daemon, Homebrew, and authenticated GitHub CLI;
`pg_config`, `psql`, `cargo`, and `rustc` were absent from PATH. Host PostgreSQL
and host services were not installed or changed.

## Implemented local SQL/value contract

- 2–4096 finite float32 components; `vector(n)` enforces dimensions on input,
  typed-value assignment, explicit casts, and binary protocol binding.
- Bracketed comma-separated text, with whitespace and exponent notation.
  Components use PG float4 parsing/rounding; overflow and underflow-to-zero are
  rejected. Finite representable subnormals are accepted.
- Output uses shortest round-tripping float32 text independent of
  `extra_float_digits`. Text output canonicalizes number spelling.
- Experimental binary format: network-endian signed int32 dimension followed by
  exactly that many network-endian IEEE float32 values. This is our format, not
  a pgvector binary-compatibility claim. Truncation, trailing bytes, bad dimensions,
  and non-finite components are rejected.
- Variable-length, int4-aligned storage with TOAST `external`; originals stay in PG.
- NULL inputs return NULL. Zero vectors can be stored, but cosine involving one
  raises SQLSTATE `22000`. Non-finite elements and dimension mismatch also use
  `22000`; invalid dimensions use `22023`. PG parser/protocol errors retain their
  native SQLSTATEs. These choices apply only to this development type.
- Cosine distance is `1 - dot(a,b)/(norm(a)*norm(b))`, accumulated in float64 and
  clamped to [0,2] for floating-point roundoff. Lower is closer. Ties require an
  application tie-breaker (for example `ORDER BY distance, id`). No ANN behavior.
- No access method, opclass, remote index, BM25 stub, outbox, hook, or background
  worker is registered. Ordinary PG value/table transactions work normally;
  they provide no evidence for the planned cross-system transaction protocol.
- Installation requires superuser; ordinary roles receive schema usage and can
  use the functions/type with normal table permissions. There is no preload step.

## Unresolved decisions and next executable gates

The [C remote read experiment](remote-read-spike.md) selects libcurl for an
isolated candidate and adds actual SQL-to-LambdaDB Tag queries. Its transport
and restricted SQL wrapper are separate from the product extension. It does not
close the planner/executor, strict-read, remote-write, or production credential
gates below.

The [Custom Scan experiment](executor-spike.md) now exercises vector and BM25
through actual PG plans, with scan-owned scores, prepared statements, and
correlated rescans. It is limited to a frozen five-row fixture; it does not
implement an index access method, mutable-source synchronization, or own writes.

The separate [IAM lifecycle experiment](index-lifecycle-spike.md) adds real local
index relations with WAL-backed generation metadata, transactional rebuild/drop,
recovery, and logical-restore tests. Its indexes remain unpublished and reject
every search. It does not connect the Custom Scan experiment to product indexes.

The [change capture experiment](change-capture-spike.md) connects those index
generations to a logged source registry and outbox. Trigger-based capture covers
source DML, HOT, PK changes, rollback, and durable retirement records. It remains
a separate SQL prototype. The [batch replay experiment](batch-replay-spike.md) adds
a synchronous test worker, per-attempt Branch isolation, verified Tags, and atomic
PG publication/exact outbox deletion. The [snapshot reader](snapshot-read-spike.md)
connects those tables to explicit C/SQL JSON queries: bounded complete vector
overlays and same-corpus BM25 reads. The [snapshot-aware Custom Scan](snapshot-executor-spike.md)
now returns typed heap slots with explicit index/column/key binding, same-snapshot
row identity checks, SQL filtering/sorting/LIMIT, prepared refresh and rescans.
It still performs bounded full heap/history scans and rejects changed-corpus BM25.
The [BM25 overlay contract](bm25-overlay-contract.md) is a deferred server proposal,
not an approved or available API. The [independent Lucene overlay experiment](bm25-overlay-spike.md)
adds a bounded effective-statistics/oracle proof; it is not server or PG integration.
Product IAM/Custom Scan activation remains open.

| Area | Required next evidence |
| --- | --- |
| Current stabilization scope | The accepted scope above allows same-corpus BM25 only. Validate rejection, synchronization recovery and lifecycle safety without implementing a new LambdaDB overlay API. The broader rows below remain original milestone/product gates, not prerequisites for starting this restricted stabilization work. |
| First E2E milestone | Both remote vector and BM25 indexes, actual index plans, mutation/search reference comparisons, source retention across index lifecycle. BM25 remains in this milestone. |
| BM25 SQL | [Concrete SQL proposal](sql-client-contract.md) specifies match/score binding and bounded query shapes. The [snapshot Custom Scan proof](snapshot-executor-spike.md) binds scores to a registered probe index, source alias/key and execution state; product API, analyzer fixtures, and coherent corpus/overlay statistics remain open. |
| Commit response | The separate probe validates an AFTER_LOCKS candidate for specific PG18.6 schedules, including cancellation. Execute+Flush can deliver CommandComplete before commit; [18 actual-client scenarios](client-contract-validation.md) distinguish execute/commit, warnings, cancellation, and prior transaction errors. Audit cleanup-phase safety and enforce protocol scope before adopting the candidate. See [evidence](commit-worker-spike.md). |
| Outbox/worker | [Atomic capture proof](change-capture-spike.md) covers savepoints, failures, HOT, late smaller IDs, and crash recovery. [Bounded replay proof](batch-replay-spike.md) adds frozen membership, attempt fencing, readiness and atomic publication/deletion. Production scheduling, large backlogs, retention/backpressure and broader fault tests remain open. |
| Reads/health | [Statement-snapshot proof](snapshot-read-spike.md) covers Tag/delta/heap MVCC, vector own writes, prepared calls and fresh injected health. The [snapshot Custom Scan](snapshot-executor-spike.md) adds typed execution and snapshot-local TID comparison. Coherent BM25 overlays, product scan integration, durable health detection/recovery and scalable row-version lookup remain open. |
| Remote protocol | [Live compatibility experiment](live-compatibility.md) adds isolated vector/BM25 fixtures, a final indexed-marker barrier, immutable Tags, and response/score checks. Ranked continuation, production replay coverage, and BM25 overlay/statistics remain gates. The original skeleton required no credentials; the opt-in REST harness uses an ignored environment file. |
| Identity/lifecycle | [Local generation/DDL proof](index-lifecycle-spike.md) covers independent index generations and transactional physical replacement. The capture/replay probes add source epochs and owned remote bindings. Production remote cleanup, replacement activation, PK/TID/version mapping, HOT/pruning/TID reuse, and physical-restore fencing remain open. |
| Release | Maintainer, license, numerical SLOs, pilot workload, compatibility/upgrade formats and distribution policy. Repository visibility is public; branch rulesets remain organization-managed. |

The [SQL/client proposal](sql-client-contract.md) records the next contract stage
without treating proposed names or client scope as implemented features. The
bounded planner/executor proof is separate from the next product work on index
identity/lifecycle and snapshot-correct reads. Ranked
continuation, strict BM25 overlays, commit-wait latency, and production suitability
of the AFTER_LOCKS candidate remain unresolved.
No broader transaction, remote recovery, performance, managed-provider, or production-support claim follows
from the local skeleton tests.
