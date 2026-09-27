# Automatic replay discovery and durable retry

This optional superuser-only probe automatically calls the existing synchronous
Python replay adapter for committed work. Together with the
[publication completion observer](publication-commit-spike.md), a source commit
can now reach verified publication and finish its wait without the test calling
`replay()` for each generation. It is not an installed product worker or a
production service launcher.

## Implemented behavior

`Scheduler.run()` owns one dedicated, idle-autocommit PostgreSQL connection. A
session advisory lock permits one scheduler per database. A second process is
rejected, and reentrant acquisition on the same session is rejected rather than
leaking another lock count. Discovery and result recording verify ownership.
The session stays connected during remote I/O, but no SQL transaction, row lock,
or source-table lock is held across it. Transaction pooling and sharing the
scheduler connection with other work are outside the contract. PostgreSQL's
[session-lock lifetime and reentrancy](https://www.postgresql.org/docs/18/explicit-locking.html#ADVISORY-LOCKS)
and [database scope / two-integer lock identity](https://www.postgresql.org/docs/18/view-pg-locks.html)
are the basis for this coordination; owner checks require the exclusive lock.

Each iteration discovers bound, capturing generations with committed outbox
work or a pending replay batch. Unbound generations are untouched. A binding
created after startup is discovered by a later iteration. Retired sources and
generations are skipped, preserving their outbox and remote resources for the
separate lifecycle protocol.

The SQL scheduler selects one due generation, preferring the least recently
attempted among due jobs. It commits a provisional retry deadline and increments
its unconfirmed-attempt count **before** replay claim or HTTP. If the process
dies, the next process can rediscover the pending work without immediately
repeating the attempt before that deadline. Only one generation is processed at
a time; failed generations wait while due siblings can proceed. A slow active
request still delays every sibling until that request finishes or times out.

The unchanged replay protocol then claims all visible events within its existing
limits, commits a new attempt nonce, batches documents, checks the final indexed
marker, verifies a Tag and commits the exact publication receipt. Each retry
uses a fresh attempt and Branch/Tag. The scheduler does not replace replay
fencing, use maximum event IDs as a cursor, or issue one remote write per row.

Known remote/protocol errors and database statement errors schedule exponential
backoff, starting at 1 second and capped at 60 seconds by default. Test-only SQL
configuration permits a base of 10 ms–60 seconds and a cap up to 1 hour. Failure
sets the next deadline relative to the **failure response**, so a request taking
longer than the provisional deadline cannot cause an immediate busy retry.
Success clears the unconfirmed count; the loop yields between every iteration
(default 50 ms). Expected error metadata contains only `remote`/`database` and a
SQLSTATE when present, never an exception message or server response body.

A lost/broken PG connection terminates that scheduler instance. It never
reconnects inside an attempt or assumes that it still owns the advisory lock.
A caller can start a fresh process/connection, which discovers durable work and
uses the existing nonce fencing. Unexpected Python exceptions also terminate
the loop, leaving the provisional retry record intact. The database session
lock is scheduling coordination; replay's active nonce remains publication
authority, including if an old remote request outlives its PG session.

A stop event is observed between iterations. An in-flight iteration finishes
before the scheduler releases ownership; this does not cancel an HTTP request.
Stop latency inherits adapter/request timeouts. Process restart, PG reconnect,
and observer re-registration after a postmaster restart remain the caller's
responsibility.

## Status and completion authority

```sql
SELECT generation, attempts, consecutive_unconfirmed, next_attempt_at,
       last_attempt_at, last_success_at, last_error_kind, last_sqlstate
FROM pgos_scheduler_probe.jobs;

-- A recorded source transaction's actual publication evidence:
SELECT * FROM pgos_completion_probe.status('1234'::xid8);
SELECT pgos_completion_probe.is_published('1234'::xid8);
```

Scheduler rows are scheduling diagnostics, not proof of publication or remote
health. In particular, a publication response can be lost after the transaction
has committed: the scheduler may retain error/unconfirmed metadata while exact
publication coverage is complete. With no remaining outbox/pending batch, a new
scheduler does not repeat the remote work merely to clear that metadata. A
future source write can reuse the job after its backoff deadline.

The completion observer independently resolves the commit waiter from durable
coverage. Source timeout warnings, pre-claim read gating, per-generation recovery,
own-write vector behavior and restricted BM25 semantics are unchanged. Full
transaction completion still requires all affected generations; remote
publication is not atomic across indexes.

## Validation

```sh
./scripts/test-replay-scheduler.sh
# When the prerequisite image was built from this checkout:
./scripts/test-replay-scheduler.sh --reuse-completion-image
```

The suite runs actual PG18.6 / Debian12 / arm64 capture, replay, C commit hooks
and the completion observer, with a deterministic remote API fixture and local
TLS search. The disposable container uses `--network none`; `.env.local` is not
loaded. The 16 tests cover:

- Automatic two-index commit completion and multi-document RPC batching.
- Ambiguous Tag ACK followed by automatic retry with a distinct attempt nonce.
- A persistently failing index, a healthy sibling, timeout warning, retained
  source/outbox, and later automatic recovery.
- Backoff persistence across scheduler sessions, exponential growth and cap;
  delay after a slow failed request.
- Singleton ownership, reentrant rejection, lock release on session exit and
  owner checks on scheduler SQL functions.
- Actual scheduler child-process SIGKILL after attempt commit; preserved deadline
  and fresh replay attempt after replacement scheduler startup.
- PG session termination during an in-flight iteration; old loop exits and a new
  loop resumes. The checkpoint is before HTTP, not live in-flight HTTP proof.
- Immediate PG stop/WAL recovery after a scheduler has stopped with failed work;
  durable state retained, explicit process/observer restart, then automatic drain.
- Uncommitted/rolled-back events excluded, new bindings discovered after startup,
  and retired generations skipped without deleting their work.
- Database error backoff while a sibling succeeds; permission and connection
  boundaries; graceful stop after one active iteration.
- Lost response after committed publication; the writer still completes and
  replacement discovery makes no duplicate remote calls.

The preceding 15 publication-completion scenarios also passed locally with the
scheduler SQL installed and no scheduler running. CI runs both suites separately
and uploads `replay-scheduler.log` with the existing evidence.

## Confirmed scope and open decisions

Confirmed: retain PostgreSQL source/outbox authority, exact verified publication,
batched writes, the accepted BM25 restriction and no LambdaDB core change.
Implemented here: a single serial Python scheduler and durable retry policy in an
isolated probe. The language/runtime of a future product worker is not decided
by this experiment.

Still open: a production launcher/supervisor, automatic PG reconnect policy,
parallel generation scheduling, jitter and error-specific retry/pause policies,
backpressure, statement timeout/lock-wait policy, bounded shutdown and latency
SLOs. SQL work discovery and retained audit tables are not a scale design. A
permanent unsupported input retries at the cap until corrected or retired; there
is no operator pause/dead-letter interface in this increment.

Retirement completion/remote cleanup, online installation/backfill, restore and
replication behavior, live-service validation of the integrated scheduler, and
production adoption of AFTER_LOCKS remain separate gates. The product extension,
evaluation bundle and release state are unchanged.

Implementation: [scheduler](../spikes/replay_scheduler/scheduler.py),
[durable scheduling SQL](../spikes/replay_scheduler/setup.sql),
[tests](../spikes/replay_scheduler/test.py),
[runner](../scripts/test-replay-scheduler.sh).

## Opt-in live integration

The [live scheduler harness](live-scheduler-validation.md) adds real LambdaDB
writes/Tags and SQL verification to this automatic loop, reusing the shared
credential and owned-resource cleanup boundary. The same scenario runs against
local fixtures in CI. Consult its evidence section for measured live outcomes;
CI alone remains offline proof.
