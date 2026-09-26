# Commit response and worker feasibility — 2026-09-26

## Result

A bounded wait in the root resource owner's `RESOURCE_RELEASE_AFTER_LOCKS`
callback worked in the tested PostgreSQL 18.6 scenarios: a real background
worker could observe the committed capture, acquire a conflicting source-table
lock after release, commit a receipt, and complete the writer's wait. Waiting
inside `XACT_EVENT_COMMIT` timed out on the same lock schedule.

This identifies a candidate worth further review, **not a production-safe hook
or an implemented LambdaDB transaction guarantee**. PG is still in commit
cleanup with interrupts held. The product extension is unchanged: this probe
is separately compiled/installed only inside its disposable test container.

An independent protocol limitation was demonstrated: extended-protocol
`Execute + Flush` can deliver `CommandComplete` before the transaction commits.
The commit and any synchronization warning occur on `Sync`. Therefore a promise
that *every* command-completion message establishes source commit/publication
cannot apply to arbitrary extended-protocol or pipeline clients.

## Run and inspect

```sh
./scripts/test-commit-worker.sh
```

The harness asserts PostgreSQL 18.6, logged fixture tables, and `fsync`,
`synchronous_commit`, and `full_page_writes` all enabled. It uses raw protocol
messages over local Unix sockets, concurrent observer connections, actual PG
transaction/resource callbacks, and an actual dynamic background worker.
No port is published and the test container has no network access.

- [Probe module](../spikes/commit_worker/probe.c), [its Makefile](../spikes/commit_worker/Makefile)
- [Logged source/outbox/receipt fixture](../spikes/commit_worker/setup.sql)
- [Protocol and fault schedules](../spikes/commit_worker/test.py)
- [Container setup/cleanup](../spikes/commit_worker/run.sh), [entry point](../scripts/test-commit-worker.sh)
- [CI](../.github/workflows/ci.yml) runs this alongside the existing skeleton
  tests and attaches its log to successful evaluation artifacts.

The mock's receipt is a local durable completion record. It represents neither
remote acceptance nor search readiness nor a LambdaDB Tag. No live remote
request, IAM/opclass, search health guard, or query overlay exists in this spike.

## Executable schedules and observed results

All 20 scenarios passed locally. Each is an assertion over specified data and
message ordering, not a timing-only success test. Local diagnostic timings
included about 1003 ms for the deliberately blocked 1000 ms wait and 10 ms for
the after-lock comparison; these are observations, not performance budgets.

| # | Schedule / fault point | Assertion and observation |
| --- | --- | --- |
| 1 | Insert inside an open transaction, inspect from writer and another session, then roll back | Writer sees capture; observer sees neither source nor outbox; rollback removes both. |
| 2 | Nested savepoint release then parent rollback; released child then outer rollback; parent capture surviving child rollback | Capture and waiter marks roll back with their owning transaction; no surviving captures means no wait, while surviving parent capture still publishes. |
| 3 | Hold source ACCESS EXCLUSIVE; insert; wait in COMMIT callback | Outbox is committed and visible, but worker's ACCESS SHARE request is actually lock-blocked. Writer times out with warning and COMMIT success; replay completes after locks release. |
| 4 | Repeat #3, including a released savepoint, at root AFTER_LOCKS | Worker publishes before writer responds. No warning; outbox deleted and receipt visible. Callback reports interrupt holdoff = 1. |
| 5 | One simple-protocol INSERT, autocommit | Source and durable receipt exist when the complete response returns. |
| 6 | Extended-protocol INSERT followed by Sync | Full response through ReadyForQuery arrives after publication. |
| 7 | Explicit COMMIT through extended protocol | Publication precedes COMMIT CommandComplete and ReadyForQuery. |
| 8 | Extended Execute+Flush, withhold Sync; pause worker; then Sync | CommandComplete arrives while source/outbox are invisible to another session. Sync commits, times out, sends warning then ReadyForQuery; replay later succeeds. |
| 9 | Pause worker during a simple autocommit | Observer sees committed source/outbox before response. Warning precedes CommandComplete, followed by idle ReadyForQuery; resume drains replay. |
| 10 | Mock failure after reading committed capture | Warning plus SQL success; no receipt/deletion; resume preserves and processes the capture. |
| 11 | pg_cancel_backend while the writer is waiting after commit | Wait ends with synchronization warning, command success and idle ReadyForQuery; the connection remains usable and replay completes. The probe observes but does not clear PG's cancel flags. |
| 12 | PREPARE TRANSACTION after source mutation, with 2PC enabled in PG | Probe rejects with 0A000 before preparation; source/capture roll back and no prepared transaction remains. |
| 13 | Allocate smaller outbox ID in an uncommitted transaction; commit a larger one first | Larger event publishes; subsequently committed smaller event also publishes. No allocated-ID frontier is used. |
| 14 | INSERT, UPDATE, DELETE, reinsert same key | Four distinct event identities and complete old/new payloads retained in receipts, including the deleted text. This does not prove remote mutation ordering. |
| 15 | Terminate worker after receipt INSERT/outbox DELETE, before commit | Worker transaction rolls back; source stays committed, outbox remains, receipt absent. Writer warns; restarted worker replays. |
| 16 | Terminate worker after publication commit, before shared notification | Receipt survives and outbox is already deleted. Writer conservatively times out; durable receipt lookup distinguishes completion. Restart causes no duplicate receipt. |
| 17 | Keep an older REPEATABLE READ snapshot across publication, DELETE and VACUUM | Old snapshot retains old outbox and sees no receipt; fresh observer sees the receipt and no outbox. This is local MVCC evidence, not remote Tag retention. |
| 18 | Disconnect writer after observer confirms source commit, during wait | Bounded wait cleans up; reconnect confirms source/outbox, and worker replays. Original client cannot infer its outcome from the lost connection. |
| 19 | Immediate cluster shutdown after source commit, before delivery | WAL recovery restores source and capture. Explicitly restarting the test worker publishes the retained payload. |
| 20 | Commit a second transaction while the first worker batch is paused before publication | Publishing the first batch wakes only its matching waiter. The second remains waiting with its own outbox until a separate batch publishes. |

Worker termination in #15/#16 is SIGTERM through `pg_terminate_backend`, not a
simulated power loss. #19 uses `pg_ctl -m immediate` and confirms crash recovery.
The fixture reuses one source table with known keys; it is not a benchmark or
comprehensive randomized concurrency/fault campaign.

## Why the two callback points differ

The inspected PostgreSQL source is pinned to `REL_18_6`:

- [xact.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/transam/xact.c#L2389)
  makes the transaction visible before invoking COMMIT callbacks, then releases
  transaction resources and locks. Interrupts remain held during this cleanup.
  The source explicitly cautions that errors here cannot undo the commit.
- [resowner.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/resowner/resowner.c#L756)
  releases transaction locks in the LOCKS phase; add-on callbacks also run for
  child resource owners. The probe checks both top-level commit and
  `CurrentResourceOwner == TopTransactionResourceOwner`, plus its committed
  flag, before waiting in AFTER_LOCKS.
- [postgres.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/tcop/postgres.c#L2288)
  finishes explicit transaction commands before their completion message.
  Ordinary extended executions can report completion before Sync finishes the
  transaction. The [protocol documentation](https://www.postgresql.org/docs/18/protocol-flow.html)
  describes these distinct messages and boundaries.

The writer allocates its waiter slot and wait-event identifier before commit.
The wait itself uses only bounded latch waits and shared-state polling: no SPI,
new snapshots, network I/O, new resource-owner entries, or calls to
`CHECK_FOR_INTERRUPTS`. It observes cancel/terminate flags without resetting
them and uses WARNING, not ERROR, for incomplete synchronization. This still
needs review against PG cleanup restrictions, callback composition, shutdown,
connection failure, and the complete supported SQL/protocol surface.

The fixture trigger stores old/new payload and an exact full transaction ID in
a logged outbox inside the source transaction. Subtransaction callbacks remove
aborted waiter marks or reparent committed ones. The worker selects committed
transaction identities, transfers their exact outbox membership into receipts
in one transaction, commits, then changes shared notification state. It does not
interpret the maximum allocated outbox ID as committed coverage.

## Decision boundary and remaining work

Confirmed requirements remain source/outbox atomicity, strict snapshot-correct
search, healthy-path publication waits, and commit-success-plus-warning after
remote failure. This experiment does not change those requirements or settle
the final warning text, SQLSTATE/status API, or remote protocol.

Proposed next contract review: define transaction completion for synchronous
simple-query and extended-query-with-Sync clients, and explicitly decide the
pipeline/Flush policy. A driver may expose command completion before Sync;
waiting later cannot retract an already sent message. Do not silently promise
the same completion contract for every client.

Before adopting AFTER_LOCKS in the product, validate broader callback/resource
interactions, terminate/cancel timing around every boundary, deferred triggers,
COPY, multi-statement queries, session advisory locks, other extensions, worker
registration/failure races, connection backpressure, and server shutdown.
The current probe bounds *waiting*, not all warning delivery/cleanup latency.

Deliberate fixture limits:

- Superuser-only, one database/source table and one worker registration per
  postmaster lifetime. Sixteen waiter slots; 20 ms worker polling. Not a
  production launcher or scale/retention design. Full restart explicitly
  re-registers the worker; shared memory is never recovery authority.
- Shared notifications can be lost (#16); this causes a conservative warning.
  Durable receipts are inspectable but no automatic waiter reconciliation or
  production operation-status interface is implemented.
- No stale-worker fencing token, remote idempotency, commit-order proof,
  per-index generation/health guard, remote Tag publication, or multi-index
  coverage. The shared-memory registry is fixture coordination, not fencing.
- No supported DDL/replication/restore contract. Test resets use TRUNCATE only
  between scenarios; worker cleanup uses transactional DELETE.
- No product upgrade or persistent-format changes. The evaluation bundle test
  asserts the probe shared library is absent; normal CREATE EXTENSION still
  loads only the vector skeleton.

The next implementation work remains the vector and BM25 SQL/execution
contracts plus remote API/readiness/overlay gaps. BM25 stays in the first E2E
milestone. Production capture and worker integration should follow the protocol
and callback decisions above, with the same fault schedules carried forward.
