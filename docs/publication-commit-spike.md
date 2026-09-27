# Commit completion over verified replay publication

The optional integration probe connects PG commit waiting to the existing
capture/replay protocol's durable publication, replacing the **mock receipt**
used by the original [commit/worker experiment](commit-worker-spike.md). It
remains a superuser-only experiment; `CREATE EXTENSION pg_onesearch` and the
evaluation bundle do not enable it.

## Contract and implementation

A source transaction records its exact `(event_id, writer_xid, generation)`
requirements in the same transaction as the source and outbox. The full top-level
`xid8` identifies the writer; rollback and savepoint rollback remove both capture
and requirements. The existing subtransaction-aware C mark arms the bounded
`RESOURCE_RELEASE_AFTER_LOCKS` wait. It still performs no SQL, snapshots, network
I/O, or interrupt processing in commit cleanup.

The existing Python replay adapter continues to batch writes, fence each attempt,
verify its final indexed marker with `consistentRead: false`, create and verify
the immutable Tag, and commit `publish()`. No service API or LambdaDB core change
is introduced. The adapter is driven explicitly by the test harness; this change
does **not** add an automatic replay scheduler.

A separate C background **observer** polls committed SQL coverage for registered
waiters. Completion requires every requirement, across every affected index, to
join an event with the same event ID and writer xid in a published batch for the
same generation. The existing `publish()` checks the full frozen event payload
against the source outbox before publication and deletion commit atomically.
Neither the current head alone, a maximum event ID, a verified-but-unpublished
Tag, nor an uncommitted SQL publication can finish the writer's wait. Coalesced
remote writes retain all original event identities in the coverage audit.

Waiter identity includes database OID, PID and full writer xid. The observer
checks that identity again before updating shared state, avoiding completion of
a reused slot. If it dies after observing publication but before notifying the
writer, its restarted process queries the same durable predicate. No callback
from the replay adapter is required. A postmaster restart loses waiter state,
but retains requirements and publication records; observer registration and
replay restart remain explicit harness operations.

## Failure, status and search boundaries

The writer has a bounded wait (default 1 second, configurable 10 ms–10 seconds
in the inherited probe). Timeout or a pending cancellation emits SQLSTATE
`01000` after PG has committed. The source and remaining replay work survive.
The callback cannot inspect SQL, so a delayed observer can cause a conservative
warning even when durable publication has already succeeded. A warning is not
proof of rollback or proof that publication is still incomplete.

Superuser-only SQL exposes the durable evidence:

```sql
-- Use a recorded source top-level xid8, not the observer's current xid.
SELECT * FROM pgos_completion_probe.status('1234'::xid8);
-- generation | required_events | published_events
SELECT pgos_completion_probe.is_published('1234'::xid8);
```

`is_published` requires nonempty membership and complete coverage. Unknown or
aborted transaction IDs return false; this is not a general transaction-outcome
API. The SQL functions use the caller's snapshot; check from a fresh READ
COMMITTED statement. The observer obtains a new transaction snapshot each poll.
These records attest historical publication, not present remote availability or
continued existence of the generation. Audit records remain retained.

The optional `pending_commits` view connects incomplete requirements to the
reader's existing **latest-snapshot** health check. It conservatively blocks an
affected index immediately after source commit, including the gap before replay
claim. Completion of one index restores that index independently, while a writer
that changed multiple indexes waits for all of them. A later writer's pending
requirements can keep searches blocked even after the earlier writer completes.
This is not atomic cross-index remote publication.

The view is empty when the completion probe is not installed, preserving the
standalone reader/executor experiment's bounded committed-lag overlay behavior.
Own uncommitted requirements do not trigger this operational gate; existing
statement-snapshot vector overlay and changed-corpus BM25 rejection still apply.
The manual health veto and pending-batch gate remain additional conditions and
cannot be overridden by successful completion. Health checks retain the existing
[executor boundary](snapshot-executor-spike.md): they do not revoke rows already
buffered by a parent executor node.

## Validation

```sh
./scripts/test-publication-commit.sh
# Reuse an executor image built from this checkout:
./scripts/test-publication-commit.sh --reuse-executor-image
```

PG18.6 / Debian12 / Linux arm64, actual C callbacks/background observer,
psql and Psycopg, logged capture/replay/publication, deterministic remote API
fixture, and local TLS search. Containers use `--network none`. No `.env.local`
values are loaded and no live LambdaDB readiness or latency result is claimed.

The 15 integration tests cover:

- Multiple indexes and multiple rows; one batched document RPC per modality;
  no writer completion after only one index publishes.
- No replay/claim before timeout; committed source and outbox retained; both
  reader and Custom Scan blocked until verified publication.
- Lost write ACK, lost Tag ACK, wrong Tag marker, fresh fenced retry and recovery.
- Verified Tag followed by uncommitted publication/rollback; obsolete receipt
  rejected after replacement publication.
- Lost publication response after commit; durable success still completes the
  writer and a repeated drain makes no additional remote calls.
- Observer termination before shared notification; automatic observer restart
  reconciles durable success while the original writer is still waiting.
- Delayed observer timeout despite successful publication; status distinguishes
  durable completion from missing notification.
- Actual replay process SIGKILL after committed attempt creation; preserved work
  and fresh replay attempt after the original writer has warned.
- Late smaller event IDs and concurrent waiters with separately frozen batch
  membership; another transaction's receipt does not complete the wrong writer.
- Coalesced UPDATE/DELETE/reinsert, savepoint rollback, full rollback, own-write
  vector semantics, restricted BM25, and pre-commit rejection of 2PC.
- Immediate PG shutdown/WAL recovery with partial cross-index publication,
  explicit observer re-registration, and completion of the remaining generation.
- Real psql simple-protocol autocommit and parameterized Psycopg autocommit/Sync,
  plus explicit COMMIT through the other scenarios; permission boundaries.

The existing 20 commit/worker, 18 client-contract, 13 snapshot-reader,
19 snapshot-executor and 9 retention scenarios also pass locally. CI runs the
new suite and retains `publication-commit.log` with its other evidence.

## Open gates

- `RESOURCE_RELEASE_AFTER_LOCKS` is still a feasibility candidate, not an
  approved production hook. Broader resource/callback, cancellation/termination,
  shutdown and extension-composition testing remains necessary. The inherited
  negative `Execute + Flush` and pipeline boundaries remain unsupported promises.
- One manually registered observer per postmaster, one database, 16 waiter slots,
  20 ms polling. Capacity exhaustion fails **before** source commit. No production
  launcher, replay scheduling/backoff, connection backpressure or latency SLO.
- Replay remains a separate synchronous Python adapter. The fixture runs remote
  protocol operations in a deterministic in-process store, and one killed child
  process stops before HTTP. It does not prove process death during a live RPC.
- The trigger captures events only after installation. Tests bootstrap and publish
  their initial generations before enabling it. No online installation/backfill
  or upgrade contract; do not attach it to an existing application cluster.
- Tested completion scope is ordinary captured DML. Retirement events have no
  implemented replay completion; DDL can retain pending requirements and warn.
  Restore/replication fencing and source/index support restrictions are inherited.
- Requirement/audit retention and query cost are unbounded over time. No audit
  compaction, scalable tuple lookup or top-k continuation work is included.
- No live-service evidence for this integrated path, no LambdaDB core changes,
  and no relaxation of the accepted BM25 restriction. The original full
  transaction-search milestone and product integration remain incomplete.

Implementation: [integration SQL](../spikes/publication_commit/setup.sql),
[observer and callback](../spikes/commit_worker/probe.c),
[integration tests](../spikes/publication_commit/test.py),
[runner](../scripts/test-publication-commit.sh).
