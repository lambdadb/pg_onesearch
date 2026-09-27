# Fenced batch replay and publication experiment

This separate, superuser-only SQL/Python experiment joins the lifecycle and
change-capture probes to LambdaDB batched writes. It is not installed by
`CREATE EXTENSION pg_onesearch`, does not activate an index, and does not connect
the Custom Scan or commit-wait probes. The product remains the local C/PGXS
vector skeleton on PostgreSQL 18.6 / Debian 12 / arm64.

## Requirements, chosen approach, and open decisions

**Confirmed requirements:** PG source/outbox commit atomically; replay consumes
committed work; multiple documents should share a request; each LambdaDB
upsert/update/delete request is atomic; serially acknowledged writes are applied
in order. A Tag currently captures S3-indexed state, so write acknowledgement is
insufficient. A final unique marker must be visible with `consistentRead=false`
before creating a Tag, then the exact marker must be verified from that Tag.
The request atomicity and ordering statements are maintainer-confirmed assumptions,
not independently proven service internals.

**Selected for this probe:** one synchronous Python worker call per generation,
invoker SQL functions, READ COMMITTED, ordinary permanent heaps with immediate
single-bigint primary keys, dedicated fresh owned collections, one immutable
pending batch, and a unique Branch/Tag per attempt. Retry means a fresh attempt,
not replaying writes into a shared mutable Branch. These are experiment choices,
not the final product worker language or scheduling/retention contract.

**Open:** production C/background-worker adoption, oversized backlog draining,
transaction-aware pagination, backpressure, worker scheduling, retirement and
remote garbage collection, physical-clone fencing, credential rotation, immutable
row versions/TIDs, strict PG snapshot and own-write reads, BM25 overlay statistics,
and commit-wait integration. Publication is per generation; it is not atomic
across a source transaction's multiple indexes.

## Protocol and correctness boundary

1. `bind` associates a registered physical generation with one collection and run
   owner. Rebinding is rejected. Every claim, attempt, and publication checks the
   capture installation fence, source definition, active generation, and physical
   IAM metadata. Heap/index locks precede registry and target-row locks.
2. `claim` locks the generation's target row. It returns an existing pending batch
   or freezes **all** currently visible outbox events in one statement snapshot.
   The probe rejects more than 1,000 events or 8 MiB of uncompressed JSON text plus
   per-event overhead, and rejects the caller's own uncommitted events. Oversized
   backlogs fail closed; no partial transaction is silently claimed. The limit is
   a bounded-fixture admission rule, not a streaming memory or throughput promise.
3. Claim membership is logged and must commit before `begin_attempt`. Each attempt
   gets a new UUID and must also commit before publication. Concurrent claimers
   serialize; a newer attempt replaces the batch's active nonce. A failed or
   abandoned batch retains its exact membership while newer events remain pending.
4. Reduce the frozen events to the last operation per key. For this restricted
   schema, row and immediate primary-key locks serialize conflicting same-key
   writes before their AFTER ROW events are allocated. Different keys commute.
   Event IDs are **never a global commit-order cursor**: a smaller ID can become
   visible after a larger ID has already been published.
5. The adapter checks collection ownership and creates `attempt-<uuid>` Branch.
   Initial replay requires an empty `main` snapshot in a fresh, exclusive fixture.
   Later replay branches from the previous published attempt Branch with
   `asOf=published Tag.snapshotCommittedAt`. The new Branch's `parentSnapshot`
   **and** `headSnapshot` must equal the published snapshot ID/time before writes.
   The [published OpenAPI](https://docs.lambdadb.ai/reference/api/openapi.json)
   accepts only a Branch as a Branch-creation source, not a Tag. The previous
   published Branch is retained and never written again by this worker. Expired
   history, a conflicting snapshot, or any ambiguous Branch-create response fails
   the attempt; the worker never adopts an existing Branch by name.
6. Group final deletes and upserts into requests using the actual UTF-8 body size,
   including the attempt Branch name, with the existing 4 MiB transport ceiling.
   Acknowledge each request serially, then write one reserved marker with the
   attempt UUID. Poll fetch-by-ID with `consistentRead=false`; create a Tag from
   that Branch; verify exact marker equality from the immutable Tag. The marker's
   ID cannot collide with signed bigint document IDs. It has no indexed text/vector.
7. `publish` accepts the trusted adapter's verified snapshot receipt only for the
   active, committed attempt and unchanged parent batch. It atomically updates
   the PG publication and deletes **only exact covered event IDs and payloads**.
   It checks deletion cardinality and retains frozen events/attempts for audit.
   A duplicate identical receipt is harmless; changed or stale receipts fail.
   SQL cannot independently attest HTTP verification; this is a private trusted
   adapter boundary, not a public API accepting arbitrary receipts.

No remote I/O occurs while holding a PG transaction open. Losing a write or Tag
response, dying after Tag verification, or losing an uncommitted PG publication
leaves the frozen batch recoverable. Retrying creates another Branch from the
same published base. A stale worker can finish only its own abandoned Branch;
it cannot write into the winner's Branch, and its publication nonce is rejected.
This addresses in-flight HTTP requests that a local lease alone cannot revoke.
There is no automatic cleanup of abandoned or old versions yet.

The live harness has exclusive ownership of new temporary collections. The
installation stamp rejects logical restore to another database; an unfenced
physical clone and external writers to these Branches remain unsupported.
REINDEX or DROP during remote work prevents publication and retains retirement
obligations. This does not implement replacement-index activation or cleanup.

## Validation

```sh
./scripts/test-batch-replay.sh
# After already testing/building capture:
./scripts/test-batch-replay.sh --reuse-capture-image
```

The credential-free suite uses real PG transactions, lifecycle indexes and WAL
recovery, with a deterministic REST fixture for remote failure injection. It
covers initial/delta coalescing and batched requests; late smaller IDs; actual
same-key lock waits; concurrent claims; frozen membership; stale workers; lost
write/Tag acknowledgement; a failure after the second data RPC; death after Tag
verification; wrong parent/marker; transactional and crash rollback of publication;
idempotent receipts; committed-protocol and size guards; coverage corruption;
REINDEX/DROP; access controls; and UTF-8 request budgeting. Mock successes prove
the adapter/state-machine behavior, not deployed service behavior.

Opt-in live validation creates two fresh owned collections, then drives real
source DML through capture and replay for vector and BM25. It compares initial
and changed Tag payloads against controlled PG projections, checks old Tags,
parent snapshot identity, grouped requests, and exact PG publication coverage.

```sh
python3 spikes/batch_replay/run_live.py \
  --env-file /absolute/path/to/.env.local \
  --report /absolute/path/to/new-report.json
```

The host verifies source hashes against the pinned container image, pipes settings
on stdin, and saves a sanitized report. Cleanup discovers versions even if the
worker failed before returning its attempt records, verifies collection ownership,
deletes fixture Tags/Branches, and confirms collection absence. No credentials,
server response bodies, or signed download URLs are written to evidence. The
live outcome will be recorded separately after execution; no production runtime
or server deployment revision is pinned by this harness.

The next integration gate is a PG snapshot-bound publication/delta reader with
stable row-version identity and own writes. The real executor and commit-response
probes can then consume that state without treating an ACK or a published Tag
alone as proof of a healthy, snapshot-correct search.
