# Snapshot-safe published ref retention experiment

This is a separate superuser-only probe, not product GC. It deletes an admitted
published attempt's Tag and Branch through the existing LambdaDB ref API. It
does not delete collections, abandoned attempts, PG source rows, outbox events,
or replay audit history, and does not change LambdaDB core or BM25 semantics.

## Selected deletion boundary

Each successful PG publication now records its top-level `published_xid` in the
same transaction as the published head and exact outbox removal. A cleanup plan
may select one published batch when either:

- A successor has published and the batch is no longer the current head; or
- The generation's retirement event has committed, including after DROP or
  recapture following REINDEX.

In both cases, the superseding publication or retirement XID must be strictly
older than PostgreSQL's conservative non-removable transaction horizon. The C
helper uses `GetOldestNonRemovableTransactionId(NULL)` and converts the result to
`xid8` relative to the next full transaction ID while an active snapshot bounds
the horizon. This follows the [PG18.6 horizon implementation](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/storage/ipc/procarray.c)
and [full-XID conversion contract](https://github.com/postgres/postgres/blob/REL_18_6/src/include/access/transam.h).
It includes registered snapshots and replication-slot retention, rather than
relying on a timeout, backend PID list, or only the collector's own snapshot.
Unrelated long-running transactions/slots may conservatively delay cleanup.

This protects a statement that obtained its snapshot **before selecting its
Tag**, as well as a reader already using that Tag. No reader-side lease expiry
can silently permit deletion during a paused query. Only the existing bounded
primary/READ COMMITTED read paths are supported; no new standby, exported-snapshot,
physical-clone, cursor or cross-cluster contract is established.

The planner locks the generation then target in the replay-compatible order.
It retains any published batch that is the parent of a pending batch. It also
retains a parent when a child has any superseded attempt: an older worker may
still have an in-flight Branch-create request using that parent, even after its
PG connection disappeared or a newer attempt published. Published winning refs
are immutable under the trusted adapter protocol. New claims use the current
head; they cannot create a new dependency on an already admitted old batch.

Abandoned attempts themselves are never candidates. Proving remote quiescence
for ambiguous requests remains open; a local lease or age threshold is not
treated as that proof. As a consequence, failed attempts can retain resources
indefinitely in this probe. Collection-level retirement completion is deferred.

## Durable cleanup and retry

`pgos_retention_probe.plan(generation)` returns at most one batch and commits a
logged cleanup job before HTTP. The trusted Python adapter then:

1. Checks the collection's recorded run owner.
2. Lists both ref kinds and checks the exact attempt name and published snapshot
   ID/time against the Tag and Branch head before deleting either ref.
3. Deletes the Tag, then Branch. Already-absent refs and DELETE 404 can be retried;
   other errors, including alias conflicts, leave the job pending. Aliases are
   never removed or retargeted by this worker.
4. Re-lists both kinds to verify absence, then commits `finish(batch)`.

DELETE success alone is insufficient. A lost response, partial deletion, process
failure, or PG restart leaves a resumable job. Concurrent collectors can repeat
the same fixed-name deletions and idempotent completion. As with replay receipts,
SQL cannot independently attest HTTP observations; `finish` is a private trusted
adapter interface. No PG transaction is held open during remote I/O.

This requires the probe's exclusive collection ownership and never-reused ref
names. The public API supplies no conditional ref-delete token here: ownership
and inventory checks do not solve concurrent external modification or same-name
recreation. Such writers and unfenced physical clones remain unsupported. The
[published OpenAPI](https://docs.lambdadb.ai/reference/api/openapi.json), inspected
on 2026-09-28, documents DELETE 200/404 and alias-conflict 409 for these refs.
No live deletion is performed as part of this credential-free experiment.

SQL batches/events remain intact because the snapshot assembler reconstructs
the effective base through the parent chain. Remote ref cleanup does not remove
the 64-ancestor/4,096-event admission limits. Audit compaction, scalable planning,
automated scheduling, storage reclamation latency and retention SLOs remain open.

## Reproduce and evidence

```sh
./scripts/test-retention.sh
# After rebuilding/testing the snapshot executor from the same sources:
./scripts/test-retention.sh --reuse-executor-image
```

Nine credential-free tests use actual PG transactions, the C snapshot horizon,
Custom Scan and a local TLS search fixture with deterministic ref lifecycle
responses. They cover old statements paused before Tag selection; retained current
and replay-parent refs; abandoned attempts after a winning retry; DROP rollback
and committed retirement; REINDEX replacement isolation; partial deletion and
WAL recovery after immediate shutdown; simulated collector interruption before completion;
concurrent collectors; ownership/snapshot/inventory mismatch; alias conflicts;
false deletion acknowledgements; plan rollback and permissions. Search and replay
continue after eligible old refs are removed, with PG source/audit rows retained.

The 13 snapshot-reader and 19 executor tests also pass with the publication-XID
change. These results are local protocol evidence, not live LambdaDB failure or
physical storage reclamation evidence. CI runs the retention suite and retains
its log alongside the other probe logs.
