# Durable registry and atomic change capture experiment

This separate SQL/PL/pgSQL prototype connects the previous real IAM generation
probe to logged registry/outbox tables. Source writes and their change records
commit or roll back together. It runs in a disposable PG18.6 cluster and is not
installed by the product extension or included in its evaluation bundle.

The confirmed requirements remain PG-owned originals, independent ownership per
index, atomic source/replay capture, post-commit remote synchronization, and
snapshot-correct searches including own writes. This change proves the local
capture part only. No remote resources, credentials, HTTP requests, worker,
commit-response wait, Tag publication, or search overlays are added.

## Selected prototype mechanism

The extension language remains C/PGXS. SQL triggers are used here to make the
transactional capture rules executable and inspectable before choosing a product
catalog/capture implementation. An IAM insert callback alone does not capture
DELETE and can be skipped for HOT updates. An AFTER ROW trigger records the
actual inserted/updated/deleted rows, including HOT and COPY. There is no network
I/O per row; later replay must batch requests using the confirmed LambdaDB
per-request atomicity contract. Trigger overhead and queue throughput are not
benchmarked or accepted product performance.

| Logged relation | Meaning |
| --- | --- |
| `installation` | Capturing database OID and PG system identifier; rejects use after a logical restore to another database |
| `sources` | Random source epoch, heap OID, bigint PK attribute, capturing/retired state |
| `generations` | IAM generation UUID, source epoch, index OID, indexed attribute/mode/dimensions, reserved resource key, capturing/retire_pending state |
| `outbox` | Event ID, writer xid8, source/generation identity, operation, logical key, and projected document |

`capturing` means local changes are being recorded. It does not mean the index
is ready for search. The IAM stays permanently unpublished. A reserved resource
key is a local naming candidate, not proof of a remote collection's existence,
ownership, or endpoint/project binding. Source epochs identify registrations;
they are not a complete physical-clone fencing protocol.

The outbox is an ordinary logged table with no acknowledgement/deletion API.
Its identity sequence and xid8 identify records/transactions, **not commit
order or a publication frontier**. No worker scans `event_id > last_seen`.
The test commits a transaction with larger event IDs first, then verifies that
the smaller IDs from the late commit remain visible in the pending relation.
This demonstrates why a high-water cursor is unsafe; it does not implement an
ordered replay algorithm, transaction batching, or an atomic worker claim.

## Registration and capture

After creating the lifecycle probe's indexes:

```sql
SELECT pgos_capture_probe.register_index('docs_text'::regclass);
SELECT pgos_capture_probe.register_index('docs_vector'::regclass);

BEGIN;
UPDATE docs SET content = 'changed', id = 42 WHERE id = 1;
-- Original changes and generation-specific events exist in this transaction.
-- Other connections see neither until commit.
COMMIT;
```

Registration takes the heap's AccessExclusiveLock before index locks and keeps
it to transaction end. It validates the source definition, installs ALWAYS
statement/row triggers once, writes a create event, and captures non-NULL initial
values with a set-based INSERT. Writers before registration must finish before
the seed snapshot; writers after registration use the triggers. Re-registering
the same capturing generation is idempotent and does not duplicate seed events.
Registration, trigger creation, and seed records roll back together.

Each inserted/updated non-NULL indexed value yields a projected upsert for its
generation: a string `id` plus `content` or a numeric `embedding` array. NULL
values yield delete records, since they are excluded from that remote search
representation. DELETE records the old key. A PK change records old-key delete
before new-key upsert/delete for each index. Unchanged indexed fields may produce
redundant upserts on HOT updates; no change-deduplication optimization is claimed.
The whole SQL transaction remains atomic across all target generations. An
injected failure on the second generation rolls back the first event and source
mutation as well. That does not prove cross-collection remote atomicity.

The bounded context is superuser, READ COMMITTED, origin replication role,
permanent ordinary non-inherited/non-partitioned heap, no RLS, and an immediate
single bigint PK. Statement guards recheck the definition and index generations
even for zero-row statements. Column renames use stable attribute numbers.
ALWAYS triggers reject replica-role writes instead of silently missing capture.
Superuser disabling/replacing triggers, editing private catalogs, or bypassing
event triggers remains outside this trusted test setup. There is no product
privilege model, general DDL enforcement, or two-phase-commit acceptance claim.

## Rebuild, retirement, and restore boundaries

Blocking REINDEX changes the physical generation. DML then fails with `55000`
until `register_index` explicitly binds the new generation. That registration
atomically marks the prior generation retire_pending, appends retirement work,
and seeds the replacement. Prefer REINDEX plus registration in one transaction;
both physical replacement and registry changes roll back on failure.

A `sql_drop` event trigger retains generation/source rows and prior outbox
records when an index or source table is dropped, and appends idempotent
retirement events. Dropping one index leaves the sibling capturing. Dropping
the source retires its remaining generations. There is no foreign-key cascade
from pg_class that could erase remote cleanup obligations. Actual remote cleanup
and its ownership/retry protocol remain unimplemented.

TRUNCATE is deliberately rejected on registered sources, even though the earlier
standalone IAM probe can rebuild an empty generation: deleting all source rows
needs a replay/reset contract. VACUUM FULL and other physical generation changes
likewise need explicit re-registration before further capture. Capture triggers
remain attached after the last index is dropped; a production detach/uninstall
procedure remains open. Private registry helpers are not a public lifecycle API.

Crash recovery is tested after CHECKPOINT, a committed source/outbox change, and
an uncommitted change, using immediate stop and restart. Only the committed pair
survives. Logical dump/restore preserves pending events and retirement records
as ordinary table data. The copied installation stamp refuses capture in the
new database; no automatic rebind/replay or backlog disposal is performed.
Physical clones/PITR can retain both system identifier and database OID, so this
stamp is **not** sufficient physical-restore fencing. Product extension-member
catalog dump/migration policy and a safe restore/reseed procedure remain gates.

## Validation and next step

```sh
./scripts/test-change-capture.sh
```

This builds the lifecycle/client dependencies and runs 14 tests without external
network. CI uses `--reuse-lifecycle-image` after the lifecycle step and records
`change-capture.log` with the source revision. Tests cover seed/idempotency and
registration locking, DML/COPY/ON CONFLICT/PK/NULL behavior, a measured HOT update,
savepoint/transaction/deferred-constraint rollback, injected outbox failure,
committed visibility and late smaller IDs, rebuild/drop retirement, guard failures,
crash recovery, and logical-restore refusal with backlog preservation.

Next define fenced claiming/replay of committed transaction batches and bind it
to generation ownership, then connect batched writes, the verified indexed-marker
barrier, immutable Tags, and atomic PG publication/eligible outbox deletion.
No retention/backpressure policy, worker retries, publication coverage, source row
version/TID mapping, BM25 overlay statistics, or strict-read acceptance is proven
by these local tests.

References: PostgreSQL [trigger transactions and HOT-independent row events](https://www.postgresql.org/docs/18/trigger-definition.html),
[trigger records](https://www.postgresql.org/docs/18/plpgsql-trigger.html), and
[transactional drop events](https://www.postgresql.org/docs/18/event-trigger-definition.html).
