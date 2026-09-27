# Index lifecycle experiment

This separate C/PGXS module exercises real PostgreSQL 18.6 index access method
callbacks, index files, and transactional DDL. It stores one local metadata page
per index and never returns search results. It is excluded from the product
extension and evaluation bundle. No credentials, network, remote collections,
publication worker, or preload are required.

## Requirement versus experiment

The confirmed requirement is independent remote ownership per PG index, with PG
originals retained through build/rebuild/drop. Source changes and durable replay
must commit together; strict search and own-write semantics remain mandatory.

This experiment selects a random UUIDv4 **generation** per physical index build,
stored in the index's block 0 via Generic WAL. PG owns transactional relation
creation, replacement, and deletion. The page has a magic/version header, heap
OID, PK/value attribute numbers, mode, and vector dimensions. The status reader
checks the page format and heap binding and retains an index AccessShareLock to
transaction end. The on-disk struct is a native-layout, target-specific probe
format, not a portable or supported product storage ABI.

An index OID identifies the current PG object within a database. The generation
UUID identifies this physical build. Neither is a source-clone fencing token,
remote record identity, durable row version, or complete remote ownership record.
`state=unpublished` is a permanent probe state; there is no activation operation.

| Operation | Expected local generation behavior |
| --- | --- |
| Separate indexes, including two on the same column | Distinct generations |
| Index/column rename, ordinary DML, ordinary VACUUM | Generation unchanged |
| Successful blocking REINDEX, TRUNCATE, VACUUM FULL | New generation |
| Rolled-back CREATE INDEX | No surviving index |
| Rolled-back REINDEX, DROP INDEX, TRUNCATE | Prior index/generation restored |
| DROP INDEX | Only that local index is removed; original rows and sibling indexes remain |
| Crash after committed rebuild | Committed generation recovered from WAL |
| Crash with uncommitted rebuild/source mutation | Prior generation and source rows recovered |
| Logical pg_dump/restore | Source values preserved; CREATE INDEX allocates new generations |

Physical backup/PITR copies these identities. It does **not** create a new source
epoch or prevent the original and restored database from sharing a remote target.
That fencing protocol remains required before remote replay is enabled.

## Test-only SQL

The setup installs `pgos_lifecycle` and two non-default operator classes in a
restricted schema. This is not the proposed product `onesearch` access method.

```sql
CREATE TABLE docs (
    id bigint PRIMARY KEY,
    content text,
    embedding onesearch.vector(3)
);
CREATE INDEX docs_vector ON docs USING pgos_lifecycle
    (embedding pgos_lifecycle_probe.vector_ops);
CREATE INDEX docs_text ON docs USING pgos_lifecycle
    (content pgos_lifecycle_probe.text_ops);
SELECT pgos_lifecycle_probe.info('docs_vector'::regclass);
REINDEX INDEX docs_vector;
DROP INDEX docs_vector; -- source columns and docs_text remain
```

Builds require a permanent ordinary heap, a nondeferrable single bigint primary
key, and one plain text or dimension-constrained vector column. NULLs are allowed;
non-null zero vectors fail build and non-HOT index insertion through the existing
cosine validator. Expression/partial/multicolumn/INCLUDE/unique indexes, RLS,
partition children, temporary/unlogged tables, and index options are unsupported.
This is build-time shape validation, not a persistent guard against every later
schema or security change. Test operator-class definitions and superuser setup
are trusted; third-party operator classes are not a supported extension point.

CONCURRENTLY builds/rebuilds are rejected in `ambuild`. PG can retain an invalid
index shell after such an error. The test removes those specific invalid objects
and verifies the original index survives a rejected concurrent rebuild. This is
not a promise of automatic concurrent-DDL cleanup.

The vector class registers the existing distance operator for ORDER BY. The text
class uses ordinary equality only to exercise a real planner strategy; it is
**not BM25**. Both index paths always fail with `55000` at scan initialization,
including empty indexes. Large placeholder costs favor ordinary local plans but
do not serve as the correctness guard. Tests force both actual Index Scan plans
and verify failure, so an empty physical index cannot silently return zero hits.
No remote query or fixed-fixture Custom Scan is connected to these indexes.

The insert callback validates vector values but stores no TIDs or outbox records;
VACUUM callbacks have no TIDs to retire. Neither proves mutation capture, HOT
coverage, remote maintenance, or PK/version mapping. Indexed data remains wholly
in PG and the probe stays unusable for search after every mutation.

## Reproduction and evidence

```sh
./scripts/test-index-lifecycle.sh
```

This builds the pinned PG18.6 / Debian12 / arm64 client test image, compiles and
installs the separate module, and runs 11 lifecycle tests in a disposable cluster
with container networking disabled. CI reuses its preceding client image with
`--reuse-client-image` and retains `index-lifecycle.log`, including identity,
EXPLAIN, recovery, and restore evidence, with the tested source revision.

The suite covers the table above, failed zero-vector builds, DML validation,
superuser enforcement even after EXECUTE is granted, unsupported definitions,
concurrent-DDL rejection, and metadata-reader/rebuild lock conflicts. Recovery
uses CHECKPOINT followed by committed and uncommitted rebuilds, immediate server
stop, and restart. This is one controlled crash schedule, not exhaustive storage
fault testing. Logical restore runs into a separate database and compares source
rows and both index generations.

## Next integration gates

The later [capture experiment](change-capture-spike.md) adds logged source/index
records, initial seed capture, DML/PK/HOT capture, and durable retirement work.
Explicit re-registration binds a rebuilt generation; it does not automatically
activate rebuilt indexes or solve replay ordering. Tie generation replacement
to fenced remote creation/retirement before enabling remote DDL.
Then integrate fenced replay, verified Tags and PG publication, and the executor's
snapshot/own-write contract. BM25 corpus overlays and ranked continuation remain
separate required gates. Drop/restore must never erase pending cleanup obligations
or authorize a cloned database to mutate the original's remote resources.

References: PostgreSQL [index AM interface](https://www.postgresql.org/docs/18/indexam.html),
[Generic WAL](https://www.postgresql.org/docs/18/generic-wal.html), and
[index locking](https://www.postgresql.org/docs/18/index-locking.html).
