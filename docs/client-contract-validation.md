# Actual-client experiment — 2026-09-26

## Reproduce and boundary of evidence

```sh
./scripts/test-client-contract.sh
```

This builds the product skeleton, then a separate test image with hash-pinned
Psycopg 3.3.6 and typing-extensions 4.16.0. Psycopg uses its pure Python
implementation with the image's system libpq. Observed target: PostgreSQL/psql
18.6 (`18.6-1.pgdg12+2`), libpq `180006`, Python 3.11.2, Debian 12 Linux arm64.
No driver package is added to the product image or evaluation bundle.

The runner reuses the [commit-worker fixture](commit-worker-spike.md): a
separate preload module, disposable cluster, Unix sockets, no container network,
and fsync/synchronous_commit/full_page_writes enabled. An independent connection
checks committed source/outbox/receipt state; worker pause barriers distinguish
statement completion, local commit, and publication. A receipt is a local mock
publication, **not LambdaDB readiness**. The only shared runner change permits
an alternate Python interpreter/test path; the original raw-protocol suite
remains the default.

## Observed results

All 18 scenarios below passed locally. CI runs the same script and retains its
client/version output and full log alongside the evaluation bundle; see the PR's
run for that revision's CI result. Test code:
[`spikes/client_contract/test.py`](../spikes/client_contract/test.py).

| # | Actual call / condition | Observed result |
| --- | --- | --- |
| 1 | Default Psycopg `execute()`, then `commit()` | Execute returns INSERT rowcount 1 in INTRANS; observer sees no source. Commit blocks after source/outbox become visible, then returns after receipt publication. |
| 2 | Parameterized synchronous autocommit execute | Blocks at publication wait; returns INSERT rowcount 1 and IDLE after receipt. |
| 3 | Outer `conn.transaction()` | Context exit waits for publication and reaches IDLE. |
| 4 | Explicit commit timeout | Returns None; notice callback already received 01000. Source/outbox retained; later rollback cannot undo commit. |
| 5 | Autocommit timeout | Returns INSERT rowcount 1, IDLE, and notice; no exception. |
| 6 | Mock worker failure | Commit returns with warning; replay survives and later drains. |
| 7 | No notice handler | Normal return despite no receipt; no synchronization exception. |
| 8 | `cancel_safe()` during pre-commit pg_sleep | QueryCanceled 57014, INERROR; rollback removes source and capture. |
| 9 | `cancel_safe()` during post-commit wait | Commit returns None with interrupted-sync warning, IDLE; source/outbox survive and connection remains usable. |
| 10 | Nested transaction context rolled back | Savepoint removes child capture; only outer source/receipt survives. |
| 11 | COPY inside explicit transaction | COPY completion is still INTRANS; outer commit waits for receipt. |
| 12 | Deferred constraint trigger fails on commit | CheckViolation 23514; source/capture roll back; no sync warning. |
| 13 | Reused prepared INSERT, healthy then paused worker | First publishes; second warns and leaves replay. This is not a remote-search cached-plan health test. |
| 14 | Bound vector strings, prepared parameters, binary results | Text/cosine values correct; unknown-type binary result is expected bytes. Bad dimensions/non-finite input raise DataError 22000; no capture side effects. No custom vector adapter is registered. |
| 15 | psql explicit COMMIT with ON_ERROR_STOP | Exit 0, COMMIT output, WARNING on stderr; replay retained. |
| 16 | psql autocommit with ON_ERROR_STOP | Exit 0 plus WARNING; replay retained. |
| 17 | Psycopg pipeline execute, then explicit sync | Execute returns before commit; sync receives eventual warning; source/replay committed. Negative boundary test, not a supported-pipeline promise. |
| 18 | Earlier SQL error caught, then `commit()` | INERROR becomes IDLE; commit returns None while PG rolls back source/capture. Normal return alone does not prove source commit. |

Warning handlers copy diagnostics during the callback. Timeouts are bounded
fixture controls; normal publication tests use worker barriers, not assumptions
about how long a network or remote service should take. Expected warnings/errors
in the server log belong to the assertions above.

## What remains unproven

- The product still contains no synchronization hooks. These results do not
  establish production safety for waiting during resource cleanup, all PG paths,
  shutdown, other extensions, advisory locks, or arbitrary worker dependencies.
- No remote vector/BM25 query, actual ready Tag, overlay scoring, publication
  fencing, or affected-index execution guard is tested here.
- Other driver implementations/versions, async APIs, pools/proxies, ORMs,
  notice suppression, multi-statement autocommit batches, and broad pipeline
  behavior are outside this matrix. The raw suite separately covers selected
  disconnect/crash and extended-protocol cases.
- A committed warning is not a failed mutation. Driver APIs may hide warnings;
  an execution guard plus durable status/reconciliation contract is still needed.

The [SQL/client proposal](sql-client-contract.md) separates these observations
from proposed support scope and unresolved product decisions.
