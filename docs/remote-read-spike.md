# C remote read transport experiment

This separate PG18.6 module calls LambdaDB from C through libcurl. It implements
a bounded read transport candidate, not a PostgreSQL remote index or the first
end-to-end search milestone. `CREATE EXTENSION pg_onesearch`, its Makefile, and
the evaluation bundle do not install this module or acquire a libcurl dependency.
Both vector and BM25 remain required for the later index/executor milestone.

## Decisions for this experiment

- Keep C/PGXS and PostgreSQL 18.6 / Debian 12 / arm64. Select libcurl 7.88.1
  (Debian package `7.88.1-10+deb12u15`) for this candidate; production dependency
  and distribution support remain open.
- Separate `http.c` (HTTPS transport) from `probe.c` (LambdaDB request/envelope
  handling). No ZomboDB source was copied. Its old C version informed connection
  lifetime, interrupt handling, and error cleanup; its current Rust thread and
  Elasticsearch refresh/scroll designs are not imported.
- A lazily created multi handle owns a backend-local connection cache capped at
  two idle connections. Each call creates one easy handle and releases it and
  its headers/body on success or PG ERROR. There are no concurrent requests,
  PG calls from worker threads, transaction callbacks, or commit-phase HTTP.
- Poll curl at most 100 ms between PostgreSQL interrupt checks, outside curl
  callbacks. Require a libcurl build with asynchronous DNS. Use a 5-second total
  deadline by default and a connection deadline of at most 3 seconds.
  `pgos_remote_probe.timeout_ms` is superuser-only, range 100..60000; live tests
  explicitly use 45000. These are experiment limits, not product latency SLOs.
- HTTPS certificate and hostname verification are mandatory. Do not inherit
  proxy/netrc settings, follow redirects, or provide application retries. Request
  bodies use a read callback with a seek callback that refuses rewind: libcurl
  otherwise replayed a POST after an empty response on a reused connection in
  the fault fixture. This is not a mutation idempotency protocol.
- Bound serialized requests at 1 MiB and decompressed responses at 8 MiB.
  Receive callbacks use bounded malloc/realloc and return errors to curl; they
  never raise PostgreSQL errors through curl's stack. HTTP status/transport errors
  omit remote URLs, bodies, headers, and keys. JSON parsing uses PG soft errors
  so malformed response tokens do not appear in diagnostics.

## Test-only SQL interface

The separate setup script creates a restricted schema and function:

```sql
SELECT pgos_remote_probe.query(
    'owned-test-collection', 'checkpoint-c-read',
    '{"knn":{"field":"embedding","queryVector":[1,0,0],"k":10}}'::jsonb,
    10
);
```

It returns a JSONB query envelope, not heap tuples/TIDs or the proposed BM25
match/score interface. Calls always POST to `/query` with an immutable Tag ref,
`includeVectors=true`, and size 1..100. The query argument must be an object;
LambdaDB validates its query DSL. Resource names are restricted to 1..128 ASCII
letters, digits, underscores, or hyphens. This is a deliberately narrower probe
contract, not a statement of all names accepted by the service.

The adapter validates JSON and the `isDocsInline`/`docs` envelope shape. It does
not yet validate every document identity/score or establish complete ranked
coverage. Offloaded results raise `0A000` explicitly, without fetching their URL
or returning partial data. The existing Python harness supports downloads;
porting that path with isolated credentials is a separate gate.

Only UTF8 databases and superuser calls are supported; a C-level privilege check
still applies if someone grants SQL EXECUTE. Connection values come from the
isolated postmaster's environment, not SQL arguments/GUCs. This is test credential
plumbing, not an approved production role/secret-management model.

## Reproduce

```sh
./scripts/test-remote-read.sh
python3 spikes/remote_read/run_live.py \
  --env-file /absolute/path/to/.env.local \
  --report artifacts/remote-read-live.json
```

The first command builds the test image and runs without container networking;
a local HTTPS fixture and PostgreSQL communicate over loopback. It generates
ephemeral certificates, installs the separate module, and uses hash-pinned
Psycopg from the existing client test image. CI runs this credential-free suite.

The second command is opt-in and requires the image from the first. It verifies
the image's probe source hashes against the worktree, pins the image ID for the
run, and creates two randomly named collections containing five synthetic docs.
It reuses the existing harness's ownership checks, atomic batched writes, exact
final-marker polling with `consistentRead=false`, Tag verification, and cleanup.
The C module itself sends only read queries. Writes/resource administration remain
in the host Python fixture.

Live settings enter the container through stdin, then the isolated postmaster's
environment. They are not in Docker configuration, command arguments, image
layers, or SQL. Host-local admins and container processes can still inspect that
environment; do not use this mechanism as a production secret store. The runner
records sanitized checks and per-file hashes and removes the container/resources
on normal completion/failure. Forced termination or a Docker/service outage can
leave resources; inspect the report's ownership records before recovery.

## Evidence and limits

The local TLS suite passes 16 tests covering connection reuse; Tag body/auth;
401/429/500, redirects, disconnects and truncated responses without duplicate
POSTs; malformed JSON/UTF8/NUL/envelopes with redacted errors; explicit offload
rejection; gzip and size/expansion limits; total/statement deadlines; explicit
cancel and savepoint recovery; backend termination; repeated-error socket counts;
input/role checks; certificate/hostname rejection; and server-log redaction.

These bounded tests do not prove cancellation latency for every DNS/TLS/library
failure or complete absence of native-memory leaks. They do not establish a
production connection pool or cross-backend credential isolation.

The [live evidence record](evidence/remote-read-2026-09-27.json) records a passed
run on September 27, 2026, 09:22:36–09:24:42 UTC. Both C query executions per
modality matched the Python reference's IDs and scores on the same immutable
Tag: five vector hits and three BM25 hits. The marker-to-verified-Tag waits were
64.736 and 58.134 seconds, respectively; these are fixture observations, not an
SLO. Both collections and their Tags were removed and collection absence was
confirmed by the existing cleanup procedure.

The run began with uncommitted code on `b927a30`; every recorded probe source
hash was subsequently verified against commit `b10ea05` and the test image.
The evidence preserves that distinction and does not pin a backend deployment
revision. The existing local vector regression, 33 protocol cases, dump/restore,
and clean evaluation-bundle installation also passed.

Planner hooks,
IAM/CustomScan selection, execution-local BM25 score binding, PG row visibility,
own writes, response downloads, ranked continuation, writes/outbox/fencing, and
Tag publication are still unimplemented by this candidate. In particular, a
prepared SQL function call does not prove index rescan or cached-plan safety.

## Source references

- ZomboDB historical C [connection lifetime](https://github.com/zombodb/zombodb/blob/07f850fa7c308f40e9eb1206fc2255bd48384e5f/src/c/rest/curl_support.c)
  and [request/interrupt handling](https://github.com/zombodb/zombodb/blob/07f850fa7c308f40e9eb1206fc2255bd48384e5f/src/c/rest/rest.c).
- libcurl [multi polling](https://curl.se/libcurl/c/curl_multi_poll.html),
  [receive callback](https://curl.se/libcurl/c/CURLOPT_WRITEFUNCTION.html), and
  [seek/replay callback](https://curl.se/libcurl/c/CURLOPT_SEEKFUNCTION.html).
- [Existing live-service evidence and owner-confirmed guarantees](live-compatibility.md).
