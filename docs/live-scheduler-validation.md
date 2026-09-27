# Live automatic replay and commit completion

This opt-in harness connects the serial Python scheduler, real LambdaDB writes
and immutable Tags, the C publication observer/commit callback, and actual SQL
Custom Scans. It extends the offline [scheduler experiment](replay-scheduler-spike.md)
without activating any product extension feature.

## Reproduce

Build the images and run credential-free checks first:

```sh
./scripts/test-replay-scheduler.sh
python3 -m unittest discover -s spikes/snapshot_read -p test_cleanup.py -v
```

Then explicitly supply local credentials and a new report path:

```sh
python3 spikes/replay_scheduler/run_live.py \
  --env-file /absolute/path/to/.env.local \
  --report artifacts/scheduler-live.json
```

The host verifies that the image's relevant source files match the checkout,
records the Git revision, dirty state, image ID and source hashes, creates one
owned vector collection and one owned BM25 collection, and passes credentials
to the container over stdin. Credentials are not Docker configuration, SQL,
command arguments or report fields. The live container needs network access;
CI uses only local fixtures with `--network none`.

The shared snapshot harness first removes the worker container and independently
confirms its absence. Only then does it inspect the collections' run ownership
markers, discover owned attempt Tags/Branches, delete them and delete the
collections, verifying absence. If container absence cannot be established,
remote cleanup is deferred and the report fails. This is fixture cleanup, not a
production retention collector or proof of physical storage reclamation.

## Scenarios and measurements

- Initial capture registration and binding, followed by automatic bootstrap
  publication for both indexes. Bootstrap runs before the scheduler starts and
  uses a 10 ms probe wait; these setup warnings are not application latency data.
- Own-write vector overlay and rejection of changed-corpus BM25, followed by
  savepoint rollback.
- One source transaction updates two rows in both indexes with the scheduler
  running. The inherited wait is set to its current 10-second maximum. Record
  COMMIT response latency, callback outcome, warning SQLSTATE and durable
  completion checked after the response; then wait for full publication.
- Stop the scheduler, commit another two-row/two-index transaction with a 250 ms
  wait, and require a `01000` warning, retained source/outbox and blocked SQL
  searches for both pending indexes.
- Restart the scheduler and locally discard exactly one successful live upsert
  response for the vector collection. This simulates an ambiguous client ACK
  after the service accepted a real write; it is not a service outage or a
  network fault injected into LambdaDB. Verify a fresh fenced attempt, complete
  transaction coverage and automatic search recovery.
- At initial, normal-update and recovered phases, require actual Custom Scan
  plans with the current published Tag. Compare vector SQL rows/distances with
  PostgreSQL's exact local calculation and BM25 membership/scores with a direct
  query against that same live Tag.

The report includes per-phase timing, commit outcomes, source transaction
coverage, scheduler phase/attempt events, document batch sizes, marker polling
and Tag identities, SQL results/plans, and owned-resource cleanup. Publication
timing begins before COMMIT and includes the bounded response wait; per-attempt
marker timing covers the existing adapter's indexed-marker polling. Polling
adds measurement granularity; these tiny serial fixtures are not latency SLOs.

A passed run can include a timeout warning even with a functioning remote
service. It means the source was committed and publication later completed as
verified, not that the healthy no-warning commit contract was achieved within
10 seconds. If live commits exceed the cap, increasing a commit-cleanup wait is
not automatically an approved remedy. Callback safety, wait policy and worker
concurrency need a separate decision based on the measured result.

## Recorded live run — 2026-09-28 KST

[Sanitized evidence](evidence/scheduler-live-2026-09-28.json) records source
`9cc681a8373fdc0ea4fca955a4962ce123fd754f`, a clean checkout at launch, matching
image/source hashes, and the image ID. The run took place at
2026-09-27 16:40:30–16:46:48 UTC (September 28 in Korea). Afterward, the recorded
hashes were compared with the runtime source again and the report was checked
for local connection-setting values before committing it.

| Observation | Result |
| --- | --- |
| Automatic bootstrap, two indexes | 133.697 seconds |
| Normal two-row/two-index COMMIT response, 10-second cap | 10.003 seconds; `01000`, callback `timeout`; publication incomplete at the subsequent status check |
| Full publication of that transaction | 120.347 seconds from COMMIT start |
| Paused-scheduler COMMIT response, 250 ms cap | 0.252 seconds; `01000`; source and four indexed events retained; both searches blocked |
| Restart plus locally discarded live upsert ACK | Full publication at 119.320 seconds from paused COMMIT start; two distinct vector attempts |
| Six successful attempt marker intervals | 54.387–69.724 seconds, 27–34 polls each |
| SQL validation | Six actual Custom Scan plans/results: two modalities at initial, updated and recovered phases |
| Final writer coverage | Two required events and two published events for each of two generations |
| Cleanup | Worker container and both owned collections confirmed absent; no version-cleanup errors |

Every recorded successful document upsert used a two-document batch. The failed
client call was a real accepted upsert whose response was discarded locally;
it is recorded in `faults`, and is not counted as an acknowledged entry in the
adapter's `writes` array. No single-document source replay was introduced.

This run passed automatic publication, warning/data preservation, exact recovery,
SQL result/score comparison and cleanup checks. **It did not demonstrate a
healthy no-warning source COMMIT within the current 10-second cap.** That path
passes only in the offline fixture so far. Each measured indexed-marker interval
alone exceeded the cap; the serial two-index run also includes their combined
cost. These are observations from one small run, not an attribution to a fixed
service-wide timer or an SLO.

The accepted healthy-commit requirement remains unchanged. The mismatch between
that requirement, the current wait cap and observed publication time is an open
product/operational gate. Do not silently extend commit-cleanup waits or change
the completion contract based on this report. Define the acceptable wait and
completion policy before production adoption.

## Offline and live evidence boundaries

The credential-free runner also executes this entire scenario with deterministic
API/TLS fixtures. It asserts both the no-warning completion path and the paused
warning/recovery path, two-index coverage, actual Custom Scans, and the injected
lost-response retry. The shared cleanup suite covers the scheduler variant's
container-absence gate. CI does not use `.env.local` or contact LambdaDB.

The live scenario restarts the scheduler thread/connection gracefully; process
SIGKILL and PG crash/restart remain evidence from the offline scheduler suite.
The live endpoint's server build is not pinned. No general BM25 overlay,
production launcher, automatic reconnect, callback adoption, scale or release
claim follows from this harness.

Implementation: [live scenario](../spikes/replay_scheduler/live.py),
[host runner](../spikes/replay_scheduler/run_live.py),
[shared cleanup gate](../spikes/snapshot_read/run_live.py),
[offline scenario](../spikes/replay_scheduler/live_fixture.py).
