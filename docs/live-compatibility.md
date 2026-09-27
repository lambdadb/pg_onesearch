# Live LambdaDB compatibility experiment

This opt-in REST harness checks the remote behaviors needed before implementing
PostgreSQL index execution. It does not install a remote PostgreSQL access method,
implement PG snapshot overlays, or prove production transaction guarantees.
[SQL/client contract](sql-client-contract.md) remains a proposal.

## Run

Use Python 3.10+ and a dedicated test project. No third-party Python package is
required. Copy [`.env.example`](../.env.example) to an ignored `.env.local`, or
supply the three variables through the process environment. The base URL must be
an HTTPS origin; the project name is supplied separately. The dotenv reader
accepts literal assignments and quoted values, without shell execution or
variable interpolation. An explicitly supplied file is authoritative.

```sh
./scripts/test-live-compatibility.sh \
  --env-file /absolute/path/to/.env.local \
  --report artifacts/live-compatibility.json
```

The report path must not already exist. A separate worktree can read the main
checkout's ignored file by absolute path; do not copy secrets into the worktree,
Docker image, PR, or CI. Live execution is manual and creates two randomly named
collections with independent vector and BM25 indexes. It stores roughly 9 MB of
synthetic data to exercise downloaded results and sends a bounded set of API
calls. This incurs ordinary service usage. Credential-free CI runs only the
harness safety tests plus the existing PG suites.

Reports contain fixture scores, timings, generated resource names, cleanup
results, code revision/hash, and a hash of the target URL/project. They omit
connection values, headers, raw error bodies, and presigned URLs. Raw reports
stay under ignored `artifacts/`; a reviewed public evidence summary accompanies
this experiment. The script uses finite HTTP/poll limits. `--poll-timeout` changes
the indexed-marker wait (default 360 seconds), not a product SLO.

## Confirmed by the owner on 2026-09-27

These are user-provided service guarantees, distinct from our test observations:

- Documents within one upsert, update, or delete request are processed atomically.
- With requests sent serially after each acknowledgement, application follows
  acknowledgement order. The experiment uses one writer per fresh collection.
- Tags currently capture the S3-indexed state. Visibility of the final written
  document through fetch-by-ID with `consistentRead=false` can establish that
  preceding acknowledged writes have reached that state.

The harness batches data rows by operation, splitting at a conservative 4 MiB
serialized request budget. Several requests do not become one atomic transaction.
It does not retry mutations after an ambiguous transport failure.

For each checkpoint, it sends a final marker document after the data requests
have acknowledged. The marker has a run-specific revision and no text/vector
field, so it does not enter the tested text/vector scoring corpus. It repeatedly
fetches that ID on `main` with **`consistentRead=false`**, requiring the exact
current revision rather than mere existence. This also covers a delete-only
final data operation. It then creates an immutable Tag, checks the marker again
through that Tag, and checks the expected source state and search results.

This is a scoped workaround, not a general remote commit token. It assumes
serial writes in an isolated, unpartitioned collection; it does not establish
ordering across writers, partitions, PG transactions, or collections. A production
worker still needs fencing, durable replay membership, and a way to prove that
a published snapshot covers the intended transaction. The fixture verifies both
collections separately; no cross-collection atomicity is claimed.

## Checks and interpretation

- Five known documents: vector membership/order versus local exact cosine;
  remote scores compared with `1 - distance/2` as an observed mapping, not an
  adopted SQL distance representation or a large-scale ANN recall claim.
- Standard-analyzed BM25: term/OR matches, case folding, no-token and repeated
  queries; record scores against an ASCII-only, live-corpus BM25 reference.
  A numerical mismatch is reported as an observation, not hidden or converted
  into proof of strict scoring. Deleted-document/shard statistics and arbitrary
  analyzer behavior remain separate questions.
- Multi-document update, upsert, and delete requests; new Tag sees the expected
  final payload/membership, old Tag retains its original payload and scores.
- Invalid query dimensions, zero vector, and Tag plus `consistentRead=true`
  rejection; the configured test expectations are HTTP 400.
- 110 additional matching documents with stored unindexed payloads: a size-100
  query must exercise `isDocsInline=false`, download the full result array, and
  verify payload integrity. A fresh download request carries no API key;
  gzip bodies are decoded with a separate decompressed-size limit. Redirects,
  malformed arrays, and duplicate document identities are rejected.
- Immutable list pagination independently enumerates the entire fixture.
  This is ID enumeration, not ranked query continuation. The harness demonstrates
  that rejecting all candidates in one top-100 page can miss ten eligible rows.

The [published API schema](https://docs.lambdadb.ai/reference/api/openapi.json),
[query limits](https://docs.lambdadb.ai/guides/search/limits), and
[index types](https://docs.lambdadb.ai/guides/collections/index-types) define the
request/response baseline. The small BM25 comparison uses default parameter
values from [Lucene BM25Similarity](https://lucene.apache.org/core/10_3_1/core/org/apache/lucene/search/similarities/BM25Similarity.html);
this does not assert the deployment's engine version or hidden configuration.

## Cleanup and failure handling

A generated name is checked for absence before creation. Each collection carries
a unique `pgos-run` metadata marker. Cleanup rereads that marker before deleting
tracked Tags and the collection, and confirms absence with HTTP 404. It never
adopts or deletes an existing colliding collection. Resource creation/Tag intent
is journaled to the report before sending the mutation.

Normal failures and interruption attempt cleanup in `finally`. A forced process
kill, machine failure, or persistent service error can prevent cleanup; consult
the report's generated names and ownership marker before recovery. A cleanup
failure makes the run fail. Do not blindly rerun a mutation after losing its
acknowledgement. The harness is an experiment, not a production REST client.

## Observed run — 2026-09-27

The [public evidence record](evidence/live-compatibility-2026-09-27.json) contains
26 completed checks/observations from 08:34:09–08:39:51 UTC, using Python 3.14.5.
The tested harness matches commit `22c8281482b1e7474df53a848fac3c81c306eb44` and
aggregate SHA-256 `4e0b7b61e9c8944ad47d9a8484afc90409f421ae8ba7ed5df588ee45f16fa5d5`.
The worktree was dirty because documentation/CI changes were being prepared;
the three harness Python files were verified byte-for-byte against that commit.
The public export omits the private target fingerprint and records the raw
report's checksum. It does not identify or pin the backend deployment revision.

| Observation | Result and limit |
| --- | --- |
| Indexed-marker barrier | Five checkpoints passed. Marker write through Tag creation/recheck took 48.130–102.549 seconds in this run. This is one experiment, not a latency SLO; synchronous commit integration must account for indexing delay. |
| Batching | Initial data: 5 documents per request. Mutation phases: 2-document update, 2-document upsert, 2-ID delete. Large data: 52 + 52 + 6 documents under the 4 MiB budget. Single-document writes were checkpoint markers only. |
| Vector | All five candidates matched the expected small corpus before/after mutation. Remote order agreed with local cosine; max difference from `1 - distance/2` was about 2.4e-8. This is not general ANN recall evidence. |
| BM25 | Initial and mutated `alpha` scores matched the small live-corpus reference within 6.5e-9. Repeating `alpha` doubled the observed scores. These observations do not establish arbitrary analyzer/shard/overlay statistics. |
| Immutable Tags | Previous Tag payloads and scores stayed unchanged after batch mutations; the previous BM25 Tag also retained its state after large corpus growth. |
| Large response | One offloaded download returned 100 complete items, each with its expected 80,000-character payload. One gzip response with Content-Encoding was decoded. API-key omission on the fresh download request is also covered by an offline test. |
| Query coverage | For 110 known matches, size 100 returned 100 and `total=100`; no ranked cursor was present. Size 101 returned HTTP 400. `total` did not establish corpus exhaustion. |
| Enumeration vs ranking | Four immutable list pages enumerated all 116 documents, including the marker. A constructed local rejection of the returned top-100 candidate IDs left ten eligible IDs unseen. List enumeration is not ranked continuation. |
| Cleanup | Both generated collections and all five tracked Tags were deleted; collection absence was confirmed with HTTP 404. No existing collection was modified. |

An earlier run completed the small vector/BM25 and Tag checks but failed when
interpreting the compressed large-response body as JSON. Both collections from
that run were also removed. Bounded gzip decoding was added, then the entire
live experiment above was rerun successfully; the failure is not counted as a
successful run. Nine credential-free safety tests cover batching, settings,
marker revision checks, cleanup ownership, result downloads, and gzip expansion.

## Remaining implementation gates

The bounded remote compatibility stage is complete for this fixture. Next is
an actual PostgreSQL planner/executor experiment for both modalities, with
execution-local BM25 match/score binding and guards on reused plans/rescans.
Strict snapshots and own writes still require coherent base-plus-delta scoring;
a tiny committed-corpus BM25 match does not solve that problem. Ranked candidate
continuation or a proven complete fallback, production replay/worker fencing,
remote publication coverage, and acceptable commit-wait latency remain open.
