# BM25 request-scoped overlay: requirements and proposed contract

**Status: deferred proposal.** No server endpoint, request fields, implementation,
or delivery commitment is approved by this document. The extension probe still
rejects a changed indexed corpus before making a BM25 HTTP request.

The owner-approved [stabilization scope](development.md#accepted-stabilization-scope--2026-09-27)
retains that rejection and defers LambdaDB core changes and temporary search
Branches. This document preserves a candidate for future review, not the next
implementation task. Its rebuilt-corpus scoring oracle is an experimental choice;
document visibility and scoring-statistics policy must be reviewed separately
before a generic server feature is selected. No scoring alternative is approved.

## Confirmed requirements

The [reviewed design](https://github.com/lambdadb/sbrain/blob/628fdf77aaf36a4544c2083d11579525c95c9931/projects/lambdadb-postgresql-extension.md)
requires snapshot-consistent search, eligible own writes, current PG originals,
and no silent local BM25 fallback. The effective search corpus must be the
immutable selected base with all snapshot-visible replacements/deletions applied.
Changes in another uncommitted or aborted transaction must not enter that corpus.
An updated document must mask its old version even if it no longer matches.

Membership and scores must describe **one corpus**. Filtering changed keys out of
old hits does not establish corresponding changes in corpus/term statistics.
Rescoring only an old top-k cannot discover a previously omitted candidate whose
rank changes after deletion or replacement. A second independent delta ranking
also does not establish a common score scale.

## Current public API evidence

The [published OpenAPI](https://docs.lambdadb.ai/reference/api/openapi.json) was
retrieved on 2026-09-27, SHA-256
`ad930a79ab27115a326bde6dff69c472670f8ba0eda65dde1c5f01ff76678563`.
`POST /collections/{collectionName}/query` exposes `query`, `ref`, `size`,
`consistentRead`, `includeVectors`, `sort`, `fields`, and `partitionFilter`.
Its top-level request disallows additional properties and limits size to 100.
The query object itself permits arbitrary properties, so inspecting this schema
alone cannot prove the absence of an undocumented server capability.

No request-scoped replacement/deletion list, scoring-statistics contract, or
ranked continuation contract was found in that schema. `consistentRead=true`
is constrained to Branch refs; it does not specify a private PG transaction
applied over a pinned Tag. Do not infer overlay support or globally coherent
statistics from the existing query endpoint. This inspection is of the public
contract, not an audit or publication of private LambdaDB implementation code.

## Proposed semantic contract, for server review

The following names illustrate required information, not an executable API:

```json
{
  "base": {"kind": "tag", "name": "immutable-published-tag"},
  "overlay": {
    "upserts": [{"id": "42", "content": "replacement text"}],
    "deletes": ["17"]
  },
  "query": {"queryString": {"query": "alpha", "defaultField": "content", "skipSyntax": true}},
  "limit": 20
}
```

- Resolve the Tag once and pin its immutable snapshot for the whole request.
  Normalize to one final operation per key; reject conflicting duplicate keys.
  Upserts replace the entire indexed document, rather than merge unspecified fields.
- Build the effective corpus `(base minus all changed keys) union upserts`.
  Deletes of absent keys are idempotent. Null indexed values map to deletions in
  the PG client; schema/analyzer errors fail the whole request.
- Tokenize base and replacements using the same pinned analyzer configuration,
  field rules, norms and scoring parameters. Determine corpus size, field length
  statistics and term document frequencies from the same effective corpus across
  every participating partition/node. No node-local approximation may be silently
  presented as this contract.
- Enumerate/rank against that effective corpus, including candidates outside the
  former base top-k. Return a deterministic tie key and enough information to
  distinguish base and replacement revisions. Echo the resolved base snapshot
  and a digest identifying query, normalized overlay and scoring configuration.
- For larger client-side SQL filtering, provide either complete bounded results
  with an exhaustion indication or ranked continuation pinned to the same request
  context. Define expiry and fail explicitly after expiry; never restart silently
  on a different base/statistics view. Pagination and exactness remain open design
  decisions, not capabilities of the current probe.
- Keep the overlay private to this query context. No collection write, Branch,
  Tag, publication, or visibility to another request should result. Cancellation,
  failure, and retry must not persist transaction-private documents.
- Specify admission limits and errors for document bytes/count, distinct terms,
  computation, timeout, and concurrent requests. Reject before returning partial
  success when the promised semantics cannot be met. Avoid document contents in
  ordinary error/audit logs.

These semantics should be usable by non-PostgreSQL clients as well. Transport
shape, endpoint placement, continuation ownership, exact distributed statistics,
cost, limits, compatibility versioning and server ownership are **unresolved**.
Creating a collection/Branch per source transaction is not the selected approach.

## Acceptance fixtures before enabling changed-corpus BM25

Compare an overlay request with a separately materialized immutable collection
containing exactly the effective corpus and identical analyzer/scoring settings.
Validate IDs, deterministic order and scores within an agreed tolerance; this
oracle construction is an acceptance fixture, not the intended runtime design.

| Fixture | Required observation |
| --- | --- |
| Empty overlay | Membership must match the base. Score compatibility with ordinary Tag queries versus a new live-corpus scoring mode remains unresolved; see the experiment below. |
| Matching insert, nonmatching replacement, deletion | New candidate included; both stale matching versions absent |
| Term-frequency and document-length changes | Scores agree with rebuilt effective corpus |
| Delete a frequent/rare-term document outside returned top-k | Other scores/ranking reflect new corpus statistics |
| Candidate originally below base top-k | Candidate can enter the final top-k after overlay |
| Duplicate/conflicting keys, null/invalid fields | Defined atomic rejection or normalization, no partial corpus |
| Multiple partitions and different placement | Same agreed scoring semantics, no node-local score drift |
| Concurrent queries with different private overlays | Neither query observes the other's changes |
| Page continuation with local SQL filtering | No gaps/duplicates; pinned scores/base; explicit exhaustion/expiry |
| Cancellation, timeout, retry, invalid base | No persistent writes; no successful partial response |

An available endpoint alone will not complete the first-E2E gate: connect it to
the same registered PG snapshot and test own writes, rollback, committed lag,
publication races, health, score scope, and source tuple identity in the actual
executor. Until then only unchanged-corpus BM25 is admitted.

## Executable local follow-up

The [independent Lucene experiment](bm25-overlay-spike.md) exposes a scoring
choice that must be settled before approving this API: ordinary Tag scores can
retain statistics for physically deleted postings, whereas the fresh-corpus oracle
does not. Empty-overlay score compatibility and universal rebuild equivalence
cannot both be assumed. The original empty-overlay acceptance row above is now
explicitly marked unresolved; snapshot membership/isolation requirements remain.

The experiment also tests bounded
request-local replacements/deletes against a rebuilt corpus oracle, including
negative controls for stale statistics and old-top-k candidate loss. It establishes
a small local scoring construction, not approval or availability of a LambdaDB
endpoint. The PG changed-corpus rejection remains in place. Term rewriting when
all visible occurrences disappear is also part of the contract; correcting only
score inputs is insufficient for arbitrary query types.
