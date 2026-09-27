# Bounded BM25 overlay and effective-statistics experiment

This is an independent Apache Lucene correctness experiment for the
[proposed overlay contract](bm25-overlay-contract.md). It is not a LambdaDB API,
a PostgreSQL implementation, or a new supported search mode. No private LambdaDB
code is included. The PG extension remains C/PGXS; Java is used only here to test
Lucene behavior directly. Changed-corpus BM25 in the PG probes still rejects.

Under the [accepted stabilization scope](development.md#accepted-stabilization-scope--2026-09-27),
this experiment remains research evidence. Server integration is deferred while
the existing restricted paths are stabilized; the oracle does not select a new
LambdaDB scoring policy.

## Question and explicit scope

Can immutable base readers plus private replacements/deletes produce the same
BM25 membership, scores and ordering as an independently rebuilt effective corpus?

Selected experiment choices: Corretto 25.0.2, Lucene 10.4.0, StandardAnalyzer,
default BM25Similarity, one text field, pre-normalized term queries/disjunctions,
unique fixture IDs and disjoint base partitions, at most 64 physical base documents,
64 final overlay operations, 64 effective documents and 4 KiB UTF-8 per document.
These are admission limits, not proposed production limits or peak-memory guarantees.
There is no JSON endpoint, wire duplicate-key validation, Tag resolver, PG snapshot,
background worker, cloud I/O, local PG BM25 fallback, or service deployment.

The candidate keeps each base reader immutable, masks every replaced/deleted ID,
and adds a request-owned in-memory delta index. It preserves existing hard-delete
visibility. A request-local statistics pass enumerates **all** terms and live
postings to compute effective document/field/term counts. Searches use those common
counts and the existing document norms. Cache sharing is disabled for visibility
wrappers; a different request cannot reuse the wrong live-document mask.

This is deliberately a full vocabulary/postings scan, followed by complete bounded
candidate enumeration. It provides a correctness reference, not a scalable server
algorithm. The oracle separately rebuilds the effective document map into a fresh
index with the same analyzer/similarity. It does not use the candidate statistics
calculator. Both use Lucene scoring, so this compares overlay correctness rather
than independently proving the BM25 formula or analyzer implementation.

## Why result masking alone is insufficient

Lucene documents that term frequencies and field statistics can include deleted
postings until merges remove them. Changing visible hits therefore does not itself
establish changed corpus statistics. See the [IndexReader statistics contract](https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/index/IndexReader.html#docFreq(org.apache.lucene.index.Term)).
The [IndexSearcher statistics hooks](https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/search/IndexSearcher.html#termStatistics(org.apache.lucene.index.Term,int,long))
permit a common statistics source across readers; the caller must supply correct
values for its chosen scoring semantics.

The negative control uses the same masked readers and private delta with ordinary
raw-reader statistics. Six fixtures produce the correct matching IDs but different
scores from the rebuilt effective corpus. For example, deleting one matching
document changes the remaining score from `0.36074576` in the visibility-only
control to `0.53744066` in both the candidate and oracle. This demonstrates a
semantic distinction, not a bug report against ordinary Lucene deletion semantics.

A multi-term fixture also moves document `a` from outside the old top-1 to the new
first position. Rescoring the old top-1 cannot find that document, even with correct
statistics. The candidate enumerates the whole admitted corpus; no fixed over-fetch
or large-corpus continuation claim follows.

An initial implementation attempted to return absent statistics for terms present
only in masked postings; it failed during Lucene scoring. The final restricted
query builder replaces such term clauses with `MatchNoDocsQuery` and short-circuits
an empty effective text corpus. Both cases have regression fixtures. Prefix/fuzzy
expansion, phrases, synonyms, wildcard queries, boosts and arbitrary Boolean query
semantics require their own rewrite/statistics design and are not admitted here.

## Newly exposed scoring-contract decision

The `retained_hard_deletes` case has **no request overlay**. Its rebuilt live-corpus
scores still differ from ordinary base-reader scores because the latter retain
statistics from physically deleted postings. Thus two proposed acceptance promises
are not automatically compatible: (1) empty overlay preserves ordinary Tag scores,
and (2) every request equals a freshly rebuilt live corpus. This experiment chooses
(2) as its local oracle only; it does not approve a change to service scoring.

Before a server endpoint is implemented, decide whether a new explicit scoring
mode defines live-corpus statistics even for empty overlays, or whether Tag score
compatibility takes priority with a different oracle/statistics definition. Global
versus node-local statistics introduces another compatibility dimension. The
unchanged PG same-corpus path continues to use the existing remote scores. No
silently different empty-overlay scoring mode has been connected to it.

## Reproduction and evidence

```sh
./scripts/test-bm25-overlay.sh
# Optionally save source/image-bound JSON; refuses to overwrite an existing report:
./scripts/test-bm25-overlay.sh --report /absolute/path/to/new-report.json
```

Docker pins the Corretto image digest and verifies SHA-256 for both Maven jars.
`javac -Xlint:all -Werror` must pass. Execution uses `--network none`; the host runner
compares all four probe input hashes with the exact image ID before running and
records the source revision, dirty state, runtime, scores and outcomes. CI retains
the result JSON and text log. A source-dirty run is labeled as such, never silently
attributed to an unmodified commit.

Fourteen cases assert:

- Empty overlay, insert/new term, delete-only, nonmatching replacement, changed
  frequency/length, mixed operations and physically retained hard deletions.
- All documents deleted, empty text, absent-key deletion, a removed-only term,
  and an unmatched query.
- A new winner outside the previous top-k.
- One composite reader versus partitioned readers, plus independently searched
  leaves sharing the same global in-process statistics; two concurrent private
  requests over the same base remain separate and leave the base unchanged.
- Conflicting final operations, document byte limits and effective-corpus limits
  reject; rejected request construction releases borrowed base references.

Ordered IDs and finite scores must match the rebuilt oracle within absolute
`1e-6`. Tie ordering uses the fixture's external string ID. The partition case
uses one process and a shared statistics object; it does **not** exercise a real
multi-node statistics exchange or establish placement-independent cloud results.
Cancellation, process failure, advanced analyzers and large-corpus performance
are not exercised. The over-budget construction failure only tests its local
reader/reference cleanup path.

## What this enables next

Confirmed by this experiment: for the admitted fixture scope, visible-postings
statistics plus complete candidate coverage reproduce the rebuilt oracle while
keeping the base immutable and overlays request-local.

Still proposed: the server API shape, how a Tag pins readers, a scalable statistics
algorithm, global aggregation/routing, query-language coverage, admission limits,
continuation and cancellation/retry lifetime. The first server experiment should
make these boundaries explicit and reject unsupported scope. Only after server
implementation and actual PG/remote validation should changed-corpus BM25 be enabled.

### Recorded local result, 2026-09-27

The [source/image-bound report](evidence/bm25-overlay-2026-09-27.json) records all
14 cases passing at clean implementation `edd7e37`. Six visibility-only controls
have the expected score mismatch, while the effective-statistics candidate matches
the rebuilt oracle in every comparison. The old-top-k case records old winner `b`
and new winner `a`. This evidence is from an isolated Linux arm64 JVM with no
network; no live LambdaDB or PostgreSQL run is represented by this report.
