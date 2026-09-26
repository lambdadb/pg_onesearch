# pg_onesearch

Local development workspace prepared on September 26, 2026. This directory has
not been initialized as a Git repository and contains no extension implementation.
Steven selected `pg_onesearch` as the project, intended repository, and SQL
extension name on September 26, 2026. No package or extension has been released.

Text and vector search for PostgreSQL, powered by LambdaDB.

The initial extension will require a connection to LambdaDB Cloud. Standalone
installation means no pgvector dependency; it does not mean offline or entirely
local search. BYOC and self-hosted LambdaDB are future product directions
reported by Steven, not currently available deployment options in this plan.

## Canonical plan

Read the complete [reviewed design and release plan](https://github.com/lambdadb/sbrain/blob/628fdf77aaf36a4544c2083d11579525c95c9931/projects/lambdadb-postgresql-extension.md)
before implementation. This pinned revision is the handoff baseline; the
[sbrain project on main](https://github.com/lambdadb/sbrain/blob/main/projects/lambdadb-postgresql-extension.md)
is the canonical document following the merge of
[PR #52](https://github.com/lambdadb/sbrain/pull/52). This README is an entry point, not another editable design spec.

Steven merged PR #52 on September 26, 2026 as `681ed6b`. The merge ancestry
and all four changed documents were verified against the reviewed PR head.
The local sbrain `main` was updated and the merged documentation worktree
and branch were removed. The naming decision followed that merge: examples in
the pinned plan still use the provisional extension name `lambdadb`. Use
`pg_onesearch` for the extension identifier, control file, and SQL migration
filenames when implementing them. Schema, type, access-method, function, and
operator names remain design choices; do not mechanically rename all occurrences
of the LambdaDB service name. Reconcile these examples in the next plan update.

The intended installation statement, once implemented, is:

```sql
CREATE EXTENSION pg_onesearch;
```

## Start the next development session

Open this directory as the workspace and read the linked plan, particularly
"Implementation decisions and first development milestone", the transaction
sections, the acceptance criteria, and "Release and branch policy".

Start with a bounded architecture and build spike:

1. Inspect available tooling, choose and record the implementation language,
   and pin one PostgreSQL/build target. C is the current recommendation; Rust/pgrx
   remains an option. PG 18, Ubuntu 24.04, and x86_64 are examples, not selected
   or tested support commitments.
2. Establish the dedicated source repository and the planned `main`/`develop`
   workflow, with reproducible build, install, and test commands. Remote repository
   creation and publication have not been performed in this preparation step.
3. Build a standalone installable skeleton and extension-owned float32 vector
   type with I/O validation and local exact distance evaluation. Keep PostgreSQL
   as the source of original vectors and text; do not require pgvector.
4. Keep both vector and BM25 retrieval in the first end-to-end milestone. Finalize
   their SQL contract and verify actual index plans against the source plan.
5. Investigate safe post-commit response waits and worker coordination early.
   Atomic source/outbox capture, replay, Tag readiness, and PG publication need
   executable fault tests before any transaction guarantee is claimed.

Use the plan's acceptance criteria as the implementation contract. In particular,
a remote failure after PG commit must not be represented as a rolled-back source
write; preserve replay work, return the specified synchronization warning, and
block affected-index searches until verified recovery. Snapshot-visible overlays
and BM25 corpus/statistics consistency remain correctness gates.

Record unresolved decisions and actual test evidence as implementation proceeds.
No runtime, upgrade, recovery, performance, or release validation is established
by the planning documents or this workspace preparation.
