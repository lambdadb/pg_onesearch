# Skeleton validation — 2026-09-26

## Result and bounds

Local Docker build, PGXS installation, SQL regression, actual libpq extended
protocol, binary COPY, logical dump/restore, and clean-runtime bundle installation
passed on PostgreSQL 18.6 / Debian 12 / Linux arm64. No hosted CI was run.
This proves the local value-type skeleton only. The reviewed design's full
end-to-end or transaction acceptance criteria are not marked complete.

## Reproduce

From the repository root, with Docker running:

```sh
./scripts/test.sh
./scripts/package.sh
./scripts/test-package.sh
```

The tests create disposable databases/containers, do not publish a port, and
run database clients with container networking disabled. The build needs network
access for image/package downloads. No LambdaDB service or credentials are used.
The clean-package test validates the checksum and installs into a fresh official
PostgreSQL image, without the extension build toolchain.

## Observed evidence

| Check | Observation |
| --- | --- |
| Build/install | GCC shared object and Clang LLVM bitcode compiled without emitted compiler warnings; PGXS installed library, control and versioned SQL files. |
| Clean extension creation | `pg_onesearch` version `0.1.0-dev`; only `plpgsql` also installed. No pgvector dependency or preload. |
| SQL regression | `make installcheck`: 1/1 suite passed. Checked expected output is committed in `test/expected/vector.out`; it was inspected before accepting the baseline. |
| Type validation | 2 and 4096 dimensions accepted; 0/1/4097, modifier count, dimension mismatch, malformed text, NaN/infinities, float32 overflow/underflow rejected. Typed assignment is checked as well as literals. |
| Exact distance | Same/orthogonal/opposite directions returned 0/1/2; independent analytic reference matched; large finite/subnormal inputs remained finite; zero-vector cosine rejected and NULL propagated. |
| Plan | `Limit -> Sort -> Seq Scan on vectors`; no remote index plan is claimed. |
| Storage | 4096-dimensional value generated real TOAST storage; output and distance detoasted successfully; text round-trip matched. Original content/vector values survived local rollback. |
| Extended protocol | 33 libpq parameter/result cases passed, including text/binary binding, byte-for-byte binary round-trip, malformed length/dimension/non-finite rejection with SQLSTATE assertions, typmod enforcement and seeded independent cosine references through dimension 4096. |
| Permissions | Ordinary non-superuser role could use the type after superuser installation. This is not a remote RLS/tenant-isolation test. |
| COPY / restore | Binary COPY to/from a table passed. Custom-format pg_dump/pg_restore into another database retained text, vector, and NULL values. This is local-type restoration, not remote-source fencing or physical recovery. |
| Evaluation bundle | Archive checksum passed; extraction into the separate clean PG image, `CREATE EXTENSION`, and an asserted cosine result of 1 all succeeded. |
| Cleanup | Disposable test clusters and containers removed; the local development image and evaluation archive remain. |

During test authoring, the initial regression run intentionally had an empty
expected-output file; the captured output was checked against the intended
contract and committed. Subsequent runs passed against that baseline. An early
image build preceded source-file creation and failed for the missing source;
the completed source then built successfully. A one-off clean-runtime invocation
had shell quoting errors; `scripts/test-package.sh` replaced it and passed.
These setup failures are not counted as passing runs.

## Build identity and evaluation artifact

- PostgreSQL runtime/headers: `18.6-1.pgdg12+2`.
- GCC: `12.2.0-14+deb12u1`; Make: `4.3-4.1`.
- Clang/LLVM: `1:19.1.7-3~deb12u1` (PGXS bitcode).
- Base: `postgres:18.6-bookworm`, digest
  `sha256:3725f4e2499eef5134592b3b4ab79a543ed7f8e533b05b5b637af926630f6650`.
- Development image: local `pg_onesearch:0.1.0-dev`; this mutable local tag is not
  a release identity or a published registry artifact.
- Bundle: `artifacts/pg_onesearch-0.1.0-dev-pg18.6-debian12-arm64.tar.gz`.
- Bundle SHA-256:
  `cb2e3d2061b71aac7123b46e42b0c6e3ade7dd003e701dfac56a7bf37402d82b`.
- Installed `pg_onesearch.so` SHA-256:
  `7d7e7fd90ffa508e0f832ea0f8657708cf547e99e976d11f786e9e8575019d9b`.

The archive contains the shared object, control/install SQL, and PGXS bitcode.
Generated artifacts are ignored by Git. Rebuilding can change archive timestamps
and digests; use the regenerated `artifacts/SHA256SUMS` for that artifact. The
pinned compiler/PG packages do not freeze every transitive apt dependency, so
bit-for-bit release reproducibility remains unproven.

## Not exercised or delivered

Remote vector/BM25 queries, IAM/opclasses, live LambdaDB compatibility, replay,
worker fencing, Tag readiness/publication, post-commit response integration,
snapshot overlays, remote outage behavior, fault recovery, performance, upgrades,
other PG majors/OS/CPU targets, and hosted CI/release distribution remain open.
See [implementation decisions](development.md) for the next executable gates.
