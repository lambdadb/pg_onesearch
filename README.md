# pg_onesearch

Text and vector search for PostgreSQL, powered by LambdaDB.

**Status: installable development skeleton (`0.1.0-dev`), not a search service
or a release.** C / PGXS, PostgreSQL 18.6, Debian 12, Linux arm64 is the first
build and validation target. The host development machine is macOS arm64;
builds and database tests run in Docker.

Implemented: standalone `CREATE EXTENSION pg_onesearch`, an extension-owned
float32 vector type, validated text/binary I/O, dimension modifiers, and local
exact cosine distance. No pgvector dependency, credentials, preload setting,
or network access is needed for this local skeleton.

The intended remote vector and BM25 paths require LambdaDB Cloud. Neither is
implemented yet. Standalone installation means no pgvector dependency, not
an offline replacement for LambdaDB. BYOC and self-hosting remain future
product directions, not available deployment choices in this plan.

## Build and test

Prerequisite: Docker with Linux arm64 support. From this repository:

```sh
./scripts/test.sh
```

This builds the digest-pinned image, installs the extension with PGXS, starts a
disposable PostgreSQL cluster with no TCP listener or container network, runs
SQL regression and actual libpq text/binary protocol tests, tests binary COPY
and logical dump/restore, and removes the test container and cluster.

For an interactive local database (development trust authentication, no host
port or persistent volume):

```sh
docker run --rm -d --name pg-onesearch-demo --network none \
  --platform linux/arm64 -e POSTGRES_HOST_AUTH_METHOD=trust \
  pg_onesearch:0.1.0-dev
# Wait until this reports accepting connections:
docker exec pg-onesearch-demo pg_isready -U postgres
docker exec -it pg-onesearch-demo psql -U postgres
```

```sql
CREATE EXTENSION pg_onesearch;
CREATE TABLE documents (
    id bigint PRIMARY KEY,
    content text,
    embedding onesearch.vector(3)
);
INSERT INTO documents VALUES
    (1, 'First document', '[1,0,0]'),
    (2, 'Second document', '[0,1,0]');
SELECT id, embedding OPERATOR(onesearch.<=>) '[1,0,0]'::onesearch.vector AS distance
FROM documents ORDER BY distance, id LIMIT 10;
```

Remove the demo and its disposable storage with `docker stop pg-onesearch-demo`.
For a PG18 installation with server headers and PGXS, conventional source
installation is `make PG_CONFIG=/path/to/pg_config` followed by
`make PG_CONFIG=/path/to/pg_config install` with filesystem installation rights.
Only the Docker target above has been validated; other PG majors are rejected
at compile time. `CREATE EXTENSION` requires a database superuser.

## Evaluation bundle

```sh
./scripts/package.sh
./scripts/test-package.sh
```

The resulting `artifacts/pg_onesearch-0.1.0-dev-pg18.6-debian12-arm64.tar.gz`
contains PGXS-installed files rooted at `usr/`; `artifacts/SHA256SUMS` records
its checksum. Install by extracting into `/` only in a disposable matching
Debian/PGDG PostgreSQL 18.6 arm64 environment, then run `CREATE EXTENSION`.
This is an internal evaluation bundle, not a `.deb`, supported upgrade, or
published release. Do not replace binaries in a running production database.

## Design and development status

Maintainer background: the [reviewed design and release plan](https://github.com/lambdadb/sbrain/blob/628fdf77aaf36a4544c2083d11579525c95c9931/projects/lambdadb-postgresql-extension.md).
The [canonical plan on main](https://github.com/lambdadb/sbrain/blob/main/projects/lambdadb-postgresql-extension.md)
was merged through [PR #52](https://github.com/lambdadb/sbrain/pull/52) as `681ed6b`.
Its provisional extension name `lambdadb` is superseded by Steven's selected
`pg_onesearch`; the service name remains LambdaDB. The local implementation
record does not replace or edit that canonical plan.

- [Implementation choices, confirmed requirements, and unresolved gates](docs/development.md)
- [Actual validation evidence and limitations](docs/validation.md)
- [Commit/worker feasibility experiment and protocol limitations](docs/commit-worker-spike.md)
- [Proposed vector/BM25 SQL and client contract, with unresolved gates](docs/sql-client-contract.md)
- [Actual psql/Psycopg completion and warning evidence](docs/client-contract-validation.md)
- [Live LambdaDB compatibility harness and indexed-write barrier](docs/live-compatibility.md)
- [C/libcurl read transport candidate and validation scope](docs/remote-read-spike.md)
- [Vector/BM25 Custom Scan executor experiment](docs/executor-spike.md)
- [IAM generation, DDL rollback, and recovery experiment](docs/index-lifecycle-spike.md)
- [Durable registry and atomic change capture experiment](docs/change-capture-spike.md)
- [Fenced batch replay, verified Tags, and atomic publication experiment](docs/batch-replay-spike.md)
- [Statement snapshots, own-write vector reads, and BM25 boundaries](docs/snapshot-read-spike.md)

Run `./scripts/test-commit-worker.sh` for the isolated commit/worker experiment.
It compiles a separate test-only module in a disposable container. That module
is not installed by `CREATE EXTENSION pg_onesearch` or included in its evaluation
bundle; the product skeleton still has no outbox or background worker.
Run `./scripts/test-client-contract.sh` to exercise the same fixture with psql
and hash-pinned Psycopg 3.3.6 in a separate test image.

The public source repository is [lambdadb/pg_onesearch](https://github.com/lambdadb/pg_onesearch).
Feature PRs target `develop`; `main` is reserved for release-validated
implementation. Branch rulesets are managed by the LambdaDB organization.

[CI](https://github.com/lambdadb/pg_onesearch/actions/workflows/ci.yml) builds and
tests the arm64 Docker target, commit/worker probe, and actual clients, verifies clean bundle
installation, and retains the tested evaluation bundle, checksum, source
revision, build environment, and probe/client logs for 14 days. CI artifacts are development outputs, not published releases.
The source-linked sbrain plan may require organization access; the local
implementation and validation documents above describe the public skeleton.
No release tag or stable distribution has been published; license selection
remains open.
