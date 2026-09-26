#!/bin/sh
# Validate the already-built bundle on the clean, pinned runtime image.
set -eu
cd "$(dirname "$0")/.."
(cd artifacts && shasum -a 256 -c SHA256SUMS)
docker run --rm -i --platform linux/arm64 --network none --entrypoint sh \
    -v "$PWD/artifacts:/artifacts:ro" \
    postgres:18.6-bookworm@sha256:3725f4e2499eef5134592b3b4ab79a543ed7f8e533b05b5b637af926630f6650 <<'CONTAINER'
set -eu
tar -xzf /artifacts/pg_onesearch-0.1.0-dev-pg18.6-debian12-arm64.tar.gz -C /
export PGDATA=/tmp/onesearch-clean PGHOST=/tmp PGUSER=postgres
install -d -o postgres -g postgres "$PGDATA"
gosu postgres initdb --no-locale --encoding=UTF8 --auth=trust >/dev/null
gosu postgres pg_ctl -l "$PGDATA/server.log" -o "-c listen_addresses='' -k /tmp" -w start
trap 'gosu postgres pg_ctl -m immediate stop >/dev/null 2>&1 || true' EXIT
gosu postgres psql -X -v ON_ERROR_STOP=1 <<'SQL'
CREATE EXTENSION pg_onesearch;
SELECT onesearch.cosine_distance('[1,0]', '[0,1]') AS distance;
SELECT extname, extversion FROM pg_extension ORDER BY extname;
DO $$ BEGIN
    IF onesearch.cosine_distance('[1,0]', '[0,1]') <> 1 THEN
        RAISE EXCEPTION 'incorrect cosine distance';
    END IF;
END $$;
SQL
CONTAINER
