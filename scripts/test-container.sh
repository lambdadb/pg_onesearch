#!/bin/sh
# Runs inside the development image as postgres; never uses a host database.
set -eu
export PGHOST=/tmp/pg_onesearch-socket PGPORT=55432 PGUSER=postgres
PGDATA=$(mktemp -d /tmp/pg_onesearch-data.XXXXXX)
export PGDATA
mkdir -p "$PGHOST"
cleanup() {
    pg_ctl -D "$PGDATA" -m immediate stop >/dev/null 2>&1 || true
    rm -rf "$PGDATA" "$PGHOST"
}
trap cleanup EXIT HUP INT TERM
initdb -D "$PGDATA" --no-locale --encoding=UTF8 --auth=trust >/dev/null
pg_ctl -D "$PGDATA" -l "$PGDATA/server.log" -o "-c listen_addresses='' -k $PGHOST -p $PGPORT" -w start
make PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config installcheck
python3 test/protocol.py
