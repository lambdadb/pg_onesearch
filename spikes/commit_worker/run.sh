#!/bin/sh
set -eu
# Compile/install the probe ONLY inside this disposable container.
make -C spikes/commit_worker PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config CC=gcc
make -C spikes/commit_worker PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config install
export PGHOST=/tmp/onesearch-probe-socket PGPORT=55433 PGUSER=postgres PGDATABASE=postgres
export PGDATA=/tmp/onesearch-probe-data
install -d -o postgres -g postgres "$PGDATA" "$PGHOST"
cleanup() {
    gosu postgres pg_ctl -D "$PGDATA" -m immediate stop >/dev/null 2>&1 || true
    cat "$PGDATA/server.log" | tail -n 30
}
trap cleanup EXIT HUP INT TERM
gosu postgres initdb -D "$PGDATA" --no-locale --encoding=UTF8 --auth=trust >/dev/null
cat >> "$PGDATA/postgresql.conf" <<CONF
listen_addresses = ''
unix_socket_directories = '$PGHOST'
port = $PGPORT
shared_preload_libraries = 'pg_onesearch_commit_probe'
max_prepared_transactions = 10
CONF
gosu postgres pg_ctl -l "$PGDATA/server.log" -w start
gosu postgres psql -X -v ON_ERROR_STOP=1 -f spikes/commit_worker/setup.sql
gosu postgres python3 -u spikes/commit_worker/test.py
