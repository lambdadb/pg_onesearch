#!/bin/sh
set -eu
export PGDATA=/tmp/pgos-lifecycle-data PGHOST=/tmp/pgos-lifecycle-socket
export PGPORT=55435 PGUSER=postgres PGDATABASE=postgres
install -d -o postgres -g postgres "$PGDATA" "$PGHOST"
trap 'gosu postgres pg_ctl -D "$PGDATA" -m immediate stop >/dev/null 2>&1 || true' EXIT HUP INT TERM
gosu postgres initdb -D "$PGDATA" --no-locale --encoding=UTF8 --auth=trust >/dev/null
cat >> "$PGDATA/postgresql.conf" <<CONF
listen_addresses = ''
unix_socket_directories = '$PGHOST'
port = $PGPORT
log_min_error_statement = 'panic'
CONF
gosu postgres pg_ctl -l "$PGDATA/server.log" -w start >/dev/null
gosu postgres psql -X -v ON_ERROR_STOP=1 -f spikes/index_lifecycle/setup.sql >/dev/null
/opt/client-venv/bin/python -u spikes/index_lifecycle/test.py
