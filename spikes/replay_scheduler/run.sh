#!/bin/sh
set -eu
mode=${1:-test}
case "$mode" in test|live|live_fixture) ;; *) exit 2 ;; esac
export PGDATA=/tmp/pgos-scheduler-data PGHOST=/tmp/pgos-scheduler-socket
export PGPORT=55440 PGUSER=postgres PGDATABASE=postgres
install -d -o postgres -g postgres "$PGDATA" "$PGHOST"
trap 'gosu postgres pg_ctl -D "$PGDATA" -m immediate stop >/dev/null 2>&1 || true' EXIT HUP INT TERM
if [ "$mode" != live ]; then
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost \
    -addext subjectAltName=DNS:localhost -keyout /tmp/probe.key -out /tmp/probe.crt >/dev/null 2>&1
export LAMBDADB_BASE_URL=https://localhost:18443 LAMBDADB_PROJECT_NAME=fixture
export LAMBDADB_PROJECT_API_KEY=fixture-secret PGOS_PROBE_CA_FILE=/tmp/probe.crt
fi
gosu postgres initdb -D "$PGDATA" --no-locale --encoding=UTF8 --auth=trust >/dev/null
cat >> "$PGDATA/postgresql.conf" <<CONF
listen_addresses = ''
unix_socket_directories = '$PGHOST'
port = $PGPORT
shared_preload_libraries = 'pg_onesearch_commit_probe,pg_onesearch_snapshot_probe,pg_onesearch_snapshot_executor'
log_min_error_statement = 'panic'
max_prepared_transactions = 10
CONF
gosu postgres pg_ctl -l "$PGDATA/server.log" -w start >/dev/null
gosu postgres psql -X -v ON_ERROR_STOP=1 -f spikes/index_lifecycle/setup.sql >/dev/null
gosu postgres psql -X -v ON_ERROR_STOP=1 -f spikes/change_capture/setup.sql >/dev/null
gosu postgres psql -X -v ON_ERROR_STOP=1 -f spikes/batch_replay/setup.sql >/dev/null
gosu postgres psql -X -v ON_ERROR_STOP=1 -f spikes/remote_read/setup.sql >/dev/null
gosu postgres psql -X -v ON_ERROR_STOP=1 -f spikes/snapshot_read/setup.sql >/dev/null
gosu postgres psql -X -v ON_ERROR_STOP=1 -f spikes/snapshot_executor/setup.sql >/dev/null
gosu postgres psql -X -v ON_ERROR_STOP=1 -f spikes/publication_commit/setup.sql >/dev/null
gosu postgres psql -X -v ON_ERROR_STOP=1 -f spikes/replay_scheduler/setup.sql >/dev/null
/opt/client-venv/bin/python -u "spikes/replay_scheduler/$mode.py"
