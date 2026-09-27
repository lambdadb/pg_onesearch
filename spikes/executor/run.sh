#!/bin/sh
set -eu
export PGHOST=/tmp/pgos-remote-socket PGPORT=55434 PGUSER=postgres PGDATABASE=postgres
export PGDATA=/tmp/pgos-remote-data
install -d -o postgres -g postgres "$PGDATA" "$PGHOST"
cleanup() {
    gosu postgres pg_ctl -D "$PGDATA" -m immediate stop >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM
if [ "${1:-offline}" = offline ]; then
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost \
        -addext subjectAltName=DNS:localhost -keyout /tmp/probe.key -out /tmp/probe.crt >/dev/null 2>&1
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost \
        -addext subjectAltName=DNS:localhost -keyout /tmp/untrusted.key -out /tmp/untrusted.crt >/dev/null 2>&1
    openssl req -new -newkey rsa:2048 -nodes -subj /CN=wrong.invalid \
        -addext subjectAltName=DNS:wrong.invalid -keyout /tmp/mismatch.key -out /tmp/mismatch.csr >/dev/null 2>&1
    openssl x509 -req -in /tmp/mismatch.csr -CA /tmp/probe.crt -CAkey /tmp/probe.key \
        -CAcreateserial -days 1 -copy_extensions copy -out /tmp/mismatch.crt >/dev/null 2>&1
    chown postgres:postgres /tmp/probe.key /tmp/probe.crt /tmp/untrusted.key /tmp/mismatch.key
    export LAMBDADB_BASE_URL=https://localhost:18443 LAMBDADB_PROJECT_NAME=fixture
    export LAMBDADB_PROJECT_API_KEY=fixture-secret PGOS_PROBE_CA_FILE=/tmp/probe.crt
fi
gosu postgres initdb -D "$PGDATA" --no-locale --encoding=UTF8 --auth=trust >/dev/null
cat >> "$PGDATA/postgresql.conf" <<CONF
listen_addresses = ''
unix_socket_directories = '$PGHOST'
port = $PGPORT
shared_preload_libraries = 'pg_onesearch_executor_probe'
log_statement = 'none'
log_min_error_statement = 'panic'
CONF
gosu postgres pg_ctl -l "$PGDATA/server.log" -w start >/dev/null
gosu postgres psql -X -v ON_ERROR_STOP=1 -f spikes/remote_read/setup.sql >/dev/null
gosu postgres psql -X -v ON_ERROR_STOP=1 -f spikes/executor/setup.sql >/dev/null
gosu postgres /opt/client-venv/bin/python -u "spikes/executor/${1:-offline}.py"
