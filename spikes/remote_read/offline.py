"""Actual PG/libcurl requests against a local HTTPS fault fixture; no external network."""
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import ssl
import threading
import time
import unittest

import psycopg
from psycopg.types.json import Jsonb

REQUESTS = []
CONNECTIONS = 0


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def setup(self):
        global CONNECTIONS
        super().setup()
        CONNECTIONS += 1
        self.connection_id = CONNECTIONS

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        mode = self.path.split('/')[-2]
        REQUESTS.append((mode, body, dict(self.headers)))
        payload = json.dumps({'isDocsInline': True, 'docs': [
            {'doc': {'id': 'd1', 'connection': self.connection_id}, 'score': 1.0}]}).encode()
        status, headers = 200, {}
        if mode == 'slow':
            time.sleep(3)
        elif mode == 'disconnect':
            self.connection.shutdown(socket.SHUT_RDWR)
            self.close_connection = True
            return
        elif mode == 'truncated':
            headers['Content-Length'] = str(len(payload) + 50)
            self.close_connection = True
        elif mode == 'redirect':
            status = 302
            headers['Location'] = 'https://localhost:18443/projects/fixture/collections/forbidden/query?secret=sentinel'
        elif mode.startswith('status'):
            status = int(mode[6:])
            payload = b'sentinel fixture-secret https://private.invalid/?token=sentinel'
        elif mode == 'invalid':
            payload = b'{"private":"sentinel", broken'
        elif mode == 'utf8':
            payload = b'{"private":"sentinel\xff"}'
        elif mode == 'nul':
            payload += b'\x00'
        elif mode == 'offloaded':
            payload = b'{"isDocsInline":false,"docs":[],"docsUrl":"https://private.invalid/?sentinel"}'
        elif mode == 'shape':
            payload = b'{"docs":[],"isDocsInline":"true"}'
        elif mode in ('big', 'gzipbig'):
            payload = b' ' * (8 * 1024 * 1024 + 1)
        if mode in ('gzip', 'gzipbig'):
            payload = gzip.compress(payload)
            headers['Content-Encoding'] = 'gzip'
        try:
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            headers.setdefault('Content-Length', str(len(payload)))
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            pass


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def get_request(self):
        connection, address = super().get_request()
        try:
            connection.settimeout(5)
            return self.context.wrap_socket(connection, server_side=True), address
        except Exception:
            connection.close()
            raise

    def handle_error(self, *args):
        pass


def connect():
    return psycopg.connect(autocommit=True)


def query(conn, collection='ok', tag='checkpoint-test', value=None, size=10):
    return conn.execute('SELECT pgos_remote_probe.query(%s,%s,%s,%s)',
                        (collection, tag, Jsonb(value or {'matchAll': {}}), size)).fetchone()[0]


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.conn = connect()
        self.addCleanup(self.conn.close)

    def failure(self, mode, state):
        before = len(REQUESTS)
        with self.assertRaises(psycopg.Error) as caught:
            query(self.conn, mode)
        self.assertEqual(caught.exception.sqlstate, state)
        for private in ('sentinel', 'fixture-secret', 'https://private', 'token='):
            self.assertNotIn(private, str(caught.exception))
        self.assertEqual(len(REQUESTS), before + 1, 'must not retry or follow redirects')
        self.assertEqual(query(self.conn)['docs'][0]['doc']['id'], 'd1')

    def test_connection_reuse_and_tag_request(self):
        a, b = query(self.conn), query(self.conn)
        self.assertEqual(a['docs'][0]['doc']['connection'], b['docs'][0]['doc']['connection'])
        _, body, headers = REQUESTS[-1]
        self.assertEqual(body['ref'], {'kind': 'tag', 'name': 'checkpoint-test'})
        self.assertNotIn('consistentRead', body)
        self.assertEqual(headers['x-api-key'], 'fixture-secret')

    def test_errors_redacted_without_retry(self):
        for mode in ('status401', 'status429', 'status500', 'redirect', 'disconnect', 'truncated'):
            with self.subTest(mode=mode):
                self.failure(mode, '08006')

    def test_invalid_responses_redacted(self):
        for mode in ('invalid', 'utf8', 'nul', 'shape'):
            with self.subTest(mode=mode):
                self.failure(mode, '22000')

    def test_offloaded_results_fail_explicitly(self):
        self.failure('offloaded', '0A000')

    def test_gzip(self):
        self.assertEqual(query(self.conn, 'gzip')['docs'][0]['doc']['id'], 'd1')

    def test_response_and_decompression_limits(self):
        for mode in ('big', 'gzipbig'):
            self.failure(mode, '54000')

    def test_request_limit(self):
        before = len(REQUESTS)
        with self.assertRaises(psycopg.errors.ProgramLimitExceeded):
            query(self.conn, value={'queryString': {'query': 'a' * (1024 * 1024)}})
        self.assertEqual(len(REQUESTS), before)
        query(self.conn)

    def test_total_timeout(self):
        self.conn.execute("SET pgos_remote_probe.timeout_ms = 200")
        started = time.monotonic()
        self.failure('slow', '08006')
        self.assertLess(time.monotonic() - started, 1.5)

    def test_statement_timeout(self):
        self.conn.execute("SET statement_timeout = '200ms'")
        started = time.monotonic()
        self.failure('slow', '57014')
        self.assertLess(time.monotonic() - started, 1.5)

    def test_explicit_cancel_and_savepoint_recovery(self):
        self.conn.execute('BEGIN')
        self.conn.execute('SAVEPOINT before_remote')
        cancel = threading.Timer(.2, self.conn.cancel)
        started = time.monotonic()
        cancel.start()
        try:
            with self.assertRaises(psycopg.errors.QueryCanceled):
                query(self.conn, 'slow')
        finally:
            cancel.join()
        self.assertLess(time.monotonic() - started, 1.5)
        self.conn.execute('ROLLBACK TO before_remote')
        query(self.conn)
        self.conn.execute('COMMIT')

    def test_terminate_backend(self):
        pid = self.conn.info.backend_pid
        def terminate():
            with connect() as admin:
                admin.execute('SELECT pg_terminate_backend(%s)', (pid,))
        timer = threading.Timer(.2, terminate)
        timer.start()
        started = time.monotonic()
        try:
            with self.assertRaises(psycopg.OperationalError):
                query(self.conn, 'slow')
        finally:
            timer.join()
        self.assertLess(time.monotonic() - started, 1.5)
        with connect() as other:
            query(other)

    def test_repeated_failure_releases_sockets(self):
        query(self.conn)
        fds = Path(f'/proc/{self.conn.info.backend_pid}/fd')
        before = len(list(fds.iterdir()))
        for _ in range(30):
            self.failure('truncated', '08006')
        self.assertLessEqual(len(list(fds.iterdir())), before + 2)

    def test_input_validation_without_network(self):
        for args in (('../evil', 'tag', {}, 1), ('ok', 'branch/name', {}, 1),
                     ('ok', 'tag', [], 1), ('ok', 'tag', {}, 101)):
            before = len(REQUESTS)
            with self.assertRaises(psycopg.errors.InvalidParameterValue):
                self.conn.execute('SELECT pgos_remote_probe.query(%s,%s,%s,%s)',
                                  (args[0], args[1], Jsonb(args[2]), args[3]))
            self.assertEqual(len(REQUESTS), before)

    def test_role_check_even_after_grant(self):
        self.conn.execute('CREATE ROLE probe_reader')
        self.addCleanup(lambda: self.conn.execute('DROP ROLE probe_reader'))
        self.conn.execute('GRANT USAGE ON SCHEMA pgos_remote_probe TO probe_reader')
        self.conn.execute('GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgos_remote_probe TO probe_reader')
        try:
            self.conn.execute('SET ROLE probe_reader')
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                query(self.conn)
        finally:
            self.conn.execute('RESET ROLE')
            self.conn.execute('DROP OWNED BY probe_reader')

    def test_tls_certificate_and_hostname_verification(self):
        for name in ('untrusted', 'mismatch'):
            invalid = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            invalid.load_cert_chain(f'/tmp/{name}.crt', f'/tmp/{name}.key')
            server.context = invalid
            before = len(REQUESTS)
            try:
                with connect() as fresh:
                    with self.assertRaises(psycopg.errors.ConnectionFailure):
                        query(fresh)
                self.assertEqual(len(REQUESTS), before, 'TLS rejection must precede sending credentials')
            finally:
                server.context = context
        query(self.conn)

    def test_z_server_diagnostics_redacted(self):
        log = Path('/tmp/pgos-remote-data/server.log').read_text()
        for private in ('fixture-secret', 'sentinel', 'private.invalid'):
            self.assertNotIn(private, log)


if __name__ == '__main__':
    server = Server(('127.0.0.1', 18443), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain('/tmp/probe.crt', '/tmp/probe.key')
    server.context = context
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        unittest.main(verbosity=2)
    finally:
        server.shutdown()
        server.server_close()
