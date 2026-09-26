"""Actual psql/Psycopg behavior against the isolated commit-worker probe.

This tests driver completion boundaries and notices, not LambdaDB readiness.
The product vector type is exercised through ordinary bound parameters too.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import platform
import struct
import subprocess
import threading
import time

import psycopg
from psycopg import pq


checks = []
connections = []
pool = ThreadPoolExecutor(max_workers=2)


def connect(*, autocommit=True, wait_ms=1500):
    conn = psycopg.connect(autocommit=True)
    connections.append(conn)
    conn.execute(f"SET onesearch_probe.wait_ms = {wait_ms}")
    conn.autocommit = autocommit
    return conn


admin = connect()


def scalar(query, params=None):
    return admin.execute(query, params).fetchone()[0]


def status():
    return tuple(map(int, scalar("SELECT onesearch_probe.status()").split(',')))


def mode(value):
    admin.execute("SELECT onesearch_probe.control(%s)", (value,))


def count(table):
    assert table in ('source', 'outbox', 'receipts')
    return scalar(f"SELECT count(*) FROM onesearch_probe.{table}")


def wait_for(check, label, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.02)
    raise AssertionError(f"timeout waiting for {label}")


def reset():
    mode(1)
    wait_for(lambda: status()[1] == 0 and status()[2] == 0, "idle fixture")
    admin.execute("TRUNCATE onesearch_probe.source, onesearch_probe.outbox, onesearch_probe.receipts")


def recover(expected=1):
    mode(0)
    wait_for(lambda: count('outbox') == 0 and count('receipts') == expected, "durable receipts")


def insert(conn, key=1, content='document', *, prepare=None):
    return conn.execute(
        "INSERT INTO onesearch_probe.source VALUES (%s, %s, %s::onesearch.vector(3))",
        (key, content, '[1,0,0]'), prepare=prepare,
    )


def notices(conn):
    observed = []

    def collect(diag):
        # Diagnostic lifetime ends on return; copy fields immediately.
        observed.append((diag.sqlstate, diag.message_primary, diag.message_detail))

    conn.add_notice_handler(collect)
    return observed


def warning(observed):
    assert len(observed) == 1, observed
    assert observed[0][0] == '01000', observed
    assert 'source committed; synchronization incomplete' in observed[0][1], observed


def block_then_publish(action):
    future = pool.submit(action)
    wait_for(lambda: status()[2] == 1, "post-commit publication wait")
    assert not future.done()
    assert count('source') == count('outbox') == 1 and count('receipts') == 0
    mode(0)
    result = future.result(10)
    assert count('receipts') == 1 and count('outbox') == 0
    return result


def passed(name, detail=''):
    checks.append(name)
    print(f"PASS {len(checks):02d}: {name}" + (f" — {detail}" if detail else ''), flush=True)


try:
    assert psycopg.__version__ == '3.3.6'
    assert pq.__impl__ == 'python'
    assert scalar('SHOW server_version_num') == '180006'
    for name in ('fsync', 'synchronous_commit', 'full_page_writes'):
        assert scalar(f'SHOW {name}') == 'on'
    print(f"CLIENT TARGET: Psycopg {psycopg.__version__} ({pq.__impl__}), libpq {pq.version()}, Python {platform.python_version()}", flush=True)
    print(subprocess.check_output(['psql', '--version'], text=True).strip(), flush=True)
    scalar('SELECT onesearch_probe.start_worker()')
    wait_for(lambda: status()[0] > 0, 'worker start')

    reset()
    with closing(connect(autocommit=False)) as conn:
        observed = notices(conn)
        cursor = insert(conn)
        assert cursor.statusmessage == 'INSERT 0 1' and cursor.rowcount == 1
        assert conn.info.transaction_status == pq.TransactionStatus.INTRANS
        assert count('source') == count('outbox') == count('receipts') == 0
        assert not observed
        assert block_then_publish(conn.commit) is None
        assert conn.info.transaction_status == pq.TransactionStatus.IDLE
        assert not observed
    passed('default execute returns inside transaction; commit waits for publication')

    reset()
    with closing(connect()) as conn:
        observed = notices(conn)
        cursor = block_then_publish(lambda: insert(conn))
        assert cursor.statusmessage == 'INSERT 0 1' and cursor.rowcount == 1
        assert conn.info.transaction_status == pq.TransactionStatus.IDLE
        assert not observed
    passed('parameterized autocommit execute waits through synchronization')

    reset()
    with closing(connect()) as conn:
        def transaction_context():
            with conn.transaction():
                insert(conn)
                assert conn.info.transaction_status == pq.TransactionStatus.INTRANS
                assert count('source') == 0
        block_then_publish(transaction_context)
        assert conn.info.transaction_status == pq.TransactionStatus.IDLE
    passed('transaction context exit is the outer commit boundary')

    reset()
    with closing(connect(autocommit=False, wait_ms=300)) as conn:
        observed = notices(conn)
        insert(conn)
        assert conn.commit() is None
        warning(observed)  # Notice handler ran before commit() returned.
        assert conn.info.transaction_status == pq.TransactionStatus.IDLE
        assert count('source') == count('outbox') == 1 and count('receipts') == 0
        conn.rollback()  # A rollback after commit cannot undo it.
        assert count('source') == count('outbox') == 1
    recover()
    passed('explicit commit timeout returns normally with 01000 notice; rollback cannot undo commit')

    reset()
    with closing(connect(wait_ms=300)) as conn:
        observed = notices(conn)
        cursor = insert(conn)
        warning(observed)
        assert cursor.rowcount == 1 and cursor.statusmessage == 'INSERT 0 1'
        assert conn.info.transaction_status == pq.TransactionStatus.IDLE
        assert count('source') == count('outbox') == 1
    recover()
    passed('autocommit timeout delivers notice without a driver exception')

    reset()
    mode(2)
    with closing(connect(autocommit=False)) as conn:
        observed = notices(conn)
        insert(conn)
        conn.commit()
        warning(observed)
        assert 'mock_failure' in observed[0][2]
        assert count('outbox') == 1 and count('receipts') == 0
    recover()
    passed('mock remote failure is a warning, not a rolled-back transaction')

    reset()
    with closing(connect(wait_ms=300)) as conn:
        # No notice handler: normal driver return does not prove publication.
        assert insert(conn).rowcount == 1
        assert conn.info.transaction_status == pq.TransactionStatus.IDLE
        assert count('outbox') == 1 and count('receipts') == 0
    recover()
    passed('without a notice handler synchronization warnings are not raised as exceptions')

    reset()
    with closing(connect(autocommit=False)) as conn:
        observed = notices(conn)
        insert(conn)
        future = pool.submit(conn.execute, 'SELECT pg_sleep(30)')
        wait_for(lambda: scalar("SELECT wait_event = 'PgSleep' FROM pg_stat_activity WHERE pid = %s", (conn.info.backend_pid,)), 'pre-commit sleep')
        conn.cancel_safe(timeout=3)
        try:
            future.result(5)
            raise AssertionError('expected pre-commit cancellation error')
        except psycopg.errors.QueryCanceled as exc:
            assert exc.sqlstate == '57014'
        assert conn.info.transaction_status == pq.TransactionStatus.INERROR
        conn.rollback()
        assert count('source') == count('outbox') == 0 and not observed
    passed('cancel before commit raises 57014; rollback removes source and capture')

    reset()
    with closing(connect(autocommit=False, wait_ms=5000)) as conn:
        observed = notices(conn)
        insert(conn)
        future = pool.submit(conn.commit)
        wait_for(lambda: status()[2] == 1, 'post-commit wait before cancel_safe')
        conn.cancel_safe(timeout=3)
        assert future.result(5) is None
        warning(observed)
        assert 'interrupted' in observed[0][2]
        assert conn.info.transaction_status == pq.TransactionStatus.IDLE
        assert count('source') == count('outbox') == 1
        assert conn.execute('SELECT 1').fetchone()[0] == 1
        conn.rollback()
    recover()
    passed('cancel_safe after commit returns success plus warning and preserves source')

    reset()
    with closing(connect()) as conn:
        def nested_context():
            with conn.transaction():
                insert(conn, 1)
                with conn.transaction():
                    insert(conn, 2)
                    raise psycopg.Rollback()
                assert conn.execute('SELECT count(*) FROM onesearch_probe.outbox').fetchone()[0] == 1
        block_then_publish(nested_context)
        assert scalar('SELECT id FROM onesearch_probe.source') == 1
    passed('nested transaction context rolls back its savepoint; only outer commit publishes')

    reset()
    with closing(connect(autocommit=False)) as conn:
        with conn.cursor().copy('COPY onesearch_probe.source (id,content,embedding) FROM STDIN') as copy:
            copy.write_row((1, 'copied', '[1,0,0]'))
        assert conn.info.transaction_status == pq.TransactionStatus.INTRANS
        assert count('source') == 0
        block_then_publish(conn.commit)
        assert scalar('SELECT content FROM onesearch_probe.source') == 'copied'
    passed('COPY completion inside a transaction precedes commit/publication')

    reset()
    admin.execute("""
        CREATE FUNCTION onesearch_probe.deferred_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
            IF NEW.content = 'reject-at-commit' THEN
                RAISE EXCEPTION 'deferred rejection' USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END $$;
        CREATE CONSTRAINT TRIGGER deferred_guard AFTER INSERT ON onesearch_probe.source
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
        EXECUTE FUNCTION onesearch_probe.deferred_guard();
    """)
    with closing(connect(autocommit=False)) as conn:
        observed = notices(conn)
        insert(conn, content='reject-at-commit')
        try:
            conn.commit()
            raise AssertionError('expected deferred constraint failure')
        except psycopg.errors.CheckViolation as exc:
            assert exc.sqlstate == '23514'
        assert conn.info.transaction_status == pq.TransactionStatus.IDLE
        assert count('source') == count('outbox') == 0 and not observed
    admin.execute('DROP TRIGGER deferred_guard ON onesearch_probe.source; DROP FUNCTION onesearch_probe.deferred_guard()')
    passed('deferred trigger failure during commit is a real rollback, without sync warning')

    reset()
    with closing(connect(wait_ms=300)) as conn:
        observed = notices(conn)
        mode(0)
        insert(conn, 1, prepare=True)
        assert count('receipts') == 1 and not observed
        mode(1)
        wait_for(lambda: status()[1] == 0, 'worker paused before reused prepared statement')
        insert(conn, 2, prepare=True)
        warning(observed)
        assert count('source') == 2 and count('outbox') == count('receipts') == 1
    recover(2)
    passed('reused prepared INSERT still observes a later publication timeout')

    reset()
    with closing(connect()) as conn:
        row = conn.execute('SELECT %s::onesearch.vector(3)::text, onesearch.cosine_distance(%s::onesearch.vector, %s::onesearch.vector)',
                           ('[1,0,0]', '[1,0,0]', '[0,1,0]'), prepare=True).fetchone()
        assert row == ('[1,0,0]', 1.0)
        with conn.cursor(binary=True) as cursor:
            cursor.execute('SELECT %s::onesearch.vector(3)', ('[1,0,0]',))
            assert cursor.fetchone()[0] == struct.pack('!ifff', 3, 1, 0, 0)
        for value, state in [('[1,2]', '22000'), ('[NaN,0,0]', '22000')]:
            try:
                conn.execute('SELECT %s::onesearch.vector(3)', (value,), prepare=True)
                raise AssertionError('invalid vector accepted')
            except psycopg.DataError as exc:
                assert exc.sqlstate == state
        assert count('source') == count('outbox') == count('receipts') == 0
    passed('vector text binding, binary result bytes, prepared parameters and validation errors')

    reset()
    result = subprocess.run(['psql', '-XAt', '-v', 'ON_ERROR_STOP=1',
                             '-c', "SET onesearch_probe.wait_ms=300; BEGIN; INSERT INTO onesearch_probe.source VALUES (1,'psql explicit','[1,0,0]'); COMMIT;"],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert 'COMMIT' in result.stdout and 'WARNING:' in result.stderr
    assert 'source committed; synchronization incomplete' in result.stderr
    assert count('source') == count('outbox') == 1
    recover()
    passed('psql explicit COMMIT: exit status 0 and synchronization WARNING on stderr')

    reset()
    result = subprocess.run(['psql', '-XAt', '-v', 'ON_ERROR_STOP=1',
                             '-c', 'SET onesearch_probe.wait_ms=300',
                             '-c', "INSERT INTO onesearch_probe.source VALUES (1,'psql auto','[1,0,0]')"],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0 and 'WARNING:' in result.stderr, result
    assert count('source') == count('outbox') == 1
    recover()
    passed('psql autocommit: ON_ERROR_STOP does not turn warnings into errors')

    reset()
    with closing(connect(wait_ms=300)) as conn:
        observed = notices(conn)
        executed, allow_sync = threading.Event(), threading.Event()
        def pipelined():
            with conn.pipeline() as pipeline:
                cursor = insert(conn)
                executed.set()
                assert allow_sync.wait(5)
                pipeline.sync()
                assert cursor.rowcount == 1
        future = pool.submit(pipelined)
        assert executed.wait(5)
        assert not future.done() and count('source') == count('receipts') == 0
        allow_sync.set()
        future.result(5)
        warning(observed)
        assert conn.info.transaction_status == pq.TransactionStatus.IDLE
        assert count('source') == count('outbox') == 1
    recover()
    passed('pipeline execute return is not commit; sync delivers the eventual warning')

    reset()
    with closing(connect(autocommit=False)) as conn:
        observed = notices(conn)
        insert(conn)
        try:
            conn.execute('SELECT 1 / 0')
            raise AssertionError('expected division error')
        except psycopg.errors.DivisionByZero:
            pass
        assert conn.info.transaction_status == pq.TransactionStatus.INERROR
        assert conn.commit() is None
        assert conn.info.transaction_status == pq.TransactionStatus.IDLE
        assert count('source') == count('outbox') == 0 and not observed
    passed('commit() after an earlier transaction error can return normally while PG rolls back')

    print(f'PASS: {len(checks)} actual-client contract scenarios', flush=True)
finally:
    pool.shutdown(wait=True)
    for conn in connections:
        conn.close()
