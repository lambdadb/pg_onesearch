"""Raw PG protocol + concurrent sessions, no client package dependencies."""
import concurrent.futures
import os
import socket
import struct
import subprocess
import time


class Client:
    def __init__(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(15)
        self.sock.connect(f"{os.environ['PGHOST']}/.s.PGSQL.{os.environ['PGPORT']}")
        payload = struct.pack("!i", 196608) + b"user\0postgres\0database\0postgres\0\0"
        self.sock.sendall(struct.pack("!i", len(payload) + 4) + payload)
        self.receive(b"Z")

    def close(self):
        self.sock.close()

    def exact(self, length):
        data = bytearray()
        while len(data) < length:
            chunk = self.sock.recv(length - len(data))
            if not chunk:
                raise EOFError("backend disconnected")
            data.extend(chunk)
        return bytes(data)

    def send(self, tag, payload=b""):
        self.sock.sendall(tag + struct.pack("!i", len(payload) + 4) + payload)

    def receive(self, until):
        messages = []
        while True:
            tag = self.exact(1)
            size = struct.unpack("!i", self.exact(4))[0]
            data = self.exact(size - 4)
            messages.append((tag, data))
            if tag == until:
                return messages

    def query(self, sql, *, allow_error=False):
        self.send(b"Q", sql.encode() + b"\0")
        messages = self.receive(b"Z")
        if not allow_error:
            assert not any(t == b"E" for t, _ in messages), messages
        return messages

    def scalar(self, sql):
        messages = self.query(sql)
        rows = [p for t, p in messages if t == b"D"]
        assert len(rows) == 1, messages
        length = struct.unpack("!i", rows[0][2:6])[0]
        return rows[0][6:6 + length].decode()

    def extended(self, sql, *, sync=True):
        self.send(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!h", 0))
        self.send(b"B", b"\0\0" + struct.pack("!hhh", 0, 0, 0))
        self.send(b"E", b"\0" + struct.pack("!i", 0))
        self.send(b"S" if sync else b"H")
        return self.receive(b"Z" if sync else b"C")


def wait_for(check, description, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.02)
    raise AssertionError(f"timeout waiting for {description}")


def committed(messages, command, warning=False):
    tags = [t for t, _ in messages]
    assert b"E" not in tags, messages
    assert (b"C", command.encode() + b"\0") in messages, messages
    assert messages[-1] == (b"Z", b"I"), messages
    if warning:
        assert b"N" in tags and tags.index(b"N") < tags.index(b"C"), messages
        assert any(b"source committed; synchronization incomplete" in p for t, p in messages if t == b"N")
    else:
        assert b"N" not in tags, messages


pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)
admin, writer = Client(), Client()
checks = []


def status():
    return tuple(map(int, admin.scalar("SELECT onesearch_probe.status()").split(",")))


def mode(value):
    admin.query(f"SELECT onesearch_probe.control({value})")


def count(table):
    return int(admin.scalar(f"SELECT count(*) FROM onesearch_probe.{table}"))


def reset():
    mode(1)
    wait_for(lambda: status()[1] == 0, "idle worker")
    admin.query("TRUNCATE onesearch_probe.source, onesearch_probe.outbox, onesearch_probe.receipts")
    writer.query("SET onesearch_probe.wait_phase = after_locks; SET onesearch_probe.wait_ms = 1000")


def row(client, key):
    client.query(f"INSERT INTO onesearch_probe.source VALUES ({key}, 'document-{key}', '[1,0,0]')")


def waiting():
    wait_for(lambda: status()[2] > 0, "writer publication wait")


def drained(expected):
    wait_for(lambda: count('outbox') == 0 and count('receipts') == expected,
             f"{expected} published receipts")


def passed(name, evidence=""):
    checks.append(name)
    print(f"PASS {len(checks):02d}: {name}" + (f" — {evidence}" if evidence else ""), flush=True)


try:
    assert admin.scalar("SHOW server_version_num") == '180006'
    for setting in ['fsync', 'synchronous_commit', 'full_page_writes']:
        assert admin.scalar(f"SHOW {setting}") == 'on', setting
    assert admin.scalar("SELECT bool_and(relpersistence = 'p') FROM pg_class WHERE oid IN ('onesearch_probe.source'::regclass, 'onesearch_probe.outbox'::regclass, 'onesearch_probe.receipts'::regclass)") == 't'
    print("TARGET: PostgreSQL 18.6; fsync/synchronous_commit/full_page_writes=on; logged source/outbox/receipts", flush=True)
    admin.scalar("SELECT onesearch_probe.start_worker()")
    wait_for(lambda: status()[0] > 0, "worker startup")
    reset()
    writer.query("BEGIN")
    row(writer, 1)
    assert writer.scalar("SELECT count(*) FROM onesearch_probe.outbox") == '1'
    assert count('source') == count('outbox') == 0
    writer.query("ROLLBACK")
    assert count('source') == count('outbox') == count('receipts') == 0
    passed("atomic capture: own-write visibility, other-session exclusion, full rollback")

    # A transaction containing only rolled-back captures must not register a wait.
    writer.query("BEGIN; SAVEPOINT a; SAVEPOINT b")
    row(writer, 2)
    writer.query("RELEASE b; ROLLBACK TO a")
    committed(writer.query("COMMIT"), "COMMIT")
    assert count('source') == count('outbox') == 0
    writer.query("BEGIN; SAVEPOINT a")
    row(writer, 3)
    writer.query("RELEASE a; ROLLBACK")
    assert count('source') == count('outbox') == 0
    writer.query("BEGIN")
    row(writer, 22)
    writer.query("SAVEPOINT child")
    row(writer, 23)
    writer.query("ROLLBACK TO child")
    mode(0)
    committed(writer.query("COMMIT"), "COMMIT")
    drained(1)
    assert count('source') == 1
    assert admin.scalar("SELECT payload->'new'->>'id' FROM onesearch_probe.receipts") == '22'
    passed("savepoints remove aborted captures/marks and retain surviving parent writes")

    # Negative control: commit callback is after visibility but before lock release.
    reset()
    writer.query("SET onesearch_probe.wait_phase = commit")
    writer.query("BEGIN; LOCK TABLE onesearch_probe.source IN ACCESS EXCLUSIVE MODE")
    row(writer, 4)
    mode(0)
    reply = pool.submit(writer.query, "COMMIT")
    waiting()
    wait_for(lambda: admin.scalar("SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'onesearch commit probe' AND wait_event_type = 'Lock'") == '1', "worker blocked on source lock")
    assert count('outbox') == 1  # committed and visible, despite writer not responding
    committed(reply.result(5), "COMMIT", warning=True)
    evidence = writer.scalar("SELECT onesearch_probe.last_result()")
    assert 'phase=1 outcome=timeout' in evidence
    drained(1)
    passed("COMMIT callback negative control: visible outbox, retained source lock, timeout", evidence)

    # Candidate callback runs only on the top owner's AFTER_LOCKS phase.
    reset()
    writer.query("BEGIN; LOCK TABLE onesearch_probe.source IN ACCESS EXCLUSIVE MODE; SAVEPOINT a")
    row(writer, 5)
    writer.query("RELEASE a")
    mode(0)
    committed(writer.query("COMMIT"), "COMMIT")
    evidence = writer.scalar("SELECT onesearch_probe.last_result()")
    assert 'phase=2 outcome=published holdoff=1' in evidence
    assert count('receipts') == 1 and count('outbox') == 0
    passed("after-lock explicit COMMIT publishes despite source ACCESS EXCLUSIVE lock", evidence)

    reset()
    mode(0)
    committed(writer.query("INSERT INTO onesearch_probe.source VALUES (6, 'auto', '[0,1,0]')"), "INSERT 0 1")
    assert count('receipts') == 1
    passed("simple-protocol autocommit waits for durable receipt")

    reset()
    mode(0)
    committed(writer.extended("INSERT INTO onesearch_probe.source VALUES (7, 'extended', '[0,1,0]')"), "INSERT 0 1")
    assert count('receipts') == 1
    passed("extended-protocol autocommit with Sync completes after publication")

    reset()
    writer.query("BEGIN")
    row(writer, 8)
    mode(0)
    committed(writer.extended("COMMIT"), "COMMIT")
    assert count('receipts') == 1
    passed("extended-protocol explicit COMMIT waits before CommandComplete")

    # Flush deliberately exposes CommandComplete before the Sync commit boundary.
    reset()
    early = writer.extended("INSERT INTO onesearch_probe.source VALUES (9, 'flush', '[0,1,0]')", sync=False)
    assert early[-1] == (b'C', b'INSERT 0 1\0')
    assert count('source') == count('outbox') == 0
    writer.send(b'S')
    late = writer.receive(b'Z')
    assert [t for t, _ in late] == [b'N', b'Z'], late
    assert count('source') == count('outbox') == 1
    mode(0)
    drained(1)
    passed("Execute+Flush: CommandComplete precedes commit; timeout warning arrives at Sync")

    reset()
    reply = pool.submit(writer.query, "INSERT INTO onesearch_probe.source VALUES (10, 'timeout', '[1,0,0]')")
    waiting()
    assert count('source') == count('outbox') == 1
    committed(reply.result(5), "INSERT 0 1", warning=True)
    assert count('receipts') == 0
    mode(0)
    drained(1)
    passed("paused worker: warning + commit success, durable replay, later recovery")

    reset()
    mode(2)
    committed(writer.query("INSERT INTO onesearch_probe.source VALUES (11, 'failure', '[1,0,0]')"), "INSERT 0 1", warning=True)
    assert 'outcome=mock_failure' in writer.scalar("SELECT onesearch_probe.last_result()")
    assert count('outbox') == 1 and count('receipts') == 0
    mode(0)
    drained(1)
    passed("mock remote failure preserves source and replay, then recovers")

    reset()
    writer.query("SET onesearch_probe.wait_ms = 5000")
    pid = int(writer.scalar("SELECT pg_backend_pid()"))
    reply = pool.submit(writer.query, "INSERT INTO onesearch_probe.source VALUES (12, 'cancel', '[1,0,0]')")
    waiting()
    assert admin.scalar(f"SELECT pg_cancel_backend({pid})") == 't'
    result = reply.result(5)
    committed(result, "INSERT 0 1", warning=True)
    assert count('source') == count('outbox') == 1
    evidence = writer.scalar("SELECT onesearch_probe.last_result()")
    assert 'outcome=interrupted' in evidence
    assert writer.scalar("SELECT 1") == '1'
    mode(0)
    drained(1)
    passed("cancel during post-commit wait preserves commit and usable connection", evidence)

    reset()
    writer.query("BEGIN")
    row(writer, 13)
    error = writer.query("PREPARE TRANSACTION 'onesearch_probe_prepared'", allow_error=True)
    assert any(t == b'E' and b'0A000' in p for t, p in error), error
    assert count('source') == count('outbox') == 0
    assert admin.scalar("SELECT count(*) FROM pg_prepared_xacts") == '0'
    passed("prepared source transaction rejected before preparation; capture rolls back")

    # Larger allocated outbox ID can publish while a lower ID is uncommitted.
    reset()
    other = Client()
    writer.query("BEGIN")
    row(writer, 14)
    small = int(writer.scalar("SELECT min(id) FROM onesearch_probe.outbox"))
    mode(0)
    row(other, 15)
    large = int(admin.scalar("SELECT min(id) FROM onesearch_probe.receipts"))
    assert small < large and count('receipts') == 1
    committed(writer.query("COMMIT"), "COMMIT")
    drained(2)
    other.close()
    passed("late smaller outbox ID is processed after earlier publication of larger ID", f"{small} < {large}")

    # Preserve full UPDATE/DELETE payloads and separate exact event identities.
    reset()
    mode(0)
    row(writer, 16)
    writer.query("UPDATE onesearch_probe.source SET content='changed' WHERE id=16")
    writer.query("DELETE FROM onesearch_probe.source WHERE id=16")
    row(writer, 16)
    assert admin.scalar("SELECT string_agg(operation, ',' ORDER BY id) FROM onesearch_probe.receipts") == 'INSERT,UPDATE,DELETE,INSERT'
    assert admin.scalar("SELECT payload->'old'->>'content' FROM onesearch_probe.receipts WHERE operation='DELETE'") == 'changed'
    passed("UPDATE/DELETE/reinsert preserve replay payload and event identities")

    # Terminate the worker with its receipt insertion/deletion still uncommitted.
    reset()
    mode(3)
    reply = pool.submit(writer.query, "INSERT INTO onesearch_probe.source VALUES (17, 'publication', '[1,0,0]')")
    wait_for(lambda: status()[1] == 2, "worker before publication commit")
    assert count('outbox') == 1 and count('receipts') == 0
    old_pid = status()[0]
    assert admin.scalar(f"SELECT pg_terminate_backend({old_pid}, 5000)") == 't'
    mode(1)
    committed(reply.result(5), "INSERT 0 1", warning=True)
    wait_for(lambda: status()[0] not in (0, old_pid), "worker restart")
    assert count('outbox') == 1 and count('receipts') == 0
    mode(0)
    drained(1)
    passed("worker termination before publication commit rolls back deletion; restarted worker replays")

    reset()
    mode(4)
    reply = pool.submit(writer.query, "INSERT INTO onesearch_probe.source VALUES (20, 'lost-notify', '[1,0,0]')")
    wait_for(lambda: status()[1] == 3, "durable publication before shared notification")
    assert count('outbox') == 0 and count('receipts') == 1
    old_pid = status()[0]
    assert admin.scalar(f"SELECT pg_terminate_backend({old_pid}, 5000)") == 't'
    mode(1)
    committed(reply.result(5), "INSERT 0 1", warning=True)
    wait_for(lambda: status()[0] not in (0, old_pid), "worker restart after lost notification")
    assert count('outbox') == 0 and count('receipts') == 1
    mode(0)
    drained(1)
    passed("lost post-publication notification: conservative timeout, durable receipt survives")

    reset()
    committed(writer.query("INSERT INTO onesearch_probe.source VALUES (21, 'snapshot', '[1,0,0]')"), "INSERT 0 1", warning=True)
    reader = Client()
    reader.query("BEGIN ISOLATION LEVEL REPEATABLE READ")
    assert reader.scalar("SELECT count(*) FROM onesearch_probe.outbox") == '1'
    mode(0)
    drained(1)
    assert reader.scalar("SELECT count(*) FROM onesearch_probe.outbox") == '1'
    assert reader.scalar("SELECT count(*) FROM onesearch_probe.receipts") == '0'
    admin.query("VACUUM onesearch_probe.outbox")
    assert reader.scalar("SELECT count(*) FROM onesearch_probe.outbox") == '1'
    reader.query("COMMIT")
    reader.close()
    passed("old snapshot retains outbox across publication, DELETE and VACUUM")

    reset()
    writer.send(b'Q', b"INSERT INTO onesearch_probe.source VALUES (18, 'disconnect', '[1,0,0]')\0")
    waiting()
    assert count('outbox') == 1
    writer.close()
    wait_for(lambda: status()[2] == 0, "disconnected writer bounded cleanup")
    writer = Client()
    assert count('source') == count('outbox') == 1
    mode(0)
    drained(1)
    passed("disconnect after source commit preserves replay and recovers")

    # Crash the entire cluster after source commit, before any mock delivery.
    reset()
    writer.send(b'Q', b"INSERT INTO onesearch_probe.source VALUES (19, 'restart', '[1,0,0]')\0")
    waiting()
    assert count('source') == count('outbox') == 1
    subprocess.run(['pg_ctl', '-m', 'immediate', '-w', 'stop'], check=True)
    writer.close()
    admin.close()
    subprocess.run(['pg_ctl', '-l', os.environ['PGDATA'] + '/server.log', '-w', 'start'], check=True)
    admin, writer = Client(), Client()
    assert count('source') == count('outbox') == 1 and count('receipts') == 0
    admin.scalar("SELECT onesearch_probe.start_worker()")
    mode(0)
    drained(1)
    assert admin.scalar("SELECT payload->'new'->>'content' FROM onesearch_probe.receipts") == 'restart'
    passed("immediate PostgreSQL crash after source commit: WAL recovery retains source and replay")
    reset()
    writer.query("SET onesearch_probe.wait_ms = 5000")
    other = Client()
    other.query("SET onesearch_probe.wait_ms = 5000")
    mode(3)
    first = pool.submit(writer.query, "INSERT INTO onesearch_probe.source VALUES (30, 'first-waiter', '[1,0,0]')")
    wait_for(lambda: status()[1] == 2, "first batch before publication commit")
    second = pool.submit(other.query, "INSERT INTO onesearch_probe.source VALUES (31, 'second-waiter', '[1,0,0]')")
    wait_for(lambda: status()[2] == 2, "two independent committed waiters")
    mode(1)  # Let first batch commit, but prevent a second batch from starting.
    committed(first.result(5), "INSERT 0 1")
    assert not second.done(), "second transaction woke on unrelated publication"
    assert count('receipts') == count('outbox') == 1
    assert admin.scalar("SELECT payload->'new'->>'id' FROM onesearch_probe.receipts") == '30'
    mode(0)
    committed(second.result(5), "INSERT 0 1")
    drained(2)
    other.close()
    passed("concurrent waiters complete only for their own published transaction membership")
    print(f"PASS: {len(checks)} commit/worker scenarios", flush=True)
finally:
    writer.close()
    admin.close()
    pool.shutdown(wait=True)
