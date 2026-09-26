"""Real libpq extended-protocol tests, binary COPY, and logical dump/restore.

Uses only Python's standard library and libpq supplied by the development image.
Runs against the disposable cluster from scripts/test-container.sh.
"""
import ctypes as c
import math
from pathlib import Path
import random
import struct
import subprocess
import tempfile


def sql(query, db="postgres"):
    return subprocess.check_output(
        ["psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-d", db, "-c", query], text=True
    ).strip()


pq = c.CDLL("libpq.so.5")
for name, args, result in [
    ("PQconnectdb", [c.c_char_p], c.c_void_p),
    ("PQstatus", [c.c_void_p], c.c_int),
    ("PQfinish", [c.c_void_p], None),
    ("PQexecParams", [c.c_void_p, c.c_char_p, c.c_int, c.POINTER(c.c_uint),
                      c.POINTER(c.c_char_p), c.POINTER(c.c_int),
                      c.POINTER(c.c_int), c.c_int], c.c_void_p),
    ("PQresultStatus", [c.c_void_p], c.c_int),
    ("PQresultErrorMessage", [c.c_void_p], c.c_char_p),
    ("PQresultErrorField", [c.c_void_p, c.c_int], c.c_char_p),
    ("PQgetvalue", [c.c_void_p, c.c_int, c.c_int], c.c_void_p),
    ("PQgetlength", [c.c_void_p, c.c_int, c.c_int], c.c_int),
    ("PQclear", [c.c_void_p], None),
]:
    fn = getattr(pq, name)
    fn.argtypes, fn.restype = args, result

sql("CREATE EXTENSION pg_onesearch")
oid = int(sql("SELECT 'onesearch.vector'::regtype::oid"))
conn = pq.PQconnectdb(b"dbname=postgres")
assert pq.PQstatus(conn) == 0
checks = 0


def param(payload, query="SELECT $1", *, binary=True, output_binary=True, error=None):
    global checks
    types = (c.c_uint * 1)(oid)
    values = (c.c_char_p * 1)(payload)
    lengths = (c.c_int * 1)(len(payload))
    formats = (c.c_int * 1)(int(binary))
    result = pq.PQexecParams(conn, query.encode(), 1, types, values, lengths,
                             formats, int(output_binary))
    assert result
    try:
        if error:
            assert pq.PQresultStatus(result) == 7, "expected rejected input"
            state = pq.PQresultErrorField(result, ord("C")).decode()
            assert state == error, (state, pq.PQresultErrorMessage(result))
            value = None
        else:
            assert pq.PQresultStatus(result) == 2, pq.PQresultErrorMessage(result)
            value = c.string_at(pq.PQgetvalue(result, 0, 0), pq.PQgetlength(result, 0, 0))
        checks += 1
        return value
    finally:
        pq.PQclear(result)


try:
    wire = struct.pack("!iff", 2, 1.0, -2.0)
    assert param(wire) == wire
    assert param(b"[1,-2]", binary=False) == wire
    assert param(wire, output_binary=False) == b"[1,-2]"
    assert param(wire, "SELECT $1::onesearch.vector(2)") == wire
    param(wire, "SELECT $1::onesearch.vector(3)", error="22000")
    for dim in [-1, 0, 1, 4097, 2147483647]:
        param(struct.pack("!i", dim), error="22023")
    for bad in [wire[:3]]:
        param(bad, error="08P01")
    for bad in [wire[:4], wire[:-1], wire + b"x"]:
        param(bad, error="22P03")
    for bad in [float("nan"), float("inf"), -float("inf")]:
        param(struct.pack("!iff", 2, bad, 1), error="22000")
    # Independently calculated float64 references over float32-rounded inputs.
    rng = random.Random(260926)
    for dim in [2, 3, 17, 4096]:
        a = [rng.uniform(-1, 1) for _ in range(dim)]
        a_wire = struct.pack(f"!i{dim}f", dim, *a)
        a = struct.unpack(f"!{dim}f", a_wire[4:])
        assert param(a_wire) == a_wire
        text = param(a_wire, output_binary=False)
        assert param(text, binary=False) == a_wire
        b = [1.0] * dim
        expected = 1 - math.fsum(a) / math.sqrt(math.fsum(x*x for x in a) * dim)
        literal = "[" + ",".join(map(str, b)) + "]"
        actual = float(param(a_wire, f"SELECT onesearch.cosine_distance($1, '{literal}')",
                             output_binary=False))
        assert abs(actual - expected) < 1e-12, (actual, expected)
finally:
    pq.PQfinish(conn)

# Non-superuser use and ordinary PostgreSQL privileges.
sql("CREATE ROLE vector_reader")
assert sql("SET ROLE vector_reader; SELECT '[1,2]'::onesearch.vector").endswith("[1,2]")
sql("DROP ROLE vector_reader")
# Exercise receive through COPY and durable source storage through pg_dump/restore.
sql("CREATE TABLE source_vectors (id integer, body text, v onesearch.vector(2));"
    "INSERT INTO source_vectors VALUES (1, 'source stays in PG', '[1,-2]'), (2, 'nullable', NULL)")
with tempfile.TemporaryDirectory(prefix="onesearch-protocol-") as directory:
    binary_path = Path(directory) / "values.bin"
    sql(f"COPY source_vectors TO '{binary_path}' (FORMAT binary)")
    sql("CREATE TABLE copied_vectors (LIKE source_vectors)")
    sql(f"COPY copied_vectors FROM '{binary_path}' (FORMAT binary)")
    assert sql("SELECT count(*) FROM copied_vectors WHERE id = 1 AND v::text = '[1,-2]'") == "1"
    sql("CREATE DATABASE restored_vectors")
    dump = subprocess.check_output(["pg_dump", "-Fc", "-d", "postgres"])
    subprocess.run(["pg_restore", "--exit-on-error", "-d", "restored_vectors"], input=dump, check=True)
    assert sql("SELECT body || ':' || v::text FROM source_vectors WHERE id = 1",
               "restored_vectors") == "source stays in PG:[1,-2]"
    assert sql("SELECT count(*) FROM source_vectors WHERE v IS NULL", "restored_vectors") == "1"
    sql("DROP DATABASE restored_vectors")
print(f"PASS: {checks} extended-protocol cases, non-superuser use, binary COPY, logical dump/restore")
