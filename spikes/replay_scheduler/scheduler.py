"""Serial, test-only automatic drain over the existing synchronous replay adapter.

One dedicated autocommit connection, one scheduler per database, no reconnect
inside an attempt. The caller restarts the process/connection after PG failure.
"""
from pathlib import Path
import sys

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'batch_replay'))
from worker import replay, ProbeError, require

S = 'pgos_scheduler_probe'
LOCK = (1885826931, 1920363641)


class Scheduler:
    def __init__(self, conn, remote_for, checkpoint=lambda generation, phase: None):
        self.conn = conn
        self.remote_for = remote_for
        self.checkpoint = checkpoint
        self.started = False

    def idle(self):
        require(self.conn.autocommit and self.conn.info.transaction_status == 0,
                'Scheduler requires an idle dedicated autocommit connection')

    def start(self):
        self.idle()
        require(not self.started, 'Scheduler already started')
        require(self.conn.execute(f'SELECT {S}.acquire()').fetchone()[0],
                'Another scheduler owns this database')
        self.started = True
        return self

    def close(self):
        if self.started:
            self.started = False
            if not self.conn.closed and not self.conn.broken:
                self.conn.execute('SELECT pg_advisory_unlock(%s,%s)', LOCK)

    def step(self):
        require(self.started, 'Scheduler is not started')
        self.idle()
        generation = self.conn.execute(f'SELECT {S}.next_generation()').fetchone()[0]
        if generation is None:
            return None
        self.checkpoint(generation, 'scheduled')
        try:
            result = replay(self.conn, generation, self.remote_for(generation),
                            checkpoint=lambda phase: self.checkpoint(generation, phase))
        except (ProbeError, psycopg.Error) as exc:
            # A broken PG session has lost its leadership and cannot finish or
            # reconnect into this attempt. The durable deadline survives exit.
            if self.conn.closed or self.conn.broken:
                raise
            self.idle()
            kind = 'database' if isinstance(exc, psycopg.Error) else 'remote'
            code = exc.sqlstate if isinstance(exc, psycopg.Error) else None
            self.conn.execute(f'SELECT {S}.finish(%s,%s,%s)', (generation, kind, code))
            return {'generation': generation, 'outcome': 'retry', 'error_kind': kind, 'sqlstate': code}
        self.conn.execute(f'SELECT {S}.finish(%s)', (generation,))
        return {'generation': generation, 'outcome': 'published' if result else 'idle'}

    def run(self, stop, poll_seconds=.05):
        require(.01 <= poll_seconds <= 5, 'Polling interval must be between 10 ms and 5 seconds')
        try:
            self.start()
            while not stop.is_set():
                # Always yield, including after failure or a successful busy
                # generation. Durable per-generation deadlines control retries.
                self.step()
                stop.wait(poll_seconds)
        finally:
            self.close()
