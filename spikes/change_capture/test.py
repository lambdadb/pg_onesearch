"""Logged registry/outbox semantics, using real lifecycle indexes and PG transactions."""
import json
import os
import subprocess
import unittest

import psycopg

P = 'pgos_capture_probe'


def command(*args, **kwargs):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT, **kwargs)


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg.connect(autocommit=True)
        self.addCleanup(lambda: self.conn.close())
        self.conn.execute('DROP SCHEMA IF EXISTS fixture CASCADE')
        self.conn.execute(f'TRUNCATE {P}.sources CASCADE')
        self.conn.execute('CREATE SCHEMA fixture')
        self.conn.execute('''CREATE TABLE fixture.docs(id bigint PRIMARY KEY, content text,
          embedding onesearch.vector(3), note text) WITH (fillfactor=50)''')
        self.conn.execute("INSERT INTO fixture.docs VALUES (1,'alpha','[1,0,0]','a'),(2,'beta','[0,1,0]','b'),(3,NULL,NULL,NULL)")
        self.conn.execute('CREATE INDEX txt ON fixture.docs USING pgos_lifecycle(content pgos_lifecycle_probe.text_ops)')
        self.conn.execute('CREATE INDEX vec ON fixture.docs USING pgos_lifecycle(embedding pgos_lifecycle_probe.vector_ops)')

    def register(self, name='txt', conn=None):
        return (conn or self.conn).execute(f'SELECT {P}.register_index(%s::regclass)', ('fixture.'+name,)).fetchone()[0]

    def both(self):
        return self.register(), self.register('vec')

    def events(self, conn=None):
        return (conn or self.conn).execute(f'''SELECT event_id,generation::text,writer_xid::text,
          operation,row_key,document FROM {P}.outbox ORDER BY event_id''').fetchall()

    def rows(self, conn=None):
        return (conn or self.conn).execute('SELECT id,content,embedding::text,note FROM fixture.docs ORDER BY id').fetchall()

    def test_registration_seeds_and_is_idempotent(self):
        text, vector = self.both()
        events = self.events()
        self.assertEqual(len(events), 6)  # create + two non-null source rows per index
        self.assertEqual(self.register(), text)
        self.assertEqual(self.events(), events)
        self.assertEqual(self.conn.execute(f'SELECT count(*) FROM {P}.sources').fetchone()[0], 1)
        generations = self.conn.execute(f'SELECT source_epoch,generation,resource_key,state FROM {P}.generations').fetchall()
        self.assertEqual(len({row[0] for row in generations}), 1)
        self.assertEqual(len({row[2] for row in generations}), 2)
        self.assertTrue(all(row[3] == 'capturing' for row in generations))
        vector_docs = [e[5] for e in events if e[1]==str(vector) and e[3]=='upsert']
        self.assertEqual(vector_docs, [{'id':'1','embedding':[1,0,0]}, {'id':'2','embedding':[0,1,0]}])
        print('REGISTRY:', json.dumps(generations, default=str), flush=True)

    def test_registration_rollback_removes_seed_and_triggers(self):
        with self.conn.transaction():
            self.conn.execute('SAVEPOINT attach')
            self.both()
            self.conn.execute('ROLLBACK TO attach')
        self.assertEqual(self.events(), [])
        self.assertEqual(self.conn.execute(f'SELECT count(*) FROM {P}.sources').fetchone()[0], 0)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM pg_trigger WHERE tgrelid='fixture.docs'::regclass AND NOT tgisinternal").fetchone()[0], 0)
        self.conn.execute("UPDATE fixture.docs SET note='unregistered' WHERE id=1")
        self.both()
        self.assertEqual(len(self.events()), 6)

    def test_registration_rechecks_definition_and_waits_for_prior_writes(self):
        self.conn.execute('ALTER TABLE fixture.docs ENABLE ROW LEVEL SECURITY')
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.register()
        self.conn.execute('ALTER TABLE fixture.docs DISABLE ROW LEVEL SECURITY')
        self.assertEqual(self.events(),[])
        with psycopg.connect() as writer, psycopg.connect(autocommit=True) as registering:
            writer.execute("INSERT INTO fixture.docs VALUES (9,'before seed','[1,0,0]',NULL)")
            registering.execute("SET lock_timeout='100ms'")
            with self.assertRaises(psycopg.errors.LockNotAvailable):
                self.register(conn=registering)
            writer.commit()
            self.register(conn=registering)
        self.assertEqual([(e[3],e[4]) for e in self.events()], [('create',None),('upsert',1),('upsert',2),('upsert',9)])

    def test_insert_update_delete_pk_change_and_null(self):
        text, vector = self.both()
        start = len(self.events())
        with self.conn.transaction():
            self.conn.execute("INSERT INTO fixture.docs VALUES(4,'new','[0,0,1]','n')")
            self.conn.execute("UPDATE fixture.docs SET id=40,content='moved' WHERE id=4")
            self.conn.execute('UPDATE fixture.docs SET embedding=NULL WHERE id=40')
            self.conn.execute('DELETE FROM fixture.docs WHERE id=40')
        events = self.events()[start:]
        self.assertEqual(len(events), 10)
        self.assertEqual(len({e[2] for e in events}), 1)
        for generation in (text,vector):
            specific = [(e[3],e[4]) for e in events if e[1]==str(generation)]
            self.assertEqual(specific[:3], [('upsert',4),('delete',4),('upsert',40)])
            self.assertEqual(specific[-1], ('delete',40))
        self.assertIn(('delete',40), [(e[3],e[4]) for e in events if e[1]==str(vector)])
        self.assertEqual([r[0] for r in self.rows()], [1,2,3])

    def test_copy_and_on_conflict_capture_each_result(self):
        self.both()
        start = len(self.events())
        with self.conn.cursor().copy('COPY fixture.docs FROM STDIN') as copy:
            for i in range(10,30):
                copy.write_row((i,'bulk','[1,0,0]','copy'))
        events = self.events()[start:]
        self.assertEqual(len(events), 40)
        self.assertEqual(len({e[2] for e in events}), 1)
        self.conn.execute("INSERT INTO fixture.docs VALUES (10,'conflict','[0,1,0]','upsert') ON CONFLICT(id) DO UPDATE SET content=excluded.content,embedding=excluded.embedding")
        self.assertEqual(len(self.events()), start+42)
        self.conn.execute("INSERT INTO fixture.docs(id) VALUES (10) ON CONFLICT DO NOTHING")
        self.assertEqual(len(self.events()), start+42)

    def test_hot_update_is_captured(self):
        self.both()
        start = len(self.events())
        self.conn.execute('SELECT pg_stat_force_next_flush()')
        self.conn.execute('SELECT pg_stat_clear_snapshot()')
        before = self.conn.execute("SELECT n_tup_hot_upd FROM pg_stat_all_tables WHERE relid='fixture.docs'::regclass").fetchone()[0]
        self.conn.execute("UPDATE fixture.docs SET note='HOT only' WHERE id=1")
        self.conn.execute('SELECT pg_stat_force_next_flush()')
        self.conn.execute('SELECT pg_stat_clear_snapshot()')
        after = self.conn.execute("SELECT n_tup_hot_upd FROM pg_stat_all_tables WHERE relid='fixture.docs'::regclass").fetchone()[0]
        self.assertGreater(after,before)
        self.assertEqual(len(self.events()), start+2)
        self.assertTrue(all(e[3]=='upsert' and e[4]==1 for e in self.events()[start:]))
        print('HOT: observed physical HOT update and two generation records', flush=True)

    def test_rollback_savepoint_and_cross_index_capture_failure(self):
        self.both()
        before, rows = self.events(), self.rows()
        with self.conn.transaction():
            self.conn.execute('SAVEPOINT changed')
            self.conn.execute("UPDATE fixture.docs SET content='rollback' WHERE id=1")
            self.assertEqual(len(self.events()),len(before)+2)
            self.conn.execute('ROLLBACK TO changed')
            self.assertEqual(self.events(),before)
        self.assertEqual(self.rows(),rows)
        with self.assertRaises(psycopg.errors.DataException):
            with self.conn.transaction():
                self.conn.execute("UPDATE fixture.docs SET content='also rollback' WHERE id=1")
                self.conn.execute("UPDATE fixture.docs SET embedding='[0,0,0]' WHERE id=2")
        self.assertEqual(self.events(),before)
        self.assertEqual(self.rows(),rows)
        # Fail on the second sorted generation, after the first record was inserted.
        self.conn.execute(f'''CREATE FUNCTION fixture.reject_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN IF NEW.operation='upsert' AND NEW.generation=(SELECT max(generation::text)::uuid FROM {P}.generations)
          THEN RAISE EXCEPTION 'injected outbox failure'; END IF; RETURN NEW; END $$''')
        self.conn.execute(f'CREATE TRIGGER reject_outbox BEFORE INSERT ON {P}.outbox FOR EACH ROW EXECUTE FUNCTION fixture.reject_outbox()')
        try:
            with self.assertRaises(psycopg.errors.RaiseException):
                self.conn.execute("UPDATE fixture.docs SET content='must not commit' WHERE id=1")
            self.assertEqual(self.rows(),rows)
            self.assertEqual(self.events(),before)
        finally:
            self.conn.execute(f'DROP TRIGGER reject_outbox ON {P}.outbox')

    def test_deferred_commit_failure_rolls_back_capture(self):
        self.both()
        before, rows = self.events(),self.rows()
        self.conn.execute('ALTER TABLE fixture.docs ADD CONSTRAINT unique_note UNIQUE(note) DEFERRABLE INITIALLY DEFERRED')
        with self.assertRaises(psycopg.errors.UniqueViolation):
            with self.conn.transaction():
                self.conn.execute("UPDATE fixture.docs SET note='same' WHERE id IN (1,2)")
                self.assertEqual(len(self.events()),len(before)+4)
        self.assertEqual(self.events(),before)
        self.assertEqual(self.rows(),rows)

    def test_committed_visibility_and_late_smaller_event_id(self):
        self.both()
        before = self.events()
        with psycopg.connect() as a, psycopg.connect() as b, psycopg.connect(autocommit=True) as observer:
            a.execute("UPDATE fixture.docs SET note='late' WHERE id=1")
            small = a.execute(f'SELECT max(event_id) FROM {P}.outbox').fetchone()[0]
            b.execute("UPDATE fixture.docs SET note='early' WHERE id=2")
            large = b.execute(f'SELECT max(event_id) FROM {P}.outbox').fetchone()[0]
            self.assertLess(small,large)
            self.assertEqual(self.events(observer),before)
            b.commit()
            self.assertEqual(len(self.events(observer)),len(before)+2)
            a.commit()
            all_events = self.events(observer)
            self.assertEqual(len(all_events),len(before)+4)
            self.assertEqual(sum(e[0] <= small for e in all_events[len(before):]),2)
            print('ORDERING: higher event IDs committed first; lower IDs remained discoverable', flush=True)

    def test_registration_excludes_concurrent_writers(self):
        with psycopg.connect() as registering, psycopg.connect(autocommit=True) as writer:
            self.register(conn=registering)
            writer.execute("SET lock_timeout='100ms'")
            with self.assertRaises(psycopg.errors.LockNotAvailable):
                writer.execute("INSERT INTO fixture.docs VALUES(9,'after','[1,0,0]',NULL)")
            registering.commit()
            writer.execute("INSERT INTO fixture.docs VALUES(9,'after','[1,0,0]',NULL)")
        self.assertEqual([(e[3],e[4]) for e in self.events()], [('create',None),('upsert',1),('upsert',2),('upsert',9)])

    def test_reindex_registration_and_drop_keep_retirement_work(self):
        old, sibling = self.both()
        before, rows = self.events(),self.rows()
        with self.conn.transaction():
            self.conn.execute('SAVEPOINT rebuilt')
            self.conn.execute('REINDEX INDEX fixture.txt')
            new = self.register()
            self.assertNotEqual(old,new)
            self.conn.execute('ROLLBACK TO rebuilt')
        self.assertEqual(self.events(),before)
        self.assertEqual(self.register(),old)
        self.conn.execute('REINDEX INDEX fixture.txt')
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute("UPDATE fixture.docs SET note='blocked' WHERE id=1")
        new = self.register()
        self.assertNotEqual(old,new)
        self.assertEqual(self.conn.execute(f'SELECT state FROM {P}.generations WHERE generation=%s',(old,)).fetchone()[0], 'retire_pending')
        with self.conn.transaction():
            self.conn.execute('SAVEPOINT drop_index')
            self.conn.execute('DROP INDEX fixture.txt')
            self.assertEqual(self.events()[-1][3], 'retire')
            self.conn.execute('ROLLBACK TO drop_index')
        self.conn.execute('DROP INDEX fixture.txt')
        self.assertEqual(self.rows(),rows)
        tail = len(self.events())
        self.conn.execute("UPDATE fixture.docs SET note='one index remains' WHERE id=1")
        self.assertEqual([(e[1],e[3]) for e in self.events()[tail:]],[(str(sibling),'upsert')])
        self.conn.execute('DROP TABLE fixture.docs')
        self.assertEqual(self.conn.execute(f"SELECT count(*) FROM {P}.generations WHERE state='retire_pending'").fetchone()[0],3)
        self.assertEqual(self.conn.execute(f"SELECT count(*) FROM {P}.sources WHERE state='retired'").fetchone()[0],1)
        self.assertEqual(sum(e[3]=='retire' for e in self.events()),3)

    def test_rename_and_fail_closed_contexts(self):
        self.both()
        self.conn.execute('ALTER TABLE fixture.docs RENAME COLUMN content TO body')
        self.conn.execute('ALTER TABLE fixture.docs RENAME COLUMN id TO key')
        self.conn.execute("UPDATE fixture.docs SET body='renamed' WHERE key=1")
        self.assertTrue(any(e[5]=={'id':'1','content':'renamed'} for e in self.events()))
        with self.assertRaises(psycopg.errors.FeatureNotSupported):
            self.conn.execute('TRUNCATE fixture.docs')
        with self.assertRaises(psycopg.errors.FeatureNotSupported):
            with self.conn.transaction():
                self.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                self.conn.execute("UPDATE fixture.docs SET note='unsupported' WHERE key=1")
        self.conn.execute('SET session_replication_role=replica')
        try:
            with self.assertRaises(psycopg.errors.FeatureNotSupported):
                self.conn.execute("UPDATE fixture.docs SET note='bypass' WHERE key=1")
        finally:
            self.conn.execute('SET session_replication_role=origin')
        self.conn.execute('ALTER TABLE fixture.docs DROP CONSTRAINT docs_pkey')
        with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
            self.conn.execute("UPDATE fixture.docs SET note='no key' WHERE key=1")

    def test_wal_recovery_keeps_only_committed_source_and_events(self):
        self.both()
        self.conn.execute('CHECKPOINT')
        self.conn.execute("UPDATE fixture.docs SET note='durable' WHERE id=1")
        rows,events = self.rows(),self.events()
        pending = psycopg.connect()
        pending.execute("UPDATE fixture.docs SET note='uncommitted' WHERE id=2")
        command('gosu','postgres','pg_ctl','-m','immediate','-w','stop')
        command('gosu','postgres','pg_ctl','-l',os.environ['PGDATA']+'/server.log','-w','start')
        pending.close()
        self.conn.close()
        self.conn = psycopg.connect(autocommit=True)
        self.assertEqual(self.rows(),rows)
        self.assertEqual(self.events(),events)
        self.assertEqual(self.conn.execute(f"SELECT relpersistence FROM pg_class WHERE oid IN ('{P}.sources'::regclass,'{P}.generations'::regclass,'{P}.outbox'::regclass)").fetchall(),[('p',),('p',),('p',)])
        print('RECOVERY: committed source and outbox preserved together; uncommitted pair absent', flush=True)

    def test_logical_restore_preserves_backlog_but_refuses_capture(self):
        self.both()
        self.conn.execute("UPDATE fixture.docs SET content='pending' WHERE id=1")
        self.conn.execute('DROP INDEX fixture.txt')
        before = self.events()
        dump = command('pg_dump','--no-owner','--no-privileges')
        command('createdb','capture_restore')
        try:
            subprocess.run(['psql','-X','-v','ON_ERROR_STOP=1','-d','capture_restore'],input=dump,text=True,
                           stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True)
            with psycopg.connect(dbname='capture_restore',autocommit=True) as restored:
                self.assertEqual(self.events(restored),before)
                with self.assertRaisesRegex(psycopg.errors.ObjectNotInPrerequisiteState,'restore fencing'):
                    restored.execute("UPDATE fixture.docs SET note='must not replay into original' WHERE id=1")
            print('RESTORE: pending records/retirement retained; copied installation cannot capture', flush=True)
        finally:
            command('dropdb','capture_restore')


if __name__ == '__main__':
    unittest.main(verbosity=2)
