"""Real IAM DDL, WAL recovery, source preservation, and unpublished scan tests."""
import json
import os
import subprocess
import unittest
import uuid

import psycopg


def command(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT)


def restart():
    command('gosu', 'postgres', 'pg_ctl', '-m', 'immediate', '-w', 'stop')
    command('gosu', 'postgres', 'pg_ctl', '-l', os.environ['PGDATA'] + '/server.log', '-w', 'start')


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg.connect(autocommit=True)
        self.addCleanup(lambda: self.conn.close())
        self.conn.execute('DROP SCHEMA IF EXISTS fixture CASCADE')
        self.conn.execute('CREATE SCHEMA fixture')
        self.conn.execute('''CREATE TABLE fixture.docs (
          id bigint PRIMARY KEY, content text, embedding onesearch.vector(3))''')
        self.conn.execute("INSERT INTO fixture.docs VALUES (1,'alpha','[1,0,0]'), (2,'beta','[0,1,0]'), (3,NULL,NULL)")

    def build(self, name='vec', column='embedding', table='fixture.docs', modifier=''):
        opclass = 'vector_ops' if column == 'embedding' else 'text_ops'
        self.conn.execute(f'CREATE INDEX {modifier} {name} ON {table} USING pgos_lifecycle ({column} pgos_lifecycle_probe.{opclass})')

    def info(self, name='vec', connection=None):
        return (connection or self.conn).execute('SELECT pgos_lifecycle_probe.info(%s::regclass)', ('fixture.' + name,)).fetchone()[0]

    def rows(self):
        return self.conn.execute('SELECT id,content,embedding::text FROM fixture.docs ORDER BY id').fetchall()

    def test_independent_generations_and_rename(self):
        self.build()
        self.build('txt', 'content')
        self.build('vec2')
        vector, text = self.info(), self.info('txt')
        self.assertEqual(len({vector['generation'], text['generation'], self.info('vec2')['generation']}), 3)
        self.assertEqual(uuid.UUID(vector['generation']).version, 4)
        self.assertEqual(vector['dimensions'], 3)
        self.assertEqual(vector['state'], 'unpublished')
        self.assertEqual(text['mode'], 'text')
        self.conn.execute('ALTER INDEX fixture.vec RENAME TO renamed')
        self.assertEqual(self.info('renamed'), vector)
        self.conn.execute('ALTER TABLE fixture.docs RENAME COLUMN embedding TO vector_value')
        self.assertEqual(self.info('renamed'), vector)
        print('IDENTITIES:', json.dumps({'vector': vector, 'text': text}), flush=True)

    def test_reindex_and_drop_rollback_preserve_source(self):
        self.build()
        self.build('txt', 'content')
        before, sibling, rows = self.info(), self.info('txt'), self.rows()
        with self.conn.transaction():
            self.conn.execute('SAVEPOINT rebuild')
            self.conn.execute('REINDEX INDEX fixture.vec')
            self.assertNotEqual(self.info()['generation'], before['generation'])
            self.conn.execute('ROLLBACK TO rebuild')
            self.assertEqual(self.info(), before)
            self.conn.execute('SAVEPOINT dropped')
            self.conn.execute('DROP INDEX fixture.vec')
            self.conn.execute('ROLLBACK TO dropped')
            self.assertEqual(self.info(), before)
        self.conn.execute('REINDEX INDEX fixture.vec')
        self.assertNotEqual(self.info()['generation'], before['generation'])
        self.assertEqual(self.info('txt'), sibling)
        self.conn.execute('DROP INDEX fixture.vec')
        self.assertEqual(self.info('txt'), sibling)
        self.assertEqual(self.rows(), rows)
        self.build()
        self.assertNotEqual(self.info()['generation'], before['generation'])
        self.conn.execute('DROP INDEX fixture.vec')
        self.conn.execute('DROP INDEX fixture.txt')
        self.assertEqual(self.rows(), rows)

    def test_create_rollback_and_failed_build(self):
        with self.conn.transaction():
            self.conn.execute('SAVEPOINT created')
            self.build()
            self.conn.execute('ROLLBACK TO created')
        self.assertIsNone(self.conn.execute("SELECT to_regclass('fixture.vec')").fetchone()[0])
        self.build('txt', 'content')
        before = self.info('txt')
        self.conn.execute("UPDATE fixture.docs SET embedding='[0,0,0]' WHERE id=1")
        with self.assertRaises(psycopg.errors.DataException):
            self.build()
        self.assertIsNone(self.conn.execute("SELECT to_regclass('fixture.vec')").fetchone()[0])
        self.assertEqual(self.info('txt'), before)
        self.assertEqual(self.rows()[0][2], '[0,0,0]')

    def test_dml_validation_and_vacuum(self):
        self.build()
        before = self.info()
        with self.assertRaises(psycopg.errors.DataException):
            self.conn.execute("UPDATE fixture.docs SET embedding='[0,0,0]' WHERE id=1")
        self.assertEqual(self.rows()[0][2], '[1,0,0]')
        self.conn.execute("UPDATE fixture.docs SET content='changed' WHERE id=1")
        self.conn.execute("UPDATE fixture.docs SET embedding='[1,1,0]' WHERE id=2")
        self.conn.execute('DELETE FROM fixture.docs WHERE id=3')
        self.conn.execute("INSERT INTO fixture.docs VALUES (4,'new','[0,0,1]')")
        self.conn.execute('VACUUM fixture.docs')
        self.assertEqual(self.info(), before)
        rows = self.rows()
        self.conn.execute('VACUUM FULL fixture.docs')
        self.assertNotEqual(self.info()['generation'], before['generation'])
        self.assertEqual(self.rows(), rows)

    def test_truncate_rollback_and_new_generation(self):
        self.build()
        before, rows = self.info(), self.rows()
        with self.conn.transaction():
            self.conn.execute('SAVEPOINT emptied')
            self.conn.execute('TRUNCATE fixture.docs')
            self.assertNotEqual(self.info()['generation'], before['generation'])
            self.conn.execute('ROLLBACK TO emptied')
        self.assertEqual(self.info(), before)
        self.assertEqual(self.rows(), rows)
        self.conn.execute('TRUNCATE fixture.docs')
        self.assertNotEqual(self.info()['generation'], before['generation'])
        self.assertEqual(self.rows(), [])

    def test_unpublished_actual_index_paths_fail(self):
        self.build()
        self.build('txt', 'content')
        self.conn.execute('SET enable_seqscan=off')
        queries = ["SELECT id FROM fixture.docs WHERE content='alpha'",
                   "SELECT id FROM fixture.docs ORDER BY embedding OPERATOR(onesearch.<=>) '[1,0,0]'::onesearch.vector LIMIT 2"]
        for sql in queries:
            plan = self.conn.execute('EXPLAIN (FORMAT JSON) ' + sql).fetchone()[0][0]['Plan']
            def nodes(node):
                yield node
                for child in node.get('Plans', []):
                    yield from nodes(child)
            self.assertTrue(any(n['Node Type'] == 'Index Scan' for n in nodes(plan)), plan)
            print('UNPUBLISHED PLAN:', json.dumps(plan), flush=True)
            with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
                self.conn.execute(sql)
        self.conn.execute('SET enable_seqscan=on')
        self.assertEqual(len(self.rows()), 3)
        self.conn.execute('TRUNCATE fixture.docs')
        self.conn.execute('SET enable_seqscan=off')
        for sql in queries:
            with self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState):
                self.conn.execute(sql)

    def test_rejected_definitions_and_privileges(self):
        statements = [
            'CREATE INDEX bad ON fixture.docs USING pgos_lifecycle ((lower(content)) pgos_lifecycle_probe.text_ops)',
            'CREATE INDEX bad ON fixture.docs USING pgos_lifecycle (content pgos_lifecycle_probe.text_ops) WHERE id>1',
            'CREATE INDEX bad ON fixture.docs USING pgos_lifecycle (content pgos_lifecycle_probe.text_ops) INCLUDE (id)',
            'CREATE UNIQUE INDEX bad ON fixture.docs USING pgos_lifecycle (content pgos_lifecycle_probe.text_ops)',
        ]
        for sql in statements:
            with self.subTest(sql=sql):
                with self.assertRaises(psycopg.errors.FeatureNotSupported):
                    self.conn.execute(sql)
        with self.assertRaises(psycopg.errors.InvalidParameterValue):
            self.conn.execute('CREATE INDEX bad ON fixture.docs USING pgos_lifecycle (content pgos_lifecycle_probe.text_ops) WITH (fillfactor=90)')
        for prefix, name, definition in (
                ('UNLOGGED', 'unlogged', 'id bigint PRIMARY KEY, content text'),
                ('TEMP', 'temporary', 'id bigint PRIMARY KEY, content text'),
                ('', 'missingpk', 'id bigint, content text'),
                ('', 'wrongpk', 'id text PRIMARY KEY, content text'),
                ('', 'compositepk', 'id bigint, n int, content text, PRIMARY KEY(id,n)'),
                ('', 'loosevector', 'id bigint PRIMARY KEY, embedding onesearch.vector'),
                ('', 'partitioned', 'id bigint PRIMARY KEY, content text') ):
            name = name if prefix == 'TEMP' else 'fixture.' + name
            suffix = ' PARTITION BY RANGE(id)' if name.endswith('partitioned') else ''
            self.conn.execute(f'CREATE {prefix} TABLE {name} ({definition}){suffix}')
            if suffix:
                self.conn.execute('CREATE TABLE fixture.part PARTITION OF fixture.partitioned FOR VALUES FROM (0) TO (10)')
            with self.assertRaises(psycopg.errors.FeatureNotSupported):
                self.build('bad', 'embedding' if 'embedding' in definition else 'content', name)
        self.conn.execute('ALTER TABLE fixture.docs ENABLE ROW LEVEL SECURITY')
        with self.assertRaises(psycopg.errors.FeatureNotSupported):
            self.build()
        self.conn.execute('ALTER TABLE fixture.docs DISABLE ROW LEVEL SECURITY')
        self.build()
        self.conn.execute('CREATE ROLE lifecycle_reader')
        def drop_reader():
            self.conn.execute('RESET ROLE')
            self.conn.execute('DROP OWNED BY lifecycle_reader')
            self.conn.execute('DROP ROLE lifecycle_reader')
        self.addCleanup(drop_reader)
        self.conn.execute('GRANT USAGE ON SCHEMA pgos_lifecycle_probe,fixture TO lifecycle_reader')
        self.conn.execute('GRANT EXECUTE ON FUNCTION pgos_lifecycle_probe.info(regclass) TO lifecycle_reader')
        self.conn.execute('SET ROLE lifecycle_reader')
        with self.assertRaisesRegex(psycopg.errors.InsufficientPrivilege, 'lifecycle probe requires superuser'):
            self.info()
        self.conn.execute('RESET ROLE')
        with self.assertRaises(psycopg.errors.FeatureNotSupported):
            self.conn.execute("SELECT pgos_lifecycle_probe.info('fixture.docs')")

    def test_concurrent_build_rejected_and_recoverable(self):
        with self.assertRaises(psycopg.errors.FeatureNotSupported):
            self.build(modifier='CONCURRENTLY')
        valid = self.conn.execute("SELECT indisvalid FROM pg_index WHERE indexrelid='fixture.vec'::regclass").fetchone()[0]
        self.assertFalse(valid)  # PG keeps a failed concurrent build's invalid shell.
        self.conn.execute('DROP INDEX fixture.vec')
        self.build()
        before = self.info()
        with self.assertRaises(psycopg.errors.FeatureNotSupported):
            self.conn.execute('REINDEX INDEX CONCURRENTLY fixture.vec')
        self.assertEqual(self.info(), before)
        invalid = self.conn.execute("""SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
          WHERE i.indrelid='fixture.docs'::regclass AND NOT i.indisvalid""").fetchall()
        self.assertEqual(len(invalid), 1)
        for (name,) in invalid:
            self.conn.execute(psycopg.sql.SQL('DROP INDEX fixture.{}').format(psycopg.sql.Identifier(name)))
        self.conn.execute('REINDEX INDEX fixture.vec')
        self.assertNotEqual(self.info()['generation'], before['generation'])

    def test_lock_blocks_reindex_until_reader_finishes(self):
        self.build()
        before = self.info()
        with psycopg.connect(autocommit=True) as other:
            with self.conn.transaction():
                self.info()  # Holds the index AccessShareLock until transaction end.
                other.execute("SET lock_timeout='100ms'")
                with self.assertRaises(psycopg.errors.LockNotAvailable):
                    other.execute('REINDEX INDEX fixture.vec')
            other.execute('REINDEX INDEX fixture.vec')
        self.assertNotEqual(self.info()['generation'], before['generation'])

    def test_wal_recovery_keeps_committed_and_discards_uncommitted(self):
        self.build()
        self.build('txt', 'content')
        before, sibling, rows = self.info(), self.info('txt'), self.rows()
        self.conn.execute('CHECKPOINT')
        self.conn.execute('REINDEX INDEX fixture.vec')
        committed = self.info()
        self.assertNotEqual(committed['generation'], before['generation'])
        uncommitted = psycopg.connect()
        uncommitted.execute('REINDEX INDEX fixture.txt')
        uncommitted.execute("UPDATE fixture.docs SET content='not committed' WHERE id=1")
        restart()
        uncommitted.close()
        self.conn.close()
        self.conn = psycopg.connect(autocommit=True)
        self.assertEqual(self.info(), committed)
        self.assertEqual(self.info('txt'), sibling)
        self.assertEqual(self.rows(), rows)
        print('RECOVERY: committed generation preserved; uncommitted rebuild/source update rolled back', flush=True)

    def test_logical_restore_allocates_new_generations(self):
        self.build()
        self.build('txt', 'content')
        before = [self.info()['generation'], self.info('txt')['generation']]
        rows = self.rows()
        dump = command('pg_dump', '--no-owner', '--no-privileges')
        command('createdb', 'lifecycle_restore')
        try:
            subprocess.run(['psql', '-X', '-v', 'ON_ERROR_STOP=1', '-d', 'lifecycle_restore'], input=dump,
                           text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
            with psycopg.connect(dbname='lifecycle_restore', autocommit=True) as restored:
                after = [self.info(connection=restored)['generation'], self.info('txt', restored)['generation']]
                self.assertTrue(set(before).isdisjoint(after))
                self.assertEqual(restored.execute('SELECT id,content,embedding::text FROM fixture.docs ORDER BY id').fetchall(), rows)
            print('RESTORE: new generations; original text/vectors preserved', flush=True)
        finally:
            command('dropdb', 'lifecycle_restore')


if __name__ == '__main__':
    unittest.main(verbosity=2)
