CREATE EXTENSION pg_onesearch;
SELECT extversion FROM pg_extension WHERE extname = 'pg_onesearch';
SELECT count(*) AS pgvector_dependencies FROM pg_extension WHERE extname = 'vector';
SELECT ' [1, 2.5, -3e-2] '::onesearch.vector;
SET extra_float_digits = -3;
SELECT '[0.1,1.2345678]'::onesearch.vector;
RESET extra_float_digits;
SELECT encode(onesearch.vector_send('[1,-2]'::onesearch.vector), 'hex');
SELECT '[0,0]'::onesearch.vector;
SELECT '[1,0]'::onesearch.vector OPERATOR(onesearch.<=>) '[1,0]'::onesearch.vector AS same,
       '[1,0]'::onesearch.vector OPERATOR(onesearch.<=>) '[0,1]'::onesearch.vector AS orthogonal,
       '[1,0]'::onesearch.vector OPERATOR(onesearch.<=>) '[-1,0]'::onesearch.vector AS opposite;
SELECT abs(onesearch.cosine_distance('[1,2,3]', '[4,5,6]') -
           (1 - 32.0 / sqrt(14.0 * 77.0))) < 1e-14 AS reference_matches;
SELECT onesearch.cosine_distance('[3e38,3e38]', '[3e38,-3e38]') AS large_finite,
       onesearch.cosine_distance('[1e-40,0]', '[0,1e-40]') AS subnormal;
SELECT onesearch.cosine_distance(NULL, '[1,0]') IS NULL AS strict_null;
CREATE TABLE vectors (id integer PRIMARY KEY, content text, v onesearch.vector(3));
INSERT INTO vectors VALUES (1, 'first', '[1,0,0]'), (2, 'second', '[0,1,0]'), (3, 'third', '[-1,0,0]');
PREPARE nearest(onesearch.vector) AS
    SELECT id FROM vectors ORDER BY v OPERATOR(onesearch.<=>) $1, id LIMIT 2;
EXECUTE nearest('[1,0,0]');
EXPLAIN (COSTS OFF) SELECT id FROM vectors
ORDER BY v OPERATOR(onesearch.<=>) '[1,0,0]'::onesearch.vector LIMIT 2;
SELECT format_type(atttypid, atttypmod) FROM pg_attribute
WHERE attrelid = 'vectors'::regclass AND attname = 'v';
-- Assignment of an already typed value must enforce the column's typmod too.
INSERT INTO vectors VALUES (4, 'wrong', '[1,2]'::onesearch.vector);
SELECT '[1,2]'::onesearch.vector(3);
SELECT onesearch.cosine_distance('[1,2]', '[1,2,3]');
SELECT onesearch.cosine_distance('[0,0]', '[1,0]');
SELECT '[]'::onesearch.vector;
SELECT '[1]'::onesearch.vector;
SELECT '[1,2]'::onesearch.vector(1);
SELECT '[1,2]'::onesearch.vector(4097);
SELECT '[1,2]'::onesearch.vector(2,3);
SELECT '[NaN,1]'::onesearch.vector;
SELECT '[Infinity,1]'::onesearch.vector;
SELECT '[-Infinity,1]'::onesearch.vector;
SELECT '[1e100,1]'::onesearch.vector;
SELECT '[1e-100,1]'::onesearch.vector;
SELECT ''::onesearch.vector;
SELECT '1,2'::onesearch.vector;
SELECT '[1,]'::onesearch.vector;
SELECT '[,2]'::onesearch.vector;
SELECT '[1 2,3]'::onesearch.vector;
SELECT '[1,2'::onesearch.vector;
SELECT '[1,2]junk'::onesearch.vector;
SELECT '[1,,2]'::onesearch.vector;
SELECT ('[' || repeat('1,',4096) || '1]')::onesearch.vector;
-- Force an out-of-line value and exercise detoasting in output and distance.
CREATE TABLE large_vectors (v onesearch.vector(4096));
INSERT INTO large_vectors SELECT ('[' || string_agg((i::real / 4096)::text, ',') || ']')::onesearch.vector
FROM generate_series(1,4096) i;
SELECT length(v::text) > 16000 AS large_output,
       abs(onesearch.cosine_distance(v,v)) < 1e-14 AS toasted_distance,
       v::text = (v::text::onesearch.vector)::text AS text_roundtrip
FROM large_vectors;
SELECT pg_relation_size(reltoastrelid) > 0 AS toast_has_storage FROM pg_class WHERE oid = 'large_vectors'::regclass;
BEGIN;
UPDATE vectors SET v = '[0,0,1]' WHERE id = 1;
SAVEPOINT before_delete;
DELETE FROM vectors WHERE id = 2;
ROLLBACK TO before_delete;
ROLLBACK;
SELECT id, content, v FROM vectors ORDER BY id;
DEALLOCATE nearest;
DROP TABLE large_vectors, vectors;
DROP EXTENSION pg_onesearch;
CREATE EXTENSION pg_onesearch;
DROP EXTENSION pg_onesearch;
