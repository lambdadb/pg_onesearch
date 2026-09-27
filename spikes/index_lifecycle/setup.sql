CREATE EXTENSION pg_onesearch;
CREATE SCHEMA pgos_lifecycle_probe;
REVOKE ALL ON SCHEMA pgos_lifecycle_probe FROM PUBLIC;
CREATE FUNCTION pgos_lifecycle_probe.handler(internal) RETURNS index_am_handler
AS '$libdir/pg_onesearch_lifecycle_probe','pgos_lifecycle_handler' LANGUAGE C STRICT;
CREATE ACCESS METHOD pgos_lifecycle TYPE INDEX HANDLER pgos_lifecycle_probe.handler;
-- These register real paths only to prove that unpublished scans fail closed.
-- Text equality is a test strategy, not BM25 matching or a product operator.
CREATE OPERATOR CLASS pgos_lifecycle_probe.text_ops FOR TYPE text USING pgos_lifecycle
AS OPERATOR 1 = (text,text);
CREATE OPERATOR CLASS pgos_lifecycle_probe.vector_ops FOR TYPE onesearch.vector USING pgos_lifecycle
AS OPERATOR 1 onesearch.<=> (onesearch.vector,onesearch.vector) FOR ORDER BY pg_catalog.float_ops;
CREATE FUNCTION pgos_lifecycle_probe.info(regclass) RETURNS jsonb
AS '$libdir/pg_onesearch_lifecycle_probe','pgos_lifecycle_info' LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgos_lifecycle_probe FROM PUBLIC;
