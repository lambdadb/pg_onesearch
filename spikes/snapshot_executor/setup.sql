CREATE SCHEMA pgos_snapshot_executor;
REVOKE ALL ON SCHEMA pgos_snapshot_executor FROM PUBLIC;
CREATE FUNCTION pgos_snapshot_executor.match(regclass,text,text) RETURNS boolean
AS '$libdir/pg_onesearch_snapshot_executor','pgos_executor_match' LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
CREATE FUNCTION pgos_snapshot_executor.vector_match(regclass,onesearch.vector,onesearch.vector) RETURNS boolean
AS '$libdir/pg_onesearch_snapshot_executor','pgos_executor_match' LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
CREATE FUNCTION pgos_snapshot_executor.score(regclass,bigint) RETURNS double precision
AS '$libdir/pg_onesearch_snapshot_executor','pgos_executor_score' LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgos_snapshot_executor FROM PUBLIC;
