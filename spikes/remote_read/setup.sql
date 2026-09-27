CREATE SCHEMA pgos_remote_probe;
REVOKE ALL ON SCHEMA pgos_remote_probe FROM PUBLIC;
CREATE FUNCTION pgos_remote_probe.query(collection text, tag text, query jsonb, size integer DEFAULT 10)
RETURNS jsonb AS '$libdir/pg_onesearch_remote_probe', 'pgos_remote_query'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON FUNCTION pgos_remote_probe.query(text, text, jsonb, integer) FROM PUBLIC;
