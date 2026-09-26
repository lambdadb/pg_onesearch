-- Fixture only: superuser, one database, one source table, no remote service.
CREATE EXTENSION pg_onesearch;
CREATE SCHEMA onesearch_probe;
REVOKE ALL ON SCHEMA onesearch_probe FROM PUBLIC;
CREATE FUNCTION onesearch_probe.mark() RETURNS void
AS '$libdir/pg_onesearch_commit_probe', 'onesearch_probe_mark' LANGUAGE C;
CREATE FUNCTION onesearch_probe.start_worker() RETURNS integer
AS '$libdir/pg_onesearch_commit_probe', 'onesearch_probe_start' LANGUAGE C;
CREATE FUNCTION onesearch_probe.control(integer) RETURNS void
AS '$libdir/pg_onesearch_commit_probe', 'onesearch_probe_control' LANGUAGE C STRICT;
CREATE FUNCTION onesearch_probe.status() RETURNS text
AS '$libdir/pg_onesearch_commit_probe', 'onesearch_probe_status' LANGUAGE C;
CREATE FUNCTION onesearch_probe.last_result() RETURNS text
AS '$libdir/pg_onesearch_commit_probe', 'onesearch_probe_last' LANGUAGE C;
CREATE TABLE onesearch_probe.source (
    id bigint PRIMARY KEY, content text, embedding onesearch.vector(3)
);
CREATE TABLE onesearch_probe.outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    xid xid8 NOT NULL, operation text NOT NULL, payload jsonb NOT NULL
);
CREATE TABLE onesearch_probe.receipts (
    id bigint PRIMARY KEY, xid xid8 NOT NULL, operation text NOT NULL, payload jsonb NOT NULL
);
CREATE FUNCTION onesearch_probe.capture() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    INSERT INTO onesearch_probe.outbox(xid, operation, payload)
    VALUES (pg_current_xact_id(), TG_OP,
            jsonb_build_object('old', to_jsonb(OLD), 'new', to_jsonb(NEW)));
    PERFORM onesearch_probe.mark();
    RETURN NULL;
END
$$;
CREATE TRIGGER capture AFTER INSERT OR UPDATE OR DELETE ON onesearch_probe.source
FOR EACH ROW EXECUTE FUNCTION onesearch_probe.capture();
