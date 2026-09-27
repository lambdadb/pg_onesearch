-- Optional integration probe. Install after capture, replay, and snapshot SQL.
CREATE SCHEMA pgos_completion_probe;
REVOKE ALL ON SCHEMA pgos_completion_probe FROM PUBLIC;
CREATE FUNCTION pgos_completion_probe.mark() RETURNS void
AS '$libdir/pg_onesearch_commit_probe', 'onesearch_probe_mark' LANGUAGE C;
CREATE FUNCTION pgos_completion_probe.start_worker() RETURNS integer
AS '$libdir/pg_onesearch_commit_probe', 'onesearch_probe_start_publication' LANGUAGE C;
CREATE FUNCTION pgos_completion_probe.control(integer) RETURNS void
AS '$libdir/pg_onesearch_commit_probe', 'onesearch_probe_control' LANGUAGE C STRICT;
CREATE FUNCTION pgos_completion_probe.worker_status() RETURNS text
AS '$libdir/pg_onesearch_commit_probe', 'onesearch_probe_status' LANGUAGE C;
CREATE FUNCTION pgos_completion_probe.last_result() RETURNS text
AS '$libdir/pg_onesearch_commit_probe', 'onesearch_probe_last' LANGUAGE C;

-- Membership is recorded atomically with every outbox INSERT, including events
-- coalesced by remote replay. No max(event_id) cursor or shared-memory receipt.
CREATE TABLE pgos_completion_probe.requirements (
    event_id bigint PRIMARY KEY,
    writer_xid xid8 NOT NULL,
    generation uuid NOT NULL REFERENCES pgos_capture_probe.generations
);
CREATE INDEX requirements_writer ON pgos_completion_probe.requirements(writer_xid);
CREATE FUNCTION pgos_completion_probe.capture() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    PERFORM pgos_capture_probe.assert_context();
    IF NEW.writer_xid <> pg_current_xact_id() THEN
        RAISE EXCEPTION 'completion capture requires the source transaction identity' USING ERRCODE='55000';
    END IF;
    INSERT INTO pgos_completion_probe.requirements VALUES(NEW.event_id,NEW.writer_xid,NEW.generation);
    PERFORM pgos_completion_probe.mark();
    RETURN NULL;
END $$;
CREATE TRIGGER completion AFTER INSERT ON pgos_capture_probe.outbox
FOR EACH ROW EXECUTE FUNCTION pgos_completion_probe.capture();

-- publish() already checks exact immutable event payloads against the outbox.
-- The remaining join proves each required event belongs to a committed receipt
-- for its own generation and full writer xid, including all sibling indexes.
CREATE VIEW pgos_completion_probe.coverage AS
SELECT r.*, coalesce(b.state='published',false) AS published
FROM pgos_completion_probe.requirements r
LEFT JOIN pgos_replay_probe.events e ON e.event_id=r.event_id AND e.writer_xid=r.writer_xid
LEFT JOIN pgos_replay_probe.batches b ON b.id=e.batch_id AND b.generation=r.generation;
CREATE FUNCTION pgos_completion_probe.status(writer xid8)
RETURNS TABLE(generation uuid, required_events bigint, published_events bigint)
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $$
    SELECT generation,count(*),count(*) FILTER (WHERE published)
    FROM pgos_completion_probe.coverage WHERE writer_xid=writer GROUP BY generation
$$;
CREATE FUNCTION pgos_completion_probe.is_published(writer xid8) RETURNS boolean
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $$
    SELECT count(*)>0 AND coalesce(bool_and(published),false)
    FROM pgos_completion_probe.coverage WHERE writer_xid=writer
$$;

-- Conservative operational gate from source commit onward, even before claim.
-- This does not alter the statement's Tag/data snapshot or the test-health veto.
CREATE OR REPLACE VIEW pgos_snapshot_probe.pending_commits AS
SELECT DISTINCT generation,writer_xid FROM pgos_completion_probe.coverage WHERE NOT published;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgos_completion_probe FROM PUBLIC;
