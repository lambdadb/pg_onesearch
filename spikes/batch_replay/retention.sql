-- Bounded, trusted-adapter cleanup for published attempt refs only.
-- Keep SQL history: snapshot reconstruction still traverses the entire batch chain.
CREATE SCHEMA pgos_retention_probe;
REVOKE ALL ON SCHEMA pgos_retention_probe FROM PUBLIC;
CREATE FUNCTION pgos_retention_probe.horizon() RETURNS xid8
AS '$libdir/pg_onesearch_snapshot_probe','pgos_retention_horizon'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

CREATE TABLE pgos_retention_probe.jobs (
    batch_id uuid PRIMARY KEY REFERENCES pgos_replay_probe.batches,
    planned_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','done'))
);

CREATE FUNCTION pgos_retention_probe.plan(target uuid) RETURNS uuid
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE g pgos_capture_probe.generations; head uuid; candidate uuid; boundary xid8;
BEGIN
    PERFORM pgos_capture_probe.assert_context();
    -- Match replay's registry -> target lock order; retirement updates the registry.
    SELECT * INTO STRICT g FROM pgos_capture_probe.generations WHERE generation=target FOR SHARE;
    SELECT current_batch INTO STRICT head FROM pgos_replay_probe.targets WHERE generation=target FOR UPDATE;
    SELECT j.batch_id INTO candidate FROM pgos_retention_probe.jobs j
      JOIN pgos_replay_probe.batches b ON b.id=j.batch_id
      WHERE b.generation=target AND j.state='pending' ORDER BY j.batch_id LIMIT 1;
    IF FOUND THEN RETURN candidate; END IF;
    boundary := pgos_retention_probe.horizon();
    SELECT b.id INTO candidate FROM pgos_replay_probe.batches b
      WHERE b.generation=target AND b.state='published'
        AND NOT EXISTS (SELECT FROM pgos_retention_probe.jobs j WHERE j.batch_id=b.id)
        AND (
          -- Supersession committed before every potentially referencing snapshot.
          (b.id IS DISTINCT FROM head AND EXISTS (
            SELECT FROM pgos_replay_probe.batches successor WHERE successor.parent_batch=b.id
              AND successor.generation=target AND successor.state='published'
              AND successor.published_xid<boundary))
          OR
          (g.state='retire_pending' AND EXISTS (
            SELECT FROM pgos_capture_probe.outbox o WHERE o.generation=target
              AND o.operation='retire' AND o.writer_xid<boundary))
        )
        -- A pending child can still branch from this base. An abandoned attempt
        -- may have an in-flight HTTP request even after its PG connection died.
        -- Never infer quiescence from a nonce replacement, timeout or local lease.
        AND NOT EXISTS (
          SELECT FROM pgos_replay_probe.batches child WHERE child.parent_batch=b.id
            AND (child.state='pending' OR EXISTS (
              SELECT FROM pgos_replay_probe.attempts a WHERE a.batch_id=child.id
                AND a.id IS DISTINCT FROM child.active_attempt))
        )
      ORDER BY b.published_xid,b.id LIMIT 1;
    IF candidate IS NULL THEN RETURN NULL; END IF;
    INSERT INTO pgos_retention_probe.jobs(batch_id) VALUES(candidate);
    RETURN candidate;
END $$;

-- As with replay.publish, a private trusted adapter attests remote verification.
CREATE FUNCTION pgos_retention_probe.finish(batch uuid) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE j pgos_retention_probe.jobs;
BEGIN
    PERFORM pgos_capture_probe.assert_context();
    SELECT * INTO STRICT j FROM pgos_retention_probe.jobs WHERE batch_id=batch FOR UPDATE;
    IF j.planned_xid=pg_current_xact_id() THEN
        RAISE EXCEPTION 'cleanup plan must commit before remote deletion' USING ERRCODE='55000';
    END IF;
    UPDATE pgos_retention_probe.jobs SET state='done' WHERE batch_id=batch;
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgos_retention_probe FROM PUBLIC;
