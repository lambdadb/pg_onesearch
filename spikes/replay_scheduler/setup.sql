-- Test-only serial replay scheduler; install after the publication completion probe.
CREATE SCHEMA pgos_scheduler_probe;
REVOKE ALL ON SCHEMA pgos_scheduler_probe FROM PUBLIC;
CREATE TABLE pgos_scheduler_probe.config (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    base_delay_ms integer NOT NULL DEFAULT 1000 CHECK (base_delay_ms BETWEEN 10 AND 60000),
    max_delay_ms integer NOT NULL DEFAULT 60000 CHECK (max_delay_ms BETWEEN base_delay_ms AND 3600000)
);
INSERT INTO pgos_scheduler_probe.config DEFAULT VALUES;
CREATE TABLE pgos_scheduler_probe.jobs (
    generation uuid PRIMARY KEY REFERENCES pgos_replay_probe.targets,
    attempts bigint NOT NULL DEFAULT 0 CHECK (attempts>=0),
    consecutive_unconfirmed integer NOT NULL DEFAULT 0 CHECK (consecutive_unconfirmed>=0),
    next_attempt_at timestamptz NOT NULL DEFAULT '-infinity',
    last_attempt_at timestamptz,
    last_success_at timestamptz,
    last_error_kind text CHECK (last_error_kind IN ('remote','database')),
    last_sqlstate text CHECK (last_sqlstate ~ '^[A-Z0-9]{5}$')
);

-- Two-int session advisory lock, scoped by PostgreSQL to the current database.
-- One dedicated connection owns the serial loop; never use a transaction pool.
CREATE FUNCTION pgos_scheduler_probe.acquire() RETURNS boolean LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    PERFORM pgos_capture_probe.assert_context();
    -- Advisory locks are reentrant; reject a second scheduler on the same session.
    IF EXISTS (SELECT FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid()
               AND classid=1885826931 AND objid=1920363641 AND objsubid=2 AND granted) THEN
        RAISE EXCEPTION 'scheduler already acquired on this session' USING ERRCODE='55000';
    END IF;
    RETURN pg_try_advisory_lock(1885826931,1920363641);
END $$;
CREATE FUNCTION pgos_scheduler_probe.assert_owner() RETURNS void LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    PERFORM pgos_capture_probe.assert_context();
    IF NOT EXISTS (SELECT FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid()
                   AND classid=1885826931 AND objid=1920363641 AND objsubid=2
                   AND mode='ExclusiveLock' AND granted) THEN
        RAISE EXCEPTION 'scheduler session does not own the database lock' USING ERRCODE='55000';
    END IF;
END $$;
CREATE VIEW pgos_scheduler_probe.work AS
SELECT t.generation FROM pgos_replay_probe.targets t
JOIN pgos_capture_probe.generations g USING(generation)
JOIN pgos_capture_probe.sources s ON s.epoch=g.source_epoch
WHERE g.state='capturing' AND s.state='capturing'
  AND (EXISTS (SELECT FROM pgos_capture_probe.outbox o WHERE o.generation=t.generation)
       OR EXISTS (SELECT FROM pgos_replay_probe.batches b WHERE b.generation=t.generation AND b.state='pending'));

CREATE FUNCTION pgos_scheduler_probe.next_generation() RETURNS uuid LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
DECLARE target uuid; cfg pgos_scheduler_probe.config;
BEGIN
    PERFORM pgos_scheduler_probe.assert_owner();
    SELECT * INTO STRICT cfg FROM pgos_scheduler_probe.config;
    INSERT INTO pgos_scheduler_probe.jobs(generation)
      SELECT generation FROM pgos_scheduler_probe.work ON CONFLICT DO NOTHING;
    -- Fair among due generations: a hot source cannot always outrank a sibling.
    SELECT j.generation INTO target FROM pgos_scheduler_probe.jobs j
      JOIN pgos_scheduler_probe.work w USING(generation)
      WHERE j.next_attempt_at<=clock_timestamp()
      ORDER BY j.last_attempt_at NULLS FIRST,j.generation LIMIT 1 FOR UPDATE OF j;
    IF target IS NULL THEN RETURN NULL; END IF;
    -- Persist a retry deadline BEFORE claim/HTTP. Death leaves an unconfirmed
    -- attempt, not an immediate restart loop. Success resets the backoff below.
    UPDATE pgos_scheduler_probe.jobs SET attempts=attempts+1,
      next_attempt_at=clock_timestamp()+make_interval(secs=>
        least(cfg.max_delay_ms::double precision,
              cfg.base_delay_ms*power(2::double precision,least(consecutive_unconfirmed,20)))/1000),
      consecutive_unconfirmed=least(consecutive_unconfirmed,2147483646)+1,
      last_attempt_at=clock_timestamp(),last_error_kind=NULL,last_sqlstate=NULL
      WHERE generation=target;
    RETURN target;
END $$;
CREATE FUNCTION pgos_scheduler_probe.finish(target uuid, error_kind text DEFAULT NULL, sqlstate text DEFAULT NULL)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE cfg pgos_scheduler_probe.config;
BEGIN
    PERFORM pgos_scheduler_probe.assert_owner();
    SELECT * INTO STRICT cfg FROM pgos_scheduler_probe.config;
    IF error_kind IS NULL THEN
        UPDATE pgos_scheduler_probe.jobs SET consecutive_unconfirmed=0,next_attempt_at=clock_timestamp(),
          last_success_at=clock_timestamp(),last_error_kind=NULL,last_sqlstate=NULL WHERE generation=target;
    ELSE
        -- A slow failed request must still wait after failure; the deadline set
        -- before HTTP alone could already have expired. Persist only safe codes.
        UPDATE pgos_scheduler_probe.jobs SET last_error_kind=error_kind,last_sqlstate=sqlstate,
          next_attempt_at=clock_timestamp()+make_interval(secs=>
            least(cfg.max_delay_ms::double precision,
                  cfg.base_delay_ms*power(2::double precision,least(consecutive_unconfirmed-1,20)))/1000)
          WHERE generation=target;
    END IF;
    IF NOT FOUND THEN RAISE EXCEPTION 'scheduler job missing' USING ERRCODE='55000'; END IF;
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgos_scheduler_probe FROM PUBLIC;
