-- Superuser-only, isolated replay protocol experiment; never installed by the extension.
CREATE SCHEMA pgos_replay_probe;
REVOKE ALL ON SCHEMA pgos_replay_probe FROM PUBLIC;
CREATE TABLE pgos_replay_probe.targets (
    generation uuid PRIMARY KEY REFERENCES pgos_capture_probe.generations,
    collection_name text NOT NULL UNIQUE CHECK (collection_name ~ '^pgos-live-[a-z0-9-]+$'),
    owner_run text NOT NULL CHECK (owner_run ~ '^[a-f0-9]{32}$'),
    current_batch uuid
);
CREATE TABLE pgos_replay_probe.batches (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    generation uuid NOT NULL REFERENCES pgos_replay_probe.targets,
    parent_batch uuid REFERENCES pgos_replay_probe.batches,
    claim_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','published')),
    active_attempt uuid,
    snapshot_id text,
    snapshot_committed_at bigint,
    CHECK ((state='pending' AND snapshot_id IS NULL AND snapshot_committed_at IS NULL)
        OR (state='published' AND active_attempt IS NOT NULL AND length(snapshot_id)>0 AND snapshot_committed_at IS NOT NULL))
);
ALTER TABLE pgos_replay_probe.targets ADD FOREIGN KEY(current_batch) REFERENCES pgos_replay_probe.batches;
CREATE UNIQUE INDEX one_pending_batch ON pgos_replay_probe.batches(generation) WHERE state='pending';
CREATE TABLE pgos_replay_probe.events (
    batch_id uuid NOT NULL REFERENCES pgos_replay_probe.batches,
    event_id bigint PRIMARY KEY,
    writer_xid xid8 NOT NULL,
    operation text NOT NULL,
    row_key bigint,
    document jsonb
);
CREATE TABLE pgos_replay_probe.attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id uuid NOT NULL REFERENCES pgos_replay_probe.batches,
    started_xid xid8 NOT NULL DEFAULT pg_current_xact_id()
);
ALTER TABLE pgos_replay_probe.batches ADD FOREIGN KEY(active_attempt) REFERENCES pgos_replay_probe.attempts;

-- Lock heap/index before registry/target rows, matching capture registration/DDL.
CREATE FUNCTION pgos_replay_probe.lock_generation(target uuid) RETURNS void LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
DECLARE g pgos_capture_probe.generations; s pgos_capture_probe.sources; meta jsonb;
BEGIN
    PERFORM pgos_capture_probe.assert_context();
    SELECT * INTO STRICT g FROM pgos_capture_probe.generations WHERE generation=target;
    SELECT * INTO STRICT s FROM pgos_capture_probe.sources WHERE epoch=g.source_epoch;
    IF s.state <> 'capturing' OR g.state <> 'capturing' THEN
        RAISE EXCEPTION 'generation is retired' USING ERRCODE='55000';
    END IF;
    EXECUTE format('LOCK TABLE %s IN ACCESS SHARE MODE',s.heap_oid::regclass);
    meta := pgos_lifecycle_probe.info(g.index_oid::regclass);
    PERFORM pgos_capture_probe.check_definition(s.heap_oid,s.key_attnum);
    IF (meta->>'generation')::uuid <> target OR (meta->>'heap_oid')::oid <> s.heap_oid
       OR (meta->>'key_attnum')::smallint <> s.key_attnum
       OR (meta->>'value_attnum')::smallint <> g.value_attnum
       OR meta->>'mode' <> g.mode OR (meta->>'dimensions')::int <> g.dimensions THEN
        RAISE EXCEPTION 'physical generation changed' USING ERRCODE='55000';
    END IF;
    SELECT * INTO STRICT g FROM pgos_capture_probe.generations WHERE generation=target FOR SHARE;
    IF g.state <> 'capturing' THEN
        RAISE EXCEPTION 'generation retired during replay' USING ERRCODE='55000';
    END IF;
END $$;

CREATE FUNCTION pgos_replay_probe.bind(target uuid, collection text, owner text) RETURNS void LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    PERFORM pgos_replay_probe.lock_generation(target);
    INSERT INTO pgos_replay_probe.targets VALUES(target,collection,owner,NULL)
      ON CONFLICT(generation) DO NOTHING;
    IF NOT EXISTS (SELECT FROM pgos_replay_probe.targets WHERE generation=target
                   AND collection_name=collection AND owner_run=owner) THEN
        RAISE EXCEPTION 'target cannot be rebound' USING ERRCODE='55000';
    END IF;
END $$;

CREATE FUNCTION pgos_replay_probe.claim(target uuid) RETURNS uuid LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
DECLARE head uuid; batch uuid; n bigint; bytes bigint; own boolean;
BEGIN
    PERFORM pgos_replay_probe.lock_generation(target);
    SELECT current_batch INTO STRICT head FROM pgos_replay_probe.targets WHERE generation=target FOR UPDATE;
    SELECT id INTO batch FROM pgos_replay_probe.batches WHERE generation=target AND state='pending';
    IF FOUND THEN RETURN batch; END IF;
    INSERT INTO pgos_replay_probe.batches(generation,parent_batch) VALUES(target,head) RETURNING id INTO batch;
    -- One statement snapshot, all visible events or fail closed: never truncate a writer transaction.
    INSERT INTO pgos_replay_probe.events
      SELECT batch,event_id,writer_xid,operation,row_key,document FROM pgos_capture_probe.outbox
      WHERE generation=target ORDER BY event_id LIMIT 1001;
    SELECT count(*),coalesce(sum(128+coalesce(octet_length(document::text),0)),0),coalesce(bool_or(writer_xid=pg_current_xact_id()),false)
      INTO n,bytes,own FROM pgos_replay_probe.events e WHERE batch_id=batch;
    IF n>1000 OR bytes>8388608 OR own THEN
        RAISE EXCEPTION 'claim requires committed events within 1000 events / 8 MiB' USING ERRCODE='54000';
    END IF;
    IF EXISTS (SELECT FROM pgos_replay_probe.events WHERE batch_id=batch AND operation='retire') THEN
        RAISE EXCEPTION 'retirement replay is not implemented' USING ERRCODE='55000';
    END IF;
    IF n=0 THEN DELETE FROM pgos_replay_probe.batches WHERE id=batch; RETURN NULL; END IF;
    RETURN batch;
END $$;

CREATE FUNCTION pgos_replay_probe.begin_attempt(batch uuid) RETURNS uuid LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
DECLARE b pgos_replay_probe.batches; attempt uuid;
BEGIN
    SELECT * INTO STRICT b FROM pgos_replay_probe.batches WHERE id=batch;
    PERFORM pgos_replay_probe.lock_generation(b.generation);
    PERFORM 1 FROM pgos_replay_probe.targets WHERE generation=b.generation FOR UPDATE;
    SELECT * INTO STRICT b FROM pgos_replay_probe.batches WHERE id=batch FOR UPDATE;
    IF b.state <> 'pending' OR b.claim_xid=pg_current_xact_id() THEN
        RAISE EXCEPTION 'attempt requires a committed pending batch' USING ERRCODE='55000';
    END IF;
    INSERT INTO pgos_replay_probe.attempts(batch_id) VALUES(batch) RETURNING id INTO attempt;
    UPDATE pgos_replay_probe.batches SET active_attempt=attempt WHERE id=batch;
    RETURN attempt;
END $$;

-- The trusted adapter supplies a receipt only after checking exact marker + immutable Tag.
-- SQL cannot independently attest an HTTP response. Attempts and coverage remain auditable.
CREATE FUNCTION pgos_replay_probe.publish(attempt uuid, snapshot text, committed_at bigint)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE a pgos_replay_probe.attempts; b pgos_replay_probe.batches; head uuid; removed bigint; covered bigint;
BEGIN
    SELECT * INTO STRICT a FROM pgos_replay_probe.attempts WHERE id=attempt;
    SELECT * INTO STRICT b FROM pgos_replay_probe.batches WHERE id=a.batch_id;
    PERFORM pgos_replay_probe.lock_generation(b.generation);
    SELECT current_batch INTO STRICT head FROM pgos_replay_probe.targets WHERE generation=b.generation FOR UPDATE;
    SELECT * INTO STRICT b FROM pgos_replay_probe.batches WHERE id=a.batch_id FOR UPDATE;
    IF b.active_attempt IS DISTINCT FROM attempt OR a.started_xid=pg_current_xact_id()
       OR snapshot IS NULL OR length(snapshot)=0 OR committed_at IS NULL THEN
        RAISE EXCEPTION 'stale or uncommitted attempt / invalid receipt' USING ERRCODE='55000';
    END IF;
    IF b.state='published' THEN
        IF b.snapshot_id=snapshot AND b.snapshot_committed_at=committed_at THEN RETURN; END IF;
        RAISE EXCEPTION 'published receipt cannot change' USING ERRCODE='55000';
    END IF;
    IF head IS DISTINCT FROM b.parent_batch THEN
        RAISE EXCEPTION 'publication base changed' USING ERRCODE='55000';
    END IF;
    SELECT count(*) INTO covered FROM pgos_replay_probe.events WHERE batch_id=b.id;
    DELETE FROM pgos_capture_probe.outbox o USING pgos_replay_probe.events e
      WHERE e.batch_id=b.id AND o.event_id=e.event_id AND o.generation=b.generation
        AND o.writer_xid=e.writer_xid AND o.operation=e.operation
        AND o.row_key IS NOT DISTINCT FROM e.row_key AND o.document IS NOT DISTINCT FROM e.document;
    GET DIAGNOSTICS removed=ROW_COUNT;
    IF removed<>covered OR covered=0 THEN
        RAISE EXCEPTION 'outbox coverage changed' USING ERRCODE='55000';
    END IF;
    UPDATE pgos_replay_probe.batches SET state='published',snapshot_id=snapshot,snapshot_committed_at=committed_at WHERE id=b.id;
    UPDATE pgos_replay_probe.targets SET current_batch=b.id WHERE generation=b.generation;
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgos_replay_probe FROM PUBLIC;
