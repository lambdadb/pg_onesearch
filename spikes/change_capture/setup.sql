-- Isolated logged registry/outbox experiment; not installed by the extension.
CREATE SCHEMA pgos_capture_probe;
REVOKE ALL ON SCHEMA pgos_capture_probe FROM PUBLIC;
CREATE TABLE pgos_capture_probe.installation (
    singleton boolean PRIMARY KEY CHECK (singleton),
    database_oid oid NOT NULL,
    system_identifier bigint NOT NULL
);
INSERT INTO pgos_capture_probe.installation
SELECT true, d.oid, c.system_identifier FROM pg_database d, pg_control_system() c
WHERE d.datname=current_database();
CREATE TABLE pgos_capture_probe.sources (
    epoch uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    heap_oid oid NOT NULL,
    key_attnum smallint NOT NULL,
    state text NOT NULL CHECK (state IN ('capturing','retired'))
);
CREATE UNIQUE INDEX sources_current_heap ON pgos_capture_probe.sources(heap_oid) WHERE state='capturing';
CREATE TABLE pgos_capture_probe.generations (
    generation uuid PRIMARY KEY,
    source_epoch uuid NOT NULL REFERENCES pgos_capture_probe.sources(epoch),
    index_oid oid NOT NULL,
    value_attnum smallint NOT NULL,
    mode text NOT NULL CHECK (mode IN ('vector','text')),
    dimensions integer NOT NULL,
    resource_key text NOT NULL UNIQUE,
    state text NOT NULL CHECK (state IN ('capturing','retire_pending')),
    UNIQUE(generation,source_epoch)
);
CREATE UNIQUE INDEX generations_current_index ON pgos_capture_probe.generations(index_oid) WHERE state='capturing';
CREATE TABLE pgos_capture_probe.outbox (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_epoch uuid NOT NULL,
    generation uuid NOT NULL,
    writer_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    operation text NOT NULL CHECK (operation IN ('create','upsert','delete','retire')),
    row_key bigint,
    document jsonb,
    FOREIGN KEY(generation,source_epoch) REFERENCES pgos_capture_probe.generations(generation,source_epoch),
    CHECK ((operation='upsert' AND row_key IS NOT NULL AND document IS NOT NULL)
        OR (operation='delete' AND row_key IS NOT NULL AND document IS NULL)
        OR (operation IN ('create','retire') AND row_key IS NULL AND document IS NULL))
);

CREATE FUNCTION pgos_capture_probe.assert_context() RETURNS void LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname=current_user) THEN
        RAISE EXCEPTION 'capture probe requires superuser' USING ERRCODE='42501';
    END IF;
    IF current_setting('transaction_isolation') <> 'read committed'
       OR current_setting('session_replication_role') <> 'origin' THEN
        RAISE EXCEPTION 'capture probe requires READ COMMITTED and origin role' USING ERRCODE='0A000';
    END IF;
    IF NOT EXISTS (SELECT FROM pgos_capture_probe.installation i, pg_control_system() c, pg_database d
                   WHERE d.datname=current_database() AND d.oid=i.database_oid
                     AND c.system_identifier=i.system_identifier) THEN
        RAISE EXCEPTION 'capture probe installation requires restore fencing' USING ERRCODE='55000';
    END IF;
END $$;

CREATE FUNCTION pgos_capture_probe.project(mode text, key bigint, value jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $$
    SELECT CASE WHEN value IS NULL OR value='null'::jsonb THEN NULL
        WHEN mode='vector' THEN jsonb_build_object('id',key::text,'embedding',(value #>> '{}')::jsonb)
        ELSE jsonb_build_object('id',key::text,'content',value) END
$$;

CREATE FUNCTION pgos_capture_probe.retire(target uuid) RETURNS void LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    PERFORM pgos_capture_probe.assert_context();
    WITH retired AS (
        UPDATE pgos_capture_probe.generations SET state='retire_pending'
        WHERE generation=target AND state='capturing' RETURNING *
    ) INSERT INTO pgos_capture_probe.outbox(source_epoch,generation,operation)
      SELECT source_epoch,generation,'retire' FROM retired;
END $$;

CREATE FUNCTION pgos_capture_probe.check_definition(target_heap oid, target_key smallint) RETURNS void LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_class c JOIN pg_index p ON p.indrelid=c.oid
                   JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=target_key
                   WHERE c.oid=target_heap AND c.relkind='r' AND c.relpersistence='p'
                     AND NOT c.relrowsecurity AND NOT c.relispartition
                     AND a.atttypid='bigint'::regtype AND a.attnotnull AND NOT a.attisdropped
                     AND p.indisprimary AND p.indisvalid AND p.indimmediate
                     AND p.indnkeyatts=1 AND p.indkey[0]=target_key)
       OR EXISTS (SELECT FROM pg_inherits WHERE inhrelid=target_heap OR inhparent=target_heap) THEN
        RAISE EXCEPTION 'capture source definition changed' USING ERRCODE='55000';
    END IF;
END $$;

CREATE FUNCTION pgos_capture_probe.guard_source() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
DECLARE src pgos_capture_probe.sources; g pgos_capture_probe.generations; meta jsonb;
BEGIN
    PERFORM pgos_capture_probe.assert_context();
    SELECT * INTO src FROM pgos_capture_probe.sources
      WHERE epoch=TG_ARGV[0]::uuid AND heap_oid=TG_RELID AND state='capturing';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'capture source binding is unavailable' USING ERRCODE='55000';
    END IF;
    IF TG_OP='TRUNCATE' THEN
        RAISE EXCEPTION 'registered source TRUNCATE requires a replay reset protocol' USING ERRCODE='0A000';
    END IF;
    PERFORM pgos_capture_probe.check_definition(TG_RELID,src.key_attnum);
    FOR g IN SELECT * FROM pgos_capture_probe.generations
              WHERE source_epoch=src.epoch AND state='capturing' ORDER BY generation LOOP
        meta := pgos_lifecycle_probe.info(g.index_oid::regclass);
        IF (meta->>'generation')::uuid <> g.generation OR (meta->>'heap_oid')::oid <> TG_RELID
           OR (meta->>'key_attnum')::smallint <> src.key_attnum
           OR (meta->>'value_attnum')::smallint <> g.value_attnum
           OR meta->>'mode' <> g.mode OR (meta->>'dimensions')::int <> g.dimensions THEN
            RAISE EXCEPTION 'capture index generation requires registration' USING ERRCODE='55000';
        END IF;
    END LOOP;
    RETURN NULL;
END $$;

CREATE FUNCTION pgos_capture_probe.capture_row() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
DECLARE src pgos_capture_probe.sources; g record; key_name name; old_key bigint; new_key bigint;
        old_row jsonb; new_row jsonb; payload jsonb;
BEGIN
    PERFORM pgos_capture_probe.assert_context();
    SELECT * INTO STRICT src FROM pgos_capture_probe.sources
      WHERE epoch=TG_ARGV[0]::uuid AND heap_oid=TG_RELID AND state='capturing';
    SELECT attname INTO STRICT key_name FROM pg_attribute
      WHERE attrelid=TG_RELID AND attnum=src.key_attnum AND NOT attisdropped;
    IF TG_OP <> 'INSERT' THEN old_row := to_jsonb(OLD); old_key := (old_row->>key_name)::bigint; END IF;
    IF TG_OP <> 'DELETE' THEN new_row := to_jsonb(NEW); new_key := (new_row->>key_name)::bigint; END IF;
    FOR g IN SELECT x.*,a.attname FROM pgos_capture_probe.generations x
             JOIN pg_attribute a ON a.attrelid=TG_RELID AND a.attnum=x.value_attnum AND NOT a.attisdropped
             WHERE source_epoch=src.epoch AND x.state='capturing' ORDER BY generation LOOP
        IF TG_OP='DELETE' OR (TG_OP='UPDATE' AND old_key IS DISTINCT FROM new_key) THEN
            INSERT INTO pgos_capture_probe.outbox(source_epoch,generation,operation,row_key)
                VALUES(src.epoch,g.generation,'delete',old_key);
        END IF;
        IF TG_OP <> 'DELETE' THEN
            payload := pgos_capture_probe.project(g.mode,new_key,new_row->g.attname);
            INSERT INTO pgos_capture_probe.outbox(source_epoch,generation,operation,row_key,document)
                VALUES(src.epoch,g.generation,CASE WHEN payload IS NULL THEN 'delete' ELSE 'upsert' END,new_key,payload);
        END IF;
    END LOOP;
    RETURN NULL;
END $$;

CREATE FUNCTION pgos_capture_probe.register_index(target regclass) RETURNS uuid LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
DECLARE heap oid; meta jsonb; gen uuid; src pgos_capture_probe.sources; prior record;
        key_name name; value_name name; created_source boolean := false;
BEGIN
    PERFORM pgos_capture_probe.assert_context();
    SELECT indrelid INTO STRICT heap FROM pg_index WHERE indexrelid=target;
    -- Acquire heap before index locks; wait for writers before taking the seed snapshot.
    EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE',heap::regclass);
    meta := pgos_lifecycle_probe.info(target);
    IF (meta->>'heap_oid')::oid <> heap OR EXISTS (SELECT FROM pg_inherits WHERE inhrelid=heap OR inhparent=heap) THEN
        RAISE EXCEPTION 'unsupported capture source' USING ERRCODE='0A000';
    END IF;
    PERFORM pgos_capture_probe.check_definition(heap,(meta->>'key_attnum')::smallint);
    gen := (meta->>'generation')::uuid;
    SELECT * INTO src FROM pgos_capture_probe.sources WHERE heap_oid=heap AND state='capturing';
    IF NOT FOUND THEN
        INSERT INTO pgos_capture_probe.sources(heap_oid,key_attnum,state)
        VALUES(heap,(meta->>'key_attnum')::smallint,'capturing') RETURNING * INTO src;
        created_source := true;
    ELSIF src.key_attnum <> (meta->>'key_attnum')::smallint THEN
        RAISE EXCEPTION 'source primary key definition changed' USING ERRCODE='55000';
    END IF;
    IF EXISTS (SELECT FROM pgos_capture_probe.generations WHERE generation=gen) THEN
        IF NOT EXISTS (SELECT FROM pgos_capture_probe.generations WHERE generation=gen
                       AND source_epoch=src.epoch AND index_oid=target AND state='capturing') THEN
            RAISE EXCEPTION 'generation cannot be rebound or reactivated' USING ERRCODE='55000';
        END IF;
        RETURN gen;
    END IF;
    FOR prior IN SELECT generation FROM pgos_capture_probe.generations
                 WHERE index_oid=target AND state='capturing' LOOP
        PERFORM pgos_capture_probe.retire(prior.generation);
    END LOOP;
    INSERT INTO pgos_capture_probe.generations(generation,source_epoch,index_oid,value_attnum,mode,dimensions,resource_key,state)
      VALUES(gen,src.epoch,target,(meta->>'value_attnum')::smallint,meta->>'mode',(meta->>'dimensions')::int,
             'pgos-'||replace(src.epoch::text,'-','')||'-'||replace(gen::text,'-',''),'capturing');
    INSERT INTO pgos_capture_probe.outbox(source_epoch,generation,operation) VALUES(src.epoch,gen,'create');
    IF created_source THEN
        EXECUTE format('CREATE TRIGGER pgos_capture_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION pgos_capture_probe.guard_source(%L)',heap::regclass,src.epoch);
        EXECUTE format('CREATE TRIGGER pgos_capture_rows AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION pgos_capture_probe.capture_row(%L)',heap::regclass,src.epoch);
        EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER pgos_capture_guard',heap::regclass);
        EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER pgos_capture_rows',heap::regclass);
    END IF;
    SELECT attname INTO STRICT key_name FROM pg_attribute WHERE attrelid=heap AND attnum=src.key_attnum AND NOT attisdropped;
    SELECT attname INTO STRICT value_name FROM pg_attribute WHERE attrelid=heap AND attnum=(meta->>'value_attnum')::smallint AND NOT attisdropped;
    EXECUTE format('INSERT INTO pgos_capture_probe.outbox(source_epoch,generation,operation,row_key,document)
      SELECT $1,$2,''upsert'',%I,pgos_capture_probe.project($3,%I,to_jsonb(%I)) FROM %s WHERE %I IS NOT NULL',
      key_name,key_name,value_name,heap::regclass,value_name) USING src.epoch,gen,meta->>'mode';
    RETURN gen;
END $$;

CREATE FUNCTION pgos_capture_probe.dropped_objects() RETURNS event_trigger LANGUAGE plpgsql
SET search_path=pg_catalog,pg_temp AS $$
DECLARE obj record; g record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
               WHERE classid='pg_class'::regclass AND objsubid=0 LOOP
        FOR g IN SELECT x.generation FROM pgos_capture_probe.generations x
                 JOIN pgos_capture_probe.sources s ON s.epoch=x.source_epoch
                 WHERE x.state='capturing' AND (x.index_oid=obj.objid OR s.heap_oid=obj.objid) LOOP
            PERFORM pgos_capture_probe.retire(g.generation);
        END LOOP;
        UPDATE pgos_capture_probe.sources SET state='retired' WHERE heap_oid=obj.objid AND state='capturing';
    END LOOP;
END $$;
CREATE EVENT TRIGGER pgos_capture_drop ON sql_drop EXECUTE FUNCTION pgos_capture_probe.dropped_objects();
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgos_capture_probe FROM PUBLIC;
