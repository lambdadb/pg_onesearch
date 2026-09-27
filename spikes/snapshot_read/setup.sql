CREATE SCHEMA pgos_snapshot_probe;
REVOKE ALL ON SCHEMA pgos_snapshot_probe FROM PUBLIC;
-- An additional fault-injection veto. True cannot override missing publication
-- or a pending replay batch; the C reader checks those durable protocol states.
CREATE TABLE pgos_snapshot_probe.health (
    generation uuid PRIMARY KEY REFERENCES pgos_capture_probe.generations,
    healthy boolean NOT NULL
);
CREATE FUNCTION pgos_snapshot_probe.set_test_health(target regclass, healthy boolean)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE gen uuid;
BEGIN
    PERFORM pgos_capture_probe.assert_context();
    SELECT generation INTO STRICT gen FROM pgos_capture_probe.generations WHERE index_oid=target AND state='capturing';
    INSERT INTO pgos_snapshot_probe.health VALUES(gen,healthy)
      ON CONFLICT(generation) DO UPDATE SET healthy=excluded.healthy;
END $$;

-- STABLE is essential: all data queries and dynamic heap reads use the caller's snapshot.
CREATE FUNCTION pgos_snapshot_probe.assemble(target regclass) RETURNS jsonb
LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $$
DECLARE g pgos_capture_probe.generations; src pgos_capture_probe.sources;
        t pgos_replay_probe.targets; b pgos_replay_probe.batches;
        meta jsonb; base jsonb; delta jsonb; rows jsonb; effective jsonb; actual jsonb;
        chain uuid[]; key_name name; value_name name; result jsonb; event_count bigint;
BEGIN
    PERFORM pgos_capture_probe.assert_context();
    SELECT * INTO STRICT g FROM pgos_capture_probe.generations WHERE index_oid=target AND state='capturing';
    SELECT * INTO STRICT src FROM pgos_capture_probe.sources WHERE epoch=g.source_epoch AND state='capturing';
    meta := pgos_lifecycle_probe.info(target);
    IF (meta->>'generation')::uuid <> g.generation OR (meta->>'heap_oid')::oid <> src.heap_oid
       OR (meta->>'key_attnum')::smallint <> src.key_attnum
       OR (meta->>'value_attnum')::smallint <> g.value_attnum
       OR meta->>'mode' <> g.mode OR (meta->>'dimensions')::int <> g.dimensions THEN
        RAISE EXCEPTION 'snapshot index generation changed' USING ERRCODE='55000';
    END IF;
    PERFORM pgos_capture_probe.check_definition(src.heap_oid,src.key_attnum);
    SELECT * INTO STRICT t FROM pgos_replay_probe.targets WHERE generation=g.generation;
    SELECT * INTO b FROM pgos_replay_probe.batches WHERE id=t.current_batch AND state='published';
    IF NOT FOUND THEN RAISE EXCEPTION 'no published base for snapshot' USING ERRCODE='55000'; END IF;
    -- Newest batch first, NEVER global event_id order across publications.
    WITH RECURSIVE history AS (
        SELECT b.id AS id,b.parent_batch AS parent,0 AS depth
        UNION ALL SELECT p.id,p.parent_batch,h.depth+1 FROM history h JOIN pgos_replay_probe.batches p ON p.id=h.parent
          WHERE h.depth<64 AND p.generation=g.generation AND p.state='published'
    ) SELECT array_agg(id ORDER BY depth) INTO chain FROM history;
    IF cardinality(chain)>64 OR EXISTS (SELECT FROM pgos_replay_probe.batches WHERE id=chain[cardinality(chain)] AND parent_batch IS NOT NULL) THEN
        RAISE EXCEPTION 'snapshot history exceeds bounded probe or has a gap' USING ERRCODE='54000';
    END IF;
    SELECT count(*) INTO event_count FROM pgos_replay_probe.events WHERE batch_id=ANY(chain);
    IF event_count>4096 THEN RAISE EXCEPTION 'snapshot history event budget exceeded' USING ERRCODE='54000'; END IF;
    SELECT coalesce(jsonb_object_agg(row_key::text,jsonb_build_object('event',event_id::text,'doc',document)),'{}') INTO base
      FROM (SELECT DISTINCT ON(row_key) row_key,event_id,operation,document
            FROM pgos_replay_probe.events WHERE batch_id=ANY(chain) AND row_key IS NOT NULL
            ORDER BY row_key,array_position(chain,batch_id),event_id DESC) latest WHERE operation='upsert';
    SELECT count(*) INTO event_count FROM pgos_capture_probe.outbox WHERE generation=g.generation;
    IF event_count>1000 THEN RAISE EXCEPTION 'snapshot delta event budget exceeded' USING ERRCODE='54000'; END IF;
    SELECT coalesce(jsonb_object_agg(row_key::text,jsonb_build_object('event',event_id::text,'operation',operation,
            'doc',document,'own',coalesce(writer_xid=pg_current_xact_id_if_assigned(),false))),'{}') INTO delta
      FROM (SELECT DISTINCT ON(row_key) * FROM pgos_capture_probe.outbox
            WHERE generation=g.generation AND row_key IS NOT NULL ORDER BY row_key,event_id DESC) latest;
    SELECT attname INTO STRICT key_name FROM pg_attribute WHERE attrelid=src.heap_oid AND attnum=src.key_attnum AND NOT attisdropped;
    SELECT attname INTO STRICT value_name FROM pg_attribute WHERE attrelid=src.heap_oid AND attnum=g.value_attnum AND NOT attisdropped;
    EXECUTE format('SELECT coalesce(jsonb_agg(x ORDER BY (x->>''id'')::bigint),''[]'') FROM
        (SELECT jsonb_build_object(''id'',%I::text,''row'',to_jsonb(d),''tid'',ctid::text,
         ''doc'',pgos_capture_probe.project($1,%I,to_jsonb(%I))) AS x FROM %s d LIMIT 65) q',
         key_name,key_name,value_name,src.heap_oid::regclass) INTO rows USING g.mode;
    IF jsonb_array_length(rows)>64 THEN RAISE EXCEPTION 'snapshot heap exceeds 64-row probe' USING ERRCODE='54000'; END IF;
    SELECT coalesce(jsonb_object_agg(key,value),'{}') INTO effective FROM jsonb_each(base||delta) WHERE value->'doc'<>'null'::jsonb;
    SELECT coalesce(jsonb_object_agg(key,value->'doc'),'{}') INTO actual FROM jsonb_each(effective);
    IF actual IS DISTINCT FROM (SELECT coalesce(jsonb_object_agg(x->>'id',x->'doc'),'{}') FROM jsonb_array_elements(rows) x WHERE x->'doc'<>'null'::jsonb) THEN
        RAISE EXCEPTION 'base plus delta differs from snapshot heap' USING ERRCODE='55000';
    END IF;
    IF (SELECT count(*) FROM jsonb_each(base))>64 THEN
        RAISE EXCEPTION 'snapshot base exceeds 64-document probe' USING ERRCODE='54000';
    END IF;
    result := jsonb_build_object('generation',g.generation,'source_epoch',src.epoch,'collection',t.collection_name,
         'batch',b.id,'tag','attempt-'||replace(b.active_attempt::text,'-',''),'mode',g.mode,'dimensions',g.dimensions,
         'base',base,'delta',delta,'effective',effective,'rows',rows);
    IF octet_length(result::text)>4194304 THEN RAISE EXCEPTION 'snapshot view exceeds 4 MiB' USING ERRCODE='54000'; END IF;
    RETURN result;
END $$;

CREATE FUNCTION pgos_snapshot_probe.search_view(view jsonb, query jsonb) RETURNS jsonb
LANGUAGE plpgsql STABLE SET search_path=pg_catalog,pg_temp AS $$
DECLARE response jsonb; hits jsonb; hit jsonb; hit_key text; query_vector onesearch.vector;
        docs jsonb; results jsonb; q text;
BEGIN
    IF view->>'mode'='vector' THEN
        IF jsonb_typeof(query)<>'array' OR jsonb_array_length(query)<>(view->>'dimensions')::int THEN
            RAISE EXCEPTION 'vector query dimensions differ' USING ERRCODE='22023';
        END IF;
        query_vector := query::text::onesearch.vector;
        PERFORM onesearch.cosine_distance(query_vector,query_vector);
        response := pgos_remote_probe.query(view->>'collection',view->>'tag',jsonb_build_object(
            'knn',jsonb_build_object('field','embedding','queryVector',query,'k',100)),100);
    ELSE
        SELECT coalesce(jsonb_object_agg(key,value->'doc'),'{}') INTO docs FROM jsonb_each(view->'base');
        IF docs IS DISTINCT FROM (SELECT coalesce(jsonb_object_agg(key,value->'doc'),'{}') FROM jsonb_each(view->'effective')) THEN
            RAISE EXCEPTION 'BM25 changed corpus requires a coherent scoring overlay' USING ERRCODE='0A000';
        END IF;
        IF jsonb_typeof(query)<>'string' OR length(btrim(query #>> '{}'))=0 THEN
            RAISE EXCEPTION 'BM25 query requires nonempty text' USING ERRCODE='22023';
        END IF;
        q := query #>> '{}';
        response := pgos_remote_probe.query(view->>'collection',view->>'tag',jsonb_build_object(
            'queryString',jsonb_build_object('query',q,'defaultField','content','skipSyntax',true)),100);
    END IF;
    hits := response->'docs';
    FOR hit IN SELECT value FROM jsonb_array_elements(hits) LOOP
        hit_key := hit->'doc'->>'id';
        IF NOT (view->'base' ? hit_key) OR (hit->'doc') IS DISTINCT FROM (view->'base'->hit_key->'doc') THEN
            RAISE EXCEPTION 'remote document does not match selected base revision' USING ERRCODE='55000';
        END IF;
    END LOOP;
    IF view->>'mode'='vector' THEN
        IF jsonb_array_length(hits)<>(SELECT count(*) FROM jsonb_each(view->'base')) THEN
            RAISE EXCEPTION 'remote vector base coverage is incomplete' USING ERRCODE='55000';
        END IF;
        SELECT coalesce(jsonb_agg(value ORDER BY distance,(value->>'id')::bigint),'[]') INTO results FROM (
          SELECT x || jsonb_build_object('version',(view->>'generation')||':'||(view->'effective'->(x->>'id')->>'event'),
                 'distance',onesearch.cosine_distance((x->'doc'->'embedding')::text::onesearch.vector,query_vector)) AS value,
                 onesearch.cosine_distance((x->'doc'->'embedding')::text::onesearch.vector,query_vector) AS distance
          FROM jsonb_array_elements(view->'rows') x WHERE x->'doc'<>'null'::jsonb) ranked;
    ELSE
        SELECT coalesce(jsonb_agg(x || jsonb_build_object('version',(view->>'generation')||':'||
            (view->'effective'->(x->>'id')->>'event'),'score',h->'score') ORDER BY (h->>'score')::float8 DESC,(x->>'id')::bigint),'[]')
          INTO results FROM jsonb_array_elements(hits) h JOIN jsonb_array_elements(view->'rows') x ON x->>'id'=h->'doc'->>'id';
    END IF;
    RETURN jsonb_build_object('generation',view->'generation','batch',view->'batch','tag',view->'tag','results',results);
END $$;

CREATE FUNCTION pgos_snapshot_probe.view(regclass) RETURNS jsonb
AS '$libdir/pg_onesearch_snapshot_probe','pgos_snapshot_view' LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
CREATE FUNCTION pgos_snapshot_probe.search(regclass,jsonb) RETURNS jsonb
AS '$libdir/pg_onesearch_snapshot_probe','pgos_snapshot_search' LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
CREATE FUNCTION pgos_snapshot_probe.check_health(regclass) RETURNS void
AS '$libdir/pg_onesearch_snapshot_probe','pgos_snapshot_check_health' LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgos_snapshot_probe FROM PUBLIC;
