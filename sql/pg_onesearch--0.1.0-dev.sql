\echo Use "CREATE EXTENSION pg_onesearch" to load this file. \quit

CREATE TYPE onesearch.vector;

CREATE FUNCTION onesearch.vector_in(cstring, oid, integer)
RETURNS onesearch.vector AS 'MODULE_PATHNAME', 'onesearch_vector_in'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION onesearch.vector_out(onesearch.vector)
RETURNS cstring AS 'MODULE_PATHNAME', 'onesearch_vector_out'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION onesearch.vector_recv(internal, oid, integer)
RETURNS onesearch.vector AS 'MODULE_PATHNAME', 'onesearch_vector_recv'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION onesearch.vector_send(onesearch.vector)
RETURNS bytea AS 'MODULE_PATHNAME', 'onesearch_vector_send'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION onesearch.vector_typmod_in(cstring[])
RETURNS integer AS 'MODULE_PATHNAME', 'onesearch_vector_typmod_in'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE FUNCTION onesearch.vector_typmod_out(integer)
RETURNS cstring AS 'MODULE_PATHNAME', 'onesearch_vector_typmod_out'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE onesearch.vector (
    INPUT = onesearch.vector_in,
    OUTPUT = onesearch.vector_out,
    RECEIVE = onesearch.vector_recv,
    SEND = onesearch.vector_send,
    TYPMOD_IN = onesearch.vector_typmod_in,
    TYPMOD_OUT = onesearch.vector_typmod_out,
    INTERNALLENGTH = variable,
    ALIGNMENT = int4,
    STORAGE = external
);

CREATE FUNCTION onesearch.vector(onesearch.vector, integer, boolean)
RETURNS onesearch.vector AS 'MODULE_PATHNAME', 'onesearch_vector_coerce'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE CAST (onesearch.vector AS onesearch.vector)
WITH FUNCTION onesearch.vector(onesearch.vector, integer, boolean) AS IMPLICIT;

CREATE FUNCTION onesearch.cosine_distance(onesearch.vector, onesearch.vector)
RETURNS double precision AS 'MODULE_PATHNAME', 'onesearch_cosine_distance'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OPERATOR onesearch.<=> (
    LEFTARG = onesearch.vector,
    RIGHTARG = onesearch.vector,
    FUNCTION = onesearch.cosine_distance,
    COMMUTATOR = OPERATOR(onesearch.<=>)
);

COMMENT ON TYPE onesearch.vector IS
'Experimental float32 vector, 2-4096 dimensions; no remote index in this skeleton';
COMMENT ON FUNCTION onesearch.cosine_distance(onesearch.vector, onesearch.vector) IS
'Local exact cosine distance; rejects zero vectors; NULL inputs return NULL';

-- Installation requires a superuser; ordinary roles can use the value type.
GRANT USAGE ON SCHEMA onesearch TO PUBLIC;
