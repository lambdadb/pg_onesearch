CREATE EXTENSION pg_onesearch;
CREATE SCHEMA pgos_executor_probe;
REVOKE ALL ON SCHEMA pgos_executor_probe FROM PUBLIC;
CREATE TABLE pgos_executor_probe.documents (
  id bigint PRIMARY KEY CHECK (id BETWEEN 1 AND 5),
  content text NOT NULL,
  embedding onesearch.vector(3) NOT NULL
);
INSERT INTO pgos_executor_probe.documents VALUES
 (1,'alpha alpha beta','[1,0,0]'),
 (2,'alpha beta beta beta','[0.8,0.6,0]'),
 (3,'beta gamma','[0,1,0]'),
 (4,'delta epsilon','[-1,0,0]'),
 (5,'alpha alpha beta','[1,0,0]');
CREATE FUNCTION pgos_executor_probe.frozen() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'executor probe fixture is frozen' USING ERRCODE='0A000'; END $$;
CREATE TRIGGER frozen BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON pgos_executor_probe.documents
FOR EACH STATEMENT EXECUTE FUNCTION pgos_executor_probe.frozen();
CREATE FUNCTION pgos_executor_probe.match(text,text) RETURNS boolean
AS '$libdir/pg_onesearch_executor_probe','pgos_executor_match' LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
CREATE FUNCTION pgos_executor_probe.vector_match(onesearch.vector,onesearch.vector) RETURNS boolean
AS '$libdir/pg_onesearch_executor_probe','pgos_executor_match' LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
CREATE FUNCTION pgos_executor_probe.score(regclass,bigint) RETURNS double precision
AS '$libdir/pg_onesearch_executor_probe','pgos_executor_score' LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgos_executor_probe FROM PUBLIC;
ANALYZE pgos_executor_probe.documents;
