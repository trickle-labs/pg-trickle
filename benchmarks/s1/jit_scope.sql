\set ON_ERROR_STOP on
CREATE TABLE s1_jit_source (id int PRIMARY KEY, value int NOT NULL);
INSERT INTO s1_jit_source VALUES (1, 1);
SELECT pgtrickle.create_stream_table(
    name => 's1_jit_stream', query => 'SELECT id, value FROM s1_jit_source',
    schedule => '1h', refresh_mode => 'DIFFERENTIAL', initialize => false,
    orchestration_mode => 'EXTERNAL');
CREATE TABLE s1_jit_seen (enabled boolean NOT NULL);
CREATE FUNCTION s1_jit_audit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO public.s1_jit_seen VALUES (current_setting('jit')::boolean);
    RETURN NULL;
END $$;
CREATE TRIGGER s1_jit_audit AFTER INSERT OR UPDATE OR DELETE ON s1_jit_stream
    FOR EACH ROW EXECUTE FUNCTION s1_jit_audit();
BEGIN;
SET LOCAL jit = on;
SET LOCAL pg_trickle.planner_aggressive = on;
DO $$
DECLARE digest bytea;
BEGIN
    SELECT graph_digest INTO STRICT digest
      FROM pgtrickle.graph_contract(ARRAY['public.s1_jit_stream'::regclass]);
    PERFORM 1 FROM pgtrickle.refresh_graph_strict(
        ARRAY['public.s1_jit_stream'::regclass], digest, 'ALLOW');
    TRUNCATE s1_jit_seen;
    UPDATE s1_jit_source SET value = 2 WHERE id = 1;
    PERFORM 1 FROM pgtrickle.refresh_graph_strict(
        ARRAY['public.s1_jit_stream'::regclass], digest, 'ERROR');
    IF NOT EXISTS (SELECT FROM s1_jit_seen)
       OR EXISTS (SELECT FROM s1_jit_seen WHERE enabled)
       OR current_setting('jit') <> 'on' THEN
        RAISE EXCEPTION 'candidate lacks scoped small-graph JIT hint or leaks it';
    END IF;
    IF (SELECT array_agg(ROW(id, value)::text ORDER BY id) FROM s1_jit_stream)
       IS DISTINCT FROM ARRAY['(1,2)'] THEN
        RAISE EXCEPTION 'small-graph JIT regression changed complete output';
    END IF;
END $$;
ROLLBACK;
