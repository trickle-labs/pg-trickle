\set ON_ERROR_STOP on
-- Run only in a disposable database with pg_trickle installed and scheduler disabled.
CREATE SCHEMA retention_delete_probe;
SET search_path = retention_delete_probe, public;
CREATE TABLE events (id int NOT NULL, d date NOT NULL, amount int NOT NULL) PARTITION BY RANGE (d);
CREATE TABLE old PARTITION OF events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE live PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
INSERT INTO events VALUES (1,'2025-06-01',10),(2,'2026-06-01',20),(3,'2026-06-02',9);
SELECT pgtrickle.create_stream_table('totals','SELECT d,sum(amount) AS total,count(*) AS n FROM events GROUP BY d','1h','DIFFERENTIAL',partition_by=>'HASH:d');
SELECT pgtrickle.create_stream_table('downstream','SELECT d,total,n FROM totals','1h','DIFFERENTIAL');
SELECT pgtrickle.set_stream_table_partition_retention('totals','TRIM','d','2026-01-01');
DELETE FROM events WHERE id=3;
UPDATE events SET amount=21 WHERE id=2;
INSERT INTO events VALUES (4,'2026-06-03',11);
LOAD 'auto_explain';
SET client_min_messages=log;
SET auto_explain.log_min_duration=0;
SET auto_explain.log_analyze=on;
SET auto_explain.log_timing=off;
SET auto_explain.log_nested_statements=on;
SET auto_explain.log_verbose=on;
SET auto_explain.log_format=json;
ALTER TABLE events DETACH PARTITION old;
SET auto_explain.log_min_duration=-1;
SELECT pgtrickle.refresh_stream_table('downstream');
DO $$
BEGIN
  IF EXISTS (((SELECT d,sum(amount) AS total,count(*) AS n FROM events GROUP BY d)
              EXCEPT ALL (SELECT d,total,n FROM totals))
             UNION ALL ((SELECT d,total,n FROM totals)
                        EXCEPT ALL (SELECT d,sum(amount) AS total,count(*) AS n FROM events GROUP BY d)))
     OR EXISTS (((SELECT d,total,n FROM totals) EXCEPT ALL (SELECT d,total,n FROM downstream))
                UNION ALL ((SELECT d,total,n FROM downstream) EXCEPT ALL (SELECT d,total,n FROM totals))) THEN
    RAISE EXCEPTION 'retention output differs from the source or downstream';
  END IF;
  IF (SELECT count(*) FROM pgtrickle.pgt_stream_tables s
             CROSS JOIN LATERAL (SELECT h.action,h.status,h.was_full_fallback
               FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id=s.pgt_id ORDER BY h.refresh_id DESC LIMIT 1) h
             WHERE s.pgt_schema='retention_delete_probe' AND s.pgt_name IN ('totals','downstream')
               AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback) <> 2 THEN
    RAISE EXCEPTION 'expected completed differential refreshes';
  END IF;
END $$;
DROP SCHEMA retention_delete_probe CASCADE;
