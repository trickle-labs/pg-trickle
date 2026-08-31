CREATE TABLE vector_agg_source (
    id bigint PRIMARY KEY,
    group_id integer NOT NULL,
    amount integer
);

SET pg_trickle.refresh_strategy = 'differential';

INSERT INTO vector_agg_source (id, group_id, amount)
SELECT i, (i % 10000)::integer, ((i * 17 + 13) % 100000)::integer
FROM generate_series(1, 1000000) AS rows(i);

ANALYZE vector_agg_source;

SELECT pgtrickle.create_stream_table(
    'vector_agg_result',
    $$
        SELECT group_id,
               SUM(amount) AS amount_sum,
               COUNT(*) AS row_count,
               AVG(amount) AS amount_avg
        FROM vector_agg_source
        GROUP BY group_id
    $$,
    '1m',
    'DIFFERENTIAL'
);

UPDATE vector_agg_source
SET amount = amount + 1
WHERE id BETWEEN 1 AND 70000;

DELETE FROM vector_agg_source
WHERE id BETWEEN 70001 AND 85000;

INSERT INTO vector_agg_source (id, group_id, amount)
SELECT 1000000 + i,
       ((1000000 + i) % 10000)::integer,
       (((1000000 + i) * 17 + 13) % 100000)::integer
FROM generate_series(1, 15000) AS rows(i);
