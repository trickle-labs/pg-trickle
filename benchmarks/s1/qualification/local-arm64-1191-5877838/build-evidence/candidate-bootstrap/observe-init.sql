COPY (SELECT json_build_object(
        'max_worker_processes', current_setting('max_worker_processes')::integer,
        'extension_workers', (SELECT count(*) FROM pg_stat_activity
            WHERE backend_type LIKE 'pg_trickle%' OR backend_type LIKE 'pg-react%')))
        TO '/tmp/s1-init-settings.json';
