{#
  pgtrickle_create_stream_table(name, query, schedule, refresh_mode, initialize, cdc_mode, ...)

  Creates a new stream table via pgtrickle.create_stream_table().
  Called by the stream_table materialization on first run.

  Args:
    name (str): Stream table name (may be schema-qualified)
    query (str): The defining SQL query
    schedule (str|none): Refresh schedule (e.g., '1m', '5m', '0 */2 * * *').
                         Pass none for pg_trickle's CALCULATED schedule (SQL NULL).
    refresh_mode (str): 'FULL', 'DIFFERENTIAL', 'AUTO', or 'IMMEDIATE'
    initialize (bool): Whether to populate immediately on creation
    cdc_mode (str|none): Optional CDC mode override ('auto', 'trigger', 'wal')
    partition_by (str|none): Optional column name to partition the storage table by (RANGE).
                             Cannot be changed after creation.
    append_only (bool): Skip delete bookkeeping for insert-only sources (default false)
    temporal (bool): Enable temporal IVM mode (default false)
    storage_backend (str|none): Columnar backend ('heap','citus','unlogged')
    diamond_consistency (str|none): Diamond dependency consistency ('STRICT','RELAXED')
    diamond_schedule_policy (str|none): Diamond scheduling policy ('ATOMIC','INDEPENDENT')
    pooler_compatibility_mode (bool): pgBouncer/Odyssey compatibility (default false)
    max_differential_joins (int|none): Cap on join count in DIFFERENTIAL mode
    max_delta_fraction (float|none): Delta/full fallback threshold (0.0–1.0)
    output_distribution_column (str|none): Citus distribution column for storage table
#}
{% macro pgtrickle_create_stream_table(name, query, schedule, refresh_mode, initialize, cdc_mode=none, partition_by=none, append_only=false, temporal=false, storage_backend=none, diamond_consistency=none, diamond_schedule_policy=none, pooler_compatibility_mode=false, max_differential_joins=none, max_delta_fraction=none, output_distribution_column=none) %}
  {#
    Run create_stream_table() outside of dbt's model transaction.
    dbt wraps the model's main statement in BEGIN...ROLLBACK (for testing /
    dry-run purposes). Any SQL executed via run_query() shares the same
    connection and is therefore also rolled back. To prevent this, we embed
    explicit BEGIN / COMMIT so the DDL commits unconditionally.
  #}
  {% call statement('pgtrickle_create', auto_begin=False, fetch_result=False) %}
    BEGIN;
    SELECT pgtrickle.create_stream_table(
      {{ dbt.string_literal(name) }},
      $pgtrickle${{ query }}$pgtrickle$,
      {% if schedule is none %}'calculated'{% else %}{{ dbt.string_literal(schedule) }}{% endif %},
      {{ dbt.string_literal(refresh_mode) }},
      {{ initialize }},
      {% if diamond_consistency is none %}NULL{% else %}{{ dbt.string_literal(diamond_consistency) }}{% endif %},
      {% if diamond_schedule_policy is none %}NULL{% else %}{{ dbt.string_literal(diamond_schedule_policy) }}{% endif %},
      {% if cdc_mode is none %}NULL{% else %}{{ dbt.string_literal(cdc_mode) }}{% endif %},
      {{ append_only }},
      {{ pooler_compatibility_mode }},
      {% if partition_by is none %}NULL{% else %}{{ dbt.string_literal(partition_by) }}{% endif %},
      {% if max_differential_joins is none %}NULL{% else %}{{ max_differential_joins }}{% endif %},
      {% if max_delta_fraction is none %}NULL{% else %}{{ max_delta_fraction }}{% endif %},
      {% if output_distribution_column is none %}NULL{% else %}{{ dbt.string_literal(output_distribution_column) }}{% endif %},
      {{ temporal }},
      {% if storage_backend is none %}NULL{% else %}{{ dbt.string_literal(storage_backend) }}{% endif %}
    );
    COMMIT;
  {% endcall %}
  {{ log("pg_trickle: created stream table '" ~ name ~ "'", info=true) }}
{% endmacro %}
