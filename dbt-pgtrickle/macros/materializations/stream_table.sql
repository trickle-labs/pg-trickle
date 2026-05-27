{#
  stream_table materialization

  Custom dbt materialization that maps dbt's lifecycle onto pg_trickle's SQL API.

  When pg_trickle ≥ 0.6.0 is available, uses the idempotent
  create_or_replace_stream_table() — one function call handles create, no-op,
  config-only alter, and full query replacement automatically.

  Falls back to the legacy check-then-decide pattern for pg_trickle < 0.6.0.

  Config keys:
    materialized: 'stream_table'
    schedule: str|null (default '1m')
    refresh_mode: 'AUTO', 'FULL', 'DIFFERENTIAL', or 'IMMEDIATE' (default 'AUTO')
    initialize: bool (default true)
    status: 'ACTIVE' or 'PAUSED' or null (default null — no change)
    stream_table_name: str (default model name)
    stream_table_schema: str (default target schema)
    cdc_mode: 'auto', 'trigger', 'wal', or null (default null — use GUC)
    partition_by: str|null — partition storage table by RANGE on this column
    fuse: 'off'|'on'|'auto'|null — circuit-breaker fuse mode
    fuse_ceiling: int|null — fuse change-count ceiling
    fuse_sensitivity: int|null — fuse consecutive-observation threshold
    append_only: bool (default false) — skip delete bookkeeping for insert-only sources
    temporal: bool (default false) — enable temporal IVM mode
    storage_backend: 'heap'|'citus'|'unlogged'|null — columnar/storage backend
    diamond_consistency: 'STRICT'|'RELAXED'|null — diamond dependency consistency policy
    diamond_schedule_policy: 'ATOMIC'|'INDEPENDENT'|null — diamond scheduling policy
    pooler_compatibility_mode: bool (default false) — pgBouncer/Odyssey compatibility
    max_differential_joins: int|null — cap on join count in DIFFERENTIAL mode
    max_delta_fraction: float|null — delta/full fallback threshold (0.0–1.0)
    output_distribution_column: str|null — Citus distribution column for the storage table
#}
{% materialization stream_table, adapter='postgres' %}

  {%- set target_relation = this.incorporate(type='table') -%}

  {# -- Model config -- #}
  {%- set schedule = config.get('schedule', '1m') -%}
  {%- set refresh_mode = config.get('refresh_mode', 'AUTO') -%}
  {%- set cdc_mode = config.get('cdc_mode', none) -%}
  {%- set initialize = config.get('initialize', true) -%}
  {%- set status = config.get('status', none) -%}
  {%- set st_name = config.get('stream_table_name', target_relation.identifier) -%}
  {%- set st_schema = config.get('stream_table_schema', target_relation.schema) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set fuse = config.get('fuse', none) -%}
  {%- set fuse_ceiling = config.get('fuse_ceiling', none) -%}
  {%- set fuse_sensitivity = config.get('fuse_sensitivity', none) -%}
  {# A46-17: options added to match CreateStreamTableOptions in Rust #}
  {%- set append_only = config.get('append_only', false) -%}
  {%- set temporal = config.get('temporal', false) -%}
  {%- set storage_backend = config.get('storage_backend', none) -%}
  {%- set diamond_consistency = config.get('diamond_consistency', none) -%}
  {%- set diamond_schedule_policy = config.get('diamond_schedule_policy', none) -%}
  {%- set pooler_compatibility_mode = config.get('pooler_compatibility_mode', false) -%}
  {%- set max_differential_joins = config.get('max_differential_joins', none) -%}
  {%- set max_delta_fraction = config.get('max_delta_fraction', none) -%}
  {%- set output_distribution_column = config.get('output_distribution_column', none) -%}
  {#- should_full_refresh() is the stable API from dbt 1.0+; flags.FULL_REFRESH
      was deprecated in dbt 1.10 and may warn or fail in 1.11+. -#}
  {%- set full_refresh_mode = should_full_refresh() -%}

  {# -- Always schema-qualify the stream table name -- #}
  {%- set qualified_name = st_schema ~ '.' ~ st_name -%}

  {# -- Authoritative existence check via pg_trickle catalog.
       We don't rely solely on dbt's relation cache because the stream table
       may have been created/dropped outside dbt. -- #}
  {%- set st_exists = dbt_pgtrickle.pgtrickle_stream_table_exists(qualified_name) -%}

  {{ log("pg_trickle: materializing stream table '" ~ qualified_name ~ "'", info=true) }}

  {{ run_hooks(pre_hooks) }}

  {# -- Full refresh: drop and recreate -- #}
  {% if full_refresh_mode and st_exists %}
    {{ dbt_pgtrickle.pgtrickle_drop_stream_table(qualified_name) }}
    {% set st_exists = false %}
  {% endif %}

  {# -- Get the compiled SQL (the defining query) -- #}
  {%- set defining_query = sql -%}

  {# -- Detect whether create_or_replace_stream_table() is available (≥ 0.6.0).
       Cache the result per invocation so we only probe once. -- #}
  {%- set has_cor = dbt_pgtrickle.pgtrickle_has_create_or_replace() -%}

  {% if has_cor and not full_refresh_mode %}
    {# ── Fast path: idempotent create_or_replace (pg_trickle ≥ 0.6.0) ── #}
    {{ dbt_pgtrickle.pgtrickle_create_or_replace_stream_table(
         qualified_name, defining_query, schedule, refresh_mode, initialize, cdc_mode,
         partition_by=partition_by,
         append_only=append_only, temporal=temporal, storage_backend=storage_backend,
         diamond_consistency=diamond_consistency, diamond_schedule_policy=diamond_schedule_policy,
         pooler_compatibility_mode=pooler_compatibility_mode,
         max_differential_joins=max_differential_joins, max_delta_fraction=max_delta_fraction,
         output_distribution_column=output_distribution_column
       ) }}

    {# Handle status/fuse changes separately — create_or_replace doesn't accept them #}
    {% if (status is not none and st_exists) or fuse is not none or fuse_ceiling is not none or fuse_sensitivity is not none %}
      {%- set current_info = dbt_pgtrickle.pgtrickle_get_stream_table_info(qualified_name) -%}
      {% if current_info and (current_info.status != status or fuse is not none or fuse_ceiling is not none or fuse_sensitivity is not none) %}
        {{ dbt_pgtrickle.pgtrickle_alter_stream_table(
             qualified_name, schedule, refresh_mode,
             status=status, current_info=current_info,
             cdc_mode=cdc_mode,
             fuse=fuse, fuse_ceiling=fuse_ceiling, fuse_sensitivity=fuse_sensitivity,
             append_only=append_only, max_differential_joins=max_differential_joins,
             max_delta_fraction=max_delta_fraction
           ) }}
      {% endif %}
    {% endif %}
  {% else %}
    {# ── Legacy path: check-then-decide (pg_trickle < 0.6.0 or --full-refresh) ── #}
    {% if not st_exists %}
      {# -- CREATE: stream table does not exist yet -- #}
      {{ dbt_pgtrickle.pgtrickle_create_stream_table(
            qualified_name, defining_query, schedule, refresh_mode, initialize, cdc_mode,
            partition_by=partition_by,
            append_only=append_only, temporal=temporal, storage_backend=storage_backend,
            diamond_consistency=diamond_consistency, diamond_schedule_policy=diamond_schedule_policy,
            pooler_compatibility_mode=pooler_compatibility_mode,
            max_differential_joins=max_differential_joins, max_delta_fraction=max_delta_fraction,
            output_distribution_column=output_distribution_column
         ) }}

      {# -- Apply fuse/status settings that CREATE doesn't support -- #}
      {% if status is not none or fuse is not none or fuse_ceiling is not none or fuse_sensitivity is not none %}
        {%- set current_info = dbt_pgtrickle.pgtrickle_get_stream_table_info(qualified_name) -%}
        {% if current_info %}
          {{ dbt_pgtrickle.pgtrickle_alter_stream_table(
               qualified_name, schedule, refresh_mode,
               status=status, current_info=current_info,
               cdc_mode=cdc_mode,
               fuse=fuse, fuse_ceiling=fuse_ceiling, fuse_sensitivity=fuse_sensitivity,
               append_only=append_only, max_differential_joins=max_differential_joins,
               max_delta_fraction=max_delta_fraction
             ) }}
        {% endif %}
      {% endif %}
    {% else %}
      {# -- UPDATE: stream table exists — check if query changed -- #}
      {%- set current_info = dbt_pgtrickle.pgtrickle_get_stream_table_info(qualified_name) -%}

      {% if current_info and current_info.defining_query != defining_query %}
        {# Query changed: use ALTER ... query => to migrate in place #}
        {{ log("pg_trickle: query changed — altering '" ~ qualified_name ~ "' in place", info=true) }}
        {{ dbt_pgtrickle.pgtrickle_alter_stream_table(
             qualified_name, schedule, refresh_mode,
             status=status, current_info=current_info,
             cdc_mode=cdc_mode,
             query=defining_query,
             fuse=fuse, fuse_ceiling=fuse_ceiling, fuse_sensitivity=fuse_sensitivity,
             append_only=append_only, max_differential_joins=max_differential_joins,
             max_delta_fraction=max_delta_fraction
           ) }}
      {% else %}
        {# Query unchanged: update schedule/mode/status/fuse if they differ.
           Pass current_info to avoid redundant catalog lookup. #}
        {{ dbt_pgtrickle.pgtrickle_alter_stream_table(
             qualified_name, schedule, refresh_mode,
             status=status, current_info=current_info,
             cdc_mode=cdc_mode,
             fuse=fuse, fuse_ceiling=fuse_ceiling, fuse_sensitivity=fuse_sensitivity,
             append_only=append_only, max_differential_joins=max_differential_joins,
             max_delta_fraction=max_delta_fraction
           ) }}
      {% endif %}
    {% endif %}
  {% endif %}

  {# dbt requires the 'main' statement to be executed at least once.
     Our DDL runs via run_query() (separate connection), so we satisfy the
     framework with a lightweight no-op on the main connection. #}
  {% call statement('main') %}
    SELECT 1
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
