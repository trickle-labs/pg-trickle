#!/usr/bin/env python3
"""Generate sql/archive/pg_trickle--0.81.0.sql from the 0.80.0 base."""

with open('sql/archive/pg_trickle--0.80.0.sql', 'r') as f:
    lines = f.readlines()

# Find the last finalize block start
cutoff = len(lines)
for i in range(len(lines) - 1, -1, -1):
    if '-- finalize' in lines[i]:
        for j in range(i - 1, -1, -1):
            if '/* <begin connected objects> */' in lines[j]:
                cutoff = j
                break
        break

base = lines[:cutoff]

additions = [
    '\n',
    '/* <begin connected objects> */\n',
    '-- src/api/diagnostics.rs\n',
    '-- pg_trickle::api::diagnostics::commit_latency_stats\n',
    'CREATE  FUNCTION pgtrickle."commit_latency_stats"() RETURNS TABLE (\n',
    '        "pgt_schema" TEXT,  /* String */\n',
    '        "pgt_name" TEXT,  /* String */\n',
    '        "samples" bigint,  /* i64 */\n',
    '        "min_ms" double precision,  /* f64 */\n',
    '        "p50_ms" double precision,  /* f64 */\n',
    '        "p95_ms" double precision,  /* f64 */\n',
    '        "p95_ms" double precision,  /* f64 */\n',
    '        "max_ms" double precision,  /* f64 */\n',
    '        "tracking_mode" TEXT  /* String */\n',
    ')\n',
    'STABLE PARALLEL SAFE\n',
    'LANGUAGE c /* Rust */\n',
    "AS 'MODULE_PATHNAME', 'commit_latency_stats_wrapper';\n",
    '/* </end connected objects> */\n',
    '\n',
    '/* <begin connected objects> */\n',
    '-- src/api/diagnostics.rs\n',
    '-- pg_trickle::api::diagnostics::tune_recommendations\n',
    'CREATE  FUNCTION pgtrickle."tune_recommendations"() RETURNS TABLE (\n',
    '        "guc_name" TEXT,  /* String */\n',
    '        "current_value" TEXT,  /* String */\n',
    '        "recommended_value" TEXT,  /* String */\n',
    '        "reason" TEXT  /* String */\n',
    ')\n',
    'STABLE PARALLEL SAFE\n',
    'LANGUAGE c /* Rust */\n',
    "AS 'MODULE_PATHNAME', 'tune_recommendations_wrapper';\n",
    '/* </end connected objects> */\n',
    '\n',
    '/* <begin connected objects> */\n',
    '-- src/api/diagnostics.rs\n',
    '-- pg_trickle::api::diagnostics::preview_stream_table\n',
    'CREATE  FUNCTION pgtrickle."preview_stream_table"(\n',
    '        "query" TEXT /* & str */\n',
    ') RETURNS TABLE (\n',
    '        "property" TEXT,  /* String */\n',
    '        "value" TEXT  /* String */\n',
    ')\n',
    'LANGUAGE c /* Rust */\n',
    "AS 'MODULE_PATHNAME', 'preview_stream_table_wrapper';\n",
    '/* </end connected objects> */\n',
    '\n',
    '/* <begin connected objects> */\n',
    '-- src/api/create.rs\n',
    '-- pg_trickle::api::create::create_stream_table_realtime\n',
    'CREATE  FUNCTION pgtrickle."create_stream_table_realtime"(\n',
    '        "name" TEXT, /* & str */\n',
    '        "query" TEXT, /* & str */\n',
    '        "cdc_mode" TEXT DEFAULT NULL, /* Option < & str > */\n',
    '        "append_only" bool DEFAULT false, /* bool */\n',
    '        "partition_by" TEXT DEFAULT NULL, /* Option < & str > */\n',
    '        "max_differential_joins" INT DEFAULT NULL, /* Option < i32 > */\n',
    '        "max_delta_fraction" double precision DEFAULT NULL /* Option < f64 > */\n',
    ') RETURNS void\n',
    '\n',
    'LANGUAGE c /* Rust */\n',
    "AS 'MODULE_PATHNAME', 'create_stream_table_realtime_wrapper';\n",
    '/* </end connected objects> */\n',
    '\n',
    '/* <begin connected objects> */\n',
    '-- src/api/create.rs\n',
    '-- pg_trickle::api::create::create_stream_table_batch\n',
    'CREATE  FUNCTION pgtrickle."create_stream_table_batch"(\n',
    '        "name" TEXT, /* & str */\n',
    '        "query" TEXT, /* & str */\n',
    '        "cdc_mode" TEXT DEFAULT NULL, /* Option < & str > */\n',
    '        "append_only" bool DEFAULT false, /* bool */\n',
    '        "partition_by" TEXT DEFAULT NULL, /* Option < & str > */\n',
    '        "max_differential_joins" INT DEFAULT NULL, /* Option < i32 > */\n',
    '        "max_delta_fraction" double precision DEFAULT NULL /* Option < f64 > */\n',
    ') RETURNS void\n',
    '\n',
    'LANGUAGE c /* Rust */\n',
    "AS 'MODULE_PATHNAME', 'create_stream_table_batch_wrapper';\n",
    '/* </end connected objects> */\n',
    '\n',
    '/* <begin connected objects> */\n',
    '-- src/api/create.rs\n',
    '-- pg_trickle::api::create::create_stream_table_cost_optimized\n',
    'CREATE  FUNCTION pgtrickle."create_stream_table_cost_optimized"(\n',
    '        "name" TEXT, /* & str */\n',
    '        "query" TEXT, /* & str */\n',
    '        "cdc_mode" TEXT DEFAULT NULL, /* Option < & str > */\n',
    '        "append_only" bool DEFAULT false, /* bool */\n',
    '        "partition_by" TEXT DEFAULT NULL, /* Option < & str > */\n',
    '        "max_differential_joins" INT DEFAULT NULL, /* Option < i32 > */\n',
    '        "max_delta_fraction" double precision DEFAULT NULL /* Option < f64 > */\n',
    ') RETURNS void\n',
    '\n',
    'LANGUAGE c /* Rust */\n',
    "AS 'MODULE_PATHNAME', 'create_stream_table_cost_optimized_wrapper';\n",
    '/* </end connected objects> */\n',
    '\n',
    '/* <begin connected objects> */\n',
    '-- src/lib.rs:1124\n',
    '-- requires:\n',
    '--   _signal_launcher_rescan\n',
    '\n',
    '-- finalize\n',
    '\n',
    'SELECT pgtrickle._signal_launcher_rescan();\n',
    '/* </end connected objects> */\n',
]

with open('sql/archive/pg_trickle--0.81.0.sql', 'w') as f:
    f.writelines(base)
    f.writelines(additions)

total = sum(1 for _ in open('sql/archive/pg_trickle--0.81.0.sql'))
print(f'Done. Total lines: {total}')
