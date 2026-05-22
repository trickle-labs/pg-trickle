# OpenTelemetry Operator Guide

> **Status:** Available feature with comprehensive operator guidance.

pg_trickle supports W3C Trace Context propagation through the refresh pipeline.
When enabled, distributed traces initiated in application sessions are linked to
CDC capture events and refresh spans, allowing full end-to-end latency attribution
from user write to materialized result.

---

## How It Works

1. **Application session** sets `pg_trickle.trace_id` to a W3C traceparent header.
2. **CDC trigger** captures `pg_trickle.trace_id` from the session GUC and stores
   it in the `__pgt_trace_context` column of the change buffer table.
3. **Scheduler** reads the trace context when consuming the change buffer.
4. **Refresh pipeline** propagates the trace context through the DIFF/FULL refresh
   cycle and exports a child span to the configured OTLP endpoint.

```
Application session                pg_trickle background worker
      │                                       │
      │ SET pg_trickle.trace_id = '...'       │
      │ INSERT INTO source_table ...          │
      │  └─► CDC trigger captures trace ──────┤
      │                                       │ Reads change buffer
      │                                       │ Opens child span
      │                                       │ Exports span to OTLP
      │                                       └─► Refresh complete
```

---

## Configuration

### Minimal setup

```sql
-- Enable trace propagation
ALTER SYSTEM SET pg_trickle.enable_trace_propagation = true;

-- Set the OTLP endpoint (gRPC)
ALTER SYSTEM SET pg_trickle.otel_endpoint = 'http://localhost:4317';

SELECT pg_reload_conf();
```

### Per-session usage

```sql
-- Set traceparent before DML (propagates through CDC to refresh span)
SET pg_trickle.trace_id = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';

-- Your DML here
INSERT INTO orders (id, total) VALUES (42, 99.99);
-- pg_trickle CDC trigger stores the trace context in the change buffer
```

---

## GUC Reference

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `pg_trickle.enable_trace_propagation` | `bool` | `false` | Enable W3C Trace Context capture and export |
| `pg_trickle.otel_endpoint` | `string` | `''` | OTLP/gRPC endpoint (empty = disabled) |
| `pg_trickle.trace_id` | `string` | `''` | Session W3C traceparent header |

---

## Collector Configuration Examples

### Jaeger (all-in-one)

```bash
docker run -d \
  -p 4317:4317 \
  -p 16686:16686 \
  jaegertracing/all-in-one:latest
```

```sql
ALTER SYSTEM SET pg_trickle.otel_endpoint = 'http://localhost:4317';
ALTER SYSTEM SET pg_trickle.enable_trace_propagation = true;
SELECT pg_reload_conf();
```

Access traces at `http://localhost:16686`. Look for service name `pg_trickle`.

### Grafana Tempo

```yaml
# docker-compose.yaml excerpt
tempo:
  image: grafana/tempo:latest
  ports:
    - "4317:4317"   # OTLP gRPC
    - "3200:3200"   # Tempo HTTP API
```

```sql
ALTER SYSTEM SET pg_trickle.otel_endpoint = 'http://tempo:4317';
```

### OpenTelemetry Collector (recommended for production)

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"

exporters:
  otlp:
    endpoint: "your-backend:4317"
    tls:
      insecure: false

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp]
```

```sql
ALTER SYSTEM SET pg_trickle.otel_endpoint = 'http://otel-collector:4317';
```

---

## Failure Behavior

pg_trickle's trace export is **best-effort**:

| Scenario | Behavior |
|----------|----------|
| OTLP endpoint unreachable | Span export silently skipped; refresh continues |
| OTLP endpoint returns error | Warning logged; refresh continues |
| OTLP connection timeout | Export attempt abandoned after 2 s; refresh continues |
| `trace_id` not set | Span has no parent; exported as a root span |
| `enable_trace_propagation = false` | No spans exported; no overhead |

> **Important:** Trace export failures never block or delay refresh cycles.
> Monitoring refresh latency with `pgtrickle.sla_summary()` is unaffected by
> OTLP endpoint health.

---

## Verifying Trace Export

### Check that trace context is captured

```sql
-- After enabling trace propagation and making a write:
SET pg_trickle.trace_id = '00-aaaabbbbccccdddd0000111122223333-0102030405060708-01';
INSERT INTO my_source_table VALUES (...);

-- Check the change buffer (replace <oid> with the source table OID):
SELECT __pgt_trace_context
FROM pgtrickle_changes.changes_<oid>
ORDER BY ctid DESC
LIMIT 5;
-- Should return the traceparent you set
```

### Check the OTLP endpoint

```bash
# Quick check: send a test span to verify connectivity
grpcurl -plaintext -d '{}' localhost:4317 opentelemetry.proto.collector.trace.v1.TraceService/Export
```

---

## Span Attributes

Refresh spans exported by pg_trickle include:

| Attribute | Value |
|-----------|-------|
| `service.name` | `pg_trickle` |
| `db.system` | `postgresql` |
| `db.name` | current database name |
| `pgt.stream_table` | `schema.stream_table_name` |
| `pgt.refresh_mode` | `DIFFERENTIAL` or `FULL` |
| `pgt.duration_ms` | Refresh duration in milliseconds |
| `pgt.cycle_id` | Scheduler cycle identifier |

---

## Troubleshooting

### Spans not appearing in the collector

1. Verify `enable_trace_propagation = true`:
   ```sql
   SHOW pg_trickle.enable_trace_propagation;
   ```

2. Verify `otel_endpoint` is set and reachable:
   ```sql
   SHOW pg_trickle.otel_endpoint;
   ```

3. Check PostgreSQL logs for OTLP export warnings:
   ```bash
   grep -i "otel\|otlp\|trace" /var/log/postgresql/postgresql.log
   ```

4. Ensure the collector is listening on the correct port and protocol (gRPC, not HTTP/1.1).

### `__pgt_trace_context` column missing

The column is added automatically when upgrading. If it is missing
from a change buffer table:

```sql
-- Re-run the migration (idempotent):
ALTER TABLE pgtrickle_changes.changes_<oid>
  ADD COLUMN IF NOT EXISTS __pgt_trace_context TEXT;
```

---

## Integration Test

A dockerized integration test against a local OTLP collector is available
under `tests/e2e_otel_tests.rs`. It verifies:

- Span export success to a live collector
- Timeout and endpoint-failure handling (export is skipped, refresh completes)
- Trace context round-trip from session GUC through change buffer to span

Run with:
```bash
just test-e2e -- --test e2e_otel_tests
```

---

*See also: [CONFIGURATION.md](CONFIGURATION.md) · [TROUBLESHOOTING.md](TROUBLESHOOTING.md)*
