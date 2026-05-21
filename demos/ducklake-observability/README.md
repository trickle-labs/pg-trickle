# Demo: DuckLake Observability in a Box

*Production-quality monitoring for your DuckLake deployment — five minutes from clone to live Grafana dashboard*

---

## What you'll build

You'll attach a pre-built Grafana dashboard to an existing DuckLake PostgreSQL
catalog. Five pg_trickle stream tables continuously aggregate DuckLake's metadata
tables (file counts, snapshot rates, storage growth) and surface the results as
live operational panels.

The result is a single-URL monitoring page that answers:
- **"Do I need to run compaction?"** — small file counts per table
- **"Is my ingestion rate healthy?"** — commits per minute per table
- **"How fast is my lake growing?"** — active bytes over time
- **"Who is writing what?"** — tenant write activity (multi-tenant deployments)

> **Don't have an existing DuckLake instance?**
> Jump to [Sandbox Mode](#sandbox-mode-no-existing-ducklake-instance) to spin up
> a self-contained demo catalog in 30 seconds.

---

## Background: How does this work?

DuckLake stores its catalog metadata in ordinary PostgreSQL tables:
`ducklake_data_file`, `ducklake_snapshot`, `ducklake_table_changes`, and so on.
These are the same tables DuckDB queries internally when you attach a DuckLake
catalog.

pg_trickle stream tables are SQL views that refresh themselves on a schedule.
This demo installs five stream tables over DuckLake's metadata — each one watches
a different aspect of catalog health and keeps its result up to date differentially
(only processing the new rows since the last refresh).

Grafana reads the stream table results from PostgreSQL using a standard
`postgres` datasource. No extra connectors, no PromQL, no ClickHouse.

---

## Prerequisites

- **Docker Engine 24+** and **Docker Compose v2**
  Verify: `docker compose version`
- **An existing DuckLake PostgreSQL catalog** with pg_trickle installed
  - Alternatively, use [Sandbox Mode](#sandbox-mode-no-existing-ducklake-instance) below
- **`psql`** installed locally to run `init_monitoring.sql`
  - macOS: `brew install libpq`
  - Debian/Ubuntu: `apt install postgresql-client`

---

## Architecture

```
Your DuckLake PostgreSQL catalog
  │  (the database where DuckDB attaches as TYPE DUCKLAKE)
  │
  │  init_monitoring.sql installs these stream tables:
  │
  ├─ ducklake_small_file_counts    (1m DIFFERENTIAL)
  │    watches: ducklake_data_file
  │    answers: which tables need compaction?
  │
  ├─ ducklake_snapshot_rate        (30s DIFFERENTIAL)
  │    watches: ducklake_snapshot, ducklake_table_changes
  │    answers: how many commits/minute per table?
  │
  ├─ ducklake_storage_growth       (5m DIFFERENTIAL)
  │    watches: ducklake_data_file (active files only)
  │    answers: how many bytes are in use per table?
  │
  ├─ ducklake_tenant_activity      (5m DIFFERENTIAL)
  │    watches: ducklake_snapshot, ducklake_table_changes
  │    answers: which schemas are writing the most?
  │
  └─ ducklake_compaction_events    (5m DIFFERENTIAL)
       watches: ducklake_data_file (deleted files)
       answers: how much has been compacted?
       ▼
Grafana container  (port 3100)
  Dashboards: Overview, Small Files, Snapshot Rate, Storage Growth
  Alerts:     small_file_count > 50 (WARNING), > 200 (CRITICAL)
              snapshot_rate > 60/min per table
```

---

## Step 1: Configure the connection

```bash
cd demos/ducklake-observability
cp .env.example .env
```

Edit `.env` and fill in your connection details:

```bash
# .env
DATABASE_URL=postgresql://myuser:mypassword@myhost:5432/lake_catalog
GRAFANA_PORT=3100      # change if port 3100 is taken
```

All six variables used by Grafana's datasource provisioning are also set:

```bash
PG_HOST=myhost
PG_PORT=5432
PG_DATABASE=lake_catalog
PG_USER=myuser
PG_PASSWORD=mypassword
```

These are automatically read by `docker-compose.yml` and injected into Grafana.

---

## Step 2: Install the stream tables

Run `init_monitoring.sql` against your DuckLake catalog:

```bash
psql "$DATABASE_URL" -f init_monitoring.sql
```

Expected output:
```
CREATE EXTENSION
 create_stream_table
---------------------
 ducklake_small_file_counts
(1 row)
 create_stream_table
---------------------
 ducklake_snapshot_rate
(1 row)
... (5 stream tables total)
```

> If you see `ERROR: relation "ducklake_data_file" does not exist`, the target
> database is not a DuckLake catalog. Double-check `DATABASE_URL`.

Verify the stream tables were created:

```bash
psql "$DATABASE_URL" -c "
    SELECT table_name, schedule, status
    FROM pgtrickle.pgt_stream_tables
    WHERE table_name LIKE 'ducklake_%'
    ORDER BY table_name;
"
```

---

## Step 3: Start Grafana

```bash
docker compose up -d grafana
```

Only the `grafana` container starts — there's no local PostgreSQL here.
Grafana connects directly to your existing catalog over the network.

Open **http://localhost:3100** (or whichever port you set in `.env`).

- Login: `admin` / `admin`
- Navigate to **Dashboards → DuckLake Observability**

---

## Step 4: Understand the dashboards

### Overview dashboard

The first screen to check. Shows:
- **Total active files** across all tables in the catalog
- **Tables with small-file warnings** (count > 50)
- **Latest snapshot ID** — is new data arriving?
- **Total storage in use** (active Parquet files only)

### Small Files dashboard

Small files are the most common source of slow DuckLake reads. Each DuckLake
snapshot that writes < 10 MB files contributes a "small file". Over time, a
table might have hundreds of small files that DuckLake must open individually
for every query.

Panels:
- Bar chart of `small_file_count` per table (current value)
- Time series of small file accumulation
- Alert fires at > 50 files (WARNING) or > 200 (CRITICAL)

**When to act:** Run `CALL ducklake_compact('schema.table')` on tables in the
CRITICAL zone.

### Snapshot Rate dashboard

Shows commits per minute per table. Useful for:
- Detecting unexpectedly high write rates (misconfigured batch jobs)
- Confirming that scheduled ingestion jobs are actually running
- Identifying tables that are being written to too frequently (should batch more)

Alert threshold: > 60 commits/minute for any single table.

### Storage Growth dashboard

Cumulative active bytes per table over time. Grafana computes the growth rate
(bytes/hour) from the time series. Set the `STORAGE_QUOTA_GB` alert rule to
fire when projected usage would exceed a threshold within 7 days.

### Tenant Activity dashboard (multi-tenant deployments)

Groups write volume by `schema_name`. Useful for chargeback or capacity planning
when multiple teams share one DuckLake catalog.

---

## Sandbox mode (no existing DuckLake instance)

If you want to explore the dashboards without connecting to a real DuckLake
catalog, you can use the companion `ducklake-oltp-lake` demo as a sandbox:

```bash
# Terminal 1: start a self-contained DuckLake stack
cd demos/ducklake-oltp-lake
docker compose up -d

# Terminal 2: attach observability to that stack
cd demos/ducklake-observability
cat > .env << 'EOF'
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres
PG_HOST=host.docker.internal
PG_PORT=5432
PG_DATABASE=postgres
PG_USER=postgres
PG_PASSWORD=postgres
GRAFANA_PORT=3100
EOF

psql "$DATABASE_URL" -f init_monitoring.sql
docker compose up -d grafana
```

Open **http://localhost:3100** — the dashboards will show the OLTP-to-lake demo
catalog's metadata.

---

## Standalone mode (bring your own Grafana)

If you already run Grafana, skip the Docker container and import the dashboards
manually:

```bash
# 1. Install stream tables
psql "$DATABASE_URL" -f init_monitoring.sql

# 2. Import Grafana dashboards via the API
GRAFANA_URL=http://your-grafana:3000
for f in grafana/dashboards/*.json; do
    curl -s -X POST "$GRAFANA_URL/api/dashboards/import" \
        -H "Content-Type: application/json" \
        -u admin:admin \
        --data-binary @"$f" | jq '.status'
done
```

---

## Alerts configuration

Edit `grafana/provisioning/alerting/rules.yml` to configure notification
channels. By default, alerts fire to the built-in Grafana notification channel.

Predefined alert rules:

| Rule name | Condition | Default severity |
|-----------|-----------|-----------------|
| `ducklake_small_files_warning` | any table > 50 small files | WARNING |
| `ducklake_small_files_critical` | any table > 200 small files | CRITICAL |
| `ducklake_snapshot_rate_warning` | any table > 60 commits/min | WARNING |
| `ducklake_storage_growth_warning` | projected to exceed `STORAGE_QUOTA_GB` in 7 days | WARNING |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Grafana shows "No data" on all panels | Stream tables not yet populated | Wait 90 s for first refresh; check `docker compose logs grafana` |
| `psql: error: connection refused` | `DATABASE_URL` points to wrong host/port | Verify the URL and that pg_trickle is installed: `SELECT * FROM pg_extension WHERE extname = 'pg_trickle'` |
| `relation "ducklake_data_file" does not exist` | Target database is not a DuckLake catalog | Connect to the correct catalog database |
| Grafana datasource shows red status | `.env` credentials wrong | Re-run `docker compose down` → fix `.env` → `docker compose up -d grafana` |
| Port 3100 already in use | Another service on 3100 | Change `GRAFANA_PORT=3101` in `.env` |
| Stream table `ducklake_tenant_activity` is empty | No multi-schema data | Normal for single-schema catalogs |

---

## Teardown

Remove the stream tables from your catalog:

```bash
psql "$DATABASE_URL" -f teardown_monitoring.sql
```

Stop Grafana:

```bash
docker compose down -v
```

---

## What you've built

- A **zero-overhead monitoring stack** for DuckLake: the stream tables are
  maintained incrementally, so monitoring queries cost only O(Δ) — not a full
  scan of `ducklake_data_file` every minute.
- **Four production-grade dashboards** covering compaction health, ingestion
  rate, storage growth, and tenant activity.
- **Pre-configured alerts** for the most common DuckLake operational issues.

---

## Related resources

- [Blog: Monitoring Your DuckLake Deployment](../../blog/ducklake-monitoring.md)
- [Demo: OLTP-to-Lake Loop](../ducklake-oltp-lake/README.md) (companion sandbox)
- [Tutorial 3: The Modern Data Stack in One Box](../../docs/tutorial-modern-data-stack-one-box.md)


A pre-packaged monitoring stack for DuckLake deployments. Five minutes from
`git clone` to a production-quality Grafana dashboard with real-time alerts
for compaction needs, snapshot rate spikes, and storage growth anomalies.

---

## What It Does

Attaches to an **existing** DuckLake PostgreSQL catalog and creates pg_trickle
stream tables over DuckLake's metadata tables. A Grafana instance reads the
stream tables and displays live operational dashboards.

```
Your DuckLake PostgreSQL catalog
  │
  │  (pg_trickle stream tables watch DuckLake metadata tables)
  ├─ ducklake_small_file_counts   (1m schedule, DIFFERENTIAL)
  ├─ ducklake_snapshot_rate       (30s schedule, DIFFERENTIAL)
  ├─ ducklake_storage_growth      (5m schedule, DIFFERENTIAL)
  ├─ ducklake_tenant_activity     (5m schedule, DIFFERENTIAL)
  └─ ducklake_compaction_events   (5m schedule, DIFFERENTIAL)
  │
Grafana
  │  Dashboards: Overview, Small Files, Snapshot Rate, Storage Growth
  │  Alerts: small_file_count > 50, snapshot_rate > 60/min, storage quota
```

---

## Quick Start

### 1. Configure the Connection

```bash
cd demos/ducklake-observability
cp .env.example .env
$EDITOR .env   # set DATABASE_URL to your DuckLake PostgreSQL instance
```

### 2. Initialise the Stream Tables

```bash
psql "$DATABASE_URL" -f init_monitoring.sql
```

### 3. Start Grafana

```bash
docker compose up -d grafana
```

Open http://localhost:3100 → **Dashboards → DuckLake Observability**.

---

## Connecting to an Existing DuckLake Instance

The `.env` file should point to your existing DuckLake PostgreSQL catalog
database — not a new container. The `init_monitoring.sql` script creates
the stream tables in that database.

```bash
# .env
DATABASE_URL=postgresql://myuser:mypassword@myhost:5432/lake_catalog
GRAFANA_PORT=3100
```

> **Note:** pg_trickle must be installed in the target database:
> `CREATE EXTENSION pg_trickle;`

---

## Stream Tables Installed by `init_monitoring.sql`

| Stream Table | Source | Refresh | Description |
|-------------|--------|---------|-------------|
| `ducklake_small_file_counts` | `ducklake_data_file` | 1 min | Small files (<10 MB) per table |
| `ducklake_snapshot_rate` | `ducklake_snapshot`, `ducklake_table_changes` | 30 sec | Commits per minute per table |
| `ducklake_storage_growth` | `ducklake_data_file` | 5 min | Active bytes per table |
| `ducklake_tenant_activity` | `ducklake_snapshot`, `ducklake_table_changes` | 5 min | Write volume per schema/tenant |
| `ducklake_compaction_events` | `ducklake_data_file` | 5 min | Files compacted per event |

---

## Grafana Dashboards

The dashboard pack includes four pre-built panels:

**Overview Dashboard**
- Total active files across all tables
- Total storage in use (active Parquet files)
- Latest snapshot ID
- Tables with small-file warnings

**Small Files Dashboard**
- Small-file count per table (time series + current value)
- Alert: count > 50 → WARNING, count > 200 → CRITICAL
- Recommended compaction target: tables ordered by small_file_count DESC

**Snapshot Rate Dashboard**
- Commits per minute per table (time series, stacked)
- Alert: any table > 60 commits/minute

**Storage Growth Dashboard**
- Cumulative active bytes per table (time series)
- Storage growth rate (bytes/hour, computed by Grafana)

---

## Alerts Configuration

Edit `grafana/provisioning/alerting/rules.yml` to configure notification
channels. By default, alerts fire to the default Grafana notification channel.

Predefined alert rules:
- `ducklake_small_files_warning`: any table > 50 small files
- `ducklake_small_files_critical`: any table > 200 small files
- `ducklake_snapshot_rate_warning`: any table > 60 commits/minute
- `ducklake_storage_growth_warning`: projected to exceed `STORAGE_QUOTA_GB` in 7 days

---

## Standalone Mode (No Docker)

If you already have a Grafana instance, skip Docker and import the dashboards
manually:

```bash
# Install stream tables only
psql "$DATABASE_URL" -f init_monitoring.sql

# Import Grafana dashboards
for f in grafana/dashboards/*.json; do
    curl -s -X POST "$GRAFANA_URL/api/dashboards/import" \
        -H "Content-Type: application/json" \
        -u admin:admin \
        -d @"$f" | jq '.status'
done
```

---

## Architecture

This demo does not use a separate `postgres` container. It connects to your
existing DuckLake PostgreSQL catalog. Only the `grafana` container is required.

```
Your DuckLake PostgreSQL ← init_monitoring.sql installs stream tables here
      │
      └── Grafana container (reads stream tables via postgres datasource)
```

---

## Teardown

To remove the stream tables from your DuckLake database:

```bash
psql "$DATABASE_URL" -f teardown_monitoring.sql
```

The teardown script drops all stream tables created by `init_monitoring.sql`.

---

## Related Resources

- [Tutorial: Monitoring Your DuckLake with pg_trickle](../../blog/ducklake-monitoring.md)
- [Blog: Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM](../../blog/ducklake-ivm-missing-piece.md)
- [Demo: The Five-Second Funnel](../ducklake-funnel/README.md)
