# Installation Guide

## Choose your installation path

| Environment | Recommended approach |
|---|---|
| **Local development / quick evaluation** | [Docker sandbox](playground/) — `docker compose up -d` in `playground/` gets you a running pg_trickle instance in ~60 seconds with no build step. |
| **Self-hosted Linux server** | [Pre-built release](#installing-from-a-pre-built-release) — download the `.tar.gz`, copy two directories, `CREATE EXTENSION`. |
| **macOS development** | [Build from source](#building-from-source) using `cargo pgrx install`, or use the Docker sandbox above. |
| **Kubernetes / CNPG** | [CloudNativePG](docs/integrations/cloudnativepg.md) — use the `Dockerfile.ghcr` image or the CNPG extension manifest. |
| **Managed PostgreSQL** | Check your provider's extension list. If pg_trickle is not available, the Docker or Kubernetes path is your best option. |

---

## Prerequisites

| Requirement | Version |
|---|---|
| PostgreSQL | 18.x |

> **Building from source** additionally requires Rust 1.85+ (edition 2024) and pgrx 0.18.x.
> Pre-built release artifacts only need a running PostgreSQL 18.x instance.

---

## Installing from a Pre-built Release

### 1. Download the release archive

Download the archive for your platform from the
[GitHub Releases](../../releases) page:

| Platform | Archive |
|---|---|
| Linux x86_64 | `pg_trickle-<ver>-pg18-linux-amd64.tar.gz` |
| macOS Apple Silicon | `pg_trickle-<ver>-pg18-macos-arm64.tar.gz` |
| Windows x64 | `pg_trickle-<ver>-pg18-windows-amd64.zip` |

Optionally verify the checksum against `SHA256SUMS.txt` from the same release:

```bash
sha256sum -c SHA256SUMS.txt
```

### 2. Extract and install

**Linux / macOS:**

```bash
tar xzf pg_trickle-<ver>-pg18-linux-amd64.tar.gz
cd pg_trickle-<ver>-pg18-linux-amd64

sudo cp lib/*.so  "$(pg_config --pkglibdir)/"
sudo cp extension/*.control extension/*.sql "$(pg_config --sharedir)/extension/"
```

**Windows (PowerShell):**

```powershell
Expand-Archive pg_trickle-<ver>-pg18-windows-amd64.zip -DestinationPath .
cd pg_trickle-<ver>-pg18-windows-amd64

Copy-Item lib\*.dll  "$(pg_config --pkglibdir)\"
Copy-Item extension\* "$(pg_config --sharedir)\extension\"
```

### 3. Using with CloudNativePG (Kubernetes)

pg_trickle is distributed as an OCI extension image for use with
[CloudNativePG Image Volume Extensions](https://cloudnative-pg.io/docs/1.28/imagevolume_extensions/).

**Requirements:** Kubernetes 1.33+, CNPG 1.28+, PostgreSQL 18.

```bash
# Pull the extension image
docker pull ghcr.io/trickle-labs/pg_trickle-ext:<ver>
```

See [cnpg/cluster-example.yaml](cnpg/cluster-example.yaml) and
[cnpg/database-example.yaml](cnpg/database-example.yaml) for complete
Cluster and Database deployment examples.

### 4. GHCR Docker image (recommended for local dev)

pg_trickle is published as a ready-to-run Docker image on the GitHub Container
Registry. PostgreSQL 18.3 and pg_trickle are pre-installed and all sensible GUC
defaults (`wal_level`, `shared_preload_libraries`, memory, scheduler settings)
are baked in — no configuration file editing needed.

```bash
docker pull ghcr.io/trickle-labs/pg_trickle:latest

docker run --rm \
  -e POSTGRES_PASSWORD=secret \
  -p 5432:5432 \
  ghcr.io/trickle-labs/pg_trickle:latest
```

`CREATE EXTENSION pg_trickle;` runs automatically on the default `postgres`
database at first startup.

Available tags:

| Tag | Meaning |
|-----|---------|
| `latest` | Most recent release |
| `pg18` | Floating alias for the latest PostgreSQL 18 build |
| `<version>-pg18.3` | Immutable tag, e.g. `0.13.0-pg18.3` |

Override any GUC at runtime without rebuilding:

```bash
docker run --rm \
  -e POSTGRES_PASSWORD=secret \
  -p 5432:5432 \
  ghcr.io/trickle-labs/pg_trickle:latest \
  -c shared_buffers=2GB -c work_mem=64MB -c effective_cache_size=6GB
```

For persistent data, mount a volume:

```bash
docker run -d \
  --name pg_trickle \
  -e POSTGRES_PASSWORD=secret \
  -p 5432:5432 \
  -v pg_trickle_data:/var/lib/postgresql/data \
  ghcr.io/trickle-labs/pg_trickle:latest
```

**Alternative — manual mount from a release archive:**
If you prefer to use the stock `postgres:18.3` image rather than the pre-built
image, extract the extension files from a release archive and mount them:

```bash
tar xzf pg_trickle-<ver>-pg18-linux-amd64.tar.gz
cd pg_trickle-<ver>-pg18-linux-amd64

docker run --rm \
  -v $PWD/lib/pg_trickle.so:/usr/lib/postgresql/18/lib/pg_trickle.so:ro \
  -v $PWD/extension/:/tmp/ext/:ro \
  -e POSTGRES_PASSWORD=postgres \
  postgres:18.3 \
  sh -c 'cp /tmp/ext/* /usr/share/postgresql/18/extension/ && \
         exec postgres -c shared_preload_libraries=pg_trickle'
```

---

## Installing from PGXN

pg_trickle is published on the [PostgreSQL Extension Network (PGXN)](https://pgxn.org/dist/pg_trickle/).
Installing via PGXN compiles the extension from source, so the Rust toolchain and pgrx are required.

### 1. Install prerequisites

```bash
# Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# pgrx build tool
cargo install --locked cargo-pgrx --version 0.18.0
cargo pgrx init --pg18 "$(pg_config --bindir)/pg_config"
```

### 2. Install the pgxn client

```bash
pip install pgxnclient
```

### 3. Install pg_trickle

```bash
pgxn install pg_trickle
```

To install a specific version:

```bash
pgxn install pg_trickle=0.10.0
```

> **Note:** After installation, follow the [PostgreSQL Configuration](#postgresql-configuration) and
> [Extension Installation](#extension-installation) steps below.

---

## Building from Source

### 1. Install Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### 2. Install pgrx

```bash
cargo install --locked cargo-pgrx --version 0.18.0
cargo pgrx init --pg18 $(pg_config --bindir)/pg_config
```

### 3. Build the Extension

```bash
# Development build (faster compilation)
cargo pgrx install --pg-config $(pg_config --bindir)/pg_config

# Release build (optimized, for production)
cargo pgrx install --release --pg-config $(pg_config --bindir)/pg_config

# Package for deployment (creates installable artifacts)
cargo pgrx package --pg-config $(pg_config --bindir)/pg_config
```

## PostgreSQL Configuration

Add the following to `postgresql.conf` **before starting PostgreSQL**:

```ini
# Required — loads the extension shared library at server start
shared_preload_libraries = 'pg_trickle'

# Must accommodate the pg_trickle launcher + one scheduler per database
# with pg_trickle installed + optional parallel refresh workers.
#
# WARNING: when this limit is reached, the launcher silently skips
# databases it cannot spawn a scheduler for and retries every 5 minutes.
# Those databases stop refreshing without any visible error.
# Check PostgreSQL logs for:
#   WARNING:  pg_trickle launcher: could not spawn scheduler for database '...'
#
# Formula:
#   1 (launcher) + N (one scheduler per DB) + max_dynamic_refresh_workers
#   + autovacuum_max_workers + parallel query workers + other extensions
#
# 32 is a safe starting point for most clusters:
max_worker_processes = 32
```

> **Note:** `wal_level = logical` and `max_replication_slots` are **not** required. The extension uses lightweight row-level triggers for CDC, not logical replication.

Restart PostgreSQL after modifying these settings:

```bash
pg_ctl restart -D /path/to/data
# or
systemctl restart postgresql
```

## Extension Installation

Connect to the target database and run:

```sql
CREATE EXTENSION pg_trickle;
```

This creates:

- The `pgtrickle` schema with catalog tables and SQL functions
- The `pgtrickle_changes` schema for change buffer tables
- Event triggers for DDL tracking
- The `pgtrickle.pg_stat_stream_tables` monitoring view

## Verification

After installation, verify everything is working:

```sql
-- Check the extension version
SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_trickle';

-- Or get a full status overview (includes version, scheduler state, stream table count)
SELECT * FROM pgtrickle.pgt_status();
```

### Quick functional test

```sql
CREATE TABLE test_source (id INT PRIMARY KEY, val TEXT);
INSERT INTO test_source VALUES (1, 'hello');

SELECT pgtrickle.create_stream_table(
    'test_st',
    'SELECT id, val FROM test_source',
    '1m',
    'FULL'
);

SELECT * FROM test_st;
-- Should return: 1 | hello

-- Clean up
SELECT pgtrickle.drop_stream_table('test_st');
DROP TABLE test_source;
```

## Upgrading

To upgrade pg_trickle to a newer version without losing data:

> For comprehensive upgrade instructions, version-specific notes,
> troubleshooting, and rollback procedures, see [docs/UPGRADING.md](docs/UPGRADING.md).

### 1. Install the new extension files

Follow the same steps as [Installing from a Pre-built Release](#installing-from-a-pre-built-release)
to overwrite the shared library and SQL files with the new version. You do **not** need to drop
the extension from your databases first.

**Linux / macOS:**

```bash
tar xzf pg_trickle-<new-ver>-pg18-linux-amd64.tar.gz
cd pg_trickle-<new-ver>-pg18-linux-amd64

sudo cp lib/*.so  "$(pg_config --pkglibdir)/"
sudo cp extension/*.control extension/*.sql "$(pg_config --sharedir)/extension/"
```

### 2. Restart PostgreSQL (when required)

If the shared library ABI has changed, restart PostgreSQL before proceeding so the
new `.so`/`.dll` is loaded. The release notes for each version will call this out
explicitly when a restart is required.

```bash
pg_ctl restart -D /path/to/data
# or
systemctl restart postgresql
```

### 3. Apply the schema migration in each database

Connect to every database where pg_trickle is installed and run:

```sql
-- Upgrade to the latest bundled version
ALTER EXTENSION pg_trickle UPDATE;

-- Or upgrade to a specific version
ALTER EXTENSION pg_trickle UPDATE TO '<new-version>';
```

PostgreSQL uses the versioned SQL migration scripts bundled with the release
(e.g. `pg_trickle--0.2.3--0.3.0.sql`, `pg_trickle--0.3.0--0.4.0.sql`) to
apply catalog and SQL-surface changes.
PostgreSQL automatically chains these scripts when you run `ALTER EXTENSION
pg_trickle UPDATE`. The command is a no-op when no migration script is needed
for a given release.

You can confirm the active version afterwards:

```sql
SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle';
```

> **Coming soon:** A future release will include a helper function
> (`pgtrickle.upgrade()`) that automates steps 2–3 across all databases in the
> cluster and validates catalog integrity after the migration. Until then, the
> manual steps above are the supported upgrade path.

---

## Uninstallation

```sql
-- Drop all stream tables first
SELECT pgtrickle.drop_stream_table(pgt_schema || '.' || pgt_name)
FROM pgtrickle.pgt_stream_tables;

-- Drop the extension
DROP EXTENSION pg_trickle CASCADE;
```

Remove `pg_trickle` from `shared_preload_libraries` in `postgresql.conf` and restart PostgreSQL.

## Troubleshooting

### Unit tests crash on macOS 26+ (`symbol not found in flat namespace`)

macOS 26 (Tahoe) changed `dyld` to eagerly resolve all flat-namespace symbols
at binary load time. pgrx extensions reference PostgreSQL server-internal
symbols (e.g. `CacheMemoryContext`, `SPI_connect`) via the
`-Wl,-undefined,dynamic_lookup` linker flag. These symbols are normally
provided by the `postgres` executable when the extension is loaded as a shared
library — but for `cargo test --lib` there is no postgres process, so the test
binary aborts immediately:

```
dyld[66617]: symbol not found in flat namespace '_CacheMemoryContext'
```

**This affects local development only** — integration tests, E2E tests, and the
extension itself running inside PostgreSQL are unaffected.

The fix is built into the `just test-unit` recipe. It automatically:

1. Compiles a tiny C stub library (`scripts/pg_stub.c` → `target/libpg_stub.dylib`)
   that provides NULL/no-op definitions for the ~28 PostgreSQL symbols.
2. Compiles the test binary with `--no-run`.
3. Runs the binary with `DYLD_INSERT_LIBRARIES` pointing to the stub.

The stub is only built on macOS 26+. On Linux or older macOS, `just test-unit`
runs `cargo test --lib` directly with no changes.

> **Note:** The stub symbols are never called — unit tests exercise pure Rust
> logic only. If a test accidentally calls a PostgreSQL function it will crash
> with a NULL dereference (the desired fail-fast behavior).

If you run unit tests without `just` (e.g. directly via `cargo test --lib`),
you can use the wrapper script instead:

```bash
./scripts/run_unit_tests.sh pg18

# With test name filter:
./scripts/run_unit_tests.sh pg18 -- test_parse_basic
```

### Extension fails to load

Ensure `shared_preload_libraries = 'pg_trickle'` is set and PostgreSQL has been **restarted** (not just reloaded). The extension requires shared memory initialization at startup.

### Background worker not starting

Check that `max_worker_processes` is high enough. In sequential mode (default) pg_trickle needs one slot per database with stream tables. With parallel refresh enabled (`pg_trickle.parallel_refresh_mode = 'on'`) it additionally needs `max_dynamic_refresh_workers` slots (default 4) shared across all databases.

See the [worker-budget formula](docs/CONFIGURATION.md#pg_tricklemax_dynamic_refresh_workers) in CONFIGURATION.md for sizing guidance.

### Check logs for details

The extension logs at various levels. Enable debug logging for more detail:

```sql
SET client_min_messages TO debug1;
```

---

## Backup & Restore

### Backing Up a pg_trickle-Enabled Database

pg_trickle uses two schemas that must be included in any backup alongside your
application schema:

- **`pgtrickle`** — stream table catalog, dependency graph, and refresh history
- **`pgtrickle_changes`** — change-buffer tables (one per CDC-enabled source)

Use a full-database dump to include application data and extension configuration:

```bash
pg_dump --format=custom --file=mydb.dump mydb
```

Logical dumps preserve durable configuration, but restored relation OIDs and
capture ownership cannot be reused automatically.

### Restoring

Stop scheduling and application writes before restoring into a fresh database:

```bash
createdb mydb_restored
pg_restore --dbname=mydb_restored --jobs=4 mydb.dump
```

After restore, inspect the capture ownership and recovery state:

```sql
SELECT * FROM pgtrickle.pgt_status();
SELECT pgtrickle.capture_instance_status();
SELECT pgtrickle.validate_recovery();
```

Keep refresh and writes disabled until recovery is complete. A changed database
identity requires explicit superuser adoption with
`pgtrickle.recover_capture_instance()`. `repair_stream_table()` does not remap
source relation OIDs after a logical restore. Follow the
[backup and restore guide](docs/BACKUP_AND_RESTORE.md) to rebuild affected stream
tables and validate the restored database before resuming service.

---

## Next Steps

- [Getting Started](docs/GETTING_STARTED.md) — Create your first stream table in 5 minutes
- [Pre-Deployment Checklist](docs/PRE_DEPLOYMENT.md) — Complete checklist for production deployments
- [Best-Practice Patterns](docs/PATTERNS.md) — Common data modeling patterns
- [Configuration Reference](docs/CONFIGURATION.md) — All GUC variables and tuning
