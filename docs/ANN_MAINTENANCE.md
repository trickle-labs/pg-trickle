# External ANN maintenance

The supervisor runs the existing one-request worker on eligible requests. It
does not create requests or infer recall from churn. A deployment needs Python
3, `psql`, the two scripts in the release archive and a direct PostgreSQL
connection. Each supervisor runs one request at a time; the worker's four
session advisory-lock slots limit cooperating reference workers per database.
Custom clients can bypass that limit.

## Roles and connection

Keep the stream owner, application reader and maintenance login distinct. The
owner controls the source and stream; the reader should receive only the
application's intended SELECT and RLS access. For each stream and partition
leaf the maintenance login will handle, an administrator can grant:

```sql
CREATE ROLE ann_maintainer LOGIN;
GRANT USAGE ON SCHEMA pgtrickle, public TO ann_maintainer;
GRANT SELECT, MAINTAIN ON public.search_corpus TO ann_maintainer;
GRANT EXECUTE ON FUNCTION pgtrickle.list_ann_maintenance_requests(bigint, integer),
  pgtrickle.ann_maintenance_request(bigint), pgtrickle.claim_ann_maintenance(bigint),
  pgtrickle.finish_ann_maintenance(bigint, boolean, text) TO ann_maintainer;
```

Grant SELECT and MAINTAIN on each storage leaf and USAGE on its schema where
partitioning is used. MAINTAIN permits PostgreSQL maintenance operations beyond
this worker; grant it only to a trusted role. The role does not need SELECT on
the private request tables or superuser privileges. A separate authorized
operator must create requests using `request_ann_maintenance` or the threshold
script. Grant `cancel_ann_maintenance` only where cancellation is required.
Do not give the reader the maintenance grants. Verify application RLS policies
separately; maintenance permissions do not establish reader isolation.

Configure a libpq service for the login and use a protected password file or
another supported credential provider, with `sslmode=verify-full` and the
appropriate CA for remote connections. Do not put passwords in command-line
arguments or logs. Use a direct connection or session pooling, not transaction
pooling: the claim and top-level `REINDEX INDEX CONCURRENTLY` must use the same
backend. Index renames, drops and definition changes must be coordinated with
the worker; its identity recheck and DDL are separate commands.

## Run and supervise

```sh
PGSERVICE=ann_maintenance python3 scripts/ann_maintenance_supervisor.py
```

For example, extract the Linux release archive at `/opt/pg-trickle`:

```sh
mkdir -p /opt/pg-trickle
tar -xzf "$PG_TRICKLE_RELEASE_ARCHIVE" --strip-components=1 -C /opt/pg-trickle
```

Run as a dedicated OS user and configure a systemd service:

```ini
[Service]
User=pgtrickle-ann
WorkingDirectory=/opt/pg-trickle
Environment=PGSERVICE=ann_maintenance
ExecStart=/usr/bin/python3 /opt/pg-trickle/scripts/ann_maintenance_supervisor.py
Restart=always
RestartSec=10
KillMode=control-group
TimeoutStopSec=70
```

Start or stop the service with the host's service manager. On a graceful stop,
the supervisor interrupts active `psql` so it requests server-side query
cancellation; a canceled request can be recorded as failed. Abrupt process or
connection loss can instead leave an interrupted request, which is reclaimed
only after its reservation expires. An interrupted concurrent reindex can
leave an invalid intermediate index. Inspect active builds and index identity before
removing remnants, as described in the [SQL reference](SQL_REFERENCE.md#pgtricklefinish_ann_maintenance).
Use a service manager that cleans up the whole service process group; restarting
only a killed supervisor can leave its `psql` child running. Loss of a `psql`
process does not guarantee immediate cancellation of an active server command.
Choose a statement timeout appropriate to the maximum acceptable rebuild time
and inspect the backend before retrying or removing intermediate indexes.
After upgrade or logical restore, recreate requests: the queue is not dumped.

The supervisor polls every 30 seconds by default. `PGS_ANN_POLL_SECONDS`
(1–60), `PGS_ANN_PAGE_SIZE` (1–1,000), `PGS_ANN_MAX_PAGES_PER_POLL` (1–100),
and `PGS_ANN_MAX_FAILURES` (1–6) bound work. The cursor continues on the next
poll when a page budget is exhausted and wraps after the final page. The worker
uses `PGS_ANN_LOCK_TIMEOUT` (default `5s`) and `PGS_ANN_STATEMENT_TIMEOUT`
(default `30min`). Failed and interrupted requests obey the extension's
persisted per-index backoff. Once the failure limit is reached, investigate
and explicitly recover the request rather than clearing catalog state blindly.
Poll or connection failures are logged and retried after the poll interval.

## Inspect and alert

The following query inspects one page; paginate by passing the largest returned
ID as the next cursor until the page is empty. A single page is not a complete
queue-depth measurement when more than 1,000 requests are visible.

```sql
SELECT r->>'request_id' AS request_id, r->>'state' AS state,
       r->>'requested_at' AS requested_at, r->>'next_eligible_at' AS retry_at,
       r->>'consecutive_failures' AS failures, r->>'last_error' AS last_error,
       r->>'identity_current' AS identity_current,
       r->>'reported_covered_at' AS last_reported_success,
       r->>'epoch_advances_since_reported_coverage' AS uncovered_epoch_advances,
       r->>'known_operations_since_reported_coverage' AS known_operations
FROM pgtrickle.list_ann_maintenance_requests(0, 1000) AS r;
```

Alert on old pending requests, repeated failures, interrupted claims, stale
identities and a stopped supervisor using thresholds chosen for the workload.
Unknown operation counts or coverage are unknown, not zero. The reported
coverage is the worker's successful claim frontier, not a recall measurement.
Track refresh lag and application embedding-generation freshness separately.
ANALYZE updates planner statistics; VACUUM reclaims tuples; neither is a
substitute for a measured reason to rebuild an ANN index. Leaving maintenance
manual is appropriate when a rebuild's benefit and operational cost are not
established.
