# pgtrickle Terminal UI & CLI Guide

`pgtrickle` is the standalone terminal client for monitoring and managing pg_trickle stream tables. It offers an interactive, auto-refreshing terminal dashboard and non-interactive commands for scripts. See also the [CLI overview in the README](../README.md#terminal-ui--cli).

## Connect

Install or build the `pgtrickle` CLI for your environment, and connect to a PostgreSQL database with pg_trickle enabled. To launch the interactive dashboard:

```sh
pgtrickle --url postgres://user:pass@host:5432/mydb
```

Use credentials with the privileges needed for the operations you intend to perform. Avoid putting passwords in shared shell history; consult `pgtrickle --help` for the connection options supported by your version.

## Interactive views

The dashboard refreshes every two seconds. It includes these views:

- **Dashboard** and **Detail**: stream-table status and refresh timing
- **Dependencies**: upstream/downstream relationships
- **Refresh Log** and **Diagnostics**: refresh activity and recommendations
- **CDC Health** and **Delta Inspector**: change capture and deltas
- **Configuration** and **Health Checks**: settings and operational findings
- **Alerts**, **Workers**, **Fuse**, and **Watermarks**: runtime health and safety

Launch the dashboard and use its on-screen key hints for navigation; terminal key bindings may vary by version. The available commands and options are discoverable with `pgtrickle --help`.

## Scriptable commands

```sh
# List stream tables as JSON for automation.
pgtrickle list --format json

# Inspect health; critical findings cause a nonzero exit status.
pgtrickle health

# Refresh a specific stream table.
pgtrickle refresh order_totals

# Inspect a stream table's refresh plan.
pgtrickle explain order_totals --analyze

# Repeated non-interactive monitoring.
pgtrickle watch -n 5
```

The CLI also offers `status`, `create`, `drop`, `alter`, `export`,
`diag`, `cdc`, `graph`, `config`, `workers`, `fuse`, `watermarks`,
and `completions`. See `pgtrickle <command> --help` for arguments and flags.

For one-shot commands, human-readable tables are the default; use
`--format json` or `--format csv` when integrating with scripts.

## Troubleshooting

- **Connection fails:** verify the PostgreSQL URL, server reachability, and user permissions.
- **No stream tables appear:** connect to the database containing the stream tables and verify `CREATE EXTENSION pg_trickle` has been run.
- **Refreshes are stale:** run `pgtrickle health` and inspect the dashboard's Workers, CDC Health, and Diagnostics views.
- **Automation needs stable output:** use a supported structured format rather than parsing interactive dashboard text.

For SQL-based diagnostics and operational procedures, read the [operations cheatsheet](OPS_CHEATSHEET.md) and [troubleshooting guide](TROUBLESHOOTING.md).
