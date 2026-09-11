# Graph V1 and Delta V1 extension contracts

v0.104.0 makes the Graph V1 and Delta V1 contracts stable. Integration code
discovers them with `pgtrickle.integration_capabilities()` and uses only the
documented SQL functions and relations returned by those functions.

Graph coordinators should read `graph_contract()` and pass its digest to
`refresh_graph_strict()` in the same transaction as their publication work.
The refresh does not commit, so a rollback removes both the materialized
changes and the coordinator's publication.

The coordinator needs owner-equivalent authority over every graph member. A
base-table owner may instead delegate source coordination with schema `USAGE`
and table `SELECT` plus `MAINTAIN`; revoking either table grant makes contract
and strict-refresh calls fail closed.

Delta consumers register with `register_output_delta_consumer()`, read batch
metadata with `output_delta_batches()`, read rows from its returned
`delta_relation`, and acknowledge with `ack_output_delta()`. A consumer uses
the resnapshot functions when an invalidation or contract change requires a
new baseline.

These contracts intentionally exclude private catalog and change-buffer
access, remote delivery, and a stable Rust ABI. `pg_tide`, dbt, and other
extensions may build adapters on the public SQL boundary without depending on
those private implementation details.
