# Files

- [External orchestration and durable consumer contracts](consumer-contracts.md) - Extension-side ownership and validation boundaries for external graph coordinators, durable output-delta consumers, logical publications, and pg_tide outbox publication. Explains transactional visibility, acknowledgement, retention, and resnapshot recovery.
- [dbt materialization lifecycle and transaction safety](dbt-materialization.md) - How the Jinja-only dbt package maps model execution to pg_trickle lifecycle calls, commits mutations, and exposes operational checks. Explains the current decision paths and the verification required when changing macros.
