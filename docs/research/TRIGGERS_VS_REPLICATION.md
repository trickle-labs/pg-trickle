# Research: Triggers vs. WAL Replication for CDC

This document analyses the architectural tradeoffs between trigger-based
CDC (pg_trickle's default) and WAL logical-replication CDC. It provides
the engineering rationale behind ADR-001 and ADR-002.

> **User-facing CDC documentation** is in [CDC Modes](../CDC_MODES.md).

---

## Abstract

Change-data capture (CDC) is the mechanism by which pg_trickle discovers which rows in a source table changed between refresh cycles. Two fundamentally different approaches exist inside PostgreSQL: **row/statement-level AFTER triggers** that fire synchronously inside the source transaction, and **logical replication** (WAL decoding) that reads the write-ahead log asynchronously after the transaction commits.

pg_trickle's default CDC mode is trigger-based (ADR-001, v0.1.0) for a single decisive reason: trigger-based CDC delivers **transactional atomicity** — the change is captured in the same transaction that made it, under the same locks, with no possibility of a committed write being invisible to the next refresh cycle. WAL-based CDC always has a decoding lag; a crash between transaction commit and WAL decode can lose a change window. For an IVM system where data correctness is non-negotiable, the trigger approach eliminates an entire class of consistency hazards.

The tradeoffs are real: trigger-based CDC adds 5–20% write-side latency on bulk DML (mitigated by statement-level triggers since v0.4.0), requires an additional change buffer table per source, and cannot capture changes made by `pg_dump` or direct file-level manipulation. WAL-based CDC has zero write-side overhead but requires `wal_level = logical`, is affected by slot lag under write storms, and has a non-trivial failure-recovery surface. This document quantifies both approaches with benchmarks, defines the boundary conditions under which WAL-mode is preferable (append-only high-throughput sources with relaxed latency requirements), and explains pg_trickle's hybrid `auto` mode that starts with triggers and promotes to WAL when conditions are right.

---

{{#include ../../plans/sql/REPORT_TRIGGERS_VS_REPLICATION.md}}
