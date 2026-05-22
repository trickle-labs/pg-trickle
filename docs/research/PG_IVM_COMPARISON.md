# Research: pg_ivm Comparison

This document is a detailed technical comparison between pg_trickle
and [pg_ivm](https://github.com/sraoss/pg_ivm) covering supported
SQL features, refresh latency, and operational differences. It is
research material for contributors and evaluators performing a
deep-dive comparison.

> **Quick comparison table** (pg_trickle vs pg_ivm and other systems) is
> in [Comparisons](../COMPARISONS.md).

---

## Abstract

`pg_ivm` and pg_trickle both implement incremental view maintenance inside PostgreSQL, but they target different maturity levels and operational requirements. `pg_ivm` is a research prototype that proves the IVM concept within PostgreSQL's standard materialized-view infrastructure — it supports a subset of SQL (single-table aggregates, simple joins) and uses statement-level BEFORE triggers with immediate synchronous refresh. pg_trickle is a production extension that targets the full TPC-H benchmark at O(Δ) complexity, supports thousands of concurrent stream tables, and provides an asynchronous scheduled refresh model with CDC-based change capture.

The key architectural divergence is the change-capture layer: `pg_ivm` uses `pg_ivm_immediate_trigger()` with `NEW TABLE`/`OLD TABLE` transition tables and refreshes synchronously within the originating transaction (adding write latency), whereas pg_trickle separates write-path (CDC triggers writing to change buffer tables) from read-path (background scheduler running differential refresh). This separation allows pg_trickle to absorb write bursts, coalesce changes, and maintain stream tables at configurable latencies without blocking the application transaction.

This document provides a feature-matrix comparison across SQL coverage, refresh strategies, operational tooling, performance characteristics, and migration path from `pg_ivm` to pg_trickle. Where `pg_ivm` supports a feature that pg_trickle does not yet support (e.g., some window function variants), the gap is documented with a planned resolution version.

---

{{#include ../../plans/ecosystem/GAP_PG_IVM_COMPARISON.md}}
