# pg_trickle roadmap

This page tracks current release work and longer-term direction. See
[CHANGELOG.md](CHANGELOG.md) for shipped releases and the linked release plans
for details.

## Current work

pg_trickle keeps PostgreSQL query results fresh through incremental view
maintenance. v0.105.3 fixed the known DVM issue and made qualification a
release requirement.

| Version | Focus | Status |
|---------|-------|--------|
| [v0.106.0](roadmap/v0.106.0.md) | Verify package support, pending-data upgrades, platform tiers, and database resource budgets with executed evidence. | In progress |
| [v0.107.0](roadmap/v0.107.0.md) | Back support claims and operator procedures with runtime tests. | Planned |
| [v0.108.0](roadmap/v0.108.0.md) | Make at most two internal changes for reproduced defects or measured performance costs. Skip this release if the evidence does not justify a change. | Conditional |

## Release constraints

- Preserve committed changes and transaction durability.
- Use differential refresh wherever supported. Keep whole-query `FULL` as a visible fallback of last resort.
- Add no SQL families, public contract versions, capture backends, integrations, or autonomous controller actions in this phase.

## v1.0 status

There is no v1.0 target date. Qualification, the 72-hour mixed-workload soak,
the longevity environment, and the release-candidate series remain deferred.
The [v1.0 plan](roadmap/v1.0.0.md-full.md) defines the stability contract,
including correctness, upgrade safety, stable public interfaces, and qualified
packages.

## Beyond v1.0

These plans are not v1.0 requirements. See each release plan for scope.

| Version | Direction | Status |
|---------|-----------|--------|
| [v1.1.0](roadmap/v1.1.0.md-full.md) | PostgreSQL 17 support, `SEARCH` and `CYCLE`, and an `auto_explain` hook. | Planned |
| [v1.2.0](roadmap/v1.2.0.md-full.md) | PGlite proof of concept and pg_partman integration. | Planned |
| [v1.3.0](roadmap/v1.3.0.md-full.md) | Extract `pg_trickle_core`. | Planned |
| [v1.4.0](roadmap/v1.4.0.md-full.md) | PGlite WASM extension. | Planned |
| [v1.5.0](roadmap/v1.5.0.md-full.md) | Reactive PGlite integration. | Planned |
| [v1.6.0](roadmap/v1.6.0.md-full.md) | Opt-in query acceleration using sufficiently fresh stream tables. | Planned |
| [v1.7.0](roadmap/v1.7.0.md-full.md) | Optional external workers and CDC consumers. | Optional |
| [v1.8.0](roadmap/v1.8.0.md-full.md) | Optional distributed computation and Kubernetes operator. | Optional |
| [v1.9.0](roadmap/v1.9.0.md-full.md) | Speculative federation and object-storage state, only if users ask for it. | Conditional |
