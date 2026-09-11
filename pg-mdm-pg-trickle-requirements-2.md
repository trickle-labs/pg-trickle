# `pg-mdm` requirements for `pg_trickle` in v0.9 through v0.11

`pg-mdm` v0.8 creates a private Graph V1 graph. v0.9 starts using that graph
for live refresh. v0.10 tests the failure and recovery cases around that
refresh. v0.11 qualifies the complete combination for release.

This document describes what those releases need from `pg_trickle`. It is
based on the `pg-mdm` v0.9 through v0.11 roadmap entries. The repository does
not yet contain separate detailed plans for those releases, so these
requirements need review before implementation starts.

## Terms used here

- A graph member is a `pg_trickle` stream table created from one compiled MDM
  stage.
- A strict refresh is a public operation that checks the expected graph
  contract, refreshes the graph, and returns the exact source boundary used by
  the refresh.
- A source boundary is the complete, identified set of source changes that a
  refresh consumed. Its digest lets `pg-mdm` record exactly what produced an
  MDM publication.
- An external graph is a graph with `orchestration_mode = EXTERNAL`. `pg-mdm`
  starts its refreshes. The normal `pg_trickle` scheduler must not refresh it
  independently.
- A qualifying graph shape is a `pg-mdm` Graph V1 fixture whose compiled queries
  support differential or scoped maintenance after initial population.

## v0.9 requires transactional graph refresh

v0.9 adds `mdm.preview()`, `mdm.refresh()`, and administrative rebuild. The
live path must call the public `pg_trickle` strict-refresh function inside the
same caller transaction as MDM publication.

The existing Graph V1 call has this shape:

```sql
SELECT *
FROM pgtrickle.refresh_graph_strict(
    ARRAY['mdm_graph.member_root'::regclass],
    expected_graph_digest,
    'ALLOW'
);
```

The result must include the following information:

- Graph contract version 1.
- A unique `graph_refresh_id`.
- A machine-readable `source_boundary`.
- A 32-byte `source_boundary_digest`.
- A completeness result that can prove the boundary is complete.

For the qualifying path, `source_boundary->>'completeness'` must be
`PROVEN`. `pg-mdm` stores the refresh ID, boundary, and boundary digest with
the publication observation or publication that follows the refresh.

### Keep the graph and MDM transaction together

The strict-refresh function must not commit independently. The caller owns
the transaction. A successful refresh followed by a successful MDM publication
must commit both changes together.

If any later step fails, PostgreSQL must roll back all of the following:

- changes to graph member rows;
- source-change positions consumed by the graph;
- the `graph_refresh_id` and source-boundary evidence written by `pg-mdm`;
- MDM memberships, golden values, reviews, and publication history; and
- public MDM output-table changes.

An injected failure after graph maintenance must leave the graph and its
source positions in their previous state. A retry must not skip source changes
or apply them twice.

### Check the graph contract before changing data

`refresh_graph_strict()` must compare the supplied graph digest with the
current Graph V1 contract before it changes a member or consumes a source
position. A mismatch must fail without partial graph work.

The operation must also reject a missing member, a changed member or source
owner, a replaced source relation, a revoked source grant, or a changed tracked
contract. The error must identify the failed condition with a stable upstream
code or identifier. `pg-mdm` rejects a changed role binding before it calls the
operation.

### Return a complete source boundary

The source boundary must describe the source state that the refresh used. It
must not claim success when a source change is outside the captured or scanned
range, when a source is unavailable, or when a required source position cannot
be proven.

When source rows change during a refresh, the operation must define which
changes belong to the returned boundary. Changes after that boundary must
remain for a later refresh. The operation must not silently combine an older
graph result with a newer source position.

`pg-mdm` reads the complete terminal evidence relations after a successful
strict refresh. The member and graph contracts must continue to identify those
relations and their dependencies through the public Graph V1 functions.

### Preserve the selected role and row security

Definition-derived SQL must run as the current owner of each graph member, with
row-level security enabled. The `pg-mdm` selected execution role must therefore
own every graph member. A helper role must not bypass that owner's table
privileges or row-level security policies.

The following cases must fail before graph mutation:

- the execution role loses schema `USAGE`;
- the execution role loses source-table `SELECT`;
- the execution role loses source-table `MAINTAIN`;
- the source owner changes;
- the source relation is replaced under the same name; or
- a tracked row-level-security policy or policy dependency changes.

Graph members retain the stricter owner-equivalent requirement. Delegated
source access does not grant permission to operate a graph member.

Graph V1 does not include caller role memberships, role bindings, or
session-dependent row-level-security inputs in its digest. `pg_trickle`
rechecks current database authorization. `pg-mdm` must validate its selected
role binding and session-dependent visibility inputs before it calls strict
refresh.

## v0.9 requires stable graph lifecycle behavior

The v0.8 graph must remain usable by the v0.9 refresh path without private
catalog access. The following public functions and contracts must remain
compatible:

```text
pgtrickle.integration_capabilities()
pgtrickle.create_stream_table(...)
pgtrickle.stream_table_contract(regclass)
pgtrickle.graph_contract(regclass[])
pgtrickle.refresh_graph_strict(regclass[], bytea, text)
pgtrickle.drop_stream_table(text, boolean)
```

Every member must continue to use these settings:

```text
initialize = false
orchestration_mode = EXTERNAL
cdc_mode = trigger
refresh_mode = AUTO
```

The first strict refresh may use a `FULL` refresh to establish the baseline.
After that baseline, qualifying graph shapes must report differential or scoped
maintenance in `node_results`. A whole-query `FULL` refresh remains a correctness
fallback, but it must report a stable action and reason. The qualification
workload must measure repeated `FULL` fallback so `AUTO` cannot hide a graph that
never refreshes incrementally.

`pg_trickle` must not refresh an external graph through ordinary scheduler
work. `pg-mdm` must be the caller that starts the strict refresh.

The existing contract functions must continue to report the member owner,
source identities, dependencies, output schema, contract generation, and
contract digest. `graph_contract()` must continue to report the complete root
closure, topological order, source set, member set, and graph digest.

## v0.10 requires operational reliability

The v0.10 roadmap does not name a new `pg_trickle` SQL feature. It requires
the existing Graph V1 implementation to behave correctly under operational
stress. `pg-mdm` needs the following evidence from the packaged upstream
release.

### Concurrency and lifecycle locking

Concurrent refresh, graph installation, rebuild, drop, and restore operations
must not use stale contracts or cross one another's graph members.

The upstream behavior must prove that:

- a refresh and a drop cannot both commit as if they ran alone;
- a refresh detects a graph change instead of publishing against an old
  digest;
- concurrent source writes have a defined boundary and remain available for a
  later refresh when they fall outside the current boundary; and
- one execution role cannot use another role's temporary graph-creation access
  or graph objects.

### Crash recovery

After a PostgreSQL or backend failure during graph refresh, recovery must leave
the graph at a valid transaction boundary. It must not leave a graph member
partly updated or advance a source position without the corresponding graph
changes.

`pg-mdm` then verifies that the recovered graph can pass contract inspection
and can complete a later strict refresh. A failed refresh must not create a
false MDM publication.

### Clone, backup, restore, and upgrade

The supported package must preserve Graph V1 behavior across the backup,
restore, clone, and extension-upgrade cases that `pg-mdm` supports.

For a clean logical restore, `pg-mdm` rebuilds database-local bindings after
the roles and source relations are restored. The upstream contract must not
require `pg-mdm` to copy private catalog rows or reuse old relation OIDs.

A restored or cloned database must not refresh the original database's graph.
Role bindings, source relation identities, member relation identities, and
contract digests must remain local to the database where the graph runs.

An extension upgrade must preserve existing Graph V1 members and their
contracts, or reject the upgrade path before it can create an inconsistent
graph. The upgrade test must use the packaged artifacts, not a source
checkout.

### Resource limits and fallback

When the graph cannot prove a complete source boundary or cannot complete a
required stage within its configured limits, the strict refresh must fail
closed. It must not publish incomplete terminal evidence or advance a source
position as if the work had completed. The qualification suite must name the
tested lock-timeout, statement-timeout, memory, and graph-size limits.

`pg-mdm` later uses its full resolver as the correctness reference. A
`pg_trickle` whole-query full refresh allowed by `full_policy = 'ALLOW'` is an
exact graph result, not a resource-failure state. Other resource pressure must
abort the caller transaction or return a defined failure state. It must never
produce a partial graph that looks valid.

Errors must remain stable enough for `pg-mdm` to report an actionable result.
The error must distinguish at least contract mismatch, incomplete source
boundary, authorization failure, concurrent lifecycle change, and resource
failure.

## v0.11 requires release qualification

v0.11 qualifies the complete system for a supported release. It does not add a
new upstream runtime feature. The release must provide a fixed, installable
`pg_trickle` package and evidence for the public behavior used by `pg-mdm`.

The qualification must cover:

- Graph V1 capability discovery and contract version checks;
- canonical member and graph contracts;
- durable `EXTERNAL` orchestration;
- strict refresh and complete source boundaries;
- commit and rollback of graph changes and source positions;
- inserts, updates, and deletes in source tables;
- no-op refreshes;
- concurrent source writes and lifecycle operations;
- injected refresh failures;
- full rebuild and fallback behavior;
- crash recovery;
- clone isolation;
- backup, restore, and extension upgrade; and
- authorization, row-level security, and forbidden private-access checks.

The qualification must run against the exact package that `pg-mdm` uses in CI.
The package record must include the upstream version, release commit, tag
object, package URL, PostgreSQL major, and SHA-256. `pg-mdm` records those
values in its `DEPENDENCIES.md` and reruns the admission suite for each
dependency update.

The shared suite must compare generated graph results with the independent
full-resolution reference path. It must cover source inserts, updates,
deletes, stewardship changes, merges, splits, golden-value-only changes,
rollback, concurrency, fallback, rebuild, and upgrade.

## Features that are not required

The v0.9 through v0.11 `pg-mdm` path does not require the following upstream
features:

- `output_delta_consumer` or Delta V1 consumption;
- WAL capture;
- direct access to `pg_trickle` private catalogs, change buffers, or scheduler
  state;
- autonomous refresh of an `EXTERNAL` graph; or
- MDM identity resolution and publication logic inside `pg_trickle`.

The V1 integration uses trigger capture and complete terminal-relation reads
after a strict Graph V1 refresh. `pg_trickle` maintains the changing
relational facts. `pg-mdm` resolves identity and publishes MDM results.

## Release gate

`pg-mdm` can complete v0.9 only when one released and checksummed
`pg_trickle` artifact passes the strict-refresh admission tests. v0.10 cannot
close until its pinned artifact passes the cumulative operational failure and
recovery tests. v0.11 cannot close until its exact pinned package passes the
shared conformance and differential-versus-full qualification suite. If the
pinned artifact changes, `pg-mdm` must rerun every applicable earlier gate.

The current `pg-mdm` baseline is `pg_trickle` `0.104.0`. The baseline supports
the owner-equivalent Graph V1 path. It does not remove the separate v0.8 gate
for delegated source authorization.
