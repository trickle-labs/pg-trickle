# Feedback on the `pg_trickle` requirements for `pg-mdm`

## Overall assessment

The [revised requirements](pg-mdm-pg-trickle-requirements-2.md) are ready to plan
against. They use the right integration boundary: `pg_trickle` maintains a
private Graph V1 graph, and `pg-mdm` owns identity resolution, review, and
publication. The live path stays inside one PostgreSQL transaction and uses
only public SQL contracts.

Do not add Delta V1 consumption, WAL capture, private-catalog access, or MDM
logic to this integration. None of those features is needed for `pg-mdm` v0.9
through v0.11.

The work fits the existing `pg_trickle` v0.105 qualification series. It does not
need a new Graph contract or a new release milestone.

## Dependency version

`pg_trickle` 0.104.0 supports the owner-equivalent Graph V1 path. It does not
support delegated base-table sources under schema `USAGE` and table `SELECT`
plus `MAINTAIN`.

Treat a released `pg_trickle` 0.105.1 package as the minimum dependency for the
delegated-source path. That release is planned to add the authorization fix and
the runtime conformance required by `pg-mdm`. `pg-mdm` v0.9 does not need to wait
for the broader package and field qualification in `pg_trickle` 0.105.2 after a
0.105.1 artifact passes the `pg-mdm` admission suite.

Pin one exact package in CI. Record its upstream version, release commit, tag
object, package URL, PostgreSQL major, and SHA-256. If the package changes, rerun
all applicable earlier admission gates. Do not assume that a later patch is
equivalent because it keeps the same Graph V1 major version.

## Execution identity and row-level security

`pg_trickle` runs definition-derived SQL as the current owner of each graph
member. It does not provide an arbitrary execution-role switch. The role that
`pg-mdm` selects for a graph must therefore own every graph member.

`pg_trickle` rechecks current source privileges and tracks result-affecting
Graph V1 contract data. `pg-mdm` remains responsible for its selected-role
binding and session-dependent row-level-security inputs. Keep role memberships,
role bindings, and session claims out of the Graph V1 digest.

The `pg-mdm` admission suite must cover both sides of this boundary:

- Revoke schema `USAGE`, source `SELECT`, or source `MAINTAIN` and require
  strict refresh to fail before committed graph mutation.
- Change the source owner, replace the source relation, or change a tracked RLS
  policy dependency and require a contract rejection.
- Change the `pg-mdm` role binding or a session-dependent visibility input and
  require `pg-mdm` to reject the refresh before it calls `pg_trickle`.
- Verify that delegated source access never grants authority over a graph
  member.

## Source-boundary proof

Do not accept `source_boundary->>'completeness' = 'PROVEN'` as sufficient proof.
The suite must interleave committed source writers with strict refresh and show
that each change is on exactly one side of the returned boundary. Changes after
the boundary must remain pending for the next refresh.

Test an injected failure after graph maintenance but before MDM publication.
After rollback, graph rows, source positions, MDM state, publication history,
and public output rows must all retain their previous committed state. A retry
must neither skip nor duplicate a source change.

The suite must also validate the returned contract version, graph refresh ID,
complete source manifest, and 32-byte boundary digest. Compare the terminal
relations with the independent full-resolution MDM path for inserts, updates,
deletes, and no-op refreshes.

## Differential behavior

The initial strict refresh may use `FULL` to establish a frontier. After that
baseline, every qualifying graph shape must report differential or scoped
maintenance in `node_results`. A qualifying graph shape is one whose compiled
queries support that maintenance.

Keep `full_policy = 'ALLOW'` because an exact `FULL` refresh is the safe fallback
when differential proof is unavailable. Still, fail qualification if supported
steady-state fixtures repeatedly use `FULL`. Otherwise `AUTO` could satisfy the
correctness tests while providing none of the expected incremental-refresh
performance.

## Release mapping

Use the following split:

- `pg-mdm` v0.9 depends on the `pg_trickle` 0.105.1 Graph V1 admission profile.
  Cover public capability discovery, graph contracts, transaction commit and
  rollback, source boundaries, delegated authorization, RLS, scheduler
  exclusion, exact results, and effective refresh strategy.
- `pg-mdm` v0.10 runs cumulative operational tests against its pinned package.
  Cover concurrent writers, lifecycle races, backend and PostgreSQL failure,
  retry, resource limits, backup, restore, clone isolation, and extension
  upgrade.
- `pg-mdm` v0.11 owns the combined release qualification. Run the shared
  differential-versus-full MDM suite against the exact package used in CI. This
  milestone should not require a new `pg_trickle` runtime feature.

The matching upstream work is recorded in the [`pg_trickle` v0.105.1
roadmap](roadmap/v0.105.1.md) and [`pg_trickle` v0.105.2
roadmap](roadmap/v0.105.2.md).

## Ownership of the shared suite

Keep the test ownership explicit. `pg_trickle` owns Graph V1 locking, boundary,
authorization, transaction, recovery, clone, upgrade, stable-error, and package
evidence. `pg-mdm` owns stewardship, merge, split, golden-value, review, and
publication semantics.

The shared suite joins those two sets of evidence. It must use public
`pg_trickle` SQL only and must never inspect private catalogs, change buffers,
or scheduler state.
