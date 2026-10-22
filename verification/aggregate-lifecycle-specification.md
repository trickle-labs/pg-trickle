# Predicate visibility and aggregate lifecycle

This bounded #1220 slice covers production decisions for HAVING admission and
retraction, aggregate group actions, and nullable output at an empty boundary.
It does not prove aggregate arithmetic, arbitrary SQL expressions, capture,
transactions, identity encoding, or the whole extension.

## Independent semantics

A result row is visible only when it exists and its predicate is TRUE.
FALSE and UNKNOWN both exclude it. This matches PostgreSQL's
[Boolean comparison predicates](https://www.postgresql.org/docs/18/functions-comparison.html)
and [HAVING semantics](https://www.postgresql.org/docs/18/queries-table-expressions.html#QUERIES-GROUP).
`Option<bool>` represents exactly those three values; `None` is UNKNOWN.

For one old/new logical key, retract the old visible row when the new row is
invisible or its public payload changed. Insert the new visible row when the
old row was invisible or its payload changed. A changed visible payload is a
bag deletion plus insertion, implemented by the existing identity-matched
upsert at the SQL boundary. An unchanged TRUE-to-TRUE row needs neither event.

For changed payloads and existing endpoint groups, the required actions are:

| Old predicate | New TRUE | New FALSE | New UNKNOWN |
| --- | --- | --- | --- |
| TRUE | retract + insert | retract | retract |
| FALSE | insert | none | none |
| UNKNOWN | insert | none | none |

An absent old group cannot be retracted, even if a supplied old predicate is
TRUE. An absent new group cannot be inserted. These rules apply independently
of payload equality and cover initialization and complete group removal.

A grouped result exists only while its row count is positive. An ungrouped
aggregate retains its singleton result on empty input, before applying HAVING.
COUNT(*) counts rows; COUNT(argument) counts non-NULL arguments; nullable
aggregates such as SUM and AVG return NULL when no admitted arguments remain.
These are separate facts: a nonempty all-NULL group still exists. PostgreSQL's
[aggregate reference](https://www.postgresql.org/docs/18/functions-aggregate.html)
defines these observations.

| Global aggregate | Old result present | New input has rows | Group action |
| --- | --- | --- | --- |
| no | no | no | skip |
| no | no | yes | insert |
| no | yes | no | delete |
| no | yes | yes | update |
| yes | no | either | insert |
| yes | yes | either | update |

## Checked executable and actual callers

`verification/aggregate_lifecycle.rs` is imported directly by `src/lib.rs`.
The same executable functions are checked by the pinned Verus compiler:

- `visibility_actions`: exact old/new bag actions for all predicate,
  existence and changed-payload inputs.
- `group_present`: global singleton preservation versus grouped row presence.
- `group_action`: insert/update/delete/skip classification, using that presence.
- `aggregate_is_null`: nullable output depends on argument presence, not group
  presence or the aggregate's numeric value.

The functions require no unchecked preconditions. Their inputs are typed
Booleans and `Option<bool>`; raw SQL text, PostgreSQL datums and arithmetic
counts never enter the verified kernel.

The ordinary Rust adapters in the same file lower finite decisions into SQL:

- `diff_filter` calls both `having_admission_sql` and
  `having_retraction_sql`. Correct plans lower to IS TRUE / IS NOT TRUE.
  Existing child-action and complete-identity guards remain required.
- Intermediate aggregate emission uses `group_presence_sql` for its old/new
  presence filters. Top-level `diff_aggregate` uses `group_action_sql` against
  the actual new-count expression, including a full rescan for a group omitted
  by HAVING. SQL's existing final action filter excludes skip actions.
- Global empty-output handling and SUM's non-NULL argument guard use
  `aggregate_value_sql`. Their count expressions remain distinct.

The renderer obtains grouping shape and aggregate family from the admitted
`OpTree`. Count predicates use generated COUNT values or COALESCE-guarded
arithmetic expressions. SQL type checking and expression evaluation remain
PostgreSQL boundaries. The kernel does not assume those arithmetic expressions
are correct merely because their presence classes are Boolean.

A previously invisible HAVING group has no stored row or accumulators.
`initialize_having_group` seeds SUM/AVG state from the affected-group rescan
before ordinary algebraic maintenance. That repair, count arithmetic, AVG
division and auxiliary-column mapping are runtime-qualified, not Verus proofs.
Composite NULL semantics and row-dependent argument exceptions remain outside
this slice, with their existing repair/test ownership unchanged.

## Specification review and conformance

The specification is checked against an independent PostgreSQL oracle, not
against expected SQL strings copied from the generator:

- `test_lifecycle_visibility_decisions_match_postgresql_bag_differences`
  evaluates old/new public bags with symmetric EXCEPT ALL for 72 finite states:
  all nine truth transitions, both endpoint-presence flags and changed versus
  unchanged payloads. It separately evaluates both lowered predicate tests and
  the old NOT control, which disagrees specifically on UNKNOWN.
- `test_lifecycle_group_and_null_plans_match_postgresql_small_bags` enumerates
  every unordered bag of size zero through two over NULL, -1, 0 and 1. Thirty
  grouped/global cases compare the lowered plans to PostgreSQL's own COUNT and
  SUM results, including a zero sum with present arguments. Eight action-table
  cases check the SQL classification separately.
- `test_lifecycle_having_sql_uses_total_truth_tests_and_identity_guards`
  checks the actual generated filter and retains its identity/action guards.
- Installed-extension tests reach all nine observed PostgreSQL transitions for
  SUM, AVG, AND and OR predicates, then check deletion, reinsertion, key
  movement and changed visible payloads. A second history covers grouped and
  global COUNT/SUM/AVG, empty/all-NULL input and scalar HAVING. Every changed
  cycle witnesses source capture and a fresh, completed DIFFERENTIAL record
  without full fallback; zero public delta is valid for invisible-to-invisible
  transitions. The exact schema/bag oracle remains primary.

This source review and independent-oracle evidence are separate from an
independent reviewer sign-off. No human or second-agent review is claimed.

## Proof controls, candidate evidence and limits

`python3 scripts/check_aggregate_lifecycle_verification.py` reuses the existing
LSN runner, pinned Verus/Z3 identities and evidence layout. A correct baseline
must pass. Five edits to executable production decisions must each fail their
postcondition: UNKNOWN not retracted, missing retraction, retained empty
group, missing global empty result, and non-NULL empty SUM. A compilation error,
missing summary or unrelated assertion is not a successful semantic control.

The existing candidate-binding helper's `--proof-kind aggregate_lifecycle`
mode requires all obligations and controls, caller/build-input hashes, both
named installed-extension cases, and matching package/installed library bytes.
CI runs the proof in the existing package prerequisite job and qualifies the
installed candidate in the existing binding job. Component execution cases
remain in the existing PR DVM target; the installed cases remain in the
allowlisted HAVING target. No second evidence system or new test tier is added.

Evidence is written to
`.p2p/work/issue-1220-aggregate-lifecycle/evidence/` and uploaded by CI. A green
proof does not establish SQL lowering, parser support, nonthrowing expressions,
numeric overflow/special values, collation, scalar representation, PostgreSQL
transactions or FFI. Those boundaries remain explicit even when all finite
conformance and installed-candidate checks pass.
