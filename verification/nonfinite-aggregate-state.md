# Non-finite SUM/AVG input state

This runtime repair follows the remaining `special_sum` failure in #1217.
It adds no formal arithmetic claim to the bounded #1220 lifecycle proof.

For exact built-in numeric, real and double precision effective arguments, retain one
private JSONB state per non-DISTINCT SUM/AVG: a numeric subtotal of finite
inputs and separate counts of NaN, positive infinity and negative infinity.
Initialize it from the same query and FILTER as the public aggregate. Update
each component with the selected child delta's inserts minus deletes.
Excluded FILTER arguments are guarded before evaluation. Direct-scan SUM and
the ordinary aggregate producer use the same state renderer.

A positive NaN count, or positive counts of both infinities, yields NaN.
Otherwise a positive infinity count yields that infinity; without any special
inputs use the finite subtotal. The public qualifying-argument count still
determines NULL for empty/all-NULL SUM and AVG. AVG retains its ordinary count
and repairs its stored sum as well as its displayed value when special inputs
are involved. A state change must be written even when the public value stays
Infinity or NaN. Existing grouped deletion, global singleton and HAVING
initialization rules remain responsible for row visibility.

Recovery after special-input removal consumes staged deltas, without a new
live-source rescan, source lock, or FULL fallback. A writer beyond the selected
cut belongs to the next refresh. A HAVING group with no stored row continues
to use the existing affected-group initialization path and seeds this state
alongside the other accumulators. The existing 0.108.2-to-0.108.3 protected
rebuild creates and populates the additional private column for old tables.

Known integer-column SUM/AVG queries keep their existing storage and producer.
Unresolved expressions use SQL type checks; the new special-input arithmetic
admits only the three exact OIDs of the effective action/FILTER-wrapped CASE
argument. CASE can coerce a domain expression to its base numeric type, which
then participates in this route. Initialization uses the same CASE coercion;
raw-domain initialization and action-guarded updates must produce the same state.
A known bare Scan domain column keeps its
existing route. This is not general domain or custom-type support. DISTINCT and intermediate
aggregate algorithms retain their existing routes. Arbitrary floating-point
regrouping, bit-level rounding equivalence and finite overflow are not proved
by this state representation or its small-value runtime examples.

Typed restoration uses the target row type and preserves grouping fields,
including NOT NULL domain keys for newly inserted groups. HAVING's unprojected
COUNT(*) uses the maintained total-row count, rather than an aggregate call in
the generated WHERE clause or a projected filtered count.

Required executable observations are the generated SQL test
`test_nonfinite_aggregate_state_uses_frozen_delta_and_restores_avg_auxiliary`,
the installed histories
`test_agg_nonfinite_sum_avg_return_to_finite_after_deletes`, and the installed
barrier schedule
`test_refresh_nonfinite_aggregate_keeps_later_writer_for_next_cut`.
The 34-case PR floor and 54-case scheduled floor remain required. Passing these
checks qualifies these histories, not all numeric arithmetic or the whole
quality program.

Frozen executable candidate `1a717660030487b1d5e63f683c86e40f528b8fe6`
passes those three named runtime observations. The state-only history now
updates a finite input from 1 to 4 while Infinity remains, removes Infinity
immediately, and requires PostgreSQL's finite result 4. The generated SQL
also compares raw-domain initialization with guarded delta admission.
Both observed floors pass: 34/34 PR cases and 54/54 scheduled cases, with no
unsupported or FULL substitution. The connected installed selection passes
47/47 (71 unrelated cases filtered), with zero retries. The existing LSN suite
also passes 11/11, including all four required installed cases. All 52 installation
observations declare that candidate and agree with the extracted package on
library SHA-256
`32b7d5f1cd53c5115581ddf68da3e043f1a1acb7252d7bf24db2312c02731259`.
The full unit suite passes 2633/2633; all 26 native aggregate tests pass.
One native suite invocation marked a passing pure lifecycle test leaky; its
isolated observation passed without that marker. Neither record was discarded.

Independent read-only reviewer `review_1220` accepted the source/specification
follow-up after identifying and resolving domain admission asymmetry and a
non-discriminating inverse-update history. This is a reviewed runtime repair,
not an additional Verus arithmetic proof. Existing production proof baselines
and executable mutation controls were regenerated, and all six corresponding
source/package/installation bindings pass on this package. Their original
proof boundaries and trusted build/test-runner assumptions remain intact.
