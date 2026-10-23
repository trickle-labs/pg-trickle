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
quality program. Installed qualification and independent review are pending.
