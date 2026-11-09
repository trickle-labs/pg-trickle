# Diverse high-dimensional centroid churn

These ARM64 tests use PostgreSQL 18.6, pg_trickle 0.109.0 and pgvector
0.8.1 or 0.8.7. The installed files match all 157 files in the earlier
candidate `1e291536bf0380b37a509b730155ca3d3a0ccea4`; its linux-arm64
archive SHA-256 is `2cd9d52c1d9d744557e2f16a7faf1cb5c44213fdc72c72beebed37122b34f73b`.
The retained attestations contain the observed file hashes. This is not the
final integrated release candidate.

The deterministic seed `0x7a139465` drives 2,000 individual insert, update,
delete, NULL and group-move refresh calls on 1,536-dimensional vectors.
Components vary across each vector, with large signed values, cancellation,
and near-zero values. Each call compares groups, counts, NULLs, dimensions and
components with native `AVG` using `1e-5 + 1e-6 * abs(native)` tolerance.
This small corpus is a numerical test, not a representative cost benchmark.

The corrected test passed on both versions ([0.8.1](retry-081.log),
[0.8.7](retry-087.log)). Both recorded a maximum absolute component error of
`1.2732925824820995e-11` and maximum error/tolerance of
`1.273291934568068e-6`. Latest completed-history entries showed algebraic
maintenance on 1,439 calls. At call 1,469 an `InvalidState` rejection caused a
group rescan and an unbound private state. The remaining 532 calls checked
native results and that actual refreshes used the unbound fallback without
repeated rebases. These are observations of one seed and do not establish why
the candidate state was rejected, or measure the cost of staying unbound.

## Failures and limitations

- Initial runs on [0.8.1](run-081.log) and [0.8.7](run-087.log) stopped at
  call 1,469 because the first test version incorrectly required the private
  state to remain bound. The [diagnostic](diagnostic-087.log) records the
  `STATE_REJECTED` / `InvalidState` fallback and the unbound marker. Visible
  output matched native AVG before that assertion.
- An intermediate [0.8.7 run](safe-087.log) failed the combined comparison at
  call 755. That version did not log the offending values or distinguish
  group/count/NULL differences from component differences. Later runs did not
  reproduce it; its cause is unresolved. The test now records those details on
  any recurrence. Passing retries do not resolve the original failure.
- The intermediate [0.8.1](safe-081.log) and
  [0.8.7 diagnostic](mismatch-087.log) runs stopped after the fallback because
  the test decoded a NULL fallback reason from a later no-change history row as
  a non-null string. The corrected test permits that only for zero delta rows.

The earlier hosted crash-recovery failure, the unconfirmed comparison failure,
and the rejection cause require further investigation. These runs do not
complete the selector cost matrix, recovery measurements, or the final
candidate qualification.
