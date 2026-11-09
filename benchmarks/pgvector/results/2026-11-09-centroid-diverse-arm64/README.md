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
repeated rebases. These are observations of one seed and do not measure the
cost of staying unbound.

## Zero-count residue investigation and fix

A read-only [snapshot](pre-rejection-state.json) from the matching
[0.8.7 candidate run](repeat-a-attestation.json) was taken before call 1,469.
The [reconstruction](analyze-state.py) applies the deterministic row move to
the saved private accumulators; its [output](reconstruction.jsonl) shows the
old group reaching zero non-NULL inputs with 137 nonzero component residues,
maximum `1.2925e-11`. The largest residue relative to the existing conservative
float64 accumulation bound was `4.94e-5`. The old zero-count guard required
exact zeros, explaining this candidate rejection. Two additional earlier-package
repetitions ([A](repeat-a.log), [B](repeat-b.log)) passed the comparison but
both took the same safe fallback.

The guard now permits a zero-count residual only when every component is
within that accumulation bound; a non-finite bound or larger residual remains
rejected. The accumulated sums, magnitudes and operation count remain in state,
so a later non-NULL input must still pass the regular error guard. Focused unit
tests cover valid residue, invalid state and a cancellation case that becomes
unsafe after a new input. The [full unit suite](fix-unit-host.log) passed 2,709
tests. An initial [sandboxed unit run](fix-unit.log) could not bind local
metrics-server sockets; the host run supplied that access.

On locally rebuilt ARM64 images using the matching generated SQL and library,
the fixed workload passed [0.8.1](fix-081.log) and [0.8.7](fix-087.log), with
zero unbound refresh calls and 1,962 latest-history algebraic observations each.
The maximum absolute error was `1.2960299500264227e-11` and the largest
error/tolerance fraction was `1.29602948093257e-6`. The library SHA-256 was
`ed579033c8353373f0d252fdbf52b5c8bc115cfaa79f8bdfdbedbb47de2cb40d`; generated
installation SQL SHA-256 was
`98afaf4c45d024cc23ec9bbbb9e55d7b71daab9c0bed66f2dbf2840c29c4dcf5`.
Image IDs were `sha256:ddb76e18e90344c2d871d644900531a080d5ee64abe539a6f72b4adec3077517`
and `sha256:4df098fed85a29775cb8d4e5b40c01e09ca600b866ccd57cb15338428ebfedd5`,
respectively. The 160-call churn and unsafe-rebase fallback tests also passed
on [0.8.1](fix-short-081.log) ([fallback](fix-unsafe-081.log)) and
[0.8.7](fix-short-087.log) ([fallback](fix-unsafe-087.log)). These are local
development images, not final release artifacts.
The first image assembly mixed SQL from a different candidate and failed at
extension creation ([0.8.1](assembly-error-081.log), [0.8.7](assembly-error-087.log));
the passing runs used the matching pair.

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

The earlier hosted crash-recovery failure and the unconfirmed comparison failure
require further investigation. These runs do not
complete the selector cost matrix, recovery measurements, or the final
candidate qualification.
