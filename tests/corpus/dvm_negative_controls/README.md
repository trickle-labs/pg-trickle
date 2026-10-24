# DVM negative controls

These scenarios are deliberately wrong: each one claims a capability the
implementation doesn't actually have, or asserts an expected result that is
guaranteed not to hold. Replaying them (via `dvm_replay.py` or the Rust
`dvm_fuzz::replay`) must always FAIL. They exist to prove that the
correctness harness's detectors — exact-mismatch and silent-fallback checks —
are actually still working, not just green because nothing is being checked.

**Each control must fail with its declared class, phase, cycle and mutation.
Setup, comparator and infrastructure failures cannot count as semantic detection.**
The uncorrupted COR-939 baseline must pass before the Rust controls run.

| Scenario | Guaranteed failure | Why |
| --- | --- | --- |
| neg_silent_fallback.json | `SilentFallback:` | Schema, defining query, and mutations are all legitimately correct (copied from `cor939_two_leaf_snapshot`). `expected_capability.expected_mode` is deliberately set to `"BATCH"` while the query actually runs in `DIFFERENTIAL` mode, so `mode_check` (Python) / `assert_effective_refresh_mode` (Rust) is guaranteed to detect the mismatch. |
| neg_multiset_mismatch.json | `MultisetMismatch:` | Schema, defining query, and the first mutation cycle are copied verbatim from `cor939_two_leaf_snapshot`. A second cycle issues a raw `UPDATE` directly against the stream table's own backing relation, bypassing the tracked source tables. Because DIFFERENTIAL refresh reconciles only deltas observed on tracked sources, it never undoes this out-of-band corruption, so the stream table permanently diverges from a fresh evaluation of the defining query. Both `exact_check` (Python) and `compare_st_to_query` (Rust) are guaranteed to report a mismatch. |
| negctrl_injected_939.json | `GeneratorInvalid:` | A COR-18 negative control with padding cycles/rows around the real mutation set. The `UPDATE ... WHERE id = 1` mutation in `simultaneous-two-leaf-change` declares `expected_affected_rows: 2` for a single-row update, so replay must fail while checking the mutation's affected-row count, before it ever reaches the exact/mode oracles. |

The multiset control sets the existing internal-refresh flag transaction-locally
in a materialized CTE before corrupting the backing row. This deliberate test
injection must pass the DML guard and reach the exact comparator; a guard error
is a failed control. The other controls leave the flag untouched.

Run all controls with `just dvm-negative-controls`, or replay one directly with:

    just dvm-replay tests/corpus/dvm_negative_controls/neg_silent_fallback.json

For an exit status that succeeds only on the declared failure, use
`python3 scripts/dvm_replay.py <scenario> --database <url> --expect-negative-control`.
Python and Rust artifacts retain the actual failure coordinates for replay.

A `--validate-only` / structural run never touches a live database and is what
`scripts/dvm_release_gate.py` uses to confirm this corpus hasn't been deleted
or corrupted. Actually proving detection still works requires a live
database, which is what `cargo nextest run --test e2e_dvm_negative_control_tests`
does in CI.
