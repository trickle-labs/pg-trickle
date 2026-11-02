# cargo-mutants pilot (#1290)

**Date:** 2026-10-29
**Tool:** cargo-mutants 27.1.0
**Scope:** 17 mutants in two pure helpers; no CI or dependency changes.

## Result

The final run killed 16 mutants in unit tests. One raw `missed` result is
classified as equivalent/irrelevant for catalog-derived contracts. There were
no unbuildable mutants, compile errors, or timeouts. The 16 kills are backed by
test failures after successful compilation, not linker or harness failures.

The pre-change unit baseline passed 2,670 tests. The final unmutated baseline
and post-change `just test-unit` both passed 2,671 tests. The first mutation
pass found four misses. Three were valid primary-key or `NOT NULL` unique-key
cases absent from the tests; the new
`source_key_policy_accepts_primary_and_not_null_unique_keys` test covers them
and the final run killed all three mutants. The remaining miss changes behavior
only for a contradictory contract (`primary = true` with nullable columns and
`NULLS NOT DISTINCT`); PostgreSQL primary-key catalog facts require non-null
columns, so that input is unreachable for this catalog-derived contract.

`just fmt` and `just lint` passed after the test change; lint reported no
warnings.

## Selection and setup

The selected functions were:

- `src/scheduler/watermark.rs::should_emit_holdback_warning` — 7 mutants for
  disable, age-threshold, rate-limit, and clock-skew decisions.
- `src/dvm/row_id_v2.rs::validate_source_key` — 10 mutants for source identity
  eligibility.

An unfiltered repository inventory was stopped in
`return_type_replacements` before it produced results; its projected disk use
was about 170 GB. That is an estimate, not measured usage. To keep this pilot
bounded, cargo-mutants ran from a temporary copy with `mutants::skip` attached
to unrelated functions and modules. The scratch copy also declared an empty
`mutants` feature and allowed the scratch-only cfg. The repository itself gained
no instrumentation, feature, or dependency.

The scratch copy retained `.cargo/config.toml`, `Cargo.lock`, bench sources, and
`scripts/gen_test_schema.py`. Unit tests used the repository PostgreSQL stub
library, `pg18`, `--lib`, and one test thread. The command was:

```sh
PATH=<cargo-mutants-27.1.0>/bin:$PATH \
CARGO_TARGET_DIR=<isolated-target> \
DYLD_INSERT_LIBRARIES=<repo>/.cargo-target/libpg_stub.dylib \
CARGO_NET_OFFLINE=true cargo mutants --no-config --gitignore=false \
  --dir <filtered-scratch-copy> --features pg18 \
  --file src/scheduler/watermark.rs \
  --file src/dvm/row_id_v2.rs \
  --re 'should_emit_holdback_warning|validate_source_key' \
  --jobs 1 --baseline run --timeout 60 --build-timeout 300 \
  --cargo-test-arg=--lib --output <results> \
  -- -- --test-threads=1
```

## Per-mutant outcomes

`killed_by_test` means the mutant compiled and a unit-test assertion failed.
The one `equivalent/irrelevant` row passed all tests and differs only on the
unreachable contract described above.

| Location | Mutation | Outcome | Evidence |
|---|---|---|---|
| `row_id_v2.rs:1543` | Replace `validate_source_key` with `Ok(())` | `killed_by_test` | Existing unsafe-key cases fail. |
| `row_id_v2.rs:1545` | Replace first `&&` with `||` | `killed_by_test` | New valid-key cases fail. |
| `row_id_v2.rs:1545` | Replace second `&&` with `||` | `killed_by_test` | New valid-key cases fail. |
| `row_id_v2.rs:1545` | Delete `!` on `columns_not_null` | `killed_by_test` | New primary-key case fails. |
| `row_id_v2.rs:1545` | Delete `!` on `nulls_not_distinct` | `equivalent/irrelevant` | Only changes a primary-key contract with nullable columns. |
| `row_id_v2.rs:1560` | Delete `!` on `immediate` | `killed_by_test` | Valid-key case fails. |
| `row_id_v2.rs:1562` | Delete `!` on `default_btree_operator_class` | `killed_by_test` | Valid-key case fails. |
| `row_id_v2.rs:1566` | Replace `&&` with `||` in nullable-key check | `killed_by_test` | Existing valid `NULLS NOT DISTINCT` case fails. |
| `row_id_v2.rs:1566` | Delete `!` on `columns_not_null` | `killed_by_test` | Existing nullable `NULLS DISTINCT` rejection fails. |
| `row_id_v2.rs:1566` | Delete `!` on `nulls_not_distinct` | `killed_by_test` | Existing valid `NULLS NOT DISTINCT` case fails. |
| `watermark.rs:213` | Replace helper result with `true` | `killed_by_test` | Disabled and below-threshold cases fail. |
| `watermark.rs:213` | Replace helper result with `false` | `killed_by_test` | Above-threshold and expired-rate-limit cases fail. |
| `watermark.rs:213` | Replace `warn_secs <= 0` with `warn_secs > 0` | `killed_by_test` | Disabled warning cases fail. |
| `watermark.rs:216` | Replace `<` with `==` | `killed_by_test` | Below-threshold case fails. |
| `watermark.rs:216` | Replace `<` with `>` | `killed_by_test` | Above-threshold case fails. |
| `watermark.rs:216` | Replace `<` with `<=` | `killed_by_test` | At-threshold case fails. |
| `watermark.rs:220` | Replace `>=` with `<` | `killed_by_test` | Rate-limit boundary cases fail. |

## Cost and recommendation

The final run took 256.57 seconds wall time (4m17s). Its peak temporary target
was about 5.0 GB; the scratch source was 61 MB and outcome files were under
5 MB. The baseline build took 73 seconds and its 2,671 tests took 5.7 seconds.
Temporary files and the locally installed tool were removed after the run.

Keep cargo-mutants as an optional, manually scoped supplement for pure Rust
helpers. Do not add it to CI or gate merges on it yet: a full-repository
inventory was too costly, while the bounded two-function run was practical.
