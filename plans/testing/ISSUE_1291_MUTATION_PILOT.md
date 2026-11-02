# Issue #1291: Loaded DVM mutation pilot

The bounded campaign completed on candidate `7a993846c004a2c681d65df2cba2442d03afffca`, based on `main` at `3331b7701c9efa8a1845780cf81ac38ce961f3c7`. It ran on PostgreSQL 18.6, Linux/amd64. Each mutant ran in its own PostgreSQL container with a separately built and hash-attested extension.

## Acceptance results

| Mutation and exact selected test | Baseline | Mutant and first exact mismatch |
|---|---|---|
| LEFT L₀ snapshot — `test_diff_full_equivalence_left_join_l0_snapshot_mutation` | Exact bag matched; `DIFFERENTIAL`, `COMPLETED`, no fallback, 3 delta rows; committed source cut `0/28D6D10`. | Detected at source cut `0/28DE3B0`; `DIFFERENTIAL`, `COMPLETED`, no fallback, 2 delta rows. The actual bag retained the stale row `(left_id=1, join_key=5, bucket=10, right_id=101, value='deleted')` alongside the new null-padded row for key 6. Mutant library SHA-256: `0293bb7e36f30e9bd5a07fa3e827699979173839a0c939734b1a7f4e23818a5d`. |
| FULL old-match guard — `test_diff_full_equivalence_full_join_old_match_guard_mutation` | Exact bag matched; `DIFFERENTIAL`, `COMPLETED`, no fallback, 5 delta rows; committed source cut `0/28F06A8`. | Detected at source cut `0/28F2120`; `DIFFERENTIAL`, `COMPLETED`, no fallback, 6 delta rows. Removing `EXISTS R_old` added a duplicate null-padded row for surviving unmatched left row `id=4`, `join_key=20`. Mutant library SHA-256: `32296b762b197885c9f552cdf04d5d2e87f9c6425eed06d614d7e69b63a6f299`. |

The baselines used package and loaded-library SHA-256 `3af623e8048a54b7a92c570f389189b42c425c18b9f1b38f34896fad21fb922e`. For each mutant, the package SHA matched the installed and loaded `pg_trickle.so` SHA above. Each source cut is recorded immediately before its target refresh; baseline and mutant runs use isolated databases with the same committed test history.

The mutation patch SHA-256 values are `f24ee92257771f8ec033374581d03f6450b9971bbedde39c1377c81f52e0d158` (LEFT) and `3ab34ce978e67e2612e87a91ceb536577c991c6e3c98e7c53f509d6affefd442` (FULL). The tested baseline source SHA-256 values are `551f90ae2a523fce14052811202d872c7781920ce6a155366b49bb4cbaba3c99` and `1522abbbe4ff77560a16867bc670abf889ae95f9b472f8d34828c9b1da16a136`; the corresponding mutated source hashes are `0bd6416a8c414943992e293cc30b51bc025fe92651866cb52c071b1ba4bd1f24` and `669fe0106439527ee5ea5a1c5fb828a02687fce21f2cc934cd0a4a809361c09d`.

The LEFT history updates the same left primary key's join key while updating and deleting right rows in the same refresh. The FULL history deletes an old match and removes a transient match on a nested right input while an unmatched same-key left witness survives. An earlier recursive LEFT history also detected its mutant; the earlier simple FULL history reached Part 5 but survived. The refined FULL history above now demonstrates the removed guard's duplicate row.

## Reproduction

Run the bounded campaign from a clean candidate checkout:

```sh
python3 scripts/run_dvm_mutation_pilot.py \
  --issue 1291 \
  --artifacts-dir /private/tmp/pgtrickle-1291-campaign \
  --min-free-gib 10 \
  --max-retained-artifact-mib 512 \
  --sample-interval-seconds 30
```

Only the two tests listed in the results table are selected by this campaign; it does not add a PR-wide mutation gate. Full campaign artifacts were retained at `/private/tmp/issue-1291-mutation-campaign-final-host` during this run.

## Resource and cleanup evidence

Run `i1291-261102091453379177-44866` completed in 90.43 seconds with 21 disk samples and no retries. Host free space started at 53,229,260,800 bytes, reached a low of 53,025,374,208 bytes, and ended at 53,210,324,992 bytes, remaining above the configured 10 GiB floor. Peak retained campaign artifacts were 75,904,438 bytes; 520,251 bytes remained. Aggregate Docker usage rose by 118,500,000 bytes at peak and returned to its starting value. Docker Desktop internal free space could not be measured.

Run-scoped cleanup completed with no remaining containers, volumes, worktrees, staging paths, or ephemeral images. The forced-failure cleanup probe confirmed an unrelated sentinel survived scoped cleanup and was then removed explicitly.
