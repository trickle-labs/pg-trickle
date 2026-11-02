# Issue #1291: Loaded DVM mutation pilot

Candidate: `dc00a725fa489059edca4a1362cca3ab25290e1e`

Campaign status: **incomplete**. The LEFT mutant was detected by the exact row
bag oracle. The FULL guard mutant compiled, loaded, reached Part 5, and survived
the selected scenario, so the issue is not complete.

## Outcomes

| Case | Baseline | Mutant | Evidence |
|---|---|---|---|
| `left-join-l0-snapshot` | Passed; differential, completed, no fallback; exact row bags matched | **Detected** by `EXACT ORACLE INVARIANT VIOLATION` | The actual bag retained the old `Company > Engineering > Backend` path and salary 120; PostgreSQL expected `Company > Research > Backend` and salary 115. |
| `full-join-old-match-guard` | Passed; differential, completed, no fallback; exact row bags matched | **Survived** | The generated Part 5 omitted the `EXISTS R_old` guard, but the exact row bags still matched for the tested right delete and left-key rename. This is not a kill. |

The LEFT history uses a recursive stream-table input. Renaming a department
produces D+I changes for the node and descendants; an employee delete is then
pending on the right input before the aggregate LEFT JOIN target refresh. This
exposes the mutant with a wrong result while keeping the emitted SQL valid.

Both baselines used library SHA-256
`3af623e8048a54b7a92c570f389189b42c425c18b9f1b38f34896fad21fb922e` for the
package and loaded library. The LEFT mutant used
`0293bb7e36f30e9bd5a07fa3e827699979173839a0c939734b1a7f4e23818a5d`; the FULL
mutant used
`32296b762b197885c9f552cdf04d5d2e87f9c6425eed06d614d7e69b63a6f299`. In each
case, the installed and loaded library hashes matched the mutant package hash.

## Reproduction

Run the bounded campaign from the candidate checkout:

```sh
python3 scripts/run_dvm_mutation_pilot.py \
  --issue 1291 \
  --artifacts-dir /private/tmp/pgtrickle-1291-campaign \
  --min-free-gib 10 \
  --max-retained-artifact-mib 512 \
  --sample-interval-seconds 30
```

The exact selected tests are
`test_diff_full_equivalence_left_join_l0_snapshot_mutation` and
`test_diff_full_equivalence_full_join_old_match_guard_mutation`.

## Resource and cleanup evidence

The final run took 114.75 seconds and sampled disk 18 times. Host free space was
60,584,800,256 bytes before, reached a low of 59,169,730,560 bytes, and ended at
59,390,427,136 bytes. It stayed above the 10 GiB floor. Peak retained campaign
artifacts were 75,819,003 bytes; 439,070 bytes remained. Aggregate Docker usage
grew by 118,500,000 bytes at peak and returned to its starting value. Docker
Desktop internal free space could not be measured. Scoped cleanup completed with
no remaining containers, volumes, worktrees, staging paths, or ephemeral images.

## Remaining work

The FULL mutant does not satisfy the requested wrong-result criterion. On the
tested base-table right input, the deleted-right-row predicate and `R_old`
predicate both matched the same join condition, and removing the latter did not
change the output bag. This suggests the curated guard may be redundant on this
path; it does not establish equivalence for every complex right input. Keep
issue #1291 open until a minimal valid history kills this mutation or the issue
is refined to use a non-equivalent FULL JOIN mutation.
