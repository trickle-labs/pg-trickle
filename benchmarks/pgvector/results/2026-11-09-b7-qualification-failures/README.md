# Hosted qualification failures for b7b6c0ca

[Qualification-only run 42656287566](https://github.com/trickle-labs/pg-trickle/actions/runs/42656287566)
tested integrated candidate `b7b6c0ca4dc2dd9cdff61a91afa000a045f45e4f`.
It built and smoke-tested the release archives, but the final qualification
failed. Publication jobs were skipped. The retained per-suite results identify
the tested linux-amd64 archive as SHA-256
`e500970f5730c4bb9dbf28db05467940522b3e73203552d6adfae5dc4f311448`.

Three suites failed:

* [Recovery](recovery.json) executed 13 of 17 selected cases; its
  [command log](recovery.log) shows the selector omitted the four scheduler
  cases. The selector is corrected in the later package branch, with a
  [local packaged 17-case run](../2026-11-09-qualification-suite-fixes-amd64/README.md).
* [WAL retirement](wal-retirement.json) selected eight cases but executed
  none because the light harness could not compile their required helpers;
  see the [log](wal-retirement.log). The later branch switches to the full
  isolated E2E harness; all eight passed in the same local evidence above.
* [Criterion](criterion-regression.json) compared against v0.108.3. Its
  [complete log](criterion-regression.log) reports 46 gated regressions on
  the first attempt and 25 on retry. The retry [machine-readable results](criterion.json)
  include 113 paired cases; `diff_aggregate/count_star_1group` was 21.7%
  slower, approximately 1,166 ns, and the largest reported relative
  regression was 76.1%. These exceed the 10% and 250 ns gate thresholds.
  This is an additional hosted run, not a final-candidate qualification or
  an explanation of the performance difference.

This older suite runner incorrectly labeled some failed *attempts* as passed
despite nonzero exit codes. The enclosing suite results and release gate failed
correctly; the attempt-status reporting is fixed on the later package branch.
No benchmark budget was waived. The corrected integrated candidate must run
the full hosted qualification, and the performance regression remains open.
