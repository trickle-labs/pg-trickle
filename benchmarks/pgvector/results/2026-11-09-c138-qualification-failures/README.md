# Candidate qualification failures

[Qualification run 42626302353](https://github.com/trickle-labs/pg-trickle/actions/runs/42626302353)
tested commit `c138e4fe10cb07774129eba1d05a54b6634805b1`. Its platform builds
and release artifact check passed, but the exact-package qualification failed.
The [retained workflow artifact](https://github.com/trickle-labs/pg-trickle/actions/runs/42626302353/artifacts/13690899392)
contains the complete suite results, logs, and raw Criterion estimates.

- The recovery suite's selector omitted four required scheduler tests. The
  [log](recovery.log.gz) shows the 13 selected cases passed, but the four
  omitted cases were not observed; release evidence correctly rejected it.
- The WAL-retirement suite used the light harness, whose shared-container
  fixture lacks the restart method needed to compile the test binary. The
  [compiler output](wal-retirement.log.gz) is retained. Restart cases require
  an isolated full E2E container rather than restarting a shared light fixture.
- The Criterion comparison against published v0.108.3 reported 42 gated
  regressions initially and 41 on the candidate retry; 40 names appeared in
  both lists. The retry compared 113 benchmarks with a 10% relative threshold
  and 250 ns absolute floor. These include SQL-generation microbenchmarks;
  they are not end-to-end query latency measurements. The
  [full comparison log](criterion.log.gz) and [summary](criterion.json) are
  retained. The raw estimates archive in the workflow artifact has SHA-256
  `ff9fe3ea6679160ef2eae6bbcc6082a68f22e099073f38c28c35ac8b6fbae410`.
  The performance regressions remain unexplained and are not waived.
  The candidate's `src/dvm/` and `benches/` trees match the task's starting
  `main` commit `012edc766b3c3f6d2c35ca4bc7af9d2676442709`; this does not
  identify which changes since v0.108.3 caused the measured differences.

The failed measurement and compile-suite records incorrectly labeled their
empty case attempts as passed despite nonzero command exit codes. Their suite
status was failed and the release gate rejected them; attempt reporting also
needs to reflect the command failure.

This evidence is for the specified older candidate, not the final integrated
commit. Fixes for suite selection and harness choice still require their own
candidate-bound execution and release qualification.
