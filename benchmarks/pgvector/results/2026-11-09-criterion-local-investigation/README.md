# Focused Criterion investigation on ARM64

On an Apple M1 Pro with Homebrew Rust 1.99.1, the
`diff_aggregate/count_star_1group` Criterion benchmark was run once for the
task's package branch at commit `9be4bc487ba31df0383f577a472c061fa27382a6`,
then once for published v0.108.3, with the same machine and quick settings
(100 samples, five-second measurement, one-second warm-up). The reported
time intervals were 5.128–5.146 µs and 4.359–4.389 µs respectively. This
single ordered ARM64 comparison is consistent with a slowdown but cannot
establish its cause or replace the hosted amd64 release gate.

An uncommitted experiment changed the existing schema/column-name consistency
check to compare borrowed iterators rather than allocate a cloned name list.
Its measured interval was 5.088–5.096 µs, and Criterion reported a 0.76%
improvement against the saved candidate baseline, within its noise threshold.
The experiment was reverted. The logs and estimate files are retained here.
The versions use their respective PostgreSQL stub sources; these pure-Rust
benchmarks do not intentionally call the stubbed functions. Builds and runs
were sequential, not randomized or isolated from host activity.

Ten-second macOS samples of the published binary and the uncommitted
allocation experiment are retained as compressed profiles. Both include
aggregate SQL generation, formatting and allocation in their sampled stacks.
The candidate profile includes the reverted experiment, and these samples do
not isolate the cause of the release-to-release difference.

The hosted release qualification still reports 41 gated regressions on retry;
see the [qualification failure evidence](../2026-11-09-c138-qualification-failures/README.md).
No release budget has been waived, and the regression cause remains open.
