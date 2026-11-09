# Focused Criterion investigation on ARM64

On an Apple M1 Pro with Homebrew Rust 1.99.1, the
`diff_aggregate/count_star_1group` Criterion benchmark was run once for the
task's package branch at commit `9be4bc487ba31df0383f577a472c061fa27382a6`,
then once for published v0.108.3, with the same machine and quick settings
(100 samples, five-second measurement, one-second warm-up). The reported
time intervals were 5.128–5.146 µs and 4.359–4.389 µs respectively. This
single ordered ARM64 comparison is consistent with a slowdown but cannot
establish its cause or replace the hosted amd64 release gate.

A later single run of the same case at the parent of the pgvector roadmap
commit, `50a1d633da718d1b8c50afb11cf8a99347ff2ef7`, reported
4.840–4.855 µs. This is between the two other intervals, suggesting that
more than one revision interval merits investigation. The runs were ordered
and separated by builds and other work, so this does not assign causation.

Another later single run at `bd96b807` reported 4.350–4.379 µs, close to
the v0.108.3 sample. Criterion marked 21 of its 100 measurements as outliers,
so this narrows the revision range to inspect but is not a controlled bisect.

Further ordered runs reported 4.334–4.343 µs at `32489808`, and
4.907–4.917 µs at `e9f1485159432d8842c69857c859fd110dad5a17`. At the
intervening aggregate-lifecycle merge `5958200f929c97b3e7d5f4d7580102c387ad211a`,
the first sample was highly variable (5.706–6.326 µs); an immediate repeat
was 5.114–5.126 µs. The earlier `32489808` binary then measured
4.509–4.534 µs. This interval includes dependency updates as well as DVM
changes. These unrandomized local runs identify a range for further profiling,
not a specific cause or a substitute for the hosted release comparison.
An additional local experiment used the `32489808` source with the Cargo
manifest and lockfile from `5958200f`; it measured 4.344–4.354 µs. This makes
the dependency update alone a less likely explanation for this local result,
but the ordered runs and host variability prevent a causal conclusion.
The immediate parent of `5958200f`, `9aa753077c2c53e153a9dd3a52d37c0a3e36b442`,
measured 4.337–4.355 µs in a subsequent run. It has the same Cargo manifest
and lockfile as `5958200f`; the merge also changes scan and context code in
addition to aggregate lifecycle SQL generation. This points to the merge as
an interval for investigation, not a particular cause or safe fix.

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

A second uncommitted experiment built the checked group-action SQL directly
into one string instead of formatting intermediate branch strings. It measured
5.205–5.214 µs; Criterion reported a 1.61% slowdown against the saved candidate
baseline. This single ordered result does not establish a general effect, and
the experiment was discarded.

A third isolated experiment reversed the nesting order of the checked group
action decision, intending to preserve its lifecycle outcomes. It measured
5.046–5.057 µs, about 1.64% (roughly 85 ns) faster than the saved candidate
sample. This is below the release gate's 250 ns absolute materiality floor
and does not resolve its regressions; the experiment was discarded without
changing production SQL.

Hosted release qualifications reported 41 and 25 gated regressions on retry;
see the [first](../2026-11-09-c138-qualification-failures/README.md) and
[second](../2026-11-09-b7-qualification-failures/README.md) failure evidence.
No release budget has been waived, and the regression cause remains open.

A further isolated [direct-buffer experiment](direct-buffer-experiment.patch)
on integrated source `a6493ee8` replaced one intermediate joined SELECT string
with direct writes into a pre-sized string. After compiling both binaries, the
same host ran control, experiment, experiment, control in that order, with
100 samples, five-second measurement and one-second warm-up per run. The
[controls](direct-buffer-control-1.log) reported 5.1305–5.1370 µs and
[5.1248–5.1323 µs](direct-buffer-control-2.log); the
[experiments](direct-buffer-probe-1.log) reported 5.0172–5.0336 µs and
[5.0169–5.0259 µs](direct-buffer-probe-2.log). The roughly 0.11 µs difference
is below the 0.25 µs materiality floor and does not explain the hosted
slowdowns. The change was discarded. Formatting and lint passed on the probe;
SQL behavior was not independently verified because the result was not
material. These local sequential samples are not the release comparison.
