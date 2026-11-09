# Alternating native ARM pgbench observations, 2026-11-07

The blocking workload ran six times in this image order: candidate, main,
main, candidate, candidate, main. Each run had one paired repetition with
the normal scale, clients, warmup and duration. The configuration order was
rotated between runs. Combining each image's three repetitions gives:

| Image | Installed/absent TPS ratios | Median | Active/absent p99 median | Result |
| --- | --- | ---: | ---: | --- |
| [Candidate](candidate.json) | 1.000, 1.068, 1.015 | 1.015 | 1.588 | Pass |
| [Main](main.json) | 0.113, 0.350, 0.961 | 0.350 | 1.741 | Fail |

All active-stream correctness checks passed. The first two main installed
measurements were unusually slow: 840 and 2,749 TPS, versus 7,402 and 7,861
TPS for their respective absent baselines. The 840 TPS sample consumed
approximately 28 container CPU seconds during its 30-second measurement;
its absent baseline consumed approximately 100 CPU seconds. This is consistent
with waiting, but does not identify the cause. The earlier main run on the
same host passed the gate. These observations do not establish a candidate
regression or prove that a hosted CI run will pass.

The host and pinned PostgreSQL base image are described in the [earlier
investigation](../2026-11-07-native-arm/README.md). The native ARM64
candidate image was `sha256:c68ba07e6dfe77b69f3f913bbecf7ee04e0ab898daea14f16764102abac801fc`;
it was built from the source corresponding to `bc1237f9`, before that commit
was created. The main image was
`sha256:67bf3a65760d91a0030fe403701f4931d1c632c147b2072147ad93513a034632`,
built from `5b823799`. The JSON records' `commit` fields identify the runner
checkout, not the source of the main image. Individual run repetition numbers
were renumbered 0 through 2 when combining the [candidate](candidate.jsonl)
and [main](main.jsonl) records. Result files were generated using the checked
three-repetition minimum. The full local logs and transaction samples are in
`/private/tmp/pgtrickle-interleaved-arm-raw.tar.gz`; they are not in Git.
