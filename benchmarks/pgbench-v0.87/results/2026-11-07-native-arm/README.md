# Native ARM pgbench investigation, 2026-11-07

These are two sequential runs of the unmodified blocking profile, each with
three paired repetitions. They were run on an Apple M1 Pro with 32 GiB RAM;
Docker Desktop 29.9.2 exposed 10 CPUs and approximately 7.75 GiB to its Linux
ARM64 VM. Both PostgreSQL containers ran natively. The candidate ran first,
followed by `main`; the two runs were not interleaved, and three repetitions
are not enough to establish statistical significance.

| Source | Extension | Installed/absent TPS median | Active/absent p99 median | Worker CPU share | Gate |
| --- | --- | ---: | ---: | ---: | --- |
| [Candidate `b85f4ee9`](candidate.json) | 0.110.0 | 0.966 | 1.630 | 0.0020 | Fail |
| [Main `5b823799`](main.json) | 0.109.0 | 1.003 | 1.781 | 0.0021 | Pass |

The installed/absent TPS budget is at least 0.99; it was the only failed
candidate check. Per-repetition ratios overlap: candidate 0.966, 1.018,
0.947; main 0.979, 1.004, 1.003. All three active-stream correctness checks
passed in each run. The result files include the other budgets, per-repetition measurements,
sample counts, refresh counts and PostgreSQL settings. Individual transaction
logs are not included here.

Both runs used PostgreSQL 18.6, the same native builder and identical
`benchmarks/pgbench-v0.87` scripts and budgets. The base image was
`postgres:18.6-bookworm@sha256:bbf83594798e144bff365a9c191c5d779f181fb3135902064ab8ff11918020fc`;
the candidate and main extension image IDs were respectively
`sha256:a36f8d9f6a8cb46d3bfdd194a072954260899564e7b5832b4058db3e21adf88f`
and `sha256:67bf3a65760d91a0030fe403701f4931d1c632c147b2072147ad93513a034632`.

These results do not prove that the candidate introduced an overhead
regression. A scheduled run of the same gate on current `main` also failed on
GitHub-hosted hardware; its three installed/absent TPS medians were 0.894,
0.914 and 0.974. Repeat interleaved baseline and candidate runs on comparable
hardware, with the full per-transaction logs retained, before changing code
or budgets in response to the observed difference.
