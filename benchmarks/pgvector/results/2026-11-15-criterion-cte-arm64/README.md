# CTE identifier allocation experiment

The CTE identifier helper now builds the sanitized prefix directly into the
result string. It retains the same character replacement and 63-byte limit;
the suffix still reserves space for the counter. This helper is shared by
the DVM operators, so the change is deliberately limited to string assembly.

On 2026-11-15, the Apple M1 Pro ran locally built optimized binaries from
integrated source `f2e48777f436516959a2ee41674279929ebf03d0`, with and
without this change. Both used Rust 1.99.1 and release settings with debug
line information. For each benchmark, the host ran control, experiment,
experiment, control, with 100 samples, one-second warmup and five-second
measurement. The binaries had SHA-256 values
`4da2118e0c1d173189d4bd5c9a92d6419d89784107c990bf3e2d1dd7fef0b6ea`
and `e7eb8655ba00f0ba161f5d0e43dbc80fabc27743821b13c62131af0d0280eed7`.

| Benchmark | Control central estimates | Experiment central estimates |
| --- | --- | --- |
| `diff_aggregate/count_star_1group` | 5.3433, 5.3233 µs | 5.0362, 5.0471 µs |
| `a44_7_scan_agg_delta/n_groups/1` | 10.504, 10.512 µs | 9.9506, 9.9565 µs |

The [control](pgt-cte-control-1.log), [experiment](pgt-cte-experiment-1.log),
[experiment](pgt-cte-experiment-2.log), and [control](pgt-cte-control-2.log)
logs retain the first comparison; the `pgt-cte-scan-*` logs retain the second.
The [control profile](profile-control.txt.gz) contains sampled source lines.
The standalone [probe](isolated-probe.rs) checks representative encoding
cases and isolates the allocation cost; it is not a replacement for the
end-to-end benchmark.

These ordered local samples suggest a reduction of roughly 0.28–0.31 µs and
0.55–0.56 µs, respectively. They compare two versions of the current source,
not the v0.108.3 release baseline. They do not establish that the hosted
Criterion regression gate will pass or measure PostgreSQL refresh latency.

The final change passed `just fmt`, `just lint`, all 2,725 unit tests, and
all 89 PostgreSQL-backed tests in `just test-dvm`. The identifier tests
cover ordinary names, non-ASCII replacement, truncation, and counter-width
growth. The DVM suites exercise generated SQL across aggregate, join, window,
and subquery operators.
