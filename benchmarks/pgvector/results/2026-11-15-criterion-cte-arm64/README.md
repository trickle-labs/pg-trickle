# CTE identifier allocation experiment

The CTE identifier helper now builds the sanitized prefix directly into the
result string, reserving space for the prefix and counter. It retains the
same character replacement and 63-byte limit. This helper is shared by the
DVM operators, so the change is deliberately limited to string assembly.

On 2026-11-15, the Apple M1 Pro ran locally built optimized binaries from
integrated source `f2e48777f436516959a2ee41674279929ebf03d0`, with and
without this change. Both used Rust 1.99.1 and release settings with debug
line information. For each benchmark, the host ran control, experiment,
experiment, control, with 100 samples, one-second warmup and five-second
measurement. The binaries had SHA-256 values
`4da2118e0c1d173189d4bd5c9a92d6419d89784107c990bf3e2d1dd7fef0b6ea`
and `7ac254261fe3bc87a07ab2065cfb3409dc636180b5cc07acc85fa1f4752f7001`.

| Benchmark | Control central estimates | Experiment central estimates |
| --- | --- | --- |
| `diff_aggregate/count_star_1group` | 5.3439, 5.3211 µs | 5.0319, 5.0368 µs |
| `a44_7_scan_agg_delta/n_groups/1` | 10.484, 10.477 µs | 10.060, 9.9797 µs |

The `pgt-cte-sized-aggregate-*` and `pgt-cte-sized-scan-*` logs retain the
final comparisons. Earlier `pgt-cte-control-*`, `pgt-cte-experiment-*`, and
`pgt-cte-scan-*` logs record a variant that reserved 63 bytes for every name.
The [control profile](profile-control.txt.gz) contains sampled source lines.
The standalone [probe](isolated-probe.rs) checks representative encoding
cases and isolates the allocation cost; it is not a replacement for the
end-to-end benchmark.

These ordered local samples suggest a reduction of roughly 0.28–0.31 µs and
0.42–0.50 µs, respectively. They compare two versions of the current source,
not the v0.108.3 release baseline. They do not establish that the hosted
Criterion regression gate will pass or measure PostgreSQL refresh latency.

The final change passed `just fmt`, `just lint`, all 2,725 unit tests, and
all 89 PostgreSQL-backed tests in `just test-dvm`. The identifier tests
cover ordinary names, non-ASCII replacement, truncation, and counter-width
growth. The DVM suites exercise generated SQL across aggregate, join, window,
and subquery operators.
