# DVM schema comparison experiment

The DVM root check now compares existing column names in order without
cloning them. It still checks the number, order and exact spelling of names;
the error path retains the full schema in its diagnostic.

On 2026-11-15 an Apple M1 Pro ran release binaries with Rust 1.99.1 and
debug line information. The control was built from `e8a291ef`; the experiment
used the same Rust sources plus the schema comparison change. The intervening
benchmark-launcher commit changes scripts and tests only. Binary SHA-256 values
were `cbcfc737463076c776f9c6110af324faa79942b70f4fff42ca897bcfc573c97d`
and `7177d4c6465bbdfeca9f9bafe37c564ce8a97d2fcb325c77ae4af8ec9d812298`.
Each case ran control, experiment, experiment, control. The aggregate used
100 samples and five seconds of measurement; the scan group overrides this
with 200 samples and ten seconds. Each had one second of warmup.

| Benchmark | Control central estimates | Experiment central estimates |
| --- | --- | --- |
| `diff_aggregate/count_star_1group` | 5.1379, 5.0971 µs | 5.1030, 5.0406 µs |
| `diff_scan/3cols` | 3.6613, 3.6307 µs | 3.5982, 3.6005 µs |
| `diff_scan/10cols` | 7.9057, 7.8046 µs | 7.7002, 7.6652 µs |
| `diff_scan/20cols` | 13.792, 13.814 µs | 13.486, 13.460 µs |

[Aggregate and wide-scan output](aggregate-and-wide.log) and
[narrow-scan output](narrow.log) retain the measurements. Criterion's printed
change percentages compare each run with the immediately preceding run; use
the ordered central estimates above for this comparison. The wider scan saved
about 0.31–0.35 µs; the other cases had smaller gains. These local results do
not establish production refresh latency or a passing release performance
gate against v0.108.3.

The change passed `just fmt`, `just lint`, all 2,725 unit tests and all 89
PostgreSQL-backed tests in `just test-dvm`.
