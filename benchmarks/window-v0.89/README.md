# v0.89 window admission artifact

`admission.json` is the v0.89 release decision for the window candidates, and
`samples.json` contains the 60 measured samples used to derive it. No window
family is runtime-enabled. Every function uses `partition_recompute` with a
`WINDOW_*` reason.

The benchmark evaluated the narrow built-in `ROW_NUMBER` candidate over one
direct keyed scan. The query used exact same-name projections and included the
complete non-null identity in `ORDER BY`. The candidate filtered changed output
rows but still recomputed the affected partition.

| Rows | Change | State candidate median ms [range] | Partition recompute median ms [range] | Recompute/state ratio |
|---:|---|---:|---:|---:|
| 1,000 | Tail insert | 29.941625 [28.490875, 31.011292] | 12.065875 [11.525541, 13.216917] | 0.4030 |
| 1,000 | Front insert | 92.454083 [44.982209, 152.869667] | 20.670208 [18.595209, 38.816750] | 0.2236 |
| 10,000 | Tail insert | 553.613125 [143.272875, 594.418208] | 24.684708 [23.715125, 25.846125] | 0.0446 |
| 10,000 | Front insert | 691.848292 [685.656292, 762.309333] | 112.537125 [99.531458, 123.249584] | 0.1627 |
| 100,000 | Tail insert | 1556.859583 [1311.842167, 1831.429541] | 164.637041 [158.997375, 192.374791] | 0.1057 |
| 100,000 | Front insert | 4190.417875 [4085.014458, 4430.121000] | 1299.224083 [1229.935416, 1420.041042] | 0.3100 |

The ratio is the partition-recompute median divided by the state-candidate
median. Every ratio is below 1.0, so the candidate failed the 20 percent
admission gate. v0.89 makes no incremental speedup claim.

Validate the repository contract offline:

```bash
python3 scripts/v0_89_release_gate.py
```

Reproduce the timing, WAL, and output-row samples with:

```bash
bash scripts/run_light_e2e_tests.sh \
  --package --test e2e_window_incremental_tests --ignored
```
