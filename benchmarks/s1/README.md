# S1 graph-refresh qualification

Run the frozen R53 joint-stack workload from [issue #1168](https://github.com/trickle-labs/pg-trickle/issues/1168)
against exact control and candidate Docker images. The candidate contains the
graph-refresh optimizations merged in #1169 and the keyless-delta fix in #1171.

The unchanged workload requires 300 admissions at >=60 cases/minute, complete
case/source vectors, zero future receipts, and a 1,000-case due burst with
p95 <=150 seconds and maximum <=180 seconds. Whole-run storage must remain
below 10 GiB. The supervisor stops at 9 GiB sampled storage, 4 GiB sampled
container memory, 45 minutes, or less than 10 GiB host free space. These are
sampled limits, not a per-operation memory guarantee.

The workflow runs both images sequentially on the same native Linux amd64
runner. This environment differs from the earlier amd64 runs emulated on an
ARM64 Docker host; historical rates cannot establish a speedup on this host.
The workload and acceptance thresholds are unchanged.

## Fixed inputs

| Component | Source |
| --- | --- |
| Corrected control | pg-trickle `1c0345791979907e2c1418b0008ea15d58311cb9` |
| Candidate | Exact PR head (or manually selected workflow ref) |
| MDM | pg-mdm `eef7937180d504e62ce1076074bbeed8cef5d5a7` |
| React | pg-react `f19d9b2c5f3d947705ea26aa1619a989f08ae1dc` |
| PostgreSQL | 18.4, pinned image digest in `Dockerfile` |
| Harness | Retained R53 files listed with original SHA-256 values in `frozen-harness.json` |

The old runnable joint-stack images were removed during host cleanup. Their
React package had admitted pg-trickle 0.108.3; the pinned React source still
admits only 0.108.0. The Dockerfile explicitly replaces that exact patch-version
pin in the 0.46.0/0.46.1 install SQL. Isolation, trigger CDC, scheduler settings,
change-ratio and Graph/Delta capability checks remain intact. Both images use
the same reconstructed React package; its installed SQL hashes are retained.

The user approved the separately identified corrected control on 2026-10-19.
The original exact control `sha256:1fdfeb8e1c08b049491583612f7e34cd4c0e27c7d789084a627ce3089e04d981`
completed two admissions and deadlocked on admission 3. Its retained
investigation and complete copied server log are in `historical-control/`.
The corrected control does not turn that failed attempt into a successful
comparison. Each new pair must complete on its own images and environment.

## Running and retaining evidence

The `S1 exact-image qualification` workflow runs for changes to this directory
or its workflow and can also be dispatched manually after the workflow is on
the default branch. Its artifact contains exact source snapshots (including
lockfiles), build logs, host configuration, image/library identities,
complete observations and vectors, resource samples, failures, and hashes.
Download artifacts before their 90-day expiration for long-term retention.

Local supervision checks need only Python's standard library:

```sh
python3 benchmarks/s1/check_frozen_harness.py
python3 benchmarks/s1/test_bootstrap_compat.py
python3 benchmarks/s1/test_check_result.py
python3 benchmarks/s1/qualification/run_bounded.py --self-check
python3 benchmarks/s1/qualification/test_supervisor.py
```

For a locally built exact joint-stack image:

```sh
PGREACT_S1_BATCH_SIZE=32 python3 benchmarks/s1/qualification/run_bounded.py \
  sha256:<full-image-id> candidate-300
```

Run names must be new; existing observations are never overwritten. The
supervisor directly invokes Docker instead of requiring the original host's
`rtk proxy` wrapper. S1 workload, SQL fixtures, and thresholds retain their saved
values. The bootstrap adapter retains the original MDM fixture and its hash,
then changes only two setup assertions: a one-record insert/delete may use the
exact affected-record resolver added by #1171 as well as the historical full
invalidation path. The helper-mutation checks remain mandatory. Both images
use the same adapter, and its input patterns must match exactly once.
`frozen-harness.json` records the original and adapted file hashes.
Diagnostic case-count overrides do not qualify the throughput gate.

`check_result.py <run-directory>` independently requires all S1 gates and
complete source/case vectors. An input-blocked harness that exits zero is
rejected, as are diagnostics, incomplete runs, future receipts, or resource
limit failures.

A passing S1 run covers this upstream throughput issue. React owner-outcome,
outage/recovery, and signed operating-envelope release gates remain separate;
the frozen harness reports those broader inputs as unresolved even after an
S1 performance pass.
