# PROVEN: issue #1098

**Contract:** v1, SHA-256 `54cb9d4f028518242e577a4b03f372371a91e9188a517f2d03d6e6d37dc4e450`  
**Candidate:** `f868e873475fdb3fbefa48154972704a8c8cc56d`  
**Comparison base:** `2dfe8dae90452120cf6760b71dd0def8c241756b`

The candidate, base, contract, and all five hashes in `candidate.json` matched. The candidate’s current review is `REVIEWED`, with no material findings. The three new ASan evidence hashes also matched.

| Requirement | Observation and oracle | Verdict |
|---|---|---|
| R01 | The instrumented workload log records PostgreSQL 18.6, the assertions-enabled boot test, and ASan symbols in both `postgres` and `pg_trickle.so`. | Proven |
| R02 | The isolated probe reports the expected `heap-buffer-overflow` at `asan_probe.c` and aborts; the test passes. | Proven |
| R03 | The tuple-copy test passes with NULL and TOAST values through repeated refresh. | Proven |
| R04–R05 | Owner and caller context tests pass, checking same-backend role, `search_path`, and `row_security` restoration and continued allowed/denied operations. | Proven |
| R06 | The worker cancellation and restart test passes, checking cancellation, reconciliation, and recovered output. | Proven |
| R07 | The diagnostic test passes after retaining raw PostgreSQL logs. The expected probe report is isolated from workload diagnostics. | Proven |
| R08 | The release gate reports 12 passing tests and checks the declared cases and workflow routing. | Proven |
| R09 | The exact-candidate Release artifact identifies the uninstrumented exact-release package separately. The full bounded instrumented slice also passed against this candidate using its separately identified ASan image. The Release run itself remains blocked; no ASan result is represented as execution of the shipped package. | Proven |

**Instrumented evidence**

- Build: [asan-f868-image-build.log](.p2p/work/issue-1098-unsafe-boundaries-under-assertions-and-sanitizers/evidence/asan-f868-image-build.log), SHA-256 `6ed3eede9e75b8b4033a75862705967fa7d4353e365bac6e30790ffc53a8e768`. Image digest `sha256:6a899c43c9b4c5ff14a95a1b14464bf001d978dc9d2075af471f56d199dbbdc4`; config digest `sha256:7135e34f1229378e63a13ce30bb12088003cde0054b364b98f6988d604026f53`.
- Workloads: [asan-f868-local-workloads.log](.p2p/work/issue-1098-unsafe-boundaries-under-assertions-and-sanitizers/evidence/asan-f868-local-workloads.log), SHA-256 `1ab1a857e9f7f634d66fffefed2dd626e49bd126a18a2da9975355b5336af2dd`; **10 passed, 0 failed**.
- Gate: [asan-f868-release-gate.log](.p2p/work/issue-1098-unsafe-boundaries-under-assertions-and-sanitizers/evidence/asan-f868-release-gate.log), SHA-256 `ac969cdef3405e1ed7bf40b806c519aed9905b36fc8e9c2aa68873da8aa52b29`; **12 passed**.

**R09 limitation:** Release run [36350963166](https://github.com/trickle-labs/pg-trickle/actions/runs/36350963166), artifact `10944569661`, is bound to this candidate. The run’s own ASan step timed out before workloads, and its evidence has no ASan workload log. Its manifest is `blocked`; `criterion-regression` also blocks publication. The separate local ASan run supplies the completed instrumented-slice evidence, while the Release artifact supplies exact-package identity. Neither is presented as the other. The archive SHA-256 is `b2ecd1a0e971bccdb502fca3d7e1b5c9330775d1f466e48ae4dcd05062539681`; `RELEASE-EVIDENCE.json` SHA-256 is `bc0ed1caf1620b59e40288c37ad69cad51811283e9cbe4b6afd2dfdc27ff8031`.

No product or report files were changed in this read-only proof stage. **Proof-report storage remains pending.**
