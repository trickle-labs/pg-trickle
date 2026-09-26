# Acceptance contract: #1098 — Exercise critical unsafe boundaries under assertions and sanitizers

Contract revision: v1  
Source: [issue #1098 snapshot](sources/issue-1098.json); [#1093 v1 contract snapshot](sources/issue-1093-contract-v1-proofed.md); [#1092 proof snapshot](sources/issue-1092.json); [#1093 proof snapshot](sources/issue-1093-proof-source.json)  
Source attribution: https://github.com/trickle-labs/pg-trickle/issues/1098 (snapshot `updatedAt` 2026-09-22T10:48:15Z); #1092 PROVEN comment https://github.com/trickle-labs/pg-trickle/issues/1092#issuecomment-5766551073; #1093 PROVEN comment https://github.com/trickle-labs/pg-trickle/issues/1093#issuecomment-5794107587  
Parent: [#1090 v1 exact snapshot](sources/issue-1090-parent-snapshot-v1.md)  
Parent snapshot: SHA-256 `c5645654a11745aee1b64549f457348a145cc9dd10d009b351c2ea89c256d982`; exact captured #1090 v1 block.  
Contribution: Refines Q1090-R01, Q1090-R03, and Q1090-R05 for selected unsafe boundaries and separately identified instrumented qualification. Q1090-R01 requires a valid baseline and a representative defect rejected for its intended reason; Q1090-R03 requires every bounded slice in relevant CI and release qualification before the parent is complete; Q1090-R05 limits public claims to actually qualified behavior and artifacts.  
Prerequisites: #1092's witnessed failure/cancellation controls are proven (local proof in [issue #1092 snapshot](sources/issue-1092.json), candidate `404d08fe1452350926e6bc481d68d92e28bdb73a`). #1093's current proof establishes separate reporting for instrumented results (local contract and proof in [#1093 v1 contract](sources/issue-1093-contract-v1-proofed.md) and [issue #1093 snapshot](sources/issue-1093-proof-source.json), candidate `6f6e58e90aba0e58c3a67944cae4ac8a373ff9a6`). #1093 remains the final release-qualification evidence path for Q1098-R09; this prerequisite proof does not itself qualify the #1098 slice.

Intended outcome: Exercise selected Rust/PostgreSQL unsafe boundaries under one supported assertion and sanitizer configuration. Reuse real workloads for tuple conversion with NULL and large values, PostgreSQL errors followed by continued use, cancellation during work, and worker cleanup or restart. Select the actual unsafe adapters reached by those workloads. Check role and search-path restoration independently of sanitizer output.

## Contract envelope

The bounded initial targets are `refresh::pipeline::copy_table_rows`, `api::security_context::with_stream_owner_context`, `api::security_context::with_caller_context`, and `scheduler::dispatch::pg_trickle_refresh_worker_main`, as reached by the named workloads. Initial configuration: Linux x86_64, PostgreSQL 18 with `--enable-cassert`, and AddressSanitizer instrumentation for the PostgreSQL process and Rust extension, in one dedicated Testcontainers image. First establish that it boots, reports assertions enabled, and detects a controlled invalid-memory fixture. If this configuration is infeasible, retain the concrete failure and select a supported replacement with the same detection/workload obligations before widening scope. This instrumented build is separate from the release package.

The existing security-context `#[pg_test]` cases do not run merely because an E2E target is selected. The live assertion oracle is `SHOW debug_assertions = on`. The contract's initial proposed implementation inputs are `tests/Dockerfile.e2e-asan`, `tests/e2e_unsafe_boundary_tests.rs`, PostgreSQL 18.6 source, a pinned nightly Rust toolchain, and `ASAN_OPTIONS=halt_on_error=1:abort_on_error=1:detect_leaks=1`; commands: `docker build --platform=linux/amd64 -f tests/Dockerfile.e2e-asan -t pg_trickle_e2e_asan:contract .` and `PGS_E2E_IMAGE=pg_trickle_e2e_asan:contract cargo test --test e2e_unsafe_boundary_tests --features pg18 -- --test-threads=1`. These inputs do not broaden the promised workloads.

## Acceptance matrix

| ID | Source | Requirement | Boundaries / counterexamples | Seam | Oracle | Planned evidence | Plan state |
|---|---|---|---|---|---|---|---|
| Q1098-R01 | [#1098](sources/issue-1098.json), What to build | The selected instrumented configuration boots, loads the extension, and has PostgreSQL assertions enabled. | Only PostgreSQL or only Rust is instrumented; assertions are off; or the sanitizer runtime cannot load. | Dedicated instrumented PostgreSQL Testcontainer | Live `SHOW debug_assertions` result and instrumentation of server and extension binaries | `test_instrumented_pg18_boot_and_assertions_enabled`: build/run the dedicated Linux x86_64 PG18 cassert/ASan container, retain build flags and runtime identity, verify ASan instrumentation in both binaries, and require the live setting to be `on`. | planned |
| Q1098-R02 | [#1098](sources/issue-1098.json), What to build / AC1 | A controlled invalid-memory fixture is detected by the selected configuration. | The probe does not execute or exits for an unrelated setup error. | Isolated instrumented invalid-memory fixture | Known fault site plus expected ASan diagnostic and unsuccessful probe execution | `test_instrumented_invalid_memory_probe_is_detected`: require the expected ASan error class and fault-site diagnostic in an isolated test-only probe; identify the probed binary and use R01 instrumentation evidence for both components or scoped probes for each. Keep probes out of shipped artifacts. | planned |
| Q1098-R03 | [#1098](sources/issue-1098.json), What to build | The actual tuple-copy adapter preserves rows containing NULL and large values through repeated work. | The workload bypasses the adapter or a lifetime error corrupts a later row. | Instrumented refresh/tuple-copy SQL | Independent public bag/schema result and clean assertion/sanitizer diagnostics | `test_pipeline_copy_null_and_toasted_rows_survive_refresh`: reach `copy_table_rows` with mixed NULL and toasted text values, perform insert/update/delete and repeated refresh, witness the adapter path, and require exact public bag/schema equality with no unexplained diagnostic. | planned |
| Q1098-R04 | [#1098](sources/issue-1098.json), What to build / AC2 | PostgreSQL ERROR inside owner context restores authorization state and permits continued use. | A role, `search_path`, or `row_security` value leaks after the error. | Instrumented owner-context SQL on one backend | Same-backend pre-error role, `search_path`, and `row_security` baseline | `test_owner_context_pg_error_restores_state_and_continues`: reach `with_stream_owner_context`, witness the expected ERROR, compare all three settings on the same backend, then verify an allowed and denied operation under restored permissions. | planned |
| Q1098-R05 | [#1098](sources/issue-1098.json), What to build / AC2 | PostgreSQL ERROR inside caller context restores authorization state and permits continued use. | Checking only `row_security` while role or `search_path` remains altered. | Instrumented caller-context SQL on one backend | Same-backend pre-error role, `search_path`, and `row_security` baseline | `test_caller_context_pg_error_restores_state_and_continues`: perform the R04 checks through `with_caller_context`, including successful continued use and the expected denied operation; explicitly reach this adapter because existing private pg_tests alone are insufficient. | planned |
| Q1098-R06 | [#1098](sources/issue-1098.json), What to build / AC1-2 | Cancellation of an active dispatched worker leaves recoverable state and permits later work. | Cancellation reaches an idle/completed backend; worker capacity remains occupied; or committed output is corrupted. | Instrumented dispatched worker and refresh SQL | Committed checkpoint and available worker capacity after cancellation | `test_dispatch_worker_cancel_cleanup_and_restart_recovers`: witness `pg_trickle_refresh_worker_main` doing the selected work, cancel and observe its error, inspect job/slot reconciliation, then require replacement work to finish and exact public results to recover. Reuse the proven #1092 witness controls. | planned |
| Q1098-R07 | [#1098](sources/issue-1098.json), AC1 | Selected instrumented workloads pass without unexplained assertion/sanitizer failures or blanket suppressions. | ASan reports an error while the runner exits successfully; workload diagnostics are suppressed; or logs are missing. | Instrumented workload run logs and exit status | Raw assertion/ASan diagnostics, with the expected isolated probe identified separately | `check_instrumented_diagnostics`: inspect retained raw logs and exits for selected workloads; reject unexplained reports, missing logs, or blanket suppression configuration; keep expected probe diagnostics separate from workload results. | planned |
| Q1098-R08 | [#1098](sources/issue-1098.json), AC3 | Changes touching selected unsafe adapters trigger their relevant checks, and the bounded slice runs in relevant CI. | A source change to pipeline, security context, or dispatch bypasses its instrumented checks. | Existing PR CI changed-path routing | Selected source paths and required case mapping | `check_unsafe_boundary_ci_routing`: at the existing CI contract entry point, verify a changed-file fixture for each named source path selects its instrumented cases and that a removed/disabled route fails. Run affected checks before shipping changes to these boundaries. | planned |
| Q1098-R09 | [#1098](sources/issue-1098.json), What to build / AC3 and [#1093 v1](sources/issue-1093-contract-v1-proofed.md), Q1093-R11 | Final release qualification executes the full bounded instrumented slice or explicitly remains incomplete; instrumented results are reported separately and cannot represent execution of the unmodified shipped library. | Boot/probe succeeds while a workload is missing; an unsupported configuration is silently replaced by ordinary smoke tests; or ASan payload is represented as exact-release runtime. | #1093 release qualification evidence | R01-R06 case inventory, separate instrumented build identity, and #1093 exact-release package identity | `check_unsafe_release_slice_complete` in #1093: require R01-R06 case identities and diagnostic validation in release qualification. If the initial build is infeasible, retain the concrete failure and a supported replacement with the same obligations before claiming acceptance. Preserve separate instrumented-build reporting per Q1093-R11. | planned |

## Unresolved gaps

- None. The initial image, probe, launch commands, and failure oracle are explicit; implementation must still build and run them.

## Open questions

- None. The initial target is Linux x86_64 PostgreSQL 18.6 with cassert and ASan; a replacement is required only if that configuration fails.

## Out of scope

- All-platform sanitizer support.
- Review of every unsafe block.
- Any workloads beyond the named bounded slice unless a supported replacement is needed to meet the same obligations.

## Advisory learnings

- None; `docs/retrospective-learnings.md` is absent.

## Change notes

- Initial contract, v1. Q1098-R01–Q1098-R09 preserve their existing meanings and IDs. Existing issue checkboxes are source criteria, not proof. The explicit v1 build inputs and commands clarify the existing agreement without changing its material promises.

## Readiness observations

- The captured #1098 issue has the `ready-for-agent` label, whose recorded meaning is “Acceptance contract is ready for unattended implementation.”
- The #1092 proof comment reports Q1092-R01–R11 proven, including witnessed cancellation and failure cases; it names candidate `404d08fe1452350926e6bc481d68d92e28bdb73a`.
- The current #1093 proof comment reports Q1093-R01–R14 proven and specifically Q1093-R11 proven; it binds to candidate `6f6e58e90aba0e58c3a67944cae4ac8a373ff9a6` and preserves the final qualification requirement for R09. The exact supplied v1 contract snapshot hashes to `7d6b90f3d204dd55b07450eb9c63f569472e2d8d46175d03c59c9e2387499744`, matching the hash in the proof comment.
- These observations establish the stated prerequisites from supplied local snapshots. #1093's final qualification outcome for this child remains pending implementation and release evidence.

## Implementation handoff

Implement the smallest complete solution inside the spec envelope. Preserve requirement IDs and promised outcomes. Capture the resulting candidate for separate review and proof.

## Proof handoff

Evaluate every requirement against v1 and one fixed candidate. Record actual evidence and verdicts in a separate proof report.

Next steps:

1. Hand the saved v1 contract to the authorized implementation workflow.
2. Include the full bounded slice in #1093 release qualification before claiming the release requirement complete.
