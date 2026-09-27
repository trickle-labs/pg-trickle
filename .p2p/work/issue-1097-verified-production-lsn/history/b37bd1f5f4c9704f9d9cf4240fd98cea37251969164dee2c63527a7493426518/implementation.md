# Implementation report: #1097

Contract: `work/issue-1097-verified-production-lsn.md`, revision v1, SHA-256 `dae07ec8c073477626add9b70a114891b5bbe7431e9790537c075bd731cc4259`
Candidate: `snapshot:sha256:3e8a030c39c44af4c090e4677ddcacd5a4659d422ff17a20e86edbb902df1018`
Comparison base: `6dc7e07c6bf53df2bf2327c4146cb140ca433f64`

## Implementation

- Added `verification/lsn.rs` as the checked parser and canonical formatter used by production code through `src/lib.rs`. Updated the named version, scheduler, recovery, refresh, CDC, and WAL LSN call sites to use the shared checked implementation or checked adapters.
- Added the pinned Verus verification runner, CI enforcement, proof-result checks, and candidate-binding checks for the executable source and installed package.
- Added Rust boundary/property coverage and `tests/e2e_lsn_contract_tests.rs` for PostgreSQL numeric ordering, predecessor spelling, and malformed frontier recovery/refresh behavior.

## Checks

- `just fmt`: passed. Evidence: `evidence/just-fmt-preserved-high-seq.log`.
- `just lint`: passed with zero warnings. Evidence: `evidence/just-lint-preserved-high-seq.log`.
- `just test-unit`: 2,590 passed. Evidence: `evidence/just-test-unit-escalated.log`.
- `just test-integration`: 161 passed. Evidence: `evidence/just-test-integration-final.log`.
- Pinned Verus run: 44 verified, 0 errors. The semantic arithmetic mutation failed verification as expected. Evidence: `evidence/lsn-verification.json`.
- Focused packaged light E2E against PostgreSQL 18.6: 4 passed, including `test_lsn_integrated_callers_preserve_valid_and_invalid_frontiers`. The installation attestation and candidate package/source binding match this candidate. Evidence: `evidence/just-e2e-lsn-contract-sanitized-candidate.log`, `evidence/lsn-install-attestation-candidate.json`, and `evidence/lsn-candidate-binding-sanitized.json`.
- Candidate-binding negative controls: 3 passed. Evidence: `evidence/lsn-candidate-binding-tests.log`.
- The full light-E2E allowlist was stopped after 1,047 of 1,448 tests completed (1,047 passed, 18 skipped); it is not a full-suite pass. Evidence: `evidence/just-test-light-e2e-final.log`.

## Remaining evidence gaps

The focused E2E covers public recovery and refresh, predecessor spelling, and PostgreSQL numeric ordering. It does not exercise malformed persisted progress through scheduler, CDC holdback, or WAL transition workflows required by Q1097-R06 and Q1097-R07. `e2e_wal_cdc_tests.rs` is disabled with `#![cfg(any())]`, and the light-E2E PostgreSQL container does not load the scheduler worker. These caller workflows remain unverified.

The initial candidate included test-generated `artifacts/dvm-fuzz/` outputs. The fixed candidate was rebuilt from the comparison base plus issue-owned changes only; its manifest excludes those outputs and validates in the isolated candidate checkout.
