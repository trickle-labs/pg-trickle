# IMPLEMENTED: issue #1097 — verify and integrate one production LSN implementation

Contract: `work/issue-1097-verified-production-lsn.md`, revision v1, SHA-256 `dae07ec8c073477626add9b70a114891b5bbe7431e9790537c075bd731cc4259`.

Parent context: #1090 v1, contract SHA-256 `c5645654a11745aee1b64549f457348a145cc9dd10d009b351c2ea89c256d982`, contribution Q1090-R03/R05/R06. Prerequisite snapshots for #1091 and #1093 are bound in `candidate.json`.

Delivery route: the user selected direct-to-main. Candidate comparison base: `2dfe8dae90452120cf6760b71dd0def8c241756b`, tree `f4b30ccf0b21eea9b38208ebcfb02f42a8bdf488`. The base tree archive, manifest, and raw commit object are saved under `.p2p/work/issue-1097-verified-production-lsn/evidence/` and were reconstructed and checked against the exact commit/tree hashes.

Candidate: `snapshot:sha256:361e2afe069e25ceb27988a01809dd9f840150e5e86307d48e422be6dce3bac9`. Candidate validation passed. Scope audit: 20 modified, 12 added, 0 deleted paths, from the comparison-base tree plus issue-owned changes; `.p2p/` and `artifacts/` are excluded. The full scope audit is `evidence/candidate-scope-2dfe8da.json`.

Implementation: a checked LSN parser, canonical formatter, and numeric comparison now serve the named version/frontier, scheduler, CDC, recovery, refresh, and WAL callers. The Verus source is executable proof code; CI checks required obligations and invokes the pinned image. The approved command change removed only unsupported `--verify`; the image digest and source path stayed unchanged.

## Requirement evidence

| ID | Implementation evidence |
|---|---|
| Q1097-R01 | Verus proved parser value and bounds; unit tests reject malformed and overflowing inputs. |
| Q1097-R02 | Verus proved parser/formatter round trips for all `u64`; boundary cases are covered. |
| Q1097-R03 | Verus proved numeric order; packaged E2E checks PostgreSQL `pg_lsn` semantics. |
| Q1097-R04 | Packaged integrated-caller E2E exercised a predecessor-style unpadded spelling derived from a committed WAL LSN and preserved its position. |
| Q1097-R05 | Named caller inventory and shared checked implementation are present; fresh full review is pending. |
| Q1097-R06 | Full E2E covered malformed-frontier preservation/recovery, CDC holdback recovery, and WAL transition rejection without partial results. |
| Q1097-R07 | `just fmt`, `just lint`, unit, integration, light LSN package E2E, and full LSN package E2E passed on this candidate. |
| Q1097-R08 | Pinned verifier: 44 verified, 0 errors; the executable semantic mutation was rejected. |
| Q1097-R09 | CI requires the proof job and obligation inventory; candidate-binding controls passed 4/4. |
| Q1097-R10 | Fresh independent review against this candidate is pending. |
| Q1097-R11 | Candidate binding passed: verified source SHA-256 `42bbc75b3196415731a5388bc87ccbe50de37294ed442a61ab510aefc7492621`, packaged library SHA-256 `622105e834be9f7b6492aa2ae3eb7d78b1b1bf8c1c19388e0574142d0e616007`, matching installed digests in four containers, and all four required runtime case IDs. |

## Checks

- `just fmt`: passed.
- `just lint`: passed with zero warnings.
- `just test-unit`: 2,593 passed.
- `just test-integration`: 161 passed.
- `python3 -m unittest scripts/test_lsn_candidate_binding.py`: 4 passed.
- Pinned Verus baseline: 44 verified, 0 errors; semantic mutation rejected.
- Packaged light LSN E2E: 4 passed.
- Packaged full LSN E2E: 7 passed, including all four required runtime cases.
- Candidate binding check: passed; package and installed library digests matched.
- Candidate and contract hashes were rechecked after these runs.

Detailed logs, verifier records, installation attestation, binding report, base reconstruction, and scope audit are in `.p2p/work/issue-1097-verified-production-lsn/evidence/`.

## Final handoff

Fresh independent contexts returned matching REVIEWED and PROVEN reports for this exact candidate. The review found no material findings; the proof covers all 11 requirements. Both reports bind the same contract, candidate key, and comparison base.

- Review: .p2p/work/issue-1097-verified-production-lsn/review.md, SHA-256 28b244d3164e867373a39f824a434dc9d902d4d87911dfc61a89dd912329bf5c.
- Proof: .p2p/work/issue-1097-verified-production-lsn/proof.md, SHA-256 88793c7f0e90d53bacc53cbaedc8955d26dde896ff0c3a1da3d442e2cc552bc3.
- Candidate: snapshot:sha256:361e2afe069e25ceb27988a01809dd9f840150e5e86307d48e422be6dce3bac9; comparison base 2dfe8dae90452120cf6760b71dd0def8c241756b.

No commit, push, PR, merge, or external tracker write has been performed.

Report storage: .p2p/work/issue-1097-verified-production-lsn/implementation.md.
