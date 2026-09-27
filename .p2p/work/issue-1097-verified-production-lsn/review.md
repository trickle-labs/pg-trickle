# REVIEWED: issue #1097 implementation

Contract: `work/issue-1097-verified-production-lsn.md`, v1, SHA-256 `dae07ec8c073477626add9b70a114891b5bbe7431e9790537c075bd731cc4259`.

Parent context: #1090 v1, snapshot `work/sources/issue-1090-parent-contract-v1.md`, SHA-256 `c5645654a11745aee1b64549f457348a145cc9dd10d009b351c2ea89c256d982`; contribution Q1090-R03/R05/R06. Bound prerequisite snapshots: #1091 SHA-256 `9e457f926f0593fc8960373c649607fe16cdd523411b2cefef83fa898ac44ef9`; #1093 SHA-256 `2610c30baa257b13b1884472d059349f719c531c2af186b696c4e5d12de5fa85`.

Candidate: `snapshot:sha256:361e2afe069e25ceb27988a01809dd9f840150e5e86307d48e422be6dce3bac9`; recoverable manifest in `.p2p/work/issue-1097-verified-production-lsn/candidate.json`, whose SHA-256 is `9251347e845a19b8b321a5d2ce2cc5dfb0c8386052cef06c75823ddf6e13272f`.

Comparison: direct-to-main route selected by the user. Base `2dfe8dae90452120cf6760b71dd0def8c241756b`, tree `f4b30ccf0b21eea9b38208ebcfb02f42a8bdf488`. Reviewed the full scoped candidate: 20 modified, 12 added, 0 deleted paths; `.p2p/` excluded.

Stability: Candidate key, contract hash, parent and prerequisite hashes, and comparison base were rechecked and match the captured records. Candidate validation succeeded; `approved-main` resolves to the stated base and tree.

Coverage: Q1097-R01–R11, including applicable Q1090-R03/R05/R06 constraints and the bound #1091/#1093 prerequisites. Examined the shared LSN implementation and specification, production callers, tests, CI jobs and scripts, candidate scope, and current candidate-matching proof/package/runtime records. No requirement was omitted.

## Contract fidelity

No material findings. The shared parser enforces the PostgreSQL LSN component grammar and `u64` bounds; numeric comparisons and formatting use the parsed value. The named production callers use the shared implementation, and invalid frontier data is rejected before progress or durable state changes. The compatibility case for unpadded persisted LSNs is covered.

## Scope and simplicity

No material findings. The shared parser, formatter, and comparison functions serve the promised callers. The added verification and candidate-binding steps are connected to CI proof and packaged-runtime obligations.

## Engineering quality

No material findings. The Verus source is imported by the Rust crate, the `unsafe` UTF-8 conversion documents its ASCII invariant, and tests cover malformed input, ordering, formatting, state preservation, and recovery. I independently checked the grammar and ordering expectations against the [PostgreSQL 18 `pg_lsn` documentation](https://www.postgresql.org/docs/18/datatype-pg-lsn.html) and [PostgreSQL 18 input implementation](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/adt/pg_lsn.c).

## Checks and limitations

Read-only actions: validated the candidate manifest and identities; checked the base ref/tree; inspected the implementation report as navigation, then verified its claims against source, callers, tests, CI, and scripts. Recomputed contract, parent, prerequisite, source, specification, and candidate-record hashes. Inspected the current top-level binding and installation records: the candidate package and four installed observations share SHA-256 `622105e834be9f7b6492aa2ae3eb7d78b1b1bf8c1c19388e0574142d0e616007`, and the runtime case IDs match the binding record. Older history records were not treated as current evidence.

The specification marks independent review pending; this report supplies that review for spec SHA-256 `d181e07582dfbc490ffa5e08fb329209217a898fd52c3e0d471986e38388382e` and source SHA-256 `42bbc75b3196415731a5388bc87ccbe50de37294ed442a61ab510aefc7492621`. No material discrepancy found. I did not run tests or proof in this review; the implementation report and separately supplied proof-context results are not represented as runs by this reviewer. No stage invocation/session identifier was exposed.

## Handoff

No finding IDs. This review does not declare acceptance proof or merge readiness. The enclosing workflow can save and reread this report at `.p2p/work/issue-1097-verified-production-lsn/review.md`; storage is pending.

Review only; acceptance proof and merge readiness are separate.

## Next steps

1. Save and reread this report at `.p2p/work/issue-1097-verified-production-lsn/review.md`.
2. If a matching proof report has not been saved, run `/prove work/issue-1097-verified-production-lsn.md; candidate snapshot:sha256:361e2afe069e25ceb27988a01809dd9f840150e5e86307d48e422be6dce3bac9`.
3. No PR exists; no further action is required unless publication is wanted. If publication is wanted, use `/publish-pr .p2p/work/issue-1097-verified-production-lsn/candidate.json; review .p2p/work/issue-1097-verified-production-lsn/review.md; proof .p2p/work/issue-1097-verified-production-lsn/proof.md; target main; draft only`.
