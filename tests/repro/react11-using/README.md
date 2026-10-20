# React #11 producer investigation

The original composed MDM source fails because pg-trickle's view-inlining
deparser discards `JoinExpr.usingClause`. A USING join has a null `quals`, so
the old code emits a bare JOIN without either USING or ON. This is a producer
defect on the documented nested-view source path, not an unsupported timestamp
alias or a React registration defect.

The public chain is `pgreact_api.author_deadline_rule` →
`pgreact_internal.create_m0_stream` → `pgtrickle.create_stream_table` →
`create_stream_table_impl` (`src/api/alter.rs`) →
`run_query_rewrite_pipeline` (`src/api/mod.rs`) → `rewrite_views_inline` →
`rewrite_views_inline_once` → `deparse_from_item_with_view_subs`
(`src/dvm/parser/rewrites.rs`). The next inlining iteration calls
`parse_first_select` → `parse_query` → PostgreSQL `raw_parser`.

`generated-invalid.sql` is the actual SQL captured immediately before that
parser call using a separate diagnostic build of the pinned producer, with
only a `pgrx::warning!` added at `parse_first_select`. It ends in:

```sql
... AS policy_case JOIN s1_audit.source selected_case) AS candidate
```

Re-executing these exact bytes in PostgreSQL 18.6 produces SQLSTATE **42601**,
cursor position **1346** (one-based), the closing parenthesis after
`selected_case`. `postgres-error.jsonl` is PostgreSQL's JSON error record.
The diagnostic library was never used for qualification.

## Inputs and reductions

- Original producer: `d8bb8eb5d45c5c9982e173e88914037433a1ee05`, version 0.108.3.
- React fixtures: `9fb04854e741535c3c41907b6be4318a1ccf6939`, PR
  [pg-react#14](https://github.com/trickle-labs/pg-react/pull/14).
- MDM: 0.14.1, `06471e647ee54a80973fb5ad6709830f9955dbff`.
- Runtime: PostgreSQL 18.6, Debian `18.6-1.pgdg13+2`, Linux ARM64.
- Original joint image:
  `sha256:3cdd0ae0ac2b15f42cabdfc08c41b758360807148f6a5399f05b9bff4066453a`.
- Profile: scheduler off, trigger CDC, change ratio 1, read committed,
  `shared_preload_libraries=pg_trickle,pg_react`.

The baseline audit reaches the unchanged `time/idle.sql` public call and fails.
`baseline.json` preserves the full before/after registration vectors: cases,
receipts, business and technical sources remain equal, and rules/work/attempts
remain empty. This reproduces the original stop without weakening acceptance.

`mdm-reductions-baseline.json` records separate transactions against the real
MDM projection. Direct projection, a simple ON join, and an aliased due timestamp
all create streams with exact EXCEPT ALL results. The simple USING join fails
at end of input; the original composed view fails near `)`. These producer-only
reductions use postgres; the public registration audit uses its original roles.

## Correction and regression

The patch preserves quoted USING keys in both raw SQL deparsers and resolves
their equality predicates in the differential operator tree. Preserving only
the SQL would leave the old parser treating USING as `ON TRUE` during refresh.
Nested input resolution is conservative: a key present in more than one input
of an already nested join is rejected with an explicit error. General merged
USING-column provenance is outside this correction.

`../../fixtures/view_using.sql` reproduces the composed due-date shape without
MDM dependencies. The old binary fails at creation. The corrected binary must
preserve exact rows through key mismatches, NULL deadlines, INSERT/UPDATE/DELETE,
refresh rollback, a subsequent committed refresh, and creation rollback. Its
worker has only exact-target MAINTAIN and no source SELECT/schema USAGE; stream
ownership remains with the creator. The Testcontainers entry is
`test_view_inline_using_preserves_results_privileges_and_rollback` in
`tests/e2e_view_tests.rs`. The adjacent test verifies quoted keys and exact
inner/left/right/full join results before and after refresh.

## Qualified pinned artifact

The pinned-source artifact is distinct from the separate current-main build:

- Local image: `pgtrickle-react11:pinned-qualified`.
- Immutable image ID:
  `sha256:bc47ac97146e486b2461e9487287b1afc144f9b5387983fa1454e961dd111931`.
- Producer library SHA-256:
  `1b5004746844e08ad7cdb2360a45651efb62866529b658c48acc8fb9c94a4515`.
- Corrected source archive SHA-256:
  `47558b48443c78336c0e00994f2e31b33b995382418fc1d5ceb79dab955e89de`.
- Exact correction-only `producer.patch` SHA-256:
  `1ef4bd312ea71860076f61f865a107dd4a2ee4c6dd9e43bdaf9919a318c8056f`.
- Builder image:
  `sha256:de453c245823bdb68c341e5b648a08f008e7cf67a336615a753baadd8c1f9150`
  (Rust 1.98.0, pgrx 0.18.0, PostgreSQL 18.4 headers; runtime is 18.6).
- React and MDM libraries retain the original manifest hashes; no consumer
  codec, grant, source, stream owner, or acceptance assertion was changed.

The local source archive and binary are also saved under
`target/react11-qualification/` (not committed). This is a locally qualified
candidate, not a published producer release. Version 0.108.3 alone cannot
distinguish it from the defective original artifact.

The final pinned audit exits 0 and records an actual `APPLIED_CONTROL` escalation
receipt **0.706636 seconds** after the idle deadline, including callback-error,
role-reset, missing-SET-membership, complete case/source vector, and unchanged
privilege assertions. `pinned-qualified.json` contains the full observations.
The new source-archive identity is used by MDM bootstrap in this run. An earlier
implementation-identical binary run delivered after 0.559336 seconds; it is not
substituted for the final artifact's qualification.

## Current-main assessment

Current main was separately fetched and rechecked at
`0cbd84dce4fede39fa36f2c8716e0faa0a9b1b14`. Its two SQL deparsers and USING
parser behavior are identical to the pinned producer before this correction.
The fix was first committed on the pin as local commit
`29518c8d973bce4c7e6567814b93abbf37d360c7`, then replayed as
`7e1700959b692f78579a41e8f10b579696db76ca` onto main for the PR. Its runtime
source exactly matches the separately built main-plus-fix source.

The separate full-package main image is `pgtrickle-react11:main-candidate`, ID
`sha256:615e19e31412c221602b6aeda43f2726d7f0ebb33dcc61bc05179ad141803909`,
with library SHA-256
`59916f095b551e4dd41ddada619a53092b866dc375527dd1fe5945b11505275d`.
It passes the 21 view-suite and 16 MAINTAIN-suite checks, including all new
regressions. Final main-tree format/lint and 2615 unit tests pass; the pinned
tree had 2611 passing unit tests. Logs are retained for both scopes.

**Full MDM compatibility on main is not qualified.** Two unchanged full-stack
audit attempts terminate during `/tests/e2e.sql:3544`, at
`mdm.refresh('composite_customer', 'ALLOW')`, before the idle fixture. Both
record PostgreSQL backend signal 9 and Docker `OOMKilled: true` on the local
7.748 GiB Docker VM. The second attempt ran after this investigation's builds,
other audit, and E2E containers stopped. Other host work was not controlled.
No causal claim about this memory failure or pg-trickle #1168 is made.
`main-first-oom.json`, `main-isolated.json`, and their Docker state/server
records preserve the failed attempts. No replacement acceptance or lower
assertion is used to turn them green.

The main image is only a compatibility diagnostic, not an admitted artifact:
its inherited archive label still names the original runtime input. Its actual
main-plus-fix build-input archive hash is
`059378a82b6ba9bdf73979c3d90ecc0e01232f62d6f38264c0ccc80d035c61a0`.
It must not be substituted for the separately qualified pinned image.

## Reproduction

Use the immutable original joint image and the pinned React checkout. Run its
unchanged audit:

```sh
DOCKER_DEFAULT_PLATFORM=linux/arm64 python3 \
  tests/integrations/pg-mdm/time/audit.py pgreact:issue11-compatibility OUTPUT
```

It must exit 1 with the recorded public registration failure. To replay the
correction from its exact original producer input:

```sh
git worktree add --detach /tmp/producer-react11 d8bb8eb5d45c5c9982e173e88914037433a1ee05
git -C /tmp/producer-react11 apply /absolute/path/to/producer.patch
docker build --platform linux/arm64 -f /absolute/path/to/Dockerfile \
  -t pgtrickle-react11:pinned-qualified /tmp/producer-react11
```

The recorded Dockerfile uses the local original runtime and builder images and
warm offline Cargo caches. Its source context includes the tracked Cargo.lock
and benches, which `git archive` normally excludes. The source archive above
contains all six build inputs: Cargo.toml, Cargo.lock, control file, src,
verification, and benches. Verify their hashes before substituting any input.

Run the same unchanged React audit against the corrected image. It must reach
an actual managed idle escalation receipt and retain all of its vector and
privilege assertions. In this investigation the only audit-runner adaptations
were an explicit ARM64/pull-never Docker selection, verbose server logging,
keeping disposable containers for inspection, and reporting the pinned React
revision for a `git archive` extraction. SQL fixtures and assertions were
byte-for-byte those of `9fb0485`; their hashes are in the observations.

Producer regression command:

```sh
DOCKER_DEFAULT_PLATFORM=linux/arm64 PGS_E2E_PLATFORM=linux/arm64 \
PGS_E2E_IMAGE=pgtrickle-react11:pinned-qualified cargo test \
  --test e2e_view_tests --test e2e_v1150_maintain_grantee_refresh_tests \
  -- --test-threads=1
```

## Integration requirements and remaining boundaries

MDM must qualify the corrected source archive/library as a new producer
artifact and update its exact artifact provenance, retaining its qualified
version, capability, CDC, scheduler, isolation, and output-delta checks. Do not
reuse the old `b8d4c59c…` archive identity for this corrected source.

React must keep the original MDM projection and its case-selection join,
stream ownership, exact-target MAINTAIN grants, codecs, and acceptance
assertions. Its version admission does not replace artifact qualification.
A published version would need the corresponding exact version admission and
the same full audit rerun; this task does not publish or activate a release.

A successful real idle receipt clears this source-creation gate only.
React #11 remains open for its remaining temporal, bounded progression,
recovery/concurrency, 1000-case/churn, and signed operating-envelope acceptance.
React #12 release qualification and pg-trickle #1168 remain independent.
