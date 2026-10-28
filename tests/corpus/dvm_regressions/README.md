# DVM regression corpus

These scenarios are versioned, standalone reproductions. The JSON file is
canonical; replay does not invoke the query or mutation generator.

| Scenario | Origin | Regression covered |
| --- | --- | --- |
| cor938_physical_width.json | #938/#939 | Aggregate rescan must use the logical projection, not the physical table width. |
| cor939_two_leaf_snapshot.json | #939 | Simultaneous aggregate-leaf changes must use a consistent old-state snapshot. |

Run the corpus with just dvm-corpus, or replay one case with:

    just dvm-replay tests/corpus/dvm_regressions/cor939_two_leaf_snapshot.json

## PostgreSQL semantic boundaries (#1217)

`e2e/dvm_fuzz/semantic.rs` extends the existing versioned scenario generator;
failures use the same JSON, SQL, row/schema diff and PostgreSQL-log artifacts.
The PR corpus target selects 34 cases. The existing nightly/weekly DVM job
sets `DVM_SEMANTIC_MAX_BAG_SIZE=3` to select 54. The published floor is in
`dvm_coverage_requirements.json`; an empty, unsupported, skipped or fallback
selection cannot satisfy it. `semantic-observations.json` lists only cases
which finish every exact comparison and fresh differential-history check.

The small domain exhausts all 15 unordered bags of size <=2 over NULL, -1, 0
and 1, including duplicates. Scheduled runs exhaust the 35 bags of size <=3.
Further cases cover nine PostgreSQL-observed HAVING truth transitions,
COUNT(argument) versus COUNT(record), scale and numeric special values,
row-dependent division protected by FILTER/CASE, CTE/projection/join/aggregate
nesting, endpoint-valid joins with invalid mixed pairs, deferred-key same-key
replacement, repeated updates, explicit rollback/savepoint rollback, and both
narrow/all-column consumer registration orders with the other consumer first
to refresh. Numeric scale is an explicit public column, so SQL equality cannot
hide it. The shared SQLancer literal domain also supplies NULL, signed and
integer boundary values, scale variants and special numeric values to initial
inserts and subsequent INSERT/UPDATE generation.

`preserve_sql_boundaries` tells the existing shrinker to remove complete cycles,
complete mutation scripts and initial row tuples, while retaining SQL types,
expressions, numeric representations and transaction statements. Failure replay
must retain its original detector and phase. Native truth assertions prevent
shrinking a truth-transition case into a different transition. No arbitrary SQL
text simplification is claimed safe for these histories.

The exact independently evaluated defining query is primary. The ternary
partition additionally preserves every bag row through IS TRUE / IS FALSE /
IS UNKNOWN. A PostgreSQL component test confirms that equivalence with NULLs
and duplicates, and rejects a control omitting UNKNOWN. It observes distinct
Hashed and Sorted aggregate plans and compares their exact results. These
transformations use only deterministic, total numeric comparisons and COUNT/
SUM on a bounded finite domain; no row order, arbitrary throwing expression,
volatile evaluation or floating point reassociation is assumed equivalent.

Existing permanent regressions remain owned by their repair work:

| Semantic class | Permanent regression / owner | Observed baseline in this stack |
| --- | --- | --- |
| Composite datum NULL | #1203 / PR #1212, COUNT composite installed regressions | All 15 generated count bags fail after differential refresh: record counts undercount NULL-containing records. |
| FILTER evaluation | #1204 / PR #1214, protected argument installed regressions | FILTER-protected division fails during differential refresh with division by zero. |
| HAVING UNKNOWN | #1205 / PR #1228, `test_having_all_nine_truth_transitions_match_postgresql` | All nine generated transitions pass, alongside the existing four-family installed regression. |
| Numeric special arithmetic | #1209, generated `special_sum` | Deleting Infinity yields NaN SUM/AVG instead of the remaining finite result. |
| Numeric observation / consumers | #1210, generated `all_first` / `narrow_first` | Scale remains 1 instead of 2 in both registration orders after the all-column consumer refreshes. |
| Mixed snapshot evaluation | #1211, generated `mixed_join` | Valid endpoint queries fail with division by zero during differential maintenance. |

The expanded baseline was executed against the unchanged production package
from `77a0dda5425bd20ffbdd834623c03f6c55453930`, PostgreSQL 18.6 amd64,
with no retries. Ten of 34 cases passed: the nine truth transitions and ternary
partition. The other 24 failed. Additional counterexamples expose a stale
Infinity-to-negative-Infinity projection, a CASE SUM remaining NULL after a
new non-NULL argument, lost deferred-key replacement, and a nested-count query
referencing a missing `__pgt_key_changed` column. These are preserved failures,
not successful coverage or repaired behavior. The owning CDC/join work must
resolve them before this semantic floor can pass.

The scheduled 54-case baseline was also executed against that same production
package: 10 passed and 44 failed, with zero retries. This establishes observed
scheduled coverage without treating the larger red selection as qualification.

After the CDC repairs at `db909f1b0a97f06b999a68a478258aa21e2e7e73`, the
34-case floor reports 16 passes and 18 failures. Both consumer orders, numeric
projection, CASE, deferred-key replacement and nested-count metadata now pass.
The remaining failures are all 15 composite COUNT bags, FILTER-protected
division, special-value SUM/AVG and mixed-snapshot division. The floor remains
red, with no retries or substituted FULL refreshes. Its local report and full
failure artifacts are under
`.p2p/work/issue-1217-semantic-generators/cdc-deferred-frozen/`.

Local commands (Docker/Testcontainers required):

```sh
bash scripts/run_e2e_tests.sh --test e2e_dvm_corpus_tests --retries 0 --test-threads 1 --no-fail-fast
DVM_SEMANTIC_MAX_BAG_SIZE=3 bash scripts/run_e2e_tests.sh --test e2e_dvm_corpus_tests --retries 0 --test-threads 1 --no-fail-fast
```

This keeps the existing corpus target in the bounded PR selection and extends
the existing DVM scheduled job; it adds no tier and removes no checks. This is
the allocation relevant to #1188, without claiming runner-time savings or
altering required-check policy. The existing DVM workflow owns scheduled
failures and uploads the same failure artifacts.
