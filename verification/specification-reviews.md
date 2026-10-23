# Independent bounded specification reviews

The user authorized independent read-only agent reviews on 2026-10-22.
Reviewers inspected frozen source, production callers and stored evidence;
they did not execute project code, tests, builds or proofs. These are bounded
specification reviews, not merge approval or whole-extension verification.

## Visibility and aggregate lifecycle (#1220)

Reviewer `/root/review_1220` inspected
`77a0dda5425bd20ffbdd834623c03f6c55453930` and found no blocking issues.
The independent visible-bag, grouped/global presence and argument-NULL
contracts are adequate and nonvacuous. Production imports the checked code;
SQL callers use total truth tests, complete identity guards, actual new group
counts and separate non-NULL argument counts. Recorded proof/caller hashes
match the frozen source. Baseline 6/0, five intended postcondition rejections
and both 12/0 runtime reports were inspected, not independently reproduced.

SQL lowering, count arithmetic, expression handling, rescan state, capture,
transactions and FFI remain outside the finite proof.

## Receipt acknowledgement arithmetic (#1222)

Reviewer `/root/review_1222` inspected
`c06b71252c86aa6e0b1da2e68927280cb9e5de65` and accepted the bounded count
contract. It has no unchecked preconditions; the exact positive i32 limit is
used by the real acknowledgement caller. PostgreSQL 18's implementation
independently supports the inclusive stopping condition. Recorded source,
caller and log hashes match; the n+1 executable mutant fails its postcondition.

Framing, recoverable prefixes, cleanup, incarnation, reset/restore and crash
durability remain unproved. The long-open-transaction receipt/catalog WAL
growth and resource failure remains unresolved. The duplicate-result fixture
is an oracle control, not an executable duplicate-replay mutation.

## CDC changed-column elision (#1221)

Reviewer `/root/review_1221` accepted the first R3 slice at
`3184b55e0ee74d82f55203bf3bd744ab9a675f1e`, with no semantic blocker.
Exact built-in OID guards enforce the small image policy; both trigger builders
use the checked decisions and preserve complete transactional D/I capture.
Independent native conformance covers 186 pairs plus scale and storage
countercontrols. The pinned baseline reports 2/0; both executable mutants
report 1/1 intended postcondition failures. Complete shared runtime reports
16/16, and the frozen named case reports one pass with 15 filtered cases.

Follow-up inspection confirms the declared frozen commit and identical
package/attested/installed library SHA-256
`6bde1e1a6c6cb9f4881a51d2bbf6411a17bf8ac2c022f75f7d13c64f178d6920`.
R1/R2 normalization, compaction/windows, arbitrary observation equivalence,
SQL/FFI and the formal correctness of the separate cache repair remain outside
this review. A domain over an admitted builtin would strengthen domain
conformance; its absence is nonblocking for the exact-OID implementation.

The expanded candidate `87cef53357918e3abb267eda44f2f324308afe4c`
received a separate review of the deferred history, keyed image adapter and
statement capture paths. The three unconditional decision contracts were
accepted; recorded evidence reports 4/0 and four intended 3/1 mutant failures.
The reviewer found an ordinary SQL blocker: underscore-only alias growth can
cross PostgreSQL's 63-byte identifier limit and collide after truncation.
A native regression reproduced SQLSTATE 42702 using valid maximum-length
payload names. Follow-up review at
`db909f1b0a97f06b999a68a478258aa21e2e7e73` accepted the short numbered alias
repair and its reproducing/passing native control. The source and proof hashes
still match, with 4/0 and four intended 3/1 mutant failures. Final follow-up
inspected 62 passing tests, zero skips/retries and all three required CDC cases.
All 49 installation observations declare that frozen commit and agree with
the package library SHA-256
`e0e4bff4237ed1fdaae7d49e7ada2d39ac87215cc96ec16fd325eb1d84edd99d`.
The independent semantic floor remains 16/34 passing and 18 failing; no broader
correctness conclusion follows. This review does not establish full
R1/R2 normalization/window correctness or persistent compaction.

## Join endpoints — #1223

The user authorized an independent read-only specification and caller review.
Reviewer `review_1220` inspected the signed pointwise semantics, unconditional
checker, occurrence/sign/boundary bindings, typed single-key USING parser path,
affected-key scope, concrete cache bypasses, source SHARE locks and cut checks,
and READ COMMITTED isolation requirement. The reviewer accepted IMMUTABLE as
a fixed determinism assumption without treating it as nonthrowing.

The initial review found an ordinary SQL blocker: unqualified internal bytea
equality could resolve through the owner's stored search path to a user
operator. A native regression reproduced extra D/I actions for an unchanged
identity. Final follow-up at executable candidate
`d84139858410627102795f493e6436c2ac943eef` accepted explicit
`OPERATOR(pg_catalog.=)` and the shadow-operator regression. No remaining
blocker was found in the bounded root computed two-scan inner-join slice.

The reviewer independently compared all 22 proof/caller/build-input hashes,
the 6/0 proof and five intended postcondition failures, and proof/native-control
log hashes. The native missing-retraction mutant fails its exact-action
comparison; the repaired native component suite passes 25/25. Both required
installed cases pass, with 25 unrelated cases filtered out. Both installation
observations, the actual extracted package and candidate binding agree on
library SHA-256
`45c3f93ada2907f527b21a4347c2230e5fc8ee11f43442dbab485e0f59e44c21`.
The reviewer executed no project code or proof. Broader running suites were
not accepted as completed evidence. This review does not establish completion
of all #1223 requirements, nested/legacy correction branches, SQL/FFI,
capture/window/identity protocols or transaction atomicity, and authorizes no
merge. Build provenance and successful named execution remain runner inputs.

## Scalar identity framing — #1224

User-authorized independent read-only reviewer `review_1222` accepted the
one-field grammar, unconditional encoder/validator iff contracts, unique
interpretation lemma and separate checked-capacity arithmetic. The reviewer
found no unchecked assumptions/external bodies or bounded theorem/caller
blocker. Ordinary pure and SQL record adapters reach the same checked encoder,
admitted payload failure cannot enter the ordinary path, and the V2 validator
retains its version gate. Named MERGE and keyed anti-join consumers compare
complete identities after probe equality.

The review found an existing malformed test vector: `scan_int4_minus_one`
omitted its separate ff terminator, while the old corpus test checked only
an ff suffix and repeated the malformed literal. Full V2 fixture validation
reproduced the failure. The corrected vector now matches the actual encoder
and the already-passing immutable predecessor installed case. The specification
also now correctly describes malformed bool validation as new and width
validation as retained. These repairs change test evidence, not valid bytes.

Independent follow-up reviewer `review_1220` accepted the fixture/doc repair
and final executable candidate `d2d44f0b30d41f99ee5dd4c3b0451dda460cac20`.
Kernel and all 13 recorded source/input hashes match that commit. Proof/log
hashes agree: 6/0, with five intended postcondition mutant failures (5/1 each).
The final installed suite passes 17/17, including both required cases, and
connected join/shared tests pass 31/31, without retries or skips. The actual
package, all 14 declared-commit installation observations and candidate binding
agree on library SHA-256
`1ae5694e1aac804fc557623531f2d6f1dc979a5cb572be99a0bc41f766e5d1c0`.
No bounded blocker remains in this reviewed slice.

Neither reviewer executed project code or proofs. This review does not approve
merge, authenticate build provenance, or prove multi-field framing, scalar
canonicalization, SQL/FFI behavior, transaction protocols or hash uniqueness.
The build/test runner remains trusted for compilation origin and named success.

## Non-finite aggregate runtime follow-up — #1217 / #1220 boundary

Independent read-only reviewer `review_1220` reviewed the supplemental
[runtime specification](nonfinite-aggregate-state.md) and production callers.
The initial frozen `e8a9f3b` review found two gaps: inverse finite updates
could cancel before the regression observed their state, and a raw domain
initializer could disagree with an action-guarded CASE's base-type admission.
The strengthened history removes Infinity immediately after finite 1-to-4,
and the shared state renderer now gives initialization the same CASE coercion.
The native check compares both rendered states and exact finite/special values.

The reviewer accepted both fixes at
`1a717660030487b1d5e63f683c86e40f528b8fe6`, with no remaining bounded
source/specification blocker. The #1220 arithmetic-proof exclusion remains
explicit. No reviewer executed code, authenticated compilation provenance,
approved a merge, or qualified arbitrary floating-point regrouping/rounding,
finite overflow or the entire quality program. Final installed evidence review
is separate from this source acceptance.

The same reviewer subsequently accepted the completed bounded nonfinite
evidence: strengthened history 1/1, connected installed selection 47/47 with
71 unrelated cases filtered, observed floors 34/34 and 54/54, and units
2633/2633. The actual package, first 44 installation observations and five
completed bindings agree on candidate and SHA-256
`32b7d5f1cd53c5115581ddf68da3e043f1a1acb7252d7bf24db2312c02731259`.
The reviewer checked six baseline/mutant records against their actual source
inputs and log hashes. The later LSN suite (11/11), eight additional installation
observations and sixth binding were completed by the primary runner after that
review; they are not silently included in the reviewer's accepted evidence.

## Supplemental semantic oracle controls — #1218

Independent read-only reviewer `review_1220` inspected supplemental specification
`semantic-oracle-controls.md` and frozen source
`503eb546dbd383d114b0faf7f531a954992951a0`. The four actual-adapter SQL
mutants require nonempty, correct baselines and successfully executed wrong
results. The checked slot bound n=4 yields the represented transaction; the
executable n+1=5 mutation returns seven messages including later source changes.
Numeric scale stays attached to complete rows and duplicate multiplicities;
the CLI control requires the exact class, phase, cycle and mutation coordinate.

The reviewer found that Python admission could count private numeric columns
explicitly listed by a custom scenario, contrary to Rust and the specification.
Follow-up at `b3d41cb67aeafc5d531e9f10f448b5e909255435` accepted explicit
`__pgt_` exclusion and the private-only regression. Final corrected live Python
replay passed all three histories and detected only the intended
`MultisetMismatch / scale_corruption exact comparison / cycle 0 / mutation null`.

Final bounded evidence was independently inspected and accepted: 27 native
tests, 22 installed selections (58 unrelated filtered), floors 34/34 and 54/54,
and 62 Python checks. The 13 frozen source hashes, seven complete log hashes
and 11 installation observations agree with the manifest and actual package.
Native Rust inputs are identical between compiled `503eb546` and reviewed
`b3d41cb`; installed backend remains `1a717660`, library SHA-256
`32b7d5f1cd53c5115581ddf68da3e043f1a1acb7252d7bf24db2312c02731259`.

No remaining bounded blocker was found. The reviewer executed no project code.
This accepts the listed observation/control boundaries, not whole-program
correctness, authenticated compilation provenance or merge approval.

## Recovery histories and snapshot/restore follow-up — #1219

Independent read-only reviewer `review_1221` inspected the original bounded
histories at `fc23780c`. The journal, rollback/cancellation, restart identity
and actual trigger witnesses were adequate, but review found a CI blocker:
tee could hide failing recovery commands without explicit Bash/pipefail.
Terminal phases also lacked explicit frontier coverage and precrash checkpoint
equality. Both gaps are repaired and enforced by the follow-up.

Source review accepted frozen backend
`0ab22465b99f5e4dde2cf595f5b5d50216b20209`: catalog-row locking/reload pairs
snapshot metadata with copied rows; sorted source locks serialize protected-cut
inspection and restore with both cleanup minima; unprovable history requires
reinitialization while a lagging consumer retains differential reuse. The
conservative READ COMMITTED / TABLE admission and self-equal-cut restriction
match the specification. No source-level blocker remained.

The test-only freeze `efae4dccbddd43b357fee1fc3017a026febeffe8` adds an
actual uncommitted restore versus sibling FULL-cleanup schedule. All three
predecessor failures are retained. Final repaired artifacts show coherent
snapshot rows/frontier, missing-history REINITIALIZE and 2 history rows during
restore, 2 after rewind, then 0 after catch-up.

The reviewer accepted completed bounded qualification: initial 10/10 (four
recovery cases, three security scenarios and three helpers), ordering 1/1,
scheduled 5/5 including ten seeds/twelve cycles, fourteen required proof-runtime
cases, 2633 units, 62 Python checks and semantic floors 34/34 and 54/54. All
seven frozen input and fourteen log hashes match. The actual package, all 40
installation observations and six regenerated bindings agree on backend and
library SHA-256
`4a1afb720c7d88d6efe27a2a24c832b0797fbf20db52314e233363d99bf38983`.
Existing six proofs/mutants retain their bounded claims; their source/caller
hashes match. No reviewer edits, builds, tests or proof executions occurred.

This accepts the observed repair, not complete recovery/cleanup/incarnation
proofs, untested schedules, remote CI success, authenticated compilation
provenance or merge approval.

## Receipt feedback follow-up — #1222

Independent read-only reviewer `review_1220` accepted the source-relative
contract and implementation at `1bcd6a8ea39a3c9ad7ab957bf1a0793fa77985f6`.
Complete recognized ordinary-DML transactions may bypass persistence only when
all rows are unrelated to the current source. Source, unrecognized and
TRUNCATE work retains the complete transaction. Exact legacy APPLIED matching
precedes skipping; checked raw-prefix counting and GET comparison remain intact.

The initial whitelist implementation `0e662c12` failed installed qualification
because replay mixes change-buffer INSERTs and receipt status UPDATEs. Both
that failure and the predecessor's 81 nested receipts remain retained, along
with the diagnosed decoded transaction. Source review did not override this
failure; the revised implementation required a new package and qualification.

Final evidence was independently inspected and accepted: WAL binary 24/24
(nineteen scenarios and five helpers), additional core 14/14, semantic floors
34/34 and 54/54, 2636 units and 26 relevant Python checks. Six source cycles
acknowledge actual source work with zero nested receipts, four additional
receipts per cycle and maximum payload 87 bytes. All 42 installation observations
and six bindings match the actual three-file package, library SHA-256
`d3f7c4709139ac8b40117c7e8faa8846d9b78542f82e3c90f3876ebee80737d4`.
Receipt Verus baseline is 2/0 with four intended mutant postcondition failures;
the other five proof records retain matching source/caller hashes. All fourteen
complete log hashes match. The manifest separates eleven backend-frozen inputs
from revised documentation instead of labeling documentation edits as frozen
backend inputs. No runtime retries were used.

The reviewer executed no tests, builds or proofs. This accepts the bounded
repair and observed evidence. Complete source/partition classification,
prefix-loop correctness, incarnation, cleanup, durability and full recovery
proofs remain outside the formal claim. Build/named-execution provenance remains
trusted runner input; this grants no merge approval.

## Initial registration snapshot follow-up — #1219 / #1222

Independent read-only reviewer `review_1221` identified and accepted a repair
for initialized creation under an already-pinned fixed snapshot. Reproducer
`e0b6a2e4` against backend `1bcd6a8e` successfully created/refreshed a consumer
with one row instead of two committed journal rows, while publishing a cut
beyond the missing writer change. The complete failing observation is retained.

Review moved isolation admission from the initializer to shared creation before
storage/CDC setup: legacy WAL reconciliation can drop slots that rollback cannot
restore. Final source `49a8ed9ed4e3ce67251296b41c74e8b000597c28` rejects
populated creation under REPEATABLE READ/SERIALIZABLE before these effects.
Deferred/external creation keeps its frontierless behavior. Review also required
fresh DIFFERENTIAL execution and writer-cut coverage instead of trusting the
requested strategy alone.

Final bounded evidence was accepted: 26/26 WAL-binary tests, fourteen core cases,
2636 units, 26 Python checks and semantic floors 34/34 and 54/54. Both isolation
rejections preserve the existing WAL slot/mode and permit later real source
acknowledgement. READ COMMITTED retry, differential catch-up and deferred
initialization across another commit match the independent journal.
All 44 actual three-file package observations and six bindings agree on library
SHA-256 `fe054e9f9cf89202c82a417fc79a866031b70de6f69e73586943dfa9f4d61670`.
Five backend-frozen input hashes, separately qualified documentation and thirteen
complete log hashes match. The initial daemon-EOF build failure and successful
same-source retry remain retained; no runtime test was retried.

The reviewer ran no project code. Existing six proof source/caller hashes match;
retaining those records does not imply new proof execution or a new theorem.
Previously stale consumers need FULL refresh/reinitialization under READ
COMMITTED. Full cleanup/incarnation/protocol correctness, authenticated build
provenance, latest-head remote CI and merge approval remain outside acceptance.

## Local WAL slot retirement (#1222 follow-up)

Two independent read-only reviewers examined the bounded specification and
production paths at frozen backend `f0a8c6439fb11dbc71aefc67f6675a95732e2cd5`.
Review found and required fixes for optional SPI lookups, physical-slot NULL
classification, references after source tracking deletion, legacy NULL slot
names, and stale qualification commands. A later source-name-reuse review
separated two facts: observed intent reassignment did not prevent old-consumer
cleanup, while a direct seeded cross-OID worker history did actually remove a
still-referenced slot. The unsupported cleanup/output-corruption prediction
was withdrawn. The final reservation check is conservative admission; durable
stable-name resolution independently repairs the global reference guard.

The direct positive history witnesses the worker Keep barrier with slot and
intent preserved, then requires actual retirement after removing the old
consumer. Source replacement separately witnesses conflict rejection, unchanged
source/owner tuple, successful old-consumer cleanup, actual retirement, retry,
fresh WAL acknowledgement and exact downstream results.

Completed bounded evidence was accepted for publication: 35/35 WAL-binary
tests, 15/15 additional core cases, semantic floors 34/34 and 54/54, 2637 units,
64 Python checks, formatting/lint without warnings, release-contract and
upgrade-completeness checks. The actual published predecessor upgrade preserves
an observed TRANSITIONING slot; completed predecessor WAL handoff remains
unqualified, with its earlier failed setup retained separately.

The proof reviewer reconstructed all 27 executable mutants and checked their
source/output hashes and intended postcondition failures. Six reexecuted
baselines pass: LSN 44/0, receipt 5/0, lifecycle 6/0, CDC 4/0, join 6/0 and
identity 6/0. Receipt declares three functional obligations; the retirement
theorem covers the shared finite gate, not SQL reference extraction, reservation
CAS, PostgreSQL locking, durability or complete recovery transitions.

The final evidence reviewer independently checked all 29 frozen input hashes,
98 complete copied logs, counterexample/positive JSONs, package, generated SQL,
runtime layer and published predecessor archive. All 55 actual four-file
installation observations match library SHA-256
`a7b58da8fb8dd0af867e3c64d423c0cd7e15471b4e87eadbc639dde52eb43303`,
candidate commit and six bindings. Every bound runtime case has a recorded
PASS in the final logs. The generated archive-only commit `b73dbf28` preserves
the SQL-line multiset and exactly matches the packaged install SQL.

Neither reviewer ran project checks or edited files. Acceptance is confined to
this local-capture publication slice. Remote Citus/dblink, full release
qualification, authenticated build/named-execution provenance, complete
prefix/incarnation/protocol proofs, latest-head remote CI and merge approval
remain outside acceptance. See `verification/slot-retirement-safety.md` and
`.p2p/work/issue-1222-slot-retirement/ownership/evidence/qualification-manifest.json`.

## Shared evidence boundary

The reviews identified that candidate binding checks source hashes and
package/installed-library equality, while trusting the surrounding build and
test runner for build provenance and successful execution of named cases.
The helper does not prove source-to-binary compilation. Historical local
installation observations had no commit field; current binding requires it
to match the requested candidate. A declared matching commit still relies on
the trusted runner and is not independent authenticated build provenance.
