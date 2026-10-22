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

## Shared evidence boundary

The reviews identified that candidate binding checks source hashes and
package/installed-library equality, while trusting the surrounding build and
test runner for build provenance and successful execution of named cases.
The helper does not prove source-to-binary compilation. Historical local
installation observations had no commit field; current binding requires it
to match the requested candidate. A declared matching commit still relies on
the trusted runner and is not independent authenticated build provenance.
