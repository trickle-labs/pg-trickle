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

## Shared evidence boundary

Both reviews identified that candidate binding checks source hashes and
package/installed-library equality, while trusting the surrounding build and
test runner for build provenance and successful execution of named cases.
The helper does not prove source-to-binary compilation. Historical local
installation observations had no commit field; current binding requires it
to match the requested candidate. A declared matching commit still relies on
the trusted runner and is not independent authenticated build provenance.
