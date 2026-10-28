# Scheduler pause and admission (#1219)

## Observed baseline and scope

The installed slot-retirement backend `f0a8c643` returns successful pause in
41 ms while a real inline refresh remains blocked at its `input_boundary`
advisory barrier. The observer confirms the worker PID, lock blocker,
unchanged rows/frontier, and successful pause result before the semantic
assertion fails. Evidence is retained in
`.p2p/work/issue-1219-pause-drain/baseline/scheduler-pause-inline-drain.json`.
The old drain check reads `inline_pgt_id`, which is never populated by the
inline refresh paths.

The INSERT-specific pending-dispatch reproducer also fails on that backend:
pause returns successfully while the selected job is still uncommitted, then
the released job actually publishes two rows instead of retaining the one-row
paused baseline. Its JSON records the committed job and changed frontier under
`dispatch-insert-baseline/`. This is a separate admission race, not an inferred
consequence of the inline drain failure.

This repair covers per-node `pause_scheduler`, including inline singletons,
independent group members, atomic groups, SCC iterations and dynamic worker
units. It does not claim a scheduler-liveness theorem or change manual refresh
admission. Global drain, remote capture and concurrent conflicting operator
controls retain their separate contracts.

Concurrent creation or alteration of IMMEDIATE dependency edges is excluded:
closure discovery reads a catalog snapshot, and topology writers do not take
these admission locks. Callers must serialize topology changes with pause.
This repair therefore claims stable dependency definitions, not a DDL fence.

## Admission and drain

Each unit expands its members through the catalog's transitive IMMEDIATE
STREAM_TABLE dependencies, then acquires transaction-scoped locks for the
whole synchronous write set before
its first refresh action, outside internal rollback scopes. Locks remain held
until the containing transaction commits or aborts, including after a failed
internal subtransaction. Shared pause flags are checked again after lock
acquisition. This includes descendants of inline singleton, atomic and SCC
roots, and does not rely on dynamic job membership alone.

Inline singleton admission follows the not-due return and precedes refresh
locking, avoiding recursive scans and admission locks for idle tables.
Pre-admission checks may perform capture/CDC maintenance or mark the root for
reinitialization; they do not publish root rows/frontier or maintain its
synchronous IMMEDIATE descendants.

Dynamic REPEATABLE READ jobs configure their dedicated worker session in a
preceding transaction. The following work transaction therefore takes its
first snapshot at the requested isolation. Setting transaction isolation at
the start of a `BackgroundWorker::transaction` closure is too late: pgrx has
already acquired its first snapshot. The persisted RR-kind adapter test
captures the predecessor's actual SQLSTATE `25001` and isolation error. Its
strict control rejects that error rather than accepting any retryable result
as a successful pause check. This test injects the supported persisted job
kind; it does not claim the current DAG builder naturally emits RR groups.

The private two-integer namespace is `1346851928`; its second key is the low
32 bits of `pgt_id`. Keys are sorted and deduplicated. A collision between IDs
separated by 2^32 can conservatively delay an unrelated unit or pause; it cannot
authorize a paused unit or report an admitted member drained. The namespace is
distinct from the existing scheduler mutex and one-bigint manual-refresh locks.
All locks are acquired with nonblocking try calls, so partial acquisition
cannot introduce an admission-lock deadlock.

Pause first publishes all target flags, then waits for the same locks and
matching QUEUED/RUNNING catalog jobs. If a worker has passed its final flag
check, it already holds every member lock, so pause cannot succeed until that
transaction ends. If pause obtains the lock first, a future worker either fails
to acquire it or observes the published flag and skips the entire unit. This
also covers a dispatch selected before pause whose job row commits afterward.
Such an unadmitted job can finish its retryable outcome after pause returns,
but cannot publish refresh rows or progress while its target remains paused.

Timeout raises an error and leaves the shared pause flags set. Transaction
rollback releases the SQL admission locks but does not undo the shared flags,
consistent with the existing pause API. A stale fixed transaction snapshot
can conservatively retain an old matching job and time out. A concurrent
explicit resume may override the pause flag; callers must coordinate competing
control requests.

## Qualification boundary

The inline tests witness admitted work, require timeout while it remains
blocked, permit the admitted transaction to finish, witness another control
dispatch while the target stays paused, and then requires exact catch-up after
resume. The transitive IMMEDIATE variant separately observes the predecessor
publishing one additional row into the paused grandchild after releasing its
scheduled root. The pending-dispatch test uses a test-only BEFORE INSERT trigger on
the job catalog to isolate actual job enqueue from maintenance writes. It
pauses before the row commits, releases the barrier, and requires terminal job
handling without changed rows/frontier before successful resume/catch-up.
The RR variant requires a retryable pause outcome with no error detail or
SQLSTATE, followed by an actual successful RR-kind job and exact results.

An initial table-lock version of the second fixture timed out before observing
a terminal job. It did not uniquely identify INSERT and is inconclusive,
not a successful semantic failure control. Its complete output and observed
wait diagnostics remain retained separately.

PostgreSQL advisory-lock lifetime, shared-state synchronization, complete unit
membership and stable dependency definitions during admission, plus trusted
installed/named-execution provenance, are adapter
assumptions. There is no new Verus theorem for these SQL operations.

## Completed candidate qualification

Frozen backend `6d7b97d0df4b2823e5c5693e265a47a1648b9622` uses image
`pg_trickle_1219:pause-6d7b97d` and package `target/pause-idle-candidate`.
Its actual extracted and installed library SHA-256 is
`4550177c694e7f3ae054244624ae5da2dc01c678c9f977b8a897e0e66df95378`.
All four witness records reach `phase=complete` against this candidate.

The complete atomicity binary passes 19/19, WAL binary 35/35, additional core
selection 13/13 and scheduler/mixed-mode binaries 28/28. The generated semantic
floors observe 34/34 and 54/54 admitted exact differential cases. No runtime
retries are used. Units pass 2637/2637, Python checks 65/65, formatting/lint
with zero warnings, and release-contract/upgrade-completeness checks pass.

All six pinned finite Verus suites were executed again: LSN 44/0, receipt 5/0,
lifecycle 6/0, CDC 4/0, join 6/0 and identity 6/0. All 27 executable semantic
mutants fail their intended postconditions. These remain existing kernels;
their execution does not prove the new SQL admission protocol. Six bindings
match the current kernel/caller inputs and all named runtime cases.

The final validator checks 90 actual four-file package/install observations,
10 backend-frozen input hashes, four positive records, four predecessor
counterexample records and 69 complete copied logs. Exact generated install
SQL bytes are bound in every payload observation. Noncomment SQL-line multisets
match the unchanged tracked 0.108.4 archive; no schema or migration is added.
See `.p2p/work/issue-1219-pause-drain/idle/evidence/qualification-manifest.json`.

Earlier prototype and `c68546c6` executions retain their own backend identity.
The first complete atomicity run passed fourteen cases and timed out while
waiting for its independent control: the fixture forced legacy refresh time
but omitted controller due time. The corrected fixture transactionally forces
both, captures controller/catalog/history state, and preserves the paused-row
and frontier assertions. It qualified the older package before all installed
phases ran again against `6d7b97d0`. An initial unit run's three sandbox socket
failures and successful host rerun remain separately retained.

This is bounded installed qualification, with no latest-candidate published
package upgrade, latest-head remote CI, full release qualification,
authenticated build provenance or merge-readiness claim. The earlier
slot-retirement upgrade evidence retains its predecessor candidate identity.

Independent read-only source/proof and final installed-evidence reviews accept
this bounded publication slice. The review record describes the required
repairs, inspected evidence and exclusions in `specification-reviews.md`.
