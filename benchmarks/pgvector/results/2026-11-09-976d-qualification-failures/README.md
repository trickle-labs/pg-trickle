# Earlier integrated package qualification failure

[Qualification-only run 42676921481](https://github.com/trickle-labs/pg-trickle/actions/runs/42676921481)
finished on 2026-11-09 for candidate
`976d04b1436ae0924f6d500983e4a0d0d6b2493c`. Its Linux amd64 archive SHA-256
was `fdd0682f288a358d8dfa729aaded5a9682c368b0336e7cf276f03f0163c8dd28`.
The uploaded evidence contained 32 passing suite records and one failed
suite, `criterion-regression`; release evidence was therefore incomplete.

The [suite record](criterion-regression.json), [measurement](criterion.json),
and [complete Criterion log](criterion.log.gz) are retained. Against the
v0.108.3 same-runner baseline, the first comparison reported 41 regressions
above both the 10% and 250 ns thresholds, and the retry reported 37. The retry
paired 113 cases and reported a maximum relative slowdown of 78.18%; the
largest individual cases and raw Criterion estimates are in the run's
`qualification-evidence` artifact. No budget was waived.

The [cross-run comparison](common-regressions.json) finds 24 labels above both
thresholds in the retry measurements of this run and the earlier `c138e4fe`
and `b7b6c0ca` qualifications. They include aggregate, scan, filter, join,
top-k and vector aggregate cases. This identifies repeatable candidates for
profiling; different hosted runs and changing source do not establish a cause.

This is an earlier integrated candidate, before subsequent scheduler,
subscription and test changes. It cannot qualify the latest candidate or
establish the cause of the measured slowdowns. The final candidate requires a
fresh release qualification and the Criterion regression remains unresolved.
