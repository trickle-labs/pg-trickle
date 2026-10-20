What this change does: It retains USING clauses when expanding views and builds
their equality predicates for differential refresh. This lets the original
MDM-backed React deadline source register without changing the consumer.

Author review covered both SQL deparsers and all callers of the shared
view-inlining path, the differential join parser, join-column rewriting,
creation/error boundaries, the SQL support documentation, and the new and
existing Testcontainers view/MAINTAIN suites. No API signature, catalog schema,
ownership transition, grant, or dependency changed.

The regression fails against the original pinned artifact. Corrected-artifact
tests compare complete result multisets, including mismatches and NULLs, before
and after differential refresh and rollback. Worker privilege and owner vectors
remain constrained. Quoted USING keys and inner/left/right/full joins pass.
Ambiguous nested merged keys fail closed and are documented; this candidate
does not claim general merged-column support.

No unresolved defect was found in the requested source shape. The pinned
producer's unchanged real managed idle audit passes. The current-main build
and its full-stack audit are separate evidence, including the recorded OOM
attempt; a passing component suite does not qualify a failed full-stack run.

Verdict: Ship as a candidate for review. This is an author review, not an
independent approval or merge authorization.

Not checked: full test-all, performance/load envelopes, production activation,
or the remaining React #11 acceptance work.
