# DVM regression corpus

These scenarios are versioned, standalone reproductions. The JSON file is
canonical; replay does not invoke the query or mutation generator.

| Scenario | Origin | Regression covered |
| --- | --- | --- |
| cor938_physical_width.json | #938/#939 | Aggregate rescan must use the logical projection, not the physical table width. |
| cor939_two_leaf_snapshot.json | #939 | Simultaneous aggregate-leaf changes must use a consistent old-state snapshot. |

Run the corpus with just dvm-corpus, or replay one case with:

    just dvm-replay tests/corpus/dvm_regressions/cor939_two_leaf_snapshot.json
