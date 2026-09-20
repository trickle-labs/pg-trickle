# Runtime-verified support summary

This file is generated from the release admission fixtures. Run
`python3 scripts/generate_capability_manifest.py` to update it.

Release: `0.108.0`

## Capability status

| Capability | Status | Available | Strategy | Diagnostic |
|---|---|---:|---|---|
| `trigger-capture` | stable | yes | trigger | — |
| `wal-capture` | stable | yes | wal | — |
| `graph-v1` | stable | yes | public_sql | — |
| `delta-v1` | stable | yes | public_sql | — |
| `vector-aggregates` | experimental | no | differential | PGT_EXT_CAPABILITY_DISABLED |
| `set-operation-differential` | unavailable | no | unavailable | UNSUPPORTED_OPERATOR |
| `incremental-window-state` | future | no | unavailable | WINDOW_INCREMENTAL_UNIMPLEMENTED |

## Executed query families

| Query family | Result | Declared mode | Effective strategy | Capture | Local recomputation | Whole-query fallback | Diagnostic | Runtime test |
|---|---|---|---|---|---|---|---|---|
| `stable-capability-discovery` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | — | `test_v104_capabilities_are_stable_by_default` |
| `graph-v1-reference-coordinator` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | — | `test_v104_graph_reference_coordinator_commits_publication` |
| `delta-v1-reference-consumer` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | — | `test_v104_delta_reference_consumer_reads_and_acknowledges` |
| `default-statement-trigger-capture` | accepted | AUTO | DIFFERENTIAL | trigger → TRIGGER | none | none | — | `test_stmt_cdc_default_trigger_is_statement_level` |
| `auto-cdc-wal-admission` | accepted | DIFFERENTIAL | DIFFERENTIAL | auto → WAL | none | none | — | `test_v107_auto_wal_transition_captures_changes` |
| `recursive-cte-recomputation-delta` | accepted | DIFFERENTIAL | DIFFERENTIAL | — → — | recursive-result | none | recursive_cte_fallback | `test_recursive_cte_non_monotone_agg_subquery_recomputation` |
| `window-partition-recomputation` | accepted | DIFFERENTIAL | DIFFERENTIAL | — → — | partition | none | WINDOW_RECOMPUTE_CHEAPER | `test_row_number_rejected_candidate_converges_via_partition_recompute` |
| `nullable-group-key-rescan` | accepted | DIFFERENTIAL | DIFFERENTIAL | — → — | group | none | — | `test_v0873_mandatory_composition_matrix` |
| `intersect-auto-full` | accepted | AUTO | FULL | — → — | whole-query | whole-query | UNSUPPORTED_OPERATOR | `test_diff_full_equivalence_intersect` |
| `mutable-case-aggregate-full-fallback` | accepted | DIFFERENTIAL | DIFFERENTIAL | — → — | whole-query | whole-query | CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK | `test_dvm1_case_in_list_mutable_full_fallback` |
| `graph-v1-2-capability-details` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | — | `test_v108_capability_details_publish_graph_1_2_and_delta_1_1` |
| `graph-v1-2-row-identity-table-srf` | accepted | DIFFERENTIAL | DIFFERENTIAL | — → — | none | none | — | `test_v108_graph_row_identity_and_table_srf_stay_differential` |
| `graph-v1-2-pg-mdm-compiler-v9-exact-evidence` | rejected | DIFFERENTIAL | UNAVAILABLE | — → — | none | none | UNSUPPORTED_OPERATOR | `test_pg_mdm_compiler_v9_bootstrap_and_mutations_match_full_without_fallback` |
| `graph-v1-2-pg-mdm-compiler-v9-immutable-equivalent` | accepted | DIFFERENTIAL | DIFFERENTIAL | — → — | none | none | — | `test_pg_mdm_compiler_v9_bootstrap_and_mutations_match_full_without_fallback` |
| `delta-v1-1-public-resnapshot` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | — | `test_v108_non_pristine_registration_and_public_resnapshot` |
| `delta-v1-1-exact-rollback` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | — | `test_v108_exact_batch_acknowledgement_rolls_back` |
| `delta-v1-1-full-invalidation` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | — | `test_v108_full_invalidation_requires_resynchronized_ack` |
| `delta-v1-1-contract-mismatch` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | — | `test_v108_qualification_requires_resnapshot_for_contract_mismatch` |
| `delta-v1-1-gap` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | — | `test_v108_qualification_requires_resnapshot_for_delta_gap` |
| `delta-v1-1-invalidated-read` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | — | `test_v108_invalidated_consumer_returns_no_partial_batch_range` |
| `delta-v1-1-detected-gap` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | DELTA_GAP | `test_v108_validation_persists_detected_delta_gap` |
| `delta-v1-1-detected-payload-mismatch` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | PAYLOAD_INCONSISTENT | `test_v108_validation_persists_detected_payload_mismatch` |
| `delta-v1-1-detected-contract-mismatch` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | CONTRACT_MISMATCH | `test_v108_validation_persists_detected_contract_mismatch` |
| `delta-v1-1-detected-row-identity-version-mismatch` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | ROW_IDENTITY_VERSION_MISMATCH | `test_v108_validation_persists_detected_row_identity_version_mismatch` |
| `delta-v1-1-clone-adoption` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | DATABASE_INSTANCE_CHANGED | `test_v108_clone_adoption_allows_new_consumer_baseline` |
| `delta-v1-1-detected-database-instance-change` | accepted | PUBLIC_SQL | PUBLIC_SQL | — → — | none | none | DATABASE_INSTANCE_CHANGED | `test_v108_validation_persists_detected_database_instance_change` |

A local group, partition, or recursive-result recomputation still runs inside
a differential refresh. Only `whole-query` in the fallback column means the
entire defining query was recomputed.
