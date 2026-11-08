# Packaged pgvector capability evidence

Generated from the recorded 2026-11-08 packaged test logs; see the [run details](README.md) for image identities and limitations. Each row requires the named tests to pass on both pgvector 0.8.1 and 0.8.7.
This is scoped test evidence, not a promise for every query shape. Unlisted behavior is not established here; sparse aggregate support is not implied by sparse passthrough or indexing.

| Capability | Tested behavior and prerequisites | Passing tests |
|---|---|---|
| Missing extension | Helper rejects setup; pgvector is required | `test_pgvector_helper_missing_extension_is_actionable_and_atomic` ([0.8.1](conformance-0.8.1.log), [0.8.7](conformance-0.8.7.log)) |
| Row-local distances | Dense, half and sparse values; differential projection/filter | `test_pgvector_distance_types_and_functions_differential`, `test_pgvector_row_local_distance_differential` ([0.8.1](conformance-0.8.1.log), [0.8.7](conformance-0.8.7.log)) |
| Top-K distance | Whole-query Top-K recomputation, not row-local maintenance | `test_pgvector_distance_topk_recomputes` ([0.8.1](conformance-0.8.1.log), [0.8.7](conformance-0.8.7.log)) |
| Vector AVG | Experimental; enable_vector_agg is off by default; affected-group rescan | `test_vector_aggregate_disabled_reports_actionable_error`, `test_vector_centroid_rescan_matches_native_avg_under_sustained_churn` ([0.8.1](conformance-0.8.1.log), [0.8.7](conformance-0.8.7.log)) |
| Halfvec AVG | Experimental; enable_vector_agg is off by default; affected-group rescan | `test_pgvector_halfvec_avg_output_type`, `test_pgvector_halfvec_avg_values_correct` ([0.8.1](conformance-0.8.1.log), [0.8.7](conformance-0.8.7.log)) |
| Algebraic vector AVG | Experimental; both aggregate and centroid-state settings are off by default; guarded fallback/rebase | `test_centroid_opt_in_applies_and_falls_back_after_corruption`, `test_centroid_randomized_refresh_churn_matches_native_avg` ([0.8.1](conformance-0.8.1.log), [0.8.7](conformance-0.8.7.log)) |
| Unknown dimensions | Mixed dimensions are rejected without data loss | `test_vector_aggregate_unknown_dimensions_rejects_mixed_input_without_data_loss` ([0.8.1](conformance-0.8.1.log), [0.8.7](conformance-0.8.7.log)) |
| Non-public extension schema | Vector aggregate and distance expressions resolve the installed extension | `test_vector_aggregate_nonpublic_extension_schema`, `test_pgvector_distance_non_public_extension_schema` ([0.8.1](conformance-0.8.1.log), [0.8.7](conformance-0.8.7.log)) |
| ANN helper | Explicit metric/options; index eligibility and half/sparse index cases | `test_embedding_stream_table_v2_metrics_and_options`, `test_embedding_stream_table_halfvec_and_sparsevec_indexes` ([0.8.1](conformance-0.8.1.log), [0.8.7](conformance-0.8.7.log)) |
| Distance subscription | Typed threshold crossing events; unsupported notification modes rejected | `test_distance_subscription_typed_predicates_and_authorization`, `test_distance_subscription_rejects_unsupported_notification_modes` ([0.8.1](conformance-0.8.1.log), [0.8.7](conformance-0.8.7.log)) |
| Hybrid retrieval | Example fixture with tenant isolation | `test_hybrid_candidates_refresh_and_tenant_isolation` ([0.8.1](hybrid-0.8.1.log), [0.8.7](hybrid-0.8.7.log)) |
| Embedding generation | External revision-safe generation and model rollover fixture | `test_revision_safe_generation_and_model_rollover` ([0.8.1](revision-0.8.1.log), [0.8.7](revision-0.8.7.log)) |
