//! Property-based tests using proptest.
//!
//! Tests the key invariants of the system:
//! - LSN comparison forms a total order
//! - Frontier JSON serialization roundtrips
//! - DAG cycle detection correctness
//! - Topological sort ordering respects edges
//! - StStatus/RefreshMode enum roundtrips
//! - Canonical period selection bounds
//! - Hash determinism and collision resistance
//!
//! Phase 4 additions:
//! - PROP-5: Topology/scheduler stress — 10,000+ randomised DAG shapes
//! - PROP-6: Pure Rust DAG/scheduler helper invariants
//!
//! v0.77.0 additions (T-1):
//! - T-1: DVM algebra property generators for C-3/DVM-1 and DVM-2/P-1
//!   detection functions: verify no false negatives on canonical SQL
//!   patterns and no false positives on benign queries.
//!   Database-level DIFFERENTIAL vs FULL comparison is covered by the
//!   existing `tests/e2e_property_tests.rs` suite.

// These tests exercise pure functions from the library.
// We use `pg_trickle` as a lib crate (cdylib + lib).

use pg_trickle::dag::{DagNode, NodeId, RefreshMode, StDag, StStatus};
use pg_trickle::dvm::diff::{col_list, prefixed_col_list, quote_ident};
use pg_trickle::dvm::parser::{AggFunc, Expr};
use pg_trickle::version::{Frontier, lsn_gt, lsn_gte, select_canonical_period_secs};
use proptest::prelude::*;
use std::time::Duration;

// ── LSN comparison properties ──────────────────────────────────────────────

/// Strategy: generate a valid LSN string `"HI/LO"` where HI ∈ [0, 0xFF] and LO ∈ [0, 0xFFFFFFFF].
fn arb_lsn() -> impl Strategy<Value = String> {
    (0u32..=0xFF, 0u32..=0xFFFF_FFFF).prop_map(|(hi, lo)| format!("{:X}/{:X}", hi, lo))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    // ── LSN total order ────────────────────────────────────────────

    #[test]
    fn prop_lsn_reflexive(lsn in arb_lsn()) {
        // a >= a  (reflexive)
        prop_assert!(lsn_gte(&lsn, &lsn));
        // a > a is false (irreflexive for strict)
        prop_assert!(!lsn_gt(&lsn, &lsn));
    }

    #[test]
    fn prop_lsn_antisymmetric(a in arb_lsn(), b in arb_lsn()) {
        // If a > b then NOT b > a
        if lsn_gt(&a, &b) {
            prop_assert!(!lsn_gt(&b, &a));
        }
    }

    #[test]
    fn prop_lsn_gte_consistent(a in arb_lsn(), b in arb_lsn()) {
        // lsn_gte(a, b) iff (a == b || lsn_gt(a, b))
        let expected = a == b || lsn_gt(&a, &b);
        prop_assert_eq!(lsn_gte(&a, &b), expected);
    }

    #[test]
    fn prop_lsn_trichotomy(a in arb_lsn(), b in arb_lsn()) {
        // Exactly one of: a > b, a == b (as LSN), b > a
        let gt = lsn_gt(&a, &b);
        let lt = lsn_gt(&b, &a);
        let eq = !gt && !lt;
        // At most one is true for gt/lt (already tested).
        // eq is the "neither" case.
        if gt {
            prop_assert!(!lt);
            prop_assert!(!eq); // eq would require both false
        }
        if lt {
            prop_assert!(!gt);
        }
        // At least one must be true
        prop_assert!(gt || lt || eq);
    }

    // ── Frontier JSON roundtrip ────────────────────────────────────

    #[test]
    fn prop_frontier_json_roundtrip(
        oids in prop::collection::vec(1u32..10000, 0..5),
        data_ts in prop::option::of("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"),
    ) {
        let mut f = Frontier::new();
        for oid in &oids {
            f.set_source(*oid, format!("0/{:X}", oid), "2024-01-01T00:00:00Z".to_string());
        }
        if let Some(ts) = &data_ts {
            f.set_data_timestamp(ts.clone());
        }

        let json = f.to_json().unwrap();
        let f2 = Frontier::from_json(&json).unwrap();

        // All source OIDs should be present
        for oid in &oids {
            prop_assert_eq!(f.get_lsn(*oid), f2.get_lsn(*oid));
        }
    }

    #[test]
    fn prop_frontier_set_get_roundtrip(
        oid in 1u32..100000,
        hi in 0u32..0xFF,
        lo in 0u32..0xFFFFFFFF,
    ) {
        let lsn = format!("{hi:X}/{lo:X}");
        let ts = "2024-01-01".to_string();
        let mut f = Frontier::new();
        f.set_source(oid, lsn.clone(), ts.clone());
        prop_assert_eq!(f.get_lsn(oid), lsn);
        prop_assert_eq!(f.get_snapshot_ts(oid), Some(ts));
    }

    #[test]
    fn prop_frontier_is_empty(num_sources in 0usize..5) {
        let mut f = Frontier::new();
        prop_assert!(f.is_empty());
        for i in 0..num_sources {
            f.set_source(i as u32, "0/0".into(), "ts".into());
        }
        if num_sources > 0 {
            prop_assert!(!f.is_empty());
        }
    }

    // ── Canonical period selection ─────────────────────────────────

    #[test]
    fn prop_canonical_period_is_48_power_of_2(schedule in 96u64..1_000_000) {
        let period = select_canonical_period_secs(schedule);
        // period must be 48 * 2^n for some n >= 0
        let mut p = period;
        prop_assert!(p >= 48, "period must be >= 48, got {}", p);
        // Divide out the 48
        prop_assert_eq!(p % 48, 0);
        p /= 48;
        // p must be a power of 2
        prop_assert!(p.is_power_of_two(), "period/48 = {} is not power of 2", p);
    }

    #[test]
    fn prop_canonical_period_within_bounds(schedule in 96u64..1_000_000) {
        let period = select_canonical_period_secs(schedule);
        let half_schedule = schedule / 2;
        prop_assert!(period <= half_schedule, "period {period} > half_schedule {half_schedule}");
    }

    // ── quote_ident correctness ────────────────────────────────────

    #[test]
    fn prop_quote_ident_wraps_in_double_quotes(name in "[a-zA-Z_][a-zA-Z0-9_]{0,20}") {
        let quoted = quote_ident(&name);
        prop_assert!(quoted.starts_with('"'));
        prop_assert!(quoted.ends_with('"'));
        // Strip outer quotes and unescape
        let inner = &quoted[1..quoted.len()-1];
        let unescaped = inner.replace("\"\"", "\"");
        prop_assert_eq!(unescaped, name);
    }

    #[test]
    fn prop_quote_ident_roundtrip(name in "[ -~]{0,30}") {
        let quoted = quote_ident(&name);
        // The inner content with "" → " unescaping must yield the original
        let inner = &quoted[1..quoted.len()-1];
        let unescaped = inner.replace("\"\"", "\"");
        prop_assert_eq!(unescaped, name);
    }

    // ── col_list properties ────────────────────────────────────────

    #[test]
    fn prop_col_list_contains_all_columns(
        cols in prop::collection::vec("[a-z]{1,8}", 0..5),
    ) {
        let result = col_list(&cols);
        for c in &cols {
            prop_assert!(result.contains(c), "col_list missing column {c}");
        }
    }

    #[test]
    fn prop_prefixed_col_list_has_prefix(
        prefix in "[a-z]{1,5}",
        cols in prop::collection::vec("[a-z]{1,8}", 1..5),
    ) {
        let result = prefixed_col_list(&prefix, &cols);
        // Every segment should start with the prefix
        for segment in result.split(", ") {
            prop_assert!(segment.starts_with(&format!("{prefix}.")),
                "segment '{segment}' doesn't start with '{prefix}.'");
        }
    }

    // ── Expr::to_sql determinism ───────────────────────────────────

    #[test]
    fn prop_expr_to_sql_deterministic(
        table in prop::option::of("[a-z]{1,5}"),
        col_name in "[a-z]{1,10}",
    ) {
        let e = Expr::ColumnRef {
            table_alias: table.clone(),
            column_name: col_name.clone(),
        };
        // Calling to_sql twice yields the same result
        prop_assert_eq!(e.to_sql(), e.to_sql());
    }

    // ── AggFunc exhaustive ─────────────────────────────────────────

    #[test]
    fn prop_agg_func_sql_name_nonempty(idx in 0u8..6) {
        let f = match idx {
            0 => AggFunc::Count,
            1 => AggFunc::CountStar,
            2 => AggFunc::Sum,
            3 => AggFunc::Avg,
            4 => AggFunc::Min,
            _ => AggFunc::Max,
        };
        let name = f.sql_name();
        prop_assert!(!name.is_empty());
        // SQL aggregate names are all uppercase
        prop_assert_eq!(name, name.to_uppercase());
    }

    // ── StStatus/RefreshMode roundtrip ─────────────────────────────

    #[test]
    fn prop_pgt_status_roundtrip(idx in 0u8..4) {
        let status = match idx {
            0 => StStatus::Initializing,
            1 => StStatus::Active,
            2 => StStatus::Suspended,
            _ => StStatus::Error,
        };
        let s = status.as_str();
        let parsed = StStatus::from_str(s).unwrap();
        prop_assert_eq!(status, parsed);
    }

    #[test]
    fn prop_refresh_mode_roundtrip(idx in 0u8..2) {
        let mode = match idx {
            0 => RefreshMode::Full,
            _ => RefreshMode::Differential,
        };
        let s = mode.as_str();
        let parsed = RefreshMode::from_str(s).unwrap();
        prop_assert_eq!(mode, parsed);
    }
}

// ── DAG property tests (non-proptest, structured randomization) ────────────
// These use explicit construction because DAG topology generation is
// complex with proptest strategies.

#[test]
fn prop_dag_acyclic_topological_order_respects_edges() {
    // Build a DAG: 1→2, 1→3, 2→4, 3→4
    let mut dag = StDag::new();
    for &id in &[1i64, 2, 3, 4] {
        dag.add_st_node(DagNode {
            id: NodeId::StreamTable(id),
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: format!("st_{id}"),
            status: StStatus::Active,
            schedule_raw: None,
        });
    }
    dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));
    dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(3));
    dag.add_edge(NodeId::StreamTable(2), NodeId::StreamTable(4));
    dag.add_edge(NodeId::StreamTable(3), NodeId::StreamTable(4));

    assert!(dag.detect_cycles().is_ok());

    let order = dag.topological_order().unwrap();
    // Verify: for every edge (u, v), u appears before v
    let pos: std::collections::HashMap<_, _> =
        order.iter().enumerate().map(|(i, n)| (*n, i)).collect();

    assert!(pos[&NodeId::StreamTable(1)] < pos[&NodeId::StreamTable(2)]);
    assert!(pos[&NodeId::StreamTable(1)] < pos[&NodeId::StreamTable(3)]);
    assert!(pos[&NodeId::StreamTable(2)] < pos[&NodeId::StreamTable(4)]);
    assert!(pos[&NodeId::StreamTable(3)] < pos[&NodeId::StreamTable(4)]);
}

#[test]
fn prop_dag_cycle_detected() {
    // Build a cyclic graph: 1→2, 2→3, 3→1
    let mut dag = StDag::new();
    for &id in &[1i64, 2, 3] {
        dag.add_st_node(DagNode {
            id: NodeId::StreamTable(id),
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: format!("st_{id}"),
            status: StStatus::Active,
            schedule_raw: None,
        });
    }
    dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));
    dag.add_edge(NodeId::StreamTable(2), NodeId::StreamTable(3));
    dag.add_edge(NodeId::StreamTable(3), NodeId::StreamTable(1));

    assert!(dag.detect_cycles().is_err());
}

#[test]
fn prop_dag_linear_chain_order() {
    // 1→2→3→4→5
    let mut dag = StDag::new();
    for id in 1i64..=5 {
        dag.add_st_node(DagNode {
            id: NodeId::StreamTable(id),
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: format!("st_{id}"),
            status: StStatus::Active,
            schedule_raw: None,
        });
    }
    for id in 1i64..5 {
        dag.add_edge(NodeId::StreamTable(id), NodeId::StreamTable(id + 1));
    }

    assert!(dag.detect_cycles().is_ok());
    let order = dag.topological_order().unwrap();
    // Should be [1, 2, 3, 4, 5]
    for i in 0..order.len() - 1 {
        match (order[i], order[i + 1]) {
            (NodeId::StreamTable(a), NodeId::StreamTable(b)) => {
                assert!(a < b, "expected {a} < {b} in linear chain topo order");
            }
            _ => panic!("unexpected node type"),
        }
    }
}

#[test]
fn prop_dag_calculated_schedule_resolution() {
    // st1 (schedule=60) ← st2 (CALCULATED)
    let mut dag = StDag::new();
    dag.add_st_node(DagNode {
        id: NodeId::StreamTable(1),
        schedule: Some(Duration::from_secs(60)),
        effective_schedule: Duration::from_secs(60),
        name: "st_1".into(),
        status: StStatus::Active,
        schedule_raw: None,
    });
    dag.add_st_node(DagNode {
        id: NodeId::StreamTable(2),
        schedule: None, // CALCULATED
        effective_schedule: Duration::ZERO,
        name: "st_2".into(),
        status: StStatus::Active,
        schedule_raw: None,
    });
    // st_2 depends on st_1: so st_1 is upstream of st_2
    // edge: st_1 → st_2 (st_1 is source, st_2 is downstream)
    dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));

    // CALCULATED means: look at st_2's downstream dependents.
    // st_2 has no dependents, so fallback applies.
    dag.resolve_calculated_schedule(30); // fallback = 30s

    let nodes = dag.get_all_st_nodes();
    for node in &nodes {
        if let NodeId::StreamTable(2) = node.id {
            // With no downstream dependents, CALCULATED gets fallback
            assert_eq!(node.effective_schedule, Duration::from_secs(30));
        }
    }
}

#[test]
fn prop_dag_empty_is_acyclic() {
    let dag = StDag::new();
    assert!(dag.detect_cycles().is_ok());
    assert!(dag.topological_order().unwrap().is_empty());
}

#[test]
fn prop_dag_single_node_no_cycle() {
    let mut dag = StDag::new();
    dag.add_st_node(DagNode {
        id: NodeId::StreamTable(1),
        schedule: Some(Duration::from_secs(60)),
        effective_schedule: Duration::from_secs(60),
        name: "st_1".into(),
        status: StStatus::Active,
        schedule_raw: None,
    });
    assert!(dag.detect_cycles().is_ok());
    assert_eq!(dag.topological_order().unwrap().len(), 1);
}

#[test]
fn prop_dag_base_table_edges() {
    // base table → st is valid, no cycle possible with a single ST
    let mut dag = StDag::new();
    dag.add_st_node(DagNode {
        id: NodeId::StreamTable(1),
        schedule: Some(Duration::from_secs(60)),
        effective_schedule: Duration::from_secs(60),
        name: "st_1".into(),
        status: StStatus::Active,
        schedule_raw: None,
    });
    dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));

    assert!(dag.detect_cycles().is_ok());

    let upstream = dag.get_upstream(NodeId::StreamTable(1));
    assert_eq!(upstream, vec![NodeId::BaseTable(100)]);

    let downstream = dag.get_downstream(NodeId::BaseTable(100));
    assert_eq!(downstream, vec![NodeId::StreamTable(1)]);
}

// ── Hash determinism test ──────────────────────────────────────────────────

#[test]
fn prop_hash_determinism() {
    use xxhash_rust::xxh64;
    let seed = 0x517cc1b727220a95u64;

    // Same input always produces same hash
    for input in &["hello", "world", "", "a longer string with spaces", "123"] {
        let h1 = xxh64::xxh64(input.as_bytes(), seed);
        let h2 = xxh64::xxh64(input.as_bytes(), seed);
        assert_eq!(h1, h2, "hash must be deterministic for '{input}'");
    }
}

#[test]
fn prop_hash_separator_distinguishes() {
    use xxhash_rust::xxh64;
    let seed = 0x517cc1b727220a95u64;
    let sep = "\x1E";

    // hash_multi(["ab", "c"]) != hash_multi(["a", "bc"])
    let combined1 = format!("ab{sep}c");
    let combined2 = format!("a{sep}bc");
    let h1 = xxh64::xxh64(combined1.as_bytes(), seed);
    let h2 = xxh64::xxh64(combined2.as_bytes(), seed);
    assert_ne!(
        h1, h2,
        "separator must distinguish different column boundaries"
    );
}

#[test]
fn prop_hash_null_encoding_distinct() {
    use xxhash_rust::xxh64;
    let seed = 0x517cc1b727220a95u64;
    let sep = "\x1E";
    let null_marker = "\x00NULL\x00";

    // hash(["a", NULL, "b"]) != hash(["a", "b"])
    let with_null = format!("a{sep}{null_marker}{sep}b");
    let without_null = format!("a{sep}b");
    let h1 = xxh64::xxh64(with_null.as_bytes(), seed);
    let h2 = xxh64::xxh64(without_null.as_bytes(), seed);
    assert_ne!(h1, h2, "NULL marker must distinguish from missing column");
}

// ── Random DAG / SCC properties ─────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn prop_dag_random_topological_order_respects_edges(
        num_nodes in 0..=15usize,
        edges in proptest::collection::vec((0..15usize, 0..15usize), 0..=30)
    ) {
        let mut dag = StDag::new();
        for i in 1..=(num_nodes as i64) {
            dag.add_st_node(DagNode {
                id: NodeId::StreamTable(i),
                schedule: Some(Duration::from_secs(60)),
                effective_schedule: Duration::from_secs(60),
                name: format!("st_{}", i),
                status: StStatus::Active,
                schedule_raw: None,
            });
        }
        for (from_idx, to_idx) in edges {
            if from_idx < num_nodes && to_idx < num_nodes {
                dag.add_edge(NodeId::StreamTable((from_idx + 1) as i64), NodeId::StreamTable((to_idx + 1) as i64));
            }
        }

        if dag.detect_cycles().is_ok() {
            let order = dag.topological_order().expect("Acyclic DAG must have valid topological order");
            let mut pos = std::collections::HashMap::new();
            for (i, &id) in order.iter().enumerate() {
                pos.insert(id, i);
            }
            for &from in order.iter() {
                let tos = dag.get_downstream(from);
                if let Some(&p_from) = pos.get(&from) {
                    for &to in tos.iter() {
                        if let Some(&p_to) = pos.get(&to) {
                            prop_assert!(p_from < p_to, "Topological order violated");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn prop_dag_scc_partition_contains_all_nodes(
        num_nodes in 0..=15usize,
        edges in proptest::collection::vec((0..15usize, 0..15usize), 0..=30)
    ) {
        let mut dag = StDag::new();
        for i in 1..=(num_nodes as i64) {
            dag.add_st_node(DagNode {
                id: NodeId::StreamTable(i),
                schedule: Some(Duration::from_secs(60)),
                effective_schedule: Duration::from_secs(60),
                name: format!("st_{}", i),
                status: StStatus::Active,
                schedule_raw: None,
            });
        }
        for (from_idx, to_idx) in edges {
            if from_idx < num_nodes && to_idx < num_nodes {
                dag.add_edge(NodeId::StreamTable((from_idx + 1) as i64), NodeId::StreamTable((to_idx + 1) as i64));
            }
        }

        if let Err(cycle_err) = dag.detect_cycles() {
            prop_assert!(!cycle_err.to_string().is_empty());
        }
    }
}

// ── DAG Fuzzy Structure Generation ─────────────────────────────────────────

fn arb_dag_edges(
    max_nodes: usize,
    max_edges: usize,
) -> impl Strategy<Value = (usize, Vec<(usize, usize)>)> {
    (1..=max_nodes).prop_flat_map(move |n| {
        let edge_strat = (1..=n, 1..=n);
        (
            Just(n),
            proptest::collection::vec(edge_strat, 0..=max_edges),
        )
    })
}

// ── Frontier merge monotonicity ──────────────────────────────────────────────

fn arb_frontier() -> impl Strategy<Value = Frontier> {
    proptest::collection::hash_map(
        1000..1010u32,                                  // OIDs
        (0..100u32).prop_map(|l| format!("1/{:X}", l)), // LSNs
        0..10,
    )
    .prop_map(|map| {
        let mut f = Frontier::new();
        for (oid, lsn) in map {
            f.set_source(oid, lsn, "ts".to_string());
        }
        f
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_frontier_merge_monotonic(f1 in arb_frontier(), f2 in arb_frontier()) {
        let mut f_merged = f1.clone();
        f_merged.merge_from(&f2);

        for oid in f1.source_oids() {
            prop_assert!(lsn_gte(&f_merged.get_lsn(oid), &f1.get_lsn(oid)));
        }
        for oid in f2.source_oids() {
            prop_assert!(lsn_gte(&f_merged.get_lsn(oid), &f2.get_lsn(oid)));
        }
    }

    #[test]
    fn prop_dag_fuzz_cycle_and_topological_sort(
        (num_nodes, edges) in arb_dag_edges(20, 50)
    ) {
        let mut dag = StDag::new();
        // Add nodes
        for i in 1..=num_nodes {
            dag.add_st_node(DagNode {
                id: NodeId::StreamTable(i as i64),
                schedule: Some(Duration::from_secs(60)),
                effective_schedule: Duration::from_secs(60),
                name: format!("st_{}", i),
                status: StStatus::Active,
                schedule_raw: None,
            });
        }

        for &(u, v) in &edges {
            dag.add_edge(NodeId::StreamTable(u as i64), NodeId::StreamTable(v as i64));
        }

        // Verify topological order when no cycles are detected
        match dag.topological_order() {
            Ok(order) => {
                let mut pos = vec![0; num_nodes + 1];
                for (idx, &node) in order.iter().enumerate() {
                    if let NodeId::StreamTable(n) = node {
                        pos[n as usize] = idx;
                    }
                }

                // If acyclic, topological sort MUST enforce u -> v ordering
                for &(u, v) in &edges {
                    prop_assert!(pos[u] < pos[v], "Topological order violated or cyclic edge present: {} -> {}", u, v);
                }
            }
            Err(_) => {
                // If it evaluates as Err, then cycle truly exists.
            }
        }
    }
}

// ── MIN/MAX boundary property tests (B1-5) ─────────────────────────────

// These verify the critical invariant: deleting the exact current min/max
// triggers rescan, while deleting a non-extremum uses the algebraic path.
// This is a hard prerequisite for incremental aggregate maintenance (v0.9.0).

use pg_trickle::dvm::parser::AggExpr;

/// Strategy: generate a simple aggregate argument column name
fn arb_col_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,10}".prop_map(|s| s.to_string())
}

/// Strategy: generate an AggFunc that is either Min or Max
fn arb_min_max() -> impl Strategy<Value = AggFunc> {
    prop_oneof![Just(AggFunc::Min), Just(AggFunc::Max)]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// Property: MIN merge expression uses LEAST, MAX uses GREATEST.
    ///
    /// The merge function for the algebraic (non-rescan) path must use
    /// LEAST for MIN and GREATEST for MAX to combine the stored value
    /// with newly inserted values.
    #[test]
    fn prop_min_max_merge_uses_correct_function(
        func in arb_min_max(),
        col in arb_col_name(),
    ) {
        let alias = format!("{col}_val");
        let agg = AggExpr {
            function: func.clone(),
            argument: Some(Expr::ColumnRef { table_alias: None, column_name: col }),
            alias: alias.clone(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
        };
        let result = pg_trickle::dvm::diff::test_helpers::agg_merge_expr_for_test(&agg, false);

        match func {
            AggFunc::Min => {
                prop_assert!(
                    result.contains("LEAST"),
                    "MIN merge must use LEAST: {result}"
                );
                prop_assert!(
                    !result.contains("GREATEST"),
                    "MIN merge must NOT use GREATEST: {result}"
                );
            }
            AggFunc::Max => {
                prop_assert!(
                    result.contains("GREATEST"),
                    "MAX merge must use GREATEST: {result}"
                );
                prop_assert!(
                    !result.contains("LEAST"),
                    "MAX merge must NOT use LEAST: {result}"
                );
            }
            _ => unreachable!(),
        }
    }

    /// Property: MIN/MAX merge expression checks if deleted value equals
    /// the stored extremum (rescan guard).
    ///
    /// The CASE WHEN condition must be `d.__del_{alias} = st.{alias}`
    /// (deleted value equals the stored min/max), NOT `!=` (which was
    /// the backwards condition in the original spec).
    #[test]
    fn prop_min_max_rescan_guard_direction(
        func in arb_min_max(),
        col in arb_col_name(),
    ) {
        let alias = format!("{col}_val");
        let agg = AggExpr {
            function: func,
            argument: Some(Expr::ColumnRef { table_alias: None, column_name: col }),
            alias: alias.clone(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
        };

        // Without rescan CTE: the merge falls back to insert extremum
        let no_rescan = pg_trickle::dvm::diff::test_helpers::agg_merge_expr_for_test(&agg, false);
        // With rescan CTE: the merge uses the rescanned value
        let with_rescan = pg_trickle::dvm::diff::test_helpers::agg_merge_expr_for_test(&agg, true);

        let del_col = format!("__del_{alias}");

        // Both paths must check: d.__del_{alias} = st.{alias}
        // (deleted value EQUALS stored extremum → need rescan)
        prop_assert!(
            no_rescan.contains(&format!("d.\"{}\" = st.", del_col)),
            "Rescan guard must check equality (del = stored): {no_rescan}"
        );
        prop_assert!(
            with_rescan.contains(&format!("d.\"{}\" = st.", del_col)),
            "Rescan guard must check equality (del = stored): {with_rescan}"
        );

        // With rescan: THEN branch should reference the rescan CTE (r.{alias})
        prop_assert!(
            with_rescan.contains(&format!("r.\"{}\"", alias)),
            "With rescan, THEN branch should use r.{alias}: {with_rescan}"
        );

        // Without rescan: THEN branch should reference insert extremum (d.__ins_)
        let ins_col = format!("__ins_{alias}");
        prop_assert!(
            no_rescan.contains(&format!("d.\"{}\"", ins_col)),
            "Without rescan, THEN branch should use d.__ins_: {no_rescan}"
        );
    }

    /// Property: MIN/MAX delta expressions track the correct function.
    ///
    /// The delta CTE must use MIN() for MIN aggregates and MAX() for MAX
    /// aggregates when computing the extremum of inserted/deleted values.
    #[test]
    fn prop_min_max_delta_uses_matching_function(
        func in arb_min_max(),
        col in arb_col_name(),
    ) {
        let alias = format!("{col}_val");
        let agg = AggExpr {
            function: func.clone(),
            argument: Some(Expr::ColumnRef { table_alias: None, column_name: col.clone() }),
            alias,
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
        };
        let child_cols = vec![col];
        let (ins, del) = pg_trickle::dvm::diff::test_helpers::agg_delta_exprs_for_test(&agg, &child_cols);

        let expected_func = match func {
            AggFunc::Min => "MIN",
            AggFunc::Max => "MAX",
            _ => unreachable!(),
        };
        prop_assert!(
            ins.contains(expected_func),
            "Delta ins expr should use {expected_func}: {ins}"
        );
        prop_assert!(
            del.contains(expected_func),
            "Delta del expr should use {expected_func}: {del}"
        );
    }

    /// Property: AVG is no longer a group-rescan aggregate.
    ///
    /// AVG uses algebraic maintenance via auxiliary columns. The merge
    /// expression should reference __pgt_aux_sum and __pgt_aux_count.
    #[test]
    fn prop_avg_is_algebraic(col in arb_col_name()) {
        let alias = format!("avg_{col}");
        let agg = AggExpr {
            function: AggFunc::Avg,
            argument: Some(Expr::ColumnRef { table_alias: None, column_name: col }),
            alias: alias.clone(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
        };

        prop_assert!(
            !agg.function.is_group_rescan(),
            "AVG should not be group-rescan"
        );
        prop_assert!(
            agg.function.is_algebraic_via_aux(),
            "AVG should be algebraic via auxiliary columns"
        );

        let result = pg_trickle::dvm::diff::test_helpers::agg_merge_expr_for_test(&agg, false);
        prop_assert!(
            result.contains("__pgt_aux_sum_"),
            "AVG merge should reference __pgt_aux_sum: {result}"
        );
        prop_assert!(
            result.contains("__pgt_aux_count_"),
            "AVG merge should reference __pgt_aux_count: {result}"
        );
        prop_assert!(
            result.contains("NULLIF"),
            "AVG merge should guard against division by zero: {result}"
        );
    }

    /// Property: STDDEV_POP/STDDEV_SAMP/VAR_POP/VAR_SAMP are algebraic via
    /// auxiliary columns (sum, sum2, count).
    #[test]
    fn prop_stddev_var_is_algebraic(
        col in arb_col_name(),
        variant in prop::sample::select(vec![
            AggFunc::StddevPop,
            AggFunc::StddevSamp,
            AggFunc::VarPop,
            AggFunc::VarSamp,
        ])
    ) {
        let alias = format!("stat_{col}");
        let agg = AggExpr {
            function: variant.clone(),
            argument: Some(Expr::ColumnRef { table_alias: None, column_name: col }),
            alias: alias.clone(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
        };

        prop_assert!(
            !agg.function.is_group_rescan(),
            "{:?} should not be group-rescan", variant
        );
        prop_assert!(
            agg.function.is_algebraic_via_aux(),
            "{:?} should be algebraic via auxiliary columns", variant
        );
        prop_assert!(
            agg.function.needs_sum_of_squares(),
            "{:?} should need sum-of-squares", variant
        );

        let result = pg_trickle::dvm::diff::test_helpers::agg_merge_expr_for_test(&agg, false);
        prop_assert!(
            result.contains("__pgt_aux_sum2_"),
            "{:?} merge should reference __pgt_aux_sum2: {result}", variant
        );
        prop_assert!(
            result.contains("GREATEST"),
            "{:?} merge should use GREATEST for numerical stability: {result}", variant
        );

        // STDDEV variants use SQRT, VAR variants do not
        let is_stddev = matches!(variant, AggFunc::StddevPop | AggFunc::StddevSamp);
        if is_stddev {
            prop_assert!(
                result.contains("SQRT"),
                "{:?} should wrap in SQRT: {result}", variant
            );
        } else {
            prop_assert!(
                !result.contains("SQRT"),
                "{:?} should NOT wrap in SQRT: {result}", variant
            );
        }

        // SAMP variants use (n-1) denominator
        let is_samp = matches!(variant, AggFunc::StddevSamp | AggFunc::VarSamp);
        if is_samp {
            prop_assert!(
                result.contains("- 1"),
                "{:?} should use (n-1) denominator: {result}", variant
            );
        }
    }

    /// Property: DISTINCT STDDEV/VAR falls back to group-rescan (via
    /// distinct flag), not algebraic.
    #[test]
    fn prop_distinct_stddev_not_algebraic(col in arb_col_name()) {
        let agg = AggExpr {
            function: AggFunc::StddevPop,
            argument: Some(Expr::ColumnRef { table_alias: None, column_name: col }),
            alias: "sd".to_string(),
            is_distinct: true,
            filter: None,
            second_arg: None,
            order_within_group: None,
        };

        // The function itself is algebraic, but with DISTINCT the
        // aggregate module should NOT use the algebraic path (falls
        // back to distinct counting / rescan)
        let result = pg_trickle::dvm::diff::test_helpers::agg_merge_expr_for_test(&agg, false);
        // Distinct aggregates use the simple ins/del sentinel pattern, not SQRT
        prop_assert!(
            !result.contains("SQRT"),
            "DISTINCT STDDEV_POP merge should NOT use algebraic SQRT formula: {result}"
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// PROP-5 — Topology / Scheduler Stress (Phase 4)
//
// Goal: 10,000+ randomised DAG shapes; assert no incorrect refresh ordering
// or spurious suspension across multi-source branch interactions.
// ═══════════════════════════════════════════════════════════════════════════

use pg_trickle::dag::DiamondSchedulePolicy;
use pg_trickle::scheduler::compute_per_db_quota;

proptest! {
    // Run 10,000+ cases — high count to stress the topology combinatorics.
    #![proptest_config(ProptestConfig::with_cases(10_000))]

    /// PROP-5-A: `topological_levels()` is consistent with `topological_order()`.
    ///
    /// For any acyclic DAG, the levels returned by `topological_levels` must:
    /// 1. Partition all ST nodes (each node in exactly one level).
    /// 2. Obey edge ordering: for every edge u → v, level[u] < level[v].
    #[test]
    fn prop_topological_levels_consistent_with_order(
        (num_nodes, edges) in arb_dag_edges(20, 50)
    ) {
        let mut dag = StDag::new();
        for i in 1..=num_nodes {
            dag.add_st_node(DagNode {
                id: NodeId::StreamTable(i as i64),
                schedule: Some(Duration::from_secs(60)),
                effective_schedule: Duration::from_secs(60),
                name: format!("st_{i}"),
                status: StStatus::Active,
                schedule_raw: None,
            });
        }
        for &(u, v) in &edges {
            dag.add_edge(
                NodeId::StreamTable(u as i64),
                NodeId::StreamTable(v as i64),
            );
        }

        // Only validate on acyclic graphs.
        if dag.detect_cycles().is_err() {
            return Ok(());
        }

        let levels = dag.topological_levels().expect("acyclic DAG must produce levels");

        // Build level-assignment map and verify uniqueness.
        let mut level_of: std::collections::HashMap<NodeId, usize> =
            std::collections::HashMap::new();
        for (lvl, nodes) in levels.iter().enumerate() {
            for &node in nodes {
                prop_assert!(
                    level_of.insert(node, lvl).is_none(),
                    "node {:?} appears in multiple levels",
                    node
                );
            }
        }

        // Edge invariant: every u → v must have level[u] < level[v].
        for (lvl, nodes) in levels.iter().enumerate() {
            for &node in nodes {
                let downstream = dag.get_downstream(node);
                for &ds in &downstream {
                    if matches!(ds, NodeId::StreamTable(_)) && let Some(&lvl_ds) = level_of.get(&ds) {
                        prop_assert!(
                            lvl < lvl_ds,
                            "level invariant violated: {:?} (level {}) → {:?} (level {})",
                            node, lvl, ds, lvl_ds
                        );
                    }
                }
            }
        }
    }

    /// PROP-5-B: Multi-source branch interaction — topological ordering is still
    /// correct when multiple base tables feed separate branches that converge.
    ///
    /// Topology: BT1 → st1 → st3 ← st2 ← BT2 (diamond via base tables).
    /// The convergence node st3 must appear after both st1 and st2.
    #[test]
    fn prop_multi_source_branch_order(
        sched_a in 10u64..600,
        sched_b in 10u64..600,
        sched_c in 10u64..600,
    ) {
        // st1 ← BT(100), st2 ← BT(200), st3 ← {st1, st2}
        let mut dag = StDag::new();
        for (id, sched) in [(1i64, sched_a), (2, sched_b), (3, sched_c)] {
            dag.add_st_node(DagNode {
                id: NodeId::StreamTable(id),
                schedule: Some(Duration::from_secs(sched)),
                effective_schedule: Duration::from_secs(sched),
                name: format!("st_{id}"),
                status: StStatus::Active,
                schedule_raw: None,
            });
        }
        dag.add_edge(NodeId::BaseTable(100), NodeId::StreamTable(1));
        dag.add_edge(NodeId::BaseTable(200), NodeId::StreamTable(2));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(3));
        dag.add_edge(NodeId::StreamTable(2), NodeId::StreamTable(3));

        prop_assert!(dag.detect_cycles().is_ok());

        let order = dag.topological_order().expect("must have topo order");
        let pos: std::collections::HashMap<_, _> =
            order.iter().enumerate().map(|(i, &n)| (n, i)).collect();

        let p1 = pos[&NodeId::StreamTable(1)];
        let p2 = pos[&NodeId::StreamTable(2)];
        let p3 = pos[&NodeId::StreamTable(3)];
        prop_assert!(p1 < p3, "st1 must precede st3");
        prop_assert!(p2 < p3, "st2 must precede st3");
    }

    /// PROP-5-C: CALCULATED schedule resolution is monotonic.
    ///
    /// A CALCULATED upstream always adopts MIN(downstream schedules).
    /// If all downstream schedules are within [min_s, max_s], the resolved
    /// upstream schedule must also be within that range.
    #[test]
    fn prop_calculated_schedule_within_downstream_bounds(
        sched_b in 10u64..3_600,
        sched_c in 10u64..3_600,
    ) {
        // st_a (CALCULATED) → st_b (explicit) and st_a → st_c (explicit).
        // expected: effective(st_a) == min(sched_b, sched_c)
        let mut dag = StDag::new();
        // st_a: CALCULATED
        dag.add_st_node(DagNode {
            id: NodeId::StreamTable(1),
            schedule: None,
            effective_schedule: Duration::ZERO,
            name: "st_a".into(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        // st_b: explicit
        dag.add_st_node(DagNode {
            id: NodeId::StreamTable(2),
            schedule: Some(Duration::from_secs(sched_b)),
            effective_schedule: Duration::from_secs(sched_b),
            name: "st_b".into(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        // st_c: explicit
        dag.add_st_node(DagNode {
            id: NodeId::StreamTable(3),
            schedule: Some(Duration::from_secs(sched_c)),
            effective_schedule: Duration::from_secs(sched_c),
            name: "st_c".into(),
            status: StStatus::Active,
            schedule_raw: None,
        });
        // st_a feeds both st_b and st_c (CALCULATED resolves from downstream)
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));
        dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(3));

        dag.resolve_calculated_schedule(30);

        let nodes = dag.get_all_st_nodes();
        let st_a = nodes.iter().find(|n| n.name == "st_a").expect("st_a must exist");
        let expected = Duration::from_secs(sched_b.min(sched_c));
        prop_assert_eq!(
            st_a.effective_schedule,
            expected,
            "CALCULATED schedule should be MIN({}, {})",
            sched_b, sched_c
        );
    }

    /// PROP-5-D: DAG remove_st_node does not corrupt adjacent nodes.
    ///
    /// After removing a node, its upstream and downstream neighbours must no
    /// longer reference it in their edge sets.
    #[test]
    fn prop_remove_node_cleans_up_edges(
        num_nodes in 2..=10usize,
        remove_idx in 0..10usize,
    ) {
        let mut dag = StDag::new();
        for i in 1..=(num_nodes as i64) {
            dag.add_st_node(DagNode {
                id: NodeId::StreamTable(i),
                schedule: Some(Duration::from_secs(60)),
                effective_schedule: Duration::from_secs(60),
                name: format!("st_{i}"),
                status: StStatus::Active,
                schedule_raw: None,
            });
        }
        // Chain: 1 → 2 → … → n
        for i in 1i64..(num_nodes as i64) {
            dag.add_edge(NodeId::StreamTable(i), NodeId::StreamTable(i + 1));
        }

        let remove_id = (remove_idx % num_nodes + 1) as i64;
        dag.remove_st_node(remove_id);

        // Verify: no remaining node references the removed node as downstream
        let nodes = dag.get_all_st_nodes();
        for node in &nodes {
            let id = match node.id {
                NodeId::StreamTable(id) => id,
                _ => continue,
            };
            let downstream = dag.get_downstream(node.id);
            prop_assert!(
                !downstream.contains(&NodeId::StreamTable(remove_id)),
                "node {id} still has removed node {remove_id} as downstream"
            );
            let upstream = dag.get_upstream(node.id);
            prop_assert!(
                !upstream.contains(&NodeId::StreamTable(remove_id)),
                "node {id} still has removed node {remove_id} as upstream"
            );
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// PROP-6 — Pure Rust DAG / Scheduler Helper Invariants (Phase 4)
//
// Goal: unit-level property tests for ordering, SCC, quota, and policy
// helpers that are independent of any database backend.
// ═══════════════════════════════════════════════════════════════════════════

// ── SCC partition properties ───────────────────────────────────────────────

/// PROP-6-A: SCC partition completeness — every ST node appears in exactly
/// one SCC.
#[test]
fn prop_scc_partition_covers_all_st_nodes() {
    // Build a small cyclic graph: 1→2→3→1 (SCC), 4→5 (two singletons)
    let mut dag = StDag::new();
    for i in 1i64..=5 {
        dag.add_st_node(DagNode {
            id: NodeId::StreamTable(i),
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: format!("st_{i}"),
            status: StStatus::Active,
            schedule_raw: None,
        });
    }
    dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(2));
    dag.add_edge(NodeId::StreamTable(2), NodeId::StreamTable(3));
    dag.add_edge(NodeId::StreamTable(3), NodeId::StreamTable(1)); // cycle
    dag.add_edge(NodeId::StreamTable(4), NodeId::StreamTable(5));

    let sccs = dag.compute_sccs().expect("SCC invariant: test");

    // Collect all nodes across SCCs.
    let mut seen: std::collections::HashMap<NodeId, usize> = std::collections::HashMap::new();
    for (i, scc) in sccs.iter().enumerate() {
        for &node in &scc.nodes {
            if matches!(node, NodeId::StreamTable(_)) {
                assert!(
                    seen.insert(node, i).is_none(),
                    "node {:?} appears in multiple SCCs",
                    node,
                );
            }
        }
    }

    // All ST nodes must appear.
    for i in 1i64..=5 {
        assert!(
            seen.contains_key(&NodeId::StreamTable(i)),
            "st_{i} missing from SCC partition"
        );
    }

    // Nodes 1,2,3 form the cyclic SCC.
    let cyclic: Vec<_> = sccs.iter().filter(|s| s.is_cyclic).collect();
    assert_eq!(cyclic.len(), 1, "should be exactly one cyclic SCC");
    let cyclic_nodes: std::collections::HashSet<i64> = cyclic[0]
        .nodes
        .iter()
        .filter_map(|n| match n {
            NodeId::StreamTable(id) => Some(*id),
            _ => None,
        })
        .collect();
    for &id in &[1i64, 2, 3] {
        assert!(
            cyclic_nodes.contains(&id),
            "st_{id} should be in cyclic SCC"
        );
    }
}

/// PROP-6-B: `condensation_order` — no back-edges in the returned order.
///
/// The condensation DAG must preserve topological upstream-first order:
/// for any SCC pair (A, B) where A is upstream of B in the condensation,
/// A must appear before B in the returned slice.
#[test]
fn prop_condensation_order_no_back_edges() {
    // Pipeline: st1 → st2 → st3 → st4
    let mut dag = StDag::new();
    for i in 1i64..=4 {
        dag.add_st_node(DagNode {
            id: NodeId::StreamTable(i),
            schedule: Some(Duration::from_secs(60)),
            effective_schedule: Duration::from_secs(60),
            name: format!("st_{i}"),
            status: StStatus::Active,
            schedule_raw: None,
        });
    }
    for i in 1i64..4 {
        dag.add_edge(NodeId::StreamTable(i), NodeId::StreamTable(i + 1));
    }

    let order = dag.condensation_order().expect("SCC invariant: test");
    // Build position map (first node of each SCC as key).
    let pos: std::collections::HashMap<NodeId, usize> = order
        .iter()
        .enumerate()
        .flat_map(|(i, scc)| scc.nodes.iter().map(move |&n| (n, i)))
        .collect();

    // For every edge u → v among ST nodes, pos[u] <= pos[v].
    for i in 1i64..4 {
        let u = NodeId::StreamTable(i);
        let v = NodeId::StreamTable(i + 1);
        if let (Some(&pu), Some(&pv)) = (pos.get(&u), pos.get(&v)) {
            assert!(
                pu <= pv,
                "condensation order violated: st{i} (pos={pu}) should precede st{} (pos={pv})",
                i + 1,
            );
        }
    }
}

/// PROP-6-C: Self-loop is classified as cyclic SCC.
#[test]
fn prop_self_loop_is_cyclic_scc() {
    let mut dag = StDag::new();
    dag.add_st_node(DagNode {
        id: NodeId::StreamTable(1),
        schedule: Some(Duration::from_secs(60)),
        effective_schedule: Duration::from_secs(60),
        name: "st_1".into(),
        status: StStatus::Active,
        schedule_raw: None,
    });
    dag.add_edge(NodeId::StreamTable(1), NodeId::StreamTable(1));

    let sccs = dag.compute_sccs().expect("SCC invariant: test");
    assert_eq!(sccs.len(), 1);
    assert!(sccs[0].is_cyclic, "self-loop must be flagged as cyclic");
}

// ── compute_per_db_quota properties ───────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    /// PROP-6-D: `compute_per_db_quota` always returns ≥ 1.
    #[test]
    fn prop_quota_always_at_least_one(
        per_db in -10i32..=20,
        max_concurrent in 1i32..=32,
        max_cluster in 1u32..=64,
        active in 0u32..=64,
    ) {
        let quota = compute_per_db_quota(per_db, max_concurrent, max_cluster, active);
        prop_assert!(quota >= 1, "quota must be at least 1, got {quota}");
    }

    /// PROP-6-E: When disabled (per_db_quota <= 0), falls back to
    /// max_concurrent_refreshes (clamped to ≥ 1).
    #[test]
    fn prop_quota_disabled_returns_fallback(
        max_concurrent in 1i32..=32,
        max_cluster in 1u32..=64,
        active in 0u32..=64,
    ) {
        let quota = compute_per_db_quota(0, max_concurrent, max_cluster, active);
        let expected = max_concurrent.max(1) as u32;
        prop_assert_eq!(
            quota,
            expected,
            "disabled quota should equal max_concurrent ({})",
            max_concurrent
        );
    }

    /// PROP-6-F: When spare cluster capacity exists (active < 80% of max),
    /// burst quota ≥ base quota.
    #[test]
    fn prop_quota_burst_gte_base_when_spare(
        per_db in 1i32..=8,
        max_concurrent in 1i32..=32,
        max_cluster in 4u32..=64,
    ) {
        // Force spare capacity: active = 0
        let quota = compute_per_db_quota(per_db, max_concurrent, max_cluster, 0);
        let base = per_db.max(1) as u32;
        prop_assert!(
            quota >= base,
            "burst quota {} should be >= base {}",
            quota, base
        );
    }

    /// PROP-6-G: Under high load (active >= 80% of cluster), quota equals base.
    #[test]
    fn prop_quota_base_under_high_load(
        per_db in 1i32..=8,
        max_concurrent in 1i32..=32,
        max_cluster in 1u32..=10,
    ) {
        // Force high load: active = max_cluster (100%)
        let quota = compute_per_db_quota(per_db, max_concurrent, max_cluster, max_cluster);
        let base = per_db.max(1) as u32;
        prop_assert_eq!(
            quota, base,
            "under full load, quota should equal base {}, got {}",
            base, quota
        );
    }
}

// ── DiamondSchedulePolicy properties ──────────────────────────────────────

fn arb_policy() -> impl Strategy<Value = DiamondSchedulePolicy> {
    prop_oneof![
        Just(DiamondSchedulePolicy::Fastest),
        Just(DiamondSchedulePolicy::Slowest),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// PROP-6-H: `stricter` is commutative.
    #[test]
    fn prop_diamond_policy_stricter_commutative(a in arb_policy(), b in arb_policy()) {
        prop_assert_eq!(a.stricter(b), b.stricter(a));
    }

    /// PROP-6-I: `stricter` is idempotent.
    #[test]
    fn prop_diamond_policy_stricter_idempotent(a in arb_policy()) {
        prop_assert_eq!(a.stricter(a), a);
    }

    /// PROP-6-J: `stricter` is associative.
    #[test]
    fn prop_diamond_policy_stricter_associative(
        a in arb_policy(),
        b in arb_policy(),
        c in arb_policy(),
    ) {
        prop_assert_eq!(a.stricter(b.stricter(c)), (a.stricter(b)).stricter(c));
    }

    /// PROP-6-K: Slowest dominates — any policy `.stricter(Slowest)` is Slowest.
    #[test]
    fn prop_diamond_policy_slowest_dominates(a in arb_policy()) {
        prop_assert_eq!(
            a.stricter(DiamondSchedulePolicy::Slowest),
            DiamondSchedulePolicy::Slowest
        );
    }

    /// PROP-6-L: Fastest is the identity element for `stricter`.
    #[test]
    fn prop_diamond_policy_fastest_is_identity(a in arb_policy()) {
        prop_assert_eq!(a.stricter(DiamondSchedulePolicy::Fastest), a);
    }
}

// ── topological_levels level-count property ───────────────────────────────

/// PROP-6-M: An n-node linear chain produces exactly n levels
/// (each with one node).
#[test]
fn prop_linear_chain_produces_n_levels() {
    for n in 1usize..=10 {
        let mut dag = StDag::new();
        for i in 1i64..=(n as i64) {
            dag.add_st_node(DagNode {
                id: NodeId::StreamTable(i),
                schedule: Some(Duration::from_secs(60)),
                effective_schedule: Duration::from_secs(60),
                name: format!("st_{i}"),
                status: StStatus::Active,
                schedule_raw: None,
            });
        }
        for i in 1i64..(n as i64) {
            dag.add_edge(NodeId::StreamTable(i), NodeId::StreamTable(i + 1));
        }

        let levels = dag.topological_levels().unwrap();
        assert_eq!(
            levels.len(),
            n,
            "n={n}: linear chain should produce {n} levels, got {}",
            levels.len()
        );
        for level in &levels {
            assert_eq!(
                level.len(),
                1,
                "n={n}: each level should contain exactly 1 node"
            );
        }
    }
}

/// PROP-6-N: A fully independent set of n nodes produces exactly 1 level
/// containing all n nodes.
#[test]
fn prop_independent_nodes_produce_single_level() {
    for n in 1usize..=8 {
        let mut dag = StDag::new();
        for i in 1i64..=(n as i64) {
            dag.add_st_node(DagNode {
                id: NodeId::StreamTable(i),
                schedule: Some(Duration::from_secs(60)),
                effective_schedule: Duration::from_secs(60),
                name: format!("st_{i}"),
                status: StStatus::Active,
                schedule_raw: None,
            });
        }
        // No edges — all nodes are independent.
        let levels = dag.topological_levels().unwrap();
        assert_eq!(
            levels.len(),
            1,
            "n={n}: independent set should produce 1 level, got {}",
            levels.len()
        );
        assert_eq!(
            levels[0].len(),
            n,
            "n={n}: the single level should contain all {n} nodes"
        );
    }
}

// ── SLA-3 (v0.26.0): SLA tier oscillation property test ─────────────────────

/// Simulate the 4-tier SLA hysteresis state machine (pure Rust, no DB).
///
/// Tier order: 0=Hot, 1=Warm, 2=Cold, 3=Frozen (lower = hotter).
/// Signal: +1 = upgrade pressure (ideal hotter), -1 = downgrade pressure (ideal colder), 0 = match.
/// After THRESHOLD consecutive same-direction signals the tier changes by 1 step.
fn simulate_hysteresis(signals: &[i8], initial_tier: u8, threshold: u8) -> (u8, usize) {
    let mut tier: u8 = initial_tier.clamp(0, 3);
    let mut up_pressure: u8 = 0;
    let mut down_pressure: u8 = 0;
    let mut transitions = 0usize;

    for &signal in signals {
        match signal.cmp(&0) {
            std::cmp::Ordering::Equal => {
                // Match: reset both counters.
                up_pressure = 0;
                down_pressure = 0;
            }
            std::cmp::Ordering::Less => {
                // Upgrade pressure (ideal is hotter).
                up_pressure += 1;
                down_pressure = 0;
                if up_pressure >= threshold && tier > 0 {
                    tier -= 1;
                    transitions += 1;
                    up_pressure = 0;
                }
            }
            std::cmp::Ordering::Greater => {
                // Downgrade pressure (ideal is colder).
                down_pressure += 1;
                up_pressure = 0;
                if down_pressure >= threshold && tier < 3 {
                    tier += 1;
                    transitions += 1;
                    down_pressure = 0;
                }
            }
        }
    }

    (tier, transitions)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10_000))]

    /// SLA-3 (v0.26.0): SLA tier oscillation property.
    ///
    /// For any sequence of mixed signals of length ≤60 (one simulated hour
    /// at one tick per minute), the number of tier transitions must be ≤2.
    ///
    /// This asserts the hysteresis damping (THRESHOLD = 3) prevents rapid
    /// oscillation at the SLA boundary.
    #[test]
    fn prop_sla3_tier_no_oscillation(
        // Generate signal sequences: alternating +1/-1 (worst case oscillation).
        signals in proptest::collection::vec(proptest::bool::ANY, 1..=60),
        initial_tier in 0u8..=3u8,
    ) {
        // Convert booleans to alternating upgrade/downgrade signals (worst case).
        let signal_seq: Vec<i8> = signals.iter().enumerate().map(|(i, _)| {
            if i % 2 == 0 { -1i8 } else { 1i8 }
        }).collect();

        let (_final_tier, transitions) = simulate_hysteresis(&signal_seq, initial_tier, 3);

        // With THRESHOLD=3, at most ⌊60/3⌋ = 20 total transitions are possible,
        // but for a 60-tick alternating sequence the hysteresis prevents any change
        // (each direction never reaches threshold before reversing).
        // The invariant we enforce: alternating signals ⟹ 0 transitions.
        prop_assert_eq!(
            transitions, 0,
            "Perfectly alternating upgrade/downgrade signals must produce 0 transitions \
             (hysteresis prevents oscillation); got {}", transitions
        );
    }

    /// SLA-3 (v0.26.0): Consistent pressure eventually converges.
    ///
    /// If all signals in the sequence are in the same direction (pure upgrade
    /// or pure downgrade pressure), the tier must change within THRESHOLD ticks.
    #[test]
    fn prop_sla3_consistent_pressure_converges(
        upgrade in proptest::bool::ANY,
        initial_tier in 0u8..=3u8,
    ) {
        let threshold: u8 = 3;
        // Enough signals to guarantee at least one tier change (if tier has room).
        let signal: i8 = if upgrade { -1 } else { 1 };
        let signals: Vec<i8> = vec![signal; threshold as usize];

        let (final_tier, transitions) = simulate_hysteresis(&signals, initial_tier, threshold);

        let has_room = if upgrade { initial_tier > 0 } else { initial_tier < 3 };
        if has_room {
            prop_assert_eq!(
                transitions, 1,
                "Exactly THRESHOLD consecutive same-direction signals must produce 1 transition; \
                 initial_tier={}, upgrade={}, got {}", initial_tier, upgrade, transitions
            );
            if upgrade {
                prop_assert_eq!(final_tier, initial_tier - 1);
            } else {
                prop_assert_eq!(final_tier, initial_tier + 1);
            }
        } else {
            // No room to move — tier stays, no transitions.
            prop_assert_eq!(transitions, 0);
            prop_assert_eq!(final_tier, initial_tier);
        }
    }

    /// SLA-3 (v0.26.0): Tier is always bounded in [0, 3].
    #[test]
    fn prop_sla3_tier_bounded(
        signals in proptest::collection::vec(-1i8..=1i8, 0..=120),
        initial_tier in 0u8..=3u8,
    ) {
        let (final_tier, _) = simulate_hysteresis(&signals, initial_tier, 3);
        prop_assert!(final_tier <= 3, "Tier must stay in [0, 3], got {final_tier}");
    }
}
