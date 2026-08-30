//! Benchmark scaffolding for pgtrickle.
//!
//! These benchmarks measure the performance of pure in-process functions.
//! Database-level benchmarks (refresh duration, delta query execution)
//! require a live PostgreSQL instance and are documented in PLAN.md Phase 11.4.
//!
//! Run with: `cargo bench` (requires pgrx pg18 feature, so may need
//! `cargo bench --no-default-features --features pg18` or just unit benchmarks).

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use pg_trickle::dag::{DagNode, NodeId, StDag, StStatus};
use pg_trickle::dvm::diff::{col_list, prefixed_col_list, quote_ident};
use pg_trickle::dvm::parser::{AggExpr, AggFunc, Column, Expr, OpTree};
use pg_trickle::dvm::row_id_v2::{encode_interval_value, row_probe_v1};
use pg_trickle::version::{Frontier, lsn_gt, select_canonical_period_secs};
use std::time::Duration;

// ── quote_ident benchmark ──────────────────────────────────────────────────

fn bench_quote_ident(c: &mut Criterion) {
    let names = [
        "simple",
        "with spaces",
        "has\"quotes",
        "a_very_long_column_name_that_is_common_in_real_schemas",
    ];

    let mut group = c.benchmark_group("quote_ident");
    group.sample_size(200);
    group.measurement_time(Duration::from_secs(10));
    for name in &names {
        group.bench_with_input(BenchmarkId::from_parameter(name), name, |b, name| {
            b.iter(|| quote_ident(black_box(name)));
        });
    }
    group.finish();
}

// ── col_list benchmark ─────────────────────────────────────────────────────

fn bench_col_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("col_list");
    group.sample_size(200);
    group.measurement_time(Duration::from_secs(10));
    for size in [1, 5, 10, 20, 50] {
        let cols: Vec<String> = (0..size).map(|i| format!("column_{i}")).collect();
        group.bench_with_input(BenchmarkId::from_parameter(size), &cols, |b, cols| {
            b.iter(|| col_list(black_box(cols)));
        });
    }
    group.finish();
}

// ── prefixed_col_list benchmark ────────────────────────────────────────────

fn bench_prefixed_col_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefixed_col_list");
    group.sample_size(200);
    group.measurement_time(Duration::from_secs(10));
    for size in [1, 5, 10, 20] {
        let cols: Vec<String> = (0..size).map(|i| format!("col_{i}")).collect();
        group.bench_with_input(BenchmarkId::from_parameter(size), &cols, |b, cols| {
            b.iter(|| prefixed_col_list(black_box("src"), black_box(cols)));
        });
    }
    group.finish();
}

// ── Expr::to_sql benchmark ─────────────────────────────────────────────────

fn bench_expr_to_sql(c: &mut Criterion) {
    let simple_col = Expr::ColumnRef {
        table_alias: None,
        column_name: "amount".to_string(),
    };

    let nested = Expr::BinaryOp {
        op: "+".to_string(),
        left: Box::new(Expr::BinaryOp {
            op: "*".to_string(),
            left: Box::new(Expr::ColumnRef {
                table_alias: Some("t".to_string()),
                column_name: "price".to_string(),
            }),
            right: Box::new(Expr::ColumnRef {
                table_alias: Some("t".to_string()),
                column_name: "qty".to_string(),
            }),
        }),
        right: Box::new(Expr::Literal("100".to_string())),
    };

    let func_call = Expr::FuncCall {
        func_name: "coalesce".to_string(),
        args: vec![
            Expr::ColumnRef {
                table_alias: None,
                column_name: "x".to_string(),
            },
            Expr::Literal("0".to_string()),
        ],
    };

    let mut group = c.benchmark_group("expr_to_sql");
    group.bench_function("simple_column", |b| {
        b.iter(|| black_box(&simple_col).to_sql());
    });
    group.bench_function("nested_binary_op", |b| {
        b.iter(|| black_box(&nested).to_sql());
    });
    group.bench_function("func_call", |b| {
        b.iter(|| black_box(&func_call).to_sql());
    });
    group.finish();
}

// ── OpTree::output_columns benchmark ───────────────────────────────────────

fn make_scan(alias: &str, oid: u32, ncols: usize) -> OpTree {
    OpTree::Scan {
        table_oid: oid,
        table_name: alias.to_string(),
        schema: "public".to_string(),
        columns: (0..ncols)
            .map(|i| Column {
                name: format!("col_{i}"),
                type_oid: 23,
                is_nullable: true,
            })
            .collect(),
        pk_columns: Vec::new(),
        alias: alias.to_string(),
    }
}

fn bench_output_columns(c: &mut Criterion) {
    let scan = make_scan("t", 1, 20);

    let join = OpTree::InnerJoin {
        condition: Expr::ColumnRef {
            table_alias: None,
            column_name: "id".to_string(),
        },
        left: Box::new(make_scan("a", 1, 10)),
        right: Box::new(make_scan("b", 2, 10)),
    };

    let aggregate = OpTree::Aggregate {
        group_by: vec![Expr::ColumnRef {
            table_alias: None,
            column_name: "region".to_string(),
        }],
        aggregates: vec![
            AggExpr {
                function: AggFunc::Sum,
                argument: Some(Expr::ColumnRef {
                    table_alias: None,
                    column_name: "amount".to_string(),
                }),
                alias: "total".to_string(),
                is_distinct: false,
                filter: None,
                second_arg: None,
                order_within_group: None,
                statistical_support: None,
            },
            AggExpr {
                function: AggFunc::CountStar,
                argument: None,
                alias: "cnt".to_string(),
                is_distinct: false,
                filter: None,
                second_arg: None,
                order_within_group: None,
                statistical_support: None,
            },
        ],
        child: Box::new(make_scan("t", 1, 5)),
    };

    let mut group = c.benchmark_group("output_columns");
    group.bench_function("scan_20cols", |b| {
        b.iter(|| black_box(&scan).output_columns());
    });
    group.bench_function("join_20cols", |b| {
        b.iter(|| black_box(&join).output_columns());
    });
    group.bench_function("aggregate", |b| {
        b.iter(|| black_box(&aggregate).output_columns());
    });
    group.finish();
}

// ── OpTree::source_oids benchmark ──────────────────────────────────────────

fn bench_source_oids(c: &mut Criterion) {
    // Deep tree: filter → join → scan+scan
    let deep = OpTree::Filter {
        predicate: Expr::Literal("true".to_string()),
        child: Box::new(OpTree::InnerJoin {
            condition: Expr::Literal("true".to_string()),
            left: Box::new(OpTree::Filter {
                predicate: Expr::Literal("true".to_string()),
                child: Box::new(make_scan("a", 1, 5)),
            }),
            right: Box::new(OpTree::Distinct {
                child: Box::new(make_scan("b", 2, 5)),
            }),
        }),
    };

    // Wide tree: union all with 10 children
    let wide = OpTree::UnionAll {
        children: (1..=10)
            .map(|i| make_scan(&format!("t{i}"), i, 3))
            .collect(),
    };

    let mut group = c.benchmark_group("source_oids");
    group.bench_function("deep_tree", |b| {
        b.iter(|| black_box(&deep).source_oids());
    });
    group.bench_function("wide_union_10", |b| {
        b.iter(|| black_box(&wide).source_oids());
    });
    group.finish();
}

// ── LSN comparison benchmark ───────────────────────────────────────────────

fn bench_lsn_comparison(c: &mut Criterion) {
    let pairs = [
        ("0/0", "0/1"),
        ("0/FFFFFFFF", "1/0"),
        ("FF/DEADBEEF", "FF/DEADBEF0"),
        ("0/0", "0/0"),
    ];

    let mut group = c.benchmark_group("lsn_gt");
    group.sample_size(200);
    group.measurement_time(Duration::from_secs(10));
    for (a, b) in &pairs {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{a}_vs_{b}")),
            &(a, b),
            |bench, (a, b)| {
                bench.iter(|| lsn_gt(black_box(a), black_box(b)));
            },
        );
    }
    group.finish();
}

// ── Frontier JSON serialization benchmark ──────────────────────────────────

fn bench_frontier_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("frontier_json");

    for n_sources in [1, 5, 10, 20] {
        let mut f = Frontier::new();
        for i in 0..n_sources {
            f.set_source(
                i as u32 + 1000,
                format!("0/{:X}", i * 1000),
                "2024-01-01T00:00:00Z".to_string(),
            );
        }
        f.set_data_timestamp("2024-06-15T12:00:00Z".to_string());

        let json = f.to_json().unwrap();

        group.bench_with_input(
            BenchmarkId::new("serialize", n_sources),
            &f,
            |b, frontier| {
                b.iter(|| black_box(frontier).to_json().unwrap());
            },
        );

        group.bench_with_input(
            BenchmarkId::new("deserialize", n_sources),
            &json,
            |b, json_str| {
                b.iter(|| Frontier::from_json(black_box(json_str)).unwrap());
            },
        );
    }
    group.finish();
}

// ── Canonical period selection benchmark ───────────────────────────────────

fn bench_canonical_period(c: &mut Criterion) {
    c.bench_function("canonical_period_60s", |b| {
        b.iter(|| select_canonical_period_secs(black_box(60)));
    });
    c.bench_function("canonical_period_3600s", |b| {
        b.iter(|| select_canonical_period_secs(black_box(3600)));
    });
    c.bench_function("canonical_period_86400s", |b| {
        b.iter(|| select_canonical_period_secs(black_box(86400)));
    });
}

// ── DAG operations benchmark ───────────────────────────────────────────────

fn bench_dag_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag");

    for n_nodes in [10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::new("build_linear_chain", n_nodes),
            &n_nodes,
            |b, &n| {
                b.iter(|| {
                    let mut dag = StDag::new();
                    for id in 1..=n as i64 {
                        dag.add_st_node(DagNode {
                            id: NodeId::StreamTable(id),
                            schedule: Some(Duration::from_secs(60)),
                            effective_schedule: Duration::from_secs(60),
                            name: format!("st_{id}"),
                            status: StStatus::Active,
                            schedule_raw: None,
                        });
                    }
                    for id in 1..n as i64 {
                        dag.add_edge(NodeId::StreamTable(id), NodeId::StreamTable(id + 1));
                    }
                    dag.detect_cycles().unwrap();
                    dag.topological_order().unwrap()
                });
            },
        );
    }
    group.finish();
}

// ── C-2: Incremental DAG rebuild benchmark ─────────────────────────────────

/// Build a DAG with `n` stream tables in a wide fan-out topology (each ST
/// depends on one shared base table), then benchmark the cost of removing
/// and re-adding a single node — simulating what `rebuild_incremental` does
/// without the SPI catalog lookup.
fn bench_dag_incremental(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_incremental");

    for n_nodes in [100, 500, 1000] {
        // Setup: build a fan-out DAG (base → st_1, base → st_2, …).
        group.bench_with_input(
            BenchmarkId::new("remove_readd_single", n_nodes),
            &n_nodes,
            |b, &n| {
                // Build the DAG once, then clone it for each iteration.
                let mut template = StDag::new();
                for id in 1..=n as i64 {
                    template.add_st_node(DagNode {
                        id: NodeId::StreamTable(id),
                        schedule: Some(Duration::from_secs(60)),
                        effective_schedule: Duration::from_secs(60),
                        name: format!("st_{id}"),
                        status: StStatus::Active,
                        schedule_raw: None,
                    });
                    // Each ST depends on base table OID 10000
                    template.add_edge(NodeId::BaseTable(10000), NodeId::StreamTable(id));
                }
                // Also add some inter-ST edges to simulate cascading deps
                for id in 1..n as i64 / 2 {
                    template.add_edge(
                        NodeId::StreamTable(id),
                        NodeId::StreamTable(id + n as i64 / 2),
                    );
                }
                template.detect_cycles().unwrap();
                template.topological_order().unwrap();

                b.iter(|| {
                    let mut dag = template.clone();
                    let target_id = n as i64 / 2; // middle node

                    // Phase 1: Remove the node (simulates rebuild_incremental Phase 1)
                    dag.remove_st_node(target_id);

                    // Phase 2: Re-add the node (simulates catalog re-query result)
                    dag.add_st_node(DagNode {
                        id: NodeId::StreamTable(target_id),
                        schedule: Some(Duration::from_secs(60)),
                        effective_schedule: Duration::from_secs(60),
                        name: format!("st_{target_id}"),
                        status: StStatus::Active,
                        schedule_raw: None,
                    });
                    dag.add_edge(NodeId::BaseTable(10000), NodeId::StreamTable(target_id));
                    if target_id <= n as i64 / 2 {
                        dag.add_edge(
                            NodeId::StreamTable(target_id),
                            NodeId::StreamTable(target_id + n as i64 / 2),
                        );
                    }

                    // Phase 3: Re-resolve CALCULATED schedules
                    dag.resolve_calculated_schedule(60);

                    black_box(&dag);
                });
            },
        );

        // Benchmark: full rebuild from scratch at same scale (for comparison)
        group.bench_with_input(
            BenchmarkId::new("full_rebuild_comparison", n_nodes),
            &n_nodes,
            |b, &n| {
                b.iter(|| {
                    let mut dag = StDag::new();
                    for id in 1..=n as i64 {
                        dag.add_st_node(DagNode {
                            id: NodeId::StreamTable(id),
                            schedule: Some(Duration::from_secs(60)),
                            effective_schedule: Duration::from_secs(60),
                            name: format!("st_{id}"),
                            status: StStatus::Active,
                            schedule_raw: None,
                        });
                        dag.add_edge(NodeId::BaseTable(10000), NodeId::StreamTable(id));
                    }
                    for id in 1..n as i64 / 2 {
                        dag.add_edge(
                            NodeId::StreamTable(id),
                            NodeId::StreamTable(id + n as i64 / 2),
                        );
                    }
                    dag.detect_cycles().unwrap();
                    dag.topological_order().unwrap();
                    black_box(&dag);
                });
            },
        );
    }
    group.finish();
}

// ── XXH64 hash benchmark ──────────────────────────────────────────────────

fn bench_xxh64(c: &mut Criterion) {
    use xxhash_rust::xxh64;
    let seed = 0x517cc1b727220a95u64;

    let mut group = c.benchmark_group("xxh64");
    for size in [16, 64, 256, 1024, 4096] {
        let data = vec![b'x'; size];
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{size}B")),
            &data,
            |b, data| {
                b.iter(|| xxh64::xxh64(black_box(data), seed));
            },
        );
    }
    group.finish();
}

fn bench_row_id_v2(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_id_v2");
    for size in [64, 128, 1024, 16 * 1024] {
        let data = vec![b'x'; size];
        group.bench_with_input(BenchmarkId::new("probe", size), &data, |b, data| {
            b.iter(|| row_probe_v1(black_box(data.clone())));
        });
    }
    group.bench_function("interval_128_bit", |b| {
        b.iter(|| {
            encode_interval_value(
                black_box(2_147_483_647),
                black_box(-2_147_483_648),
                black_box(i64::MAX),
            )
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_quote_ident,
    bench_col_list,
    bench_prefixed_col_list,
    bench_expr_to_sql,
    bench_output_columns,
    bench_source_oids,
    bench_lsn_comparison,
    bench_frontier_json,
    bench_canonical_period,
    bench_dag_operations,
    bench_dag_incremental,
    bench_xxh64,
    bench_a44_6_write_amplification,
    bench_a44_11_di_delta_scan,
    bench_row_id_v2,
);
criterion_main!(benches);

// ── A44-6: Write-amplification benchmarks ─────────────────────────────────
//
// Measures how write-heavy workloads affect hash computation cost (pure Rust).
//
// Key insight: D+I schema computes two pk_hash values per UPDATE (one for
// D-row from old values, one for I-row from new values), whereas the wide
// schema computes only one. This benchmark documents the overhead at
// various column counts. The expected overhead is ≤15% total per UPDATE
// after accounting for trigger/SPI savings.
fn bench_a44_6_write_amplification(c: &mut Criterion) {
    use xxhash_rust::xxh64;
    const SEED: u64 = 42;
    let mut group = c.benchmark_group("a44_6_write_amplification");
    group.measurement_time(Duration::from_secs(10));

    for n_cols in [4u32, 10, 20, 50] {
        let old_data: Vec<Vec<u8>> = (0..n_cols)
            .map(|i| format!("old_value_{i}").into_bytes())
            .collect();
        let new_data: Vec<Vec<u8>> = (0..n_cols)
            .map(|i| format!("new_value_{i}").into_bytes())
            .collect();

        // Wide schema: one hash per UPDATE (concatenated old values)
        let old_concat: Vec<u8> = old_data.iter().flat_map(|v| v.iter().copied()).collect();
        group.bench_with_input(
            BenchmarkId::new("wide_single_hash", n_cols),
            &old_concat,
            |b, data| {
                b.iter(|| xxh64::xxh64(black_box(data), SEED));
            },
        );

        // D+I schema: two hashes per UPDATE (old + new values separately)
        group.bench_with_input(
            BenchmarkId::new("di_double_hash", n_cols),
            &(old_data.clone(), new_data.clone()),
            |b, (olds, news)| {
                b.iter(|| {
                    let old_concat: Vec<u8> = olds.iter().flat_map(|v| v.iter().copied()).collect();
                    let new_concat: Vec<u8> = news.iter().flat_map(|v| v.iter().copied()).collect();
                    let _d = xxh64::xxh64(black_box(&old_concat), SEED);
                    let _i = xxh64::xxh64(black_box(&new_concat), SEED);
                    black_box((_d, _i))
                });
            },
        );
    }
    group.finish();
}

// ── A44-11: D+I delta scan benchmarks ─────────────────────────────────────
//
// Measures the delta SQL generation cost for the D+I CB scan path
// (diff_scan_change_buffer) at various buffer sizes and column counts.
//
// These benchmarks serve as the A44-11 baseline. Post-D+I schema, the scan
// path has no UNION ALL for UPDATE decomposition — this is the performance
// model for CB scan improvement assertions:
// - CB scan: improvement ≥20% for 50-column table at 10K+ buffer rows
// - Multi-change: improvement ≥30% when ≥50% of PKs have 2+ changes
fn bench_a44_11_di_delta_scan(c: &mut Criterion) {
    use pg_trickle::dvm::diff::DiffContext;
    use pg_trickle::dvm::parser::{AggExpr, AggFunc, Column, Expr, OpTree};

    let mut group = c.benchmark_group("a44_11_di_delta_scan");
    group.measurement_time(Duration::from_secs(10));

    for n_cols in [4u32, 10, 20, 50] {
        let cols: Vec<String> = (0..n_cols).map(|i| format!("col_{i}")).collect();
        let col_strs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();

        // Build a simple Scan → Aggregate tree to exercise the P5 direct delta path
        let scan = OpTree::Scan {
            table_oid: 16384,
            table_name: "orders".to_string(),
            schema: "public".to_string(),
            columns: col_strs
                .iter()
                .map(|&c| Column {
                    name: c.to_string(),
                    type_oid: 23,
                    is_nullable: false,
                })
                .collect(),
            pk_columns: vec!["col_0".to_string()],
            alias: "o".to_string(),
        };

        let agg = OpTree::Aggregate {
            group_by: vec![Expr::ColumnRef {
                table_alias: None,
                column_name: "col_1".to_string(),
            }],
            aggregates: vec![AggExpr {
                function: AggFunc::Sum,
                argument: Some(Expr::ColumnRef {
                    table_alias: None,
                    column_name: "col_2".to_string(),
                }),
                alias: "total".to_string(),
                is_distinct: false,
                filter: None,
                second_arg: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan),
        };

        let mut prev = pg_trickle::version::Frontier::new();
        prev.set_source(
            16384,
            "0/1000".to_string(),
            "2024-01-01T00:00:00Z".to_string(),
        );
        let mut new_f = pg_trickle::version::Frontier::new();
        new_f.set_source(
            16384,
            "0/2000".to_string(),
            "2024-01-01T00:01:00Z".to_string(),
        );

        group.bench_with_input(
            BenchmarkId::new("p5_agg_delta_ncols", n_cols),
            &agg,
            |b, agg| {
                b.iter(|| {
                    let mut ctx = DiffContext::new_standalone(prev.clone(), new_f.clone());
                    black_box(ctx.diff_node(agg).ok())
                });
            },
        );
    }
    group.finish();
}
