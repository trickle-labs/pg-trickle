//! VH-4 (v0.48.0): Vector benchmark suite for pg_trickle.
//!
//! Measures:
//!   - Vector aggregate OpTree construction cost (VectorAvg / VectorSum)
//!   - AggFunc::sql_name dispatch cost
//!   - Vector literal string encoding (vector vs halfvec)
//!   - Drift percentage computation (VP-2 drift check)
//!
//! These are pure in-process benchmarks — no PostgreSQL backend required.
//! Database-level benchmarks are covered in tests/e2e_pgvector_tests.rs.
//!
//! Run with: `cargo bench --bench pgvector_bench`

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use pg_trickle::dvm::parser::{AggExpr, AggFunc, Column, Expr, OpTree};
use std::time::Duration;

fn make_col(name: &str) -> Column {
    Column {
        name: name.to_string(),
        type_oid: 0,
        is_nullable: true,
    }
}

fn build_vector_avg_tree() -> OpTree {
    let agg = AggExpr {
        function: AggFunc::VectorAvg,
        argument: Some(Expr::ColumnRef {
            table_alias: Some("t".to_string()),
            column_name: "embedding".to_string(),
        }),
        second_arg: None,
        filter: None,
        order_within_group: None,
        statistical_support: None,
        is_distinct: false,
        alias: "centroid".to_string(),
    };
    OpTree::Aggregate {
        aggregates: vec![agg],
        group_by: vec![Expr::ColumnRef {
            table_alias: None,
            column_name: "grp".to_string(),
        }],
        child: Box::new(OpTree::Scan {
            table_oid: 12345,
            table_name: "t".to_string(),
            schema: "public".to_string(),
            columns: vec![make_col("grp"), make_col("embedding")],
            pk_columns: vec![],
            alias: "t".to_string(),
        }),
    }
}

fn build_vector_sum_tree() -> OpTree {
    let agg = AggExpr {
        function: AggFunc::VectorSum,
        argument: Some(Expr::ColumnRef {
            table_alias: Some("emb".to_string()),
            column_name: "embedding".to_string(),
        }),
        second_arg: None,
        filter: None,
        order_within_group: None,
        statistical_support: None,
        is_distinct: false,
        alias: "total_vec".to_string(),
    };
    OpTree::Aggregate {
        aggregates: vec![agg],
        group_by: vec![Expr::ColumnRef {
            table_alias: None,
            column_name: "grp".to_string(),
        }],
        child: Box::new(OpTree::Scan {
            table_oid: 99999,
            table_name: "emb".to_string(),
            schema: "public".to_string(),
            columns: vec![make_col("grp"), make_col("embedding")],
            pk_columns: vec![],
            alias: "emb".to_string(),
        }),
    }
}

// ── Bench 1: Vector aggregate OpTree construction ─────────────────────────

fn bench_vector_optree_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_optree_build");
    group.sample_size(500);
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("build_avg_tree", |b| {
        b.iter(|| black_box(build_vector_avg_tree()))
    });
    group.bench_function("build_sum_tree", |b| {
        b.iter(|| black_box(build_vector_sum_tree()))
    });
    group.finish();
}

// ── Bench 2: AggFunc::sql_name dispatch ───────────────────────────────────

fn bench_agg_func_dispatch(c: &mut Criterion) {
    let mut group = c.benchmark_group("agg_func_dispatch");
    group.sample_size(1000);
    group.measurement_time(Duration::from_secs(3));
    let funcs = vec![
        AggFunc::VectorAvg,
        AggFunc::VectorSum,
        AggFunc::Avg,
        AggFunc::Sum,
    ];
    group.bench_function("vector_avg_sql_name", |b| {
        b.iter(|| black_box(AggFunc::VectorAvg.sql_name()))
    });
    group.bench_function("vector_sum_sql_name", |b| {
        b.iter(|| black_box(AggFunc::VectorSum.sql_name()))
    });
    group.bench_function("mixed_4_funcs", |b| {
        b.iter(|| {
            for f in &funcs {
                black_box(f.sql_name());
            }
        })
    });
    group.finish();
}

// ── Bench 3: Vector literal string encoding ───────────────────────────────

fn bench_vector_string_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_string_encoding");
    group.sample_size(500);
    group.measurement_time(Duration::from_secs(5));

    let v1536: Vec<f32> = (0..1536).map(|i| i as f32 / 1536.0).collect();
    group.bench_with_input(
        BenchmarkId::new("format_vector_literal", 1536),
        &v1536,
        |b, v| {
            b.iter(|| {
                let parts: Vec<String> = v.iter().map(|f| format!("{:.6}", f)).collect();
                black_box(format!("[{}]", parts.join(",")))
            })
        },
    );

    let v768: Vec<f32> = (0..768).map(|i| i as f32 / 768.0).collect();
    group.bench_with_input(
        BenchmarkId::new("format_halfvec_literal", 768),
        &v768,
        |b, v| {
            b.iter(|| {
                let parts: Vec<String> = v.iter().map(|f| format!("{:.4}", f)).collect();
                black_box(format!("[{}]", parts.join(",")))
            })
        },
    );

    group.finish();
}

// ── Bench 4: Drift detection (VP-2) ───────────────────────────────────────

fn bench_drift_detection(c: &mut Criterion) {
    let mut group = c.benchmark_group("drift_detection");
    group.sample_size(5000);
    group.measurement_time(Duration::from_secs(3));
    let cases: Vec<(i64, i64, f64)> = vec![
        (1000, 5000, 0.20),
        (100, 1_000_000, 0.20),
        (50000, 50000, 0.20),
        (0, 10000, 0.20),
    ];
    group.bench_function("compute_drift_pct", |b| {
        b.iter(|| {
            for (changed, estimated, threshold) in &cases {
                let pct = if *estimated > 0 {
                    *changed as f64 / *estimated as f64
                } else {
                    0.0
                };
                black_box(pct >= *threshold);
            }
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_vector_optree_build,
    bench_agg_func_dispatch,
    bench_vector_string_encoding,
    bench_drift_detection,
);
criterion_main!(benches);
