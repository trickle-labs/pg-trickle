//! Shared exact correctness oracle for pg_trickle DVM tests.
//!
//! Provides schema comparison and exact multiset comparison (bag semantics)
//! using symmetric `EXCEPT ALL`, concrete row diffs, and fail-closed outcome typing.

#![allow(dead_code, clippy::result_large_err)]

use super::E2eDb;
use serde::{Deserialize, Serialize};

/// Signature of an individual column in a relation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSignature {
    pub ordinal: usize,
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
    pub collation_oid: Option<u32>,
}

/// Signature of a relation (ordered list of column signatures).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationSignature {
    pub columns: Vec<ColumnSignature>,
}

/// Detailed difference report between actual and expected relations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationDiff {
    pub actual_count: i64,
    pub expected_count: i64,
    pub extra_count: i64,
    pub missing_count: i64,
    pub extra_rows: Vec<String>,
    pub missing_rows: Vec<String>,
    pub schema_mismatch: Option<String>,
    pub actual_signature: RelationSignature,
    pub expected_signature: RelationSignature,
}

impl std::fmt::Display for RelationDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref mismatch) = self.schema_mismatch {
            writeln!(f, "Schema mismatch:\n  {mismatch}")?;
        }
        writeln!(
            f,
            "Counts: actual={}, expected={}, extra={}, missing={}",
            self.actual_count, self.expected_count, self.extra_count, self.missing_count
        )?;
        if !self.extra_rows.is_empty() {
            writeln!(f, "Extra rows in ST (sample up to 10):")?;
            for row in &self.extra_rows {
                writeln!(f, "  + {row}")?;
            }
        }
        if !self.missing_rows.is_empty() {
            writeln!(f, "Missing rows from ST (sample up to 10):")?;
            for row in &self.missing_rows {
                writeln!(f, "  - {row}")?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for RelationDiff {}

/// Fail-closed typed outcome classification for test cases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CaseOutcome {
    Passed(PassReport),
    UnsupportedAtAdmission(UnsupportedReason),
    GeneratorInvalid(GeneratorError),
    ProductFailure(ProductFailure),
    InfrastructureFailure(InfrastructureFailure),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PassReport {
    pub effective_mode: String,
    pub row_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsupportedReason {
    pub sqlstate: Option<String>,
    pub reason_code: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratorError {
    pub stage: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductFailure {
    pub reason: String,
    pub details: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InfrastructureFailure {
    pub message: String,
}

impl CaseOutcome {
    pub fn is_pass(&self) -> bool {
        matches!(self, CaseOutcome::Passed(_))
    }

    pub fn is_unsupported(&self) -> bool {
        matches!(self, CaseOutcome::UnsupportedAtAdmission(_))
    }

    pub fn is_failure(&self) -> bool {
        matches!(
            self,
            CaseOutcome::ProductFailure(_)
                | CaseOutcome::GeneratorInvalid(_)
                | CaseOutcome::InfrastructureFailure(_)
        )
    }
}

/// Classifies errors during stream table creation at admission.
pub fn classify_admission_error(err_str: &str) -> CaseOutcome {
    classify_admission_outcome(None, extract_reason_code(err_str), err_str, false)
}

/// Classifies a PostgreSQL admission error using its structured SQLSTATE and
/// pg_trickle reason code. Message text is retained for diagnostics only.
pub fn classify_admission_sqlx_error(error: &sqlx::Error) -> CaseOutcome {
    let primary_message = error.to_string();
    let detail = error
        .as_database_error()
        .and_then(|database_error| {
            database_error
                .as_error()
                .downcast_ref::<sqlx::postgres::PgDatabaseError>()
        })
        .and_then(sqlx::postgres::PgDatabaseError::detail);
    let message = detail.map_or_else(
        || primary_message.clone(),
        |detail| format!("{primary_message}\nDETAIL: {detail}"),
    );
    let sqlstate = error
        .as_database_error()
        .and_then(|database_error| database_error.code().map(|code| code.into_owned()));
    classify_admission_outcome(
        sqlstate.as_deref(),
        extract_reason_code(&message),
        &message,
        is_infrastructure_sqlx_error(error),
    )
}

pub fn is_infrastructure_sqlx_error(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::Protocol(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed
    ) || error
        .as_database_error()
        .and_then(|database_error| database_error.code())
        .is_some_and(|code| code.starts_with("08"))
}

fn classify_admission_outcome(
    sqlstate: Option<&str>,
    reason_code: Option<String>,
    message: &str,
    infrastructure: bool,
) -> CaseOutcome {
    if infrastructure || sqlstate.is_some_and(|state| state.starts_with("08")) {
        CaseOutcome::InfrastructureFailure(InfrastructureFailure {
            message: message.to_string(),
        })
    } else if matches!(sqlstate, Some("0A000")) && reason_code.is_some() {
        CaseOutcome::UnsupportedAtAdmission(UnsupportedReason {
            sqlstate: sqlstate.map(str::to_string),
            reason_code,
            message: message.to_string(),
        })
    } else if matches!(
        sqlstate,
        Some("42601" | "42703" | "42P01" | "42804" | "42P10" | "22023")
    ) {
        CaseOutcome::GeneratorInvalid(GeneratorError {
            stage: "admission".to_string(),
            message: message.to_string(),
        })
    } else {
        CaseOutcome::ProductFailure(ProductFailure {
            reason: format!("Admission failed unexpectedly: {message}"),
            details: Some(message.to_string()),
        })
    }
}

fn extract_reason_code(message: &str) -> Option<String> {
    message
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '-')
        .find(|token| token.starts_with("DVM-") || *token == "UNSUPPORTED_OPERATOR")
        .map(str::to_string)
}

/// Extract column metadata for a stream table or table.
pub async fn fetch_relation_signature_from_table(
    db: &E2eDb,
    st_table: &str,
) -> Result<RelationSignature, sqlx::Error> {
    let unquoted = st_table.trim_matches('"');

    let sql = format!(
        "SELECT \
            a.attnum::int8 AS ordinal, \
            a.attname::text AS name, \
            a.atttypid::int8 AS type_oid, \
            a.atttypmod::int8 AS typmod, \
            COALESCE(a.attcollation::int8, 0) AS collation_oid \
         FROM pg_attribute a \
         WHERE a.attrelid = to_regclass('{unquoted}') \
           AND a.attnum > 0 \
           AND NOT a.attisdropped \
           AND a.attname::text NOT LIKE '__pgt_%' \
         ORDER BY a.attnum"
    );
    let rows: Vec<(i64, String, i64, i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
        .fetch_all(&db.pool)
        .await?;

    let columns = rows
        .into_iter()
        .enumerate()
        .map(|(index, (_ord, name, toid, tmod, coll))| ColumnSignature {
            ordinal: index + 1,
            name,
            type_oid: toid as u32,
            typmod: tmod as i32,
            collation_oid: if coll > 0 { Some(coll as u32) } else { None },
        })
        .collect();

    Ok(RelationSignature { columns })
}

/// Extract column metadata for an arbitrary SELECT query by creating a temporary view.
pub async fn fetch_relation_signature_from_query(
    db: &E2eDb,
    defining_query: &str,
) -> Result<RelationSignature, sqlx::Error> {
    let mut conn = db.pool.acquire().await?;
    let tmp_view = format!("_pgt_oracle_view_{}", std::process::id());
    let create_view = format!("CREATE TEMPORARY VIEW {tmp_view} AS {defining_query}");
    sqlx::query(sqlx::AssertSqlSafe(create_view))
        .execute(&mut *conn)
        .await?;

    let sql = format!(
        "SELECT \
            a.attnum::int8 AS ordinal, \
            a.attname::text AS name, \
            a.atttypid::int8 AS type_oid, \
            a.atttypmod::int8 AS typmod, \
            COALESCE(a.attcollation::int8, 0) AS collation_oid \
         FROM pg_attribute a \
         WHERE a.attrelid = '{tmp_view}'::regclass \
           AND a.attnum > 0 \
           AND NOT a.attisdropped \
           AND a.attname::text NOT LIKE '__pgt_%' \
         ORDER BY a.attnum"
    );
    let rows: Vec<(i64, String, i64, i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
        .fetch_all(&mut *conn)
        .await?;

    let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP VIEW IF EXISTS {tmp_view}"
    )))
    .execute(&mut *conn)
    .await;

    let columns = rows
        .into_iter()
        .enumerate()
        .map(|(index, (_ord, name, toid, tmod, coll))| ColumnSignature {
            ordinal: index + 1,
            name,
            type_oid: toid as u32,
            typmod: tmod as i32,
            collation_oid: if coll > 0 { Some(coll as u32) } else { None },
        })
        .collect();

    Ok(RelationSignature { columns })
}

/// Compare two relation signatures for schema equivalence.
pub fn compare_signatures(
    actual: &RelationSignature,
    expected: &RelationSignature,
) -> Result<(), String> {
    if actual.columns.len() != expected.columns.len() {
        return Err(format!(
            "Column count mismatch: actual ST has {} columns ({:?}), expected query has {} columns ({:?})",
            actual.columns.len(),
            actual.columns.iter().map(|c| &c.name).collect::<Vec<_>>(),
            expected.columns.len(),
            expected.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
        ));
    }

    for (i, (act, exp)) in actual
        .columns
        .iter()
        .zip(expected.columns.iter())
        .enumerate()
    {
        if act.ordinal != exp.ordinal {
            return Err(format!(
                "Column {} ordinal mismatch: actual={}, expected={}",
                i + 1,
                act.ordinal,
                exp.ordinal
            ));
        }
        if act.name != exp.name {
            return Err(format!(
                "Column {} name mismatch: actual='{}', expected='{}'",
                i + 1,
                act.name,
                exp.name
            ));
        }
        if !is_type_compatible(act.type_oid, exp.type_oid) {
            return Err(format!(
                "Column {} ('{}' vs '{}') incompatible type OID: actual={}, expected={}",
                i + 1,
                act.name,
                exp.name,
                act.type_oid,
                exp.type_oid
            ));
        }
        if act.typmod != exp.typmod {
            return Err(format!(
                "Column {} ('{}') typmod mismatch: actual={}, expected={}",
                i + 1,
                act.name,
                act.typmod,
                exp.typmod
            ));
        }
        if act.collation_oid != exp.collation_oid {
            return Err(format!(
                "Column {} ('{}') collation mismatch: actual={:?}, expected={:?}",
                i + 1,
                act.name,
                act.collation_oid,
                exp.collation_oid
            ));
        }
    }

    Ok(())
}

/// Check if two PostgreSQL type OIDs are compatible for query comparison.
pub fn is_type_compatible(act_oid: u32, exp_oid: u32) -> bool {
    if act_oid == exp_oid || act_oid == 0 || exp_oid == 0 {
        return true;
    }
    // Integer family (int2=21, int4=23, int8=20, oid=26)
    let is_int = |oid| matches!(oid, 20 | 21 | 23 | 26);
    if is_int(act_oid) && is_int(exp_oid) {
        return true;
    }
    // String family (text=25, varchar=1043, bpchar=1042, name=19)
    let is_str = |oid| matches!(oid, 19 | 25 | 1042 | 1043);
    if is_str(act_oid) && is_str(exp_oid) {
        return true;
    }
    // Floating-point / numeric family (float4=700, float8=701, numeric=1700)
    let is_num = |oid| matches!(oid, 700 | 701 | 1700);
    if (is_int(act_oid) || is_num(act_oid)) && (is_int(exp_oid) || is_num(exp_oid)) {
        return true;
    }
    // Timestamp family (timestamp=1114, timestamptz=1184)
    let is_ts = |oid| matches!(oid, 1114 | 1184);
    if is_ts(act_oid) && is_ts(exp_oid) {
        return true;
    }
    // JSON family (json=114, jsonb=3802)
    let is_json = |oid| matches!(oid, 114 | 3802);
    if is_json(act_oid) && is_json(exp_oid) {
        return true;
    }
    false
}

/// Compare a stream table's content and schema against a defining query.
pub async fn compare_st_to_query(
    db: &E2eDb,
    st_table: &str,
    defining_query: &str,
) -> Result<(), RelationDiff> {
    let actual_sig = fetch_relation_signature_from_table(db, st_table)
        .await
        .unwrap_or_else(|e| panic!("Failed to fetch signature for table '{st_table}': {e}"));

    let expected_sig = fetch_relation_signature_from_query(db, defining_query)
        .await
        .unwrap_or_else(|e| panic!("Failed to fetch signature for query '{defining_query}': {e}"));

    let schema_mismatch = compare_signatures(&actual_sig, &expected_sig).err();

    // If there is any schema mismatch (arity or incompatible type), build early diff
    if let Some(ref mismatch) = schema_mismatch {
        let actual_count: i64 = db
            .query_scalar_opt(&format!("SELECT count(*) FROM {st_table}"))
            .await
            .unwrap_or(0);
        let expected_count: i64 = db
            .query_scalar_opt(&format!("SELECT count(*) FROM ({defining_query}) _q"))
            .await
            .unwrap_or(0);

        return Err(RelationDiff {
            actual_count,
            expected_count,
            extra_count: -1,
            missing_count: -1,
            extra_rows: vec![],
            missing_rows: vec![],
            schema_mismatch: Some(mismatch.clone()),
            actual_signature: actual_sig,
            expected_signature: expected_sig,
        });
    }

    // Check whether the ST has dual-count columns (__pgt_count_l, __pgt_count_r)
    let has_dual_counts: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS( \
                SELECT 1 FROM information_schema.columns \
                WHERE (table_schema || '.' || table_name = '{st_table}' \
                   OR table_name = '{st_table}') \
                AND column_name = '__pgt_count_l')"
        ))
        .await;

    let dq_upper = defining_query.to_uppercase();
    let st_relation = if has_dual_counts {
        if dq_upper.contains("INTERSECT ALL") {
            format!(
                "{st_table} CROSS JOIN generate_series(1, LEAST(__pgt_count_l, __pgt_count_r)::integer) WHERE LEAST(__pgt_count_l, __pgt_count_r) > 0"
            )
        } else if dq_upper.contains("INTERSECT") {
            format!("{st_table} WHERE __pgt_count_l > 0 AND __pgt_count_r > 0")
        } else if dq_upper.contains("EXCEPT ALL") {
            format!(
                "{st_table} CROSS JOIN generate_series(1, GREATEST(0, __pgt_count_l - __pgt_count_r)::integer) WHERE GREATEST(0, __pgt_count_l - __pgt_count_r) > 0"
            )
        } else if dq_upper.contains("EXCEPT") {
            format!("{st_table} WHERE __pgt_count_l > 0 AND __pgt_count_r = 0")
        } else {
            st_table.to_string()
        }
    } else {
        st_table.to_string()
    };

    // Build casted projection columns for json compatibility with EXCEPT ALL
    let st_select_cols = if actual_sig.columns.is_empty() {
        "*".to_string()
    } else {
        actual_sig
            .columns
            .iter()
            .map(|c| {
                if c.type_oid == 114 {
                    format!("{}::text AS {}", c.name, c.name)
                } else {
                    c.name.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    let exp_select_cols = if expected_sig.columns.is_empty() {
        "*".to_string()
    } else {
        expected_sig
            .columns
            .iter()
            .map(|c| {
                if c.type_oid == 114 {
                    format!("{}::text AS {}", c.name, c.name)
                } else {
                    c.name.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    let actual_subquery = format!("SELECT {st_select_cols} FROM {st_relation}");
    let expected_subquery = format!("SELECT {exp_select_cols} FROM ({defining_query}) __pgt_dq");

    let count_query = format!(
        "SELECT \
            (SELECT count(*) FROM ({actual_subquery}) _a)::int8 AS actual_count, \
            (SELECT count(*) FROM ({expected_subquery}) _e)::int8 AS expected_count, \
            (SELECT count(*) FROM (({actual_subquery}) EXCEPT ALL ({expected_subquery})) _de)::int8 AS extra_count, \
            (SELECT count(*) FROM (({expected_subquery}) EXCEPT ALL ({actual_subquery})) _dm)::int8 AS missing_count"
    );

    let (actual_count, expected_count, extra_count, missing_count): (i64, i64, i64, i64) =
        sqlx::query_as(sqlx::AssertSqlSafe(count_query))
            .fetch_one(&db.pool)
            .await
            .unwrap_or_else(|e| panic!("Multiset diff query failed for '{st_table}': {e}"));

    if extra_count == 0 && missing_count == 0 && schema_mismatch.is_none() {
        return Ok(());
    }

    // Fetch up to 10 sample extra and missing rows for diagnostic reporting
    let extra_rows: Vec<String> = if extra_count > 0 {
        let sql = format!(
            "SELECT row_to_json(t)::text FROM (({actual_subquery}) EXCEPT ALL ({expected_subquery})) t LIMIT 10"
        );
        let rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .fetch_all(&db.pool)
            .await
            .unwrap_or_default();
        rows.into_iter().map(|(r,)| r).collect()
    } else {
        vec![]
    };

    let missing_rows: Vec<String> = if missing_count > 0 {
        let sql = format!(
            "SELECT row_to_json(t)::text FROM (({expected_subquery}) EXCEPT ALL ({actual_subquery})) t LIMIT 10"
        );
        let rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .fetch_all(&db.pool)
            .await
            .unwrap_or_default();
        rows.into_iter().map(|(r,)| r).collect()
    } else {
        vec![]
    };

    Err(RelationDiff {
        actual_count,
        expected_count,
        extra_count,
        missing_count,
        extra_rows,
        missing_rows,
        schema_mismatch,
        actual_signature: actual_sig,
        expected_signature: expected_sig,
    })
}

/// Compare two stream tables as multisets using symmetric EXCEPT ALL.
pub async fn compare_sts(db: &E2eDb, left_st: &str, right_st: &str) -> Result<(), RelationDiff> {
    let left_sig = fetch_relation_signature_from_table(db, left_st)
        .await
        .unwrap_or_else(|e| panic!("Failed to fetch signature for left ST '{left_st}': {e}"));

    let right_sig = fetch_relation_signature_from_table(db, right_st)
        .await
        .unwrap_or_else(|e| panic!("Failed to fetch signature for right ST '{right_st}': {e}"));

    let schema_mismatch = compare_signatures(&left_sig, &right_sig).err();
    if let Some(ref mismatch) = schema_mismatch {
        let left_count: i64 = db
            .query_scalar_opt(&format!("SELECT count(*) FROM {left_st}"))
            .await
            .unwrap_or(0);
        let right_count: i64 = db
            .query_scalar_opt(&format!("SELECT count(*) FROM {right_st}"))
            .await
            .unwrap_or(0);

        return Err(RelationDiff {
            actual_count: left_count,
            expected_count: right_count,
            extra_count: -1,
            missing_count: -1,
            extra_rows: vec![],
            missing_rows: vec![],
            schema_mismatch: Some(mismatch.clone()),
            actual_signature: left_sig,
            expected_signature: right_sig,
        });
    }

    let select_cols = if left_sig.columns.is_empty() {
        "*".to_string()
    } else {
        left_sig
            .columns
            .iter()
            .map(|c| {
                if c.type_oid == 114 {
                    format!("{}::text AS {}", c.name, c.name)
                } else {
                    c.name.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    let left_subquery = format!("SELECT {select_cols} FROM {left_st}");
    let right_subquery = format!("SELECT {select_cols} FROM {right_st}");

    let count_query = format!(
        "SELECT \
            (SELECT count(*) FROM ({left_subquery}) _a)::int8 AS actual_count, \
            (SELECT count(*) FROM ({right_subquery}) _e)::int8 AS expected_count, \
            (SELECT count(*) FROM (({left_subquery}) EXCEPT ALL ({right_subquery})) _de)::int8 AS extra_count, \
            (SELECT count(*) FROM (({right_subquery}) EXCEPT ALL ({left_subquery})) _dm)::int8 AS missing_count"
    );

    let (actual_count, expected_count, extra_count, missing_count): (i64, i64, i64, i64) =
        sqlx::query_as(sqlx::AssertSqlSafe(count_query))
            .fetch_one(&db.pool)
            .await
            .unwrap_or_else(|e| {
                panic!("Multiset diff query failed for '{left_st}' vs '{right_st}': {e}")
            });

    if extra_count == 0 && missing_count == 0 && schema_mismatch.is_none() {
        return Ok(());
    }

    let extra_rows: Vec<String> = if extra_count > 0 {
        let sql = format!(
            "SELECT row_to_json(t)::text FROM (({left_subquery}) EXCEPT ALL ({right_subquery})) t LIMIT 10"
        );
        let rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .fetch_all(&db.pool)
            .await
            .unwrap_or_default();
        rows.into_iter().map(|(r,)| r).collect()
    } else {
        vec![]
    };

    let missing_rows: Vec<String> = if missing_count > 0 {
        let sql = format!(
            "SELECT row_to_json(t)::text FROM (({right_subquery}) EXCEPT ALL ({left_subquery})) t LIMIT 10"
        );
        let rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .fetch_all(&db.pool)
            .await
            .unwrap_or_default();
        rows.into_iter().map(|(r,)| r).collect()
    } else {
        vec![]
    };

    Err(RelationDiff {
        actual_count,
        expected_count,
        extra_count,
        missing_count,
        extra_rows,
        missing_rows,
        schema_mismatch,
        actual_signature: left_sig,
        expected_signature: right_sig,
    })
}

/// Assert that a stream table matches its defining query under the exact oracle.
pub async fn assert_st_query_exact(
    db: &E2eDb,
    st_table: &str,
    defining_query: &str,
    context: &str,
) {
    if let Err(diff) = compare_st_to_query(db, st_table, defining_query).await {
        panic!(
            "EXACT ORACLE INVARIANT VIOLATION in {context}:\n\
             ST: {st_table}\n\
             Query: {defining_query}\n\
             {diff}"
        );
    }
}

/// Assert that the effective refresh mode is as expected.
pub async fn assert_effective_refresh_mode(
    db: &E2eDb,
    st_name: &str,
    expected_mode: &str,
) -> Result<(), ProductFailure> {
    let unquoted = st_name.trim_matches('"');
    let pure_name = if let Some((_, r)) = unquoted.split_once('.') {
        r
    } else {
        unquoted
    };

    let mode: Option<String> = db
        .query_scalar_opt(&format!(
            "SELECT effective_refresh_mode::text \
             FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = '{pure_name}'"
        ))
        .await;

    match mode {
        Some(actual_mode) => {
            let actual_upper = actual_mode.to_uppercase();
            let exp_upper = expected_mode.to_uppercase();
            if exp_upper == "DIFFERENTIAL" {
                if actual_upper != "DIFFERENTIAL"
                    && actual_upper != "APPEND_ONLY"
                    && actual_upper != "TOP_K"
                    && !actual_upper.starts_with("DIFFERENTIAL")
                {
                    return Err(ProductFailure {
                        reason: format!(
                            "Expected DIFFERENTIAL refresh mode for '{pure_name}', but effective mode was '{actual_mode}' (silent fallback to FULL)"
                        ),
                        details: None,
                    });
                }
            } else if actual_upper != exp_upper {
                return Err(ProductFailure {
                    reason: format!(
                        "Expected refresh mode '{expected_mode}' for '{pure_name}', but got '{actual_mode}'"
                    ),
                    details: None,
                });
            }
            Ok(())
        }
        None => Err(ProductFailure {
            reason: format!("Stream table '{pure_name}' not found in pgt_stream_tables"),
            details: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signature(name: &str, typmod: i32, collation_oid: Option<u32>) -> RelationSignature {
        RelationSignature {
            columns: vec![ColumnSignature {
                ordinal: 1,
                name: name.to_string(),
                type_oid: 23,
                typmod,
                collation_oid,
            }],
        }
    }

    #[test]
    fn schema_oracle_checks_names_typmods_and_collations() {
        let expected = signature("value", 12, Some(100));
        for actual in [
            signature("other", 12, Some(100)),
            signature("value", 13, Some(100)),
            signature("value", 12, Some(101)),
        ] {
            assert!(compare_signatures(&actual, &expected).is_err());
        }
    }

    #[test]
    fn schema_oracle_keeps_postgres_type_family_compatibility() {
        let actual = RelationSignature {
            columns: vec![ColumnSignature {
                ordinal: 1,
                name: "value".to_string(),
                type_oid: 23,
                typmod: -1,
                collation_oid: None,
            }],
        };
        let expected = RelationSignature {
            columns: vec![ColumnSignature {
                ordinal: 1,
                name: "value".to_string(),
                type_oid: 20,
                typmod: -1,
                collation_oid: None,
            }],
        };
        assert!(compare_signatures(&actual, &expected).is_ok());
    }

    #[test]
    fn admission_classification_requires_structured_unsupported_reason() {
        assert!(matches!(
            classify_admission_error("ERROR: unsupported feature"),
            CaseOutcome::ProductFailure(_)
        ));
        assert!(matches!(
            classify_admission_outcome(
                Some("0A000"),
                Some("DVM-81-6-VOLATILE".to_string()),
                "unsupported",
                false,
            ),
            CaseOutcome::UnsupportedAtAdmission(UnsupportedReason {
                sqlstate: Some(_),
                reason_code: Some(_),
                ..
            })
        ));
        assert!(matches!(
            classify_admission_outcome(Some("42601"), None, "syntax error", false),
            CaseOutcome::GeneratorInvalid(_)
        ));
    }
}
