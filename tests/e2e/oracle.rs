//! Shared exact correctness oracle for pg_trickle DVM tests.
//!
//! Provides schema comparison and exact multiset comparison (bag semantics)
//! using symmetric `EXCEPT ALL`, concrete row diffs, and fail-closed outcome typing.

#![allow(dead_code, clippy::result_large_err)]

use super::E2eDb;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

const JSON_OID: u32 = 114;
static NEXT_TEMP_VIEW: AtomicU64 = AtomicU64::new(0);
type RelationColumnRows = Vec<(i64, String, i64, i64, i64)>;

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
    pub comparison_error: Option<String>,
    pub actual_signature: RelationSignature,
    pub expected_signature: RelationSignature,
}

impl std::fmt::Display for RelationDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref mismatch) = self.schema_mismatch {
            writeln!(f, "Schema mismatch:\n  {mismatch}")?;
        }
        if let Some(ref error) = self.comparison_error {
            writeln!(f, "Comparator error:\n  {error}")?;
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

impl RelationDiff {
    fn comparison_error(
        error: impl Into<String>,
        actual_signature: RelationSignature,
        expected_signature: RelationSignature,
    ) -> Self {
        Self {
            actual_count: -1,
            expected_count: -1,
            extra_count: -1,
            missing_count: -1,
            extra_rows: vec![],
            missing_rows: vec![],
            schema_mismatch: None,
            comparison_error: Some(error.into()),
            actual_signature,
            expected_signature,
        }
    }
}

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
    let relation_oid: Option<i64> = sqlx::query_scalar("SELECT to_regclass($1)::oid::int8")
        .bind(st_table)
        .fetch_one(&db.pool)
        .await?;
    let relation_oid = relation_oid
        .ok_or_else(|| sqlx::Error::Protocol(format!("relation {st_table:?} does not exist")))?;

    let rows: Vec<(i64, String, i64, i64, i64)> = sqlx::query_as(
        "SELECT \
            a.attnum::int8 AS ordinal, \
            a.attname::text AS name, \
            a.atttypid::int8 AS type_oid, \
            a.atttypmod::int8 AS typmod, \
            COALESCE(a.attcollation::int8, 0) AS collation_oid \
         FROM pg_attribute a \
         WHERE a.attrelid = $1::oid \
           AND a.attnum > 0 \
           AND NOT a.attisdropped \
           AND left(a.attname::text, 6) <> '__pgt_' \
         ORDER BY a.attnum",
    )
    .bind(relation_oid)
    .fetch_all(&db.pool)
    .await?;

    Ok(relation_signature(rows))
}

/// Extract column metadata for an arbitrary SELECT query by creating a temporary view.
pub async fn fetch_relation_signature_from_query(
    db: &E2eDb,
    defining_query: &str,
) -> Result<RelationSignature, sqlx::Error> {
    let mut conn = db.pool.acquire().await?;
    let tmp_view = format!(
        "_pgt_oracle_view_{}_{}",
        std::process::id(),
        NEXT_TEMP_VIEW.fetch_add(1, Ordering::Relaxed)
    );
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
           AND left(a.attname::text, 6) <> '__pgt_' \
         ORDER BY a.attnum"
    );
    let rows_result: Result<RelationColumnRows, sqlx::Error> =
        sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .fetch_all(&mut *conn)
            .await;

    let cleanup_result = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP VIEW IF EXISTS {tmp_view}"
    )))
    .execute(&mut *conn)
    .await;
    let rows = rows_result?;
    cleanup_result?;

    Ok(relation_signature(rows))
}

fn relation_signature(rows: Vec<(i64, String, i64, i64, i64)>) -> RelationSignature {
    RelationSignature {
        columns: rows
            .into_iter()
            .enumerate()
            .map(
                |(index, (_ord, name, type_oid, typmod, collation_oid))| ColumnSignature {
                    ordinal: index + 1,
                    name,
                    type_oid: type_oid as u32,
                    typmod: typmod as i32,
                    collation_oid: if collation_oid > 0 {
                        Some(collation_oid as u32)
                    } else {
                        None
                    },
                },
            )
            .collect(),
    }
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
                "Column {} ('{}') incompatible type OID mismatch: actual={}, expected={}",
                i + 1,
                act.name,
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
    act_oid == exp_oid
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn public_projection(signature: &RelationSignature) -> String {
    if signature.columns.is_empty() {
        return "*".to_string();
    }

    signature
        .columns
        .iter()
        .map(|column| {
            let identifier = quote_identifier(&column.name);
            if column.type_oid == JSON_OID {
                format!("{identifier}::text AS {identifier}")
            } else {
                identifier
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

async fn count_rows(db: &E2eDb, query: String) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(query))
        .fetch_one(&db.pool)
        .await
}

async fn sample_rows(db: &E2eDb, query: String) -> Result<Vec<String>, sqlx::Error> {
    let rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(query))
        .fetch_all(&db.pool)
        .await?;
    Ok(rows.into_iter().map(|(row,)| row).collect())
}

/// Compare a stream table's content and schema against a defining query.
pub async fn compare_st_to_query(
    db: &E2eDb,
    st_table: &str,
    defining_query: &str,
) -> Result<(), RelationDiff> {
    let actual_sig = match fetch_relation_signature_from_table(db, st_table).await {
        Ok(signature) => signature,
        Err(error) => {
            return Err(RelationDiff::comparison_error(
                format!("Failed to inspect actual relation '{st_table}': {error}"),
                RelationSignature { columns: vec![] },
                RelationSignature { columns: vec![] },
            ));
        }
    };

    let expected_sig = match fetch_relation_signature_from_query(db, defining_query).await {
        Ok(signature) => signature,
        Err(error) => {
            return Err(RelationDiff::comparison_error(
                format!("Failed to inspect expected query: {error}"),
                actual_sig,
                RelationSignature { columns: vec![] },
            ));
        }
    };

    let schema_mismatch = compare_signatures(&actual_sig, &expected_sig).err();

    // Schema mismatches are already a definitive failure; counts are diagnostic only.
    if let Some(ref mismatch) = schema_mismatch {
        let actual_count = count_rows(db, format!("SELECT count(*) FROM {st_table}"))
            .await
            .unwrap_or(-1);
        let expected_count = count_rows(db, format!("SELECT count(*) FROM ({defining_query}) _q"))
            .await
            .unwrap_or(-1);

        return Err(RelationDiff {
            actual_count,
            expected_count,
            extra_count: -1,
            missing_count: -1,
            extra_rows: vec![],
            missing_rows: vec![],
            schema_mismatch: Some(mismatch.clone()),
            comparison_error: None,
            actual_signature: actual_sig,
            expected_signature: expected_sig,
        });
    }

    let actual_subquery = format!("SELECT {} FROM {st_table}", public_projection(&actual_sig));
    let expected_subquery = format!(
        "SELECT {} FROM ({defining_query}) __pgt_dq",
        public_projection(&expected_sig)
    );

    let count_query = format!(
        "SELECT \
            (SELECT count(*) FROM ({actual_subquery}) _a)::int8 AS actual_count, \
            (SELECT count(*) FROM ({expected_subquery}) _e)::int8 AS expected_count, \
            (SELECT count(*) FROM (({actual_subquery}) EXCEPT ALL ({expected_subquery})) _de)::int8 AS extra_count, \
            (SELECT count(*) FROM (({expected_subquery}) EXCEPT ALL ({actual_subquery})) _dm)::int8 AS missing_count"
    );

    let (actual_count, expected_count, extra_count, missing_count): (i64, i64, i64, i64) =
        match sqlx::query_as(sqlx::AssertSqlSafe(count_query))
            .fetch_one(&db.pool)
            .await
        {
            Ok(counts) => counts,
            Err(error) => {
                return Err(RelationDiff::comparison_error(
                    format!("Multiset diff query failed for '{st_table}': {error}"),
                    actual_sig,
                    expected_sig,
                ));
            }
        };

    if extra_count == 0 && missing_count == 0 && schema_mismatch.is_none() {
        return Ok(());
    }

    // Fetch up to 10 sample extra and missing rows for diagnostic reporting
    let extra_rows = if extra_count > 0 {
        let query = format!(
            "SELECT row_to_json(t)::text FROM (({actual_subquery}) EXCEPT ALL ({expected_subquery})) t LIMIT 10"
        );
        match sample_rows(db, query).await {
            Ok(rows) => rows,
            Err(error) => {
                return Err(RelationDiff::comparison_error(
                    format!("Extra-row diagnostic query failed for '{st_table}': {error}"),
                    actual_sig,
                    expected_sig,
                ));
            }
        }
    } else {
        vec![]
    };

    let missing_rows = if missing_count > 0 {
        let query = format!(
            "SELECT row_to_json(t)::text FROM (({expected_subquery}) EXCEPT ALL ({actual_subquery})) t LIMIT 10"
        );
        match sample_rows(db, query).await {
            Ok(rows) => rows,
            Err(error) => {
                return Err(RelationDiff::comparison_error(
                    format!("Missing-row diagnostic query failed for '{st_table}': {error}"),
                    actual_sig,
                    expected_sig,
                ));
            }
        }
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
        comparison_error: None,
        actual_signature: actual_sig,
        expected_signature: expected_sig,
    })
}

/// Compare two stream tables as multisets using symmetric EXCEPT ALL.
pub async fn compare_sts(db: &E2eDb, left_st: &str, right_st: &str) -> Result<(), RelationDiff> {
    let left_sig = match fetch_relation_signature_from_table(db, left_st).await {
        Ok(signature) => signature,
        Err(error) => {
            return Err(RelationDiff::comparison_error(
                format!("Failed to inspect left relation '{left_st}': {error}"),
                RelationSignature { columns: vec![] },
                RelationSignature { columns: vec![] },
            ));
        }
    };

    let right_sig = match fetch_relation_signature_from_table(db, right_st).await {
        Ok(signature) => signature,
        Err(error) => {
            return Err(RelationDiff::comparison_error(
                format!("Failed to inspect right relation '{right_st}': {error}"),
                left_sig,
                RelationSignature { columns: vec![] },
            ));
        }
    };

    let schema_mismatch = compare_signatures(&left_sig, &right_sig).err();
    if let Some(ref mismatch) = schema_mismatch {
        let left_count = count_rows(db, format!("SELECT count(*) FROM {left_st}"))
            .await
            .unwrap_or(-1);
        let right_count = count_rows(db, format!("SELECT count(*) FROM {right_st}"))
            .await
            .unwrap_or(-1);

        return Err(RelationDiff {
            actual_count: left_count,
            expected_count: right_count,
            extra_count: -1,
            missing_count: -1,
            extra_rows: vec![],
            missing_rows: vec![],
            schema_mismatch: Some(mismatch.clone()),
            comparison_error: None,
            actual_signature: left_sig,
            expected_signature: right_sig,
        });
    }

    let select_cols = public_projection(&left_sig);

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
        match sqlx::query_as(sqlx::AssertSqlSafe(count_query))
            .fetch_one(&db.pool)
            .await
        {
            Ok(counts) => counts,
            Err(error) => {
                return Err(RelationDiff::comparison_error(
                    format!("Multiset diff query failed for '{left_st}' vs '{right_st}': {error}"),
                    left_sig,
                    right_sig,
                ));
            }
        };

    if extra_count == 0 && missing_count == 0 && schema_mismatch.is_none() {
        return Ok(());
    }

    let extra_rows = if extra_count > 0 {
        let query = format!(
            "SELECT row_to_json(t)::text FROM (({left_subquery}) EXCEPT ALL ({right_subquery})) t LIMIT 10"
        );
        match sample_rows(db, query).await {
            Ok(rows) => rows,
            Err(error) => {
                return Err(RelationDiff::comparison_error(
                    format!(
                        "Extra-row diagnostic query failed for '{left_st}' vs '{right_st}': {error}"
                    ),
                    left_sig,
                    right_sig,
                ));
            }
        }
    } else {
        vec![]
    };

    let missing_rows = if missing_count > 0 {
        let query = format!(
            "SELECT row_to_json(t)::text FROM (({right_subquery}) EXCEPT ALL ({left_subquery})) t LIMIT 10"
        );
        match sample_rows(db, query).await {
            Ok(rows) => rows,
            Err(error) => {
                return Err(RelationDiff::comparison_error(
                    format!(
                        "Missing-row diagnostic query failed for '{left_st}' vs '{right_st}': {error}"
                    ),
                    left_sig,
                    right_sig,
                ));
            }
        }
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
        comparison_error: None,
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
        assert!(compare_signatures(&signature("value", -1, Some(100)), &expected).is_err());
        for actual in [
            signature("other", 12, Some(100)),
            signature("value", 13, Some(100)),
            signature("value", 12, Some(101)),
        ] {
            assert!(compare_signatures(&actual, &expected).is_err());
        }
    }

    #[test]
    fn schema_oracle_rejects_type_differences_by_default() {
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
        assert!(compare_signatures(&actual, &expected).is_err());
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
