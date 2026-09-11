//! Minimal public-SQL clients used by the v0.104 Graph and Delta suites.

use sqlx::{Executor, Row};

#[derive(Debug)]
pub struct DeltaRegistration {
    pub consumer_id: String,
    pub delta_relation: String,
    pub state: String,
}

#[derive(Debug)]
pub struct DeltaBatch {
    pub batch_token: i64,
    pub graph_refresh_id: Option<i64>,
    pub mode: String,
    pub row_count: i64,
}

pub async fn graph_digest(pool: &sqlx::PgPool, root: &str) -> Vec<u8> {
    sqlx::query_scalar("SELECT graph_digest FROM pgtrickle.graph_contract(ARRAY[$1::regclass])")
        .bind(root)
        .fetch_one(pool)
        .await
        .expect("public graph contract should return a digest")
}

pub async fn refresh_graph<'e, E>(executor: E, root: &str, digest: &[u8]) -> i64
where
    E: Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query(
        "SELECT graph_refresh_id FROM pgtrickle.refresh_graph_strict(\
             ARRAY[$1::regclass], $2::bytea, 'ALLOW')",
    )
    .bind(root)
    .bind(digest)
    .fetch_one(executor)
    .await
    .expect("public graph refresh should succeed")
    .try_get("graph_refresh_id")
    .expect("graph refresh result should contain its ID")
}

pub async fn stream_contract_digest(pool: &sqlx::PgPool, root: &str) -> Vec<u8> {
    sqlx::query_scalar("SELECT contract_digest FROM pgtrickle.stream_table_contract($1::regclass)")
        .bind(root)
        .fetch_one(pool)
        .await
        .expect("public stream-table contract should return a digest")
}

pub async fn register(
    pool: &sqlx::PgPool,
    root: &str,
    name: &str,
    digest: &[u8],
) -> DeltaRegistration {
    let row = sqlx::query(
        "SELECT consumer_id::text AS consumer_id, delta_relation, state \
         FROM pgtrickle.register_output_delta_consumer(\
             $1::regclass, $2, $3::bytea, 'RESNAPSHOT_REQUIRED')",
    )
    .bind(root)
    .bind(name)
    .bind(digest)
    .fetch_one(pool)
    .await
    .expect("public output-delta registration should succeed");
    DeltaRegistration {
        consumer_id: row.try_get("consumer_id").expect("consumer ID is required"),
        delta_relation: row
            .try_get("delta_relation")
            .expect("typed delta relation is required"),
        state: row.try_get("state").expect("consumer state is required"),
    }
}

pub async fn begin_resnapshot<'e, E>(executor: E, consumer_id: &str) -> (String, i64)
where
    E: Executor<'e, Database = sqlx::Postgres>,
{
    let row = sqlx::query(
        "SELECT resnapshot_token::text AS resnapshot_token, log_head \
         FROM pgtrickle.begin_output_delta_resnapshot($1::uuid)",
    )
    .bind(consumer_id)
    .fetch_one(executor)
    .await
    .expect("public resnapshot should begin");
    (
        row.try_get("resnapshot_token")
            .expect("resnapshot token is required"),
        row.try_get("log_head")
            .expect("resnapshot head is required"),
    )
}

pub async fn ack_resnapshot<'e, E>(executor: E, consumer_id: &str, token: &str) -> String
where
    E: Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query_scalar("SELECT pgtrickle.ack_output_delta_resnapshot($1::uuid, $2::uuid)")
        .bind(consumer_id)
        .bind(token)
        .fetch_one(executor)
        .await
        .expect("public resnapshot acknowledgement should succeed")
}

pub async fn pending_batch<'e, E>(executor: E, consumer_id: &str) -> DeltaBatch
where
    E: Executor<'e, Database = sqlx::Postgres>,
{
    let row = sqlx::query(
        "SELECT batch_token, graph_refresh_id, mode, row_count \
         FROM pgtrickle.output_delta_batches($1::uuid, NULL::bigint) \
         ORDER BY batch_token LIMIT 1",
    )
    .bind(consumer_id)
    .fetch_one(executor)
    .await
    .expect("public output-delta API should return a pending batch");
    DeltaBatch {
        batch_token: row.try_get("batch_token").expect("batch token is required"),
        graph_refresh_id: row.try_get("graph_refresh_id").expect("graph ID is valid"),
        mode: row.try_get("mode").expect("batch mode is required"),
        row_count: row.try_get("row_count").expect("row count is required"),
    }
}

pub async fn read_payload<'e, E>(executor: E, relation: &str) -> i64
where
    E: Executor<'e, Database = sqlx::Postgres>,
{
    let sql = format!("SELECT count(*)::bigint AS row_count FROM {relation}");
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .fetch_one(executor)
        .await
        .expect("publicly returned typed delta relation should be readable")
        .try_get("row_count")
        .expect("payload row count is required")
}

pub async fn ack<'e, E>(executor: E, consumer_id: &str, through_token: i64) -> String
where
    E: Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query_scalar("SELECT pgtrickle.ack_output_delta($1::uuid, $2, 'APPLIED')")
        .bind(consumer_id)
        .bind(through_token)
        .fetch_one(executor)
        .await
        .expect("public output-delta acknowledgement should succeed")
}

pub async fn batch_lag(pool: &sqlx::PgPool, consumer_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT batch_lag FROM pgtrickle.output_delta_consumer_status() \
         WHERE consumer_id = $1::uuid",
    )
    .bind(consumer_id)
    .fetch_one(pool)
    .await
    .expect("public consumer status should return the registered consumer")
}
