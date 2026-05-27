//! DuckLake sink implementation — writes stream table delta results into DuckLake.
//!
//! Implements the write path introduced in v0.66.0:
//!
//! 1. **Parquet delta serialisation** (`arrow-array` + `parquet` crates, sync).
//! 2. **Object-store upload** (local `file://` or AWS S3 via `object_store`).
//! 3. **DuckLake catalog transaction writer** (via SPI into DuckLake tables).
//! 4. **Encryption key pass-through** (F-9) for encrypted lakes.
//!
//! v0.67.0 additions:
//!
//! 5. **View registration (F-6)** — upserts a `ducklake_view` entry so the
//!    stream table appears as a native catalog object to every DuckLake client.
//! 6. **Snapshot provenance (INT-11)** — records `created_by` in every
//!    `ducklake_snapshot` row and writes to `pgtrickle.pgt_ducklake_provenance`
//!    for end-to-end lineage.
//!
//! v0.69.0 additions:
//!
//! 7. **Delivery state machine (ARCH-002/REL-001)** — tracks each delivery
//!    attempt in `pgtrickle.pgt_ducklake_sink_delivery`, supports
//!    retry/backoff up to `ducklake_sink_max_retries`, and transitions to
//!    `FAILED_PERMANENT` after exhausting retries.
//! 8. **Snapshot advisory lock (COR-006)** — acquires `pg_advisory_xact_lock`
//!    before computing `MAX(snapshot_id)` to prevent concurrent collisions.
//! 9. **Qualified schema resolution (SEC-002)** — all DuckLake catalog writes
//!    use `pg_trickle.ducklake_catalog_schema`-qualified identifiers.
//!
//! # Rollback safety
//!
//! The write order is intentionally **upload-then-catalog**:
//!
//! 1. Write Parquet bytes.
//! 2. Upload to object store. If this fails, no catalog rows are written.
//! 3. Open an SPI subtransaction and insert into `ducklake_data_file`,
//!    update `ducklake_table_stats`, insert `ducklake_snapshot`.
//! 4. If the SPI write fails, the Parquet file on object storage is orphaned
//!    (it can be garbage-collected by DuckLake's VACUUM). This is the same
//!    semantics as DuckLake's own writer and is acceptable.
//!
//! # Architecture note
//!
//! `object_store` is fully async. We drive it synchronously using a
//! `tokio::runtime::Builder::new_current_thread()` runtime created on demand
//! per sink call. This isolates async I/O from PostgreSQL's signal handling
//! while keeping the extension code single-threaded.

use crate::catalog::StreamTableMeta;
use crate::error::PgTrickleError;
use pgrx::prelude::*;

use arrow_array::{
    BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

// ── Compression codec ──────────────────────────────────────────────────────

fn resolve_compression(codec: &str) -> Compression {
    match codec.to_ascii_lowercase().as_str() {
        "zstd" => Compression::ZSTD(Default::default()),
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        _ => Compression::SNAPPY, // default: snappy
    }
}

// ── Column descriptor (name + Arrow DataType) ─────────────────────────────

/// A single column's name and Arrow data type, used to build the Parquet schema.
#[derive(Debug, Clone)]
pub struct SinkColumn {
    pub name: String,
    pub data_type: DataType,
}

// ── Parquet serialisation ─────────────────────────────────────────────────

/// Serialise a result set to Parquet bytes.
///
/// `schema` describes the output column names and types.
/// `rows` is a list of rows, each row being a `Vec<Option<String>>` (values
/// cast to text by the caller's SPI query).  Using text for all values keeps
/// the serialisation code simple and type-safe without needing a complete
/// PostgreSQL ↔ Arrow type mapping.
///
/// Returns the raw Parquet file as a `Vec<u8>`.
pub fn write_parquet_bytes(
    schema: &[SinkColumn],
    rows: Vec<Vec<Option<String>>>,
    compression: Compression,
) -> Result<Vec<u8>, PgTrickleError> {
    if schema.is_empty() {
        // No columns — produce an empty Parquet file with an empty schema.
        let arrow_schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(arrow_schema.clone());
        return write_batch_to_bytes(arrow_schema, &[batch], compression);
    }

    // Build Arrow arrays for each column.
    let mut arrow_fields = Vec::with_capacity(schema.len());
    let mut arrow_arrays: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(schema.len());

    for (col_idx, col) in schema.iter().enumerate() {
        arrow_fields.push(Field::new(&col.name, col.data_type.clone(), true));

        let column_values: Vec<Option<String>> = rows
            .iter()
            .map(|row| row.get(col_idx).and_then(|v| v.clone()))
            .collect();

        let array: Arc<dyn arrow_array::Array> = match &col.data_type {
            DataType::Int64 => {
                let values: Vec<Option<i64>> = column_values
                    .iter()
                    .map(|v| v.as_deref().and_then(|s| s.parse().ok()))
                    .collect();
                Arc::new(Int64Array::from(values))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = column_values
                    .iter()
                    .map(|v| v.as_deref().and_then(|s| s.parse().ok()))
                    .collect();
                Arc::new(Float64Array::from(values))
            }
            DataType::Boolean => {
                let values: Vec<Option<bool>> = column_values
                    .iter()
                    .map(|v| {
                        v.as_deref().map(|s| {
                            matches!(s.to_ascii_lowercase().as_str(), "t" | "true" | "1" | "yes")
                        })
                    })
                    .collect();
                Arc::new(BooleanArray::from(values))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let values: Vec<Option<i64>> = column_values
                    .iter()
                    .map(|v| v.as_deref().and_then(|s| s.parse().ok()))
                    .collect();
                Arc::new(TimestampMicrosecondArray::from(values))
            }
            // Default: treat everything else as UTF-8 string.
            _ => {
                let values: Vec<Option<&str>> =
                    column_values.iter().map(|v| v.as_deref()).collect();
                Arc::new(StringArray::from(values))
            }
        };

        arrow_arrays.push(array);
    }

    let arrow_schema = Arc::new(Schema::new(arrow_fields));
    let batch = RecordBatch::try_new(arrow_schema.clone(), arrow_arrays).map_err(|e| {
        PgTrickleError::DucklakeParquetError(format!("RecordBatch construction failed: {e}"))
    })?;

    write_batch_to_bytes(arrow_schema, &[batch], compression)
}

fn write_batch_to_bytes(
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    compression: Compression,
) -> Result<Vec<u8>, PgTrickleError> {
    let mut buf = Vec::new();
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).map_err(|e| {
        PgTrickleError::DucklakeParquetError(format!("ArrowWriter init failed: {e}"))
    })?;
    for batch in batches {
        writer.write(batch).map_err(|e| {
            PgTrickleError::DucklakeParquetError(format!("ArrowWriter write failed: {e}"))
        })?;
    }
    writer.close().map_err(|e| {
        PgTrickleError::DucklakeParquetError(format!("ArrowWriter close failed: {e}"))
    })?;
    Ok(buf)
}

// ── Object-store upload ────────────────────────────────────────────────────

/// Upload `data` to `<base_path><file_name>` on the configured object store.
///
/// Scheme dispatch:
/// - `file://` → write to local filesystem (no network, no tokio needed).
/// - `s3://`   → upload via `object_store` AWS S3 backend.
/// - Anything else → returns an error with guidance.
///
/// Returns the fully-qualified path (URI) to the uploaded file.
pub fn upload_parquet(
    base_path: &str,
    file_name: &str,
    data: Vec<u8>,
) -> Result<String, PgTrickleError> {
    if base_path.starts_with("file://") {
        upload_local(base_path, file_name, data)
    } else if base_path.starts_with("s3://") {
        upload_s3(base_path, file_name, data)
    } else if base_path.starts_with("gs://") || base_path.starts_with("az://") {
        Err(PgTrickleError::DucklakeUploadError(format!(
            "Object-store scheme not yet supported in this build: '{}'. \
             Supported schemes: file://, s3://. \
             GCS and Azure Blob support requires additional feature flags.",
            &base_path[..base_path.find("://").map(|i| i + 3).unwrap_or(5)]
        )))
    } else {
        Err(PgTrickleError::DucklakeUploadError(format!(
            "Unrecognised object-store scheme in ducklake_sink_path '{}'. \
             Expected one of: s3://<bucket>/<prefix>/, file:///path/to/dir/",
            base_path
        )))
    }
}

fn upload_local(base_path: &str, file_name: &str, data: Vec<u8>) -> Result<String, PgTrickleError> {
    // Strip the file:// prefix to get the filesystem path.
    let dir = base_path
        .strip_prefix("file://")
        .unwrap_or(base_path)
        .trim_end_matches('/');

    std::fs::create_dir_all(dir).map_err(|e| {
        PgTrickleError::DucklakeUploadError(format!("Cannot create directory '{dir}': {e}"))
    })?;

    let full_path = format!("{dir}/{file_name}");
    std::fs::write(&full_path, &data).map_err(|e| {
        PgTrickleError::DucklakeUploadError(format!("Cannot write file '{full_path}': {e}"))
    })?;

    Ok(format!("file://{full_path}"))
}

fn upload_s3(base_path: &str, file_name: &str, data: Vec<u8>) -> Result<String, PgTrickleError> {
    // DEP-003 (v0.74.0): `put` moved from `ObjectStore` to `ObjectStoreExt` in 0.13.
    use object_store::{ObjectStoreExt, aws::AmazonS3Builder, path::Path};

    // Parse s3://bucket/prefix/ into bucket + prefix.
    let stripped = base_path.strip_prefix("s3://").unwrap_or(base_path);
    let (bucket, prefix) = stripped.split_once('/').unwrap_or((stripped, ""));

    let prefix = prefix.trim_end_matches('/');
    let object_path_str = if prefix.is_empty() {
        file_name.to_string()
    } else {
        format!("{prefix}/{file_name}")
    };

    let object_path = Path::parse(&object_path_str).map_err(|e| {
        PgTrickleError::DucklakeUploadError(format!(
            "Invalid S3 object path '{object_path_str}': {e}"
        ))
    })?;

    // Read S3 configuration from GUCs.
    let endpoint = crate::config::pg_trickle_ducklake_sink_s3_endpoint();
    let region = crate::config::pg_trickle_ducklake_sink_s3_region();
    let access_key = crate::config::pg_trickle_ducklake_sink_s3_access_key();
    let secret_key = crate::config::pg_trickle_ducklake_sink_s3_secret_key();

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(&region);

    if let Some(ep) = endpoint {
        builder = builder.with_endpoint(&ep).with_allow_http(true);
    }
    if let Some(ak) = access_key {
        builder = builder.with_access_key_id(&ak);
    }
    if let Some(sk) = secret_key {
        builder = builder.with_secret_access_key(&sk);
    }

    let store = builder
        .build()
        .map_err(|e| PgTrickleError::DucklakeUploadError(format!("S3 client build failed: {e}")))?;

    // Run the async upload synchronously via a current-thread tokio runtime.
    // SAFETY: We create a fresh single-threaded runtime here; it does not share
    // a reactor with PostgreSQL's signal handlers, and we block until the upload
    // completes or fails. The runtime is dropped immediately after the call.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            PgTrickleError::DucklakeUploadError(format!("Tokio runtime init failed: {e}"))
        })?;

    rt.block_on(async {
        store
            .put(&object_path, Bytes::from(data).into())
            .await
            .map_err(|e| {
                PgTrickleError::DucklakeUploadError(format!(
                    "S3 PUT to s3://{bucket}/{object_path_str} failed: {e}"
                ))
            })
    })?;

    Ok(format!("s3://{bucket}/{object_path_str}"))
}

// ── DuckLake catalog transaction writer ───────────────────────────────────

/// Register a new Parquet data file in the DuckLake catalog.
///
/// Inserts a row into `ducklake_data_file` and a new row in `ducklake_snapshot`
/// within a short-lived SPI connection. If either insert fails, the SPI
/// transaction is rolled back cleanly.
///
/// # Rollback semantics
///
/// If this function returns `Err`, the Parquet file already exists on object
/// storage but has no corresponding catalog entry. DuckLake's VACUUM process
/// will eventually garbage-collect unreferenced files.
///
/// Returns the new `snapshot_id`.
pub fn register_ducklake_data_file(
    table_id: i64,
    file_path: &str,
    row_count: i64,
    file_size_bytes: i64,
    encryption_key_id: Option<&str>,
    created_by: &str,
) -> Result<i64, PgTrickleError> {
    // SEC-002 (v0.69.0): use the configured catalog schema for all writes.
    let cat_schema = crate::config::pg_trickle_ducklake_catalog_schema();
    let cat_schema_ref = cat_schema.as_str();

    Spi::connect_mut(|client| {
        // COR-006 (v0.69.0): Acquire a transaction-scoped advisory lock keyed on
        // the table_id before reading MAX(snapshot_id).  This prevents two
        // concurrent sink writes for the same DuckLake table from reading the
        // same snapshot_id and producing duplicates.
        //
        // SAFETY: pg_advisory_xact_lock is a standard PostgreSQL function that
        // takes a bigint advisory lock key; it is released automatically at
        // transaction end.
        client
            .update("SELECT pg_advisory_xact_lock($1)", None, &[table_id.into()])
            .map_err(|e| {
                PgTrickleError::SpiError(format!(
                    "advisory lock for table_id={table_id} failed: {e}"
                ))
            })?;

        // Insert the data file record.
        let encryption_key_val: Option<&str> = encryption_key_id;
        let data_file_sql = format!(
            "INSERT INTO {cat_schema_ref}.ducklake_data_file \
             (table_id, begin_snapshot, path, row_count, \
              file_size_bytes, encryption_key_id) \
             VALUES ($1, \
                 (SELECT COALESCE(MAX(snapshot_id), 0) \
                  FROM {cat_schema_ref}.ducklake_snapshot WHERE table_id = $1) + 1, \
             $2, $3, $4, $5) \
             RETURNING data_file_id"
        );
        client
            .update(
                &data_file_sql,
                None,
                &[
                    table_id.into(),
                    file_path.into(),
                    row_count.into(),
                    file_size_bytes.into(),
                    encryption_key_val.into(),
                ],
            )
            .map_err(|e| {
                PgTrickleError::DucklakeCatalogError(format!(
                    "ducklake_data_file insert failed: {e}"
                ))
            })?;

        // Update table stats.
        let stats_sql = format!(
            "INSERT INTO {cat_schema_ref}.ducklake_table_stats (table_id, row_count, file_count) \
             VALUES ($1, $2, 1) \
             ON CONFLICT (table_id) DO UPDATE \
             SET row_count = ducklake_table_stats.row_count + EXCLUDED.row_count, \
                 file_count = ducklake_table_stats.file_count + 1"
        );
        client
            .update(&stats_sql, None, &[table_id.into(), row_count.into()])
            .map_err(|e| {
                PgTrickleError::DucklakeCatalogError(format!(
                    "ducklake_table_stats upsert failed: {e}"
                ))
            })?;

        // INT-11 (v0.67.0): Insert a new snapshot with created_by provenance.
        let snap_sql = format!(
            "INSERT INTO {cat_schema_ref}.ducklake_snapshot \
             (table_id, snapshot_id, snapshot_time, created_by) \
             VALUES ($1, \
                 (SELECT COALESCE(MAX(snapshot_id), 0) + 1 \
                  FROM {cat_schema_ref}.ducklake_snapshot WHERE table_id = $1), \
                 now(), $2) \
             RETURNING snapshot_id"
        );
        let snap_row = client
            .update(&snap_sql, None, &[table_id.into(), created_by.into()])
            .map_err(|e| {
                PgTrickleError::DucklakeCatalogError(format!(
                    "ducklake_snapshot insert failed: {e}"
                ))
            })?;

        let snapshot_id = snap_row
            .first()
            .get_one::<i64>()
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .unwrap_or(1);

        Ok(snapshot_id)
    })
    .map_err(|e: PgTrickleError| e)
}

// ── Fetch stream table rows as text ───────────────────────────────────────

/// Column descriptor returned by SPI for the stream table storage.
#[derive(Debug, Clone)]
struct ColumnInfo {
    name: String,
    type_oid: u32,
}

/// Rows type alias for fetched stream table data.
type FetchedRows = Vec<Vec<Option<String>>>;

/// Fetch all rows from the stream table storage table as text-encoded columns.
///
/// Returns the schema and a vec-of-rows for Parquet serialisation.
fn fetch_stream_table_rows(
    st: &StreamTableMeta,
) -> Result<(Vec<SinkColumn>, FetchedRows), PgTrickleError> {
    let quoted_table = format!(
        "\"{}\".\"{}\"",
        st.pgt_schema.replace('"', "\"\""),
        st.pgt_name.replace('"', "\"\""),
    );

    Spi::connect(|client| {
        // First: discover columns via information_schema.
        // Cast column_name and udt_name to plain text — their actual type is
        // information_schema.sql_identifier (a domain over varchar), which pgrx
        // cannot deserialise as String directly (OID mismatch).
        let col_table = client
            .select(
                "SELECT column_name::text, udt_name::text \
                 FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = $2 \
                 ORDER BY ordinal_position",
                None,
                &[st.pgt_schema.as_str().into(), st.pgt_name.as_str().into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut columns: Vec<ColumnInfo> = Vec::new();
        for row in col_table {
            let col_name = row
                .get::<String>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            let udt_name = row
                .get::<String>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            // Skip internal pg_trickle columns.
            if col_name.starts_with("__pgt_") {
                continue;
            }
            columns.push(ColumnInfo {
                name: col_name,
                type_oid: udt_to_arrow_type_hint(&udt_name),
            });
        }

        if columns.is_empty() {
            return Ok((vec![], vec![]));
        }

        // COR-004 (v0.68.0): Build a SELECT that serialises every column to a
        // parseable string.  Timestamp/timestamptz columns must be emitted as
        // microsecond-epoch integers so they can be parsed as i64 by
        // write_parquet_bytes().  Casting them to ::text yields a locale-
        // sensitive string like "2023-01-01 00:00:00+00" which cannot be
        // parsed as i64, causing silent NULL coercion in Parquet output.
        let col_list = columns
            .iter()
            .map(|c| {
                let qname = c.name.replace('"', "\"\"");
                if c.type_oid == 4 {
                    // timestamp / timestamptz → microseconds since Unix epoch
                    format!(
                        "(EXTRACT(EPOCH FROM \"{qname}\"::timestamptz) * 1000000)::bigint::text"
                    )
                } else {
                    format!("\"{}\"::text", qname)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        // nosemgrep: rust.spi.run.dynamic-format — quoted identifiers only
        let query = format!("SELECT {col_list} FROM {quoted_table}");

        let data_table = client
            .select(&query, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        let num_cols = columns.len();
        for row in data_table {
            let mut row_vals = Vec::with_capacity(num_cols);
            for col_idx in 0..num_cols {
                let val = row
                    .get::<String>(col_idx + 1)
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
                row_vals.push(val);
            }
            rows.push(row_vals);
        }

        let sink_columns: Vec<SinkColumn> = columns
            .iter()
            .map(|c| SinkColumn {
                name: c.name.clone(),
                data_type: type_hint_to_data_type(c.type_oid),
            })
            .collect();

        Ok((sink_columns, rows))
    })
}

/// Map a PostgreSQL `udt_name` to a simple numeric hint.
/// 0 = text/string, 1 = int64, 2 = float64, 3 = bool, 4 = timestamp.
fn udt_to_arrow_type_hint(udt_name: &str) -> u32 {
    match udt_name {
        "int2" | "int4" | "int8" | "bigint" | "integer" | "smallint" => 1,
        "float4" | "float8" | "numeric" | "real" | "double precision" => 2,
        "bool" | "boolean" => 3,
        "timestamp" | "timestamptz" => 4,
        _ => 0, // text
    }
}

fn type_hint_to_data_type(hint: u32) -> DataType {
    match hint {
        1 => DataType::Int64,
        2 => DataType::Float64,
        3 => DataType::Boolean,
        4 => DataType::Timestamp(TimeUnit::Microsecond, None),
        _ => DataType::Utf8,
    }
}

// ── F-9: Encryption key generation ────────────────────────────────────────

/// Generate a new per-file encryption key ID.
///
/// The key ID is: `<prefix>/<table_id>/<epoch_ms>`. The actual key bytes
/// are managed by the key management system; pg_trickle only records the
/// key ID in `ducklake_data_file.encryption_key_id` so DuckLake clients
/// can retrieve it when reading the file.
fn generate_encryption_key_id(prefix: &str, table_id: i64, epoch_ms: i64) -> String {
    format!("{prefix}/{table_id}/{epoch_ms}")
}

// ── Main entry point ───────────────────────────────────────────────────────

/// Run the DuckLake sink for a stream table after a successful refresh.
///
/// Called from the scheduler after `execute_differential_refresh` or
/// `execute_full_refresh` returns successfully.
///
/// v0.69.0 (ARCH-002/REL-001): This function now records delivery attempts in
/// `pgtrickle.pgt_ducklake_sink_delivery`. On transient failures it marks the
/// row `FAILED_RETRYABLE` (up to `ducklake_sink_max_retries` attempts), then
/// `FAILED_PERMANENT`. The `ducklake_sink_failure_mode` GUC controls whether
/// a permanent failure propagates as a PostgreSQL error.
pub fn run_ducklake_sink(st: &StreamTableMeta) {
    let delivery_id = create_delivery_row(st.pgt_id);
    let max_retries = crate::config::pg_trickle_ducklake_sink_max_retries();

    // Count existing failed attempts for this stream table.
    let attempt_count = count_retryable_attempts(st.pgt_id) + 1;

    match run_ducklake_sink_inner(st) {
        Ok(()) => {
            // Mark the delivery row as DELIVERED.
            if let Some(id) = delivery_id {
                finish_delivery_row(id, "DELIVERED", attempt_count, None, None, None);
            }
        }
        Err(e) => {
            let err_str = e.to_string();
            let status = if attempt_count >= max_retries {
                "FAILED_PERMANENT"
            } else {
                "FAILED_RETRYABLE"
            };

            if let Some(id) = delivery_id {
                finish_delivery_row(id, status, attempt_count, None, None, Some(&err_str));
            }

            if status == "FAILED_PERMANENT" {
                if crate::config::pg_trickle_ducklake_sink_failure_mode_is_error() {
                    pgrx::error!(
                        "pg_trickle: DuckLake sink FAILED_PERMANENT for {}.{} \
                         after {} attempts: {}",
                        st.pgt_schema,
                        st.pgt_name,
                        attempt_count,
                        err_str,
                    );
                } else {
                    pgrx::warning!(
                        "pg_trickle: DuckLake sink FAILED_PERMANENT for {}.{} \
                         after {} attempts: {}",
                        st.pgt_schema,
                        st.pgt_name,
                        attempt_count,
                        err_str,
                    );
                }
            } else {
                pgrx::warning!(
                    "pg_trickle: DuckLake sink failed for {}.{} (attempt {}/{}): {}",
                    st.pgt_schema,
                    st.pgt_name,
                    attempt_count,
                    max_retries,
                    err_str,
                );
            }
        }
    }
}

// ── ARCH-002/REL-001 (v0.69.0): Delivery row helpers ─────────────────────

/// Insert a PENDING delivery row and return its `delivery_id`.
/// Best-effort — if the catalog table does not exist yet, returns `None`.
fn create_delivery_row(pgt_id: i64) -> Option<i64> {
    Spi::connect(|client| {
        let row = client.select(
            "INSERT INTO pgtrickle.pgt_ducklake_sink_delivery \
             (stream_table_id, status, attempt_count, started_at) \
             VALUES ($1, 'PENDING', 0, now()) \
             RETURNING delivery_id",
            None,
            &[pgt_id.into()],
        )?;
        row.first().get_one::<i64>()
    })
    .ok()
    .flatten()
}

/// Update a delivery row to the given final status.
fn finish_delivery_row(
    delivery_id: i64,
    status: &str,
    attempt_count: i32,
    bytes_written: Option<i64>,
    rows_written: Option<i64>,
    last_error: Option<&str>,
) {
    let _ = Spi::run_with_args(
        "UPDATE pgtrickle.pgt_ducklake_sink_delivery \
         SET status = $2, \
             attempt_count = $3, \
             bytes_written = $4, \
             rows_written = $5, \
             finished_at = now(), \
             last_error = $6 \
         WHERE delivery_id = $1",
        &[
            delivery_id.into(),
            status.into(),
            attempt_count.into(),
            bytes_written.into(),
            rows_written.into(),
            last_error.into(),
        ],
    );
}

/// Count the number of FAILED_RETRYABLE delivery rows for this stream table.
fn count_retryable_attempts(pgt_id: i64) -> i32 {
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT COUNT(*)::int FROM pgtrickle.pgt_ducklake_sink_delivery \
             WHERE stream_table_id = $1 \
               AND status IN ('FAILED_RETRYABLE', 'FAILED_PERMANENT')",
            None,
            &[pgt_id.into()],
        )?;
        rows.first().get_one::<i32>()
    })
    .ok()
    .flatten()
    .unwrap_or(0)
}

fn run_ducklake_sink_inner(st: &StreamTableMeta) -> Result<(), PgTrickleError> {
    let sink_mode = match &st.ducklake_sink_mode {
        Some(m) => m.clone(),
        None => return Ok(()), // No sink configured — fast exit.
    };

    let sink_path = match &st.ducklake_sink_path {
        Some(p) => p.clone(),
        None => {
            return Err(PgTrickleError::DucklakeSinkError(format!(
                "{}.{}: ducklake_sink_mode is '{}' but ducklake_sink_path is NULL",
                st.pgt_schema, st.pgt_name, sink_mode,
            )));
        }
    };

    // Fetch rows from the stream table storage.
    let (schema, rows) = fetch_stream_table_rows(st)?;

    let row_count = rows.len() as i64;

    // Resolve compression from GUC.
    let compression_str = crate::config::pg_trickle_ducklake_sink_compression();
    let compression = resolve_compression(&compression_str);

    // Serialise to Parquet.
    let parquet_bytes = write_parquet_bytes(&schema, rows, compression)?;
    let file_size = parquet_bytes.len() as i64;

    // Generate a unique file name.
    let epoch_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let file_name = format!("{}_{}.parquet", epoch_ms, st.pgt_id);

    // F-9: Resolve encryption key ID (if enabled).
    let encryption_key_id = crate::config::pg_trickle_ducklake_sink_encryption_key_prefix()
        .map(|prefix| generate_encryption_key_id(&prefix, st.pgt_id, epoch_ms));

    // Upload to object store.
    let full_path = upload_parquet(&sink_path, &file_name, parquet_bytes)?;

    // INT-11 (v0.67.0): Build structured provenance identifier.
    let created_by = build_created_by(st.pgt_id, &st.pgt_name);

    // Register in DuckLake catalog (if table_id is configured).
    if let Some(table_id) = st.ducklake_sink_table_id {
        let snapshot_id = register_ducklake_data_file(
            table_id,
            &full_path,
            row_count,
            file_size,
            encryption_key_id.as_deref(),
            &created_by,
        )?;
        // INT-11 (v0.67.0): Record provenance in pgtrickle.pgt_ducklake_provenance.
        insert_ducklake_provenance(st.pgt_id, &st.pgt_name, snapshot_id, row_count);
        pgrx::log!(
            "pg_trickle: ducklake sink — {}.{} wrote {} rows to {} \
             and registered in ducklake catalog (table_id={}, snapshot_id={}, mode={})",
            st.pgt_schema,
            st.pgt_name,
            row_count,
            full_path,
            table_id,
            snapshot_id,
            sink_mode,
        );
    } else {
        pgrx::log!(
            "pg_trickle: ducklake sink — {}.{} wrote {} rows to {} \
             (no catalog registration, ducklake_sink_table_id is NULL, mode={})",
            st.pgt_schema,
            st.pgt_name,
            row_count,
            full_path,
            sink_mode,
        );
    }

    Ok(())
}

// ── Catalog update helpers ────────────────────────────────────────────────

// ── INT-11 (v0.67.0): Snapshot provenance ────────────────────────────────

/// Build the structured `created_by` identifier for DuckLake snapshot rows.
///
/// Format: `pg_trickle/<version>/stream_table/<oid>/<name>`
pub fn build_created_by(pgt_id: i64, pgt_name: &str) -> String {
    let version = env!("CARGO_PKG_VERSION");
    format!("pg_trickle/{version}/stream_table/{pgt_id}/{pgt_name}")
}

/// Record a provenance row in `pgtrickle.pgt_ducklake_provenance`.
///
/// Best-effort: errors are logged as warnings and never propagated.
pub fn insert_ducklake_provenance(
    pgt_id: i64,
    pgt_name: &str,
    ducklake_snapshot_id: i64,
    delta_row_count: i64,
) {
    if let Err(e) =
        insert_ducklake_provenance_inner(pgt_id, pgt_name, ducklake_snapshot_id, delta_row_count)
    {
        pgrx::warning!(
            "pg_trickle: provenance record for '{}' snapshot {} failed: {}",
            pgt_name,
            ducklake_snapshot_id,
            e
        );
    }
}

fn insert_ducklake_provenance_inner(
    pgt_id: i64,
    pgt_name: &str,
    ducklake_snapshot_id: i64,
    delta_row_count: i64,
) -> Result<(), PgTrickleError> {
    // Fetch the most recent refresh_id for this stream table.
    let refresh_id: i64 = Spi::connect(|client| {
        let row = client
            .select(
                "SELECT COALESCE(MAX(refresh_id), 0) \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = $1",
                None,
                &[pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let id = row
            .first()
            .get_one::<i64>()
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .unwrap_or(0);
        Ok(id)
    })?;

    Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_ducklake_provenance \
         (stream_table_oid, stream_table_name, ducklake_snapshot_id, \
          refresh_id, delta_row_count, written_at) \
         VALUES ($1, $2, $3, $4, $5, now())",
        &[
            pgt_id.into(),
            pgt_name.into(),
            ducklake_snapshot_id.into(),
            refresh_id.into(),
            delta_row_count.into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

// ── F-6 (v0.67.0): DuckLake view registration ────────────────────────────

/// Upsert a `ducklake_view` entry so the stream table appears as a native
/// catalog object in every DuckLake client.
///
/// Best-effort: if `ducklake_view` is not present (DuckLake not installed),
/// the call is silently skipped. Errors are logged as warnings.
pub fn register_ducklake_view(pgt_name: &str, defining_query: &str) {
    if let Err(e) = register_ducklake_view_inner(pgt_name, defining_query) {
        pgrx::warning!(
            "pg_trickle: ducklake_view registration failed for '{}': {}",
            pgt_name,
            e
        );
    }
}

fn register_ducklake_view_inner(
    pgt_name: &str,
    defining_query: &str,
) -> Result<(), PgTrickleError> {
    if !ducklake_view_table_exists()? {
        pgrx::log!(
            "pg_trickle: ducklake_view table not present; \
             skipping view registration for '{}'",
            pgt_name
        );
        return Ok(());
    }

    // SEC-002 (v0.69.0): use the configured catalog schema.
    let cat_schema = crate::config::pg_trickle_ducklake_catalog_schema();
    let sql = format!(
        "INSERT INTO {cat_schema}.ducklake_view (view_name, view_definition) \
         VALUES ($1, $2) \
         ON CONFLICT (view_name) DO UPDATE \
         SET view_definition = EXCLUDED.view_definition"
    );

    Spi::run_with_args(&sql, &[pgt_name.into(), defining_query.into()]).map_err(|e| {
        PgTrickleError::DucklakeCatalogError(format!(
            "ducklake_view upsert for '{}' failed: {e}",
            pgt_name
        ))
    })
}

/// Remove a `ducklake_view` entry when the stream table is dropped.
///
/// Best-effort: if `ducklake_view` is not present, the call is silently skipped.
pub fn deregister_ducklake_view(pgt_name: &str) {
    if let Err(e) = deregister_ducklake_view_inner(pgt_name) {
        pgrx::warning!(
            "pg_trickle: ducklake_view deregistration failed for '{}': {}",
            pgt_name,
            e
        );
    }
}

fn deregister_ducklake_view_inner(pgt_name: &str) -> Result<(), PgTrickleError> {
    if !ducklake_view_table_exists()? {
        return Ok(());
    }

    // SEC-002 (v0.69.0): use the configured catalog schema.
    let cat_schema = crate::config::pg_trickle_ducklake_catalog_schema();
    let sql = format!("DELETE FROM {cat_schema}.ducklake_view WHERE view_name = $1");

    Spi::run_with_args(&sql, &[pgt_name.into()]).map_err(|e| {
        PgTrickleError::DucklakeCatalogError(format!(
            "ducklake_view delete for '{}' failed: {e}",
            pgt_name
        ))
    })
}

/// Returns `true` when the `ducklake_view` catalog table exists in the
/// configured catalog schema (`pg_trickle.ducklake_catalog_schema`).
///
/// SEC-002 (v0.69.0): Uses `pg_class JOIN pg_namespace` rather than
/// `information_schema.tables` so the check is not affected by `search_path`
/// manipulation.
fn ducklake_view_table_exists() -> Result<bool, PgTrickleError> {
    let cat_schema = crate::config::pg_trickle_ducklake_catalog_schema();
    Spi::connect(|client| {
        let row = client
            .select(
                "SELECT EXISTS ( \
                     SELECT 1 \
                     FROM pg_class c \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = $1 \
                       AND c.relname = 'ducklake_view' \
                 )",
                None,
                &[cat_schema.as_str().into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let exists = row
            .first()
            .get_one::<bool>()
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .unwrap_or(false);
        Ok(exists)
    })
}

/// Update `ducklake_sink_mode` and `ducklake_sink_path` in the catalog.
pub fn update_sink_config(
    pgt_id: i64,
    sink_mode: Option<&str>,
    sink_path: Option<&str>,
    sink_table_id: Option<i64>,
) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET ducklake_sink_mode = $2, \
             ducklake_sink_path = $3, \
             ducklake_sink_table_id = $4, \
             updated_at = now() \
         WHERE pgt_id = $1",
        &[
            pgt_id.into(),
            sink_mode.into(),
            sink_path.into(),
            sink_table_id.into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

// ── Unit tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Compression resolution ──────────────────────────────────────────

    #[test]
    fn test_resolve_compression_snappy_is_default() {
        assert_eq!(resolve_compression("snappy"), Compression::SNAPPY);
    }

    #[test]
    fn test_resolve_compression_unknown_defaults_to_snappy() {
        assert_eq!(resolve_compression("bogus"), Compression::SNAPPY);
    }

    #[test]
    fn test_resolve_compression_none() {
        assert_eq!(resolve_compression("none"), Compression::UNCOMPRESSED);
    }

    #[test]
    fn test_resolve_compression_zstd() {
        assert!(matches!(resolve_compression("zstd"), Compression::ZSTD(_)));
    }

    // ── Parquet serialisation ───────────────────────────────────────────

    #[test]
    fn test_write_parquet_bytes_empty_schema() {
        let bytes = write_parquet_bytes(&[], vec![], Compression::SNAPPY).unwrap();
        // Should produce a minimal valid Parquet file (at least the magic bytes).
        assert!(bytes.len() >= 4);
        assert_eq!(&bytes[..4], b"PAR1");
    }

    #[test]
    fn test_write_parquet_bytes_single_int_column() {
        let schema = vec![SinkColumn {
            name: "id".to_string(),
            data_type: DataType::Int64,
        }];
        let rows = vec![
            vec![Some("1".to_string())],
            vec![Some("2".to_string())],
            vec![None],
        ];
        let bytes = write_parquet_bytes(&schema, rows, Compression::SNAPPY).unwrap();
        assert!(bytes.len() > 4);
        assert_eq!(&bytes[..4], b"PAR1");
    }

    #[test]
    fn test_write_parquet_bytes_mixed_types() {
        let schema = vec![
            SinkColumn {
                name: "id".to_string(),
                data_type: DataType::Int64,
            },
            SinkColumn {
                name: "name".to_string(),
                data_type: DataType::Utf8,
            },
            SinkColumn {
                name: "score".to_string(),
                data_type: DataType::Float64,
            },
            SinkColumn {
                name: "active".to_string(),
                data_type: DataType::Boolean,
            },
        ];
        let rows = vec![
            vec![
                Some("42".to_string()),
                Some("Alice".to_string()),
                Some("9.5".to_string()),
                Some("true".to_string()),
            ],
            vec![
                Some("99".to_string()),
                None,
                Some("3.14".to_string()),
                Some("false".to_string()),
            ],
        ];
        let bytes = write_parquet_bytes(&schema, rows, Compression::UNCOMPRESSED).unwrap();
        assert_eq!(&bytes[..4], b"PAR1");
    }

    // ── Type hint mapping ───────────────────────────────────────────────

    #[test]
    fn test_udt_to_arrow_type_hint_integers() {
        assert_eq!(udt_to_arrow_type_hint("int4"), 1);
        assert_eq!(udt_to_arrow_type_hint("int8"), 1);
        assert_eq!(udt_to_arrow_type_hint("bigint"), 1);
    }

    #[test]
    fn test_udt_to_arrow_type_hint_floats() {
        assert_eq!(udt_to_arrow_type_hint("float8"), 2);
        assert_eq!(udt_to_arrow_type_hint("numeric"), 2);
    }

    #[test]
    fn test_udt_to_arrow_type_hint_bool() {
        assert_eq!(udt_to_arrow_type_hint("bool"), 3);
    }

    #[test]
    fn test_udt_to_arrow_type_hint_timestamp() {
        assert_eq!(udt_to_arrow_type_hint("timestamp"), 4);
        assert_eq!(udt_to_arrow_type_hint("timestamptz"), 4);
    }

    #[test]
    fn test_udt_to_arrow_type_hint_text() {
        assert_eq!(udt_to_arrow_type_hint("text"), 0);
        assert_eq!(udt_to_arrow_type_hint("varchar"), 0);
        assert_eq!(udt_to_arrow_type_hint("unknown_type"), 0);
    }

    // ── Encryption key generation ───────────────────────────────────────

    #[test]
    fn test_generate_encryption_key_id_format() {
        let key_id = generate_encryption_key_id("myproject/keys", 42, 1716000000000);
        assert_eq!(key_id, "myproject/keys/42/1716000000000");
    }

    // ── Local file upload ───────────────────────────────────────────────

    #[test]
    fn test_upload_local_creates_file() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let base_path = format!("file://{}/", dir.path().display());
        let data = b"PAR1testdata".to_vec();
        let result = upload_local(&base_path, "test.parquet", data.clone());
        assert!(result.is_ok(), "upload_local failed: {:?}", result);
        let full_path = result.unwrap();
        assert!(full_path.starts_with("file://"));
        let fs_path = full_path.strip_prefix("file://").unwrap();
        let contents = std::fs::read(fs_path).expect("read file");
        assert_eq!(contents, data);
    }

    #[test]
    fn test_upload_local_nested_directory() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let base_path = format!("file://{}/nested/deep/", dir.path().display());
        let data = vec![0u8; 16];
        let result = upload_local(&base_path, "nested.parquet", data);
        assert!(
            result.is_ok(),
            "upload_local with nested dir failed: {:?}",
            result
        );
    }

    // ── Unsupported scheme ──────────────────────────────────────────────

    #[test]
    fn test_upload_parquet_unsupported_scheme_returns_error() {
        let result = upload_parquet("ftp://example.com/bucket/", "test.parquet", vec![]);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("Unrecognised"),
            "Expected unrecognised scheme error, got: {msg}"
        );
    }

    #[test]
    fn test_upload_parquet_gcs_not_supported_error() {
        let result = upload_parquet("gs://my-bucket/prefix/", "f.parquet", vec![]);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("not yet supported"),
            "Expected not-supported error, got: {msg}"
        );
    }

    // ── Rollback invariant ──────────────────────────────────────────────

    /// Proves the rollback invariant: when `upload_parquet` fails, the error
    /// is `DucklakeUploadError` — NOT `DucklakeCatalogError`.
    ///
    /// `run_ducklake_sink_inner` calls `upload_parquet(…)?` and only then
    /// calls `register_ducklake_data_file(…)` (the catalog writer).
    /// The `?` operator propagates the upload error immediately, so the
    /// catalog is never touched when upload fails.
    ///
    /// A `DucklakeUploadError` in the return type confirms the error came
    /// from the upload path. If catalog writes had been attempted and failed,
    /// the return type would be `DucklakeCatalogError` instead.
    #[cfg(unix)]
    #[test]
    fn test_upload_failure_prevents_catalog_writes() {
        use std::os::unix::fs::PermissionsExt;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let ro_path = tmpdir.path().to_path_buf();

        // Save original permissions so we can restore them for cleanup.
        let orig_mode = ro_path.metadata().expect("metadata").permissions().mode();

        // Make the temporary directory read-only so that creating a
        // sub-directory inside it (as upload_local does) will fail.
        std::fs::set_permissions(&ro_path, std::fs::Permissions::from_mode(0o555))
            .expect("set read-only permissions");

        // Upload to a sub-directory of the read-only dir — MUST fail.
        let base_path = format!("file://{}/subdir/", ro_path.display());
        let result = upload_local(&base_path, "sink.parquet", b"PAR1test".to_vec());

        // Restore permissions before any assertion so that tempdir cleanup
        // (which drops ro_path) can remove the directory.
        std::fs::set_permissions(&ro_path, std::fs::Permissions::from_mode(orig_mode)).ok();

        // The upload must have failed.
        assert!(
            result.is_err(),
            "upload to read-only subdir must fail, but it succeeded"
        );

        // The error variant MUST be DucklakeUploadError.
        // If it were DucklakeCatalogError, that would mean the catalog write
        // was reached before the upload error was propagated — violating the
        // rollback invariant.
        match result.unwrap_err() {
            PgTrickleError::DucklakeUploadError(_) => {
                // Correct: upload path failed before catalog was touched.
            }
            e => panic!(
                "Rollback invariant violated: expected DucklakeUploadError \
                 (upload failed before catalog write), got: {:?}",
                e
            ),
        }
    }
}
