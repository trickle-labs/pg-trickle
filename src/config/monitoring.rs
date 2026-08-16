//! Metrics, alerting, and distributed-tracing GUCs.

use pgrx::guc::*;

// ── GUC statics ───────────────────────────────────────────────────────────

/// Prometheus metrics exporter port.
///
/// When set to a non-zero value, the background launcher starts a lightweight
/// HTTP server on `pg_trickle.metrics_bind_address:<port>` that exposes the
/// pg_trickle metrics in Prometheus exposition format at `/metrics`.
///
/// Set to 0 (default) to disable the exporter.
pub static PGS_METRICS_PORT: GucSetting<i32> = GucSetting::<i32>::new(0);

/// Prometheus metrics exporter bind address.
///
/// Must be a literal IPv4 or IPv6 address. Defaults to loopback; use
/// `0.0.0.0` or `::` only when remote exposure is explicitly intended.
pub static PGS_METRICS_BIND_ADDRESS: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"127.0.0.1"));

/// History retention in days.
///
/// The background launcher prunes rows from `pgtrickle.pgt_refresh_history`
/// and `pgtrickle.pgt_error_log` that are older than this many days.
///
/// Default: 90 days.
pub static PGS_HISTORY_RETENTION_DAYS: GucSetting<i32> = GucSetting::<i32>::new(90);

/// MON-2: Self-monitoring auto-apply mode.
///
/// Controls when self-monitoring insights are automatically applied:
/// - `"off"` (default): Never auto-apply.
/// - `"threshold_only"`: Only apply threshold-based rules automatically.
/// - `"full"`: Apply all monitoring-based optimizations automatically.
pub static PGS_SELF_MONITORING_AUTO_APPLY: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"off"));

/// OTEL-1: Enable distributed trace context propagation.
///
/// When `true`, trace context is propagated through refresh cycles for
/// distributed tracing with OpenTelemetry.
pub static PGS_ENABLE_TRACE_PROPAGATION: GucSetting<bool> = GucSetting::<bool>::new(false);

/// OTEL-1: OpenTelemetry endpoint URL for trace export.
///
/// When set, pg_trickle will attempt to export trace spans to this
/// OpenTelemetry collector endpoint. Example: `http://localhost:4318/v1/traces`.
///
/// Set to `NULL` / empty to disable OTEL export (default).
pub static PGS_OTEL_ENDPOINT: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

/// OTEL-1: Default trace ID for correlating pg_trickle operations.
///
/// When set, this trace ID is included in all log lines and metrics
/// emitted during refresh cycles. Primarily useful for test fixtures
/// and debugging.
///
/// Set to `NULL` to disable trace ID injection (default).
pub static PGS_TRACE_ID: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

/// LOG-1: Log format for structured logging.
///
/// - `"text"` (default): Standard PostgreSQL log format.
/// - `"json"`: Emit all pg_trickle log lines as JSON objects for log
///   aggregation pipelines (e.g., Fluentd, Loki, Datadog).
pub static PGS_LOG_FORMAT: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"text"));

/// Request timeout (milliseconds) for outbound metrics HTTP requests.
///
/// When the metrics exporter or OTEL client sends an HTTP request,
/// it is cancelled if it does not complete within this many milliseconds.
///
/// Default: 5000 (5 seconds). Range: 100–60000.
pub static PGS_METRICS_REQUEST_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(5000);

/// SLA-1: SLA compliance measurement window in hours.
///
/// Controls the rolling window used to compute the `sla_compliance_ratio`
/// metric: the fraction of scheduled refreshes that completed within the
/// configured target latency in the past N hours.
///
/// Default: 24 hours. Range: 1–8760 (1 hour – 1 year).
pub static PGS_SLA_WINDOW_HOURS: GucSetting<i32> = GucSetting::<i32>::new(24);

// ── Enums ─────────────────────────────────────────────────────────────────

/// MON-2: Self-monitoring auto-apply mode enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfMonitoringAutoApply {
    /// Never auto-apply (default).
    Off,
    /// Only apply threshold-based rules automatically.
    ThresholdOnly,
    /// Apply all monitoring-based optimizations automatically.
    Full,
}

impl SelfMonitoringAutoApply {
    pub fn as_str(self) -> &'static str {
        match self {
            SelfMonitoringAutoApply::Off => "off",
            SelfMonitoringAutoApply::ThresholdOnly => "threshold_only",
            SelfMonitoringAutoApply::Full => "full",
        }
    }
}

pub(crate) fn normalize_self_monitoring_auto_apply(
    value: Option<String>,
) -> SelfMonitoringAutoApply {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("threshold_only") => SelfMonitoringAutoApply::ThresholdOnly,
        Some("full") => SelfMonitoringAutoApply::Full,
        _ => SelfMonitoringAutoApply::Off,
    }
}

/// LOG-1: Log format enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Standard PostgreSQL log format (default).
    Text,
    /// JSON-structured log output.
    Json,
}

impl LogFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            LogFormat::Text => "text",
            LogFormat::Json => "json",
        }
    }
}

pub(crate) fn normalize_log_format(value: Option<String>) -> LogFormat {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("json") => LogFormat::Json,
        _ => LogFormat::Text,
    }
}

// ── Registration ──────────────────────────────────────────────────────────

/// Register all monitoring-related GUC variables.
pub fn register_monitoring_gucs() {
    GucRegistry::define_int_guc(
        c"pg_trickle.metrics_port",
        c"Prometheus metrics exporter port (0 = disabled).",
        c"When non-zero, starts an HTTP server on the configured bind address \
           exposing metrics in Prometheus exposition format at /metrics. Set \
           to 0 to disable.",
        &PGS_METRICS_PORT,
        0,
        65535,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trickle.metrics_bind_address",
        c"Prometheus metrics exporter bind address.",
        c"Literal IPv4 or IPv6 address (default: 127.0.0.1). Use 0.0.0.0 or :: \
           only when remote exposure is explicitly intended.",
        &PGS_METRICS_BIND_ADDRESS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.history_retention_days",
        c"Number of days to retain refresh history and error log entries.",
        c"Rows in pgtrickle.pgt_refresh_history and pgtrickle.pgt_error_log older than \
           this many days are pruned by the background launcher. Default: 90.",
        &PGS_HISTORY_RETENTION_DAYS,
        1,
        3650,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trickle.self_monitoring_auto_apply",
        c"MON-2: Self-monitoring auto-apply mode: off, threshold_only, or full.",
        c"'off' (default) never auto-applies monitoring insights. \
           'threshold_only' applies threshold-based rules automatically. \
           'full' applies all monitoring-based optimizations automatically.",
        &PGS_SELF_MONITORING_AUTO_APPLY,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.enable_trace_propagation",
        c"OTEL-1: Enable distributed trace context propagation through refresh cycles.",
        c"When true, trace context is propagated for distributed tracing with OpenTelemetry. \
           Requires pg_trickle.otel_endpoint to be set for trace export.",
        &PGS_ENABLE_TRACE_PROPAGATION,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trickle.otel_endpoint",
        c"OTEL-1: OpenTelemetry collector endpoint URL (NULL = disabled).",
        c"When set, pg_trickle exports trace spans to this endpoint. \
           Example: http://localhost:4318/v1/traces. \
           Set to NULL or empty to disable trace export.",
        &PGS_OTEL_ENDPOINT,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trickle.trace_id",
        c"OTEL-1: Default trace ID for correlating pg_trickle log lines (NULL = disabled).",
        c"When set, this trace ID is included in all log lines and metrics. \
           Primarily useful for test fixtures and debugging correlation.",
        &PGS_TRACE_ID,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trickle.log_format",
        c"LOG-1: Log output format: text (default) or json.",
        c"'text' uses standard PostgreSQL log format. \
           'json' emits all pg_trickle log lines as JSON objects for log \
           aggregation pipelines (Fluentd, Loki, Datadog, etc.).",
        &PGS_LOG_FORMAT,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.metrics_request_timeout_ms",
        c"Request timeout (ms) for outbound metrics HTTP requests.",
        c"Outbound HTTP requests (metrics exporter, OTEL) are cancelled if they \
           do not complete within this many milliseconds. Default: 5000 (5 s).",
        &PGS_METRICS_REQUEST_TIMEOUT_MS,
        100,
        60_000,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.sla_window_hours",
        c"SLA-1: Rolling window (hours) for SLA compliance measurement.",
        c"The sla_compliance_ratio metric counts the fraction of scheduled refreshes \
           that completed within the target latency over the past N hours. \
           Default: 24. Range: 1–8760.",
        &PGS_SLA_WINDOW_HOURS,
        1,
        8760,
        GucContext::Suset,
        GucFlags::default(),
    );
}

// ── Accessor functions ────────────────────────────────────────────────────

/// Returns the Prometheus metrics exporter port (0 = disabled).
pub fn pg_trickle_metrics_port() -> i32 {
    PGS_METRICS_PORT.get()
}

/// Returns the configured Prometheus metrics bind address.
pub fn pg_trickle_metrics_bind_address() -> String {
    PGS_METRICS_BIND_ADDRESS
        .get()
        .and_then(|value| value.to_str().ok().map(str::to_owned))
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

/// Returns the history retention period in days.
pub fn pg_trickle_history_retention_days() -> i32 {
    PGS_HISTORY_RETENTION_DAYS.get()
}

/// MON-2: Returns the self-monitoring auto-apply mode.
pub fn pg_trickle_self_monitoring_auto_apply() -> SelfMonitoringAutoApply {
    normalize_self_monitoring_auto_apply(
        PGS_SELF_MONITORING_AUTO_APPLY
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// OTEL-1: Returns whether distributed trace propagation is enabled.
pub fn pg_trickle_enable_trace_propagation() -> bool {
    PGS_ENABLE_TRACE_PROPAGATION.get()
}

/// OTEL-1: Returns the OTEL collector endpoint, or `None` when disabled.
pub fn pg_trickle_otel_endpoint() -> Option<String> {
    PGS_OTEL_ENDPOINT
        .get()
        .and_then(|cs| cs.to_str().ok().map(str::to_owned))
        .filter(|s| !s.is_empty())
}

/// OTEL-1: Returns the trace ID, or `None` when not set.
pub fn pg_trickle_trace_id() -> Option<String> {
    PGS_TRACE_ID
        .get()
        .and_then(|cs| cs.to_str().ok().map(str::to_owned))
        .filter(|s| !s.is_empty())
}

/// LOG-1: Returns the configured log format.
pub fn pg_trickle_log_format() -> LogFormat {
    normalize_log_format(
        PGS_LOG_FORMAT
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// Returns the outbound metrics HTTP request timeout in milliseconds.
pub fn pg_trickle_metrics_request_timeout_ms() -> i32 {
    PGS_METRICS_REQUEST_TIMEOUT_MS.get()
}

/// SLA-1: Returns the SLA compliance measurement window in hours.
pub fn pg_trickle_sla_window_hours() -> i32 {
    PGS_SLA_WINDOW_HOURS.get()
}
