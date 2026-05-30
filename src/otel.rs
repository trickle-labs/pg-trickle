//! F10 (v0.37.0): Lightweight OpenTelemetry W3C Trace Context propagation.
//!
//! ## Design
//!
//! pg_trickle captures the W3C `traceparent` header from the session GUC
//! `pg_trickle.trace_id` into the change-buffer column `__pgt_trace_context`
//! at CDC trigger time. At refresh time (in the background worker), if
//! `pg_trickle.enable_trace_propagation = on` and `pg_trickle.otel_endpoint`
//! is non-empty, the trace context is read from the earliest change-buffer
//! row in the batch and used to emit child spans via OTLP HTTP/JSON.
//!
//! ## OTLP export
//!
//! Uses a minimal HTTP/1.1 POST to the OTLP traces endpoint
//! (`{otel_endpoint}/v1/traces`) with `Content-Type: application/json`.
//! No external crate dependencies — uses `std::net::TcpStream` for the
//! TCP connection and `serde_json` (already a dependency) for JSON encoding.
//!
//! The export is **non-blocking**: it runs on a background thread spawned
//! by the background worker (which is a separate OS process, so spawning
//! threads is safe there). Export failures are logged at DEBUG level and
//! never propagate to the caller.
//!
//! ## W3C Trace Context
//!
//! Format: `00-{32-hex-trace-id}-{16-hex-parent-span-id}-{2-hex-flags}`
//!
//! Example: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
//!
//! See: <https://www.w3.org/TR/trace-context/>

use std::collections::HashMap;

/// Span names emitted by pg_trickle for a full refresh cycle.
pub const SPAN_CDC_DRAIN: &str = "pgtrickle.cdc_drain";
pub const SPAN_DVM_PLAN: &str = "pgtrickle.dvm_plan";
pub const SPAN_MERGE_APPLY: &str = "pgtrickle.merge_apply";
pub const SPAN_NOTIFY_EMIT: &str = "pgtrickle.notify_emit";

/// QW-4 (v0.81.0): Additional span names for the scheduler hot path.
///
/// These spans are emitted when `pg_trickle.otel_endpoint` is set and
/// `pg_trickle.enable_trace_propagation = on`.
pub const SPAN_SCHEDULER_TICK: &str = "pgtrickle.scheduler_tick";
pub const SPAN_REFRESH_CYCLE: &str = "pgtrickle.refresh_cycle";
pub const SPAN_DELTA_EXECUTE: &str = "pgtrickle.delta_execute";
pub const SPAN_FRONTIER_ADVANCE: &str = "pgtrickle.frontier_advance";
pub const SPAN_CLEANUP: &str = "pgtrickle.cleanup";

/// Parsed W3C Trace Context from a `traceparent` header value.
#[derive(Debug, Clone)]
pub struct TraceContext {
    /// 32-hex-char trace ID.
    pub trace_id: String,
    /// 16-hex-char parent span ID (the upstream span that triggered the DML).
    pub parent_span_id: String,
    /// Sampling flags (01 = sampled).
    pub flags: u8,
}

impl TraceContext {
    /// Parse a W3C `traceparent` header value.
    ///
    /// Format: `00-{32-hex}-{16-hex}-{2-hex}`
    ///
    /// Returns `None` if the input is not a valid traceparent.
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.trim().splitn(4, '-').collect();
        if parts.len() != 4 {
            return None;
        }
        let _version = parts[0]; // "00"
        let trace_id = parts[1];
        let parent_span_id = parts[2];
        let flags_hex = parts[3];

        if trace_id.len() != 32 || parent_span_id.len() != 16 || flags_hex.len() != 2 {
            return None;
        }
        // Validate hex characters.
        if !trace_id.chars().all(|c| c.is_ascii_hexdigit())
            || !parent_span_id.chars().all(|c| c.is_ascii_hexdigit())
            || !flags_hex.chars().all(|c| c.is_ascii_hexdigit())
        {
            return None;
        }
        let flags = u8::from_str_radix(flags_hex, 16).ok()?;
        Some(Self {
            trace_id: trace_id.to_string(),
            parent_span_id: parent_span_id.to_string(),
            flags,
        })
    }

    /// Generate a new 16-hex-char span ID (pseudo-random using XOR of timestamps).
    ///
    /// Uses a simple XOR-based PRNG seeded from `SystemTime` and a per-call counter.
    /// Good enough for span ID generation — not cryptographically random.
    pub fn new_span_id() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(1);

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64;
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let id = ts.wrapping_mul(6364136223846793005).wrapping_add(counter);
        format!("{id:016x}")
    }
}

/// A single OpenTelemetry span to be exported.
#[derive(Debug, Clone)]
pub struct OtelSpan {
    pub trace_ctx: TraceContext,
    pub span_id: String,
    pub name: String,
    pub start_time_ns: u64,
    pub end_time_ns: u64,
    pub attributes: HashMap<String, String>,
    pub status_ok: bool,
}

impl OtelSpan {
    /// Build a new child span under the given trace context.
    pub fn new(
        trace_ctx: TraceContext,
        name: impl Into<String>,
        start_time_ns: u64,
        end_time_ns: u64,
    ) -> Self {
        Self {
            span_id: TraceContext::new_span_id(),
            trace_ctx,
            name: name.into(),
            start_time_ns,
            end_time_ns,
            attributes: HashMap::new(),
            status_ok: true,
        }
    }

    /// Add a string attribute to the span.
    pub fn attr(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Encode the span as an OTLP JSON payload for `POST /v1/traces`.
    pub fn to_otlp_json(&self) -> String {
        let attrs: String = self
            .attributes
            .iter()
            .map(|(k, v)| {
                format!(
                    r#"{{"key":"{k}","value":{{"stringValue":"{v}"}}}}"#,
                    k = escape_json(k),
                    v = escape_json(v)
                )
            })
            .collect::<Vec<_>>()
            .join(",");

        let status_code = if self.status_ok { 1 } else { 2 };

        format!(
            r#"{{"resourceSpans":[{{"resource":{{"attributes":[{{"key":"service.name","value":{{"stringValue":"pg_trickle"}}}}]}},"scopeSpans":[{{"scope":{{"name":"pg_trickle","version":"0.37.0"}},"spans":[{{"traceId":"{trace_id}","spanId":"{span_id}","parentSpanId":"{parent_span_id}","name":"{name}","kind":2,"startTimeUnixNano":"{start}","endTimeUnixNano":"{end}","attributes":[{attrs}],"status":{{"code":{status_code}}}}}]}}]}}]}}"#,
            trace_id = self.trace_ctx.trace_id,
            span_id = self.span_id,
            parent_span_id = self.trace_ctx.parent_span_id,
            name = escape_json(&self.name),
            start = self.start_time_ns,
            end = self.end_time_ns,
            attrs = attrs,
            status_code = status_code,
        )
    }
}

/// Escape a string for safe embedding in a JSON string value.
fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

/// Export a span to the OTLP HTTP endpoint asynchronously.
///
/// Spawns a background thread that makes the HTTP POST. The caller returns
/// immediately without waiting for the export to complete.
///
/// Called from the background worker process — thread spawning is safe there.
pub fn export_span_background(endpoint: String, span: OtelSpan) {
    std::thread::spawn(move || {
        if let Err(e) = do_export_http(&endpoint, &span) {
            // Log at DEBUG; export failures must never affect refresh.
            pgrx::debug1!(
                "[pg_trickle F10] OTLP export failed for span '{}': {}",
                span.name,
                e
            );
        }
    });
}

/// Export a span synchronously (for testing / inline use).
///
/// Returns `Ok(())` on HTTP 200/202 response, `Err(msg)` otherwise.
pub fn export_span_sync(endpoint: &str, span: &OtelSpan) -> Result<(), String> {
    do_export_http(endpoint, span)
}

/// Make a blocking HTTP/1.1 POST to `{endpoint}/v1/traces` with the OTLP JSON payload.
fn do_export_http(endpoint: &str, span: &OtelSpan) -> Result<(), String> {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpStream;

    let payload = span.to_otlp_json();
    let body = payload.as_bytes();

    // Parse the endpoint URL to extract host:port and path prefix.
    // Expected format: "http://host:port" or "host:port"
    let (host_port, path_prefix) = parse_endpoint(endpoint);

    let path = format!("{path_prefix}/v1/traces");

    let mut stream =
        TcpStream::connect(&host_port).map_err(|e| format!("connect to {host_port}: {e}"))?;

    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(5)))
        .ok();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .ok();

    let request = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: {host_port}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {len}\r\n\
         Connection: close\r\n\
         \r\n",
        path = path,
        host_port = host_port,
        len = body.len(),
    );

    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write request: {e}"))?;
    stream
        .write_all(body)
        .map_err(|e| format!("write body: {e}"))?;
    stream.flush().map_err(|e| format!("flush: {e}"))?;

    // Read response status line.
    let mut reader = BufReader::new(&mut stream);
    let mut status_line = String::new();
    reader
        .read_line(&mut status_line)
        .map_err(|e| format!("read response: {e}"))?;

    // Accept 200 OK or 204 No Content as success.
    if status_line.contains("200") || status_line.contains("204") || status_line.contains("202") {
        Ok(())
    } else {
        Err(format!("OTLP endpoint returned: {}", status_line.trim()))
    }
}

/// Parse an OTLP endpoint string into (host:port, path_prefix).
///
/// Handles:
/// - `http://localhost:4318` → ("localhost:4318", "")
/// - `localhost:4318` → ("localhost:4318", "")
/// - `http://jaeger.svc:4318/otel` → ("jaeger.svc:4318", "/otel")
fn parse_endpoint(endpoint: &str) -> (String, String) {
    let stripped = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);

    // Split at first '/' to get path prefix.
    if let Some(slash) = stripped.find('/') {
        let host_port = stripped[..slash].to_string();
        let path = stripped[slash..].to_string();
        (host_port, path)
    } else {
        (stripped.to_string(), String::new())
    }
}

/// Read the trace context from the session GUC `pg_trickle.trace_id`.
///
/// Returns `None` if the GUC is not set or empty.
/// Requires SPI context (called from within PostgreSQL).
pub fn read_session_trace_context() -> Option<TraceContext> {
    let val = crate::config::pg_trickle_trace_id()?;
    if val.is_empty() {
        return None;
    }
    TraceContext::parse(&val)
}

/// Current Unix timestamp in nanoseconds.
pub fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Log trace context at INFO level (when OTLP endpoint is not configured).
///
/// Used as a fallback when `otel_endpoint` is empty but `enable_trace_propagation`
/// is on. Ensures trace context is at least visible in PostgreSQL logs.
pub fn log_trace_context(trace_ctx: &TraceContext, phase: &str, st_name: &str) {
    pgrx::info!(
        "[pg_trickle trace] phase={} st={} trace_id={} parent_span={}",
        phase,
        st_name,
        trace_ctx.trace_id,
        trace_ctx.parent_span_id,
    );
}

// ── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_traceparent_valid() {
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let ctx = TraceContext::parse(tp).expect("valid traceparent");
        assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(ctx.parent_span_id, "00f067aa0ba902b7");
        assert_eq!(ctx.flags, 1);
    }

    #[test]
    fn test_parse_traceparent_not_sampled() {
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
        let ctx = TraceContext::parse(tp).expect("valid traceparent");
        assert_eq!(ctx.flags, 0);
    }

    #[test]
    fn test_parse_traceparent_invalid_empty() {
        assert!(TraceContext::parse("").is_none());
    }

    #[test]
    fn test_parse_traceparent_invalid_short() {
        assert!(TraceContext::parse("00-abc-def-01").is_none());
    }

    #[test]
    fn test_parse_traceparent_wrong_parts() {
        assert!(TraceContext::parse("just-three").is_none());
    }

    #[test]
    fn test_parse_endpoint_http() {
        let (hp, path) = parse_endpoint("http://localhost:4318");
        assert_eq!(hp, "localhost:4318");
        assert_eq!(path, "");
    }

    #[test]
    fn test_parse_endpoint_no_scheme() {
        let (hp, path) = parse_endpoint("jaeger.svc:4318");
        assert_eq!(hp, "jaeger.svc:4318");
        assert_eq!(path, "");
    }

    #[test]
    fn test_parse_endpoint_with_path() {
        let (hp, path) = parse_endpoint("http://otelcol.svc:4318/otel");
        assert_eq!(hp, "otelcol.svc:4318");
        assert_eq!(path, "/otel");
    }

    #[test]
    fn test_new_span_id_is_16_hex_chars() {
        let id = TraceContext::new_span_id();
        assert_eq!(id.len(), 16, "span id must be 16 hex chars");
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_new_span_id_differs() {
        let a = TraceContext::new_span_id();
        let b = TraceContext::new_span_id();
        // Two consecutive IDs should differ (counter ensures this).
        assert_ne!(a, b);
    }

    #[test]
    fn test_otlp_json_contains_trace_id() {
        let ctx =
            TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").unwrap();
        let span = OtelSpan::new(ctx, SPAN_MERGE_APPLY, 1_000_000, 2_000_000);
        let json = span.to_otlp_json();
        assert!(json.contains("4bf92f3577b34da6a3ce929d0e0e4736"));
        assert!(json.contains("pgtrickle.merge_apply"));
        assert!(json.contains("pg_trickle"));
    }

    #[test]
    fn test_otlp_json_with_attributes() {
        let ctx =
            TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").unwrap();
        let span = OtelSpan::new(ctx, SPAN_CDC_DRAIN, 1_000, 2_000)
            .attr("st.name", "my_stream_table")
            .attr("st.rows_changed", "42");
        let json = span.to_otlp_json();
        assert!(json.contains("st.name"));
        assert!(json.contains("my_stream_table"));
    }

    #[test]
    fn test_escape_json() {
        assert_eq!(escape_json(r#"a"b"#), r#"a\"b"#);
        assert_eq!(escape_json("line\nnewline"), r#"line\nnewline"#);
    }
}
