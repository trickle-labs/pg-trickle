// Scheduler-safe, stdlib-only OpenMetrics HTTP endpoint.

use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

const MAX_CONNECTIONS: usize = 16;
const MAX_ACCEPTS_PER_POLL: usize = 4;
const MAX_BYTES_PER_CONNECTION: usize = 4096;
const MAX_REQUEST_BYTES: usize = 8192;
const MAX_RESPONSE_BYTES_PER_POLL: usize = 4096;

#[derive(Debug, PartialEq)]
pub enum MetricsServerError {
    PortInUse(String),
    Timeout(String),
    Io(String),
}

impl std::fmt::Display for MetricsServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PortInUse(msg) => write!(f, "port in use: {msg}"),
            Self::Timeout(msg) => write!(f, "timeout: {msg}"),
            Self::Io(msg) => write!(f, "io error: {msg}"),
        }
    }
}

struct Connection {
    stream: TcpStream,
    request: Vec<u8>,
    response: Vec<u8>,
    written: usize,
    deadline: Instant,
}

pub struct MetricsServer {
    listener: TcpListener,
    connections: Vec<Connection>,
    request_timeout: Duration,
}

impl MetricsServer {
    pub fn start(port: u16) -> Option<Self> {
        Self::start_with_address("127.0.0.1", port).unwrap_or_else(|e| {
            pgrx::warning!("[pg_trickle] metrics endpoint unavailable: {e}");
            None
        })
    }

    pub fn start_with_address(
        bind_address: &str,
        port: u16,
    ) -> Result<Option<Self>, MetricsServerError> {
        if port == 0 {
            return Ok(None);
        }
        let ip =
            parse_bind_address(bind_address).map_err(|e| MetricsServerError::Io(e.to_string()))?;
        let addr = SocketAddr::new(ip, port);
        let listener = TcpListener::bind(addr).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AddrInUse {
                MetricsServerError::PortInUse(format!("{addr}: {e}"))
            } else {
                MetricsServerError::Io(format!("{addr}: {e}"))
            }
        })?;
        listener
            .set_nonblocking(true)
            .map_err(|e| MetricsServerError::Io(e.to_string()))?;
        pgrx::log!("[pg_trickle] metrics endpoint started on http://{addr}/metrics");
        Ok(Some(Self {
            listener,
            connections: Vec::with_capacity(MAX_CONNECTIONS),
            request_timeout: Duration::from_millis(5000),
        }))
    }

    pub fn start_result(port: u16) -> Result<Option<Self>, MetricsServerError> {
        Self::start_with_address("127.0.0.1", port)
    }

    pub fn set_request_timeout(&mut self, timeout: Duration) {
        self.request_timeout = timeout;
    }

    /// Poll without blocking the scheduler. Collection is called only after a
    /// complete, valid `GET /metrics` request has been parsed.
    pub fn poll<F>(&mut self, mut collect: F)
    where
        F: FnMut(Duration) -> Result<String, MetricsServerError>,
    {
        for _ in 0..MAX_ACCEPTS_PER_POLL {
            match self.listener.accept() {
                Ok((stream, _)) if self.connections.len() < MAX_CONNECTIONS => {
                    let _ = stream.set_nonblocking(true);
                    self.connections.push(Connection {
                        stream,
                        request: Vec::new(),
                        response: Vec::new(),
                        written: 0,
                        deadline: Instant::now() + self.request_timeout,
                    });
                }
                Ok((_stream, _)) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    pgrx::warning!("[pg_trickle] metrics accept error: {e}");
                    break;
                }
            }
        }

        let now = Instant::now();
        for connection in &mut self.connections {
            if connection.response.is_empty() {
                if now >= connection.deadline {
                    connection.response =
                        response_bytes("408 Request Timeout", "text/plain", "Request Timeout\n");
                } else {
                    let mut buf = [0u8; MAX_BYTES_PER_CONNECTION];
                    match connection.stream.read(&mut buf) {
                        Ok(0) => {
                            connection.response =
                                response_bytes("400 Bad Request", "text/plain", "Bad Request\n")
                        }
                        Ok(n) => {
                            connection.request.extend_from_slice(&buf[..n]);
                            if connection.request.len() > MAX_REQUEST_BYTES {
                                connection.response = response_bytes(
                                    "400 Bad Request",
                                    "text/plain",
                                    "Bad Request\n",
                                );
                            } else if request_complete(&connection.request) {
                                let request =
                                    std::str::from_utf8(&connection.request).unwrap_or("");
                                let remaining = connection
                                    .deadline
                                    .saturating_duration_since(Instant::now());
                                match route_kind(request) {
                                    Route::Metrics => {
                                        let body = collect(remaining);
                                        connection.response = match body {
                                            Ok(body) => response_bytes(
                                                "200 OK",
                                                "application/openmetrics-text; version=1.0.0; charset=utf-8",
                                                &body,
                                            ),
                                            Err(_) => response_bytes(
                                                "503 Service Unavailable",
                                                "text/plain",
                                                "Metrics unavailable\n",
                                            ),
                                        };
                                    }
                                    Route::Health => {
                                        connection.response =
                                            response_bytes("200 OK", "text/plain", "ok\n");
                                    }
                                    Route::NotFound => {
                                        connection.response = response_bytes(
                                            "404 Not Found",
                                            "text/plain",
                                            "Not Found\n",
                                        );
                                    }
                                    Route::BadRequest => {
                                        connection.response = response_bytes(
                                            "400 Bad Request",
                                            "text/plain",
                                            "Bad Request\n",
                                        );
                                    }
                                }
                            }
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                        Err(_) => {
                            connection.response =
                                response_bytes("400 Bad Request", "text/plain", "Bad Request\n")
                        }
                    }
                }
            }

            if !connection.response.is_empty() {
                let end = (connection.written + MAX_RESPONSE_BYTES_PER_POLL)
                    .min(connection.response.len());
                if connection.written < end {
                    match connection
                        .stream
                        .write(&connection.response[connection.written..end])
                    {
                        Ok(n) => connection.written += n,
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                        Err(_) => connection.written = connection.response.len(),
                    }
                }
            }
        }
        self.connections.retain(|connection| {
            connection.written < connection.response.len() && Instant::now() < connection.deadline
        });
    }
}

fn parse_bind_address(value: &str) -> Result<IpAddr, &'static str> {
    if value.is_empty() || value.contains('\0') || value.trim() != value {
        return Err("bind address must be a literal IPv4 or IPv6 address");
    }
    value
        .parse()
        .map_err(|_| "bind address must be a literal IPv4 or IPv6 address")
}

fn request_complete(request: &[u8]) -> bool {
    request.windows(4).any(|window| window == b"\r\n\r\n")
}

enum Route {
    Metrics,
    Health,
    NotFound,
    BadRequest,
}

fn route_kind(request: &str) -> Route {
    let first_line = request.lines().next().unwrap_or("");
    let tokens: Vec<&str> = first_line.split_whitespace().collect();
    if tokens.len() < 3 {
        return Route::BadRequest;
    }
    if tokens[0] == "GET" && (tokens[1] == "/metrics" || tokens[1].starts_with("/metrics?")) {
        Route::Metrics
    } else if tokens[0] == "GET" && (tokens[1] == "/health" || tokens[1] == "/-/healthy") {
        Route::Health
    } else {
        Route::NotFound
    }
}

fn response_bytes(status: &str, content_type: &str, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpStream;

    fn test_server() -> (MetricsServer, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        (
            MetricsServer {
                listener,
                connections: Vec::new(),
                request_timeout: Duration::from_secs(1),
            },
            addr,
        )
    }

    #[test]
    fn bind_address_accepts_literals_only() {
        assert_eq!(
            parse_bind_address("127.0.0.1").unwrap(),
            "127.0.0.1".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            parse_bind_address("::1").unwrap(),
            "::1".parse::<IpAddr>().unwrap()
        );
        assert!(parse_bind_address("localhost").is_err());
        assert!(parse_bind_address(" 127.0.0.1").is_err());
        assert!(parse_bind_address("127.0.0.1\0").is_err());
    }

    #[test]
    fn route_health_does_not_require_collection() {
        assert!(matches!(
            route_kind("GET /health HTTP/1.1\r\n\r\n"),
            Route::Health
        ));
        assert!(matches!(
            route_kind("GET /unknown HTTP/1.1\r\n\r\n"),
            Route::NotFound
        ));
    }

    #[test]
    fn request_completion_requires_headers() {
        assert!(!request_complete(b"GET /metrics HTTP/1.1\r\n"));
        assert!(request_complete(b"GET /metrics HTTP/1.1\r\n\r\n"));
    }

    #[test]
    fn idle_polls_do_not_collect() {
        let (mut server, _) = test_server();
        let mut collections = 0;
        for _ in 0..10_000 {
            server.poll(|_| {
                collections += 1;
                Ok(String::new())
            });
        }
        assert_eq!(collections, 0);
    }

    #[test]
    fn health_does_not_collect_and_metrics_collects_once() {
        let (mut server, addr) = test_server();
        let mut health = TcpStream::connect(addr).unwrap();
        std::io::Write::write_all(&mut health, b"GET /health HTTP/1.1\r\n\r\n").unwrap();
        let mut collections = 0;
        server.poll(|_| {
            collections += 1;
            Ok("metrics\n".to_string())
        });
        assert_eq!(collections, 0);

        let mut scrape = TcpStream::connect(addr).unwrap();
        std::io::Write::write_all(&mut scrape, b"GET /metrics HTTP/1.1\r\n\r\n").unwrap();
        for _ in 0..3 {
            std::thread::sleep(Duration::from_millis(1));
            server.poll(|_| {
                collections += 1;
                Ok("metrics\n".to_string())
            });
            if collections == 1 {
                break;
            }
            std::thread::yield_now();
        }
        assert_eq!(collections, 1);
    }

    #[test]
    fn collection_error_returns_503() {
        let (mut server, addr) = test_server();
        let mut scrape = TcpStream::connect(addr).unwrap();
        std::io::Write::write_all(&mut scrape, b"GET /metrics HTTP/1.1\r\n\r\n").unwrap();
        for _ in 0..3 {
            std::thread::sleep(Duration::from_millis(1));
            server.poll(|_| Err(MetricsServerError::Timeout("deadline".to_string())));
        }
        let mut response = String::new();
        scrape.set_nonblocking(false).unwrap();
        scrape.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
    }

    #[test]
    fn start_result_disabled_port_zero() {
        assert!(MetricsServer::start_result(0).unwrap().is_none());
    }
}
