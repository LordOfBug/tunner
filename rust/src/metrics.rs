//! Optional TSDB telemetry collector & background HTTPS pusher (`POST /api/v1/push`).
//!
//! Configuration precedence (highest to lowest):
//! 1. CLI flags: `--tsdb-url`, `--tsdb-token`, `--tsdb-interval`
//! 2. Environment variables: `TUNNER_TSDB_URL`, `TUNNER_TSDB_TOKEN`, `TUNNER_TSDB_INTERVAL`
//! 3. Home config file: `~/.tunner/tsdb.conf` or `~/.tunner.conf` (`key = value`)

use crate::block::{Block, CODE_PING};
use crate::tls::{build_client_connector, make_server_name};
use log::{debug, info, warn};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_rustls::TlsConnector;

const DEFAULT_INTERVAL_SECS: u64 = 15;
const MAX_LOGS_PER_PUSH: usize = 64;
const MAX_PENDING_PUSHES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TsdbConfig {
    pub url: String,
    pub token: String,
    pub interval: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedEndpoint {
    https: bool,
    host: String,
    port: u16,
    path: String,
    host_header: String,
}

impl TsdbConfig {
    /// Resolve TSDB config from CLI -> env vars -> `~/.tunner/tsdb.conf` / `~/.tunner.conf`.
    /// Returns `None` if no URL is configured.
    pub fn resolve(
        cli_url: Option<&str>,
        cli_token: Option<&str>,
        cli_interval: Option<u64>,
    ) -> Option<Self> {
        let file_cfg = load_home_config();

        let env_url = std::env::var("TUNNER_TSDB_URL").ok().filter(|s| !s.trim().is_empty());
        let env_token = std::env::var("TUNNER_TSDB_TOKEN").ok().filter(|s| !s.trim().is_empty());
        let env_interval = std::env::var("TUNNER_TSDB_INTERVAL")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok());

        let raw_url = cli_url
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or(env_url)
            .or_else(|| file_cfg.url)?;

        let token = cli_token
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or(env_token)
            .or(file_cfg.token)
            .unwrap_or_default();

        let interval_secs = cli_interval
            .or(env_interval)
            .or(file_cfg.interval)
            .unwrap_or(DEFAULT_INTERVAL_SECS)
            .max(1);

        let url = normalize_push_url(&raw_url);
        if token.is_empty() && url.starts_with("https://") {
            warn!("TSDB URL is set without a token; pushes may be rejected with 401");
        }

        Some(TsdbConfig {
            url,
            token,
            interval: Duration::from_secs(interval_secs),
        })
    }
}

#[derive(Default)]
struct FileConfig {
    url: Option<String>,
    token: Option<String>,
    interval: Option<u64>,
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

fn load_home_config() -> FileConfig {
    let Some(home) = home_dir() else {
        return FileConfig::default();
    };
    for candidate in [home.join(".tunner").join("tsdb.conf"), home.join(".tunner.conf")] {
        if candidate.is_file() {
            if let Ok(content) = std::fs::read_to_string(&candidate) {
                debug!("Loaded TSDB config from {}", candidate.display());
                return parse_config_str(&content);
            }
        }
    }
    FileConfig::default()
}

fn parse_config_str(content: &str) -> FileConfig {
    let mut cfg = FileConfig::default();
    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim().to_ascii_lowercase();
        let val = unquote(v.trim());
        if val.is_empty() {
            continue;
        }
        match key.as_str() {
            "url" | "tsdb_url" | "tsdb-url" => cfg.url = Some(val.to_string()),
            "token" | "tsdb_token" | "tsdb-token" => cfg.token = Some(val.to_string()),
            "interval" | "tsdb_interval" | "tsdb-interval" => {
                if let Ok(secs) = val.parse::<u64>() {
                    cfg.interval = Some(secs);
                }
            }
            _ => {}
        }
    }
    cfg
}

fn unquote(s: &str) -> &str {
    if s.len() >= 2
        && ((s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')))
    {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

fn normalize_push_url(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.ends_with("/api/v1/push") {
        trimmed.to_string()
    } else {
        format!("{}/api/v1/push", trimmed)
    }
}

fn parse_endpoint(url: &str) -> Result<ParsedEndpoint, String> {
    let (https, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(format!("Unsupported TSDB URL scheme: {}", url));
    };

    let (authority, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/api/v1/push"),
    };

    let default_port = if https { 443 } else { 80 };
    let (host, port) = if authority.starts_with('[') {
        if let Some(end) = authority.find(']') {
            let h = &authority[1..end];
            let p = authority[end + 1..]
                .strip_prefix(':')
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(default_port);
            (h.to_string(), p)
        } else {
            return Err(format!("Invalid IPv6 host in URL: {}", url));
        }
    } else if let Some((h, p_str)) = authority.rsplit_once(':') {
        let p = p_str
            .parse::<u16>()
            .map_err(|_| format!("Invalid port in URL: {}", url))?;
        (h.to_string(), p)
    } else {
        (authority.to_string(), default_port)
    };

    if host.is_empty() {
        return Err(format!("Empty host in TSDB URL: {}", url));
    }

    let path = if path.is_empty() || path == "/" {
        "/api/v1/push".to_string()
    } else {
        path.to_string()
    };

    Ok(ParsedEndpoint {
        https,
        host,
        port,
        path,
        host_header: authority.to_string(),
    })
}

fn detect_hostname() -> String {
    for var in ["HOSTNAME", "HOST", "COMPUTERNAME"] {
        if let Ok(v) = std::env::var(var) {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }
    if let Ok(out) = std::process::Command::new("uname").arg("-n").output() {
        if out.status.success() {
            let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !s.is_empty() {
                return s;
            }
        }
    }
    "unknown".to_string()
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn atomic_max(target: &AtomicU64, val: u64) {
    let mut cur = target.load(Ordering::Relaxed);
    while val > cur {
        match target.compare_exchange_weak(cur, val, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
}

/// Lock-free telemetry registry shared across a `tunclient` or `tunserver` process.
pub struct Metrics {
    epoch: Instant,
    pub rtt_last_us: AtomicU64,
    pub rtt_max_us: AtomicU64,
    pub rtt_sum_us: AtomicU64,
    pub rtt_count: AtomicU64,
    pub handshake_last_ms: AtomicU64,
    pub backend_connect_sum_us: AtomicU64,
    pub backend_connect_max_us: AtomicU64,
    pub backend_connect_count: AtomicU64,
    pub backend_connect_failures: AtomicU64,
    pub active_streams: AtomicU64,
    pub conn_started_total: AtomicU64,
    pub conn_graceful_total: AtomicU64,
    pub conn_local_error_total: AtomicU64,
    pub conn_remote_abort_total: AtomicU64,
    pub conn_timeout_total: AtomicU64,
    pub pause_events_total: AtomicU64,
    pub bytes_tx_total: AtomicU64,
    pub bytes_rx_total: AtomicU64,
    pub noise_bytes_tx_total: AtomicU64,
    pub blocks_tx_total: AtomicU64,
    pub blocks_rx_total: AtomicU64,
    pub write_batches_total: AtomicU64,
    pub tunnel_reconnects_total: AtomicU64,
    pub auth_failures_total: AtomicU64,
    pub tcp_retrans_total: AtomicU64,
    pub tcp_loss_retrans_units: AtomicU64,
    pub tcp_loss_sent_units: AtomicU64,
    logs: Mutex<Vec<(u64, &'static str, String)>>,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            epoch: Instant::now(),
            rtt_last_us: AtomicU64::new(0),
            rtt_max_us: AtomicU64::new(0),
            rtt_sum_us: AtomicU64::new(0),
            rtt_count: AtomicU64::new(0),
            handshake_last_ms: AtomicU64::new(0),
            backend_connect_sum_us: AtomicU64::new(0),
            backend_connect_max_us: AtomicU64::new(0),
            backend_connect_count: AtomicU64::new(0),
            backend_connect_failures: AtomicU64::new(0),
            active_streams: AtomicU64::new(0),
            conn_started_total: AtomicU64::new(0),
            conn_graceful_total: AtomicU64::new(0),
            conn_local_error_total: AtomicU64::new(0),
            conn_remote_abort_total: AtomicU64::new(0),
            conn_timeout_total: AtomicU64::new(0),
            pause_events_total: AtomicU64::new(0),
            bytes_tx_total: AtomicU64::new(0),
            bytes_rx_total: AtomicU64::new(0),
            noise_bytes_tx_total: AtomicU64::new(0),
            blocks_tx_total: AtomicU64::new(0),
            blocks_rx_total: AtomicU64::new(0),
            write_batches_total: AtomicU64::new(0),
            tunnel_reconnects_total: AtomicU64::new(0),
            auth_failures_total: AtomicU64::new(0),
            tcp_retrans_total: AtomicU64::new(0),
            tcp_loss_retrans_units: AtomicU64::new(0),
            tcp_loss_sent_units: AtomicU64::new(0),
            logs: Mutex::new(Vec::new()),
        })
    }

    pub fn now_micros(&self) -> u64 {
        self.epoch.elapsed().as_micros() as u64
    }

    /// Construct a `CODE_PING` carrying the current monotonic microsecond timestamp.
    pub fn make_ping_block(&self) -> Block {
        let ts = self.now_micros().to_be_bytes();
        Block::new_control_with_data(0, CODE_PING, bytes::Bytes::copy_from_slice(&ts))
    }

    /// Record a `CODE_PONG` payload echoing an 8-byte timestamp produced by `make_ping_block`.
    pub fn record_pong(&self, payload: &[u8]) {
        if payload.len() < 8 {
            return;
        }
        let sent_us = u64::from_be_bytes(payload[..8].try_into().unwrap());
        let now_us = self.now_micros();
        if now_us < sent_us {
            return;
        }
        let rtt_us = now_us - sent_us;
        self.rtt_last_us.store(rtt_us, Ordering::Relaxed);
        atomic_max(&self.rtt_max_us, rtt_us);
        self.rtt_sum_us.fetch_add(rtt_us, Ordering::Relaxed);
        self.rtt_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_backend_connect(&self, elapsed: Duration, ok: bool) {
        let us = elapsed.as_micros() as u64;
        self.backend_connect_sum_us.fetch_add(us, Ordering::Relaxed);
        atomic_max(&self.backend_connect_max_us, us);
        self.backend_connect_count.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.backend_connect_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_stream_end(
        &self,
        graceful: bool,
        local_err: bool,
        remote_abort: bool,
        timeout: bool,
    ) {
        if graceful {
            self.conn_graceful_total.fetch_add(1, Ordering::Relaxed);
        }
        if local_err {
            self.conn_local_error_total.fetch_add(1, Ordering::Relaxed);
        }
        if remote_abort {
            self.conn_remote_abort_total.fetch_add(1, Ordering::Relaxed);
        }
        if timeout {
            self.conn_timeout_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn log_event(&self, level: &'static str, msg: impl Into<String>) {
        let mut logs = self.logs.lock().unwrap();
        if logs.len() >= MAX_LOGS_PER_PUSH {
            logs.remove(0);
        }
        logs.push((unix_ms(), level, msg.into()));
    }
}

#[derive(Default)]
struct PrevSnapshot {
    rtt_sum_us: u64,
    rtt_count: u64,
    backend_sum_us: u64,
    backend_count: u64,
    backend_failures: u64,
    conn_graceful: u64,
    conn_local_err: u64,
    conn_remote_abort: u64,
    conn_timeout: u64,
    bytes_tx: u64,
    write_batches: u64,
    tcp_retrans_units: u64,
    tcp_sent_units: u64,
}

fn push_json_escaped(out: &mut String, s: &str) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn build_push_payload(
    metrics: &Metrics,
    prev: &mut PrevSnapshot,
    role: &str,
    host: &str,
    mode: &str,
    ts_ms: u64,
) -> String {
    let rtt_last = metrics.rtt_last_us.load(Ordering::Relaxed);
    let rtt_max = metrics.rtt_max_us.swap(0, Ordering::Relaxed);
    let rtt_sum = metrics.rtt_sum_us.load(Ordering::Relaxed);
    let rtt_cnt = metrics.rtt_count.load(Ordering::Relaxed);

    let d_rtt_sum = rtt_sum.saturating_sub(prev.rtt_sum_us);
    let d_rtt_cnt = rtt_cnt.saturating_sub(prev.rtt_count);
    let rtt_ms = if d_rtt_cnt > 0 {
        (d_rtt_sum as f64 / d_rtt_cnt as f64) / 1000.0
    } else {
        rtt_last as f64 / 1000.0
    };
    let rtt_max_ms = if rtt_max > 0 {
        rtt_max as f64 / 1000.0
    } else {
        rtt_last as f64 / 1000.0
    };

    let bc_sum = metrics.backend_connect_sum_us.load(Ordering::Relaxed);
    let bc_max = metrics.backend_connect_max_us.swap(0, Ordering::Relaxed);
    let bc_cnt = metrics.backend_connect_count.load(Ordering::Relaxed);
    let bc_fail = metrics.backend_connect_failures.load(Ordering::Relaxed);

    let d_bc_sum = bc_sum.saturating_sub(prev.backend_sum_us);
    let d_bc_cnt = bc_cnt.saturating_sub(prev.backend_count);
    let bc_ms = if d_bc_cnt > 0 {
        (d_bc_sum as f64 / d_bc_cnt as f64) / 1000.0
    } else {
        0.0
    };
    let bc_max_ms = bc_max as f64 / 1000.0;

    let graceful = metrics.conn_graceful_total.load(Ordering::Relaxed);
    let local_err = metrics.conn_local_error_total.load(Ordering::Relaxed);
    let remote_abort = metrics.conn_remote_abort_total.load(Ordering::Relaxed);
    let timeout = metrics.conn_timeout_total.load(Ordering::Relaxed);

    let d_ok = graceful.saturating_sub(prev.conn_graceful);
    let d_fail = local_err.saturating_sub(prev.conn_local_err)
        + remote_abort.saturating_sub(prev.conn_remote_abort)
        + timeout.saturating_sub(prev.conn_timeout)
        + bc_fail.saturating_sub(prev.backend_failures);
    let d_ended = d_ok + d_fail;
    let failure_rate_pct = if d_ended > 0 {
        (d_fail as f64 * 100.0) / (d_ended as f64)
    } else {
        0.0
    };

    let bytes_tx = metrics.bytes_tx_total.load(Ordering::Relaxed);
    let write_batches = metrics.write_batches_total.load(Ordering::Relaxed);
    let d_bytes_tx = bytes_tx.saturating_sub(prev.bytes_tx);
    let d_batches = write_batches.saturating_sub(prev.write_batches);
    let avg_batch_bytes = if d_batches > 0 {
        d_bytes_tx as f64 / d_batches as f64
    } else {
        0.0
    };

    let tcp_retrans_u = metrics.tcp_loss_retrans_units.load(Ordering::Relaxed);
    let tcp_sent_u = metrics.tcp_loss_sent_units.load(Ordering::Relaxed);
    let d_tcp_retrans = tcp_retrans_u.saturating_sub(prev.tcp_retrans_units);
    let d_tcp_sent = tcp_sent_u.saturating_sub(prev.tcp_sent_units);
    let packet_loss_pct = if d_tcp_sent > 0 {
        ((d_tcp_retrans as f64 * 100.0) / (d_tcp_sent as f64)).min(100.0)
    } else {
        0.0
    };

    prev.rtt_sum_us = rtt_sum;
    prev.rtt_count = rtt_cnt;
    prev.backend_sum_us = bc_sum;
    prev.backend_count = bc_cnt;
    prev.backend_failures = bc_fail;
    prev.conn_graceful = graceful;
    prev.conn_local_err = local_err;
    prev.conn_remote_abort = remote_abort;
    prev.conn_timeout = timeout;
    prev.bytes_tx = bytes_tx;
    prev.write_batches = write_batches;
    prev.tcp_retrans_units = tcp_retrans_u;
    prev.tcp_sent_units = tcp_sent_u;

    let handshake_ms = metrics.handshake_last_ms.load(Ordering::Relaxed);
    let active_streams = metrics.active_streams.load(Ordering::Relaxed);
    let conn_started = metrics.conn_started_total.load(Ordering::Relaxed);
    let pause_events = metrics.pause_events_total.load(Ordering::Relaxed);
    let bytes_rx = metrics.bytes_rx_total.load(Ordering::Relaxed);
    let noise_bytes_tx = metrics.noise_bytes_tx_total.load(Ordering::Relaxed);
    let reconnects = metrics.tunnel_reconnects_total.load(Ordering::Relaxed);
    let auth_failures = metrics.auth_failures_total.load(Ordering::Relaxed);
    let tcp_retrans = metrics.tcp_retrans_total.load(Ordering::Relaxed);

    let logs: Vec<(u64, &'static str, String)> = {
        let mut guard = metrics.logs.lock().unwrap();
        std::mem::take(&mut *guard)
    };

    let mut json = String::with_capacity(2048);
    json.push_str("{\"streams\":[{\"labels\":{\"app\":\"tunner\",\"role\":");
    push_json_escaped(&mut json, role);
    json.push_str(",\"host\":");
    push_json_escaped(&mut json, host);
    json.push_str(",\"mode\":");
    push_json_escaped(&mut json, mode);
    json.push_str(",\"version\":");
    push_json_escaped(&mut json, env!("CARGO_PKG_VERSION"));
    json.push_str("},\"metrics\":[");

    let gauges: &[(&str, f64)] = &[
        ("tunner_rtt_ms", rtt_ms),
        ("tunner_rtt_max_ms", rtt_max_ms),
        ("tunner_packet_loss_pct", packet_loss_pct),
        ("tunner_active_streams", active_streams as f64),
        ("tunner_handshake_ms", handshake_ms as f64),
        ("tunner_backend_connect_ms", bc_ms),
        ("tunner_backend_connect_max_ms", bc_max_ms),
        ("tunner_failure_rate_pct", failure_rate_pct),
        ("tunner_avg_batch_bytes", avg_batch_bytes),
    ];
    let counters: &[(&str, u64)] = &[
        ("tunner_connections_started_total", conn_started),
        ("tunner_connections_graceful_total", graceful),
        ("tunner_connections_local_error_total", local_err),
        ("tunner_connections_remote_abort_total", remote_abort),
        ("tunner_connections_timeout_total", timeout),
        ("tunner_backend_connect_failures_total", bc_fail),
        ("tunner_pause_events_total", pause_events),
        ("tunner_bytes_tx_total", bytes_tx),
        ("tunner_bytes_rx_total", bytes_rx),
        ("tunner_noise_bytes_tx_total", noise_bytes_tx),
        ("tunner_reconnects_total", reconnects),
        ("tunner_auth_failures_total", auth_failures),
        ("tunner_tcp_retrans_total", tcp_retrans),
    ];

    let mut first = true;
    for (name, val) in gauges {
        if !first {
            json.push(',');
        }
        first = false;
        json.push_str(&format!(
            "{{\"name\":\"{}\",\"kind\":\"gauge\",\"points\":[[{},{:.3}]]}}",
            name, ts_ms, val
        ));
    }
    for (name, val) in counters {
        json.push_str(&format!(
            ",{{\"name\":\"{}\",\"kind\":\"counter\",\"points\":[[{},{}]]}}",
            name, ts_ms, val
        ));
    }
    json.push(']');

    if !logs.is_empty() {
        json.push_str(",\"logs\":[");
        for (idx, (ts, level, line)) in logs.into_iter().enumerate() {
            if idx > 0 {
                json.push(',');
            }
            json.push_str(&format!("{{\"ts\":{},\"level\":\"{}\",\"line\":", ts, level));
            push_json_escaped(&mut json, &line);
            json.push('}');
        }
        json.push(']');
    }

    json.push_str("}]}");
    json
}

enum PushOutcome {
    Delivered,
    PermanentError(String),
    RetryableError(String),
}

async fn send_http_push(
    endpoint: &ParsedEndpoint,
    tls: Option<&TlsConnector>,
    token: &str,
    body: &str,
) -> PushOutcome {
    let addr = format!("{}:{}", endpoint.host, endpoint.port);
    let tcp = match tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(&addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return PushOutcome::RetryableError(format!("connect error to {}: {}", addr, e)),
        Err(_) => return PushOutcome::RetryableError(format!("connect timeout to {}", addr)),
    };

    let mut req = format!(
        "POST {} HTTP/1.1\r\n\
         Host: {}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n",
        endpoint.path,
        endpoint.host_header,
        body.len()
    );
    if !token.is_empty() {
        req.push_str(&format!("Authorization: Bearer {}\r\n", token));
    }
    req.push_str("\r\n");

    if endpoint.https {
        let Some(connector) = tls else {
            return PushOutcome::PermanentError(
                "missing TLS connector for https TSDB URL".to_string(),
            );
        };
        let sni = make_server_name(&endpoint.host);
        let mut stream =
            match tokio::time::timeout(Duration::from_secs(10), connector.connect(sni, tcp)).await {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    return PushOutcome::RetryableError(format!("TSDB TLS handshake error: {}", e))
                }
                Err(_) => return PushOutcome::RetryableError("TSDB TLS handshake timeout".into()),
            };
        exchange_http(&mut stream, req.as_bytes(), body.as_bytes()).await
    } else {
        let mut stream = tcp;
        exchange_http(&mut stream, req.as_bytes(), body.as_bytes()).await
    }
}

async fn exchange_http<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    headers: &[u8],
    body: &[u8],
) -> PushOutcome {
    let res = tokio::time::timeout(Duration::from_secs(10), async {
        stream
            .write_all(headers)
            .await
            .map_err(|e| format!("TSDB write error: {}", e))?;
        stream
            .write_all(body)
            .await
            .map_err(|e| format!("TSDB body write error: {}", e))?;
        stream
            .flush()
            .await
            .map_err(|e| format!("TSDB flush error: {}", e))?;

        let mut resp = Vec::with_capacity(512);
        let mut buf = [0u8; 512];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    resp.extend_from_slice(&buf[..n]);
                    if resp.windows(4).any(|w| w == b"\r\n\r\n") || resp.len() >= 4096 {
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && !resp.is_empty() => {
                    break;
                }
                Err(e) => return Err(format!("TSDB read error: {}", e)),
            }
        }
        let _ = stream.shutdown().await;
        let resp_str = String::from_utf8_lossy(&resp);
        let status_line = resp_str.lines().next().unwrap_or("").to_string();
        Ok(status_line)
    })
    .await;

    let status_line = match res {
        Ok(Ok(line)) => line,
        Ok(Err(e)) => return PushOutcome::RetryableError(e),
        Err(_) => return PushOutcome::RetryableError("TSDB HTTP exchange timeout".to_string()),
    };

    let code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok());
    match code {
        Some(200 | 204 | 207) => PushOutcome::Delivered,
        Some(400 | 401 | 403 | 404 | 413) => {
            PushOutcome::PermanentError(format!("TSDB HTTP error: {}", status_line))
        }
        _ => PushOutcome::RetryableError(format!("TSDB HTTP error: {}", status_line)),
    }
}

async fn flush_pending(
    pending: &mut VecDeque<String>,
    endpoint: &ParsedEndpoint,
    tls: Option<&TlsConnector>,
    token: &str,
) {
    while let Some(front) = pending.front() {
        match send_http_push(endpoint, tls, token, front).await {
            PushOutcome::Delivered => {
                pending.pop_front();
            }
            PushOutcome::PermanentError(e) => {
                warn!("Dropping permanently rejected TSDB payload: {}", e);
                pending.pop_front();
            }
            PushOutcome::RetryableError(e) => {
                debug!(
                    "TSDB push failed ({}); {} snapshot(s) queued for retry",
                    e,
                    pending.len()
                );
                break;
            }
        }
    }
}

fn enqueue_snapshot(pending: &mut VecDeque<String>, body: String) {
    if pending.len() >= MAX_PENDING_PUSHES {
        pending.pop_front();
    }
    pending.push_back(body);
}

/// Spawn the background TSDB telemetry reporter task.
/// Never panics or terminates the tunnel if TSDB is temporarily unreachable.
pub fn spawn_tsdb_reporter(
    config: TsdbConfig,
    metrics: Arc<Metrics>,
    role: &'static str,
    mode: &'static str,
    mut shutdown: watch::Receiver<bool>,
) {
    let endpoint = match parse_endpoint(&config.url) {
        Ok(ep) => ep,
        Err(e) => {
            warn!("Disabling TSDB reporter: {}", e);
            return;
        }
    };

    tokio::spawn(async move {
        let tls = if endpoint.https {
            match build_client_connector(None, None, false) {
                Ok(c) => Some(c),
                Err(e) => {
                    warn!("Disabling HTTPS TSDB reporter (TLS init failed): {}", e);
                    return;
                }
            }
        } else {
            None
        };

        let host = detect_hostname();
        info!(
            "TSDB telemetry enabled -> {} (role={}, host={}, mode={}, every {}s)",
            config.url,
            role,
            host,
            mode,
            config.interval.as_secs()
        );

        let mut prev = PrevSnapshot::default();
        let mut pending: VecDeque<String> = VecDeque::with_capacity(MAX_PENDING_PUSHES);
        let mut ticker = tokio::time::interval(config.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Consume the immediate first tick so metrics collect for one full interval first.
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let body = build_push_payload(&metrics, &mut prev, role, &host, mode, unix_ms());
                    enqueue_snapshot(&mut pending, body);
                    flush_pending(&mut pending, &endpoint, tls.as_ref(), &config.token).await;
                }
                res = shutdown.changed() => {
                    match res {
                        Ok(()) if !*shutdown.borrow() => continue,
                        Ok(()) | Err(_) => {
                            let body = build_push_payload(&metrics, &mut prev, role, &host, mode, unix_ms());
                            enqueue_snapshot(&mut pending, body);
                            flush_pending(&mut pending, &endpoint, tls.as_ref(), &config.token).await;
                            break;
                        }
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config_and_url() {
        let cfg = parse_config_str(
            "# comment\nurl = \"https://tsdb.zoomphant.com/\"\ntoken = 'abc-123'\ninterval = 30\n",
        );
        assert_eq!(cfg.url.as_deref(), Some("https://tsdb.zoomphant.com/"));
        assert_eq!(cfg.token.as_deref(), Some("abc-123"));
        assert_eq!(cfg.interval, Some(30));

        assert_eq!(
            normalize_push_url("https://tsdb.zoomphant.com/"),
            "https://tsdb.zoomphant.com/api/v1/push"
        );
        assert_eq!(
            normalize_push_url("https://tsdb.zoomphant.com/api/v1/push"),
            "https://tsdb.zoomphant.com/api/v1/push"
        );

        let ep = parse_endpoint("https://tsdb.zoomphant.com/api/v1/push").unwrap();
        assert!(ep.https);
        assert_eq!(ep.host, "tsdb.zoomphant.com");
        assert_eq!(ep.port, 443);
        assert_eq!(ep.path, "/api/v1/push");
    }

    #[test]
    fn test_metrics_rtt_and_json_escape() {
        let m = Metrics::new();
        let ping = m.make_ping_block();
        assert_eq!(ping.control_code(), CODE_PING);
        assert_eq!(ping.data.len(), 8);
        std::thread::sleep(Duration::from_millis(5));
        m.record_pong(&ping.data);
        assert!(m.rtt_count.load(Ordering::Relaxed) == 1);
        assert!(m.rtt_last_us.load(Ordering::Relaxed) >= 4_000);

        m.log_event("warn", "quote \" and \\ newline\nhere".to_string());
        let mut prev = PrevSnapshot::default();
        let body = build_push_payload(&m, &mut prev, "client", "mac", "ws-tls", 1760000000000);
        assert!(body.contains("\"tunner_rtt_ms\""));
        assert!(body.contains("quote \\\" and \\\\ newline\\nhere"));
    }

    #[tokio::test]
    async fn test_monitored_tcp_stream_samples_stats() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut b = [0u8; 4];
            s.read_exact(&mut b).await.unwrap();
            s.write_all(b"pong").await.unwrap();
        });

        let m = Metrics::new();
        let raw = TcpStream::connect(addr).await.unwrap();
        let mut client = MonitoredTcpStream::new(raw, Arc::clone(&m));
        client.write_all(b"ping").await.unwrap();
        let mut reply = [0u8; 4];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"pong");
        client.sample_now();
        drop(client);
        server.await.unwrap();

        #[cfg(any(target_os = "macos", target_os = "linux"))]
        assert!(m.tcp_loss_sent_units.load(Ordering::Relaxed) > 0);
    }
}

/// Wrapper around the tunnel `TcpStream` that periodically samples OS kernel TCP
/// retransmission statistics (`TCP_CONNECTION_INFO` on macOS, `TCP_INFO` on Linux)
/// into `Metrics` (`tunner_packet_loss_pct` and `tunner_tcp_retrans_total`).
pub struct MonitoredTcpStream {
    inner: TcpStream,
    metrics: Arc<Metrics>,
    last_sample: Instant,
    prev_retrans_pkts: u64,
    prev_retrans_units: u64,
    prev_sent_units: u64,
}

impl MonitoredTcpStream {
    pub fn new(inner: TcpStream, metrics: Arc<Metrics>) -> Self {
        Self {
            inner,
            metrics,
            last_sample: Instant::now(),
            prev_retrans_pkts: 0,
            prev_retrans_units: 0,
            prev_sent_units: 0,
        }
    }

    pub fn sample_now(&mut self) {
        if let Some((retrans_pkts, retrans_units, sent_units)) = sample_tcp_stats(&self.inner) {
            let d_pkts = retrans_pkts.saturating_sub(self.prev_retrans_pkts);
            let d_retrans = retrans_units.saturating_sub(self.prev_retrans_units);
            let d_sent = sent_units.saturating_sub(self.prev_sent_units);
            self.prev_retrans_pkts = retrans_pkts;
            self.prev_retrans_units = retrans_units;
            self.prev_sent_units = sent_units;

            if d_pkts > 0 {
                self.metrics
                    .tcp_retrans_total
                    .fetch_add(d_pkts, Ordering::Relaxed);
            }
            if d_retrans > 0 {
                self.metrics
                    .tcp_loss_retrans_units
                    .fetch_add(d_retrans, Ordering::Relaxed);
            }
            if d_sent > 0 {
                self.metrics
                    .tcp_loss_sent_units
                    .fetch_add(d_sent, Ordering::Relaxed);
            }
        }
        self.last_sample = Instant::now();
    }

    #[inline]
    fn maybe_sample(&mut self) {
        if self.last_sample.elapsed() >= Duration::from_secs(1) {
            self.sample_now();
        }
    }
}

impl Drop for MonitoredTcpStream {
    fn drop(&mut self) {
        self.sample_now();
    }
}

impl AsyncRead for MonitoredTcpStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let res = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        if res.is_ready() {
            self.maybe_sample();
        }
        res
    }
}

impl AsyncWrite for MonitoredTcpStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let res = std::pin::Pin::new(&mut self.inner).poll_write(cx, buf);
        if res.is_ready() {
            self.maybe_sample();
        }
        res
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.sample_now();
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(target_os = "macos")]
#[repr(C, align(8))]
struct MacTcpConnectionInfo {
    tcpi_state: u8,
    tcpi_snd_wscale: u8,
    tcpi_rcv_wscale: u8,
    __pad1: u8,
    tcpi_options: u32,
    tcpi_flags: u32,
    tcpi_rto: u32,
    tcpi_maxseg: u32,
    tcpi_snd_ssthresh: u32,
    tcpi_snd_cwnd: u32,
    tcpi_snd_wnd: u32,
    tcpi_snd_sbbytes: u32,
    tcpi_rcv_wnd: u32,
    tcpi_rttcur: u32,
    tcpi_srtt: u32,
    tcpi_rttvar: u32,
    tcpi_tfo_bitfields: u32,
    tcpi_txpackets: u64,
    tcpi_txbytes: u64,
    tcpi_txretransmitbytes: u64,
    tcpi_rxpackets: u64,
    tcpi_rxbytes: u64,
    tcpi_rxoutoforderbytes: u64,
    tcpi_txretransmitpackets: u64,
}

#[cfg(target_os = "macos")]
fn sample_tcp_stats(stream: &TcpStream) -> Option<(u64, u64, u64)> {
    use std::os::fd::AsRawFd;
    unsafe {
        let mut info: MacTcpConnectionInfo = std::mem::zeroed();
        let mut len = std::mem::size_of::<MacTcpConnectionInfo>() as libc::socklen_t;
        let rc = libc::getsockopt(
            stream.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_CONNECTION_INFO,
            &mut info as *mut _ as *mut libc::c_void,
            &mut len,
        );
        if rc == 0 {
            let retrans_pkts = if info.tcpi_txretransmitpackets > 0 {
                info.tcpi_txretransmitpackets
            } else if info.tcpi_txretransmitbytes > 0 {
                let mss = (info.tcpi_maxseg as u64).max(1400);
                (info.tcpi_txretransmitbytes + mss - 1) / mss
            } else {
                0
            };
            Some((retrans_pkts, info.tcpi_txretransmitbytes, info.tcpi_txbytes))
        } else {
            None
        }
    }
}

#[cfg(target_os = "linux")]
fn sample_tcp_stats(stream: &TcpStream) -> Option<(u64, u64, u64)> {
    use std::os::fd::AsRawFd;
    unsafe {
        let mut info: libc::tcp_info = std::mem::zeroed();
        let mut len = std::mem::size_of::<libc::tcp_info>() as libc::socklen_t;
        let rc = libc::getsockopt(
            stream.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_INFO,
            &mut info as *mut _ as *mut libc::c_void,
            &mut len,
        );
        if rc == 0 {
            let retrans = info.tcpi_total_retrans as u64;
            let sent = info.tcpi_segs_out as u64;
            Some((retrans, retrans, sent))
        } else {
            None
        }
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn sample_tcp_stats(_stream: &TcpStream) -> Option<(u64, u64, u64)> {
    None
}
