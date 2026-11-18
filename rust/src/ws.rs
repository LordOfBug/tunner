//! RFC 6455 WebSocket transport + HTTP Upgrade SSH-key authentication.
//!
//! Designed for running `tunner` behind Cloudflare (orange cloud) or reverse proxies:
//! - Client sends a standard HTTP/1.1 `Upgrade: websocket` request to `--ws-path`.
//! - If SSH keys are configured, client signs `"{timestamp}:{nonce_hex}:{path}"` and
//!   sends `X-Tunner-Auth` header(s) in the HTTP request (0 extra round trips).
//! - Server validates the path and (if `-a` is set) verifies `X-Tunner-Auth` against
//!   `authorized_keys` within a ±300s timestamp window.
//! - Any request with the wrong path, missing upgrade headers, or invalid auth receives
//!   an ordinary `HTTP/1.1 404 Not Found` response, defeating active probes.

use crate::auth::AUTH_NAMESPACE;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use log::{debug, info, warn};
use rand::RngCore;
use ring::digest::{digest, SHA1_FOR_LEGACY_USE_ONLY};
use ssh_key::{HashAlg, LineEnding, PrivateKey, PublicKey, SshSig};
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream};

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const AUTH_HEADER: &str = "x-tunner-auth";
const MAX_HTTP_HEADER: usize = 16 * 1024;
const MAX_WS_FRAME: usize = 2 * 1024 * 1024;
const AUTH_TIME_SKEW_SECS: u64 = 300;

const OP_CONT: u8 = 0x0;
const OP_TEXT: u8 = 0x1;
const OP_BINARY: u8 = 0x2;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xA;

fn compute_accept_key(client_key: &str) -> String {
    let mut input = String::with_capacity(client_key.len() + WS_GUID.len());
    input.push_str(client_key.trim());
    input.push_str(WS_GUID);
    let hash = digest(&SHA1_FOR_LEGACY_USE_ONLY, input.as_bytes());
    B64.encode(hash.as_ref())
}

pub fn normalize_ws_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        "/ws".to_string()
    } else if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{}", trimmed)
    }
}

fn build_auth_header_value(
    private_key: &PrivateKey,
    path: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut nonce = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut nonce);
    let nonce_hex: String = nonce.iter().map(|b| format!("{:02x}", b)).collect();

    let message = format!("{}:{}:{}", now, nonce_hex, path);
    let sig = private_key.sign(AUTH_NAMESPACE, HashAlg::Sha512, message.as_bytes())?;
    let pem = sig.to_pem(LineEnding::LF)?;

    let token = format!("{}:{}:{}", now, nonce_hex, pem);
    Ok(B64.encode(token.as_bytes()))
}

fn verify_auth_header_value(
    header_val: &str,
    expected_path: &str,
    authorized_keys: &[PublicKey],
) -> Option<String> {
    let decoded = B64.decode(header_val.trim()).ok()?;
    let decoded_str = std::str::from_utf8(&decoded).ok()?;

    let mut parts = decoded_str.splitn(3, ':');
    let ts_str = parts.next()?;
    let nonce_hex = parts.next()?;
    let pem_str = parts.next()?;

    let ts: u64 = ts_str.parse().ok()?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if now.abs_diff(ts) > AUTH_TIME_SKEW_SECS {
        warn!("WebSocket auth timestamp out of window (ts={}, now={})", ts, now);
        return None;
    }

    let sig = SshSig::from_pem(pem_str.as_bytes()).ok()?;
    let key_data = sig.public_key();
    let pub_key = PublicKey::new(key_data.clone(), "");
    let is_authorized = authorized_keys
        .iter()
        .any(|allowed| allowed.key_data() == key_data);
    if !is_authorized {
        return None;
    }

    let message = format!("{}:{}:{}", ts_str, nonce_hex, expected_path);
    if pub_key
        .verify(AUTH_NAMESPACE, message.as_bytes(), &sig)
        .is_ok()
    {
        Some(pub_key.fingerprint(HashAlg::Sha256).to_string())
    } else {
        None
    }
}

async fn read_http_headers<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(1024);
    let mut byte = [0u8; 1];
    while buf.len() < MAX_HTTP_HEADER {
        let n = reader.read(&mut byte).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "EOF while reading HTTP headers",
            ));
        }
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") {
            return Ok(buf);
        }
    }
    Err(Error::new(
        ErrorKind::InvalidData,
        "HTTP header exceeds maximum length",
    ))
}

/// Client-side WebSocket handshake + SSH key header auth.
/// Returns a `DuplexStream` backed by the RFC 6455 binary frame pump.
pub async fn ws_client_connect<S>(
    mut stream: S,
    host: &str,
    path: &str,
    candidate_keys: &[(PathBuf, PrivateKey)],
) -> Result<DuplexStream, Box<dyn std::error::Error + Send + Sync>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let ws_path = normalize_ws_path(path);
    let mut key_bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut key_bytes);
    let sec_key = B64.encode(key_bytes);

    let mut req = format!(
        "GET {} HTTP/1.1\r\n\
         Host: {}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: {}\r\n\
         Sec-WebSocket-Version: 13\r\n\
         User-Agent: Mozilla/5.0\r\n",
        ws_path, host, sec_key
    );

    for (key_path, priv_key) in candidate_keys.iter().take(4) {
        match build_auth_header_value(priv_key, &ws_path) {
            Ok(val) => {
                debug!("Attached X-Tunner-Auth signature from {}", key_path.display());
                req.push_str(&format!("X-Tunner-Auth: {}\r\n", val));
            }
            Err(e) => {
                warn!("Failed signing WS auth with {}: {}", key_path.display(), e);
            }
        }
    }
    req.push_str("\r\n");

    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;

    let raw_resp = tokio::time::timeout(Duration::from_secs(10), read_http_headers(&mut stream))
        .await
        .map_err(|_| "Timed out waiting for WebSocket upgrade response")??;
    let resp_str = String::from_utf8_lossy(&raw_resp);
    let mut lines = resp_str.lines();
    let status_line = lines.next().unwrap_or("");
    if !status_line.contains(" 101 ") {
        return Err(format!("WebSocket upgrade rejected by server: {}", status_line).into());
    }

    let expected_accept = compute_accept_key(&sec_key);
    let mut accept_ok = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("sec-websocket-accept")
                && v.trim() == expected_accept
            {
                accept_ok = true;
            }
        }
    }
    if !accept_ok {
        return Err("Invalid or missing Sec-WebSocket-Accept in server response".into());
    }

    info!("WebSocket upgrade succeeded ({}{})", host, ws_path);
    Ok(spawn_ws_pump(stream, true))
}

/// Server-side WebSocket upgrade + path & SSH-key header verification.
/// On invalid path or unauthorized key, responds with an ordinary `HTTP/1.1 404 Not Found`.
pub async fn ws_server_accept<S>(
    mut stream: S,
    expected_path: &str,
    authorized_keys: Option<&[PublicKey]>,
) -> Result<DuplexStream, Box<dyn std::error::Error + Send + Sync>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let ws_path = normalize_ws_path(expected_path);
    let raw_req = tokio::time::timeout(Duration::from_secs(10), read_http_headers(&mut stream))
        .await
        .map_err(|_| "Timed out reading HTTP upgrade request")??;
    let req_str = String::from_utf8_lossy(&raw_req);
    let mut lines = req_str.lines();
    let request_line = lines.next().unwrap_or("");

    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let req_path = parts.next().unwrap_or("");
    let clean_path = req_path.split('?').next().unwrap_or(req_path);

    let mut sec_key: Option<String> = None;
    let mut is_upgrade = false;
    let mut auth_headers: Vec<String> = Vec::new();

    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let name = k.trim();
            let val = v.trim();
            if name.eq_ignore_ascii_case("sec-websocket-key") {
                sec_key = Some(val.to_string());
            } else if name.eq_ignore_ascii_case("upgrade")
                && val.to_ascii_lowercase().contains("websocket")
            {
                is_upgrade = true;
            } else if name.eq_ignore_ascii_case(AUTH_HEADER) {
                auth_headers.push(val.to_string());
            }
        }
    }

    if !method.eq_ignore_ascii_case("GET") || clean_path != ws_path || !is_upgrade || sec_key.is_none()
    {
        let _ = send_404(&mut stream).await;
        return Err(format!(
            "Rejected non-matching WebSocket request ({} {})",
            method, clean_path
        )
        .into());
    }

    if let Some(allowed) = authorized_keys {
        let mut matched_fp = None;
        for hdr in &auth_headers {
            if let Some(fp) = verify_auth_header_value(hdr, &ws_path, allowed) {
                matched_fp = Some(fp);
                break;
            }
        }
        match matched_fp {
            Some(fp) => {
                info!("Authenticated WebSocket client with SSH key {}", fp);
            }
            None => {
                let _ = send_404(&mut stream).await;
                return Err("Rejected WebSocket request: invalid or missing X-Tunner-Auth".into());
            }
        }
    }

    let accept = compute_accept_key(&sec_key.unwrap());
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {}\r\n\r\n",
        accept
    );
    stream.write_all(resp.as_bytes()).await?;
    stream.flush().await?;

    Ok(spawn_ws_pump(stream, false))
}

async fn send_404<S: AsyncWrite + Unpin>(stream: &mut S) -> std::io::Result<()> {
    let body = b"404 Not Found\n";
    let resp = format!(
        "HTTP/1.1 404 Not Found\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(resp.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    stream.shutdown().await
}

fn spawn_ws_pump<S>(stream: S, is_client: bool) -> DuplexStream
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (app_io, internal_io) = tokio::io::duplex(64 * 1024);
    let (mut internal_rd, mut internal_wr) = tokio::io::split(internal_io);
    let (mut ws_rd, mut ws_wr) = tokio::io::split(stream);
    let (ctrl_tx, mut ctrl_rx) = tokio::sync::mpsc::unbounded_channel::<(u8, Vec<u8>)>();

    // Dedicated WebSocket reader task — never cancelled mid-frame by outbound writes
    let ctrl_tx_rd = ctrl_tx.clone();
    tokio::spawn(async move {
        loop {
            match read_ws_frame(&mut ws_rd).await {
                Ok(Some((opcode, payload))) => match opcode {
                    OP_BINARY | OP_TEXT | OP_CONT => {
                        if !payload.is_empty() && internal_wr.write_all(&payload).await.is_err() {
                            break;
                        }
                    }
                    OP_PING => {
                        if ctrl_tx_rd.send((OP_PONG, payload)).is_err() {
                            break;
                        }
                    }
                    OP_PONG => {}
                    OP_CLOSE => {
                        let _ = ctrl_tx_rd.send((OP_CLOSE, Vec::new()));
                        break;
                    }
                    _ => {}
                },
                Ok(None) | Err(_) => break,
            }
        }
        let _ = internal_wr.shutdown().await;
    });

    // Dedicated WebSocket writer task — never cancels incoming WebSocket frame reads
    tokio::spawn(async move {
        let mut out_buf = vec![0u8; 64 * 1024];
        loop {
            tokio::select! {
                 biased;
                ctrl_msg = ctrl_rx.recv() => {
                    match ctrl_msg {
                        Some((OP_PONG, payload)) => {
                            if write_ws_frame(&mut ws_wr, OP_PONG, &payload, is_client).await.is_err() {
                                break;
                            }
                        }
                        Some((OP_CLOSE, _)) | None => {
                            let _ = write_ws_frame(&mut ws_wr, OP_CLOSE, &[], is_client).await;
                            break;
                        }
                        Some(_) => {}
                    }
                }
                read_res = internal_rd.read(&mut out_buf) => {
                    match read_res {
                        Ok(0) => {
                            let _ = write_ws_frame(&mut ws_wr, OP_CLOSE, &[], is_client).await;
                            break;
                        }
                        Ok(n) => {
                            if write_ws_frame(&mut ws_wr, OP_BINARY, &out_buf[..n], is_client).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
        let _ = ws_wr.shutdown().await;
    });

    app_io
}

async fn read_ws_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<(u8, Vec<u8>)>> {
    let mut hdr = [0u8; 2];
    match reader.read_exact(&mut hdr).await {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    let opcode = hdr[0] & 0x0F;
    let masked = (hdr[1] & 0x80) != 0;
    let len_code = (hdr[1] & 0x7F) as usize;

    let payload_len = if len_code == 126 {
        let mut ext = [0u8; 2];
        reader.read_exact(&mut ext).await?;
        u16::from_be_bytes(ext) as usize
    } else if len_code == 127 {
        let mut ext = [0u8; 8];
        reader.read_exact(&mut ext).await?;
        u64::from_be_bytes(ext) as usize
    } else {
        len_code
    };

    if payload_len > MAX_WS_FRAME {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "WebSocket frame exceeds max size",
        ));
    }

    let mut mask_key = [0u8; 4];
    if masked {
        reader.read_exact(&mut mask_key).await?;
    }

    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        reader.read_exact(&mut payload).await?;
        if masked {
            for (i, b) in payload.iter_mut().enumerate() {
                *b ^= mask_key[i & 3];
            }
        }
    }

    Ok(Some((opcode, payload)))
}

async fn write_ws_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    opcode: u8,
    payload: &[u8],
    mask: bool,
) -> std::io::Result<()> {
    let len = payload.len();
    let mask_bit = if mask { 0x80u8 } else { 0x00u8 };

    let mut header = Vec::with_capacity(14);
    header.push(0x80 | (opcode & 0x0F)); // FIN = 1
    if len < 126 {
        header.push(mask_bit | (len as u8));
    } else if len <= u16::MAX as usize {
        header.push(mask_bit | 126);
        header.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        header.push(mask_bit | 127);
        header.extend_from_slice(&(len as u64).to_be_bytes());
    }

    if mask {
        let mut mask_key = [0u8; 4];
        rand::thread_rng().fill_bytes(&mut mask_key);
        header.extend_from_slice(&mask_key);
        writer.write_all(&header).await?;
        if len > 0 {
            let mut masked = payload.to_vec();
            for (i, b) in masked.iter_mut().enumerate() {
                *b ^= mask_key[i & 3];
            }
            writer.write_all(&masked).await?;
        }
    } else {
        writer.write_all(&header).await?;
        if len > 0 {
            writer.write_all(payload).await?;
        }
    }

    writer.flush().await
}
