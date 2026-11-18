# Tunner (Rust Version)

A high-performance, asynchronous TCP connection multiplexer with **TLS 1.3 encryption**, **WebSocket transport (Cloudflare / CDN compatible)**, **SSH key authentication**, **XOFF/XON per-stream backpressure**, **keepalive + auto-reconnect**, **TCP half-close (`FIN`) support**, and **jittered background noise injection**, built on Tokio.

---

## Operating Modes

Tunner supports three transport modes depending on the flags you pass:

### 1. Plain TCP Mode (Default — no TLS/WS/auth flags)
* When neither `--tls`, `--ws-path`, `-a/--authorized-keys`, nor `-k/--key` is passed, `tunserver` and `tunclient` communicate over **plain TCP**—compatible with an external `ssh -L` tunnel.

### 2. Direct TLS 1.3 Mode (`--tls`) & SSH Key Auth (`-a` / `-k`)
* Passing `--tls` or `-a / --authorized-keys <PATH>` on `tunserver` enables **TLS 1.3** encryption on the tunnel port.
  * If `--tls-cert` and `--tls-key` are omitted, `tunserver` auto-generates an ephemeral self-signed certificate on startup and prints its SHA-256 fingerprint.
* On `tunclient`:
  * TLS is enabled whenever `--tls`, `-k/--key` (without `--ws-path`), `--tls-ca`, `--tls-fingerprint`, or `--tls-insecure` is passed.
  * If `--tls-ca`, `--tls-fingerprint`, and `--tls-insecure` are all omitted, `tunclient` automatically verifies against the **OS system root CA store** (for Let's Encrypt or Cloudflare certificates).
  * If `-k` is omitted, `tunclient` automatically discovers private keys in `~/.ssh/` (`id_ed25519`, `id_ecdsa`, `id_rsa`, `id_dsa`).

### 3. WebSocket over TLS Mode (`--ws-path <PATH>`) — Cloudflare / CDN Compatible
* Passing `--ws-path /your-secret-path` wraps the tunnel in RFC 6455 binary WebSocket frames and moves SSH key authentication into the `X-Tunner-Auth` HTTP Upgrade header (zero extra round trips).
* Any HTTP request to a wrong path or without a valid SSH signature receives a normal `HTTP/1.1 404 Not Found` response, defeating active probing.
* Works directly with **Cloudflare Proxy (orange cloud)** on port `443`.

---

## Building & Testing

```bash
cd rust
cargo test
cargo build --release
```

Compiled binaries:
* `rust/target/release/tunserver`
* `rust/target/release/tunclient`

---

## CLI Reference

### `tunserver`
| Flag | Default | Description |
| :--- | :--- | :--- |
| `-i, --interface <IP>` | `0.0.0.0` | Interface to listen for tunnel clients |
| `-l, --listen <PORT>` | `8080` | Port to listen for tunnel clients |
| `-s, --server <HOST>` | `127.0.0.1` | Target backend / proxy host |
| `-f, --forward-port <PORT>` | `8888` | Target backend / proxy port |
| `-m, --mode <MODE>` | `prod` | `prod` or `test` (`test` spawns a built-in TCP echo server on `-f`) |
| `--tls` | `false` | Enable TLS 1.3 transport |
| `--tls-cert <PATH>` | *(auto)* | PEM certificate file (auto-generates self-signed if omitted) |
| `--tls-key <PATH>` | *(auto)* | PEM private key file |
| `-a, --authorized-keys <PATH>` | *(none)* | OpenSSH `authorized_keys` file (auto-enables `--tls` and enforces SSH key auth) |
| `--ws-path <PATH>` | *(none)* | Accept WebSocket upgrade on this HTTP path (e.g. `/ws`) |
| `--keepalive <SECS>` | `20` | Keepalive ping interval in seconds (`0` to disable; timeout is `3 * keepalive`) |
| `--tsdb-url <URL>` | *(none)* | Optional TSDB URL (e.g. `https://tsdb.zoomphant.com`) for metrics & logs |
| `--tsdb-token <TOKEN>` | *(none)* | Bearer token for TSDB `/api/v1/push` |
| `--tsdb-interval <SECS>` | `15` | Metrics push interval in seconds |

### `tunclient`
| Flag | Default | Description |
| :--- | :--- | :--- |
| `-i, --interface <IP>` | `127.0.0.1` | Local interface to listen for app connections |
| `-l, --listen <PORT>` | `8080` | Local port to listen for app connections |
| `<SERVER>` | *(required)* | Remote `tunserver` host |
| `-r, --remote-port <PORT>` | `8080` | Remote `tunserver` port |
| `-n, --min-rate <RATE>` | `0` | Guaranteed minimum stream rate (`0` = disabled; e.g. `512`, `10k`, `1m`) |
| `--noise-file <PATH>` | *(random)* | File to stream cyclically as dummy noise |
| `-e, --echo` | `false` | Request server to echo dummy noise back (default: server drops noise) |
| `--tls` | `false` | Enable TLS 1.3 transport (uses system root CAs if no `--tls-*` verifier flag is given) |
| `-k, --key <PATH>` | *(auto `~/.ssh/`)* | SSH private key(s) for auth (repeatable; auto-discovers `~/.ssh/id_*` if omitted) |
| `--passphrase <STR>` | *(none)* | Passphrase for encrypted SSH key (or set `TUNNER_KEY_PASSPHRASE`) |
| `--tls-ca <PATH>` | *(none)* | CA/server PEM certificate to verify server |
| `--tls-fingerprint <HEX>` | *(none)* | Pin server TLS certificate by SHA-256 hex fingerprint |
| `--tls-insecure` | `false` | Skip TLS certificate verification (still encrypts + runs SSH key auth) |
| `--ws-path <PATH>` | *(none)* | Connect over WebSocket on this HTTP path (e.g. `/ws`) |
| `--keepalive <SECS>` | `20` | Keepalive ping interval in seconds (`0` to disable) |
| `--no-reconnect` | `false` | Exit instead of auto-reconnecting when the tunnel drops |
| `--tsdb-url <URL>` | *(none)* | Optional TSDB URL (e.g. `https://tsdb.zoomphant.com`) for metrics & logs |
| `--tsdb-token <TOKEN>` | *(none)* | Bearer token for TSDB `/api/v1/push` |
| `--tsdb-interval <SECS>` | `15` | Metrics push interval in seconds |

---

## Optional TSDB Telemetry (`tsdb.zoomphant.com`)

Both `tunclient` and `tunserver` can push performance telemetry (RTT latency via `PING`/`PONG`, backend connect latency, failure rate, stream end counts, throughput, batch sizes, pause events, reconnects, auth failures, and structured events) to `POST /api/v1/push`.

### Configuration (in priority order)
1. **CLI flags:** `--tsdb-url https://tsdb.zoomphant.com --tsdb-token <TOKEN> --tsdb-interval 15`
2. **Environment variables:** `TUNNER_TSDB_URL`, `TUNNER_TSDB_TOKEN`, `TUNNER_TSDB_INTERVAL`
3. **Home config file** (`~/.tunner/tsdb.conf` or `~/.tunner.conf`):
   ```ini
   url = https://tsdb.zoomphant.com
   token = your-ingest-or-api-token
   interval = 15
   ```

Streams are tagged with `app="tunner"`, `role="client"|"server"`, `host`, `mode` (`ws-tls` | `tls` | `ws` | `tcp`), and `version` so releases and transport modes can be compared in ZQL:
```zql
{app="tunner"} | metrics tunner_rtt_ms | avg(value) every 5m by (role, mode, host, version)
```

---

## Usage Examples

### A. Cloudflare Proxy (Orange Cloud) + WebSocket + SSH Key Auth (`:443`)

**1. On your remote server** (using a Cloudflare Origin CA cert or Let's Encrypt cert, forwarding to `127.0.0.1:6666`):
```bash
sudo /usr/local/bin/tunserver \
  --tls --tls-cert /etc/ssl/cf-origin.pem --tls-key /etc/ssl/cf-origin.key \
  --ws-path /secret-tunnel \
  -a /home/ubuntu/.ssh/authorized_keys \
  -l 443 -f 6666
```

**2. On your Mac** (system root CAs verify Cloudflare's edge cert automatically):
```bash
./target/release/tunclient \
  -l 8866 -r 443 -n 500 \
  --tls --ws-path /secret-tunnel \
  -k ~/.ssh/id_ed25519 \
  proxy.foo.com
```

### B. Direct TLS 1.3 + Self-Signed Cert Pinning

**1. On your remote server:**
```bash
sudo /usr/local/bin/tunserver --tls -a /home/ubuntu/.ssh/authorized_keys -l 443 -f 6666
```

**2. On your Mac:**
```bash
./target/release/tunclient -l 8866 -r 443 -n 500 --tls-fingerprint <SHA256> -k ~/.ssh/id_ed25519 proxy.kr
```
