# Tunner (Rust Version)

A lightweight, asynchronous TCP connection multiplexer with **TLS 1.3 encryption**, **SSH key authentication**, **TCP half-close (`FIN`) support**, and **traffic shaping / background noise injection**, built on Tokio.

---

## Operating Modes

Tunner supports two transport modes depending on the flags you pass:

### 1. Plain TCP Mode (Default — no TLS/auth flags)
* When neither `--tls`, `-a/--authorized-keys`, nor `-k/--key` is passed, `tunserver` and `tunclient` communicate over **plain TCP**—exactly as before TLS support.
* Fully compatible with the Java implementation or an external `ssh -L` tunnel.

### 2. TLS 1.3 Mode (`--tls`) & Optional SSH Key Auth (`-a` / `-k`)
* Passing `--tls` on `tunserver` enables **TLS 1.3** encryption on the tunnel port.
  * If `--tls-cert` and `--tls-key` are omitted, `tunserver` auto-generates an ephemeral self-signed certificate on startup and prints its SHA-256 fingerprint.
* Passing `-a / --authorized-keys <PATH>` on `tunserver` **automatically enables TLS 1.3** and requires every connecting client to authenticate via challenge-response against the SSH public keys listed in that file (standard OpenSSH `authorized_keys` format, supporting multiple keys).
* On `tunclient`:
  * TLS is enabled whenever `--tls`, `-k/--key`, `--tls-ca`, `--tls-fingerprint`, or `--tls-insecure` is passed.
  * If `-k` is omitted in TLS mode, `tunclient` automatically discovers private keys in `~/.ssh/` (`id_ed25519`, `id_ecdsa`, `id_rsa`, `id_dsa`) and tries them until one matches the server's `authorized_keys`—just like `ssh`.
  * You can also pass `-k <PATH>` one or more times explicitly.

---

## Building

```bash
cd rust
cargo build --release
```

Compiled binaries:
* `rust/target/release/tunserver`
* `rust/target/release/tunclient`

## Running Tests

```bash
cd rust
cargo test
```

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
| `--tls` | `false` | Enable TLS 1.3 transport |
| `-k, --key <PATH>` | *(auto `~/.ssh/`)* | SSH private key(s) for auth (repeatable; auto-discovers `~/.ssh/id_*` if omitted) |
| `--passphrase <STR>` | *(none)* | Passphrase for encrypted SSH key (or set `TUNNER_KEY_PASSPHRASE`) |
| `--tls-ca <PATH>` | *(none)* | CA/server PEM certificate to verify server |
| `--tls-fingerprint <HEX>` | *(none)* | Pin server TLS certificate by SHA-256 hex fingerprint |
| `--tls-insecure` | `false` | Skip TLS certificate verification (still encrypts + runs SSH key auth) |

---

## Usage Examples

### A. TLS 1.3 + SSH Key Auth (Recommended for Direct Internet Use)

**1. On your remote server** (forwarding to a local SOCKS5 proxy on `127.0.0.1:8888`):
```bash
./target/release/tunserver -i 0.0.0.0 -l 3333 -f 8888 -a ~/.ssh/authorized_keys
```
Note the SHA-256 fingerprint printed at startup (or use your own `--tls-cert` / `--tls-key`).

**2. On your local machine** (auto-discovers `~/.ssh/id_*` keys):
```bash
./target/release/tunclient -l 6666 -r 3333 -n 512 --tls-fingerprint <SHA256> your.server.ip
```
*(Or pass `-k ~/.ssh/my_key` explicitly, or `--tls-insecure` for quick testing without pinning).*

### B. Plain TCP Mode (With Built-in Echo Test Server)

**1. Start server in test mode:**
```bash
./target/release/tunserver -m test -l 9999 -f 8888
```

**2. Start client with 100k noise:**
```bash
./target/release/tunclient -l 8080 -r 9999 -n 100k 127.0.0.1
```

**3. Test:**
```bash
nc 127.0.0.1 8080
```
