# Tunner (Rust Version)

A lightweight, asynchronous TCP connection multiplexer with traffic shaping and background dummy noise injection, built with Tokio.

## Features

- **Asynchronous & High-Performance:** Pure Tokio async runtime with non-blocking TCP sockets.
- **Connection Multiplexing:** Multiplexes multiple local client TCP connections over a single tunnel connection.
- **Wire Framing Protocol:** 8-byte header (`type_seq`, `length`, `conn_id`) + payload + 8-byte boundary alignment padding (`0xFF`).
- **Guaranteed Minimum Data Rate (Traffic Shaping):** 
  - Supports rate strings like `10k`, `500k`, `1m`, `2g`.
  - When real traffic is below the threshold, injects cyclic dummy data from a user-supplied file (or in-memory pseudo-random bytes).
  - When real traffic meets or exceeds the threshold, no noise packets are emitted.
- **Client-Controlled Echo Behavior:**
  - Default: Server silently drops dummy payload to conserve return bandwidth.
  - `--echo` (`-e`): Server reflects the dummy payload back for symmetric bidirectional traffic shaping.
- **Built-in Test Mode:** Server has a `-m test` flag that automatically spawns a local TCP EchoServer on the forward port for testing without external services.

## Building

```bash
cd rust
cargo build --release
```

The compiled binaries will be at:
- `rust/target/release/tunserver`
- `rust/target/release/tunclient`

## Running Tests

```bash
cd rust
cargo test
```

## Quick Start / Manual Testing

### 1. Start Server in Test Mode
```bash
# Starts echo server on 8888 and listens for tunnels on 9999
./target/release/tunserver -m test -l 9999 -f 8888
```

### 2. Start Client with 100k Noise Rate
```bash
# Optional: create a dummy file
head -c 1048576 /dev/urandom > /tmp/dummy_noise.bin

# Starts client listening locally on 8080, connecting to tunnel on 9999
./target/release/tunclient -l 8080 -r 9999 -n 100k --noise-file /tmp/dummy_noise.bin 127.0.0.1
```

*(Add `-e` to request server echo, or omit it to drop noise at server side).*

### 3. Test Real Traffic
```bash
nc 127.0.0.1 8080
```
Type any message and press Enter. The echo server will respond immediately while background dummy noise streams concurrently.

