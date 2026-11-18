use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct ProcessGuard(Child);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn wait_for_port(port: u16, max_retries: usize) -> Result<TcpStream, std::io::Error> {
    let addr = format!("127.0.0.1:{}", port);
    for _ in 0..max_retries {
        match TcpStream::connect(&addr).await {
            Ok(stream) => return Ok(stream),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    TcpStream::connect(&addr).await
}

#[tokio::test]
async fn test_rust_end_to_end_with_noise() -> Result<(), Box<dyn std::error::Error>> {
    let server_bin = env!("CARGO_BIN_EXE_tunserver");
    let client_bin = env!("CARGO_BIN_EXE_tunclient");

    // Create a dummy noise file
    let noise_file = std::env::temp_dir().join("test_rust_noise.dat");
    std::fs::write(&noise_file, b"DUMMY_NOISE_BYTES_0123456789\n".repeat(500))?;

    let tunnel_port = 18100;
    let echo_port = 18101;
    let client_port = 18102;

    // 1. Start TunServer in test mode (creates echo server on echo_port)
    let _server_proc = ProcessGuard(
        Command::new(server_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &tunnel_port.to_string(),
                "-f",
                &echo_port.to_string(),
                "-m",
                "test",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?,
    );

    // Wait for server to bind tunnel_port
    let _probe = wait_for_port(tunnel_port, 30).await?;
    drop(_probe);

    // 2. Start TunClient with noise enabled
    let _client_proc = ProcessGuard(
        Command::new(client_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &client_port.to_string(),
                "-r",
                &tunnel_port.to_string(),
                "-n",
                "50k",
                "--noise-file",
                noise_file.to_str().unwrap(),
                "127.0.0.1",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?,
    );

    // Wait for client to bind client_port
    let mut stream = wait_for_port(client_port, 30).await?;

    // Test small ping
    let ping = b"Ping from Rust test!\n";
    stream.write_all(ping).await?;
    stream.flush().await?;

    let mut buf = vec![0u8; ping.len()];
    stream.read_exact(&mut buf).await?;
    assert_eq!(&buf[..], ping);

    // Sleep while dummy noise streams in background
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Test large payload (15 KB)
    let large_data: Vec<u8> = (0..15360).map(|i| (i % 256) as u8).collect();
    stream.write_all(&large_data).await?;
    stream.flush().await?;

    let mut recv_data = vec![0u8; large_data.len()];
    stream.read_exact(&mut recv_data).await?;
    assert_eq!(recv_data, large_data);

    let _ = std::fs::remove_file(noise_file);
    Ok(())
}

#[tokio::test]
async fn test_no_fd_leak_on_server_close() -> Result<(), Box<dyn std::error::Error>> {
    use tokio::net::TcpListener;

    let server_bin = env!("CARGO_BIN_EXE_tunserver");
    let client_bin = env!("CARGO_BIN_EXE_tunclient");

    let tunnel_port = 18200;
    let backend_port = 18201;
    let client_port = 18202;

    // Mock backend that writes "OK\n" and immediately closes the socket (triggers CODE_ABORT from server)
    let backend_listener = TcpListener::bind(format!("127.0.0.1:{}", backend_port)).await?;
    tokio::spawn(async move {
        loop {
            if let Ok((mut sock, _)) = backend_listener.accept().await {
                tokio::spawn(async move {
                    let mut tmp = [0u8; 64];
                    let _ = sock.read(&mut tmp).await;
                    let _ = sock.write_all(b"OK\n").await;
                    let _ = sock.shutdown().await;
                });
            }
        }
    });

    let _server_proc = ProcessGuard(
        Command::new(server_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &tunnel_port.to_string(),
                "-f",
                &backend_port.to_string(),
                "-m",
                "prod",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    let _probe = wait_for_port(tunnel_port, 30).await?;
    drop(_probe);

    let _client_proc = ProcessGuard(
        Command::new(client_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &client_port.to_string(),
                "-r",
                &tunnel_port.to_string(),
                "127.0.0.1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    // Wait for client to bind
    let s = wait_for_port(client_port, 30).await?;
    drop(s);

    // Open 350 connections sequentially where the server closes first (exceeds macOS 256 FD limit if leaked)
    for i in 0..350 {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", client_port)).await?;
        stream.write_all(b"GET /\n").await?;
        let mut buf = [0u8; 3];
        stream.read_exact(&mut buf).await.map_err(|e| {
            format!("Failed at iteration {}: {}", i, e)
        })?;
        assert_eq!(&buf, b"OK\n");
        // Verify server-side close propagates EOF to client
        let mut eof_buf = [0u8; 1];
        let n = stream.read(&mut eof_buf).await?;
        assert_eq!(n, 0, "Expected EOF from server close at iteration {}", i);
    }

    Ok(())
}

#[tokio::test]
async fn test_tcp_half_close_preserves_response() -> Result<(), Box<dyn std::error::Error>> {
    use tokio::net::TcpListener;

    let server_bin = env!("CARGO_BIN_EXE_tunserver");
    let client_bin = env!("CARGO_BIN_EXE_tunclient");

    let tunnel_port = 18300;
    let backend_port = 18301;
    let client_port = 18302;

    // Mock backend that reads until client EOF (FIN) before sending its response
    let backend_listener = TcpListener::bind(format!("127.0.0.1:{}", backend_port)).await?;
    tokio::spawn(async move {
        loop {
            if let Ok((mut sock, _)) = backend_listener.accept().await {
                tokio::spawn(async move {
                    let mut req = Vec::new();
                    let _ = sock.read_to_end(&mut req).await;
                    if req == b"REQUEST_BEFORE_FIN\n" {
                        let _ = sock.write_all(b"RESPONSE_AFTER_CLIENT_EOF\n").await;
                    }
                    let _ = sock.shutdown().await;
                });
            }
        }
    });

    let _server_proc = ProcessGuard(
        Command::new(server_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &tunnel_port.to_string(),
                "-f",
                &backend_port.to_string(),
                "-m",
                "prod",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    let _probe = wait_for_port(tunnel_port, 30).await?;
    drop(_probe);

    let _client_proc = ProcessGuard(
        Command::new(client_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &client_port.to_string(),
                "-r",
                &tunnel_port.to_string(),
                "127.0.0.1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    let mut stream = wait_for_port(client_port, 30).await?;
    stream.write_all(b"REQUEST_BEFORE_FIN\n").await?;
    // Half-close write side (send TCP FIN) while keeping read side open
    stream.shutdown().await?;

    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await?;
    assert_eq!(resp, b"RESPONSE_AFTER_CLIENT_EOF\n");

    Ok(())
}

#[tokio::test]
async fn test_tls_ssh_key_auth_success_and_reject() -> Result<(), Box<dyn std::error::Error>> {
    use ssh_key::{Algorithm, LineEnding, PrivateKey};

    let server_bin = env!("CARGO_BIN_EXE_tunserver");
    let client_bin = env!("CARGO_BIN_EXE_tunclient");

    // Generate two authorized keys and one unauthorized key
    let mut rng = rand::thread_rng();
    let key1 = PrivateKey::random(&mut rng, Algorithm::Ed25519)?;
    let key2 = PrivateKey::random(&mut rng, Algorithm::Ed25519)?;
    let bad_key = PrivateKey::random(&mut rng, Algorithm::Ed25519)?;

    let tmp_dir = std::env::temp_dir();
    let auth_keys_path = tmp_dir.join("test_tunner_authorized_keys");
    let key2_path = tmp_dir.join("test_tunner_id_ed25519_2");
    let bad_key_path = tmp_dir.join("test_tunner_id_ed25519_bad");

    let auth_contents = format!(
        "{}\n{}\n",
        key1.public_key().to_openssh()?,
        key2.public_key().to_openssh()?
    );
    std::fs::write(&auth_keys_path, auth_contents)?;
    key2.write_openssh_file(&key2_path, LineEnding::LF)?;
    bad_key.write_openssh_file(&bad_key_path, LineEnding::LF)?;

    let tunnel_port = 18400;
    let echo_port = 18401;
    let client_port = 18402;

    // Start TunServer with TLS + authorized_keys (multiple keys)
    let _server_proc = ProcessGuard(
        Command::new(server_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &tunnel_port.to_string(),
                "-f",
                &echo_port.to_string(),
                "-m",
                "test",
                "--authorized-keys",
                auth_keys_path.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    let _probe = wait_for_port(tunnel_port, 30).await?;
    drop(_probe);

    // 1. Unauthorized key should be rejected by server and exit non-zero
    let bad_status = Command::new(client_bin)
        .args([
            "-i",
            "127.0.0.1",
            "-l",
            "18403",
            "-r",
            &tunnel_port.to_string(),
            "-k",
            bad_key_path.to_str().unwrap(),
            "--tls-insecure",
            "127.0.0.1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    assert!(!bad_status.success(), "Unauthorized key should be rejected");

    // 2. Multi-key fallback: pass bad_key first, then authorized key2 -> should try bad_key, fallback to key2, and succeed
    let _client_proc = ProcessGuard(
        Command::new(client_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &client_port.to_string(),
                "-r",
                &tunnel_port.to_string(),
                "-k",
                bad_key_path.to_str().unwrap(),
                "-k",
                key2_path.to_str().unwrap(),
                "--tls-insecure",
                "-n",
                "10k",
                "127.0.0.1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    let mut stream = wait_for_port(client_port, 30).await?;
    let msg = b"Authenticated over TLS 1.3 + SSH key!\n";
    stream.write_all(msg).await?;
    stream.shutdown().await?;

    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await?;
    assert_eq!(resp, msg);

    let _ = std::fs::remove_file(auth_keys_path);
    let _ = std::fs::remove_file(key2_path);
    let _ = std::fs::remove_file(bad_key_path);

    Ok(())
}

#[tokio::test]
async fn test_websocket_tls_auth_and_404_probe_defense() -> Result<(), Box<dyn std::error::Error>> {
    use ssh_key::{Algorithm, LineEnding, PrivateKey};
    use tunner::tls::{build_client_connector, make_server_name};

    let server_bin = env!("CARGO_BIN_EXE_tunserver");
    let client_bin = env!("CARGO_BIN_EXE_tunclient");

    let mut rng = rand::thread_rng();
    let key = PrivateKey::random(&mut rng, Algorithm::Ed25519)?;
    let bad_key = PrivateKey::random(&mut rng, Algorithm::Ed25519)?;

    let tmp_dir = std::env::temp_dir();
    let auth_keys_path = tmp_dir.join("test_ws_authorized_keys");
    let key_path = tmp_dir.join("test_ws_id_ed25519");
    let bad_key_path = tmp_dir.join("test_ws_id_ed25519_bad");

    std::fs::write(
        &auth_keys_path,
        format!("{}\n", key.public_key().to_openssh()?),
    )?;
    key.write_openssh_file(&key_path, LineEnding::LF)?;
    bad_key.write_openssh_file(&bad_key_path, LineEnding::LF)?;

    let tunnel_port = 18500;
    let echo_port = 18501;
    let client_port = 18502;

    let _server_proc = ProcessGuard(
        Command::new(server_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &tunnel_port.to_string(),
                "-f",
                &echo_port.to_string(),
                "-m",
                "test",
                "--tls",
                "--ws-path",
                "/secret-tunnel",
                "--authorized-keys",
                auth_keys_path.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    let _probe = wait_for_port(tunnel_port, 30).await?;
    drop(_probe);

    // 1. Active probe (normal GET /) must receive HTTP/1.1 404 Not Found
    let connector = build_client_connector(None, None, true)?;
    let tcp = TcpStream::connect(format!("127.0.0.1:{}", tunnel_port)).await?;
    let mut tls = connector
        .connect(make_server_name("127.0.0.1"), tcp)
        .await?;
    tls.write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        .await?;
    let mut probe_resp = Vec::new();
    tls.read_to_end(&mut probe_resp).await?;
    let probe_str = String::from_utf8_lossy(&probe_resp);
    assert!(
        probe_str.starts_with("HTTP/1.1 404 Not Found"),
        "Expected 404 for active probe, got: {}",
        probe_str
    );

    // 2. Unauthorized key on /secret-tunnel must be rejected (404) and client exits non-zero
    let bad_status = Command::new(client_bin)
        .args([
            "-i",
            "127.0.0.1",
            "-l",
            "18503",
            "-r",
            &tunnel_port.to_string(),
            "--ws-path",
            "/secret-tunnel",
            "-k",
            bad_key_path.to_str().unwrap(),
            "--tls-insecure",
            "127.0.0.1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    assert!(!bad_status.success(), "Unauthorized WS key should fail");

    // 3. Authorized key on /secret-tunnel succeeds and tunnels traffic over WebSocket + TLS + noise
    let _client_proc = ProcessGuard(
        Command::new(client_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &client_port.to_string(),
                "-r",
                &tunnel_port.to_string(),
                "-n",
                "50k",
                "--ws-path",
                "/secret-tunnel",
                "-k",
                key_path.to_str().unwrap(),
                "--tls-insecure",
                "127.0.0.1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    let stream = wait_for_port(client_port, 30).await?;
    let payload: Vec<u8> = (0..128_000).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();
    let (mut rd, mut wr) = stream.into_split();
    let writer = tokio::spawn(async move {
        for chunk in payload.chunks(4096) {
            wr.write_all(chunk).await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        wr.shutdown().await.unwrap();
    });

    let mut resp = Vec::new();
    rd.read_to_end(&mut resp).await?;
    writer.await?;
    assert_eq!(resp, expected);

    let _ = std::fs::remove_file(auth_keys_path);
    let _ = std::fs::remove_file(key_path);
    let _ = std::fs::remove_file(bad_key_path);
    Ok(())
}

#[tokio::test]
async fn test_pause_resume_no_head_of_line_blocking() -> Result<(), Box<dyn std::error::Error>> {
    let server_bin = env!("CARGO_BIN_EXE_tunserver");
    let client_bin = env!("CARGO_BIN_EXE_tunclient");

    let tunnel_port = 18600;
    let echo_port = 18601;
    let client_port = 18602;

    let _server_proc = ProcessGuard(
        Command::new(server_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &tunnel_port.to_string(),
                "-f",
                &echo_port.to_string(),
                "-m",
                "test",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    let _probe = wait_for_port(tunnel_port, 30).await?;
    drop(_probe);

    let _client_proc = ProcessGuard(
        Command::new(client_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &client_port.to_string(),
                "-r",
                &tunnel_port.to_string(),
                "127.0.0.1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    // Connection A: write 2.5 MB without reading yet (exceeds PAUSE_HIGH_WATERMARK = 1 MB)
    let stalled_stream = wait_for_port(client_port, 30).await?;
    let big_payload = vec![0xABu8; 2_500_000];
    let payload_clone = big_payload.clone();
    let (mut stalled_rd, mut stalled_wr) = stalled_stream.into_split();

    // Connection B: while A's receive queue is stalled/paused, B must still work without blocking
    let write_and_stalled = async move {
        let writer = tokio::spawn(async move {
            stalled_wr.write_all(&payload_clone).await.unwrap();
            stalled_wr.shutdown().await.unwrap();
        });

        // Give A time to flood over 1 MB so PAUSE triggers
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Verify Connection B completes 20 fast ping-pongs while A is not reading
        let mut fast_stream = TcpStream::connect(format!("127.0.0.1:{}", client_port)).await?;
        for i in 0..20u32 {
            let msg = format!("fast-ping-{}\n", i);
            fast_stream.write_all(msg.as_bytes()).await?;
            let mut reply = vec![0u8; msg.len()];
            tokio::time::timeout(Duration::from_secs(2), fast_stream.read_exact(&mut reply))
                .await
                .map_err(|_| "Head-of-line blocking detected: fast stream timed out!")??;
            assert_eq!(reply, msg.as_bytes());
        }
        drop(fast_stream);

        // Now drain Connection A (triggers RESUME and completes all 2.5 MB)
        let mut drained = Vec::with_capacity(big_payload.len());
        stalled_rd.read_to_end(&mut drained).await?;
        writer.await?;
        assert_eq!(drained.len(), big_payload.len());
        assert_eq!(drained, big_payload);
        Ok::<(), Box<dyn std::error::Error>>(())
    };

    write_and_stalled.await?;
    Ok(())
}

#[tokio::test]
async fn test_client_auto_reconnect() -> Result<(), Box<dyn std::error::Error>> {
    let server_bin = env!("CARGO_BIN_EXE_tunserver");
    let client_bin = env!("CARGO_BIN_EXE_tunclient");

    let tunnel_port = 18700;
    let echo_port = 18701;
    let client_port = 18702;

    let server_proc_1 = ProcessGuard(
        Command::new(server_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &tunnel_port.to_string(),
                "-f",
                &echo_port.to_string(),
                "-m",
                "test",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    let _probe = wait_for_port(tunnel_port, 30).await?;
    drop(_probe);

    let _client_proc = ProcessGuard(
        Command::new(client_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &client_port.to_string(),
                "-r",
                &tunnel_port.to_string(),
                "127.0.0.1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    // 1. First request before server restart
    let mut s1 = wait_for_port(client_port, 30).await?;
    s1.write_all(b"before-restart\n").await?;
    let mut buf1 = [0u8; 15];
    s1.read_exact(&mut buf1).await?;
    assert_eq!(&buf1, b"before-restart\n");
    drop(s1);

    // 2. Kill server and restart it
    drop(server_proc_1);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let _server_proc_2 = ProcessGuard(
        Command::new(server_bin)
            .args([
                "-i",
                "127.0.0.1",
                "-l",
                &tunnel_port.to_string(),
                "-f",
                &echo_port.to_string(),
                "-m",
                "test",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    // Wait for client supervisor to auto-reconnect and serve a new connection
    let mut reconnected = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Ok(mut s2) = TcpStream::connect(format!("127.0.0.1:{}", client_port)).await {
            if s2.write_all(b"after-restart\n").await.is_ok() {
                let mut buf2 = [0u8; 14];
                if s2.read_exact(&mut buf2).await.is_ok() && &buf2 == b"after-restart\n" {
                    reconnected = true;
                    break;
                }
            }
        }
    }
    assert!(reconnected, "tunclient did not auto-reconnect after server restart");
    Ok(())
}
