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
