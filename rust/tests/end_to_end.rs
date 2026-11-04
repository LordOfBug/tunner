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
