use bytes::Bytes;
use clap::Parser;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::interval;
use tunner::{
    auth::{client_authenticate, discover_default_keys, load_private_key},
    codec::{read_block, write_block},
    next_sequence, parse_rate,
    tls::{build_client_connector, make_server_name},
    Block, BlockType, NoiseSource, CODE_ABORT, CODE_ECHO, CODE_EOF, CODE_NOISE, CODE_START,
    MAX_NO_PADDING_LENGTH,
};

#[derive(Parser, Debug)]
#[command(name = "tunclient", about = "Tunner client daemon")]
struct Args {
    #[arg(short = 'i', long, default_value = "127.0.0.1")]
    interface: String,

    #[arg(short = 'l', long, default_value_t = 8080)]
    listen: u16,

    #[arg(help = "Remote tunnel server address")]
    server: String,

    #[arg(short = 'r', long = "remote-port", default_value_t = 8080)]
    remote_port: u16,

    #[arg(short = 'n', long = "min-rate", default_value = "0")]
    min_rate: String,

    #[arg(long = "noise-file")]
    noise_file: Option<String>,

    #[arg(short = 'e', long, default_value_t = false)]
    echo: bool,

    #[arg(long, default_value_t = false, help = "Enable TLS 1.3 transport")]
    tls: bool,

    #[arg(short = 'k', long = "key", help = "Path(s) to SSH private key(s) for client auth (auto-discovers ~/.ssh/id_* if omitted)")]
    key: Vec<String>,

    #[arg(long = "passphrase", help = "Passphrase for encrypted SSH private key (or set TUNNER_KEY_PASSPHRASE)")]
    passphrase: Option<String>,

    #[arg(long = "tls-ca", help = "Path to CA/server PEM certificate to verify server")]
    tls_ca: Option<String>,

    #[arg(long = "tls-fingerprint", help = "Expected SHA-256 hex fingerprint of server TLS certificate")]
    tls_fingerprint: Option<String>,

    #[arg(long = "tls-insecure", default_value_t = false, help = "Skip TLS server certificate verification")]
    tls_insecure: bool,
}

static CONN_COUNTER: AtomicU32 = AtomicU32::new(1);

type ConnTable = Arc<Mutex<HashMap<u32, mpsc::Sender<Block>>>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let min_rate = parse_rate(&args.min_rate);

    let use_tls = args.tls
        || !args.key.is_empty()
        || args.tls_ca.is_some()
        || args.tls_fingerprint.is_some()
        || args.tls_insecure;

    let candidate_keys = if !args.key.is_empty() {
        let mut keys = Vec::new();
        for path in &args.key {
            let k = load_private_key(Path::new(path), args.passphrase.as_deref())?;
            info!("Loaded SSH private key from {}", path);
            keys.push((PathBuf::from(path), k));
        }
        keys
    } else if use_tls {
        let keys = discover_default_keys(args.passphrase.as_deref());
        if !keys.is_empty() {
            info!("Auto-discovered {} SSH private key(s) from ~/.ssh/", keys.len());
        }
        keys
    } else {
        Vec::new()
    };

    info!("Starting Tunner Client...");
    info!("Local listen address: {}:{}", args.interface, args.listen);
    info!(
        "Remote tunnel server: {}:{} (TLS={}, candidate-keys={})",
        args.server,
        args.remote_port,
        use_tls,
        candidate_keys.len()
    );
    info!(
        "Min rate floor: {} bytes/s, echo requested: {}",
        min_rate, args.echo
    );

    // 1. Connect to remote tunserver (with optional TLS 1.3 + SSH key auth)
    let server_addr = format!("{}:{}", args.server, args.remote_port);
    let tcp_stream = match TcpStream::connect(&server_addr).await {
        Ok(s) => {
            s.set_nodelay(true)?;
            info!("Connected TCP to tunnel server at {}", server_addr);
            s
        }
        Err(e) => {
            error!("Failed to connect to tunnel server at {}: {}", server_addr, e);
            return Err(e.into());
        }
    };

    let (mut tunnel_read, mut tunnel_write): (
        Box<dyn AsyncRead + Unpin + Send>,
        Box<dyn AsyncWrite + Unpin + Send>,
    ) = if use_tls {
        let connector = build_client_connector(
            args.tls_ca.as_deref(),
            args.tls_fingerprint.as_deref(),
            args.tls_insecure,
        )?;
        let server_name = make_server_name(&args.server);
        let mut tls_stream = connector.connect(server_name, tcp_stream).await?;
        info!("TLS 1.3 handshake completed with {}", server_addr);

        match client_authenticate(&mut tls_stream, &candidate_keys).await {
            Ok(Some(matched)) => {
                info!("SSH key authentication succeeded using {}", matched.display());
            }
            Ok(None) => {
                info!("Server does not require SSH key authentication.");
            }
            Err(e) => {
                return Err(format!("SSH key authentication failed: {}", e).into());
            }
        }

        let (r, w) = tokio::io::split(tls_stream);
        (Box::new(r), Box::new(w))
    } else {
        let (r, w) = tokio::io::split(tcp_stream);
        (Box::new(r), Box::new(w))
    };

    // Channel for multiplexing blocks onto the tunnel
    let (tunnel_tx, mut tunnel_rx) = mpsc::channel::<Block>(1024);
    let real_bytes_sent_in_tick = Arc::new(AtomicU64::new(0));
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // 2. Spawn Tunnel Writer task
    let real_bytes_writer = Arc::clone(&real_bytes_sent_in_tick);
    let shutdown_tx_writer = shutdown_tx.clone();
    tokio::spawn(async move {
        while let Some(block) = tunnel_rx.recv().await {
            if block.is_data() {
                real_bytes_writer.fetch_add(block.length() as u64, Ordering::Relaxed);
            }
            if let Err(e) = write_block(&mut tunnel_write, &block).await {
                error!("Error writing block to tunnel: {}", e);
                break;
            }
        }
        let _ = shutdown_tx_writer.send(true);
        info!("Tunnel writer terminated.");
    });

    // 3. Spawn Noise Generator task (if min_rate > 0)
    if min_rate > 0 {
        let noise_source = Arc::new(NoiseSource::new(args.noise_file.as_deref()));
        let noise_tx = tunnel_tx.clone();
        let noise_real_bytes = Arc::clone(&real_bytes_sent_in_tick);
        let echo = args.echo;
        let mut noise_shutdown = shutdown_rx.clone();

        tokio::spawn(async move {
            let tick_ms = if min_rate < 1000 { 500 } else { 100 };
            let target_bytes = ((min_rate * tick_ms) / 1000).max(1);
            let mut timer = interval(Duration::from_millis(tick_ms));

            info!(
                "Noise generator active: target ~{} bytes every {} ms",
                target_bytes, tick_ms
            );

            loop {
                tokio::select! {
                    _ = timer.tick() => {}
                    _ = noise_shutdown.changed() => {
                        if *noise_shutdown.borrow() {
                            break;
                        }
                    }
                }
                let real_sent = noise_real_bytes.swap(0, Ordering::Relaxed);
                if real_sent < target_bytes {
                    let mut deficit = (target_bytes - real_sent) as usize;
                    let code = if echo { CODE_ECHO } else { CODE_NOISE };

                    while deficit > 0 {
                        let chunk_size = deficit.min(MAX_NO_PADDING_LENGTH);
                        let dummy_bytes = noise_source.get_bytes(chunk_size);
                        let noise_block = Block::new_control_with_data(0, code, dummy_bytes);
                        if noise_tx.send(noise_block).await.is_err() {
                            return;
                        }
                        deficit -= chunk_size;
                    }
                }
            }
        });
    }

    // 4. Map for active client connections
    let conns: ConnTable = Arc::new(Mutex::new(HashMap::new()));

    // 5. Spawn Tunnel Reader task
    let conns_reader = Arc::clone(&conns);
    let tunnel_tx_reader = tunnel_tx.clone();
    let shutdown_tx_reader = shutdown_tx.clone();
    tokio::spawn(async move {
        loop {
            match read_block(&mut tunnel_read).await {
                Ok(Some(block)) => {
                    let conn_id = block.conn_id;
                    match block.block_type() {
                        BlockType::Data { .. } => {
                            let maybe_tx = {
                                let map = conns_reader.lock().await;
                                map.get(&conn_id).cloned()
                            };

                            if let Some(tx) = maybe_tx {
                                if tx.send(block).await.is_err() {
                                    let mut map = conns_reader.lock().await;
                                    map.remove(&conn_id);
                                }
                            } else {
                                debug!("Received data for already-closed conn {:08x}", conn_id);
                                let _ = tunnel_tx_reader
                                    .send(Block::new_control(conn_id, CODE_ABORT))
                                    .await;
                            }
                        }
                        BlockType::Control { code } => match code {
                            CODE_EOF => {
                                let maybe_tx = {
                                    let map = conns_reader.lock().await;
                                    map.get(&conn_id).cloned()
                                };
                                if let Some(tx) = maybe_tx {
                                    let _ = tx.send(block).await;
                                }
                            }
                            CODE_ABORT => {
                                info!("Server closed conn {:08x}", conn_id);
                                let mut map = conns_reader.lock().await;
                                map.remove(&conn_id);
                            }
                            CODE_ECHO | CODE_NOISE => {
                                // Dummy or echoed dummy packet: silently drop
                            }
                            _ => {}
                        },
                        BlockType::Unknown(tag) => {
                            warn!("Unknown block tag {:04x}", tag);
                        }
                    }
                }
                Ok(None) => {
                    info!("Tunnel closed by server.");
                    break;
                }
                Err(e) => {
                    error!("Error reading from tunnel: {}", e);
                    break;
                }
            }
        }
        conns_reader.lock().await.clear();
        let _ = shutdown_tx_reader.send(true);
    });

    // 6. Listen for local app connections
    let local_addr = format!("{}:{}", args.interface, args.listen);
    let listener = TcpListener::bind(&local_addr).await?;
    info!("Tunnel client listening on {}", local_addr);

    loop {
        let (socket, peer_addr) = tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    error!("Tunnel connection lost, shutting down client.");
                    return Err("Tunnel connection closed".into());
                }
                continue;
            }
        };

        let conn_id = CONN_COUNTER.fetch_add(1, Ordering::Relaxed);
        info!("Accepted connection {:08x} from {}", conn_id, peer_addr);

        let (resp_tx, mut resp_rx) = mpsc::channel::<Block>(128);
        {
            let mut map = conns.lock().await;
            map.insert(conn_id, resp_tx);
        }

        let tunnel_tx_conn = tunnel_tx.clone();
        let conns_conn = Arc::clone(&conns);

        tokio::spawn(async move {
            let (mut socket_read, mut socket_write) = socket.into_split();

            if tunnel_tx_conn
                .send(Block::new_control(conn_id, CODE_START))
                .await
                .is_err()
            {
                conns_conn.lock().await.remove(&conn_id);
                return;
            }

            let mut buf = vec![0u8; MAX_NO_PADDING_LENGTH];
            let mut req_seq: u16 = 0;
            let mut expected_seq: u16 = 0;
            let mut read_closed = false;
            let mut write_closed = false;
            let mut aborted_by_remote = false;
            let half_close_timeout = Duration::from_secs(60);

            loop {
                tokio::select! {
                    read_res = socket_read.read(&mut buf), if !read_closed => {
                        match read_res {
                            Ok(0) => {
                                info!("Local socket {:08x} read EOF (half-close).", conn_id);
                                read_closed = true;
                                if tunnel_tx_conn.send(Block::new_control(conn_id, CODE_EOF)).await.is_err() {
                                    break;
                                }
                                if write_closed {
                                    break;
                                }
                            }
                            Ok(n) => {
                                let data = Bytes::copy_from_slice(&buf[..n]);
                                let block = Block::new_data(conn_id, req_seq, data);
                                req_seq = next_sequence(req_seq);
                                if tunnel_tx_conn.send(block).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Error reading from local socket {:08x}: {}", conn_id, e);
                                break;
                            }
                        }
                    }
                    maybe_block = resp_rx.recv(), if !write_closed => {
                        match maybe_block {
                            Some(block) if block.is_data() => {
                                let seq = block.sequence();
                                if seq != expected_seq {
                                    error!(
                                        "Out-of-order sequence on conn {:08x}: got {}, expected {}",
                                        conn_id, seq, expected_seq
                                    );
                                }
                                expected_seq = next_sequence(seq);

                                if let Err(e) = socket_write.write_all(&block.data).await {
                                    error!("Failed writing to local socket {:08x}: {}", conn_id, e);
                                    break;
                                }
                            }
                            Some(block) if block.control_code() == CODE_EOF => {
                                info!("Remote {:08x} sent EOF (half-close).", conn_id);
                                let _ = socket_write.shutdown().await;
                                write_closed = true;
                                if read_closed {
                                    break;
                                }
                            }
                            Some(_) => {}
                            None => {
                                aborted_by_remote = true;
                                break;
                            }
                        }
                    }
                    _ = tokio::time::sleep(half_close_timeout), if read_closed || write_closed => {
                        warn!("Half-closed conn {:08x} timed out, closing.", conn_id);
                        break;
                    }
                }
            }

            if !write_closed {
                let _ = socket_write.shutdown().await;
            }
            {
                let mut map = conns_conn.lock().await;
                map.remove(&conn_id);
            }
            if !aborted_by_remote && !(read_closed && write_closed) {
                let _ = tunnel_tx_conn
                    .send(Block::new_control(conn_id, CODE_ABORT))
                    .await;
            }
            info!("Connection {:08x} terminated.", conn_id);
        });
    }
}
