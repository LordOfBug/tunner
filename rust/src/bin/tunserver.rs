use bytes::Bytes;
use clap::Parser;
use log::{debug, error, info, warn};
use ssh_key::PublicKey;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio_rustls::TlsAcceptor;
use tunner::{
    auth::{load_authorized_keys, server_authenticate, server_send_no_auth},
    codec::{read_block, write_block},
    next_sequence,
    tls::build_server_acceptor,
    Block, BlockType, CODE_ABORT, CODE_ECHO, CODE_EOF, CODE_NOISE, CODE_START,
    MAX_NO_PADDING_LENGTH,
};

#[derive(Parser, Debug)]
#[command(name = "tunserver", about = "Tunner server daemon")]
struct Args {
    #[arg(short = 'i', long, default_value = "0.0.0.0")]
    interface: String,

    #[arg(short = 'l', long, default_value_t = 8080)]
    listen: u16,

    #[arg(short = 's', long, default_value = "127.0.0.1")]
    server: String,

    #[arg(short = 'f', long = "forward-port", default_value_t = 8888)]
    forward_port: u16,

    #[arg(short = 'm', long, default_value = "prod")]
    mode: String,

    #[arg(long, default_value_t = false, help = "Enable TLS 1.3 transport")]
    tls: bool,

    #[arg(long = "tls-cert", help = "Path to PEM certificate file (optional; auto-generates self-signed if omitted)")]
    tls_cert: Option<String>,

    #[arg(long = "tls-key", help = "Path to PEM private key file")]
    tls_key: Option<String>,

    #[arg(short = 'a', long = "authorized-keys", help = "Path to OpenSSH authorized_keys file for client authentication")]
    authorized_keys: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    info!("Starting Tunner Server...");
    info!("Tunnel listening port: {}:{}", args.interface, args.listen);
    info!("Backend proxy target: {}:{}", args.server, args.forward_port);
    info!("Operating mode: {}", args.mode);

    let auth_keys: Option<Arc<Vec<PublicKey>>> = match args.authorized_keys.as_deref() {
        Some(path) => {
            let keys = load_authorized_keys(Path::new(path))?;
            info!("Loaded {} authorized SSH public key(s) from {}", keys.len(), path);
            Some(Arc::new(keys))
        }
        None => None,
    };

    let use_tls =
        args.tls || args.tls_cert.is_some() || args.tls_key.is_some() || auth_keys.is_some();

    let tls_acceptor: Option<TlsAcceptor> = if use_tls {
        let (acceptor, fingerprint) =
            build_server_acceptor(args.tls_cert.as_deref(), args.tls_key.as_deref())?;
        info!("TLS 1.3 enabled. Certificate SHA-256 fingerprint: {}", fingerprint);
        Some(acceptor)
    } else {
        None
    };

    // If test mode is enabled, run an in-process TCP Echo Server on the forward port
    if args.mode.eq_ignore_ascii_case("test") {
        let echo_addr = format!("127.0.0.1:{}", args.forward_port);
        let echo_listener = TcpListener::bind(&echo_addr).await?;
        info!("Started built-in EchoServer on {}", echo_addr);

        tokio::spawn(async move {
            loop {
                match echo_listener.accept().await {
                    Ok((mut socket, peer)) => {
                        info!("EchoServer accepted connection from {}", peer);
                        tokio::spawn(async move {
                            let (mut r, mut w) = socket.split();
                            let _ = tokio::io::copy(&mut r, &mut w).await;
                            info!("EchoServer client {} disconnected", peer);
                        });
                    }
                    Err(e) => {
                        error!("EchoServer accept error: {}", e);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });
    }

    let listen_addr = format!("{}:{}", args.interface, args.listen);
    let listener = TcpListener::bind(&listen_addr).await?;
    info!("Tunner server listening for tunnel clients on {}", listen_addr);

    let backend_host = args.server;
    let backend_port = args.forward_port;

    loop {
        let (tunnel_socket, peer_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to accept tunnel connection: {}", e);
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        tunnel_socket.set_nodelay(true).ok();
        info!("Accepted new tunnel connection from {}", peer_addr);

        let target_host = backend_host.clone();
        let acceptor = tls_acceptor.clone();
        let keys = auth_keys.clone();

        tokio::spawn(async move {
            if let Some(acceptor) = acceptor {
                let mut tls_stream = match acceptor.accept(tunnel_socket).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("TLS handshake failed from {}: {}", peer_addr, e);
                        return;
                    }
                };

                if let Some(ref allowed_keys) = keys {
                    match server_authenticate(&mut tls_stream, allowed_keys).await {
                        Ok(fp) => {
                            info!("Authenticated tunnel client {} with SSH key {}", peer_addr, fp);
                        }
                        Err(e) => {
                            warn!("SSH key auth failed from {}: {}", peer_addr, e);
                            return;
                        }
                    }
                } else if let Err(e) = server_send_no_auth(&mut tls_stream).await {
                    warn!("Failed sending auth header to {}: {}", peer_addr, e);
                    return;
                }

                handle_tunnel(tls_stream, target_host, backend_port).await;
            } else {
                handle_tunnel(tunnel_socket, target_host, backend_port).await;
            }
        });
    }
}

async fn handle_tunnel<S>(tunnel_stream: S, backend_host: String, backend_port: u16)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut tunnel_read, mut tunnel_write) = tokio::io::split(tunnel_stream);
    let (tunnel_tx, mut tunnel_rx) = mpsc::channel::<Block>(1024);

    // Spawn Tunnel Writer task
    let writer_handle = tokio::spawn(async move {
        while let Some(block) = tunnel_rx.recv().await {
            if let Err(e) = write_block(&mut tunnel_write, &block).await {
                error!("Failed writing to tunnel: {}", e);
                break;
            }
        }
    });

    let connections: Arc<Mutex<HashMap<u32, mpsc::Sender<Block>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    loop {
        match read_block(&mut tunnel_read).await {
            Ok(Some(block)) => {
                let conn_id = block.conn_id;
                match block.block_type() {
                    BlockType::Control { code } => match code {
                        CODE_START => {
                            info!("Starting new backend connection {:08x}...", conn_id);
                            let target = format!("{}:{}", backend_host, backend_port);

                            let connect_res = tokio::time::timeout(
                                Duration::from_secs(5),
                                TcpStream::connect(&target),
                            )
                            .await;

                            match connect_res {
                                Ok(Ok(backend_stream)) => {
                                    backend_stream.set_nodelay(true).ok();
                                    info!(
                                        "Connected backend for {:08x} to {}",
                                        conn_id, target
                                    );

                                    let (mut backend_read, mut backend_write) =
                                        backend_stream.into_split();
                                    let (tx, mut rx) = mpsc::channel::<Block>(128);

                                    {
                                        let mut map = connections.lock().await;
                                        map.insert(conn_id, tx);
                                    }

                                    let conns_clone = Arc::clone(&connections);
                                    let tunnel_tx_clone = tunnel_tx.clone();

                                    tokio::spawn(async move {
                                        let mut buf = vec![0u8; MAX_NO_PADDING_LENGTH];
                                        let mut res_seq: u16 = 0;
                                        let mut read_closed = false;
                                        let mut write_closed = false;
                                        let mut aborted_by_client = false;
                                        let half_close_timeout = Duration::from_secs(60);

                                        loop {
                                            tokio::select! {
                                                read_res = backend_read.read(&mut buf), if !read_closed => {
                                                    match read_res {
                                                        Ok(0) => {
                                                            // Backend EOF (half-close)
                                                            read_closed = true;
                                                            if tunnel_tx_clone.send(Block::new_control(conn_id, CODE_EOF)).await.is_err() {
                                                                break;
                                                            }
                                                            if write_closed {
                                                                break;
                                                            }
                                                        }
                                                        Ok(n) => {
                                                            let data = Bytes::copy_from_slice(&buf[..n]);
                                                            let blk = Block::new_data(conn_id, res_seq, data);
                                                            res_seq = next_sequence(res_seq);
                                                            if tunnel_tx_clone.send(blk).await.is_err() {
                                                                break;
                                                            }
                                                        }
                                                        Err(_) => break,
                                                    }
                                                }
                                                maybe_blk = rx.recv(), if !write_closed => {
                                                    match maybe_blk {
                                                        Some(blk) if blk.is_data() => {
                                                            if let Err(e) = backend_write.write_all(&blk.data).await {
                                                                error!(
                                                                    "Failed writing to backend {:08x}: {}",
                                                                    conn_id, e
                                                                );
                                                                break;
                                                            }
                                                        }
                                                        Some(blk) if blk.control_code() == CODE_EOF => {
                                                            let _ = backend_write.shutdown().await;
                                                            write_closed = true;
                                                            if read_closed {
                                                                break;
                                                            }
                                                        }
                                                        Some(_) => {}
                                                        None => {
                                                            // Client sent CODE_ABORT or tunnel closed
                                                            aborted_by_client = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                                _ = tokio::time::sleep(half_close_timeout), if read_closed || write_closed => {
                                                    warn!("Half-closed backend conn {:08x} timed out, closing.", conn_id);
                                                    break;
                                                }
                                            }
                                        }

                                        if !write_closed {
                                            let _ = backend_write.shutdown().await;
                                        }
                                        {
                                            let mut map = conns_clone.lock().await;
                                            map.remove(&conn_id);
                                        }
                                        if !aborted_by_client && !(read_closed && write_closed) {
                                            let _ = tunnel_tx_clone
                                                .send(Block::new_control(conn_id, CODE_ABORT))
                                                .await;
                                        }
                                    });
                                }
                                Ok(Err(e)) => {
                                    error!(
                                        "Failed connecting backend for {:08x} to {}: {}",
                                        conn_id, target, e
                                    );
                                    let _ = tunnel_tx
                                        .send(Block::new_control(conn_id, CODE_ABORT))
                                        .await;
                                }
                                Err(_) => {
                                    error!(
                                        "Timed out connecting backend for {:08x} to {}",
                                        conn_id, target
                                    );
                                    let _ = tunnel_tx
                                        .send(Block::new_control(conn_id, CODE_ABORT))
                                        .await;
                                }
                            }
                        }
                        CODE_EOF => {
                            let maybe_tx = {
                                let map = connections.lock().await;
                                map.get(&conn_id).cloned()
                            };
                            if let Some(tx) = maybe_tx {
                                let _ = tx.send(block).await;
                            }
                        }
                        CODE_ABORT => {
                            info!("Abort received for {:08x}", conn_id);
                            let mut map = connections.lock().await;
                            map.remove(&conn_id);
                        }
                        CODE_NOISE => {
                            // Dummy data from client: silently discard to save bandwidth
                        }
                        CODE_ECHO => {
                            // Client requested echo: reflect the payload back
                            if !block.data.is_empty() {
                                let _ = tunnel_tx
                                    .send(Block::new_control_with_data(
                                        0,
                                        CODE_ECHO,
                                        block.data,
                                    ))
                                    .await;
                            } else {
                                let _ = tunnel_tx
                                    .send(Block::new_control(0, CODE_ECHO))
                                    .await;
                            }
                        }
                        _ => {}
                    },
                    BlockType::Data { .. } => {
                        let maybe_tx = {
                            let map = connections.lock().await;
                            map.get(&conn_id).cloned()
                        };

                        if let Some(tx) = maybe_tx {
                            if tx.send(block).await.is_err() {
                                let mut map = connections.lock().await;
                                map.remove(&conn_id);
                            }
                        } else {
                            debug!("Received data for already-closed conn {:08x}", conn_id);
                            let _ = tunnel_tx
                                .send(Block::new_control(conn_id, CODE_ABORT))
                                .await;
                        }
                    }
                    BlockType::Unknown(tag) => {
                        warn!("Unknown block tag {:04x}", tag);
                    }
                }
            }
            Ok(None) => {
                info!("Tunnel client disconnected.");
                break;
            }
            Err(e) => {
                error!("Tunnel read error: {}", e);
                break;
            }
        }
    }

    connections.lock().await.clear();
    writer_handle.abort();
}
