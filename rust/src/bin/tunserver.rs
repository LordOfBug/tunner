use bytes::Bytes;
use clap::Parser;
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tunner::{
    codec::{read_block, write_block},
    next_sequence, Block, BlockType, CODE_ABORT, CODE_ECHO, CODE_NOISE, CODE_START,
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    info!("Starting Tunner Server...");
    info!("Tunnel listening port: {}:{}", args.interface, args.listen);
    info!("Backend proxy target: {}:{}", args.server, args.forward_port);
    info!("Operating mode: {}", args.mode);

    // If test mode is enabled, run an in-process TCP Echo Server on the forward port
    if args.mode.eq_ignore_ascii_case("test") {
        let echo_addr = format!("127.0.0.1:{}", args.forward_port);
        let echo_listener = TcpListener::bind(&echo_addr).await?;
        info!("Started built-in EchoServer on {}", echo_addr);

        tokio::spawn(async move {
            loop {
                if let Ok((mut socket, peer)) = echo_listener.accept().await {
                    info!("EchoServer accepted connection from {}", peer);
                    tokio::spawn(async move {
                        let (mut r, mut w) = socket.split();
                        let _ = tokio::io::copy(&mut r, &mut w).await;
                        info!("EchoServer client {} disconnected", peer);
                    });
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
        let (tunnel_socket, peer_addr) = listener.accept().await?;
        tunnel_socket.set_nodelay(true)?;
        info!("Accepted new tunnel connection from {}", peer_addr);

        let target_host = backend_host.clone();
        tokio::spawn(async move {
            handle_tunnel(tunnel_socket, target_host, backend_port).await;
        });
    }
}

async fn handle_tunnel(tunnel_socket: TcpStream, backend_host: String, backend_port: u16) {
    let (mut tunnel_read, mut tunnel_write) = tunnel_socket.into_split();
    let (tunnel_tx, mut tunnel_rx) = mpsc::channel::<Block>(1024);

    // Spawn Tunnel Writer task
    tokio::spawn(async move {
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

                            match TcpStream::connect(&target).await {
                                Ok(backend_stream) => {
                                    backend_stream.set_nodelay(true).ok();
                                    info!(
                                        "Connected backend for {:08x} to {}",
                                        conn_id, target
                                    );

                                    let (backend_read, mut backend_write) =
                                        backend_stream.into_split();
                                    let (tx, mut rx) = mpsc::channel::<Block>(128);

                                    {
                                        let mut map = connections.lock().await;
                                        map.insert(conn_id, tx);
                                    }

                                    // Task 1: write incoming tunnel data to backend
                                    let conns_clone1 = Arc::clone(&connections);
                                    let tunnel_tx_clone1 = tunnel_tx.clone();
                                    tokio::spawn(async move {
                                        while let Some(blk) = rx.recv().await {
                                            if let Err(e) =
                                                backend_write.write_all(&blk.data).await
                                            {
                                                error!(
                                                    "Failed writing to backend {:08x}: {}",
                                                    conn_id, e
                                                );
                                                break;
                                            }
                                        }
                                        let _ = backend_write.shutdown().await;
                                        let mut map = conns_clone1.lock().await;
                                        map.remove(&conn_id);
                                        let _ = tunnel_tx_clone1
                                            .send(Block::new_control(conn_id, CODE_ABORT))
                                            .await;
                                    });

                                    // Task 2: read backend responses and send to tunnel
                                    let tunnel_tx_clone2 = tunnel_tx.clone();
                                    let conns_clone2 = Arc::clone(&connections);
                                    tokio::spawn(async move {
                                        let mut r = backend_read;
                                        let mut buf = vec![0u8; MAX_NO_PADDING_LENGTH];
                                        let mut res_seq: u16 = 0;

                                        loop {
                                            match r.read(&mut buf).await {
                                                Ok(0) => break, // EOF
                                                Ok(n) => {
                                                    let data =
                                                        Bytes::copy_from_slice(&buf[..n]);
                                                    let blk = Block::new_data(
                                                        conn_id, res_seq, data,
                                                    );
                                                    res_seq = next_sequence(res_seq);
                                                    if tunnel_tx_clone2.send(blk).await.is_err()
                                                    {
                                                        break;
                                                    }
                                                }
                                                Err(_) => break,
                                            }
                                        }

                                        let mut map = conns_clone2.lock().await;
                                        map.remove(&conn_id);
                                        let _ = tunnel_tx_clone2
                                            .send(Block::new_control(conn_id, CODE_ABORT))
                                            .await;
                                    });
                                }
                                Err(e) => {
                                    error!(
                                        "Failed connecting backend for {:08x} to {}: {}",
                                        conn_id, target, e
                                    );
                                    let _ = tunnel_tx
                                        .send(Block::new_control(conn_id, CODE_ABORT))
                                        .await;
                                }
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
                            warn!("Received data for unknown conn {:08x}", conn_id);
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
}

