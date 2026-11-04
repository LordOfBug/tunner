use bytes::Bytes;
use clap::Parser;
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio::time::interval;
use tunner::{
    codec::{read_block, write_block},
    next_sequence, parse_rate, Block, BlockType, NoiseSource, CODE_ABORT, CODE_ECHO, CODE_NOISE,
    CODE_START, MAX_NO_PADDING_LENGTH,
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
}

static CONN_COUNTER: AtomicU32 = AtomicU32::new(1);

type ConnTable = Arc<Mutex<HashMap<u32, mpsc::Sender<Block>>>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let min_rate = parse_rate(&args.min_rate);

    info!("Starting Tunner Client...");
    info!("Local listen address: {}:{}", args.interface, args.listen);
    info!(
        "Remote tunnel server: {}:{}",
        args.server, args.remote_port
    );
    info!(
        "Min rate floor: {} bytes/s, echo requested: {}",
        min_rate, args.echo
    );

    // 1. Connect to remote tunserver
    let server_addr = format!("{}:{}", args.server, args.remote_port);
    let tunnel_stream = match TcpStream::connect(&server_addr).await {
        Ok(s) => {
            s.set_nodelay(true)?;
            info!("Connected to tunnel server at {}", server_addr);
            s
        }
        Err(e) => {
            error!("Failed to connect to tunnel server at {}: {}", server_addr, e);
            return Err(e.into());
        }
    };

    let (mut tunnel_read, mut tunnel_write) = tunnel_stream.into_split();

    // Channel for multiplexing blocks onto the tunnel
    let (tunnel_tx, mut tunnel_rx) = mpsc::channel::<Block>(1024);
    let real_bytes_sent_in_tick = Arc::new(AtomicU64::new(0));

    // 2. Spawn Tunnel Writer task
    let real_bytes_writer = Arc::clone(&real_bytes_sent_in_tick);
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
        info!("Tunnel writer terminated.");
    });

    // 3. Spawn Noise Generator task (if min_rate > 0)
    if min_rate > 0 {
        let noise_source = Arc::new(NoiseSource::new(args.noise_file.as_deref()));
        let noise_tx = tunnel_tx.clone();
        let noise_real_bytes = Arc::clone(&real_bytes_sent_in_tick);
        let echo = args.echo;

        tokio::spawn(async move {
            let tick_ms = if min_rate < 1000 { 500 } else { 100 };
            let target_bytes = ((min_rate * tick_ms) / 1000).max(1);
            let mut timer = interval(Duration::from_millis(tick_ms));

            info!(
                "Noise generator active: target ~{} bytes every {} ms",
                target_bytes, tick_ms
            );

            loop {
                timer.tick().await;
                let real_sent = noise_real_bytes.swap(0, Ordering::Relaxed);
                if real_sent < target_bytes {
                    let mut deficit = (target_bytes - real_sent) as usize;
                    let code = if echo { CODE_ECHO } else { CODE_NOISE };

                    while deficit > 0 {
                        let chunk_size = deficit.min(MAX_NO_PADDING_LENGTH);
                        let dummy_bytes = noise_source.get_bytes(chunk_size);
                        let noise_block =
                            Block::new_control_with_data(0, code, dummy_bytes);
                        if noise_tx.send(noise_block).await.is_err() {
                            break;
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
                                    // Local connection receiver dropped
                                    let mut map = conns_reader.lock().await;
                                    map.remove(&conn_id);
                                }
                            } else {
                                warn!("Received data for non-existing conn {:08x}", conn_id);
                                let _ = tunnel_tx_reader
                                    .send(Block::new_control(conn_id, CODE_ABORT))
                                    .await;
                            }
                        }
                        BlockType::Control { code } => match code {
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
    });

    // 6. Listen for local app connections
    let local_addr = format!("{}:{}", args.interface, args.listen);
    let listener = TcpListener::bind(&local_addr).await?;
    info!("Tunnel client listening on {}", local_addr);

    loop {
        let (socket, peer_addr) = listener.accept().await?;
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

            // Notify server of new connection
            let _ = tunnel_tx_conn
                .send(Block::new_control(conn_id, CODE_START))
                .await;

            // Spawn socket response writer task
            let conn_writer = tokio::spawn(async move {
                let mut expected_seq: u16 = 0;
                while let Some(block) = resp_rx.recv().await {
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
                let _ = socket_write.shutdown().await;
            });

            // Read from local socket and send into tunnel
            let mut buf = vec![0u8; MAX_NO_PADDING_LENGTH];
            let mut req_seq: u16 = 0;
            loop {
                match socket_read.read(&mut buf).await {
                    Ok(0) => {
                        info!("Local socket {:08x} EOF, closing.", conn_id);
                        break;
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

            // Cleanup
            let _ = tunnel_tx_conn
                .send(Block::new_control(conn_id, CODE_ABORT))
                .await;
            {
                let mut map = conns_conn.lock().await;
                map.remove(&conn_id);
            }
            conn_writer.abort();
            info!("Connection {:08x} terminated.", conn_id);
        });
    }
}

