use clap::Parser;
use log::{error, info, warn};
use rand::Rng;
use ssh_key::PrivateKey;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tunner::{
    auth::{client_authenticate, discover_default_keys, load_private_key},
    configure_tunnel_socket,
    metrics::{spawn_tsdb_reporter, Metrics, TsdbConfig},
    mux::{run_stream, run_tunnel, BlockHandler, BoxedIo, Keepalive, Session, STREAM_READ_BUF},
    parse_rate,
    tls::{build_client_connector, make_server_name},
    ws::ws_client_connect,
    Block, NoiseSource, CODE_ECHO, CODE_NOISE, CODE_START,
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

    #[arg(long = "ws-path", help = "Connect over WebSocket on this HTTP path (e.g. /ws) for CDN/Cloudflare compatibility")]
    ws_path: Option<String>,

    #[arg(long = "keepalive", default_value_t = 20, help = "Keepalive ping interval in seconds (0 to disable)")]
    keepalive: u64,

    #[arg(long = "no-reconnect", default_value_t = false, help = "Exit instead of auto-reconnecting when tunnel drops")]
    no_reconnect: bool,

    #[arg(long = "tsdb-url", help = "Optional TSDB base or push URL (e.g. https://tsdb.zoomphant.com)")]
    tsdb_url: Option<String>,

    #[arg(long = "tsdb-token", help = "Optional TSDB bearer token (or set TUNNER_TSDB_TOKEN / ~/.tunner/tsdb.conf)")]
    tsdb_token: Option<String>,

    #[arg(long = "tsdb-interval", help = "TSDB push interval in seconds (default: 15)")]
    tsdb_interval: Option<u64>,
}

static CONN_COUNTER: AtomicU32 = AtomicU32::new(1);

struct ClientHandler;
impl BlockHandler for ClientHandler {}

async fn establish_tunnel(
    args: &Args,
    use_tls: bool,
    candidate_keys: &[(PathBuf, PrivateKey)],
) -> Result<BoxedIo, Box<dyn std::error::Error + Send + Sync>> {
    let server_addr = format!("{}:{}", args.server, args.remote_port);
    let tcp_stream = TcpStream::connect(&server_addr).await?;
    configure_tunnel_socket(&tcp_stream);
    info!("Connected TCP to tunnel server at {}", server_addr);

    if use_tls {
        let connector = build_client_connector(
            args.tls_ca.as_deref(),
            args.tls_fingerprint.as_deref(),
            args.tls_insecure,
        )
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })?;
        let server_name = make_server_name(&args.server);
        let mut tls_stream = connector.connect(server_name, tcp_stream).await?;
        info!("TLS 1.3 handshake completed with {}", server_addr);

        if let Some(ref path) = args.ws_path {
            let ws_stream =
                ws_client_connect(tls_stream, &args.server, path, candidate_keys).await?;
            Ok(Box::new(ws_stream))
        } else {
            match client_authenticate(&mut tls_stream, candidate_keys).await {
                Ok(Some(matched)) => {
                    info!(
                        "SSH key authentication succeeded using {}",
                        matched.display()
                    );
                }
                Ok(None) => {
                    info!("Server does not require SSH key authentication.");
                }
                Err(e) => {
                    return Err(format!("SSH key authentication failed: {}", e).into());
                }
            }
            Ok(Box::new(tls_stream))
        }
    } else if let Some(ref path) = args.ws_path {
        let ws_stream = ws_client_connect(tcp_stream, &args.server, path, candidate_keys).await?;
        Ok(Box::new(ws_stream))
    } else {
        Ok(Box::new(tcp_stream))
    }
}

fn spawn_noise_task(
    min_rate: u64,
    echo: bool,
    noise_source: Arc<NoiseSource>,
    session: Session,
    real_bytes: Arc<AtomicU64>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let base_tick_ms: u64 = if min_rate < 1000 { 500 } else { 100 };
        let target_bytes = ((min_rate * base_tick_ms) / 1000).max(1);
        let code = if echo { CODE_ECHO } else { CODE_NOISE };

        info!(
            "Noise generator active: target ~{} bytes every ~{} ms (with jitter)",
            target_bytes, base_tick_ms
        );

        loop {
            // Apply ±25% random timing jitter to avoid a fixed metronome signature
            let jittered_ms = {
                let mut rng = rand::thread_rng();
                let low = (base_tick_ms * 3) / 4;
                let high = (base_tick_ms * 5) / 4;
                rng.gen_range(low..=high.max(low + 1))
            };

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(jittered_ms)) => {}
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
            }

            let real_sent = real_bytes.swap(0, Ordering::Relaxed);
            // Skip noise injection if real traffic already met the rate or the data queue is busy
            if real_sent >= target_bytes || session.data.capacity() < 32 {
                continue;
            }

            let mut deficit = (target_bytes - real_sent) as usize;
            while deficit > 0 {
                let max_chunk = deficit.min(STREAM_READ_BUF);
                let chunk_size = if max_chunk > 512 {
                    rand::thread_rng().gen_range(512..=max_chunk)
                } else {
                    max_chunk
                };
                let dummy_bytes = noise_source.get_bytes(chunk_size);
                let noise_block = Block::new_control_with_data(0, code, dummy_bytes);
                if session.data.try_send(noise_block).is_err() {
                    break;
                }
                deficit -= chunk_size;
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Arc::new(Args::parse());
    let min_rate = parse_rate(&args.min_rate);

    let use_tls = args.tls
        || (!args.key.is_empty() && args.ws_path.is_none())
        || args.tls_ca.is_some()
        || args.tls_fingerprint.is_some()
        || args.tls_insecure;

    let want_keys = use_tls || args.ws_path.is_some() || !args.key.is_empty();
    let candidate_keys: Arc<Vec<(PathBuf, PrivateKey)>> = Arc::new(if !args.key.is_empty() {
        let mut keys = Vec::new();
        for path in &args.key {
            let k = load_private_key(Path::new(path), args.passphrase.as_deref())?;
            info!("Loaded SSH private key from {}", path);
            keys.push((PathBuf::from(path), k));
        }
        keys
    } else if want_keys {
        let keys = discover_default_keys(args.passphrase.as_deref());
        if !keys.is_empty() {
            info!(
                "Auto-discovered {} SSH private key(s) from ~/.ssh/",
                keys.len()
            );
        }
        keys
    } else {
        Vec::new()
    });

    info!("Starting Tunner Client...");
    info!("Local listen address: {}:{}", args.interface, args.listen);
    info!(
        "Remote tunnel server: {}:{} (TLS={}, WS={:?}, candidate-keys={})",
        args.server,
        args.remote_port,
        use_tls,
        args.ws_path,
        candidate_keys.len()
    );
    info!(
        "Min rate floor: {} bytes/s, echo requested: {}, keepalive: {}s",
        min_rate, args.echo, args.keepalive
    );

    let metrics = Metrics::new();
    let transport_mode: &'static str = match (use_tls, args.ws_path.is_some()) {
        (true, true) => "ws-tls",
        (true, false) => "tls",
        (false, true) => "ws",
        (false, false) => "tcp",
    };

    // 1. Establish initial tunnel connection before binding local port so auth/config errors fail fast
    let hs_start = Instant::now();
    let initial_stream = establish_tunnel(&args, use_tls, &candidate_keys)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    metrics
        .handshake_last_ms
        .store(hs_start.elapsed().as_millis() as u64, Ordering::Relaxed);
    metrics.log_event(
        "info",
        format!("connected to {}:{} ({})", args.server, args.remote_port, transport_mode),
    );

    // 2. Bind local listener once; it stays alive across tunnel reconnections
    let local_addr = format!("{}:{}", args.interface, args.listen);
    let listener = TcpListener::bind(&local_addr).await?;
    info!("Tunnel client listening on {}", local_addr);

    let noise_source = if min_rate > 0 {
        Some(Arc::new(NoiseSource::new(args.noise_file.as_deref())))
    } else {
        None
    };

    let active_session: Arc<Mutex<Option<Session>>> = Arc::new(Mutex::new(None));
    let (fatal_tx, mut fatal_rx) = watch::channel(false);

    if let Some(tsdb_cfg) = TsdbConfig::resolve(
        args.tsdb_url.as_deref(),
        args.tsdb_token.as_deref(),
        args.tsdb_interval,
    ) {
        spawn_tsdb_reporter(
            tsdb_cfg,
            Arc::clone(&metrics),
            "client",
            transport_mode,
            fatal_rx.clone(),
        );
    }

    // 3. Spawn tunnel supervisor (manages current tunnel session + auto-reconnect)
    let sup_args = Arc::clone(&args);
    let sup_keys = Arc::clone(&candidate_keys);
    let sup_session = Arc::clone(&active_session);
    let sup_metrics = Arc::clone(&metrics);
    let keepalive = Keepalive::from_secs(args.keepalive);

    tokio::spawn(async move {
        let mut current_stream = Some(initial_stream);
        let mut backoff_secs = 1u64;

        loop {
            let stream = match current_stream.take() {
                Some(s) => {
                    backoff_secs = 1;
                    s
                }
                None => {
                    let hs_t0 = Instant::now();
                    match establish_tunnel(&sup_args, use_tls, &sup_keys).await {
                        Ok(s) => {
                            info!(
                                "Reconnected tunnel to {}:{}",
                                sup_args.server, sup_args.remote_port
                            );
                            sup_metrics
                                .handshake_last_ms
                                .store(hs_t0.elapsed().as_millis() as u64, Ordering::Relaxed);
                            sup_metrics
                                .tunnel_reconnects_total
                                .fetch_add(1, Ordering::Relaxed);
                            sup_metrics.log_event(
                                "info",
                                format!("reconnected to {}:{}", sup_args.server, sup_args.remote_port),
                            );
                            backoff_secs = 1;
                            s
                        }
                        Err(e) => {
                            let jitter_ms = rand::thread_rng().gen_range(0..500);
                            warn!(
                                "Reconnect failed ({}); retrying in ~{}s...",
                                e, backoff_secs
                            );
                            sup_metrics.log_event("warn", format!("reconnect failed: {}", e));
                            tokio::time::sleep(
                                Duration::from_secs(backoff_secs) + Duration::from_millis(jitter_ms),
                            )
                            .await;
                            backoff_secs = (backoff_secs * 2).min(30);
                            continue;
                        }
                    }
                }
            };

            let (session, io) = Session::new_with_metrics(Arc::clone(&sup_metrics));
            *sup_session.lock().unwrap() = Some(session.clone());

            let (noise_stop_tx, noise_stop_rx) = watch::channel(false);
            let real_bytes = Arc::new(AtomicU64::new(0));
            if let Some(ref ns) = noise_source {
                spawn_noise_task(
                    min_rate,
                    sup_args.echo,
                    Arc::clone(ns),
                    session.clone(),
                    Arc::clone(&real_bytes),
                    noise_stop_rx,
                );
            }

            let reason = run_tunnel(
                stream,
                session,
                io,
                ClientHandler,
                keepalive,
                Some(real_bytes),
            )
            .await;

            let _ = noise_stop_tx.send(true);
            *sup_session.lock().unwrap() = None;
            sup_metrics.log_event("warn", format!("tunnel disconnected: {}", reason));

            if sup_args.no_reconnect {
                error!("Tunnel disconnected ({}); shutting down.", reason);
                let _ = fatal_tx.send(true);
                return;
            }

            warn!("Tunnel disconnected ({}); reconnecting...", reason);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    // 4. Accept local connections and multiplex onto the current active session
    loop {
        let (socket, peer_addr) = tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("Failed to accept local connection: {}", e);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                }
            }
            _ = fatal_rx.changed() => {
                if *fatal_rx.borrow() {
                    return Err("Tunnel connection closed".into());
                }
                continue;
            }
        };

        let _ = socket.set_nodelay(true);
        let maybe_session = active_session.lock().unwrap().clone();
        let Some(session) = maybe_session.filter(|s| !s.is_closed()) else {
            warn!(
                "Rejecting connection from {} while tunnel is reconnecting",
                peer_addr
            );
            continue;
        };

        let conn_id = CONN_COUNTER.fetch_add(1, Ordering::Relaxed);
        info!("Accepted connection {:08x} from {}", conn_id, peer_addr);

        let rx = session.register(conn_id);
        tokio::spawn(async move {
            if session
                .data
                .send(Block::new_control(conn_id, CODE_START))
                .await
                .is_err()
            {
                session.unregister(conn_id);
                return;
            }
            run_stream(conn_id, socket, rx, session).await;
        });
    }
}
