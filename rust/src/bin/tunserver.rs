use clap::Parser;
use log::{error, info, warn};
use ssh_key::PublicKey;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tunner::{
    auth::{load_authorized_keys, server_authenticate, server_send_no_auth},
    configure_tunnel_socket,
    metrics::{spawn_tsdb_reporter, Metrics, MonitoredTcpStream, TsdbConfig},
    mux::{run_stream, run_tunnel, BlockHandler, Keepalive, Session},
    tls::build_server_acceptor,
    ws::{normalize_ws_path, ws_server_accept},
    Block, CODE_ABORT, CODE_ECHO,
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

    #[arg(long = "ws-route", help = "WebSocket route mapping: <PATH>[=<HOST:PORT|PORT>] (repeatable, e.g. /kr=6666 --ws-route /ip=127.0.0.1:1080)")]
    ws_route: Vec<String>,

    #[arg(long = "keepalive", default_value_t = 20, help = "Keepalive ping interval in seconds (0 to disable)")]
    keepalive: u64,

    #[arg(long = "tsdb-url", help = "Optional TSDB base or push URL (e.g. https://tsdb.zoomphant.com)")]
    tsdb_url: Option<String>,

    #[arg(long = "tsdb-token", help = "Optional TSDB bearer token (or set TUNNER_TSDB_TOKEN / ~/.tunner/tsdb.conf)")]
    tsdb_token: Option<String>,

    #[arg(long = "tsdb-interval", help = "TSDB push interval in seconds (default: 15)")]
    tsdb_interval: Option<u64>,
}

struct ServerHandler {
    backend_target: Arc<String>,
}

fn parse_ws_routes(
    raw: &[String],
    default_host: &str,
    default_port: u16,
) -> Result<Option<Arc<HashMap<String, String>>>, Box<dyn std::error::Error>> {
    if raw.is_empty() {
        return Ok(None);
    }
    let mut map = HashMap::new();
    for entry in raw {
        let (path_part, target) = match entry.split_once('=') {
            Some((p, t)) => {
                let t = t.trim();
                if t.is_empty() {
                    return Err(format!("Empty target in --ws-route '{}'", entry).into());
                }
                let addr = if t.parse::<u16>().is_ok() {
                    format!("{}:{}", default_host, t)
                } else {
                    t.to_string()
                };
                (p, addr)
            }
            None => (entry.as_str(), format!("{}:{}", default_host, default_port)),
        };
        let norm_path = normalize_ws_path(path_part);
        info!("WebSocket route enabled: {} -> {}", norm_path, target);
        map.insert(norm_path, target);
    }
    Ok(Some(Arc::new(map)))
}

impl BlockHandler for ServerHandler {
    fn on_start(&mut self, conn_id: u32, session: &Session) {
        // Register synchronously before spawning so any data blocks that arrive
        // immediately after CODE_START are buffered while backend connect runs.
        let rx = session.register(conn_id);
        let session = session.clone();
        let target = Arc::clone(&self.backend_target);

        tokio::spawn(async move {
            let connect_t0 = Instant::now();
            let connect_res =
                tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(target.as_str()))
                    .await;
            let elapsed = connect_t0.elapsed();

            match connect_res {
                Ok(Ok(backend_stream)) => {
                    session.metrics.record_backend_connect(elapsed, true);
                    let _ = backend_stream.set_nodelay(true);
                    info!("Connected backend for {:08x} to {}", conn_id, target);
                    run_stream(conn_id, backend_stream, rx, session).await;
                }
                Ok(Err(e)) => {
                    session.metrics.record_backend_connect(elapsed, false);
                    session
                        .metrics
                        .log_event("warn", format!("backend connect failed to {}: {}", target, e));
                    warn!(
                        "Failed connecting backend for {:08x} to {}: {}",
                        conn_id, target, e
                    );
                    session.unregister(conn_id);
                    let _ = session
                        .data
                        .send(Block::new_control(conn_id, CODE_ABORT))
                        .await;
                }
                Err(_) => {
                    session.metrics.record_backend_connect(elapsed, false);
                    session
                        .metrics
                        .log_event("warn", format!("backend connect timeout to {}", target));
                    warn!("Timed out connecting backend for {:08x} to {}", conn_id, target);
                    session.unregister(conn_id);
                    let _ = session
                        .data
                        .send(Block::new_control(conn_id, CODE_ABORT))
                        .await;
                }
            }
        });
    }

    fn on_echo(&mut self, block: Block, session: &Session) {
        let reply = if block.data.is_empty() {
            Block::new_control(0, CODE_ECHO)
        } else {
            Block::new_control_with_data(0, CODE_ECHO, block.data)
        };
        let _ = session.ctrl.send(reply);
    }
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
            info!(
                "Loaded {} authorized SSH public key(s) from {}",
                keys.len(),
                path
            );
            Some(Arc::new(keys))
        }
        None => None,
    };

    let use_tls =
        args.tls || args.tls_cert.is_some() || args.tls_key.is_some() || auth_keys.is_some();

    let tls_acceptor: Option<TlsAcceptor> = if use_tls {
        let (acceptor, fingerprint) =
            build_server_acceptor(args.tls_cert.as_deref(), args.tls_key.as_deref())?;
        info!(
            "TLS 1.3 enabled. Certificate SHA-256 fingerprint: {}",
            fingerprint
        );
        Some(acceptor)
    } else {
        None
    };

    let ws_routes = parse_ws_routes(&args.ws_route, &args.server, args.forward_port)?;

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

    let metrics = Metrics::new();
    let transport_mode: &'static str = match (use_tls, ws_routes.is_some()) {
        (true, true) => "ws-tls",
        (true, false) => "tls",
        (false, true) => "ws",
        (false, false) => "tcp",
    };
    let (_tsdb_stop_tx, tsdb_stop_rx) = watch::channel(false);
    if let Some(tsdb_cfg) = TsdbConfig::resolve(
        args.tsdb_url.as_deref(),
        args.tsdb_token.as_deref(),
        args.tsdb_interval,
    ) {
        spawn_tsdb_reporter(
            tsdb_cfg,
            Arc::clone(&metrics),
            "server",
            transport_mode,
            None,
            tsdb_stop_rx,
        );
    }

    let backend_target = Arc::new(format!("{}:{}", args.server, args.forward_port));
    let keepalive = Keepalive::from_secs(args.keepalive);

    loop {
        let (tunnel_socket, peer_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to accept tunnel connection: {}", e);
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        configure_tunnel_socket(&tunnel_socket);
        info!("Accepted new tunnel connection from {}", peer_addr);

        let target = Arc::clone(&backend_target);
        let acceptor = tls_acceptor.clone();
        let keys = auth_keys.clone();
        let routes = ws_routes.clone();
        let conn_metrics = Arc::clone(&metrics);

        tokio::spawn(async move {
            let tunnel_socket = MonitoredTcpStream::new(tunnel_socket, Arc::clone(&conn_metrics));
            if let Some(acceptor) = acceptor {
                let mut tls_stream = match acceptor.accept(tunnel_socket).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("TLS handshake failed from {}: {}", peer_addr, e);
                        return;
                    }
                };

                if let Some(ref routes_map) = routes {
                    match ws_server_accept(
                        tls_stream,
                        routes_map,
                        keys.as_deref().map(|v| v.as_slice()),
                    )
                    .await
                    {
                        Ok((ws_stream, route_target)) => {
                            serve_session(
                                ws_stream,
                                Arc::new(route_target),
                                keepalive,
                                peer_addr,
                                conn_metrics,
                            )
                            .await;
                        }
                        Err(e) => {
                            conn_metrics
                                .auth_failures_total
                                .fetch_add(1, Ordering::Relaxed);
                            conn_metrics
                                .log_event("warn", format!("ws auth failed from {}: {}", peer_addr, e));
                            warn!("WebSocket handshake failed from {}: {}", peer_addr, e);
                        }
                    }
                } else {
                    if let Some(ref allowed_keys) = keys {
                        match server_authenticate(&mut tls_stream, allowed_keys).await {
                            Ok(fp) => {
                                info!(
                                    "Authenticated tunnel client {} with SSH key {}",
                                    peer_addr, fp
                                );
                            }
                            Err(e) => {
                                conn_metrics
                                    .auth_failures_total
                                    .fetch_add(1, Ordering::Relaxed);
                                conn_metrics.log_event(
                                    "warn",
                                    format!("ssh auth failed from {}: {}", peer_addr, e),
                                );
                                warn!("SSH key auth failed from {}: {}", peer_addr, e);
                                return;
                            }
                        }
                    } else if let Err(e) = server_send_no_auth(&mut tls_stream).await {
                        warn!("Failed sending auth header to {}: {}", peer_addr, e);
                        return;
                    }

                    serve_session(tls_stream, target, keepalive, peer_addr, conn_metrics).await;
                }
            } else if let Some(ref routes_map) = routes {
                match ws_server_accept(
                    tunnel_socket,
                    routes_map,
                    keys.as_deref().map(|v| v.as_slice()),
                )
                .await
                {
                    Ok((ws_stream, route_target)) => {
                        serve_session(
                            ws_stream,
                            Arc::new(route_target),
                            keepalive,
                            peer_addr,
                            conn_metrics,
                        )
                        .await;
                    }
                    Err(e) => {
                        conn_metrics
                            .auth_failures_total
                            .fetch_add(1, Ordering::Relaxed);
                        conn_metrics
                            .log_event("warn", format!("ws auth failed from {}: {}", peer_addr, e));
                        warn!("WebSocket handshake failed from {}: {}", peer_addr, e);
                    }
                }
            } else {
                serve_session(tunnel_socket, target, keepalive, peer_addr, conn_metrics).await;
            }
        });
    }
}

async fn serve_session<S>(
    stream: S,
    backend_target: Arc<String>,
    keepalive: Keepalive,
    peer_addr: std::net::SocketAddr,
    metrics: Arc<Metrics>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    metrics.log_event("info", format!("session started from {}", peer_addr));
    let (session, io) = Session::new_with_metrics(Arc::clone(&metrics));
    let handler = ServerHandler { backend_target };
    let reason = run_tunnel(stream, session, io, handler, keepalive, None).await;
    metrics.log_event("info", format!("session ended ({}): {}", peer_addr, reason));
    info!("Tunnel session from {} ended: {}", peer_addr, reason);
}
