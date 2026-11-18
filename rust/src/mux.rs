//! Shared tunnel multiplexing core used by both `tunclient` and `tunserver`.
//!
//! Design:
//! - One **writer task** per tunnel drains two queues: an unbounded, high-priority
//!   control queue (PAUSE/RESUME/ABORT/PING) and a bounded data queue. Queued blocks
//!   are coalesced into a single flush (one syscall / TLS record) per batch, and a
//!   PING is emitted whenever the writer has been idle for the keepalive interval.
//! - One **reader loop** per tunnel dispatches incoming blocks to per-connection
//!   unbounded queues. It never awaits on a single connection, so one slow
//!   connection can no longer stall the whole tunnel (no head-of-line blocking).
//! - **Backpressure (XOFF/XON)**: the reader tracks bytes queued per connection.
//!   Above `PAUSE_HIGH_WATERMARK` it sends `CODE_PAUSE`; the peer stops reading that
//!   connection's local socket (TCP pushes back on the app). When the queue drains
//!   below `RESUME_LOW_WATERMARK` the connection task sends `CODE_RESUME`.
//! - **Liveness**: if nothing is received within the keepalive timeout the tunnel is
//!   declared dead and torn down.

use crate::block::*;
use crate::codec::{read_block, write_block_noflush};
use bytes::Bytes;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// Send PAUSE once this many bytes are queued for one connection.
pub const PAUSE_HIGH_WATERMARK: usize = 1024 * 1024;
/// Send RESUME once the queue drains to this many bytes.
pub const RESUME_LOW_WATERMARK: usize = 256 * 1024;
/// Max payload read from a local socket per data block (smaller blocks interleave better).
pub const STREAM_READ_BUF: usize = 16 * 1024;
/// Bounded shared data queue towards the tunnel writer (in blocks).
const DATA_QUEUE_BLOCKS: usize = 256;
/// Max bytes coalesced into a single flush.
const MAX_BATCH_BYTES: usize = 256 * 1024;
/// Tunnel socket buffer sizes.
const IO_BUF: usize = 64 * 1024;
/// Idle timeout for a half-closed connection.
const HALF_CLOSE_TIMEOUT: Duration = Duration::from_secs(60);

/// Any bidirectional async byte stream usable as a tunnel transport.
pub trait TunnelIo: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> TunnelIo for T {}
pub type BoxedIo = Box<dyn TunnelIo>;

#[derive(Debug)]
pub enum StreamEvent {
    Data(Bytes),
    Eof,
    Pause,
    Resume,
    Abort,
}

struct FlowState {
    queued: AtomicUsize,
    pause_sent: AtomicBool,
}

#[derive(Clone)]
struct StreamHandle {
    tx: mpsc::UnboundedSender<StreamEvent>,
    flow: Arc<FlowState>,
}

/// Receiving side of a connection's event queue, owned by its connection task.
pub struct StreamReceiver {
    rx: mpsc::UnboundedReceiver<StreamEvent>,
    flow: Arc<FlowState>,
}

/// Cheap, cloneable handle to a live tunnel session.
#[derive(Clone)]
pub struct Session {
    pub ctrl: mpsc::UnboundedSender<Block>,
    pub data: mpsc::Sender<Block>,
    streams: Arc<Mutex<HashMap<u32, StreamHandle>>>,
}

/// Receiving ends of the session queues, consumed by `run_tunnel`.
pub struct SessionIo {
    ctrl_rx: mpsc::UnboundedReceiver<Block>,
    data_rx: mpsc::Receiver<Block>,
}

impl Session {
    pub fn new() -> (Session, SessionIo) {
        let (ctrl, ctrl_rx) = mpsc::unbounded_channel();
        let (data, data_rx) = mpsc::channel(DATA_QUEUE_BLOCKS);
        (
            Session {
                ctrl,
                data,
                streams: Arc::new(Mutex::new(HashMap::new())),
            },
            SessionIo { ctrl_rx, data_rx },
        )
    }

    /// Register a connection id and return the receiver for its events.
    pub fn register(&self, conn_id: u32) -> StreamReceiver {
        let (tx, rx) = mpsc::unbounded_channel();
        let flow = Arc::new(FlowState {
            queued: AtomicUsize::new(0),
            pause_sent: AtomicBool::new(false),
        });
        self.streams.lock().unwrap().insert(
            conn_id,
            StreamHandle {
                tx,
                flow: Arc::clone(&flow),
            },
        );
        StreamReceiver { rx, flow }
    }

    pub fn unregister(&self, conn_id: u32) {
        self.streams.lock().unwrap().remove(&conn_id);
    }

    /// True once the tunnel writer has gone away (session is dead).
    pub fn is_closed(&self) -> bool {
        self.data.is_closed()
    }

    pub fn active_streams(&self) -> usize {
        self.streams.lock().unwrap().len()
    }

    fn get(&self, conn_id: u32) -> Option<StreamHandle> {
        self.streams.lock().unwrap().get(&conn_id).cloned()
    }

    fn deliver(&self, conn_id: u32, ev: StreamEvent) {
        if let Some(h) = self.get(conn_id) {
            let _ = h.tx.send(ev);
        }
    }

    fn abort_all(&self) {
        let handles: Vec<StreamHandle> =
            self.streams.lock().unwrap().drain().map(|(_, h)| h).collect();
        for h in handles {
            let _ = h.tx.send(StreamEvent::Abort);
        }
    }
}

/// Side-specific handling of control blocks the core doesn't handle itself.
pub trait BlockHandler: Send {
    /// `CODE_START` from the peer (server side opens a backend connection).
    fn on_start(&mut self, _conn_id: u32, _session: &Session) {}
    /// `CODE_ECHO` from the peer.
    fn on_echo(&mut self, _block: Block, _session: &Session) {}
}

#[derive(Debug, Clone, Copy)]
pub struct Keepalive {
    /// Send a PING when the writer has been idle this long.
    pub interval: Option<Duration>,
    /// Declare the tunnel dead if nothing was received for this long.
    pub timeout: Option<Duration>,
}

impl Keepalive {
    /// `secs == 0` disables keepalive; otherwise ping every `secs`, dead after `3 * secs`.
    pub fn from_secs(secs: u64) -> Self {
        if secs == 0 {
            Keepalive {
                interval: None,
                timeout: None,
            }
        } else {
            Keepalive {
                interval: Some(Duration::from_secs(secs)),
                timeout: Some(Duration::from_secs(secs * 3)),
            }
        }
    }
}

/// Tune a raw tunnel TCP socket: no Nagle, OS-level TCP keepalive as a backstop.
pub fn configure_tunnel_socket(s: &TcpStream) {
    let _ = s.set_nodelay(true);
    let ka = socket2::TcpKeepalive::new().with_time(Duration::from_secs(30));
    let _ = socket2::SockRef::from(s).set_tcp_keepalive(&ka);
}

/// Close a TCP socket with RST (SO_LINGER=0) so the app sees an error instead of a clean EOF.
pub fn reset_socket(s: &TcpStream) {
    let _ = socket2::SockRef::from(s).set_linger(Some(Duration::ZERO));
}

/// Run a tunnel until it dies. Returns a human-readable reason.
/// On exit all registered connections receive `Abort` (their local sockets get RST).
pub async fn run_tunnel<S, H>(
    stream: S,
    session: Session,
    io: SessionIo,
    mut handler: H,
    keepalive: Keepalive,
    real_bytes: Option<Arc<AtomicU64>>,
) -> String
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: BlockHandler,
{
    let (r, w) = tokio::io::split(stream);
    let mut writer = tokio::spawn(run_writer(
        w,
        io.ctrl_rx,
        io.data_rx,
        keepalive.interval,
        real_bytes,
    ));

    let reason = tokio::select! {
        r = run_reader(r, &session, &mut handler, keepalive.timeout) => r,
        w = &mut writer => match w {
            Ok(Ok(())) => "tunnel writer finished".to_string(),
            Ok(Err(e)) => format!("tunnel write error: {}", e),
            Err(e) => format!("tunnel writer task failed: {}", e),
        },
    };

    writer.abort();
    session.abort_all();
    reason
}

async fn run_writer<W: AsyncWrite + Unpin>(
    w: W,
    mut ctrl_rx: mpsc::UnboundedReceiver<Block>,
    mut data_rx: mpsc::Receiver<Block>,
    ping_every: Option<Duration>,
    real_bytes: Option<Arc<AtomicU64>>,
) -> std::io::Result<()> {
    let mut w = BufWriter::with_capacity(IO_BUF, w);
    let idle = ping_every.unwrap_or(Duration::from_secs(365 * 24 * 3600));
    let mut ctrl_open = true;
    let mut data_open = true;

    let count = |b: &Block| {
        if let Some(rb) = &real_bytes {
            if b.is_data() {
                rb.fetch_add(b.length() as u64, Ordering::Relaxed);
            }
        }
    };

    loop {
        if !ctrl_open && !data_open {
            return Ok(());
        }
        let deadline = Instant::now() + idle;
        let first = tokio::select! {
            biased;
            b = ctrl_rx.recv(), if ctrl_open => match b {
                Some(b) => b,
                None => { ctrl_open = false; continue; }
            },
            b = data_rx.recv(), if data_open => match b {
                Some(b) => b,
                None => { data_open = false; continue; }
            },
            _ = tokio::time::sleep_until(deadline) => Block::new_control(0, CODE_PING),
        };

        count(&first);
        let mut batch = 8 + first.length();
        write_block_noflush(&mut w, &first).await?;

        // Coalesce whatever is already queued (control first) into this flush.
        while batch < MAX_BATCH_BYTES {
            let next = match ctrl_rx.try_recv() {
                Ok(b) => b,
                Err(_) => match data_rx.try_recv() {
                    Ok(b) => b,
                    Err(_) => break,
                },
            };
            count(&next);
            batch += 8 + next.length();
            write_block_noflush(&mut w, &next).await?;
        }
        w.flush().await?;
    }
}

async fn run_reader<R, H>(
    r: R,
    session: &Session,
    handler: &mut H,
    timeout: Option<Duration>,
) -> String
where
    R: AsyncRead + Unpin,
    H: BlockHandler,
{
    let mut r = BufReader::with_capacity(IO_BUF, r);
    // Remember ids we already answered with ABORT, to avoid one ABORT per stray block.
    let mut aborted_unknown: HashSet<u32> = HashSet::new();

    loop {
        let res = match timeout {
            Some(t) => match tokio::time::timeout(t, read_block(&mut r)).await {
                Ok(res) => res,
                Err(_) => {
                    return format!("nothing received for {}s (keepalive timeout)", t.as_secs())
                }
            },
            None => read_block(&mut r).await,
        };
        let block = match res {
            Ok(Some(b)) => b,
            Ok(None) => return "tunnel closed by peer".to_string(),
            Err(e) => return format!("tunnel read error: {}", e),
        };

        let conn_id = block.conn_id;
        match block.block_type() {
            BlockType::Data { .. } => match session.get(conn_id) {
                Some(h) => {
                    let len = block.data.len();
                    let queued = h.flow.queued.fetch_add(len, Ordering::SeqCst) + len;
                    // Must flag PAUSE *before* handing over the data, so the connection
                    // task is guaranteed to observe `pause_sent` when it drains.
                    if queued > PAUSE_HIGH_WATERMARK && !h.flow.pause_sent.swap(true, Ordering::SeqCst)
                    {
                        debug!("conn {:08x}: {} bytes queued, sending PAUSE", conn_id, queued);
                        let _ = session.ctrl.send(Block::new_control(conn_id, CODE_PAUSE));
                    }
                    if h.tx.send(StreamEvent::Data(block.data)).is_err() {
                        session.unregister(conn_id);
                    }
                }
                None => {
                    if aborted_unknown.len() > 4096 {
                        aborted_unknown.clear();
                    }
                    if aborted_unknown.insert(conn_id) {
                        debug!("Data for unknown/closed conn {:08x}, sending ABORT", conn_id);
                        let _ = session.ctrl.send(Block::new_control(conn_id, CODE_ABORT));
                    }
                }
            },
            BlockType::Control { code } => match code {
                CODE_EOF => session.deliver(conn_id, StreamEvent::Eof),
                CODE_PAUSE => session.deliver(conn_id, StreamEvent::Pause),
                CODE_RESUME => session.deliver(conn_id, StreamEvent::Resume),
                CODE_ABORT => {
                    let removed = session.streams.lock().unwrap().remove(&conn_id);
                    if let Some(h) = removed {
                        debug!("Peer aborted conn {:08x}", conn_id);
                        let _ = h.tx.send(StreamEvent::Abort);
                    }
                }
                CODE_START => handler.on_start(conn_id, session),
                CODE_ECHO => handler.on_echo(block, session),
                CODE_NOISE | CODE_PING => {}
                other => debug!("Ignoring unknown control code {}", other),
            },
            BlockType::Unknown(tag) => warn!("Unknown block tag {:04x}", tag),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamEnd {
    /// Both directions closed cleanly (FIN both ways).
    Graceful,
    /// Local socket failed (read/write error).
    LocalError,
    /// Half-closed connection was idle too long.
    Timeout,
    /// Peer aborted or tunnel died.
    RemoteAbort,
}

fn log_local_io_error(conn_id: u32, op: &str, e: &std::io::Error) {
    match e.kind() {
        ErrorKind::ConnectionReset | ErrorKind::BrokenPipe | ErrorKind::ConnectionAborted => {
            debug!("conn {:08x}: local socket {} reset by peer: {}", conn_id, op, e)
        }
        _ => warn!("conn {:08x}: local socket {} error: {}", conn_id, op, e),
    }
}

/// Pump one local TCP socket <-> the tunnel until the connection ends.
/// The caller must have registered `conn_id` (and, on the client, sent `CODE_START`).
pub async fn run_stream(conn_id: u32, mut socket: TcpStream, rx: StreamReceiver, session: Session) {
    let end = {
        let (rd, wr) = socket.split();
        stream_loop(conn_id, rd, wr, rx, &session).await
    };
    session.unregister(conn_id);

    match end {
        StreamEnd::Graceful => {}
        StreamEnd::RemoteAbort => reset_socket(&socket),
        StreamEnd::LocalError | StreamEnd::Timeout => {
            let _ = session.data.send(Block::new_control(conn_id, CODE_ABORT)).await;
            reset_socket(&socket);
        }
    }
    info!("Connection {:08x} terminated ({:?}).", conn_id, end);
}

async fn stream_loop<R, W>(
    conn_id: u32,
    mut rd: R,
    mut wr: W,
    mut rx: StreamReceiver,
    session: &Session,
) -> StreamEnd
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; STREAM_READ_BUF];
    let mut seq: u16 = 0;
    let mut read_closed = false;
    let mut write_closed = false;
    let mut paused = false;

    loop {
        tokio::select! {
            res = rd.read(&mut buf), if !read_closed && !paused => match res {
                Ok(0) => {
                    debug!("conn {:08x}: local EOF (half-close)", conn_id);
                    read_closed = true;
                    if session.data.send(Block::new_control(conn_id, CODE_EOF)).await.is_err() {
                        return StreamEnd::RemoteAbort;
                    }
                    if write_closed {
                        return StreamEnd::Graceful;
                    }
                }
                Ok(n) => {
                    let blk = Block::new_data(conn_id, seq, Bytes::copy_from_slice(&buf[..n]));
                    seq = next_sequence(seq);
                    if session.data.send(blk).await.is_err() {
                        return StreamEnd::RemoteAbort;
                    }
                }
                Err(e) => {
                    log_local_io_error(conn_id, "read", &e);
                    return StreamEnd::LocalError;
                }
            },
            ev = rx.rx.recv() => match ev {
                Some(StreamEvent::Data(d)) => {
                    let len = d.len();
                    if !write_closed {
                        if let Err(e) = wr.write_all(&d).await {
                            log_local_io_error(conn_id, "write", &e);
                            return StreamEnd::LocalError;
                        }
                    }
                    let left = rx.flow.queued.fetch_sub(len, Ordering::SeqCst) - len;
                    if left <= RESUME_LOW_WATERMARK && rx.flow.pause_sent.swap(false, Ordering::SeqCst) {
                        debug!("conn {:08x}: queue drained, sending RESUME", conn_id);
                        let _ = session.ctrl.send(Block::new_control(conn_id, CODE_RESUME));
                    }
                }
                Some(StreamEvent::Eof) => {
                    debug!("conn {:08x}: remote EOF (half-close)", conn_id);
                    if !write_closed {
                        let _ = wr.shutdown().await;
                        write_closed = true;
                    }
                    if read_closed {
                        return StreamEnd::Graceful;
                    }
                }
                Some(StreamEvent::Pause) => paused = true,
                Some(StreamEvent::Resume) => paused = false,
                Some(StreamEvent::Abort) | None => return StreamEnd::RemoteAbort,
            },
            _ = tokio::time::sleep(HALF_CLOSE_TIMEOUT), if read_closed || write_closed => {
                warn!("conn {:08x}: half-closed connection idle, closing", conn_id);
                return StreamEnd::Timeout;
            }
        }
    }
}
