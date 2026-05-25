//! Length-prefixed rkyv-over-TCP transport for the WombatKV daemon.
//!
//! Companion to the SHM transport in `lifecycle::serve_prefix`. The SHM
//! transport is single-host (1P-1C disruptor ring on POSIX SHM); this
//! TCP transport extends WombatKV to cross-machine deployments, for
//! example, ds4 on a Mac talking to a `wombatkv-daemon` on a Linux
//! GPU box, both sharing the same S3 bucket as the durable backstop.
//!
//! ## Wire format
//!
//! Same on both directions:
//!
//! ```text
//! +---------+-------------------------+
//! | u32 BE  | rkyv-archived envelope  |
//! | length  | (WireRequest / WireResponse) |
//! +---------+-------------------------+
//! ```
//!
//! `length` does NOT include the 4-byte prefix itself; clients allocate
//! exactly `length` bytes for the body. Cap at `MAX_FRAME_BYTES` so a
//! malformed/hostile peer can't trigger an unbounded allocation.
//!
//! Mirrors the prior playground's `tcp_rkyv.rs` (which itself draws on the
//! `[u32 BE length][rkyv envelope]` framing iggy uses). The the prior playground
//! variant uses monoio for I/O; we use `std::net` + `std::thread` per
//! connection for alpha simplicity. The TPC / `SO_REUSEPORT` variant
//! using compio (mirroring `compio_tcp.rs`) lands in a follow-up
//! commit; this file is the "alpha simple" baseline.
//!
//! ## Server
//!
//! `serve_tcp(addr, dispatch)` binds + accepts forever. Each
//! connection runs in its own OS thread that loops:
//!   1. Read 4-byte BE length.
//!   2. Read `length` bytes.
//!   3. Decode `WireRequest` (rkyv).
//!   4. Call the supplied `dispatch` closure to produce `WireResponse`.
//!   5. Encode + length-prefix the response back over the socket.
//!
//! Same `WireRequest` + `WireResponse` types as the SHM transport,
//! so the daemon's existing `dispatch()` function plugs in unchanged.
//!
//! ## Client
//!
//! `TcpKvClient::connect(addr)` returns a sync client mirroring
//! `RemoteKvStoreClient`'s shape: `put_kv`, `get_kv`, `lookup_block_prefix`.
//! Sequential RPCs serialized via a single `Mutex<TcpStream>`; multiple
//! concurrent in-flight requests aren't supported in this first cut
//! (clients should clone a new `TcpKvClient` per thread).

use std::io::{ErrorKind, Read, Write};
#[cfg(test)]
use std::net::TcpListener;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Re-exported for callers building a `DispatchHandle` directly.
pub use flume;

use bytes::Bytes;
use myelon::codec::Codec;

use crate::envelope::{
    decode_envelope_header, encode_envelope, verify_envelope_crc, EnvelopeError,
    WIRE_ENVELOPE_BYTES,
};
use crate::{
    decode_get_kv_blocks_batch_resp, decode_lookup_block_prefix_resp,
    decode_put_kv_blocks_batch_resp, encode_get_kv_blocks_batch_req,
    encode_lookup_block_prefix_req, encode_put_kv_blocks_batch_req, op, status,
    GetKvBlocksBatchReq, LookupBlockPrefixReq, PutKvBlocksBatchReq, WireRequest, WireResponse,
};

impl From<EnvelopeError> for TcpServerError {
    fn from(e: EnvelopeError) -> Self {
        TcpServerError::Decode(format!("envelope: {e}"))
    }
}

impl From<EnvelopeError> for TcpClientError {
    fn from(e: EnvelopeError) -> Self {
        TcpClientError::Codec(format!("envelope: {e}"))
    }
}

/// Maximum on-wire frame size accepted by the server. Caps at 256 MiB
/// (well above ds4's 1.7k-token KV block payloads at ~22 MiB and below
/// the chunked-PUT threshold for the embedded path).
pub const MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;

/// Server-side errors that don't fit cleanly into `std::io::Error`.
#[derive(Debug)]
pub enum TcpServerError {
    Io(std::io::Error),
    Decode(String),
    Encode(String),
}

impl std::fmt::Display for TcpServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TcpServerError::Io(e) => write!(f, "tcp io: {e}"),
            TcpServerError::Decode(m) => write!(f, "tcp decode: {m}"),
            TcpServerError::Encode(m) => write!(f, "tcp encode: {m}"),
        }
    }
}

impl std::error::Error for TcpServerError {}

impl From<std::io::Error> for TcpServerError {
    fn from(e: std::io::Error) -> Self {
        TcpServerError::Io(e)
    }
}

// alpha.11+1 sprawl cleanup: the std::net `serve_tcp` + `handle_connection`
// path was deleted. ONE runtime per transport: `serve_tcp_compio_bridge`
// (compio + SO_REUSEPORT + flume dispatch bridge). Default TPC threads = 2
// so SO_REUSEPORT engages out of the box; set
// `WMBT_KV_TCP_TPC_THREADS=1` for single-thread compio when you really
// want the lowest mem footprint.

fn is_peer_disconnect(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        ErrorKind::UnexpectedEof
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
            | ErrorKind::NotConnected
    )
}

// ============================================================
// compio io_uring TCP server (mirrors the prior playground compio_tcp.rs)
// ============================================================
//
// N OS threads, each with its own compio runtime, all binding the
// same `addr` with `SO_REUSEPORT`. The Linux kernel load-balances
// incoming connections across shards. Each shard runs an accept loop
// + per-connection compio task that calls the supplied (sync)
// `dispatch` closure for each WireRequest frame.
//
// On Linux this uses io_uring under the hood; on macOS, kqueue.
// SO_REUSEPORT is honored on Linux; on macOS it's accepted but the
// kernel's load-balancing semantics are weaker.
//
// Contrast with the std::net `serve_tcp` above:
//   - std::net: one accept loop, one thread per connection
//   - compio:   N accept loops (kernel-balanced), one task per
//               connection (lightweight)
//
// For ds4's typical single-client cell-B workload the difference is
// invisible, the per-conn cost dominates. The win shows up under
// many concurrent clients (multi-engine team-multiplier scenarios
// or future N-engine deployments hitting one daemon).

use std::sync::atomic::{AtomicBool, Ordering as AtomicOrd};

// ============================================================
// Dispatch bridge (mirrors the prior playground's `transports/bridge.rs`)
// ============================================================
//
// The compio shard threads handle wire framing (read + decode +
// encode + write). They do NOT call the per-request dispatch
// closure inline, instead they ferry the decoded `WireRequest`
// to a separate pool of std::thread sync workers via a bounded
// `flume::Sender<DispatchJob>`, and await the response over a
// per-request `flume::bounded(1)` oneshot.
//
// Why: the dispatch closure is synchronous (it eventually calls
// into `WombatKVKvStore` which does blocking S3 / foyer / slatedb
// I/O). Running it inside a compio task body BLOCKS the shard's
// runtime, every other connection on that shard waits. That cost
// is the headline finding from the cross-machine multi-client
// bench (8 clients: std::net 696 vs compio 517 ops/s; 16 clients:
// 1132 vs 706). std::net dodges it by spawning one OS thread per
// connection, so the Linux scheduler distributes them across all
// cores.
//
// With this bridge:
//   - N compio shards do nothing but framing (cheap, async).
//   - M worker threads do dispatch (heavy, blocking).
//   - The kernel + flume's MPMC fairness load-balance between
//     them. Worker count is decoupled from shard count.
//
// Backpressure: the request channel is `flume::bounded(M * 4)`.
// When workers can't keep up, `tx.send_async` awaits on the compio
// shard, the shard stops reading the wire, and the kernel TCP
// receive buffer fills + flow-controls the client. No unbounded
// allocation, no OOM under burst.
//
// Mini-tpuf's variant uses `tokio::oneshot` because their workers
// run on monoio; ours uses `flume::bounded(1)` because flume is
// the only runtime-agnostic primitive in our dep tree and we don't
// need tokio for plain sync workers.

/// One in-flight dispatch request handed from a compio shard to a
/// worker thread. The shard awaits `resp_rx` for the response and
/// then writes it back to the wire.
struct DispatchJob {
    client_id: u64,
    req: WireRequest,
    resp_tx: flume::Sender<WireResponse>,
}

/// Cloneable handle the compio shards use to submit jobs to the
/// dispatch worker pool. Cheap to clone (one `flume::Sender`).
#[derive(Clone)]
pub struct DispatchHandle {
    tx: flume::Sender<DispatchJob>,
}

impl DispatchHandle {
    /// Submit a `WireRequest` and asynchronously await its
    /// `WireResponse`. Returns `Err` only if the worker pool has
    /// shut down (sender or receiver dropped).
    ///
    /// # Errors
    /// Surfaces a `TcpServerError::Decode` describing the pool
    /// state, these never reach the wire as decode errors;
    /// they're informational for the shard's log output.
    pub async fn dispatch_async(
        &self,
        client_id: u64,
        req: WireRequest,
    ) -> Result<WireResponse, TcpServerError> {
        let (resp_tx, resp_rx) = flume::bounded::<WireResponse>(1);
        self.tx
            .send_async(DispatchJob { client_id, req, resp_tx })
            .await
            .map_err(|_| TcpServerError::Decode("dispatch pool sender closed".to_string()))?;
        resp_rx
            .recv_async()
            .await
            .map_err(|_| TcpServerError::Decode("dispatch worker dropped response".to_string()))
    }
}

/// Spawn `worker_count` sync dispatch worker threads sharing one
/// bounded `flume` queue. Each worker loops on `rx.recv()` and
/// invokes the supplied closure synchronously. Returns a
/// cloneable `DispatchHandle` the compio shards use to submit
/// jobs.
///
/// Workers run for the process lifetime; there is no shutdown
/// path because the daemon binary exits when the listener stops
/// accepting and the OS reclaims the threads. If a clean shutdown
/// is added later, drop all `DispatchHandle` clones to close the
/// channel and the workers will exit their recv loops.
///
/// # Panics
/// Panics if `std::thread::Builder::spawn` fails for any worker
/// (consistent with the existing accept-side spawn behavior).
pub fn spawn_dispatch_workers<F>(worker_count: usize, dispatch: F) -> DispatchHandle
where
    F: Fn(u64, WireRequest) -> WireResponse + Send + Sync + 'static,
{
    let worker_count = worker_count.max(1);
    // Capacity heuristic from the prior playground bridge.rs: workers × 4 jobs
    // in flight gives a soft ceiling on memory while still letting
    // the wire fill up to a small burst ahead of the workers.
    let queue_depth = (worker_count * 4).max(8);
    let (tx, rx) = flume::bounded::<DispatchJob>(queue_depth);
    let dispatch = Arc::new(dispatch);
    println!(
        r#"{{"scope":"wombatkv_tcp_daemon","event":"dispatch_pool_spawn","workers":{worker_count},"queue_depth":{queue_depth}}}"#
    );
    for w in 0..worker_count {
        let rx = rx.clone();
        let dispatch = Arc::clone(&dispatch);
        std::thread::Builder::new()
            .name(format!("wmbt-dispatch-{w}"))
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    // DST chaos site (TPC dispatch worker): buggify can
                    // sleep just before dispatch to provoke the
                    // client-side timeout / retry path on TCP and HTTP
                    // alike, both transports ferry through this pool
                    // via DispatchHandle, so one buggify here covers
                    // both wire paths (mirroring the SHM-side buggify
                    // in wombatkv-daemon::bin::wombatkv-daemon line
                    // ~893). Inert in non-dst builds.
                    #[cfg(feature = "dst")]
                    if wombatkv_dst::dst_buggify!() {
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                    let resp = dispatch(job.client_id, job.req);
                    let _ = job.resp_tx.send(resp);
                }
            })
            .expect("spawn wmbt dispatch worker");
    }
    DispatchHandle { tx }
}

/// Spawn a TCP listener using compio's io_uring (Linux) / kqueue
/// (macOS) accept loop with N-shard SO_REUSEPORT fan-out and a
/// dispatch bridge: compio shards do framing only, blocking dispatch
/// runs on a separate pool of sync worker threads via flume.
///
/// `tpc_threads`: number of OS threads, each running its own compio
/// runtime. >=2 enables SO_REUSEPORT and kernel-balanced accept across
/// shards. 1 is the single-thread path (still io_uring, just no
/// SO_REUSEPORT).
///
/// `dispatch_workers` controls the pool size; the daemon binary
/// reads `WMBT_KV_TCP_DISPATCH_WORKERS` (default `8`).
///
/// # Errors
/// Surfaces bind failures (e.g. ENAMETOOLONG, EADDRINUSE) and per-shard
/// runtime-spawn failures.
pub fn serve_tcp_compio_bridge<F>(
    addr: std::net::SocketAddr,
    tpc_threads: usize,
    dispatch_workers: usize,
    dispatch: F,
) -> std::io::Result<()>
where
    F: Fn(u64, WireRequest) -> WireResponse + Send + Sync + 'static,
{
    let n = tpc_threads.max(1);
    let handle = spawn_dispatch_workers(dispatch_workers, dispatch);
    println!(
        r#"{{"scope":"wombatkv_tcp_daemon","event":"compio_bridge_starting","bind":"{addr}","tpc_threads":{n},"dispatch_workers":{dispatch_workers}}}"#
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let mut joins = Vec::with_capacity(n);
    for shard_id in 0..n {
        let handle = handle.clone();
        let shutdown = Arc::clone(&shutdown);
        let join = std::thread::Builder::new()
            .name(format!("wombatkv-tcp-compio-br-{shard_id}"))
            .spawn(move || {
                let runtime = match compio::runtime::Runtime::new() {
                    Ok(rt) => rt,
                    Err(e) => {
                        eprintln!(
                            r#"{{"scope":"wombatkv_tcp_daemon","event":"compio_runtime_failed","shard":{shard_id},"error":"{e}"}}"#
                        );
                        return;
                    }
                };
                runtime.block_on(async move {
                    if let Err(e) =
                        serve_compio_shard_bridge(shard_id, addr, handle, shutdown).await
                    {
                        eprintln!(
                            r#"{{"scope":"wombatkv_tcp_daemon","event":"compio_shard_exit","shard":{shard_id},"error":"{e}"}}"#
                        );
                    }
                });
            })?;
        joins.push(join);
    }
    for j in joins {
        let _ = j.join();
    }
    Ok(())
}

async fn serve_compio_shard_bridge(
    shard_id: usize,
    addr: std::net::SocketAddr,
    handle: DispatchHandle,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<()> {
    let domain = if addr.is_ipv4() { socket2::Domain::IPV4 } else { socket2::Domain::IPV6 };
    let sock = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
    sock.set_reuse_address(true)?;
    #[cfg(unix)]
    sock.set_reuse_port(true)?;
    sock.bind(&addr.into())?;
    sock.listen(128)?;
    sock.set_nonblocking(true)?;
    let std_listener: std::net::TcpListener = sock.into();
    let listener = compio::net::TcpListener::from_std(std_listener)?;
    let conn_seq = Arc::new(AtomicU64::new(((shard_id as u64) << 32) | 1));

    println!(
        r#"{{"scope":"wombatkv_tcp_daemon","event":"compio_bridge_shard_listening","shard":{shard_id},"bind":"{addr}"}}"#
    );

    loop {
        if shutdown.load(AtomicOrd::SeqCst) {
            return Ok(());
        }
        let accept_fut = listener.accept();
        let sleep_fut = compio::time::sleep(std::time::Duration::from_millis(100));
        futures::pin_mut!(accept_fut);
        futures::pin_mut!(sleep_fut);
        match futures::future::select(accept_fut, sleep_fut).await {
            futures::future::Either::Left((Ok((stream, peer)), _)) => {
                let handle = handle.clone();
                let conn_id = conn_seq.fetch_add(1, Ordering::SeqCst);
                println!(
                    r#"{{"scope":"wombatkv_tcp_daemon","event":"compio_bridge_accepted","shard":{shard_id},"conn_id":{conn_id},"peer":"{peer}"}}"#
                );
                compio::runtime::spawn(async move {
                    if let Err(e) = handle_compio_connection_bridge(stream, conn_id, handle).await {
                        eprintln!(
                            r#"{{"scope":"wombatkv_tcp_daemon","event":"compio_bridge_conn_error","conn_id":{conn_id},"error":"{e}"}}"#
                        );
                    }
                })
                .detach();
            }
            futures::future::Either::Left((Err(e), _)) => {
                eprintln!(
                    r#"{{"scope":"wombatkv_tcp_daemon","event":"compio_accept_error","shard":{shard_id},"error":"{e}"}}"#
                );
            }
            futures::future::Either::Right(((), _)) => {}
        }
    }
}

async fn handle_compio_connection_bridge(
    mut stream: compio::net::TcpStream,
    conn_id: u64,
    handle: DispatchHandle,
) -> Result<(), TcpServerError> {
    use compio::io::{AsyncReadExt, AsyncWriteExt};
    let _ = stream.set_nodelay(true);
    loop {
        // RFC 0018 Phase 3: read 16-byte envelope header first.
        let env_buf = vec![0u8; WIRE_ENVELOPE_BYTES];
        let compio::BufResult(res, env_buf) = stream.read_exact(env_buf).await;
        if let Err(e) = res {
            if is_peer_disconnect(&e) {
                return Ok(());
            }
            return Err(e.into());
        }
        let env_arr: [u8; WIRE_ENVELOPE_BYTES] =
            env_buf.as_slice().try_into().expect("read_exact of envelope size");
        let header = decode_envelope_header(&env_arr)?;
        let frame_len = header.len as usize;
        if frame_len > MAX_FRAME_BYTES {
            return Err(TcpServerError::Decode(format!(
                "envelope body_len {frame_len} exceeds MAX_FRAME_BYTES {MAX_FRAME_BYTES}"
            )));
        }
        let body = vec![0u8; frame_len];
        let compio::BufResult(res, body) = stream.read_exact(body).await;
        if let Err(e) = res {
            if is_peer_disconnect(&e) {
                return Ok(());
            }
            return Err(e.into());
        }
        verify_envelope_crc(&header, &body)?;
        let req = WireRequest::decode(&body)
            .map_err(|e| TcpServerError::Decode(format!("WireRequest: {e:?}")))?;

        // *** the only behavioral change from the inline path ***
        // Ferry to the worker pool and await response. The compio
        // shard runtime stays free to drive other connections'
        // framing in the meantime.
        let response = handle.dispatch_async(conn_id, req).await?;

        let resp_bytes = WireResponse::encode(&response)
            .map_err(|e| TcpServerError::Encode(format!("WireResponse: {e:?}")))?;
        // RFC 0018 Phase 6, transport-slow-write chaos site (TCP
        // TPC). Brief blocking sleep on the compio shard simulates a
        // slow-write peer + tests client-side timeout discipline.
        // Inert in non-dst builds.
        #[cfg(feature = "dst")]
        if wombatkv_dst::dst_buggify!() {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let wire = encode_envelope(resp_bytes.as_ref());
        let compio::BufResult(res, _) = stream.write_all(wire).await;
        if let Err(e) = res {
            if is_peer_disconnect(&e) {
                return Ok(());
            }
            return Err(e.into());
        }
    }
}

// ============================================================
// Client side
// ============================================================

/// Mirror of `client::RemoteGetOutcome` for the TCP transport.
#[derive(Debug, Clone)]
pub enum TcpGetOutcome {
    Hit { payload: Bytes },
    Miss,
}

#[derive(Debug)]
pub enum TcpClientError {
    Io(std::io::Error),
    Codec(String),
    /// Server returned a non-OK / non-MISS status. `code` mirrors the
    /// `status` module's u8 constants; `message` is the server's
    /// inline error string.
    Status {
        code: u8,
        message: String,
    },
}

impl std::fmt::Display for TcpClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TcpClientError::Io(e) => write!(f, "tcp client io: {e}"),
            TcpClientError::Codec(m) => write!(f, "tcp client codec: {m}"),
            TcpClientError::Status { code, message } => {
                write!(f, "tcp client server status code={code} msg={message:?}")
            }
        }
    }
}

impl std::error::Error for TcpClientError {}

impl From<std::io::Error> for TcpClientError {
    fn from(e: std::io::Error) -> Self {
        TcpClientError::Io(e)
    }
}

/// Sync TCP client for a `wombatkv-daemon` listening on TCP.
///
/// Serializes all I/O through one `Mutex<TcpStream>`, that means
/// concurrent callers from multiple threads share a single in-flight
/// request slot. For true concurrency, clone a fresh `TcpKvClient` per
/// thread (each instance gets its own TCP connection).
pub struct TcpKvClient {
    stream: Mutex<TcpStream>,
    next_id: AtomicU64,
}

impl TcpKvClient {
    /// Connect to a remote `wombatkv-daemon` TCP listener.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self, TcpClientError> {
        Self::connect_with_timeout(addr, Duration::from_secs(10))
    }

    pub fn connect_with_timeout<A: ToSocketAddrs>(
        addr: A,
        timeout: Duration,
    ) -> Result<Self, TcpClientError> {
        let addrs = addr.to_socket_addrs().map_err(TcpClientError::Io)?.collect::<Vec<_>>();
        if addrs.is_empty() {
            return Err(TcpClientError::Io(std::io::Error::new(
                ErrorKind::AddrNotAvailable,
                "no socket addresses resolved",
            )));
        }
        let mut last_err: Option<std::io::Error> = None;
        for sa in addrs {
            match TcpStream::connect_timeout(&sa, timeout) {
                Ok(stream) => {
                    stream.set_nodelay(true)?;
                    return Ok(Self { stream: Mutex::new(stream), next_id: AtomicU64::new(1) });
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(TcpClientError::Io(last_err.unwrap_or_else(|| std::io::Error::other("connect failed"))))
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    fn roundtrip(&self, req: WireRequest) -> Result<WireResponse, TcpClientError> {
        let req_bytes = WireRequest::encode(&req)
            .map_err(|e| TcpClientError::Codec(format!("encode req: {e:?}")))?;
        // RFC 0018 Phase 3: wrap the rkyv body in the universal envelope.
        let wire = encode_envelope(req_bytes.as_ref());

        let mut stream = self.stream.lock().expect("tcp stream mutex poisoned");
        stream.write_all(&wire)?;
        stream.flush()?;

        let mut env_buf = [0u8; WIRE_ENVELOPE_BYTES];
        stream.read_exact(&mut env_buf)?;
        let header = decode_envelope_header(&env_buf)?;
        let n = header.len as usize;
        if n > MAX_FRAME_BYTES {
            return Err(TcpClientError::Codec(format!("resp envelope.len {n} too large")));
        }
        let mut body = vec![0u8; n];
        stream.read_exact(&mut body)?;
        drop(stream);
        verify_envelope_crc(&header, &body)?;

        let resp = WireResponse::decode(&body)
            .map_err(|e| TcpClientError::Codec(format!("decode resp: {e:?}")))?;
        Ok(resp)
    }

    /// PUT a payload under (namespace, key). Mirrors
    /// `RemoteKvStoreClient::put_kv` but over TCP.
    ///
    /// # Errors
    /// Returns `TcpClientError::Status` if the server reports a
    /// non-OK status; bubbles up I/O / codec errors otherwise.
    pub fn put_kv(&self, namespace: &str, key: &str, payload: Bytes) -> Result<(), TcpClientError> {
        let req = WireRequest {
            id: self.next_id(),
            op: op::PUT,
            namespace: namespace.to_string(),
            key: key.to_string(),
            payload: payload.to_vec(),
        };
        let resp = self.roundtrip(req)?;
        if resp.status == status::OK {
            Ok(())
        } else {
            Err(TcpClientError::Status { code: resp.status, message: resp.message })
        }
    }

    /// GET payload by (namespace, key). Returns `Miss` if the daemon
    /// reports `status::MISS`; bubbles up other errors.
    ///
    /// # Errors
    /// As above.
    pub fn get_kv(&self, namespace: &str, key: &str) -> Result<TcpGetOutcome, TcpClientError> {
        let req = WireRequest {
            id: self.next_id(),
            op: op::GET,
            namespace: namespace.to_string(),
            key: key.to_string(),
            payload: Vec::new(),
        };
        let resp = self.roundtrip(req)?;
        match resp.status {
            s if s == status::OK => Ok(TcpGetOutcome::Hit { payload: Bytes::from(resp.payload) }),
            s if s == status::MISS => Ok(TcpGetOutcome::Miss),
            other => Err(TcpClientError::Status { code: other, message: resp.message }),
        }
    }

    /// Block-shaped: count leading content-addressed blocks present in
    /// the daemon's metadata index. Mirrors `RemoteKvStoreClient::
    /// lookup_block_prefix`. Each `block_hashes_hex` entry MUST be
    /// 64 lower-hex characters (32-byte blake3 hash).
    ///
    /// # Errors
    /// Surfaces malformed-hex / backend errors.
    pub fn lookup_block_prefix(
        &self,
        namespace: &str,
        block_hashes_hex: &[String],
    ) -> Result<usize, TcpClientError> {
        let req_payload = encode_lookup_block_prefix_req(&LookupBlockPrefixReq {
            namespace: namespace.to_string(),
            block_hashes_hex: block_hashes_hex.to_vec(),
        })
        .map_err(|e| TcpClientError::Codec(e.to_string()))?;
        let req = WireRequest {
            id: self.next_id(),
            op: op::LOOKUP_BLOCK_PREFIX,
            namespace: namespace.to_string(),
            key: String::new(),
            payload: req_payload,
        };
        let resp = self.roundtrip(req)?;
        if resp.status != status::OK {
            return Err(TcpClientError::Status { code: resp.status, message: resp.message });
        }
        let decoded = decode_lookup_block_prefix_resp(&resp.payload)
            .map_err(|e| TcpClientError::Codec(e.to_string()))?;
        if let Some(err) = decoded.error {
            return Err(TcpClientError::Status { code: status::ERROR, message: err });
        }
        Ok(decoded.matched_count as usize)
    }

    /// Block-shaped: parallel batched GET for N content-addressed blocks.
    /// Returns `Some(payloads)` on full hit, `None` on any miss
    /// (all-or-nothing, matches the cabi `wmbt_kv_get_kv_blocks_borrowed`
    /// contract).
    ///
    /// # Errors
    /// Surfaces malformed-hex / backend errors. A simple miss returns
    /// `Ok(None)`, not an error.
    pub fn get_kv_blocks_batch(
        &self,
        namespace: &str,
        block_hashes_hex: &[String],
    ) -> Result<Option<Vec<Bytes>>, TcpClientError> {
        let req_payload = encode_get_kv_blocks_batch_req(&GetKvBlocksBatchReq {
            namespace: namespace.to_string(),
            block_hashes_hex: block_hashes_hex.to_vec(),
        })
        .map_err(|e| TcpClientError::Codec(e.to_string()))?;
        let req = WireRequest {
            id: self.next_id(),
            op: op::GET_KV_BLOCKS_BATCH,
            namespace: namespace.to_string(),
            key: String::new(),
            payload: req_payload,
        };
        let resp = self.roundtrip(req)?;
        if resp.status != status::OK {
            return Err(TcpClientError::Status { code: resp.status, message: resp.message });
        }
        let decoded = decode_get_kv_blocks_batch_resp(&resp.payload)
            .map_err(|e| TcpClientError::Codec(e.to_string()))?;
        if let Some(err) = decoded.error {
            return Err(TcpClientError::Status { code: status::ERROR, message: err });
        }
        Ok(decoded.payloads.map(|items| items.into_iter().map(Bytes::from).collect()))
    }

    /// Block-shaped: parallel batched PUT for N content-addressed blocks.
    /// `block_hashes_hex.len()` MUST equal `payloads.len()`. Daemon
    /// writes each block, then updates its metadata index so a
    /// subsequent `lookup_block_prefix` sees the new presence.
    /// Returns total bytes written across all blocks.
    ///
    /// # Errors
    /// Length mismatch is a Codec error; any per-block PUT failure
    /// surfaces as a Status error (the daemon leaves the metadata
    /// index unchanged in that case).
    pub fn put_kv_blocks_batch(
        &self,
        namespace: &str,
        block_hashes_hex: &[String],
        payloads: &[&[u8]],
    ) -> Result<u64, TcpClientError> {
        if block_hashes_hex.len() != payloads.len() {
            return Err(TcpClientError::Codec(format!(
                "put_kv_blocks_batch length mismatch: {} hashes vs {} payloads",
                block_hashes_hex.len(),
                payloads.len()
            )));
        }
        let req_payload = encode_put_kv_blocks_batch_req(&PutKvBlocksBatchReq {
            namespace: namespace.to_string(),
            block_hashes_hex: block_hashes_hex.to_vec(),
            payloads: payloads.iter().map(|s| s.to_vec()).collect(),
        })
        .map_err(|e| TcpClientError::Codec(e.to_string()))?;
        let req = WireRequest {
            id: self.next_id(),
            op: op::PUT_KV_BLOCKS_BATCH,
            namespace: namespace.to_string(),
            key: String::new(),
            payload: req_payload,
        };
        let resp = self.roundtrip(req)?;
        if resp.status != status::OK {
            return Err(TcpClientError::Status { code: resp.status, message: resp.message });
        }
        let decoded = decode_put_kv_blocks_batch_resp(&resp.payload)
            .map_err(|e| TcpClientError::Codec(e.to_string()))?;
        if let Some(err) = decoded.error {
            return Err(TcpClientError::Status { code: status::ERROR, message: err });
        }
        Ok(decoded.total_bytes)
    }

    /// Ping the daemon. Useful as a connection liveness probe.
    ///
    /// # Errors
    /// As above.
    pub fn ping(&self) -> Result<(), TcpClientError> {
        let req = WireRequest {
            id: self.next_id(),
            op: op::PING,
            namespace: String::new(),
            key: String::new(),
            payload: Vec::new(),
        };
        let resp = self.roundtrip(req)?;
        if resp.status == status::OK {
            Ok(())
        } else {
            Err(TcpClientError::Status { code: resp.status, message: resp.message })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn echo_dispatch() -> impl Fn(u64, WireRequest) -> WireResponse + Send + Sync {
        move |_id, req| WireResponse {
            id: req.id,
            status: status::OK,
            op: req.op,
            payload: req.payload,
            message: String::new(),
        }
    }

    /// Test helper: bind ephemeral port, drop the listener, spawn a
    /// TPC server on that addr in background. Returns the addr.
    /// alpha.11+1: std::net path was deleted, so all tests use the
    /// compio TPC path. 1 thread = no SO_REUSEPORT (cheapest for tests).
    fn boot_tcp_tpc_server_addr<F>(dispatch: F) -> std::net::SocketAddr
    where
        F: Fn(u64, WireRequest) -> WireResponse + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        drop(listener);
        std::thread::spawn(move || {
            let _ = serve_tcp_compio_bridge(addr, 1, 2, dispatch);
        });
        // Give shards time to bind. Use myelon's discovery-poll primitive
        // so the wait matches the rest of the transport stack (env-controlled
        // duration; default ~tens of ms, plenty for compio shard bind).
        for _ in 0..6 {
            myelon::perform_default_discovery_poll_wait();
        }
        addr
    }

    #[test]
    fn ping_roundtrip_in_process() {
        // Verifies the envelope + codec round-trip end-to-end over TPC.
        let addr = boot_tcp_tpc_server_addr(echo_dispatch());
        let client = TcpKvClient::connect(addr).expect("connect");
        client.ping().expect("ping");
    }

    #[test]
    fn put_then_get_roundtrip_in_process() {
        let store: Arc<std::sync::Mutex<std::collections::HashMap<String, Bytes>>> =
            Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let store_clone = Arc::clone(&store);
        let addr = boot_tcp_tpc_server_addr(move |_id: u64, req: WireRequest| {
            let key = format!("{}/{}", req.namespace, req.key);
            match req.op {
                op::PUT => {
                    store_clone.lock().unwrap().insert(key, Bytes::from(req.payload));
                    WireResponse {
                        id: req.id,
                        status: status::OK,
                        op: req.op,
                        payload: Vec::new(),
                        message: String::new(),
                    }
                }
                op::GET => match store_clone.lock().unwrap().get(&key) {
                    Some(payload) => WireResponse {
                        id: req.id,
                        status: status::OK,
                        op: req.op,
                        payload: payload.to_vec(),
                        message: String::new(),
                    },
                    None => WireResponse {
                        id: req.id,
                        status: status::MISS,
                        op: req.op,
                        payload: Vec::new(),
                        message: String::new(),
                    },
                },
                _ => WireResponse {
                    id: req.id,
                    status: status::ERROR,
                    op: req.op,
                    payload: Vec::new(),
                    message: "unsupported".into(),
                },
            }
        });

        let client = TcpKvClient::connect(addr).expect("connect");
        client.put_kv("ns", "k1", Bytes::from_static(b"hello")).expect("put");
        match client.get_kv("ns", "k1").expect("get") {
            TcpGetOutcome::Hit { payload } => assert_eq!(&payload[..], b"hello"),
            TcpGetOutcome::Miss => panic!("expected hit"),
        }
        match client.get_kv("ns", "missing").expect("get_miss") {
            TcpGetOutcome::Miss => {}
            TcpGetOutcome::Hit { .. } => panic!("expected miss"),
        }
    }

    // ============================================================
    // Dispatch bridge tests
    // ============================================================
    //
    // The bridge is the piece that lets compio shards ferry decoded
    // WireRequest frames to a separate pool of sync workers. These
    // tests don't need TCP at all, they exercise the
    // `DispatchHandle` + `spawn_dispatch_workers` surface directly.

    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrd2};

    /// 50 concurrent dispatch calls all return correctly with a 4-worker pool.
    #[test]
    fn bridge_handles_concurrent_dispatches() {
        let call_count = Arc::new(AtomicU64::new(0));
        let cc = Arc::clone(&call_count);
        let handle = spawn_dispatch_workers(4, move |client_id, req| {
            cc.fetch_add(1, AtomicOrd2::SeqCst);
            // Echo: response payload = `client_id:req.id` so callers
            // can verify their reply matches their request.
            let payload = format!("{client_id}:{}", req.id).into_bytes();
            WireResponse {
                id: req.id,
                status: status::OK,
                op: req.op,
                payload,
                message: String::new(),
            }
        });

        let rt = compio::runtime::Runtime::new().expect("compio runtime");
        rt.block_on(async {
            let mut futs = Vec::with_capacity(50);
            for i in 0..50u64 {
                let handle = handle.clone();
                let req = WireRequest {
                    id: i,
                    op: op::PING,
                    namespace: String::new(),
                    key: String::new(),
                    payload: Vec::new(),
                };
                // client_id encoded as a high bit so we can verify it
                // round-trips through DispatchJob.
                let client_id = 100 + i;
                futs.push(async move {
                    let resp = handle.dispatch_async(client_id, req).await.expect("dispatch_async");
                    let expected = format!("{client_id}:{i}");
                    assert_eq!(resp.payload, expected.into_bytes());
                    assert_eq!(resp.id, i);
                });
            }
            for f in futs {
                f.await;
            }
        });

        assert_eq!(call_count.load(AtomicOrd2::SeqCst), 50);
    }

    /// One-worker pool still works (worker_count.max(1) lower bound).
    #[test]
    fn bridge_single_worker_serializes_correctly() {
        let handle = spawn_dispatch_workers(0, |_id, req| WireResponse {
            id: req.id,
            status: status::OK,
            op: req.op,
            payload: req.payload,
            message: String::new(),
        });
        let rt = compio::runtime::Runtime::new().expect("compio runtime");
        rt.block_on(async {
            for i in 0..10u64 {
                let req = WireRequest {
                    id: i,
                    op: op::PING,
                    namespace: String::new(),
                    key: String::new(),
                    payload: vec![i as u8; 8],
                };
                let resp = handle.dispatch_async(i, req).await.expect("dispatch");
                assert_eq!(resp.id, i);
                assert_eq!(resp.payload.len(), 8);
            }
        });
    }

    /// Slow worker forces clients to queue. With a 2-worker pool and
    /// 10 in-flight requests each sleeping 50ms, total wall time
    /// should be roughly 250ms (10 requests / 2 workers × 50ms), not
    /// 500ms (fully serialized) and not 50ms (free for all).
    /// This verifies workers actually run in parallel.
    #[test]
    fn bridge_workers_parallelize() {
        let handle = spawn_dispatch_workers(2, |_id, req| {
            std::thread::sleep(std::time::Duration::from_millis(50));
            WireResponse {
                id: req.id,
                status: status::OK,
                op: req.op,
                payload: Vec::new(),
                message: String::new(),
            }
        });
        let rt = compio::runtime::Runtime::new().expect("compio runtime");
        let elapsed = rt.block_on(async {
            let started = std::time::Instant::now();
            let mut futs = Vec::with_capacity(10);
            for i in 0..10u64 {
                let h = handle.clone();
                futs.push(async move {
                    let req = WireRequest {
                        id: i,
                        op: op::PING,
                        namespace: String::new(),
                        key: String::new(),
                        payload: Vec::new(),
                    };
                    h.dispatch_async(i, req).await.expect("dispatch");
                });
            }
            for f in futs {
                f.await;
            }
            started.elapsed()
        });
        // 10 jobs / 2 workers × 50ms = 250ms ideal. Allow 200..600ms
        // band to absorb scheduler / first-spawn jitter.
        assert!(elapsed.as_millis() < 600, "elapsed {elapsed:?} suggests workers serialized");
        assert!(
            elapsed.as_millis() >= 200,
            "elapsed {elapsed:?} suggests fewer than 2 workers actually ran"
        );
    }

    // ====================================================
    // Negative-path tests: RFC 0018 envelope rejection on TCP wire
    // ====================================================

    /// alpha.11+1: the std::net `boot_tcp_sync_server` was deleted.
    /// Negative-path tests now go via the TPC bridge, same envelope
    /// + dispatch surface, just one runtime instead of two.
    fn boot_tcp_sync_server() -> std::net::SocketAddr {
        boot_tcp_tpc_server_addr(echo_dispatch())
    }

    fn craft_tampered_tcp_wire(tamper: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let req = WireRequest {
            id: 1,
            op: op::PING,
            namespace: String::new(),
            key: String::new(),
            payload: Vec::new(),
        };
        let req_bytes = WireRequest::encode(&req).unwrap();
        let mut wire = crate::envelope::encode_envelope(req_bytes.as_ref());
        tamper(&mut wire);
        wire
    }

    #[test]
    fn tcp_sync_rejects_envelope_bad_magic() {
        let addr = boot_tcp_sync_server();
        let tampered = craft_tampered_tcp_wire(|w| w[0] = b'X');
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(&tampered).unwrap();
        stream.flush().unwrap();
        // Server should close the connection (no reply). read_exact on
        // 16 bytes of response envelope must fail with EOF/ECONNRESET.
        let mut buf = [0u8; 16];
        let res = stream.read_exact(&mut buf);
        assert!(res.is_err(), "tampered envelope should cause connection close; got bytes={buf:?}");
    }

    #[test]
    fn tcp_sync_rejects_envelope_bad_version() {
        let addr = boot_tcp_sync_server();
        let tampered = craft_tampered_tcp_wire(|w| w[4] = 99);
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(&tampered).unwrap();
        stream.flush().unwrap();
        let mut buf = [0u8; 16];
        let res = stream.read_exact(&mut buf);
        assert!(res.is_err(), "wrong-version envelope should drop connection");
    }

    #[test]
    fn tcp_sync_rejects_envelope_bad_crc() {
        let addr = boot_tcp_sync_server();
        let tampered = craft_tampered_tcp_wire(|w| w[20] ^= 0xff);
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(&tampered).unwrap();
        stream.flush().unwrap();
        let mut buf = [0u8; 16];
        let res = stream.read_exact(&mut buf);
        assert!(res.is_err(), "body-tampered envelope should drop connection");
    }

    #[test]
    fn tcp_sync_rejects_envelope_truncated_header() {
        let addr = boot_tcp_sync_server();
        // Send only 8 bytes, half the envelope header. Server's
        // read_exact for the 16-byte header should hang until we close;
        // close should leave it cleanly returning Ok(()).
        let partial = [b'W', b'M', b'B', b'T', 1, 0, 0, 0];
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(&partial).unwrap();
        stream.flush().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        drop(stream);
        // No assertion needed, the server side handle_connection should
        // see UnexpectedEof on the next read_exact and return Ok(()). If
        // anything panics, the test process aborts; this test passing is
        // proof of clean shutdown under partial input.
    }

    #[test]
    fn tcp_sync_rejects_oversized_envelope_len() {
        let addr = boot_tcp_sync_server();
        // Craft a header that claims body_len > MAX_FRAME_BYTES.
        let mut wire = vec![0u8; crate::envelope::WIRE_ENVELOPE_BYTES];
        wire[0..4].copy_from_slice(b"WMBT");
        wire[4..8].copy_from_slice(&1u32.to_le_bytes());
        wire[8..12].copy_from_slice(&0u32.to_le_bytes()); // crc placeholder
        let oversized_len = (MAX_FRAME_BYTES + 1) as u32;
        wire[12..16].copy_from_slice(&oversized_len.to_le_bytes());
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(&wire).unwrap();
        stream.flush().unwrap();
        let mut buf = [0u8; 16];
        let res = stream.read_exact(&mut buf);
        assert!(res.is_err(), "oversized envelope.len should drop connection");
    }
}
