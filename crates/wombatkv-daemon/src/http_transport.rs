//! HTTP/1.1 + rkyv transport for the WombatKV daemon.
//!
//! Companion to [`tcp_transport`](crate::tcp_transport). Where the TCP
//! path frames `WireRequest` / `WireResponse` directly on the socket
//! with a 4-byte length prefix, this transport wraps the **same**
//! length-prefixed rkyv body inside HTTP/1.1 POST requests and
//! responses. The on-wire codec inside the body is byte-identical to
//! TCP, only the framing around it changes.
//!
//! Why both? the prior playground exposes JSON-over-HTTP/1 for cross-language
//! workers but keeps the rkyv path TCP-only (and notes "Phase 3 can
//! layer rkyv on top if benchmarks show JSON serde dominating"). We
//! want rkyv's compactness + HTTP's load-balancer / reverse-proxy /
//! middlebox friendliness in the same package.
//!
//! ## Wire format
//!
//! ```text
//! Request:
//!   POST /wmbt/v1/rpc HTTP/1.1\r\n
//!   Host: <addr>\r\n
//!   Content-Type: application/x-wombatkv-rkyv\r\n
//!   Content-Length: <N>\r\n
//!   Connection: keep-alive\r\n
//!   \r\n
//!   [rkyv-archived WireRequest, N bytes]
//!
//! Response:
//!   HTTP/1.1 200 OK\r\n
//!   Content-Type: application/x-wombatkv-rkyv\r\n
//!   Content-Length: <M>\r\n
//!   Connection: keep-alive\r\n
//!   \r\n
//!   [rkyv-archived WireResponse, M bytes]
//! ```
//!
//! The body is the bare rkyv-archived envelope, no internal length
//! prefix. HTTP's `Content-Length` already frames the payload, so a
//! second prefix is redundant. Dropping it has two upsides:
//!   1. The rkyv bytes start at offset 0 of an aligned `Vec<u8>`
//!      allocation, satisfying rkyv 0.8's 8-byte pointer-alignment
//!      requirement without a copy.
//!   2. Saves 4 bytes per RPC.
//!
//! The TCP transport keeps its `[u32 BE length][rkyv]` framing because
//! the socket itself is unframed; TCP needs the prefix to know how
//! many bytes the next frame occupies. HTTP gets framing from the
//! protocol layer.
//!
//! Application-level errors (status::ERROR in the WireResponse) flow
//! through with HTTP 200, the body carries the verdict. Only
//! protocol-level failures (malformed HTTP, unknown path, oversized
//! body) produce non-200 HTTP statuses.
//!
//! ## Server
//!
//! [`serve_http_compio_bridge`] mirrors
//! [`crate::tcp_transport::serve_tcp_compio_bridge`], binds a
//! `TcpListener`, spawns one OS thread per connection, and loops on
//! the connection serving keep-alive requests until the peer closes.
//! Each connection's request handler decodes the HTTP head, reads
//! exactly `Content-Length` body bytes, strips the 4-byte length
//! prefix, decodes `WireRequest`, calls the supplied `dispatch`
//! closure (the same closure plugged into `serve_tcp_compio_bridge`
//!, they share a dispatch surface), and writes the response.
//!
//! ## Client
//!
//! [`HttpKvClient`] mirrors `TcpKvClient`'s sync API
//! (`put_kv`, `get_kv`, `lookup_block_prefix`,
//! `get_kv_blocks_batch`, `put_kv_blocks_batch`, `ping`). One
//! persistent `TcpStream` per client, serialized through a `Mutex`;
//! clone the client per thread for true concurrency. Sends HTTP/1.1
//! keep-alive so a single TCP connection serves many RPCs.

use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use myelon::codec::Codec;

use crate::envelope::{decode_envelope, encode_envelope, EnvelopeError};
use crate::{
    decode_get_kv_blocks_batch_resp, decode_lookup_block_prefix_resp,
    decode_put_kv_blocks_batch_resp, encode_get_kv_blocks_batch_req,
    encode_lookup_block_prefix_req, encode_put_kv_blocks_batch_req, op, status,
    GetKvBlocksBatchReq, LookupBlockPrefixReq, PutKvBlocksBatchReq, WireRequest, WireResponse,
};

impl From<EnvelopeError> for HttpServerError {
    fn from(e: EnvelopeError) -> Self {
        HttpServerError::Decode(format!("envelope: {e}"))
    }
}

impl From<EnvelopeError> for HttpClientError {
    fn from(e: EnvelopeError) -> Self {
        HttpClientError::Codec(format!("envelope: {e}"))
    }
}

/// Path the daemon serves rkyv-framed RPCs on.
pub const RPC_PATH: &str = "/wmbt/v1/rpc";

/// Lightweight liveness check, returns 200 with empty body on success.
pub const PING_PATH: &str = "/wmbt/v1/ping";

/// MIME type used for the rkyv-framed body in both directions.
pub const CONTENT_TYPE: &str = "application/x-wombatkv-rkyv";

/// Maximum body size accepted by the server. Matches
/// [`crate::tcp_transport::MAX_FRAME_BYTES`] so the two transports
/// have the same payload ceiling. The body is the bare rkyv envelope
/// (no internal length prefix), so the cap applies directly to the
/// rkyv-archived payload.
pub const MAX_BODY_BYTES: usize = 256 * 1024 * 1024;

/// Cap the HTTP head we'll parse (request line + headers, up to the
/// first `\r\n\r\n`). A WombatKV client never sends close to this
/// many bytes of headers: 16 KiB is enough headroom for proxies that
/// inject extra hops while still rejecting hostile traffic that omits
/// the blank-line terminator.
pub const MAX_HEAD_BYTES: usize = 16 * 1024;

/// Server-side errors specific to the HTTP transport layer.
#[derive(Debug)]
pub enum HttpServerError {
    Io(std::io::Error),
    BadRequest(String),
    Decode(String),
    Encode(String),
}

impl std::fmt::Display for HttpServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpServerError::Io(e) => write!(f, "http io: {e}"),
            HttpServerError::BadRequest(m) => write!(f, "http bad request: {m}"),
            HttpServerError::Decode(m) => write!(f, "http decode: {m}"),
            HttpServerError::Encode(m) => write!(f, "http encode: {m}"),
        }
    }
}

impl std::error::Error for HttpServerError {}

impl From<std::io::Error> for HttpServerError {
    fn from(e: std::io::Error) -> Self {
        HttpServerError::Io(e)
    }
}

// alpha.11+1 sprawl cleanup: the std::net `serve_http` + `handle_http_connection`
// path + sync helpers (read_request_head, write_simple_response) were
// deleted. ONE runtime per transport: `serve_http_compio_bridge`
// (compio + SO_REUSEPORT + flume dispatch bridge). Default TPC threads = 2.

/// Parsed HTTP/1.1 request line + headers we care about. Used by the
/// compio TPC path's `parse_http_head_from_buf` and consumed by the
/// per-route dispatcher.
struct RequestHead {
    method: String,
    path: String,
    content_length: Option<usize>,
    keep_alive: bool,
}

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
// compio TPC HTTP server (mirrors tcp_transport's compio path)
// ============================================================
//
// N OS threads, each with its own compio runtime, all binding the
// same `addr` with `SO_REUSEPORT`. Kernel load-balances accepts
// across shards. Each shard runs an accept loop + per-connection
// compio task that ferries decoded WireRequests to the shared
// `DispatchHandle` worker pool (flume) and awaits the WireResponse
// without blocking the shard runtime.
//
// Identical architecture to `tcp_transport::serve_tcp_compio_bridge`
//, see that module's header for the rationale and bench numbers.
// HTTP/1.1 head parsing in compio uses a chunked-read accumulator
// that scans for `\r\n\r\n`, then a `Content-Length`-bounded body
// read. Keep-alive across requests is preserved by maintaining the
// per-connection accumulator buffer and draining consumed bytes
// after each request.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering as AtomicOrd;

/// Spawn an HTTP listener using compio's io_uring (Linux) / kqueue
/// (macOS) accept loop with N-shard SO_REUSEPORT fan-out, plus a
/// dispatch bridge: shards do framing only, blocking dispatch runs
/// on a separate sync worker pool via flume.
///
/// `tpc_threads`: number of OS threads each running its own compio
/// runtime. >=2 enables SO_REUSEPORT and kernel-balanced accept across
/// shards. 1 is the single-thread compio path (still io_uring, just
/// no SO_REUSEPORT).
///
/// `dispatch_workers` is the worker-pool size; the daemon binary
/// reads `WMBT_KV_HTTP_DISPATCH_WORKERS` (default `8`).
///
/// # Errors
/// Surfaces bind failures and per-shard runtime-spawn failures.
pub fn serve_http_compio_bridge<F>(
    addr: std::net::SocketAddr,
    tpc_threads: usize,
    dispatch_workers: usize,
    dispatch: F,
) -> std::io::Result<()>
where
    F: Fn(u64, WireRequest) -> WireResponse + Send + Sync + 'static,
{
    let n = tpc_threads.max(1);
    let handle = crate::tcp_transport::spawn_dispatch_workers(dispatch_workers, dispatch);
    println!(
        r#"{{"scope":"wombatkv_http_daemon","event":"compio_bridge_starting","bind":"{addr}","tpc_threads":{n},"dispatch_workers":{dispatch_workers}}}"#
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let mut joins = Vec::with_capacity(n);
    for shard_id in 0..n {
        let handle = handle.clone();
        let shutdown = Arc::clone(&shutdown);
        let join = std::thread::Builder::new()
            .name(format!("wombatkv-http-compio-br-{shard_id}"))
            .spawn(move || {
                let runtime = match compio::runtime::Runtime::new() {
                    Ok(rt) => rt,
                    Err(e) => {
                        eprintln!(
                            r#"{{"scope":"wombatkv_http_daemon","event":"compio_runtime_failed","shard":{shard_id},"error":"{e}"}}"#
                        );
                        return;
                    }
                };
                runtime.block_on(async move {
                    if let Err(e) =
                        serve_compio_http_shard_bridge(shard_id, addr, handle, shutdown).await
                    {
                        eprintln!(
                            r#"{{"scope":"wombatkv_http_daemon","event":"compio_shard_exit","shard":{shard_id},"error":"{e}"}}"#
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

async fn serve_compio_http_shard_bridge(
    shard_id: usize,
    addr: std::net::SocketAddr,
    handle: crate::tcp_transport::DispatchHandle,
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
        r#"{{"scope":"wombatkv_http_daemon","event":"compio_bridge_shard_listening","shard":{shard_id},"bind":"{addr}"}}"#
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
                    r#"{{"scope":"wombatkv_http_daemon","event":"compio_bridge_accepted","shard":{shard_id},"conn_id":{conn_id},"peer":"{peer}"}}"#
                );
                compio::runtime::spawn(async move {
                    if let Err(e) =
                        handle_compio_http_connection_bridge(stream, conn_id, handle).await
                    {
                        eprintln!(
                            r#"{{"scope":"wombatkv_http_daemon","event":"compio_bridge_conn_error","conn_id":{conn_id},"error":"{e}"}}"#
                        );
                    }
                })
                .detach();
            }
            futures::future::Either::Left((Err(e), _)) => {
                eprintln!(
                    r#"{{"scope":"wombatkv_http_daemon","event":"compio_accept_error","shard":{shard_id},"error":"{e}"}}"#
                );
            }
            futures::future::Either::Right(((), _)) => {}
        }
    }
}

async fn handle_compio_http_connection_bridge(
    mut stream: compio::net::TcpStream,
    conn_id: u64,
    handle: crate::tcp_transport::DispatchHandle,
) -> Result<(), HttpServerError> {
    let _ = stream.set_nodelay(true);

    // Persistent accumulator across keep-alive requests on this
    // connection. After processing each request we drain the consumed
    // prefix; any tail (e.g. start of a pipelined next request) stays
    // for the next loop iteration.
    let mut buf: Vec<u8> = Vec::with_capacity(8192);

    loop {
        let body_start = match read_compio_until_double_crlf(&mut stream, &mut buf).await {
            Ok(pos) => pos,
            Err(HttpServerError::Io(e)) if is_peer_disconnect(&e) => return Ok(()),
            Err(e) => return Err(e),
        };

        // Strip the double-CRLF terminator off the head we hand to the parser.
        let head_end = body_start - 4;
        let head = parse_http_head_from_buf(&buf[..head_end])?;
        let RequestHead { method, path, content_length, keep_alive } = head;

        match process_compio_http_route(
            &mut stream,
            &handle,
            conn_id,
            &method,
            &path,
            content_length,
            keep_alive,
            &mut buf,
            body_start,
        )
        .await
        {
            CompioRouteOutcome::ContinueKeepAlive => {}
            CompioRouteOutcome::CloseConnection => return Ok(()),
            CompioRouteOutcome::Error(e) => return Err(e),
        }
    }
}

enum CompioRouteOutcome {
    ContinueKeepAlive,
    CloseConnection,
    Error(HttpServerError),
}

#[allow(clippy::too_many_arguments)]
async fn process_compio_http_route(
    stream: &mut compio::net::TcpStream,
    handle: &crate::tcp_transport::DispatchHandle,
    conn_id: u64,
    method: &str,
    path: &str,
    content_length: Option<usize>,
    keep_alive: bool,
    buf: &mut Vec<u8>,
    body_start: usize,
) -> CompioRouteOutcome {
    // PING, no body
    if method == "GET" && path == PING_PATH {
        buf.drain(..body_start);
        if let Err(e) =
            write_compio_response(stream, 200, "OK", "text/plain", &[], keep_alive).await
        {
            return CompioRouteOutcome::Error(e);
        }
        return if keep_alive {
            CompioRouteOutcome::ContinueKeepAlive
        } else {
            CompioRouteOutcome::CloseConnection
        };
    }

    // Anything other than POST /wmbt/v1/rpc is a 404
    if !(method == "POST" && path == RPC_PATH) {
        buf.drain(..body_start);
        if let Err(e) = write_compio_response(
            stream,
            404,
            "Not Found",
            "text/plain",
            b"unknown route",
            keep_alive,
        )
        .await
        {
            return CompioRouteOutcome::Error(e);
        }
        return if keep_alive {
            CompioRouteOutcome::ContinueKeepAlive
        } else {
            CompioRouteOutcome::CloseConnection
        };
    }

    let Some(len) = content_length else {
        let _ = write_compio_response(
            stream,
            411,
            "Length Required",
            "text/plain",
            b"missing Content-Length",
            false,
        )
        .await;
        return CompioRouteOutcome::CloseConnection;
    };
    if len > MAX_BODY_BYTES {
        let body = format!("body {len} > MAX_BODY_BYTES {MAX_BODY_BYTES}");
        let _ = write_compio_response(
            stream,
            413,
            "Payload Too Large",
            "text/plain",
            body.as_bytes(),
            false,
        )
        .await;
        return CompioRouteOutcome::CloseConnection;
    }

    // Drain head from buf so body starts at buf[0]. Any tail bytes we
    // accidentally read past the head terminator are now at the front
    // (start of the body proper).
    buf.drain(..body_start);

    while buf.len() < len {
        let chunk_size = std::cmp::min(len - buf.len(), 4096);
        let chunk = vec![0u8; chunk_size];
        use compio::io::AsyncRead;
        let compio::BufResult(res, chunk) = stream.read(chunk).await;
        let n = match res {
            Ok(n) => n,
            Err(e) if is_peer_disconnect(&e) => return CompioRouteOutcome::CloseConnection,
            Err(e) => return CompioRouteOutcome::Error(e.into()),
        };
        if n == 0 {
            return CompioRouteOutcome::Error(HttpServerError::Io(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "peer closed mid-body",
            )));
        }
        buf.extend_from_slice(&chunk[..n]);
    }

    // RFC 0018 Phase 3: HTTP body = [envelope 16][rkyv body of envelope.len bytes].
    let req_bytes = match decode_envelope(&buf[..len]) {
        Ok(b) => b,
        Err(e) => {
            return CompioRouteOutcome::Error(HttpServerError::Decode(format!("envelope: {e}")))
        }
    };
    let req = match WireRequest::decode(req_bytes) {
        Ok(r) => r,
        Err(e) => {
            return CompioRouteOutcome::Error(HttpServerError::Decode(format!(
                "WireRequest: {e:?}"
            )))
        }
    };
    buf.drain(..len);

    let response = match handle.dispatch_async(conn_id, req).await {
        Ok(r) => r,
        Err(e) => {
            return CompioRouteOutcome::Error(HttpServerError::Decode(format!(
                "dispatch_async: {e}"
            )))
        }
    };
    let resp_bytes = match WireResponse::encode(&response) {
        Ok(b) => b,
        Err(e) => {
            return CompioRouteOutcome::Error(HttpServerError::Encode(format!(
                "WireResponse: {e:?}"
            )))
        }
    };
    // RFC 0018 Phase 6, transport-slow-write chaos site (HTTP TPC).
    // Brief blocking sleep on the compio shard simulates a slow-write
    // peer + tests client-side timeout discipline. Inert in non-dst
    // builds.
    #[cfg(feature = "dst")]
    if wombatkv_dst::dst_buggify!() {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let wire = encode_envelope(resp_bytes.as_ref());
    if let Err(e) = write_compio_response(stream, 200, "OK", CONTENT_TYPE, &wire, keep_alive).await
    {
        return CompioRouteOutcome::Error(e);
    }
    if keep_alive {
        CompioRouteOutcome::ContinueKeepAlive
    } else {
        CompioRouteOutcome::CloseConnection
    }
}

async fn read_compio_until_double_crlf(
    stream: &mut compio::net::TcpStream,
    buf: &mut Vec<u8>,
) -> Result<usize, HttpServerError> {
    use compio::io::AsyncRead;

    // Already buffered from a prior pipelined request?
    if let Some(pos) = find_double_crlf(buf) {
        return Ok(pos + 4);
    }

    loop {
        if buf.len() > MAX_HEAD_BYTES {
            return Err(HttpServerError::BadRequest(format!(
                "header section exceeds {MAX_HEAD_BYTES} bytes"
            )));
        }
        let chunk = vec![0u8; 4096];
        let compio::BufResult(res, chunk) = stream.read(chunk).await;
        let n = res.map_err(HttpServerError::Io)?;
        if n == 0 {
            return Err(HttpServerError::Io(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "peer closed before head terminator",
            )));
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_double_crlf(buf) {
            return Ok(pos + 4);
        }
    }
}

fn find_double_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Parse a request head from a byte slice (the bytes BEFORE the
/// terminating `\r\n\r\n`). Shared between the std::net path's
/// `read_request_head` and the compio path's accumulator-based reader.
fn parse_http_head_from_buf(head_bytes: &[u8]) -> Result<RequestHead, HttpServerError> {
    let head_str = std::str::from_utf8(head_bytes)
        .map_err(|e| HttpServerError::BadRequest(format!("head not utf-8: {e}")))?;
    let mut lines = head_str.split("\r\n");

    let request_line = lines
        .next()
        .ok_or_else(|| HttpServerError::BadRequest("missing request line".to_string()))?;
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 3 {
        return Err(HttpServerError::BadRequest(format!(
            "malformed request line: {request_line:?}"
        )));
    }
    let method = parts[0].to_string();
    let path = parts[1].to_string();
    let http_version = parts[2];

    let mut content_length: Option<usize> = None;
    let mut connection_header: Option<String> = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some(colon) = line.find(':') else {
            return Err(HttpServerError::BadRequest(format!("malformed header: {line:?}")));
        };
        let name = line[..colon].trim().to_ascii_lowercase();
        let value = line[colon + 1..].trim();
        match name.as_str() {
            "content-length" => {
                content_length = Some(value.parse::<usize>().map_err(|e| {
                    HttpServerError::BadRequest(format!("bad Content-Length {value:?}: {e}"))
                })?);
            }
            "connection" => {
                connection_header = Some(value.to_ascii_lowercase());
            }
            _ => {}
        }
    }

    let keep_alive = match connection_header.as_deref() {
        Some("close") => false,
        Some("keep-alive") => true,
        _ => http_version.eq_ignore_ascii_case("HTTP/1.1"),
    };

    Ok(RequestHead { method, path, content_length, keep_alive })
}

async fn write_compio_response(
    stream: &mut compio::net::TcpStream,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &[u8],
    keep_alive: bool,
) -> Result<(), HttpServerError> {
    use compio::io::AsyncWriteExt;

    let conn = if keep_alive { "keep-alive" } else { "close" };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: {conn}\r\n\
         \r\n",
        body.len(),
    );
    let head_bytes = head.into_bytes();
    let compio::BufResult(res, _) = stream.write_all(head_bytes).await;
    res.map_err(HttpServerError::Io)?;
    if !body.is_empty() {
        let body_owned = body.to_vec();
        let compio::BufResult(res, _) = stream.write_all(body_owned).await;
        res.map_err(HttpServerError::Io)?;
    }
    Ok(())
}

// ============================================================
// Client side
// ============================================================

/// Mirror of `client::RemoteGetOutcome` / `TcpGetOutcome` for the HTTP transport.
#[derive(Debug, Clone)]
pub enum HttpGetOutcome {
    Hit { payload: Bytes },
    Miss,
}

#[derive(Debug)]
pub enum HttpClientError {
    Io(std::io::Error),
    Codec(String),
    /// HTTP-level failure (non-200 status or malformed response).
    Http {
        status: u16,
        message: String,
    },
    /// Server returned a non-OK / non-MISS WireResponse status.
    Status {
        code: u8,
        message: String,
    },
}

impl std::fmt::Display for HttpClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpClientError::Io(e) => write!(f, "http client io: {e}"),
            HttpClientError::Codec(m) => write!(f, "http client codec: {m}"),
            HttpClientError::Http { status, message } => {
                write!(f, "http client http status={status} msg={message:?}")
            }
            HttpClientError::Status { code, message } => {
                write!(f, "http client server status code={code} msg={message:?}")
            }
        }
    }
}

impl std::error::Error for HttpClientError {}

impl From<std::io::Error> for HttpClientError {
    fn from(e: std::io::Error) -> Self {
        HttpClientError::Io(e)
    }
}

/// Sync HTTP/1.1 client for a `wombatkv-daemon` HTTP listener.
///
/// One persistent keep-alive `TcpStream` per client; all RPCs
/// serialize through a `Mutex`. For real concurrency, clone the
/// client per thread (each instance gets its own TCP connection).
pub struct HttpKvClient {
    stream: Mutex<TcpStream>,
    host_header: String,
    next_id: AtomicU64,
}

impl HttpKvClient {
    /// Connect to a remote `wombatkv-daemon` HTTP listener.
    pub fn connect<A: ToSocketAddrs + std::fmt::Display>(addr: A) -> Result<Self, HttpClientError> {
        Self::connect_with_timeout(addr, Duration::from_secs(10))
    }

    pub fn connect_with_timeout<A: ToSocketAddrs + std::fmt::Display>(
        addr: A,
        timeout: Duration,
    ) -> Result<Self, HttpClientError> {
        let host_header = format!("{addr}");
        let addrs = addr.to_socket_addrs().map_err(HttpClientError::Io)?.collect::<Vec<_>>();
        if addrs.is_empty() {
            return Err(HttpClientError::Io(std::io::Error::new(
                ErrorKind::AddrNotAvailable,
                "no socket addresses resolved",
            )));
        }
        let mut last_err: Option<std::io::Error> = None;
        for sa in addrs {
            match TcpStream::connect_timeout(&sa, timeout) {
                Ok(stream) => {
                    stream.set_nodelay(true)?;
                    return Ok(Self {
                        stream: Mutex::new(stream),
                        host_header,
                        next_id: AtomicU64::new(1),
                    });
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(HttpClientError::Io(
            last_err.unwrap_or_else(|| std::io::Error::other("connect failed")),
        ))
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    fn roundtrip(&self, req: WireRequest) -> Result<WireResponse, HttpClientError> {
        let req_bytes = WireRequest::encode(&req)
            .map_err(|e| HttpClientError::Codec(format!("encode req: {e:?}")))?;
        // RFC 0018 Phase 3: wrap the rkyv body in the universal envelope
        // before sending as the HTTP request body.
        let wire = encode_envelope(req_bytes.as_ref());
        let head = format!(
            "POST {RPC_PATH} HTTP/1.1\r\n\
             Host: {host}\r\n\
             Content-Type: {CONTENT_TYPE}\r\n\
             Content-Length: {len}\r\n\
             Connection: keep-alive\r\n\
             \r\n",
            host = self.host_header,
            len = wire.len(),
        );

        let mut stream = self.stream.lock().expect("http stream mutex poisoned");
        stream.write_all(head.as_bytes())?;
        stream.write_all(&wire)?;
        stream.flush()?;

        let read_stream = stream.try_clone()?;
        let mut reader = BufReader::new(read_stream);
        let (status, content_length, _keep_alive) = read_response_head(&mut reader)?;
        let mut resp_body = vec![0u8; content_length];
        reader.read_exact(&mut resp_body)?;
        drop(stream);

        if status != 200 {
            let message = String::from_utf8_lossy(&resp_body).into_owned();
            return Err(HttpClientError::Http { status, message });
        }
        let resp_bytes = decode_envelope(&resp_body)?;
        WireResponse::decode(resp_bytes)
            .map_err(|e| HttpClientError::Codec(format!("decode resp: {e:?}")))
    }

    /// PUT a payload under (namespace, key). Mirrors
    /// `TcpKvClient::put_kv`.
    pub fn put_kv(
        &self,
        namespace: &str,
        key: &str,
        payload: Bytes,
    ) -> Result<(), HttpClientError> {
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
            Err(HttpClientError::Status { code: resp.status, message: resp.message })
        }
    }

    /// GET payload by (namespace, key). `HttpGetOutcome::Miss` on
    /// daemon-reported miss; other errors bubble up.
    pub fn get_kv(&self, namespace: &str, key: &str) -> Result<HttpGetOutcome, HttpClientError> {
        let req = WireRequest {
            id: self.next_id(),
            op: op::GET,
            namespace: namespace.to_string(),
            key: key.to_string(),
            payload: Vec::new(),
        };
        let resp = self.roundtrip(req)?;
        match resp.status {
            s if s == status::OK => Ok(HttpGetOutcome::Hit { payload: Bytes::from(resp.payload) }),
            s if s == status::MISS => Ok(HttpGetOutcome::Miss),
            other => Err(HttpClientError::Status { code: other, message: resp.message }),
        }
    }

    /// Block-shaped: count leading content-addressed blocks present
    /// in the daemon's metadata index.
    pub fn lookup_block_prefix(
        &self,
        namespace: &str,
        block_hashes_hex: &[String],
    ) -> Result<usize, HttpClientError> {
        let req_payload = encode_lookup_block_prefix_req(&LookupBlockPrefixReq {
            namespace: namespace.to_string(),
            block_hashes_hex: block_hashes_hex.to_vec(),
        })
        .map_err(|e| HttpClientError::Codec(e.to_string()))?;
        let req = WireRequest {
            id: self.next_id(),
            op: op::LOOKUP_BLOCK_PREFIX,
            namespace: namespace.to_string(),
            key: String::new(),
            payload: req_payload,
        };
        let resp = self.roundtrip(req)?;
        if resp.status != status::OK {
            return Err(HttpClientError::Status { code: resp.status, message: resp.message });
        }
        let decoded = decode_lookup_block_prefix_resp(&resp.payload)
            .map_err(|e| HttpClientError::Codec(e.to_string()))?;
        if let Some(err) = decoded.error {
            return Err(HttpClientError::Status { code: status::ERROR, message: err });
        }
        Ok(decoded.matched_count as usize)
    }

    /// Block-shaped: parallel batched GET. `Some(payloads)` on full
    /// hit, `None` on any miss (all-or-nothing semantics matching the
    /// cabi `wmbt_kv_get_kv_blocks_borrowed` contract).
    pub fn get_kv_blocks_batch(
        &self,
        namespace: &str,
        block_hashes_hex: &[String],
    ) -> Result<Option<Vec<Bytes>>, HttpClientError> {
        let req_payload = encode_get_kv_blocks_batch_req(&GetKvBlocksBatchReq {
            namespace: namespace.to_string(),
            block_hashes_hex: block_hashes_hex.to_vec(),
        })
        .map_err(|e| HttpClientError::Codec(e.to_string()))?;
        let req = WireRequest {
            id: self.next_id(),
            op: op::GET_KV_BLOCKS_BATCH,
            namespace: namespace.to_string(),
            key: String::new(),
            payload: req_payload,
        };
        let resp = self.roundtrip(req)?;
        if resp.status != status::OK {
            return Err(HttpClientError::Status { code: resp.status, message: resp.message });
        }
        let decoded = decode_get_kv_blocks_batch_resp(&resp.payload)
            .map_err(|e| HttpClientError::Codec(e.to_string()))?;
        if let Some(err) = decoded.error {
            return Err(HttpClientError::Status { code: status::ERROR, message: err });
        }
        Ok(decoded.payloads.map(|items| items.into_iter().map(Bytes::from).collect()))
    }

    /// Block-shaped: parallel batched PUT.
    pub fn put_kv_blocks_batch(
        &self,
        namespace: &str,
        block_hashes_hex: &[String],
        payloads: &[&[u8]],
    ) -> Result<u64, HttpClientError> {
        if block_hashes_hex.len() != payloads.len() {
            return Err(HttpClientError::Codec(format!(
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
        .map_err(|e| HttpClientError::Codec(e.to_string()))?;
        let req = WireRequest {
            id: self.next_id(),
            op: op::PUT_KV_BLOCKS_BATCH,
            namespace: namespace.to_string(),
            key: String::new(),
            payload: req_payload,
        };
        let resp = self.roundtrip(req)?;
        if resp.status != status::OK {
            return Err(HttpClientError::Status { code: resp.status, message: resp.message });
        }
        let decoded = decode_put_kv_blocks_batch_resp(&resp.payload)
            .map_err(|e| HttpClientError::Codec(e.to_string()))?;
        if let Some(err) = decoded.error {
            return Err(HttpClientError::Status { code: status::ERROR, message: err });
        }
        Ok(decoded.total_bytes)
    }

    /// Ping the daemon. Useful as a connection liveness probe.
    pub fn ping(&self) -> Result<(), HttpClientError> {
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
            Err(HttpClientError::Status { code: resp.status, message: resp.message })
        }
    }
}

fn read_response_head(
    reader: &mut BufReader<TcpStream>,
) -> Result<(u16, usize, bool), HttpClientError> {
    let mut status_line = String::new();
    let n = reader.read_line(&mut status_line).map_err(HttpClientError::Io)?;
    if n == 0 {
        return Err(HttpClientError::Io(std::io::Error::new(
            ErrorKind::UnexpectedEof,
            "peer closed before response",
        )));
    }
    let parts: Vec<&str> = status_line.splitn(3, ' ').collect();
    if parts.len() < 2 {
        return Err(HttpClientError::Codec(format!(
            "malformed status line: {:?}",
            status_line.trim()
        )));
    }
    let http_version = parts[0];
    let status: u16 = parts[1]
        .parse()
        .map_err(|e| HttpClientError::Codec(format!("bad status code {:?}: {e}", parts[1])))?;

    let mut content_length: Option<usize> = None;
    let mut connection_header: Option<String> = None;
    let mut head_bytes = n;
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).map_err(HttpClientError::Io)?;
        if n == 0 {
            return Err(HttpClientError::Io(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "peer closed mid-response-headers",
            )));
        }
        head_bytes += n;
        if head_bytes > MAX_HEAD_BYTES {
            return Err(HttpClientError::Codec(format!(
                "response head exceeds {MAX_HEAD_BYTES} bytes"
            )));
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        let Some(colon) = trimmed.find(':') else {
            return Err(HttpClientError::Codec(format!("malformed response header: {trimmed:?}")));
        };
        let name = trimmed[..colon].trim().to_ascii_lowercase();
        let value = trimmed[colon + 1..].trim();
        match name.as_str() {
            "content-length" => {
                content_length = Some(value.parse::<usize>().map_err(|e| {
                    HttpClientError::Codec(format!("bad Content-Length {value:?}: {e}"))
                })?);
            }
            "connection" => {
                connection_header = Some(value.to_ascii_lowercase());
            }
            _ => {}
        }
    }
    let len = content_length
        .ok_or_else(|| HttpClientError::Codec("response missing Content-Length".to_string()))?;
    if len > MAX_BODY_BYTES {
        return Err(HttpClientError::Codec(format!(
            "response Content-Length {len} > MAX_BODY_BYTES {MAX_BODY_BYTES}"
        )));
    }
    let keep_alive = match connection_header.as_deref() {
        Some("close") => false,
        Some("keep-alive") => true,
        _ => http_version.eq_ignore_ascii_case("HTTP/1.1"),
    };
    Ok((status, len, keep_alive))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// alpha.11+1: std::net path deleted; tests use compio TPC server.
    /// Returns (addr, conn_count_placeholder) for back-compat with
    /// existing tests that read but don't strictly require the count.
    fn boot_server<F>(dispatch: F) -> (std::net::SocketAddr, Arc<AtomicUsize>)
    where
        F: Fn(u64, WireRequest) -> WireResponse + Send + Sync + 'static,
    {
        let addr = boot_tpc_server(1, 2, dispatch);
        (addr, Arc::new(AtomicUsize::new(0)))
    }

    #[test]
    fn ping_roundtrip() {
        let (addr, _conn_count) = boot_server(|_cid, req| {
            assert_eq!(req.op, op::PING);
            WireResponse {
                id: req.id,
                status: status::OK,
                op: req.op,
                payload: Vec::new(),
                message: String::new(),
            }
        });
        let client = HttpKvClient::connect(addr).unwrap();
        client.ping().unwrap();
        client.ping().unwrap();
        client.ping().unwrap();
    }

    #[test]
    fn put_then_get_roundtrip() {
        use std::collections::HashMap;
        use std::sync::Mutex as StdMutex;
        let store: Arc<StdMutex<HashMap<String, Vec<u8>>>> =
            Arc::new(StdMutex::new(HashMap::new()));
        let store_clone = Arc::clone(&store);
        let (addr, _conn_count) = boot_server(move |_cid, req| {
            let key = format!("{}::{}", req.namespace, req.key);
            match req.op {
                op::PUT => {
                    store_clone.lock().unwrap().insert(key, req.payload);
                    WireResponse {
                        id: req.id,
                        status: status::OK,
                        op: req.op,
                        payload: Vec::new(),
                        message: String::new(),
                    }
                }
                op::GET => {
                    let map = store_clone.lock().unwrap();
                    match map.get(&key) {
                        Some(v) => WireResponse {
                            id: req.id,
                            status: status::OK,
                            op: req.op,
                            payload: v.clone(),
                            message: String::new(),
                        },
                        None => WireResponse {
                            id: req.id,
                            status: status::MISS,
                            op: req.op,
                            payload: Vec::new(),
                            message: String::new(),
                        },
                    }
                }
                _ => WireResponse {
                    id: req.id,
                    status: status::ERROR,
                    op: req.op,
                    payload: Vec::new(),
                    message: "unexpected op".to_string(),
                },
            }
        });

        let client = HttpKvClient::connect(addr).unwrap();
        client.put_kv("ns", "k1", Bytes::from_static(b"hello")).unwrap();
        client.put_kv("ns", "k2", Bytes::from_static(&[0u8; 4096])).unwrap();
        match client.get_kv("ns", "k1").unwrap() {
            HttpGetOutcome::Hit { payload } => assert_eq!(payload.as_ref(), b"hello"),
            HttpGetOutcome::Miss => panic!("k1 should hit"),
        }
        match client.get_kv("ns", "k2").unwrap() {
            HttpGetOutcome::Hit { payload } => assert_eq!(payload.len(), 4096),
            HttpGetOutcome::Miss => panic!("k2 should hit"),
        }
        match client.get_kv("ns", "missing").unwrap() {
            HttpGetOutcome::Hit { .. } => panic!("missing should miss"),
            HttpGetOutcome::Miss => {}
        }
    }

    #[test]
    fn ping_endpoint_returns_200() {
        let (addr, _) = boot_server(|_cid, req| WireResponse {
            id: req.id,
            status: status::OK,
            op: req.op,
            payload: Vec::new(),
            message: String::new(),
        });
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(b"GET /wmbt/v1/ping HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();
        let mut buf = [0u8; 256];
        let n = stream.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(s.starts_with("HTTP/1.1 200 OK\r\n"), "got: {s:?}");
    }

    #[test]
    fn unknown_route_returns_404() {
        let (addr, _) = boot_server(|_cid, req| WireResponse {
            id: req.id,
            status: status::OK,
            op: req.op,
            payload: Vec::new(),
            message: String::new(),
        });
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(b"GET /bogus HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();
        let mut buf = [0u8; 256];
        let n = stream.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(s.starts_with("HTTP/1.1 404 Not Found\r\n"), "got: {s:?}");
    }

    // ====================================================
    // TPC (compio-bridge) tests
    // ====================================================
    //
    // These exercise `serve_http_compio_bridge`. They share a port
    // allocation strategy: bind an ephemeral TcpListener, capture
    // its addr, drop it, then start the TPC server on that addr.
    // SO_REUSEPORT (set by serve_compio_http_shard_bridge) covers
    // the brief race window.

    fn pick_ephemeral_addr() -> std::net::SocketAddr {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        addr
    }

    fn boot_tpc_server<F>(
        tpc_threads: usize,
        dispatch_workers: usize,
        dispatch: F,
    ) -> std::net::SocketAddr
    where
        F: Fn(u64, WireRequest) -> WireResponse + Send + Sync + 'static,
    {
        let addr = pick_ephemeral_addr();
        std::thread::spawn(move || {
            let _ = serve_http_compio_bridge(addr, tpc_threads, dispatch_workers, dispatch);
        });
        // Give shards time to bind. Use myelon's discovery-poll primitive
        // so the wait matches the rest of the transport stack (env-controlled
        // duration via myelon's wait config).
        for _ in 0..6 {
            myelon::perform_default_discovery_poll_wait();
        }
        addr
    }

    #[test]
    fn http_tpc_ping_roundtrip() {
        let addr = boot_tpc_server(1, 2, |_cid, req| {
            assert_eq!(req.op, op::PING);
            WireResponse {
                id: req.id,
                status: status::OK,
                op: req.op,
                payload: Vec::new(),
                message: String::new(),
            }
        });
        let client = HttpKvClient::connect(addr).unwrap();
        client.ping().unwrap();
        client.ping().unwrap();
        client.ping().unwrap();
    }

    #[test]
    fn http_tpc_put_then_get_roundtrip() {
        use std::collections::HashMap;
        use std::sync::Mutex as StdMutex;
        let store: Arc<StdMutex<HashMap<String, Vec<u8>>>> =
            Arc::new(StdMutex::new(HashMap::new()));
        let store_clone = Arc::clone(&store);
        let addr = boot_tpc_server(1, 2, move |_cid, req| {
            let key = format!("{}::{}", req.namespace, req.key);
            match req.op {
                op::PUT => {
                    store_clone.lock().unwrap().insert(key, req.payload);
                    WireResponse {
                        id: req.id,
                        status: status::OK,
                        op: req.op,
                        payload: Vec::new(),
                        message: String::new(),
                    }
                }
                op::GET => {
                    let map = store_clone.lock().unwrap();
                    match map.get(&key) {
                        Some(v) => WireResponse {
                            id: req.id,
                            status: status::OK,
                            op: req.op,
                            payload: v.clone(),
                            message: String::new(),
                        },
                        None => WireResponse {
                            id: req.id,
                            status: status::MISS,
                            op: req.op,
                            payload: Vec::new(),
                            message: String::new(),
                        },
                    }
                }
                _ => WireResponse {
                    id: req.id,
                    status: status::ERROR,
                    op: req.op,
                    payload: Vec::new(),
                    message: "unexpected op".to_string(),
                },
            }
        });

        let client = HttpKvClient::connect(addr).unwrap();
        client.put_kv("ns", "k1", Bytes::from_static(b"hello")).unwrap();
        client.put_kv("ns", "k2", Bytes::from_static(&[0u8; 4096])).unwrap();
        match client.get_kv("ns", "k1").unwrap() {
            HttpGetOutcome::Hit { payload } => assert_eq!(payload.as_ref(), b"hello"),
            HttpGetOutcome::Miss => panic!("k1 should hit"),
        }
        match client.get_kv("ns", "k2").unwrap() {
            HttpGetOutcome::Hit { payload } => assert_eq!(payload.len(), 4096),
            HttpGetOutcome::Miss => panic!("k2 should hit"),
        }
        match client.get_kv("ns", "missing").unwrap() {
            HttpGetOutcome::Hit { .. } => panic!("missing should miss"),
            HttpGetOutcome::Miss => {}
        }
    }

    #[test]
    fn http_tpc_concurrent_clients_correctness() {
        // The value-add test for TPC: N clients hit the server
        // simultaneously. Each does a series of put + get + verify.
        // We're checking correctness under concurrent load, not perf -
        // perf is captured separately via the daemon binary.
        use std::collections::HashMap;
        use std::sync::Mutex as StdMutex;
        let store: Arc<StdMutex<HashMap<String, Vec<u8>>>> =
            Arc::new(StdMutex::new(HashMap::new()));
        let store_clone = Arc::clone(&store);
        // 2 TPC shards + 4 dispatch workers = realistic minimal TPC.
        let addr = boot_tpc_server(2, 4, move |_cid, req| {
            let key = format!("{}::{}", req.namespace, req.key);
            match req.op {
                op::PUT => {
                    store_clone.lock().unwrap().insert(key, req.payload);
                    WireResponse {
                        id: req.id,
                        status: status::OK,
                        op: req.op,
                        payload: Vec::new(),
                        message: String::new(),
                    }
                }
                op::GET => {
                    let map = store_clone.lock().unwrap();
                    match map.get(&key) {
                        Some(v) => WireResponse {
                            id: req.id,
                            status: status::OK,
                            op: req.op,
                            payload: v.clone(),
                            message: String::new(),
                        },
                        None => WireResponse {
                            id: req.id,
                            status: status::MISS,
                            op: req.op,
                            payload: Vec::new(),
                            message: String::new(),
                        },
                    }
                }
                _ => WireResponse {
                    id: req.id,
                    status: status::ERROR,
                    op: req.op,
                    payload: Vec::new(),
                    message: "unexpected op".to_string(),
                },
            }
        });

        let n_clients: usize = 8;
        let n_requests: usize = 25;
        let mut handles = Vec::with_capacity(n_clients);
        for client_id in 0..n_clients {
            let h = std::thread::spawn(move || {
                let client = HttpKvClient::connect(addr).unwrap();
                for req_id in 0..n_requests {
                    let key = format!("c{client_id}-r{req_id}");
                    let payload_str = format!("payload-c{client_id}-r{req_id}");
                    let payload_bytes = payload_str.as_bytes().to_vec();
                    client
                        .put_kv("ns", &key, Bytes::from(payload_bytes.clone()))
                        .unwrap_or_else(|e| panic!("client {client_id} put {key}: {e}"));
                    match client
                        .get_kv("ns", &key)
                        .unwrap_or_else(|e| panic!("client {client_id} get {key}: {e}"))
                    {
                        HttpGetOutcome::Hit { payload } => {
                            assert_eq!(
                                payload.as_ref(),
                                payload_bytes.as_slice(),
                                "client {client_id} req {req_id} payload mismatch"
                            );
                        }
                        HttpGetOutcome::Miss => {
                            panic!("client {client_id} req {req_id} unexpected miss");
                        }
                    }
                }
            });
            handles.push(h);
        }
        for (i, h) in handles.into_iter().enumerate() {
            h.join().unwrap_or_else(|_| panic!("client {i} panicked"));
        }

        // Verify total store has expected count: n_clients * n_requests
        let final_store = store.lock().unwrap();
        assert_eq!(
            final_store.len(),
            n_clients * n_requests,
            "expected {} keys, got {}",
            n_clients * n_requests,
            final_store.len()
        );
    }

    #[test]
    fn http_tpc_keep_alive_pipelined() {
        // Single client, many sequential requests on one keep-alive
        // connection. Verifies the per-connection accumulator-buffer
        // logic correctly handles back-to-back requests without losing
        // bytes between them.
        let addr = boot_tpc_server(1, 2, |_cid, req| {
            assert_eq!(req.op, op::PING);
            WireResponse {
                id: req.id,
                status: status::OK,
                op: req.op,
                payload: Vec::new(),
                message: String::new(),
            }
        });
        let client = HttpKvClient::connect(addr).unwrap();
        for _ in 0..50 {
            client.ping().unwrap();
        }
    }

    // ====================================================
    // Negative-path tests: RFC 0018 envelope rejection on the wire
    // ====================================================
    //
    // The envelope module's own unit tests prove encode/decode is
    // correct in isolation. These tests prove the WIRE layer actually
    // gates on the envelope checks, a malicious peer can't smuggle a
    // tampered or wrong-version envelope past the server.

    /// Hand-rolled HTTP request crafter, we need raw byte control to
    /// inject specific malformed envelopes. HttpKvClient always
    /// produces correct envelopes so we bypass it.
    fn raw_http_post_rpc(stream: &mut TcpStream, host: &str, body: &[u8]) {
        let head = format!(
            "POST {RPC_PATH} HTTP/1.1\r\n\
             Host: {host}\r\n\
             Content-Type: {CONTENT_TYPE}\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
        stream.flush().unwrap();
    }

    fn craft_tampered_envelope_body(tamper: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        // Encode a valid PING request, wrap in envelope, then tamper.
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

    /// Helper: boot a sync HTTP server (not TPC), return addr.
    fn boot_sync_server<F>(dispatch: F) -> std::net::SocketAddr
    where
        F: Fn(u64, WireRequest) -> WireResponse + Send + Sync + 'static,
    {
        // alpha.11+1: std::net path deleted; corruption tests run against
        // the TPC server. Same envelope + drop-on-bad-frame behavior; this
        // is the path that's actually production now.
        boot_tpc_server(1, 2, dispatch)
    }

    #[test]
    fn http_sync_rejects_envelope_bad_magic() {
        let addr = boot_sync_server(|_cid, req| WireResponse {
            id: req.id,
            status: status::OK,
            op: req.op,
            payload: Vec::new(),
            message: String::new(),
        });
        let tampered = craft_tampered_envelope_body(|wire| {
            wire[0] = b'X'; // clobber 'W' in 'WMBT'
        });
        let mut stream = TcpStream::connect(addr).unwrap();
        raw_http_post_rpc(&mut stream, "127.0.0.1", &tampered);
        // Server should close the connection without producing a 200 OK
        // response. Read whatever it sends and confirm not a 200.
        let mut resp = String::new();
        let _ = stream.read_to_string(&mut resp);
        // Server may either drop the connection (empty resp) or send
        // 500/400, both signal that the envelope was rejected. The
        // critical contract: no 200 OK with poisoned response body.
        assert!(
            !resp.starts_with("HTTP/1.1 200 OK"),
            "tampered envelope should NOT yield 200 OK; got {resp:?}"
        );
    }

    #[test]
    fn http_sync_rejects_envelope_bad_version() {
        let addr = boot_sync_server(|_cid, req| WireResponse {
            id: req.id,
            status: status::OK,
            op: req.op,
            payload: Vec::new(),
            message: String::new(),
        });
        let tampered = craft_tampered_envelope_body(|wire| {
            wire[4] = 99; // version byte 0
        });
        let mut stream = TcpStream::connect(addr).unwrap();
        raw_http_post_rpc(&mut stream, "127.0.0.1", &tampered);
        let mut resp = String::new();
        let _ = stream.read_to_string(&mut resp);
        assert!(
            !resp.starts_with("HTTP/1.1 200 OK"),
            "wrong-version envelope should NOT yield 200 OK; got {resp:?}"
        );
    }

    #[test]
    fn http_sync_rejects_envelope_bad_crc() {
        let addr = boot_sync_server(|_cid, req| WireResponse {
            id: req.id,
            status: status::OK,
            op: req.op,
            payload: Vec::new(),
            message: String::new(),
        });
        let tampered = craft_tampered_envelope_body(|wire| {
            // Flip a byte inside the body (after the 16-byte envelope).
            wire[20] ^= 0xff;
        });
        let mut stream = TcpStream::connect(addr).unwrap();
        raw_http_post_rpc(&mut stream, "127.0.0.1", &tampered);
        let mut resp = String::new();
        let _ = stream.read_to_string(&mut resp);
        assert!(
            !resp.starts_with("HTTP/1.1 200 OK"),
            "body-tampered envelope should NOT yield 200 OK; got {resp:?}"
        );
    }

    #[test]
    fn http_sync_rejects_envelope_truncated() {
        let addr = boot_sync_server(|_cid, req| WireResponse {
            id: req.id,
            status: status::OK,
            op: req.op,
            payload: Vec::new(),
            message: String::new(),
        });
        let valid_wire = craft_tampered_envelope_body(|_| {});
        let truncated = &valid_wire[..valid_wire.len() - 1]; // strip last byte
        let mut stream = TcpStream::connect(addr).unwrap();
        raw_http_post_rpc(&mut stream, "127.0.0.1", truncated);
        let mut resp = String::new();
        let _ = stream.read_to_string(&mut resp);
        assert!(
            !resp.starts_with("HTTP/1.1 200 OK"),
            "truncated envelope should NOT yield 200 OK; got {resp:?}"
        );
    }

    #[test]
    fn http_sync_oversized_body_rejected() {
        let addr = boot_sync_server(|_cid, req| WireResponse {
            id: req.id,
            status: status::OK,
            op: req.op,
            payload: Vec::new(),
            message: String::new(),
        });
        // Build an HTTP request whose Content-Length exceeds MAX_BODY_BYTES.
        // Use a small body but lie about the length in the header.
        let head = format!(
            "POST {RPC_PATH} HTTP/1.1\r\n\
             Host: 127.0.0.1\r\n\
             Content-Type: {CONTENT_TYPE}\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n",
            MAX_BODY_BYTES + 1
        );
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(head.as_bytes()).unwrap();
        stream.flush().unwrap();
        let mut resp = String::new();
        let _ = stream.read_to_string(&mut resp);
        // Server should send 413 Payload Too Large before reading the body.
        assert!(
            resp.starts_with("HTTP/1.1 413"),
            "oversized Content-Length should yield 413; got {resp:?}"
        );
    }
}
