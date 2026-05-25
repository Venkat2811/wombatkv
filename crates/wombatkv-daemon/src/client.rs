#![deny(unsafe_code)]
//! Remote KV-store client over the SHM ring transport.
//!
//! Mirrors the surface of [`wombatkv_node::embed::WombatKVKvStore`] so the
//! same engine code can call either an in-process store (embedded) or a
//! co-located daemon by choosing which type to construct at startup.
//!
//! # Threading
//!
//! The myelon SHM transport types stay on a dedicated worker thread with
//! an enlarged stack, the same pattern the bench uses. All public methods
//! send a command over a `std::sync::mpsc` channel, the worker performs
//! the publish/recv, and replies on a one-shot reply channel.
//!
//! Concurrency: methods are `&self` and may be called from any thread,
//! including from inside a tokio runtime. The worker serializes calls so
//! the daemon sees a clean request/response stream.
//!
//! # Liveness (liveness hardening, 2026-04-30)
//!
//! Every public method waits on its reply channel with a configurable
//! timeout (default `DEFAULT_CALL_TIMEOUT` = 30 s). If the timeout fires
//! the call returns [`RemoteError::Timeout`] and the client transitions
//! to a "dead" state; subsequent calls fast-fail with
//! [`RemoteError::BackendUnavailable`] instead of queuing up behind a
//! stuck worker. The worker thread itself is leaked when the daemon
//! truly dies (it stays blocked on `recv_owned`); the client process
//! pays for one zombie thread per dead daemon, which is acceptable
//! given that engines typically have a single long-lived client and
//! daemon failures are rare.
//!
//! Reconnect today is "drop the client, build a new one", the engine
//! decides when to retry. Transparent reconnect is a future enhancement.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use bytes::Bytes;

use myelon::typed_transport::{TypedConsumer, TypedProducer};

use crate::{
    decode_bytes_batch, decode_get_kv_blocks_batch_resp, decode_lookup_block_prefix_resp,
    decode_put_kv_blocks_batch_resp, effective_attach_timeout, encode_bytes_batch,
    encode_get_kv_blocks_batch_req, encode_key_batch, encode_lookup_block_prefix_req,
    encode_put_kv_blocks_batch_req, fits_one_frame, op as op_codes, segment_names, status,
    ArenaReader, ClientHeartbeat, GetKvBlocksBatchReq, LookupBlockPrefixReq, PutKvBlocksBatchReq,
    ShmFrame, WireRequest, WireResponse, DEFAULT_RING_DEPTH, FRAME_DATA_BYTES, REQ_CONSUMER_ID,
};

/// 128 MiB worker stack, same as the daemon worker.
const WORKER_STACK_BYTES: usize = 128 * 1024 * 1024;

const LARGE_MANIFEST_MAGIC: &str = "WMBT_KV_SHM_LARGE_V1";
const LARGE_CHUNK_KEY_PREFIX: &str = "__wmbt_kv_shm_large";
const LARGE_CHUNK_BYTES: usize = FRAME_DATA_BYTES - 4096;
const ARENA_MESSAGE_PREFIX: &str = "arena:";
const ARENA_PATH_ENV: &str = "WMBT_KV_DAEMON_SHM_ARENA_PATH";

/// Default deadline for a single client call. Longer than any reasonable
/// daemon-side foyer/S3 round-trip; if it fires the daemon is considered
/// non-responsive and the client transitions to dead state. Tunable via
/// [`ClientOptions::call_timeout`].
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Tier the daemon served the value from. Mirrors `wombatkv_node::embed::HitTier`
/// without taking a wombatkv-node dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteHitTier {
    Foyer,
    ObjectStore,
}

/// Outcome of `get_kv`. Mirrors `wombatkv_node::embed::GetOutcome`.
#[derive(Debug, Clone)]
pub enum RemoteGetOutcome {
    Hit { tier: RemoteHitTier, payload: Bytes },
    Miss,
}

/// Errors surfaced by the client.
#[derive(Debug)]
pub enum RemoteError {
    /// Failed to attach to the SHM ring. Daemon may not be running.
    Connect(String),
    /// Worker thread panicked or quit unexpectedly.
    WorkerGone,
    /// Transport-level error during a single request.
    Transport(String),
    /// Daemon returned a non-OK status.
    DaemonStatus { status: u8, message: String },
    /// Reply did not arrive within the configured call timeout. The
    /// daemon may be dead, hung, or under such heavy load that requests
    /// can no longer progress. The client transitions to dead state on
    /// the first timeout, subsequent calls fast-fail with
    /// [`Self::BackendUnavailable`] until the engine drops + reconnects.
    Timeout(Duration),
    /// The client previously timed out and is now in dead state.
    /// Engine should drop this handle and build a new one.
    BackendUnavailable,
}

impl std::fmt::Display for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(m) => write!(f, "remote connect: {m}"),
            Self::WorkerGone => write!(f, "remote worker thread gone"),
            Self::Transport(m) => write!(f, "remote transport: {m}"),
            Self::DaemonStatus { status, message } => {
                write!(f, "daemon status {status}: {message}")
            }
            Self::Timeout(d) => write!(f, "remote call timed out after {d:?}"),
            Self::BackendUnavailable => {
                write!(f, "remote backend unavailable (client in dead state)")
            }
        }
    }
}

impl std::error::Error for RemoteError {}

/// Tunable options for [`RemoteKvStoreClient`].
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Per-call deadline. Defaults to [`DEFAULT_CALL_TIMEOUT`].
    pub call_timeout: Duration,
    /// Ring depth must match the daemon's. Defaults to
    /// [`DEFAULT_RING_DEPTH`].
    pub depth: usize,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self { call_timeout: DEFAULT_CALL_TIMEOUT, depth: DEFAULT_RING_DEPTH }
    }
}

/// Commands sent from public methods to the worker thread.
enum Cmd {
    Put {
        namespace: String,
        key: String,
        payload: Bytes,
        reply: Sender<Result<(), RemoteError>>,
    },
    Get {
        namespace: String,
        key: String,
        reply: Sender<Result<RemoteGetOutcome, RemoteError>>,
    },
    GetMany {
        namespace: String,
        keys: Vec<String>,
        reply: Sender<Result<Option<Bytes>, RemoteError>>,
    },
    Exists {
        namespace: String,
        key: String,
        reply: Sender<Result<bool, RemoteError>>,
    },
    List {
        namespace: String,
        reply: Sender<Result<Bytes, RemoteError>>,
    },
    Restore {
        namespace: String,
        reply: Sender<Result<usize, RemoteError>>,
    },
    Clear {
        reply: Sender<Result<(), RemoteError>>,
    },
    Ping {
        reply: Sender<Result<(), RemoteError>>,
    },
    Stats {
        reply: Sender<Result<String, RemoteError>>,
    },
    /// Block-shaped: count leading hashes present in the daemon's
    /// metadata index. See [`RemoteKvStoreClient::lookup_block_prefix`].
    LookupBlockPrefix {
        namespace: String,
        block_hashes_hex: Vec<String>,
        reply: Sender<Result<usize, RemoteError>>,
    },
    /// Block-shaped: parallel batched GET. See
    /// [`RemoteKvStoreClient::get_kv_blocks_batch`].
    GetKvBlocksBatch {
        namespace: String,
        block_hashes_hex: Vec<String>,
        reply: Sender<Result<Option<Vec<Bytes>>, RemoteError>>,
    },
    /// Block-shaped: parallel batched PUT + daemon-side metadata index
    /// update. See [`RemoteKvStoreClient::put_kv_blocks_batch`].
    PutKvBlocksBatch {
        namespace: String,
        block_hashes_hex: Vec<String>,
        payloads: Vec<Vec<u8>>,
        reply: Sender<Result<u64, RemoteError>>,
    },
    Shutdown,
}

/// Remote KV-store client. Holds a worker thread that owns the SHM ring
/// endpoints; drop the client to stop the worker.
pub struct RemoteKvStoreClient {
    cmd_tx: Sender<Cmd>,
    worker: Option<JoinHandle<()>>,
    /// Per-call deadline. Captured at connect time and used by every
    /// public method's `recv_timeout`.
    call_timeout: Duration,
    /// Set to `true` on the first [`RemoteError::Timeout`] from any
    /// public method. Once set, all subsequent calls fast-fail with
    /// [`RemoteError::BackendUnavailable`] without queuing a Cmd. The
    /// flag is shared with the worker so it can also observe shutdown.
    dead: Arc<AtomicBool>,
}

impl RemoteKvStoreClient {
    /// Connect to a daemon running with the given `prefix`. Uses the
    /// default ring depth (`DEFAULT_RING_DEPTH`) and call timeout
    /// (`DEFAULT_CALL_TIMEOUT`).
    pub fn connect(prefix: &str) -> Result<Self, RemoteError> {
        Self::connect_with_options(prefix, ClientOptions::default())
    }

    /// Connect with an explicit ring depth (must match the daemon's).
    /// Equivalent to `connect_with_options(prefix, ClientOptions { depth, ..Default::default() })`.
    pub fn connect_with_depth(prefix: &str, depth: usize) -> Result<Self, RemoteError> {
        let mut opts = ClientOptions::default();
        opts.depth = depth;
        Self::connect_with_options(prefix, opts)
    }

    /// Connect with custom options.
    pub fn connect_with_options(prefix: &str, opts: ClientOptions) -> Result<Self, RemoteError> {
        // Fail loud before the worker spawn / mmap: if the prefix would
        // produce names that exceed the macOS portable SHM budget, the
        // mmap below would fail with `os error 63` and the caller would
        // see a cryptic kernel error instead of an actionable message.
        crate::validate_segment_name_budget(prefix)
            .map_err(|e| RemoteError::Connect(e.to_string()))?;
        let (req_seg, resp_seg) = segment_names(prefix);
        let prefix = prefix.to_string();
        let (cmd_tx, cmd_rx) = mpsc::channel::<Cmd>();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<(), RemoteError>>();
        let depth = opts.depth;

        let worker = thread::Builder::new()
            .name(format!("wombatkv-shm-client-{prefix}"))
            .stack_size(WORKER_STACK_BYTES)
            .spawn(move || worker_loop(prefix, req_seg, resp_seg, depth, cmd_rx, ready_tx))
            .map_err(|err| RemoteError::Connect(format!("spawn worker: {err}")))?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                cmd_tx,
                worker: Some(worker),
                call_timeout: opts.call_timeout,
                dead: Arc::new(AtomicBool::new(false)),
            }),
            Ok(Err(err)) => {
                // Worker reported a connect error. Drain its handle.
                let _ = worker.join();
                Err(err)
            }
            Err(_) => {
                // Worker died before signalling.
                let _ = worker.join();
                Err(RemoteError::Connect("worker exited before signalling ready".to_string()))
            }
        }
    }

    /// Returns `true` if the client has timed out and stopped serving
    /// new requests. Engine code should drop the handle and reconnect.
    #[must_use]
    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
    }

    fn check_alive(&self) -> Result<(), RemoteError> {
        if self.is_dead() {
            Err(RemoteError::BackendUnavailable)
        } else {
            Ok(())
        }
    }

    /// Internal: send a cmd + wait on its reply with the configured
    /// timeout. On timeout, mark dead and return Timeout error.
    fn dispatch<T>(
        &self,
        send_cmd: impl FnOnce(Sender<Result<T, RemoteError>>) -> Cmd,
    ) -> Result<T, RemoteError> {
        self.check_alive()?;
        let (reply_tx, reply_rx) = mpsc::channel();
        self.cmd_tx.send(send_cmd(reply_tx)).map_err(|_| RemoteError::WorkerGone)?;
        match reply_rx.recv_timeout(self.call_timeout) {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => {
                self.dead.store(true, Ordering::Release);
                Err(RemoteError::Timeout(self.call_timeout))
            }
            Err(RecvTimeoutError::Disconnected) => Err(RemoteError::WorkerGone),
        }
    }

    /// PUT a single key/value blob. Idempotent on `(namespace, key)`.
    pub fn put_kv(&self, namespace: &str, key: &str, payload: Bytes) -> Result<(), RemoteError> {
        if !fits_one_frame(payload.len()) {
            return self.put_large_kv(namespace, key, payload);
        }
        self.put_small_kv(namespace, key, payload)
    }

    fn put_small_kv(&self, namespace: &str, key: &str, payload: Bytes) -> Result<(), RemoteError> {
        let namespace = namespace.to_string();
        let key = key.to_string();
        self.dispatch(|reply| Cmd::Put { namespace, key, payload, reply })
    }

    fn put_large_kv(&self, namespace: &str, key: &str, payload: Bytes) -> Result<(), RemoteError> {
        let id = large_payload_id(namespace, key, &payload);
        let chunk_count = payload.len().div_ceil(LARGE_CHUNK_BYTES);

        for (idx, chunk) in payload.chunks(LARGE_CHUNK_BYTES).enumerate() {
            self.put_small_kv(
                namespace,
                &large_chunk_key(&id, idx),
                Bytes::copy_from_slice(chunk),
            )?;
        }

        let manifest = large_manifest(&id, payload.len(), chunk_count);
        self.put_small_kv(namespace, key, Bytes::from(manifest))
    }

    /// GET a single key/value blob. Returns `Miss` only on absence;
    /// transient daemon errors come back as `Err`.
    pub fn get_kv(&self, namespace: &str, key: &str) -> Result<RemoteGetOutcome, RemoteError> {
        let outcome = self.get_small_kv(namespace, key)?;
        match outcome {
            RemoteGetOutcome::Hit { tier, payload } => {
                if let Some(manifest) = parse_large_manifest(&payload) {
                    self.get_large_kv(namespace, tier, manifest)
                } else {
                    Ok(RemoteGetOutcome::Hit { tier, payload })
                }
            }
            RemoteGetOutcome::Miss => Ok(RemoteGetOutcome::Miss),
        }
    }

    fn get_small_kv(&self, namespace: &str, key: &str) -> Result<RemoteGetOutcome, RemoteError> {
        let namespace = namespace.to_string();
        let key = key.to_string();
        self.dispatch(|reply| Cmd::Get { namespace, key, reply })
    }

    /// Batched GET for one engine transfer. Returns an encoded bytes batch
    /// preserving `keys` order, or `None` when any key is missing.
    pub fn get_many_kv_batch(
        &self,
        namespace: &str,
        keys: &[String],
    ) -> Result<Option<Bytes>, RemoteError> {
        let namespace_owned = namespace.to_string();
        let keys_owned = keys.to_vec();
        match self.dispatch(|reply| Cmd::GetMany {
            namespace: namespace_owned,
            keys: keys_owned.clone(),
            reply,
        }) {
            Err(RemoteError::DaemonStatus { status: status::TOO_LARGE, .. }) => {
                self.get_many_kv_batch_serial(namespace, &keys_owned)
            }
            other => other,
        }
    }

    fn get_many_kv_batch_serial(
        &self,
        namespace: &str,
        keys: &[String],
    ) -> Result<Option<Bytes>, RemoteError> {
        let mut items = Vec::with_capacity(keys.len());
        for key in keys {
            match self.get_kv(namespace, key)? {
                RemoteGetOutcome::Hit { payload, .. } => items.push(payload),
                RemoteGetOutcome::Miss => return Ok(None),
            }
        }
        Ok(Some(Bytes::from(encode_bytes_batch(&items))))
    }

    fn get_large_kv(
        &self,
        namespace: &str,
        tier: RemoteHitTier,
        manifest: LargeManifest,
    ) -> Result<RemoteGetOutcome, RemoteError> {
        let mut out = Vec::with_capacity(manifest.total_len);
        for idx in 0..manifest.chunk_count {
            match self.get_small_kv(namespace, &large_chunk_key(&manifest.id, idx))? {
                RemoteGetOutcome::Hit { payload, .. } => out.extend_from_slice(&payload),
                RemoteGetOutcome::Miss => {
                    return Err(RemoteError::DaemonStatus {
                        status: status::MISS,
                        message: format!("missing large-payload chunk {idx} for {}", manifest.id),
                    });
                }
            }
        }
        if out.len() != manifest.total_len {
            return Err(RemoteError::Transport(format!(
                "large-payload length mismatch: expected {} got {}",
                manifest.total_len,
                out.len()
            )));
        }
        Ok(RemoteGetOutcome::Hit { tier, payload: Bytes::from(out) })
    }

    /// EXISTS, same path as GET but the daemon does not transfer the
    /// payload bytes back over the wire.
    pub fn exists(&self, namespace: &str, key: &str) -> Result<bool, RemoteError> {
        let namespace = namespace.to_string();
        let key = key.to_string();
        self.dispatch(|reply| Cmd::Exists { namespace, key, reply })
    }

    /// List namespace-relative keys as an encoded key batch.
    pub fn list_keys_batch(&self, namespace: &str) -> Result<Bytes, RemoteError> {
        let namespace = namespace.to_string();
        self.dispatch(|reply| Cmd::List { namespace, reply })
    }

    /// Hydrate the daemon's foyer cache from S3 for the given namespace.
    /// Returns the number of keys restored.
    pub fn restore_from_s3(&self, namespace: &str) -> Result<usize, RemoteError> {
        let namespace = namespace.to_string();
        self.dispatch(|reply| Cmd::Restore { namespace, reply })
    }

    /// Drop the daemon's foyer state. Test/admin only.
    pub fn clear_foyer(&self) -> Result<(), RemoteError> {
        self.dispatch(|reply| Cmd::Clear { reply })
    }

    /// Round-trip ping for liveness checks.
    pub fn ping(&self) -> Result<(), RemoteError> {
        self.dispatch(|reply| Cmd::Ping { reply })
    }

    /// Daemon-side metrics snapshot, JSON lines.
    pub fn stats(&self) -> Result<String, RemoteError> {
        self.dispatch(|reply| Cmd::Stats { reply })
    }

    /// Block-shaped: count leading hashes present in the daemon's
    /// metadata index. Mirrors `wmbt_kv_lookup_block_prefix` semantics:
    /// returns the number of hashes, counted from index 0 of
    /// `block_hashes_hex`, that the daemon's metadata index recognized
    /// before the first miss. A leading miss returns `0`.
    ///
    /// Each entry of `block_hashes_hex` MUST be exactly 64 lower-hex
    /// characters (32-byte blake3 hash); the daemon rejects malformed
    /// input with `RemoteError::DaemonStatus`.
    pub fn lookup_block_prefix(
        &self,
        namespace: &str,
        block_hashes_hex: &[String],
    ) -> Result<usize, RemoteError> {
        let namespace = namespace.to_string();
        let block_hashes_hex = block_hashes_hex.to_vec();
        self.dispatch(|reply| Cmd::LookupBlockPrefix { namespace, block_hashes_hex, reply })
    }

    /// Block-shaped: parallel batched GET for content-addressed blocks.
    /// On full hit returns `Some(per-block bytes in input order)`; on
    /// any miss returns `None` (all-or-nothing, matches the cabi
    /// `wmbt_kv_get_kv_blocks_borrowed` contract).
    pub fn get_kv_blocks_batch(
        &self,
        namespace: &str,
        block_hashes_hex: &[String],
    ) -> Result<Option<Vec<Bytes>>, RemoteError> {
        let namespace = namespace.to_string();
        let block_hashes_hex = block_hashes_hex.to_vec();
        self.dispatch(|reply| Cmd::GetKvBlocksBatch { namespace, block_hashes_hex, reply })
    }

    /// Block-shaped: parallel batched PUT for content-addressed blocks.
    /// The daemon writes each block, then updates its in-process metadata
    /// index so a subsequent [`Self::lookup_block_prefix`] sees the new
    /// presence. Returns total bytes written across all blocks.
    ///
    /// `block_hashes_hex.len()` MUST equal `payloads.len()`. The caller's
    /// payload slices are copied into the daemon transport (rkyv-encoded
    /// inside the request frame).
    pub fn put_kv_blocks_batch(
        &self,
        namespace: &str,
        block_hashes_hex: &[String],
        payloads: &[&[u8]],
    ) -> Result<u64, RemoteError> {
        if block_hashes_hex.len() != payloads.len() {
            return Err(RemoteError::Transport(format!(
                "put_kv_blocks_batch length mismatch: {} hashes vs {} payloads",
                block_hashes_hex.len(),
                payloads.len()
            )));
        }
        let namespace = namespace.to_string();
        let block_hashes_hex = block_hashes_hex.to_vec();
        let payloads: Vec<Vec<u8>> = payloads.iter().map(|s| s.to_vec()).collect();
        self.dispatch(|reply| Cmd::PutKvBlocksBatch {
            namespace,
            block_hashes_hex,
            payloads,
            reply,
        })
    }

    /// Health-check ping with a custom (typically short) timeout.
    /// Useful for liveness probes that want to fail faster than the
    /// default `call_timeout`. Marks the client dead on timeout, same
    /// as any other call.
    pub fn ping_with_timeout(&self, timeout: Duration) -> Result<(), RemoteError> {
        self.check_alive()?;
        let (reply_tx, reply_rx) = mpsc::channel();
        self.cmd_tx.send(Cmd::Ping { reply: reply_tx }).map_err(|_| RemoteError::WorkerGone)?;
        match reply_rx.recv_timeout(timeout) {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => {
                self.dead.store(true, Ordering::Release);
                Err(RemoteError::Timeout(timeout))
            }
            Err(RecvTimeoutError::Disconnected) => Err(RemoteError::WorkerGone),
        }
    }
}

impl Drop for RemoteKvStoreClient {
    fn drop(&mut self) {
        // Best-effort shutdown signal. The worker is in `cmd_rx.recv()`
        // when idle, so this wakes it up and lets it exit cleanly when
        // the daemon is alive.
        let _ = self.cmd_tx.send(Cmd::Shutdown);

        if let Some(handle) = self.worker.take() {
            if self.is_dead() {
                // Daemon is gone and the worker is busy-spinning forever
                // in `recv_owned`. Joining would block Drop forever.
                // Detach (drop the JoinHandle), the OS will reap the
                // thread on process exit. This is the documented leak
                // in the module-level "Liveness" comment.
                drop(handle);
            } else {
                // Happy path: daemon is alive, worker will see the
                // Shutdown cmd and return.
                let _ = handle.join();
            }
        }
    }
}

fn worker_loop(
    prefix: String,
    req_seg: String,
    resp_seg: String,
    depth: usize,
    cmd_rx: Receiver<Cmd>,
    ready_tx: Sender<Result<(), RemoteError>>,
) {
    let heartbeat = match ClientHeartbeat::acquire(&prefix) {
        Ok(heartbeat) => heartbeat,
        Err(err) => {
            let _ = ready_tx.send(Err(RemoteError::Connect(err)));
            return;
        }
    };

    let (mut req_producer, mut resp_consumer) = match wait_open_client(&req_seg, &resp_seg, depth) {
        Ok(pair) => pair,
        Err(err) => {
            let _ = ready_tx.send(Err(RemoteError::Connect(err)));
            return;
        }
    };

    let arena_reader = match arena_reader_from_env() {
        Ok(reader) => reader,
        Err(err) => {
            let _ = ready_tx.send(Err(err));
            return;
        }
    };

    if !req_producer.discover_consumer_id(REQ_CONSUMER_ID, effective_attach_timeout()) {
        let _ = ready_tx.send(Err(RemoteError::Connect(
            "client could not discover daemon consumer".to_string(),
        )));
        return;
    }

    let _ = ready_tx.send(Ok(()));

    // Move the heartbeat to its own thread so it keeps writing during
    // long round-trips. The original loop beat between commands and
    // missed the entire window of a single chunked PUT against a slow
    // daemon, the daemon then concluded the client was stale and
    // reopened the rings mid-call. With a dedicated thread the cmd
    // loop is free to block on `cmd_rx.recv()` without worrying about
    // liveness.
    let heartbeat_stop = Arc::new(AtomicBool::new(false));
    let heartbeat_handle =
        heartbeat.map(|hb| spawn_heartbeat_thread(&prefix, hb, heartbeat_stop.clone()));

    let mut next_id: u64 = 0;
    loop {
        let cmd = match cmd_rx.recv() {
            Ok(cmd) => cmd,
            Err(_) => break,
        };
        match cmd {
            Cmd::Shutdown => {
                let id = take_id(&mut next_id);
                let req = WireRequest {
                    id,
                    op: op_codes::CLOSE,
                    namespace: String::new(),
                    key: String::new(),
                    payload: Vec::new(),
                };
                let _ = round_trip(&mut req_producer, &mut resp_consumer, &req);
                break;
            }
            Cmd::Put { namespace, key, payload, reply } => {
                let id = take_id(&mut next_id);
                let req = WireRequest {
                    id,
                    op: op_codes::PUT,
                    namespace,
                    key,
                    payload: payload.to_vec(),
                };
                let result =
                    round_trip(&mut req_producer, &mut resp_consumer, &req).and_then(|resp| {
                        match resp.status {
                            status::OK => Ok(()),
                            s => {
                                Err(RemoteError::DaemonStatus { status: s, message: resp.message })
                            }
                        }
                    });
                let _ = reply.send(result);
            }
            Cmd::Get { namespace, key, reply } => {
                let id = take_id(&mut next_id);
                let req =
                    WireRequest { id, op: op_codes::GET, namespace, key, payload: Vec::new() };
                let result =
                    round_trip(&mut req_producer, &mut resp_consumer, &req).and_then(|resp| {
                        match resp.status {
                            status::OK => {
                                let tier = parse_tier(&resp.message);
                                if let Some((offset, len)) = parse_arena_ref(&resp.message) {
                                    let reader = arena_reader.as_ref().ok_or_else(|| {
                                        RemoteError::Transport(format!(
                                        "daemon returned arena ref but {ARENA_PATH_ENV} is unset"
                                    ))
                                    })?;
                                    reader.read_payload(offset, len).map_err(|err| {
                                        RemoteError::Transport(format!(
                                            "arena read offset={offset} len={len}: {err}"
                                        ))
                                    })?;
                                    let payload = Bytes::from_owner(ArenaOwnedBytes {
                                        reader: reader.clone(),
                                        offset,
                                        len,
                                    });
                                    Ok(RemoteGetOutcome::Hit { tier, payload })
                                } else {
                                    Ok(RemoteGetOutcome::Hit {
                                        tier,
                                        payload: Bytes::from(resp.payload),
                                    })
                                }
                            }
                            status::MISS => Ok(RemoteGetOutcome::Miss),
                            s => {
                                Err(RemoteError::DaemonStatus { status: s, message: resp.message })
                            }
                        }
                    });
                let _ = reply.send(result);
            }
            Cmd::GetMany { namespace, keys, reply } => {
                let id = take_id(&mut next_id);
                let req = WireRequest {
                    id,
                    op: op_codes::GET_MANY,
                    namespace,
                    key: String::new(),
                    payload: encode_key_batch(&keys),
                };
                let result =
                    round_trip(&mut req_producer, &mut resp_consumer, &req).and_then(|resp| {
                        match resp.status {
                            status::OK => {
                                let batch =
                                    if let Some((offset, len)) = parse_arena_ref(&resp.message) {
                                        let reader = arena_reader.as_ref().ok_or_else(|| {
                                            RemoteError::Transport(format!(
                                        "daemon returned arena ref but {ARENA_PATH_ENV} is unset"
                                    ))
                                        })?;
                                        reader.read_payload(offset, len).map_err(|err| {
                                            RemoteError::Transport(format!(
                                                "arena read offset={offset} len={len}: {err}"
                                            ))
                                        })?;
                                        Bytes::from_owner(ArenaOwnedBytes {
                                            reader: reader.clone(),
                                            offset,
                                            len,
                                        })
                                    } else {
                                        Bytes::from(resp.payload)
                                    };
                                decode_bytes_batch(batch.clone()).map_err(|err| {
                                    RemoteError::Transport(format!("decode get_many batch: {err}"))
                                })?;
                                Ok(Some(batch))
                            }
                            status::MISS => Ok(None),
                            s => {
                                Err(RemoteError::DaemonStatus { status: s, message: resp.message })
                            }
                        }
                    });
                let _ = reply.send(result);
            }
            Cmd::Exists { namespace, key, reply } => {
                let id = take_id(&mut next_id);
                let req =
                    WireRequest { id, op: op_codes::EXISTS, namespace, key, payload: Vec::new() };
                let result =
                    round_trip(&mut req_producer, &mut resp_consumer, &req).and_then(|resp| {
                        match resp.status {
                            status::OK => Ok(true),
                            status::MISS => Ok(false),
                            s => {
                                Err(RemoteError::DaemonStatus { status: s, message: resp.message })
                            }
                        }
                    });
                let _ = reply.send(result);
            }
            Cmd::List { namespace, reply } => {
                let id = take_id(&mut next_id);
                let req = WireRequest {
                    id,
                    op: op_codes::LIST,
                    namespace,
                    key: String::new(),
                    payload: Vec::new(),
                };
                let result =
                    round_trip(&mut req_producer, &mut resp_consumer, &req).and_then(|resp| {
                        match resp.status {
                            status::OK => Ok(Bytes::from(resp.payload)),
                            s => {
                                Err(RemoteError::DaemonStatus { status: s, message: resp.message })
                            }
                        }
                    });
                let _ = reply.send(result);
            }
            Cmd::Restore { namespace, reply } => {
                let id = take_id(&mut next_id);
                let req = WireRequest {
                    id,
                    op: op_codes::RESTORE,
                    namespace,
                    key: String::new(),
                    payload: Vec::new(),
                };
                let result =
                    round_trip(&mut req_producer, &mut resp_consumer, &req).and_then(|resp| {
                        match resp.status {
                            status::OK => resp.message.parse::<usize>().map_err(|err| {
                                RemoteError::Transport(format!(
                                    "restore count parse: {err} (msg={:?})",
                                    resp.message
                                ))
                            }),
                            s => {
                                Err(RemoteError::DaemonStatus { status: s, message: resp.message })
                            }
                        }
                    });
                let _ = reply.send(result);
            }
            Cmd::Clear { reply } => {
                let id = take_id(&mut next_id);
                let req = WireRequest {
                    id,
                    op: op_codes::CLEAR,
                    namespace: String::new(),
                    key: String::new(),
                    payload: Vec::new(),
                };
                let result =
                    round_trip(&mut req_producer, &mut resp_consumer, &req).and_then(|resp| {
                        match resp.status {
                            status::OK => Ok(()),
                            s => {
                                Err(RemoteError::DaemonStatus { status: s, message: resp.message })
                            }
                        }
                    });
                let _ = reply.send(result);
            }
            Cmd::Ping { reply } => {
                let id = take_id(&mut next_id);
                let req = WireRequest {
                    id,
                    op: op_codes::PING,
                    namespace: String::new(),
                    key: String::new(),
                    payload: Vec::new(),
                };
                let result =
                    round_trip(&mut req_producer, &mut resp_consumer, &req).and_then(|resp| {
                        match resp.status {
                            status::OK => Ok(()),
                            s => {
                                Err(RemoteError::DaemonStatus { status: s, message: resp.message })
                            }
                        }
                    });
                let _ = reply.send(result);
            }
            Cmd::Stats { reply } => {
                let id = take_id(&mut next_id);
                let req = WireRequest {
                    id,
                    op: op_codes::STATS,
                    namespace: String::new(),
                    key: String::new(),
                    payload: Vec::new(),
                };
                let result =
                    round_trip(&mut req_producer, &mut resp_consumer, &req).and_then(|resp| {
                        match resp.status {
                            status::OK => Ok(resp.message),
                            s => {
                                Err(RemoteError::DaemonStatus { status: s, message: resp.message })
                            }
                        }
                    });
                let _ = reply.send(result);
            }
            Cmd::LookupBlockPrefix { namespace, block_hashes_hex, reply } => {
                let id = take_id(&mut next_id);
                let payload = match encode_lookup_block_prefix_req(&LookupBlockPrefixReq {
                    namespace: namespace.clone(),
                    block_hashes_hex,
                }) {
                    Ok(p) => p,
                    Err(err) => {
                        let _ = reply.send(Err(RemoteError::Transport(err.to_string())));
                        continue;
                    }
                };
                let req = WireRequest {
                    id,
                    op: op_codes::LOOKUP_BLOCK_PREFIX,
                    namespace,
                    key: String::new(),
                    payload,
                };
                let result =
                    round_trip(&mut req_producer, &mut resp_consumer, &req).and_then(|resp| {
                        match resp.status {
                            status::OK => {
                                let decoded = decode_lookup_block_prefix_resp(&resp.payload)
                                    .map_err(|err| {
                                        RemoteError::Transport(format!(
                                            "decode lookup_block_prefix resp: {err}"
                                        ))
                                    })?;
                                if let Some(err) = decoded.error {
                                    return Err(RemoteError::DaemonStatus {
                                        status: status::ERROR,
                                        message: err,
                                    });
                                }
                                Ok(decoded.matched_count as usize)
                            }
                            s => {
                                Err(RemoteError::DaemonStatus { status: s, message: resp.message })
                            }
                        }
                    });
                let _ = reply.send(result);
            }
            Cmd::GetKvBlocksBatch { namespace, block_hashes_hex, reply } => {
                let id = take_id(&mut next_id);
                let payload = match encode_get_kv_blocks_batch_req(&GetKvBlocksBatchReq {
                    namespace: namespace.clone(),
                    block_hashes_hex,
                }) {
                    Ok(p) => p,
                    Err(err) => {
                        let _ = reply.send(Err(RemoteError::Transport(err.to_string())));
                        continue;
                    }
                };
                let req = WireRequest {
                    id,
                    op: op_codes::GET_KV_BLOCKS_BATCH,
                    namespace,
                    key: String::new(),
                    payload,
                };
                let result =
                    round_trip(&mut req_producer, &mut resp_consumer, &req).and_then(|resp| {
                        match resp.status {
                            status::OK => {
                                let decoded = decode_get_kv_blocks_batch_resp(&resp.payload)
                                    .map_err(|err| {
                                        RemoteError::Transport(format!(
                                            "decode get_kv_blocks_batch resp: {err}"
                                        ))
                                    })?;
                                if let Some(err) = decoded.error {
                                    return Err(RemoteError::DaemonStatus {
                                        status: status::ERROR,
                                        message: err,
                                    });
                                }
                                Ok(decoded
                                    .payloads
                                    .map(|items| items.into_iter().map(Bytes::from).collect()))
                            }
                            s => {
                                Err(RemoteError::DaemonStatus { status: s, message: resp.message })
                            }
                        }
                    });
                let _ = reply.send(result);
            }
            Cmd::PutKvBlocksBatch { namespace, block_hashes_hex, payloads, reply } => {
                let id = take_id(&mut next_id);
                let payload = match encode_put_kv_blocks_batch_req(&PutKvBlocksBatchReq {
                    namespace: namespace.clone(),
                    block_hashes_hex,
                    payloads,
                }) {
                    Ok(p) => p,
                    Err(err) => {
                        let _ = reply.send(Err(RemoteError::Transport(err.to_string())));
                        continue;
                    }
                };
                let req = WireRequest {
                    id,
                    op: op_codes::PUT_KV_BLOCKS_BATCH,
                    namespace,
                    key: String::new(),
                    payload,
                };
                let result =
                    round_trip(&mut req_producer, &mut resp_consumer, &req).and_then(|resp| {
                        match resp.status {
                            status::OK => {
                                let decoded = decode_put_kv_blocks_batch_resp(&resp.payload)
                                    .map_err(|err| {
                                        RemoteError::Transport(format!(
                                            "decode put_kv_blocks_batch resp: {err}"
                                        ))
                                    })?;
                                if let Some(err) = decoded.error {
                                    return Err(RemoteError::DaemonStatus {
                                        status: status::ERROR,
                                        message: err,
                                    });
                                }
                                Ok(decoded.total_bytes)
                            }
                            s => {
                                Err(RemoteError::DaemonStatus { status: s, message: resp.message })
                            }
                        }
                    });
                let _ = reply.send(result);
            }
        }
    }

    heartbeat_stop.store(true, Ordering::Release);
    if let Some(handle) = heartbeat_handle {
        let _ = handle.join();
    }
}

fn spawn_heartbeat_thread(
    prefix: &str,
    mut heartbeat: ClientHeartbeat,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    let interval = heartbeat.interval();
    let name = format!("wombatkv-shm-hb-{prefix}");
    thread::Builder::new()
        .name(name)
        .stack_size(64 * 1024)
        .spawn(move || {
            // Loop until either the worker tells us to stop or the
            // heartbeat is taken over by another client. The interval
            // is short (default 500 ms) so we wake often enough to
            // notice a shutdown signal promptly without being a CPU
            // burden.
            while !stop.load(Ordering::Acquire) {
                if heartbeat.beat().is_err() {
                    // Heartbeat replaced by another client. Stop
                    // beating so the new owner isn't fighting us.
                    break;
                }
                thread::sleep(interval);
            }
        })
        .expect("spawn wombatkv-shm heartbeat thread")
}

fn take_id(next: &mut u64) -> u64 {
    let id = *next;
    *next = next.wrapping_add(1);
    id
}

fn round_trip(
    req_producer: &mut TypedProducer<ShmFrame>,
    resp_consumer: &mut TypedConsumer<ShmFrame>,
    req: &WireRequest,
) -> Result<WireResponse, RemoteError> {
    req_producer
        .publish(req, 1)
        .map_err(|err| RemoteError::Transport(format!("publish: {err}")))?;
    let (_kind, resp) = resp_consumer
        .recv_owned::<WireResponse>()
        .map_err(|err| RemoteError::Transport(format!("recv: {err}")))?;
    if resp.id != req.id {
        return Err(RemoteError::Transport(format!(
            "response id mismatch: req={} resp={}",
            req.id, resp.id
        )));
    }
    Ok(resp)
}

fn parse_tier(message: &str) -> RemoteHitTier {
    if message.split(';').any(|part| part == "tier:1") {
        RemoteHitTier::Foyer
    } else {
        RemoteHitTier::ObjectStore
    }
}

fn parse_arena_ref(message: &str) -> Option<(u64, u32)> {
    let encoded = message.split(';').find_map(|part| part.strip_prefix(ARENA_MESSAGE_PREFIX))?;
    let mut fields = encoded.split(':');
    let offset = fields.next()?.parse().ok()?;
    let len = fields.next()?.parse().ok()?;
    if fields.next().is_some() {
        return None;
    }
    Some((offset, len))
}

struct ArenaOwnedBytes {
    reader: Arc<ArenaReader>,
    offset: u64,
    len: u32,
}

impl AsRef<[u8]> for ArenaOwnedBytes {
    fn as_ref(&self) -> &[u8] {
        self.reader.read_payload(self.offset, self.len).expect("validated arena-backed Bytes slice")
    }
}

fn arena_reader_from_env() -> Result<Option<Arc<ArenaReader>>, RemoteError> {
    let Ok(path) = std::env::var(ARENA_PATH_ENV) else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(None);
    }
    ArenaReader::open(&PathBuf::from(path))
        .map(Arc::new)
        .map(Some)
        .map_err(|err| RemoteError::Connect(format!("open {ARENA_PATH_ENV}: {err}")))
}

#[derive(Debug, Clone)]
struct LargeManifest {
    id: String,
    total_len: usize,
    chunk_count: usize,
}

fn large_payload_id(namespace: &str, key: &str, payload: &[u8]) -> String {
    let mut h = blake3::Hasher::new();
    h.update(b"wombatkv-shm-large-v1\0");
    h.update(namespace.as_bytes());
    h.update(b"\0");
    h.update(key.as_bytes());
    h.update(b"\0");
    h.update(&(payload.len() as u64).to_le_bytes());
    h.update(payload);
    h.finalize().to_hex().to_string()
}

fn large_chunk_key(id: &str, idx: usize) -> String {
    format!("{LARGE_CHUNK_KEY_PREFIX}/{id}/{idx:08}")
}

fn large_manifest(id: &str, total_len: usize, chunk_count: usize) -> String {
    format!("{LARGE_MANIFEST_MAGIC}\n{id}\n{total_len}\n{chunk_count}\n")
}

fn parse_large_manifest(payload: &[u8]) -> Option<LargeManifest> {
    let text = std::str::from_utf8(payload).ok()?;
    let mut lines = text.lines();
    if lines.next()? != LARGE_MANIFEST_MAGIC {
        return None;
    }
    let id = lines.next()?.to_string();
    let total_len = lines.next()?.parse().ok()?;
    let chunk_count = lines.next()?.parse().ok()?;
    Some(LargeManifest { id, total_len, chunk_count })
}

fn wait_open_client(
    req_seg: &str,
    resp_seg: &str,
    depth: usize,
) -> Result<(TypedProducer<ShmFrame>, TypedConsumer<ShmFrame>), String> {
    use std::time::Instant;
    let deadline = Instant::now() + effective_attach_timeout();
    loop {
        match crate::open_client(req_seg, resp_seg, depth) {
            Ok(pair) => return Ok(pair),
            Err(err) => {
                let msg = format!("{err}");
                if Instant::now() >= deadline {
                    return Err(msg);
                }
                thread::sleep(Duration::from_millis(25));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::SystemTime;

    fn unique_prefix() -> String {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let nanos =
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
        let s = format!("{nanos:x}{seq:x}");
        let n = s.len();
        format!("c{}", &s[n.saturating_sub(6)..])
    }

    /// Spawn a daemon for the given prefix. Requires `WMBT_KV_DAEMON_SHM_DAEMON_BIN`
    /// or a release build at `target/release/wombatkv-daemon`. Skips
    /// the test if the binary cannot be found OR S3 env vars are missing.
    struct DaemonGuard {
        child: Option<Child>,
    }

    impl DaemonGuard {
        fn try_spawn(prefix: &str) -> Option<Self> {
            if std::env::var("WMBT_KV_S3_ENDPOINT").is_err() {
                eprintln!("skipping client/daemon test: WMBT_KV_S3_ENDPOINT not set");
                return None;
            }
            let bin = match std::env::var("WMBT_KV_DAEMON_SHM_DAEMON_BIN") {
                Ok(p) => std::path::PathBuf::from(p),
                Err(_) => default_daemon_bin()?,
            };
            if !bin.is_file() {
                eprintln!("skipping client/daemon test: daemon bin not found at {bin:?}");
                return None;
            }
            let child = Command::new(&bin)
                .arg("--prefix")
                .arg(prefix)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .ok()?;
            Some(Self { child: Some(child) })
        }
    }

    impl Drop for DaemonGuard {
        fn drop(&mut self) {
            if let Some(mut c) = self.child.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }

    fn default_daemon_bin() -> Option<std::path::PathBuf> {
        let cwd = std::env::current_dir().ok()?;
        let mut dir = cwd.as_path().to_path_buf();
        for _ in 0..6 {
            let cand = dir.join("target/release/wombatkv-daemon");
            if cand.is_file() {
                return Some(cand);
            }
            dir = dir.parent()?.to_path_buf();
        }
        None
    }

    #[test]
    fn ping_round_trip() {
        let prefix = unique_prefix();
        let Some(_guard) = DaemonGuard::try_spawn(&prefix) else {
            return;
        };
        let client = match RemoteKvStoreClient::connect(&prefix) {
            Ok(c) => c,
            Err(err) => {
                eprintln!("connect failed: {err}, skipping");
                return;
            }
        };
        client.ping().expect("ping ok");
    }

    #[test]
    fn put_then_get_round_trip() {
        let prefix = unique_prefix();
        let Some(_guard) = DaemonGuard::try_spawn(&prefix) else {
            return;
        };
        let client = match RemoteKvStoreClient::connect(&prefix) {
            Ok(c) => c,
            Err(err) => {
                eprintln!("connect failed: {err}, skipping");
                return;
            }
        };
        let payload = Bytes::from(vec![0xAB; 4096]);
        client.put_kv("client-test", "k1", payload.clone()).expect("put");
        match client.get_kv("client-test", "k1").expect("get") {
            RemoteGetOutcome::Hit { payload: got, .. } => {
                assert_eq!(got.len(), payload.len());
                assert_eq!(&got[..16], &payload[..16]);
            }
            RemoteGetOutcome::Miss => panic!("expected hit"),
        }
        assert!(client.exists("client-test", "k1").expect("exists"));
        assert!(!client.exists("client-test", "missing").expect("exists"));
    }

    #[test]
    fn large_manifest_round_trips() {
        let id = "abc123";
        let manifest = large_manifest(id, 12345, 7);
        let parsed = parse_large_manifest(manifest.as_bytes()).expect("manifest");
        assert_eq!(parsed.id, id);
        assert_eq!(parsed.total_len, 12345);
        assert_eq!(parsed.chunk_count, 7);
        assert!(parse_large_manifest(b"ordinary payload").is_none());
    }
}
