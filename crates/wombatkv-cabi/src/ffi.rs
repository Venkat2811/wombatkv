//! Extern "C" surface, see `include/wombatkv.h` for the canonical
//! API documentation. This file is the Rust side of that contract.
//!
//! Every `pub extern "C" fn` in this module takes `*const T` / `*mut T`
//! arguments by design, this is the FFI ABI for C callers (ds4 and
//! future llama.cpp / custom integrations). Functions present a
//! safe-from-Rust API to internal callers (null-checks + error codes);
//! marking each signature `unsafe fn` would break Rust callers that
//! already wrap them through the safe surface above. The
//! `#![allow(clippy::not_unsafe_ptr_arg_deref)]` below documents this
//! intent and overrides the workspace `-D clippy::correctness` gate.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::cell::RefCell;
use std::ffi::{c_char, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use wombatkv_daemon::{
    decode_bytes_batch, encode_bytes_batch, ClientOptions, RemoteGetOutcome, RemoteKvStoreClient,
};
use wombatkv_node::embed::{EmbedConfig, GetOutcome, WombatKVKvStore};
use wombatkv_node::foyer_cache::FoyerCacheConfig;
use wombatkv_radix::{BlockHash, BlockMeta, MetadataIndex};
use wombatkv_store::wal_store::{S3ObjectStore, S3ObjectStoreConfig};

/// ABI major version. Bump on backwards-incompatible C ABI changes.
///
/// 0.1.0-alpha ships with a single consolidated ABI version (1.0). The
/// pre-alpha development history of 1.1 / 1.2 / 1.3 / 1.4 / 1.5 / 1.6
/// has been collapsed since no external consumer ever depended on those
/// intermediate versions. Post-alpha breaking changes bump to 2.0;
/// post-alpha additive changes bump the minor.
pub const ABI_MAJOR: u16 = 1;
/// ABI minor version. Bump on backwards-compatible C ABI extensions.
pub const ABI_MINOR: u16 = 0;

/// Object-key namespace for content-addressed block payloads.
///
/// The chunked path uses `<base_key>:chunk:b3=<hex>` because each chunk
/// is tied to a parent monolithic blob's key. Block-shaped
/// payloads are pure content-addressed, there is no parent key, so
/// they live at a standalone path. Producers and consumers MUST agree
/// on this scheme; it is part of the C ABI contract. The constant
/// lives in `wombatkv-radix` so the prefetch path in `wombatkv-node`
/// reads from the same definition (skew here previously broke the
/// prefetch worker, see commit 2f76296).
use wombatkv_radix::{BLOCK_KEY_PREFIX, SIDECAR_RAW_TAIL_KEY_PREFIX};

const DEFAULT_FOYER_DIR: &str = "/tmp/wombatkv-cabi-foyer";
const DEFAULT_S3_PREFIX: &str = "kv/cabi";
const DEFAULT_NAMESPACE: &str = "default";

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(message: impl Into<String>) {
    let s = message.into();
    let cs = CString::new(s).unwrap_or_else(|_| {
        CString::new("error message contained interior NUL byte").expect("static safe")
    });
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(cs);
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| *slot.borrow_mut() = None);
}

enum Backend {
    Embedded(Arc<WombatKVKvStore<S3ObjectStore>>),
    /// SHM daemon (same host, 1P-1C disruptor ring).
    Remote(Arc<RemoteKvStoreClient>),
    /// TCP daemon (cross-host, length-prefixed rkyv envelope).
    /// See `wombatkv_daemon::tcp_transport`. The handle owns one
    /// persistent TCP connection per `RemoteTcp` instance; for true
    /// concurrency, callers clone fresh `TcpKvClient`s per thread.
    RemoteTcp(Arc<wombatkv_daemon::tcp_transport::TcpKvClient>),
    /// HTTP/1.1 + rkyv daemon (cross-host, load-balancer friendly).
    /// Same rkyv envelope as `RemoteTcp` but wrapped in HTTP POSTs
    /// to `/wmbt/v1/rpc`. See `wombatkv_daemon::http_transport`. One
    /// persistent keep-alive connection per instance.
    RemoteHttp(Arc<wombatkv_daemon::http_transport::HttpKvClient>),
}

impl Backend {
    fn put_kv(&self, namespace: &str, key: &str, payload: Bytes) -> Result<(), String> {
        match self {
            Self::Embedded(store) => {
                // `WMBT_KV_EMBEDDED_ASYNC_S3` mirrors the daemon-side
                // `WMBT_KV_DAEMON_SHM_ASYNC_PUT`: returns to the caller as
                // soon as foyer has the bytes; the slow ObjectStore PUT
                // runs on a detached thread. Lets ds4 start Metal decode
                // immediately after cold prefill instead of waiting on
                // the ~1.5 s S3 PUT.
                //
                // 0.1.0-alpha: default-on. Opt out with
                // `WMBT_KV_EMBEDDED_ASYNC_S3=0` (or empty).
                if env_bool_default_on("WMBT_KV_EMBEDDED_ASYNC_S3") {
                    wombatkv_node::embed::WombatKVKvStore::put_kv_async_s3(
                        store.clone(),
                        namespace,
                        key,
                        payload,
                    );
                    Ok(())
                } else {
                    store.put_kv(namespace, key, payload).map_err(|err| format!("{err}"))
                }
            }
            Self::Remote(client) => {
                client.put_kv(namespace, key, payload).map_err(|err| format!("{err}"))
            }
            Self::RemoteTcp(client) => {
                client.put_kv(namespace, key, payload).map_err(|err| format!("{err}"))
            }
            Self::RemoteHttp(client) => {
                client.put_kv(namespace, key, payload).map_err(|err| format!("{err}"))
            }
        }
    }

    fn get_kv(&self, namespace: &str, key: &str) -> Result<Option<Bytes>, String> {
        match self {
            Self::Embedded(store) => match store.get_kv(namespace, key) {
                Ok(GetOutcome::Hit { payload, .. }) => Ok(Some(payload)),
                Ok(GetOutcome::Miss) => Ok(None),
                Err(err) => Err(format!("{err}")),
            },
            Self::Remote(client) => match client.get_kv(namespace, key) {
                Ok(RemoteGetOutcome::Hit { payload, .. }) => Ok(Some(payload)),
                Ok(RemoteGetOutcome::Miss) => Ok(None),
                Err(err) => Err(format!("{err}")),
            },
            Self::RemoteTcp(client) => match client.get_kv(namespace, key) {
                Ok(wombatkv_daemon::tcp_transport::TcpGetOutcome::Hit { payload }) => {
                    Ok(Some(payload))
                }
                Ok(wombatkv_daemon::tcp_transport::TcpGetOutcome::Miss) => Ok(None),
                Err(err) => Err(format!("{err}")),
            },
            Self::RemoteHttp(client) => match client.get_kv(namespace, key) {
                Ok(wombatkv_daemon::http_transport::HttpGetOutcome::Hit { payload }) => {
                    Ok(Some(payload))
                }
                Ok(wombatkv_daemon::http_transport::HttpGetOutcome::Miss) => Ok(None),
                Err(err) => Err(format!("{err}")),
            },
        }
    }

    fn get_many_kv_batch(&self, namespace: &str, keys: &[String]) -> Result<Option<Bytes>, String> {
        match self {
            Self::Embedded(store) => {
                // Parallel fanout via std::thread::scope. Each thread either
                // hits the flat cache (sync std::fs::read, no I/O wait) or
                // blocks on its own S3 GET. For K blocks of ~2 MB each and
                // an S3 RTT of ~10-20 ms, this turns sequential N*RTT into
                // ~1*RTT, the parallel-cold-fetch win KVBlock/0.1 demands.
                //
                // All-or-nothing semantics preserved: any miss => Ok(None)
                // so callers can fall back to the monolithic load path.
                use std::sync::atomic::{AtomicBool, Ordering};
                let started = std::time::Instant::now();
                let store_ref = store.as_ref();
                let saw_miss = AtomicBool::new(false);
                let results: Vec<Result<Option<Bytes>, String>> = std::thread::scope(|s| {
                    let handles: Vec<_> = keys
                        .iter()
                        .map(|key| {
                            s.spawn(|| {
                                if saw_miss.load(Ordering::Relaxed) {
                                    return Ok(None);
                                }
                                match store_ref.get_kv(namespace, key) {
                                    Ok(GetOutcome::Hit { payload, .. }) => Ok(Some(payload)),
                                    Ok(GetOutcome::Miss) => {
                                        saw_miss.store(true, Ordering::Relaxed);
                                        Ok(None)
                                    }
                                    Err(err) => Err(format!("{err}")),
                                }
                            })
                        })
                        .collect();
                    handles
                        .into_iter()
                        .map(|h| {
                            h.join().unwrap_or_else(|_| Err("get_many thread panic".to_string()))
                        })
                        .collect()
                });
                let elapsed_us = started.elapsed().as_micros() as u64;
                let key_count = keys.len() as u64;
                if std::env::var("WMBT_KV_TIMING").ok().as_deref() == Some("1") {
                    eprintln!(
                        "[MyelonInstr] {{\"scope\":\"wmbt_kv_timing\",\"fn\":\"get_many_kv_batch\",\
                         \"stages\":{{\"total_us\":{elapsed_us},\"key_count\":{key_count}}}}}"
                    );
                }
                let mut items = Vec::with_capacity(keys.len());
                for r in results {
                    match r {
                        Ok(Some(payload)) => items.push(payload),
                        Ok(None) => return Ok(None),
                        Err(e) => return Err(e),
                    }
                }
                Ok(Some(Bytes::from(encode_bytes_batch(&items))))
            }
            Self::Remote(client) => {
                client.get_many_kv_batch(namespace, keys).map_err(|err| format!("{err}"))
            }
            Self::RemoteTcp(_) => {
                // Alpha: TCP transport doesn't ship a GET_MANY op yet.
                // ds4's hot path doesn't use get_many_kv_batch directly;
                // get_kv_blocks below has its own dedicated TCP path via
                // get_kv_blocks_batch which IS supported. Surface a clean
                // error here so any future caller hitting this gets an
                // actionable message rather than wedging.
                Err("get_many_kv_batch not implemented for TCP backend; use \
                     get_kv_blocks for block-shaped batched GETs"
                    .to_string())
            }
            Self::RemoteHttp(_) => {
                // Mirror the TCP backend's stance: HTTP inherits the
                // same opcode table, so GET_MANY is also not in the
                // alpha wire set. ds4 takes the dedicated
                // get_kv_blocks_batch path for the block-shaped batched
                // GET hot path.
                Err("get_many_kv_batch not implemented for HTTP backend; use \
                     get_kv_blocks for block-shaped batched GETs"
                    .to_string())
            }
        }
    }

    /// Count leading hashes present in the backend.
    ///
    /// On the embedded backend this is a direct call into
    /// `InMemoryMetadataIndex::longest_prefix`. On the remote backend
    /// the request is shipped over the daemon transport as a
    /// `LOOKUP_BLOCK_PREFIX` opcode (rkyv-encoded payload); the daemon
    /// resolves it against its own metadata index. The C ABI surface
    /// (`wmbt_kv_lookup_block_prefix`) is fully functional under both
    /// backends as of ABI 1.5+.
    fn lookup_block_prefix_hex(
        &self,
        namespace: &str,
        block_hashes: &[BlockHash],
    ) -> Result<usize, String> {
        match self {
            Self::Embedded(store) => Ok(store.metadata_index().longest_prefix(block_hashes)),
            Self::Remote(client) => {
                // Daemon expects hashes as 64-char lower-hex strings -
                // the same encoding the cabi block-key derives, so we
                // hex-encode here at the wire boundary.
                let hex_list: Vec<String> = block_hashes
                    .iter()
                    .map(|h| {
                        let mut s = String::with_capacity(64);
                        for b in h {
                            s.push_str(&format!("{b:02x}"));
                        }
                        s
                    })
                    .collect();
                client.lookup_block_prefix(namespace, &hex_list).map_err(|err| format!("{err}"))
            }
            Self::RemoteTcp(client) => {
                let hex_list: Vec<String> = block_hashes
                    .iter()
                    .map(|h| {
                        let mut s = String::with_capacity(64);
                        for b in h {
                            s.push_str(&format!("{b:02x}"));
                        }
                        s
                    })
                    .collect();
                client.lookup_block_prefix(namespace, &hex_list).map_err(|err| format!("{err}"))
            }
            Self::RemoteHttp(client) => {
                let hex_list: Vec<String> = block_hashes
                    .iter()
                    .map(|h| {
                        let mut s = String::with_capacity(64);
                        for b in h {
                            s.push_str(&format!("{b:02x}"));
                        }
                        s
                    })
                    .collect();
                client.lookup_block_prefix(namespace, &hex_list).map_err(|err| format!("{err}"))
            }
        }
    }

    /// Parallel block PUT. Mirrors `WombatKVKvStore::put_kv_chunked_s3`'s
    /// `thread::scope` fanout: K threads = K simultaneous S3 PUTs against
    /// the same backend. After each successful write the metadata index
    /// is updated so subsequent `lookup_block_prefix` reflects the new
    /// presence (root entry, zero parent/seq, the C ABI does not yet
    /// carry chain wiring).
    fn put_kv_blocks(
        &self,
        namespace: &str,
        block_hashes: &[BlockHash],
        payloads: &[&[u8]],
    ) -> Result<u64, String> {
        debug_assert_eq!(block_hashes.len(), payloads.len());
        let keys: Vec<String> = block_hashes.iter().map(block_key_for_hash).collect();
        match self {
            Self::Embedded(store) => {
                let store_ref = store.as_ref();
                let results: Vec<Result<(), String>> = std::thread::scope(|s| {
                    let handles: Vec<_> = keys
                        .iter()
                        .zip(payloads.iter())
                        .map(|(key, payload)| {
                            s.spawn(move || {
                                let bytes = Bytes::copy_from_slice(payload);
                                store_ref
                                    .put_kv(namespace, key, bytes)
                                    .map_err(|err| format!("put_kv {key}: {err}"))
                            })
                        })
                        .collect();
                    handles
                        .into_iter()
                        .map(|h| {
                            h.join()
                                .unwrap_or_else(|_| Err("put_kv_blocks thread panic".to_string()))
                        })
                        .collect()
                });
                for r in results {
                    r?;
                }
                let index = store.metadata_index();
                let mut total: u64 = 0;
                for (hash, payload) in block_hashes.iter().zip(payloads.iter()) {
                    let meta = BlockMeta::new_root(payload.len() as u64, [0u8; 24], [0u8; 16]);
                    index.insert(*hash, meta);
                    total = total.saturating_add(payload.len() as u64);
                }
                Ok(total)
            }
            Self::Remote(client) => {
                // Use the dedicated PUT_KV_BLOCKS_BATCH op instead of N
                // sequential put_kv calls: the daemon parallel-puts and, critically, updates its in-process metadata index
                // server-side so a subsequent
                // `wmbt_kv_lookup_block_prefix` returns the correct
                // `matched_count` for these hashes. The pre-batch path
                // wrote the bytes correctly but left the daemon's
                // metadata index empty, so the lookup leg always reported
                // 0 hits. RFC 0006 §6.
                //
                // We hex-encode hashes here (the wire format is 64-char
                // lower-hex strings, mirroring `block_key_for_hash`'s
                // suffix) and copy each payload into a `Vec<u8>`, the
                // rkyv-encoded request frame is owned end-to-end, no
                // cross-thread borrows.
                let hex_list: Vec<String> = block_hashes
                    .iter()
                    .map(|h| {
                        let mut s = String::with_capacity(64);
                        for b in h {
                            s.push_str(&format!("{b:02x}"));
                        }
                        s
                    })
                    .collect();
                client
                    .put_kv_blocks_batch(namespace, &hex_list, payloads)
                    .map_err(|err| format!("remote put_kv_blocks_batch: {err}"))
            }
            Self::RemoteTcp(client) => {
                let hex_list: Vec<String> = block_hashes
                    .iter()
                    .map(|h| {
                        let mut s = String::with_capacity(64);
                        for b in h {
                            s.push_str(&format!("{b:02x}"));
                        }
                        s
                    })
                    .collect();
                client
                    .put_kv_blocks_batch(namespace, &hex_list, payloads)
                    .map_err(|err| format!("tcp put_kv_blocks_batch: {err}"))
            }
            Self::RemoteHttp(client) => {
                let hex_list: Vec<String> = block_hashes
                    .iter()
                    .map(|h| {
                        let mut s = String::with_capacity(64);
                        for b in h {
                            s.push_str(&format!("{b:02x}"));
                        }
                        s
                    })
                    .collect();
                client
                    .put_kv_blocks_batch(namespace, &hex_list, payloads)
                    .map_err(|err| format!("http put_kv_blocks_batch: {err}"))
            }
        }
    }

    /// Block GET reusing `get_many_kv_batch`. Returns per-block `Bytes`
    /// slices in input order on hit, `None` on any miss. The encoded
    /// batch is decoded into a `Vec<Bytes>` so the C ABI can hand back
    /// N borrowed payload pointers, every slice shares the underlying
    /// `Bytes` Arc, so the `BorrowInner` only needs to hold the Vec.
    fn get_kv_blocks(
        &self,
        namespace: &str,
        block_hashes: &[BlockHash],
    ) -> Result<Option<Vec<Bytes>>, String> {
        // TCP / HTTP shortcut: ds4's hot path needs this and the daemon has a
        // dedicated GET_KV_BLOCKS_BATCH op. Bypass the encode/decode
        // round-trip the Embedded + Remote SHM paths use via
        // `get_many_kv_batch`.
        if let Self::RemoteTcp(client) = self {
            let hex_list: Vec<String> = block_hashes
                .iter()
                .map(|h| {
                    let mut s = String::with_capacity(64);
                    for b in h {
                        s.push_str(&format!("{b:02x}"));
                    }
                    s
                })
                .collect();
            return client
                .get_kv_blocks_batch(namespace, &hex_list)
                .map_err(|err| format!("tcp get_kv_blocks_batch: {err}"));
        }
        if let Self::RemoteHttp(client) = self {
            let hex_list: Vec<String> = block_hashes
                .iter()
                .map(|h| {
                    let mut s = String::with_capacity(64);
                    for b in h {
                        s.push_str(&format!("{b:02x}"));
                    }
                    s
                })
                .collect();
            return client
                .get_kv_blocks_batch(namespace, &hex_list)
                .map_err(|err| format!("http get_kv_blocks_batch: {err}"));
        }
        let keys: Vec<String> = block_hashes.iter().map(block_key_for_hash).collect();
        match self.get_many_kv_batch(namespace, &keys)? {
            None => Ok(None),
            Some(batch) => {
                let items = decode_bytes_batch(batch)
                    .map_err(|err| format!("decode_bytes_batch: {err}"))?;
                if items.len() != block_hashes.len() {
                    return Err(format!(
                        "get_kv_blocks decoded {} items, expected {}",
                        items.len(),
                        block_hashes.len()
                    ));
                }
                Ok(Some(items))
            }
        }
    }
}

/// Compose the content-addressed key for a block payload.
///
/// The chunked path uses `<base_key>:chunk:b3=<hex>` because each chunk
/// is tied to a parent monolithic blob's key. Block-prefix entries have no
/// parent (the hash *is* the address), so they live under a standalone
/// prefix. Producers and consumers MUST agree on this scheme; it is part
/// of the C ABI contract documented in `wombatkv.h`.
fn block_key_for_hash(hash: &BlockHash) -> String {
    let mut s = String::with_capacity(BLOCK_KEY_PREFIX.len() + 64);
    s.push_str(BLOCK_KEY_PREFIX);
    for b in hash {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Decode a 64-char lower-hex string into a 32-byte `BlockHash`. Mirrors
/// `slatedb_meta::decode_hex_32` but local to the FFI surface so we
/// don't depend on a private helper.
fn parse_block_hash_hex(hex: &str) -> Result<BlockHash, String> {
    let bytes = hex.as_bytes();
    if bytes.len() != 64 {
        return Err(format!("block hash hex must be 64 chars; got {} for {hex:?}", bytes.len()));
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        let hi = decode_hex_nibble(bytes[2 * i])
            .ok_or_else(|| format!("bad hex char at pos {} in {hex:?}", 2 * i))?;
        let lo = decode_hex_nibble(bytes[2 * i + 1])
            .ok_or_else(|| format!("bad hex char at pos {} in {hex:?}", 2 * i + 1))?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn decode_hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Parse a C array of NUL-terminated hex strings into `BlockHash`es.
fn parse_block_hash_array(
    block_hashes_hex: *const *const c_char,
    block_count: usize,
) -> Result<Vec<BlockHash>, String> {
    if block_count == 0 {
        return Ok(Vec::new());
    }
    if block_hashes_hex.is_null() {
        return Err("block_hashes_hex is NULL with block_count > 0".to_string());
    }
    let mut out = Vec::with_capacity(block_count);
    for i in 0..block_count {
        // SAFETY: the caller asserts `block_hashes_hex` points to
        // `block_count` valid `*const c_char` entries; each is a
        // NUL-terminated C string valid for the duration of the call.
        #[allow(unsafe_code)]
        let ptr = unsafe { *block_hashes_hex.add(i) };
        if ptr.is_null() {
            return Err(format!("block_hashes_hex[{i}] is NULL"));
        }
        // SAFETY: as above.
        #[allow(unsafe_code)]
        let cstr = unsafe { CStr::from_ptr(ptr) };
        let hex = cstr.to_str().map_err(|_| format!("block_hashes_hex[{i}] not UTF-8"))?;
        out.push(parse_block_hash_hex(hex)?);
    }
    Ok(out)
}

/// Public Rust handle. The cdylib hands an opaque `wmbt_kv_handle_t*` to
/// C; we cast it back to `&Handle` on every call.
pub struct Handle {
    backend: Backend,
    namespace: String,
    /// Optional background block-prefetch worker (RFC 0008 §6).
    /// Activated by `WMBT_KV_PREFETCH_INTERVAL_MS=<ms>` and only for the
    /// embedded backend (the remote backend has its own prefetch loop).
    /// Holding it here ties the worker's lifetime to the FFI handle so
    /// it is joined cleanly on `wmbt_kv_free`.
    _prefetch_worker: Option<wombatkv_node::block_prefetch::PrefetchWorker>,
    /// Optional background LRU eviction worker (RFC 0009 §4).
    /// Activated by `WMBT_KV_NAMESPACE_MAX_BYTES=<N>`. Off by default
    /// to preserve the existing unbounded-growth behavior for any
    /// deployment that hasn't explicitly opted in. Tied to the handle
    /// lifetime so it joins on `wmbt_kv_free`.
    _eviction_worker: Option<wombatkv_node::lru::LruEvictionWorker>,
}

impl Handle {
    /// Build from environment. Public so the rlib path can use it in
    /// tests without the C ABI dance.
    pub fn from_env() -> Result<Self, String> {
        // Alpha banner (suppressible with `WMBT_KV_QUIET_BANNER=1`) and
        // experimental-capability warnings emit once per process so
        // operators see the headline disclaimer plus any non-headline
        // feature flags they've opted into.
        emit_alpha_banner();
        emit_experimental_warnings();

        // Every WMBT_KV_* env var the cabi handle-init
        // consumes lands in CabiConfig::from_env. Single rustdoc
        // surface, single audit point. Defaults applied below.
        let cabi_cfg = crate::config::CabiConfig::from_env();
        let namespace = if cabi_cfg.namespace.is_empty() {
            DEFAULT_NAMESPACE.into()
        } else {
            cabi_cfg.namespace.clone()
        };

        if let Some(prefix) = cabi_cfg.remote_prefix.as_deref() {
            let client =
                RemoteKvStoreClient::connect_with_options(prefix, remote_client_options_from_env())
                    .map_err(|err| format!("RemoteKvStoreClient::connect({prefix}): {err}"))?;
            return Ok(Self {
                backend: Backend::Remote(Arc::new(client)),
                namespace,
                _prefetch_worker: None,
                _eviction_worker: None,
            });
        }

        // Cross-machine: ds4 (or any engine) on host A connects to a
        // wombatkv-daemon on host B over TCP. The daemon owns the foyer
        // + S3 backend; the engine's process just shuttles WireRequest
        // frames over a length-prefixed rkyv envelope. See RFC 0014 +
        // `wombatkv_daemon::tcp_transport`.
        if let Some(addr) = cabi_cfg.tcp_addr.as_deref() {
            return Self::from_env_tcp(addr);
        }

        // Same shape as WMBT_KV_TCP_ADDR but the daemon side speaks
        // HTTP/1.1 + rkyv instead of length-prefixed rkyv. Useful when
        // the engine sits behind an HTTP-aware proxy or load balancer.
        if let Some(addr) = cabi_cfg.http_addr.as_deref() {
            return Self::from_env_http(addr);
        }

        let s3_cfg =
            S3ObjectStoreConfig::from_env().map_err(|err| format!("S3 config: {err:?}"))?;
        let s3 = S3ObjectStore::new(s3_cfg).map_err(|err| format!("S3 new: {err:?}"))?;
        s3.ensure_bucket().map_err(|err| format!("ensure_bucket: {err:?}"))?;

        let mut foyer = FoyerCacheConfig::default();
        // 0.1.0-alpha: default to `~/.wombatkv/puffer` and auto-mkdir.
        // Explicit `WMBT_KV_PUFFER_DIR=<path>` overrides; empty string
        // (filtered by CabiConfig::from_env) falls back to the home-dir
        // default, there is no "off" for puffer dir.
        foyer.ssd_dir = cabi_cfg.puffer_dir.clone().unwrap_or_else(default_puffer_dir);
        if let Some(p) = cabi_cfg.puffer_ram_bytes {
            foyer.ram_bytes = p;
        }
        if let Some(p) = cabi_cfg.puffer_disk_bytes {
            foyer.ssd_bytes = p;
        }
        if let Some(p) = cabi_cfg.puffer_block_size_bytes {
            foyer.block_size = p;
        }
        foyer.iouring = false;

        // Keep a copy of the resolved puffer dir for downstream defaults
        // that anchor on it (SlateDB root, etc.). `foyer` itself is
        // about to be moved into `EmbedConfig`.
        let puffer_dir = foyer.ssd_dir.clone();

        let s3_prefix = cabi_cfg.s3_prefix.clone().unwrap_or_else(|| DEFAULT_S3_PREFIX.into());

        // Block-storage compression policy is env-driven. Default off so
        // an existing bucket sees no behavior change after upgrade; flip
        // on with `WMBT_KV_BLOCK_COMPRESS=zstd` once the writers and
        // readers are at the same version.
        let block_compression = wombatkv_node::compression::BlockCompressionConfig::from_env();
        if block_compression.is_enabled() {
            eprintln!(
                "wombatkv[compress]: block-storage compression enabled (algo={:?}, level={})",
                block_compression.algo, block_compression.level
            );
        }
        let cfg = EmbedConfig {
            s3_prefix,
            foyer,
            write_through_s3: true,
            compression: block_compression,
        };
        let store =
            WombatKVKvStore::new(cfg, s3).map_err(|err| format!("WombatKVKvStore::new: {err}"))?;
        // L1 SlateDB bootstrap (RFC 0008 §5 fast path): UNCONDITIONAL.
        //
        // SlateDB is the production metadata index. We open it on every
        // handle init; the slatedb root is `WMBT_KV_SLATEDB_PATH` if set,
        // otherwise `<puffer_dir>/slatedb`. Node identity comes from
        // `MYELON_NODE_ID` (default = hostname or "default-node").
        //
        // If SlateDB open fails (disk perms, etc.) we log and continue
        // without the L1 index, the in-memory L0 + world-knowledge
        // bootstrap below still hydrate the substrate from S3.
        //
        // RFC 0009: when the LRU eviction worker is enabled, it needs
        // a reference to the open SlateDB so it can keep L1 in sync
        // with L0 on evictions. We retain the Arc here regardless of
        // whether the worker eventually spawns; dropping the option
        // costs nothing.
        let slatedb_index: Option<Arc<wombatkv_radix::SlateDbMetadataIndex>> = {
            let slatedb_root =
                cabi_cfg.slatedb_path.clone().unwrap_or_else(|| puffer_dir.join("slatedb"));
            // MYELON_NODE_ID is myelon's env (not WMBT_KV_*) so it stays a
            // direct read here, see config.rs "What lives elsewhere".
            let node_id = std::env::var("MYELON_NODE_ID").unwrap_or_else(|_| hostname_or_default());
            match wombatkv_radix::SlateDbMetadataIndex::open_local(
                &slatedb_root,
                &node_id,
                &namespace,
            ) {
                Ok(slatedb_index) => {
                    match store.bootstrap_from_slatedb(&slatedb_index) {
                        Ok(n) => {
                            eprintln!(
                                "wombatkv[bootstrap]: hydrated {n} blocks from SlateDB at \
                                 {slatedb_root:?} for node_id={node_id:?} namespace={namespace:?}"
                            );
                        }
                        Err(err) => {
                            eprintln!(
                                "wombatkv[bootstrap]: slatedb load failed (continuing): {err}"
                            );
                        }
                    }
                    Some(Arc::new(slatedb_index))
                }
                Err(err) => {
                    eprintln!(
                        "wombatkv[bootstrap]: SlateDB open at {slatedb_root:?} failed \
                         (continuing without L1): {err}"
                    );
                    None
                }
            }
        };
        // World-knowledge bootstrap (RFC 0008 §5): UNCONDITIONAL.
        //
        // Indexes block keys at `wombatkv/v1/block/b3=<hex>` from the S3
        // namespace prefix. Runs after the SlateDB pass so any blocks not
        // yet persisted to SlateDB still get picked up. `insert` preserves
        // already-present hashes, so the two passes are idempotent.
        match store.bootstrap_world_knowledge(&namespace) {
            Ok(n) => {
                eprintln!(
                    "wombatkv[bootstrap]: indexed {n} blocks from S3 prefix \
                     for namespace={namespace:?}"
                );
            }
            Err(err) => {
                eprintln!("wombatkv[bootstrap]: world-knowledge load failed (continuing): {err}");
            }
        }

        let store_arc = Arc::new(store);

        // Background block-prefetch worker (RFC 0008 §6). Default-on:
        //   WMBT_KV_PREFETCH_INTERVAL_MS=<ms>       → default 30000; `=0` disables
        //   WMBT_KV_PREFETCH_TOP_K=<N>              → defaults to 8
        //   WMBT_KV_FINGERPRINT24=<48-hex chars>    → optional active-
        //                                            model digest for
        //                                            affinity scoring;
        //                                            zero-filled if unset
        //   WMBT_KV_PREFETCH_DRY_RUN=1              → v1 log-only escape
        //                                            hatch (consumed by
        //                                            `start_prefetcher`)
        //
        // v2 (default) issues `get_kv` for top-K candidates each cycle
        // and materializes their payloads into the local flat tier.
        let prefetch_worker = prefetch_worker_from_env(&store_arc, &namespace);

        // LRU eviction worker (RFC 0009 §4). Opt-in:
        //   WMBT_KV_NAMESPACE_MAX_BYTES=<N>          → enable; 0/unset
        //                                              disables (default)
        //   WMBT_KV_EVICTION_INTERVAL_SECS=<N>       → defaults to 30
        //
        // Default-off so existing deployments see no behavior change.
        // When enabled, the worker keeps S3 + L0 + L1 in sync; foyer
        // ages out naturally (its API has no public single-key remove).
        let eviction_worker =
            eviction_worker_from_env(&store_arc, &namespace, slatedb_index.clone());

        Ok(Self {
            backend: Backend::Embedded(store_arc),
            namespace,
            _prefetch_worker: prefetch_worker,
            _eviction_worker: eviction_worker,
        })
    }

    /// Build a TCP-backed handle connected to a remote
    /// `wombatkv-daemon` listening on `addr` (e.g. `203.0.113.5:7878`).
    /// Public so the rlib path can test cross-machine plumbing without
    /// the C-ABI dance.
    ///
    /// Daemon side: `wombatkv-daemon --tcp <addr>`.
    ///
    /// Errors propagate `TcpKvClient::connect` failures (refused,
    /// timeout, DNS, etc.).
    pub fn from_env_tcp(addr: &str) -> Result<Self, String> {
        emit_alpha_banner();
        emit_experimental_warnings();
        let namespace =
            std::env::var("WMBT_KV_NAMESPACE").unwrap_or_else(|_| DEFAULT_NAMESPACE.into());
        let client = wombatkv_daemon::tcp_transport::TcpKvClient::connect(addr)
            .map_err(|err| format!("TcpKvClient::connect({addr}): {err}"))?;
        Ok(Self {
            backend: Backend::RemoteTcp(Arc::new(client)),
            namespace,
            _prefetch_worker: None,
            _eviction_worker: None,
        })
    }

    /// Build an HTTP-backed handle connected to a remote
    /// `wombatkv-daemon` listening on `addr` (e.g. `203.0.113.5:7879`).
    /// Daemon side: `wombatkv-daemon --http <addr>`.
    ///
    /// Same WireRequest/WireResponse rkyv envelope as `from_env_tcp`,
    /// wrapped in HTTP/1.1 POSTs to `/wmbt/v1/rpc`. Use this when the
    /// path between engine and daemon goes through an HTTP-aware
    /// load balancer or reverse proxy.
    pub fn from_env_http(addr: &str) -> Result<Self, String> {
        emit_alpha_banner();
        emit_experimental_warnings();
        let namespace =
            std::env::var("WMBT_KV_NAMESPACE").unwrap_or_else(|_| DEFAULT_NAMESPACE.into());
        let client = wombatkv_daemon::http_transport::HttpKvClient::connect(addr)
            .map_err(|err| format!("HttpKvClient::connect({addr}): {err}"))?;
        Ok(Self {
            backend: Backend::RemoteHttp(Arc::new(client)),
            namespace,
            _prefetch_worker: None,
            _eviction_worker: None,
        })
    }

    /// Build from an explicit kvstore (test/integration use).
    #[must_use]
    pub fn from_kvstore(
        kvstore: Arc<WombatKVKvStore<S3ObjectStore>>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            backend: Backend::Embedded(kvstore),
            namespace: namespace.into(),
            _prefetch_worker: None,
            _eviction_worker: None,
        }
    }

    /// How many of `block_hashes` are present in the metadata index,
    /// counted from the start of the chain. Returns N when *all* hashes
    /// are present; returns 0 on a leading miss. See
    /// `InMemoryMetadataIndex::longest_prefix`.
    ///
    /// Backend dispatch:
    ///   - Embedded: direct in-process call into the local
    ///     `InMemoryMetadataIndex`.
    ///   - Remote: ships a `LOOKUP_BLOCK_PREFIX` opcode (rkyv-encoded) to
    ///     the daemon, which resolves against its own metadata index.
    ///     ABI 1.5+ on the daemon side is required.
    pub fn lookup_block_prefix(&self, block_hashes: &[BlockHash]) -> Result<usize, String> {
        self.backend.lookup_block_prefix_hex(&self.namespace, block_hashes)
    }

    /// Parallel PUT for a batch of content-addressed blocks.
    ///
    /// Each block lives at `wombatkv/v1/block/b3=<hex>` (the standalone
    /// content-addressed key scheme; see `block_key_for_hash`). Returns
    /// total bytes written on success.
    pub fn put_kv_blocks(
        &self,
        namespace: &str,
        block_hashes: &[BlockHash],
        payloads: &[&[u8]],
    ) -> Result<u64, String> {
        if block_hashes.len() != payloads.len() {
            return Err(format!(
                "put_kv_blocks length mismatch: {} hashes vs {} payloads",
                block_hashes.len(),
                payloads.len()
            ));
        }
        self.backend.put_kv_blocks(namespace, block_hashes, payloads)
    }

    /// Parallel GET for a batch of content-addressed blocks. Returns
    /// `Some(per-block bytes in input order)` on full hit, `None` on
    /// any miss (all-or-nothing, matches `get_many_kv_batch` semantics).
    pub fn get_kv_blocks(
        &self,
        namespace: &str,
        block_hashes: &[BlockHash],
    ) -> Result<Option<Vec<Bytes>>, String> {
        self.backend.get_kv_blocks(namespace, block_hashes)
    }

    /// PUT a raw-tail sidecar blob keyed by chain-tip block hash. The
    /// sidecar lives under `wombatkv/v1/sidecar/raw_tail/b3=<hex>`, distinct
    /// from the per-block `wombatkv/v1/block/...` keyspace so the block chain
    /// wire format remains untouched (RFC 0007 §10.P5 sidecar
    /// architecture).
    ///
    /// `chain_tip_hash_hex` MUST be a 64-character lower-hex blake3
    /// digest of the LAST block in the chain.
    pub fn put_raw_tail(
        &self,
        namespace: &str,
        chain_tip_hash_hex: &str,
        payload: &[u8],
    ) -> Result<usize, String> {
        if chain_tip_hash_hex.len() != 64 {
            return Err(format!(
                "put_raw_tail: chain_tip_hash_hex must be 64 chars; got {}",
                chain_tip_hash_hex.len()
            ));
        }
        let key = format!("{SIDECAR_RAW_TAIL_KEY_PREFIX}{chain_tip_hash_hex}");
        self.backend.put_kv(namespace, &key, Bytes::copy_from_slice(payload))?;
        Ok(payload.len())
    }

    /// GET a raw-tail sidecar blob keyed by chain-tip block hash. Returns
    /// `Ok(None)` on miss, the caller falls back to the slow re-prefill
    /// path. The sidecar is purely an optimization; absence is not an
    /// error.
    pub fn get_raw_tail(
        &self,
        namespace: &str,
        chain_tip_hash_hex: &str,
    ) -> Result<Option<Bytes>, String> {
        if chain_tip_hash_hex.len() != 64 {
            return Err(format!(
                "get_raw_tail: chain_tip_hash_hex must be 64 chars; got {}",
                chain_tip_hash_hex.len()
            ));
        }
        let key = format!("{SIDECAR_RAW_TAIL_KEY_PREFIX}{chain_tip_hash_hex}");
        self.backend.get_kv(namespace, &key)
    }
}

/// Build a `PrefetchWorker` from environment. Defaults to a 30-second
/// interval when `WMBT_KV_PREFETCH_INTERVAL_MS` is unset; opt out with `=0`.
///
/// Envs:
///   `WMBT_KV_PREFETCH_INTERVAL_MS` = <ms>            default 30000; `=0` disables
///   `WMBT_KV_PREFETCH_TOP_K`   = <N>                default 8
///   `WMBT_KV_FINGERPRINT24`    = <48-hex chars>     optional active-model
///                                                 digest for affinity
///                                                 scoring (zero-filled
///                                                 when unset/malformed)
///   `WMBT_KV_PREFETCH_DRY_RUN` = 1                  v1 log-only escape
///                                                 hatch (consumed by
///                                                 `start_prefetcher`)
fn prefetch_worker_from_env(
    store: &Arc<WombatKVKvStore<S3ObjectStore>>,
    namespace: &str,
) -> Option<wombatkv_node::block_prefetch::PrefetchWorker> {
    const DEFAULT_PREFETCH_INTERVAL_MS: u64 = 30_000;
    let interval_ms =
        env_parse::<u64>("WMBT_KV_PREFETCH_INTERVAL_MS").unwrap_or(DEFAULT_PREFETCH_INTERVAL_MS);
    if interval_ms == 0 {
        return None;
    }
    // Default to 64 so a typical cell-B chain (up to ~50 blocks for a
    // 6k-token context) is fully covered by one prefetch cycle. Smaller
    // values left 5+ blocks falling through to S3 GETs on the prompt
    // path and capped the cell-B speedup; see v0.1.0-alpha.2 bench
    // notes (1300 tokens needed top_k=13, 4842 tokens needed top_k=40).
    let top_k = env_parse::<usize>("WMBT_KV_PREFETCH_TOP_K").unwrap_or(64);
    let model_digest = model_digest_from_env();
    let dry_run = wombatkv_node::block_prefetch::dry_run_enabled();
    let cfg = wombatkv_node::block_prefetch::PrefetchConfig {
        interval: Duration::from_millis(interval_ms),
        top_k,
        model_digest,
        namespace: namespace.to_string(),
    };
    eprintln!(
        "wombatkv[prefetch]: starting block prefetch worker \
         (interval_ms={interval_ms}, top_k={top_k}, namespace={namespace:?}, \
         dry_run={dry_run})"
    );
    Some(store.start_prefetcher(cfg))
}

/// Build + spawn the LRU eviction worker from env. Returns `None`
/// when `WMBT_KV_NAMESPACE_MAX_BYTES` is unset, 0, or unparseable -
/// the default-off safe state.
///
/// Reads:
///   `WMBT_KV_NAMESPACE_MAX_BYTES`    = <N>     enable per-namespace cap
///   `WMBT_KV_EVICTION_INTERVAL_SECS` = <N>     default 30
fn eviction_worker_from_env(
    store: &Arc<WombatKVKvStore<S3ObjectStore>>,
    namespace: &str,
    slatedb: Option<Arc<wombatkv_radix::SlateDbMetadataIndex>>,
) -> Option<wombatkv_node::lru::LruEvictionWorker> {
    let cfg = wombatkv_node::lru::LruConfig::from_env(namespace)?;
    eprintln!(
        "wombatkv[lru]: starting eviction worker \
         (namespace_max_bytes={}, interval_secs={}, namespace={:?}, \
         slatedb_attached={})",
        cfg.namespace_max_bytes,
        cfg.interval.as_secs(),
        cfg.namespace,
        slatedb.is_some()
    );
    Some(store.start_eviction_worker(cfg, slatedb))
}

/// Parse `WMBT_KV_FINGERPRINT24` as a 48-char lower-hex string into a
/// 24-byte model digest. Zero-filled on absence or parse failure (in
/// which case affinity scoring is a no-op, every block scores the
/// same on the model dimension).
fn model_digest_from_env() -> [u8; 24] {
    let Some(hex) = std::env::var("WMBT_KV_FINGERPRINT24").ok() else {
        return [0u8; 24];
    };
    if hex.len() != 48 {
        eprintln!(
            "wombatkv[prefetch]: WMBT_KV_FINGERPRINT24 must be 48 hex chars; \
             got {} chars, using zero digest",
            hex.len()
        );
        return [0u8; 24];
    }
    let bytes = hex.as_bytes();
    let mut out = [0u8; 24];
    for i in 0..24 {
        let Some(hi) = decode_hex_nibble(bytes[2 * i]) else {
            eprintln!(
                "wombatkv[prefetch]: WMBT_KV_FINGERPRINT24 has bad hex char at \
                 pos {}, using zero digest",
                2 * i
            );
            return [0u8; 24];
        };
        let Some(lo) = decode_hex_nibble(bytes[2 * i + 1]) else {
            eprintln!(
                "wombatkv[prefetch]: WMBT_KV_FINGERPRINT24 has bad hex char at \
                 pos {}, using zero digest",
                2 * i + 1
            );
            return [0u8; 24];
        };
        out[i] = (hi << 4) | lo;
    }
    out
}

fn remote_client_options_from_env() -> ClientOptions {
    let mut opts = ClientOptions::default();
    if let Some(depth) = env_parse::<usize>("WMBT_KV_DAEMON_SHM_DEPTH") {
        opts.depth = depth;
    }
    if let Some(timeout_ms) = env_parse::<u64>("WMBT_KV_REMOTE_CALL_TIMEOUT_MS")
        .or_else(|| env_parse::<u64>("WMBT_KV_DAEMON_SHM_CALL_TIMEOUT_MS"))
    {
        opts.call_timeout = Duration::from_millis(timeout_ms);
    }
    opts
}

fn env_parse<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name).ok().and_then(|value| value.parse::<T>().ok())
}

/// Read a boolean-style env var with a default-on policy for the
/// 0.1.0-alpha core-feature flags.
///
/// - Missing → returns `true` (alpha default-on).
/// - Empty string OR any value parsed as "falsy" → returns `false`
///   (explicit opt-out).
/// - Truthy values (`1`, `true`, `yes`, `on`, case-insensitive) →
///   returns `true`.
/// - Anything else → returns `false` so a typo can't silently leave
///   the feature on if the user clearly tried to disable it.
fn env_bool_default_on(name: &str) -> bool {
    match std::env::var(name) {
        Err(_) => true,
        Ok(raw) => {
            let s = raw.trim().to_ascii_lowercase();
            matches!(s.as_str(), "1" | "true" | "yes" | "on")
        }
    }
}

/// Emit the 0.1.0-alpha banner once per process, unless the user has
/// suppressed it with `WMBT_KV_QUIET_BANNER=1`.
///
/// The banner is intentionally a single line so it doesn't clutter
/// llama.cpp/ds4 startup output and matches the alpha vocabulary used
/// in the README. Mac-anchored because the headline validation matrix
/// (5-session, 5-cell cross-restart showcase) was run on M3/M4 with
/// native `MinIO`; `Linux+io_uring` and cloud-S3 are the next milestones.
static BANNER_EMITTED: std::sync::OnceLock<()> = std::sync::OnceLock::new();

fn emit_alpha_banner() {
    if BANNER_EMITTED.set(()).is_err() {
        return;
    }
    if matches!(
        std::env::var("WMBT_KV_QUIET_BANNER").ok().as_deref(),
        Some("1" | "true" | "yes" | "on")
    ) {
        return;
    }
    eprintln!(
        "WombatKV 0.1.0-alpha: validated on macOS M3/M4 + native MinIO. \
         Linux io_uring + cloud-S3 are beta milestones."
    );
}

/// Emit one-line stderr warnings for any experimental 0.1.0-alpha
/// capabilities the user has explicitly opted into.
///
/// Reduced surface in alpha, zstd compression and the prefetch worker
/// graduated out of "experimental" (zstd is a production-grade
/// byte-pass-through codec with verified round-trip; prefetch is what
/// anyone opting into `WombatKV` wants anyway). They stay as opt-in env
/// vars but no longer print a warning.
///
/// What remains experimental:
/// - `WMBT_KV_NAMESPACE_MAX_BYTES > 0`     → LRU eviction (RFC 0009), state-mutating; data-loss risk if buggy
///
/// Emits once per process via `EXPERIMENTAL_WARNINGS_EMITTED`; subsequent
/// `Handle::from_env` calls (e.g. inside test loops) stay silent.
static EXPERIMENTAL_WARNINGS_EMITTED: std::sync::OnceLock<()> = std::sync::OnceLock::new();

fn emit_experimental_warnings() {
    if EXPERIMENTAL_WARNINGS_EMITTED.set(()).is_err() {
        return;
    }
    // LRU, gated by a positive byte budget.
    let lru_on = std::env::var("WMBT_KV_NAMESPACE_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .is_some_and(|n| n > 0);
    if lru_on {
        eprintln!(
            "WombatKV: LRU eviction is experimental in 0.1.0-alpha; \
             please report behavior at https://github.com/Venkat2811/wombatkv/issues"
        );
    }
}

/// Resolve the default puffer directory under the user's home.
///
/// Alpha default: `~/.wombatkv/puffer` (auto-mkdir).
///
/// Falls back to `DEFAULT_FOYER_DIR` (`/tmp/...`) if the home dir is
/// unknown or `mkdir -p` fails. The previous default (`/tmp/...`) is
/// preserved as the fallback so we never harder-fail on a path the
/// user did not configure.
fn default_puffer_dir() -> PathBuf {
    // $HOME is the canonical POSIX home variable; macOS + Linux both
    // set it. We avoid the `dirs` crate to keep the wombatkv-cabi
    // dep tree minimal, this resolution only runs once per
    // Handle::from_env.
    let home = std::env::var("HOME").ok().filter(|s| !s.is_empty());
    let candidate = match home {
        Some(h) => PathBuf::from(h).join(".wombatkv").join("puffer"),
        None => return PathBuf::from(DEFAULT_FOYER_DIR),
    };
    match std::fs::create_dir_all(&candidate) {
        Ok(()) => candidate,
        Err(err) => {
            eprintln!(
                "wombatkv: could not create default puffer dir {candidate:?} \
                 ({err}); falling back to {DEFAULT_FOYER_DIR}"
            );
            PathBuf::from(DEFAULT_FOYER_DIR)
        }
    }
}

/// Resolve a node-id default when `MYELON_NODE_ID` is unset. Prefers
/// `HOSTNAME` (Linux shells), then `COMPUTERNAME` (Windows), then the
/// literal `default-node`. Avoids pulling in a `hostname` crate because
/// `SlateDB` only treats this as an opaque path component.
fn hostname_or_default() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "default-node".to_string())
}

// ============================================================
// extern "C" surface
// ============================================================

/// Opaque handle (named `wmbt_kv_handle` in the C header).
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct wmbt_kv_handle {
    _private: [u8; 0],
}

/// Pack ABI version as `(major << 16) | minor`.
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_abi_version() -> u32 {
    (u32::from(ABI_MAJOR) << 16) | u32::from(ABI_MINOR)
}

/// Compute a 64-hex-char (32-byte) blake3 digest of the input. Output
/// buffer MUST be at least 65 bytes (64 hex chars + NUL terminator).
///
/// Designed to drop into ds4's `kvblock_compute_hashes` in place of the
/// hand-rolled `sha1_64hex` (which composes two sha1 calls to produce
/// 32 bytes). blake3 is ~5-10× faster on short inputs (SIMD on aarch64
/// via the `blake3` crate's aarch64-neon path) and is a proper
/// cryptographically-keyed hash family rather than a domain-extension
/// hack. RFC 0011 §C5; RFC 0013 §6 for the broader content-addressing
/// rationale.
///
/// Returns 0 on success, -1 on null arg.
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_blake3_64hex(
    in_ptr: *const u8,
    in_len: usize,
    out_ptr: *mut std::os::raw::c_char,
) -> i32 {
    if in_ptr.is_null() || out_ptr.is_null() {
        return -1;
    }
    // SAFETY: caller guarantees `in_ptr` points to in_len readable bytes
    // and `out_ptr` points to a ≥65-byte writable buffer (documented).
    let input = unsafe { std::slice::from_raw_parts(in_ptr, in_len) };
    let mut hasher = blake3::Hasher::new();
    hasher.update(input);
    let digest = hasher.finalize(); // 32 bytes
    let hex_chars = b"0123456789abcdef";
    let mut buf = [0u8; 65];
    for (i, b) in digest.as_bytes().iter().enumerate() {
        buf[i * 2] = hex_chars[(b >> 4) as usize];
        buf[i * 2 + 1] = hex_chars[(b & 0x0f) as usize];
    }
    buf[64] = 0;
    // SAFETY: out_ptr has been declared ≥65 bytes by the caller contract.
    unsafe {
        std::ptr::copy_nonoverlapping(buf.as_ptr(), out_ptr.cast::<u8>(), 65);
    }
    0
}

/// One-shot CRC32C (Castagnoli polynomial) over `in_len` bytes at
/// `in_ptr`. Equivalent to `wmbt_kv_crc32c_append(0, in_ptr, in_len)`.
///
/// Returns the finalized CRC32C value. A null `in_ptr` with `in_len == 0`
/// is treated as the empty input (returns 0). A null `in_ptr` with
/// `in_len != 0` returns 0 (caller error).
///
/// Implementation: dispatches to hardware CRC32C at runtime via the
/// `crc32c` Rust crate, x86_64 SSE4.2 (Intel Nehalem+, AMD
/// Bulldozer+), ARMv8.1 CRC32 (Apple Silicon, AWS Graviton, Ampere),
/// or a software-table fallback. Engines (ds4, vLLM, SGLang, ...)
/// route their envelope-CRC verification paths through this so each
/// integration inherits hardware acceleration without re-implementing
/// the ifdef ladder per language.
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_crc32c(in_ptr: *const u8, in_len: usize) -> u32 {
    if in_len == 0 {
        return 0;
    }
    if in_ptr.is_null() {
        return 0;
    }
    // SAFETY: caller guarantees `in_ptr` points to `in_len` readable bytes.
    let bytes = unsafe { std::slice::from_raw_parts(in_ptr, in_len) };
    crc32c::crc32c(bytes)
}

/// Append `in_len` bytes at `in_ptr` to a running CRC32C accumulator
/// and return the new accumulator value. Use to chain multiple buffers
/// into a single CRC32C without forcing the caller to materialise them
/// contiguously. `crc` is the accumulator from the previous call (or 0
/// to start fresh).
///
/// A null `in_ptr` with `in_len == 0` returns `crc` unchanged. A null
/// `in_ptr` with `in_len != 0` returns `crc` unchanged (caller error).
///
/// Same hardware-dispatch story as [`wmbt_kv_crc32c`].
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_crc32c_append(crc: u32, in_ptr: *const u8, in_len: usize) -> u32 {
    if in_len == 0 || in_ptr.is_null() {
        return crc;
    }
    // SAFETY: caller guarantees `in_ptr` points to `in_len` readable bytes.
    let bytes = unsafe { std::slice::from_raw_parts(in_ptr, in_len) };
    crc32c::crc32c_append(crc, bytes)
}

/// Construct a handle from environment variables. Returns NULL on
/// error; inspect `wmbt_kv_last_error()` for the message.
///
/// The returned pointer must be released with [`wmbt_kv_free`].
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_init_from_env() -> *mut wmbt_kv_handle {
    clear_last_error();
    let outcome = catch_unwind(AssertUnwindSafe(|| match Handle::from_env() {
        Ok(h) => {
            let boxed = Box::new(h);
            // SAFETY: Box::into_raw produces a valid heap pointer that
            // we retain ownership over until wmbt_kv_free; we cast through
            // the opaque `wmbt_kv_handle` type so C never sees the inner
            // layout.
            #[allow(unsafe_code)]
            let ptr = Box::into_raw(boxed).cast::<wmbt_kv_handle>();
            ptr
        }
        Err(msg) => {
            set_last_error(msg);
            std::ptr::null_mut()
        }
    }));
    if let Ok(ptr) = outcome {
        ptr
    } else {
        set_last_error("panic in wmbt_kv_init_from_env");
        std::ptr::null_mut()
    }
}

/// Construct a handle that talks to a remote `wombatkv-daemon` over
/// TCP. `addr` is a NUL-terminated "host:port" string (e.g.
/// `"203.0.113.5:7878"`). The daemon must be running with
/// `wombatkv-daemon --tcp <addr>`.
///
/// Returns NULL on connect failure; inspect [`wmbt_kv_last_error()`].
/// Released via [`wmbt_kv_free`] like any other handle.
///
/// Equivalent to setting `WMBT_KV_TCP_ADDR=<addr>` in the env and
/// calling [`wmbt_kv_init_from_env`]. The explicit constructor exists
/// so ds4 (and other C-callable engines) can pick TCP without
/// going through env mutation.
///
/// Cross-machine architecture (RFC 0014):
///   engine on host A → libwombatkv.dylib (this handle) → TCP → \
///       wombatkv-daemon on host B → foyer + S3.
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_open_tcp(addr: *const c_char) -> *mut wmbt_kv_handle {
    clear_last_error();
    if addr.is_null() {
        set_last_error("wmbt_kv_open_tcp: addr is NULL");
        return std::ptr::null_mut();
    }
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: caller contract, addr is a NUL-terminated C string.
        // We do not retain the pointer past this scope.
        #[allow(unsafe_code)]
        let addr_str = unsafe { std::ffi::CStr::from_ptr(addr) };
        let addr_str = match addr_str.to_str() {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!("wmbt_kv_open_tcp: addr not utf-8: {e}"));
                return std::ptr::null_mut();
            }
        };
        match Handle::from_env_tcp(addr_str) {
            Ok(h) => {
                let boxed = Box::new(h);
                #[allow(unsafe_code)]
                let ptr = Box::into_raw(boxed).cast::<wmbt_kv_handle>();
                ptr
            }
            Err(msg) => {
                set_last_error(msg);
                std::ptr::null_mut()
            }
        }
    }));
    if let Ok(ptr) = outcome {
        ptr
    } else {
        set_last_error("panic in wmbt_kv_open_tcp");
        std::ptr::null_mut()
    }
}

/// Construct an HTTP-backed handle for the given `addr` ("host:port",
/// NUL-terminated). Daemon side must be running
/// `wombatkv-daemon --http <addr>`. Equivalent to setting
/// `WMBT_KV_HTTP_ADDR=<addr>` in the env and calling
/// [`wmbt_kv_init_from_env`] but lets engines pick HTTP without
/// mutating env vars.
///
/// Cross-machine architecture (RFC 0014 + this RFC):
///   engine on host A → libwombatkv.dylib (this handle) → HTTP/1.1 → \
///       reverse-proxy / load balancer → wombatkv-daemon on host B →
///       foyer + S3.
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_open_http(addr: *const c_char) -> *mut wmbt_kv_handle {
    clear_last_error();
    if addr.is_null() {
        set_last_error("wmbt_kv_open_http: addr is NULL");
        return std::ptr::null_mut();
    }
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: caller contract, addr is a NUL-terminated C string.
        // We do not retain the pointer past this scope.
        #[allow(unsafe_code)]
        let addr_str = unsafe { std::ffi::CStr::from_ptr(addr) };
        let addr_str = match addr_str.to_str() {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!("wmbt_kv_open_http: addr not utf-8: {e}"));
                return std::ptr::null_mut();
            }
        };
        match Handle::from_env_http(addr_str) {
            Ok(h) => {
                let boxed = Box::new(h);
                #[allow(unsafe_code)]
                let ptr = Box::into_raw(boxed).cast::<wmbt_kv_handle>();
                ptr
            }
            Err(msg) => {
                set_last_error(msg);
                std::ptr::null_mut()
            }
        }
    }));
    if let Ok(ptr) = outcome {
        ptr
    } else {
        set_last_error("panic in wmbt_kv_open_http");
        std::ptr::null_mut()
    }
}

/// Release a handle constructed by [`wmbt_kv_init_from_env`]. Safe to
/// pass NULL.
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_free(handle: *mut wmbt_kv_handle) {
    if handle.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: handle was produced by Box::into_raw on a Handle
        // boxed by `wmbt_kv_init_from_env`. The C caller is contractually
        // required to pass each handle exactly once.
        #[allow(unsafe_code)]
        unsafe {
            drop(Box::from_raw(handle.cast::<Handle>()));
        }
    }));
}

/// Opaque borrow handle returned by the zero-copy `*_borrowed` getter
/// functions. The C caller treats it as an opaque cookie and passes it
/// back to `wmbt_kv_release_borrow` when done reading the payload.
#[repr(C)]
pub struct wmbt_kv_borrow {
    _private: [u8; 0],
}

/// Inner storage for a `wmbt_kv_borrow`. Boxed and heap-leaked; the
/// caller owns the box and must release it via `wmbt_kv_release_borrow`.
/// Holds a `Bytes` so the uncompressed path can keep foyer's Arc-shared
/// payload without an extra heap allocation.
///
/// The `_multi` / `_ptrs` / `_lens` fields exist for block-shaped borrows
/// (see `wmbt_kv_get_kv_blocks_borrowed`) that hand back N independent
/// payload pointers in a single borrow. They are empty for single-key
/// borrows. All fields are dropped together when the C caller releases
/// the borrow.
struct BorrowInner {
    data: Bytes,
    /// Per-block payloads (empty for single-key borrows). Each `Bytes`
    /// keeps its underlying buffer alive until `BorrowInner` drops; the
    /// pointers in `_ptrs` index into these.
    _multi: Vec<Bytes>,
    /// C-visible array of per-block payload pointers (one per `_multi`
    /// entry). Stored here so the array memory stays valid as long as
    /// the borrow does.
    _ptrs: Vec<*const u8>,
    /// C-visible array of per-block payload lengths.
    _lens: Vec<usize>,
}

/// Release a borrow obtained from any of the `*_borrowed` functions
/// (`wmbt_kv_get_kv_blocks_borrowed`, `wmbt_kv_get_raw_tail_borrowed`).
/// Safe to pass NULL.
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_release_borrow(borrow: *mut wmbt_kv_borrow) {
    if borrow.is_null() {
        return;
    }
    let _outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: borrow is the same raw pointer Box::into_raw produced
        // in one of the *_borrowed extern fns, so Box::from_raw reclaims
        // ownership and drops the Vec.
        #[allow(unsafe_code)]
        unsafe {
            drop(Box::from_raw(borrow.cast::<BorrowInner>()));
        }
    }));
}

// ============================================================
// Block-shaped surfaces: ABI 1.5
// ============================================================
//
// These wrap the metadata-index and batched-IO paths for callers that
// already own a content-addressed blocks (vLLM connector, SGLang
// connector, ds4_server.s block-prefix path). Each block is identified by a
// 32-byte blake3 hash passed as a 64-char lower-hex C string; objects
// live at `wombatkv/v1/block/b3=<hex>` (see `block_key_for_hash`).

/// Walk a chain of block hashes and report how many leading hashes are
/// present in the in-memory metadata index.
///
/// `*out_matched_count` is the number of hashes, counted from index 0, that are present in the index. Stops at the first miss. The
/// embedded backend serves this from `InMemoryMetadataIndex`; the
/// remote backend returns `-1` with "`lookup_block_prefix` not supported
/// on remote backend yet".
///
/// Returns:
///   0  success; `*out_matched_count` set.
///  -1  error; see `err_buf` (if provided) or `wmbt_kv_last_error()`.
///
/// `err_buf` is optional. When non-NULL and `err_len > 0`, the most
/// recent error message is copied (NUL-terminated, truncated if needed)
/// into that buffer. The thread-local last-error slot is also set, so
/// `wmbt_kv_last_error()` remains a valid fallback.
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_lookup_block_prefix(
    handle: *mut wmbt_kv_handle,
    namespace: *const c_char,
    block_hashes_hex: *const *const c_char,
    block_count: usize,
    out_matched_count: *mut usize,
    err_buf: *mut c_char,
    err_len: usize,
) -> i32 {
    clear_last_error();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if out_matched_count.is_null() {
            return record_block_error("out_matched_count is NULL", err_buf, err_len);
        }
        let h = match handle_ref(handle) {
            Ok(h) => h,
            Err(()) => return copy_last_error_to(err_buf, err_len),
        };
        // namespace is parsed for parity with the other block-shaped
        // calls (and to surface NULL/UTF-8 errors here rather than at
        // an internal layer), but the metadata index is process-global, we accept any namespace string.
        let _ns = match cstr_to_str(namespace) {
            Ok(s) => s,
            Err(()) => return copy_last_error_to(err_buf, err_len),
        };
        let hashes = match parse_block_hash_array(block_hashes_hex, block_count) {
            Ok(v) => v,
            Err(msg) => return record_block_error(&msg, err_buf, err_len),
        };
        match h.lookup_block_prefix(&hashes) {
            Ok(matched) => {
                // SAFETY: out_matched_count is non-NULL (checked above)
                // and points at writable storage per the C contract.
                #[allow(unsafe_code)]
                unsafe {
                    *out_matched_count = matched;
                }
                0
            }
            Err(msg) => record_block_error(&msg, err_buf, err_len),
        }
    }));
    match outcome {
        Ok(rc) => rc,
        Err(_) => record_block_error("panic in wmbt_kv_lookup_block_prefix", err_buf, err_len),
    }
}

/// Parallel GET for a batch of content-addressed blocks.
///
/// On full hit: writes N borrowed payload pointers + N lengths into the
/// caller's arrays and an opaque release handle into `*out_borrow`. The
/// pointers are valid until `wmbt_kv_release_borrow(*out_borrow)`.
///
/// All-or-nothing semantics: a single missing block returns 0 (miss).
/// Callers can then fall back to a cold-fetch path.
///
/// Block keys are derived from each hash as `wombatkv/v1/block/b3=<hex>` -
/// the C ABI's standalone content-addressed scheme.
///
/// Returns:
///   1  all blocks hit; outputs set.
///   0  at least one miss.
///  -1  error; see `wmbt_kv_last_error()`.
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_get_kv_blocks_borrowed(
    handle: *mut wmbt_kv_handle,
    namespace: *const c_char,
    block_hashes_hex: *const *const c_char,
    block_count: usize,
    out_payload_ptrs: *mut *const *const u8,
    out_payload_lens: *mut *const usize,
    out_borrow: *mut *mut wmbt_kv_borrow,
) -> i32 {
    clear_last_error();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if out_payload_ptrs.is_null() || out_payload_lens.is_null() || out_borrow.is_null() {
            set_last_error("out_payload_ptrs / out_payload_lens / out_borrow must not be NULL");
            return -1;
        }
        let h = match handle_ref(handle) {
            Ok(h) => h,
            Err(()) => return -1,
        };
        let ns = match cstr_to_str(namespace) {
            Ok(s) => s,
            Err(()) => return -1,
        };
        let hashes = match parse_block_hash_array(block_hashes_hex, block_count) {
            Ok(v) => v,
            Err(msg) => {
                set_last_error(msg);
                return -1;
            }
        };
        match h.get_kv_blocks(ns, &hashes) {
            Ok(None) => 0,
            Ok(Some(items)) => {
                publish_borrowed_blocks(items, out_payload_ptrs, out_payload_lens, out_borrow)
            }
            Err(msg) => {
                set_last_error(msg);
                -1
            }
        }
    }));
    if let Ok(rc) = outcome {
        rc
    } else {
        set_last_error("panic in wmbt_kv_get_kv_blocks_borrowed");
        -1
    }
}

/// Parallel PUT for a batch of content-addressed blocks.
///
/// Each block lives at `wombatkv/v1/block/b3=<hex>` (the standalone scheme
/// matching `wmbt_kv_get_kv_blocks_borrowed`). The metadata index is
/// updated after every successful write so a subsequent
/// `wmbt_kv_lookup_block_prefix` sees the new presence.
///
/// Returns total bytes written across all blocks on success, `-1` on
/// error. Partial failures leave the metadata index unchanged for the
/// failed batch (the index update only runs after every per-key PUT
/// succeeds).
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_put_kv_blocks(
    handle: *mut wmbt_kv_handle,
    namespace: *const c_char,
    block_hashes_hex: *const *const c_char,
    payloads: *const *const u8,
    lens: *const usize,
    block_count: usize,
) -> i64 {
    clear_last_error();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let h = match handle_ref(handle) {
            Ok(h) => h,
            Err(()) => return -1,
        };
        let ns = match cstr_to_str(namespace) {
            Ok(s) => s,
            Err(()) => return -1,
        };
        let hashes = match parse_block_hash_array(block_hashes_hex, block_count) {
            Ok(v) => v,
            Err(msg) => {
                set_last_error(msg);
                return -1;
            }
        };
        if block_count == 0 {
            return 0;
        }
        if payloads.is_null() || lens.is_null() {
            set_last_error("payloads / lens must not be NULL with block_count > 0");
            return -1;
        }
        let mut payload_slices: Vec<&[u8]> = Vec::with_capacity(block_count);
        for i in 0..block_count {
            // SAFETY: caller asserts `payloads`/`lens` point to
            // `block_count` valid entries; each `payloads[i]` is a
            // pointer to `lens[i]` valid bytes for the duration of
            // the call.
            #[allow(unsafe_code)]
            let payload_ptr = unsafe { *payloads.add(i) };
            #[allow(unsafe_code)]
            let len = unsafe { *lens.add(i) };
            if len == 0 {
                payload_slices.push(&[]);
                continue;
            }
            if payload_ptr.is_null() {
                set_last_error(format!("payloads[{i}] is NULL with len > 0"));
                return -1;
            }
            #[allow(unsafe_code)]
            let s = unsafe { std::slice::from_raw_parts(payload_ptr, len) };
            payload_slices.push(s);
        }
        match h.put_kv_blocks(ns, &hashes, &payload_slices) {
            Ok(total) => i64::try_from(total).unwrap_or(i64::MAX),
            Err(msg) => {
                set_last_error(msg);
                -1
            }
        }
    }));
    if let Ok(rc) = outcome {
        rc
    } else {
        set_last_error("panic in wmbt_kv_put_kv_blocks");
        -1
    }
}

// ============================================================
// Raw-tail sidecar: ABI 1.6 (RFC 0007 §10.P5 sidecar architecture)
// ============================================================
//
// One sidecar object per unique chain-tip block hash, keyed under
// `wombatkv/v1/sidecar/raw_tail/b3=<chain_tip_hex>`. Storage cost is
// O(unique chain-tips), not O(blocks). The block chain itself is
// unchanged: `wmbt_kv_put_kv_blocks` / `wmbt_kv_get_kv_blocks_borrowed`
// continue to round-trip byte-identical v1 envelopes.
//
// Producer flow (after wmbt_kv_put_kv_blocks succeeds for N blocks):
//   wmbt_kv_put_raw_tail(handle, ns, chain[N-1], raw_bytes, len, ...)
//
// Consumer flow (after wmbt_kv_get_kv_blocks_borrowed + load_blocks for
// matched=M blocks):
//   wmbt_kv_get_raw_tail_borrowed(handle, ns, chain[M-1], &p, &len,
//                                 &borrow, err, errlen)
// On hit, install raw bytes into the engine's SWA ring and skip the
// post-load re-prefill of the last DS4_N_SWA tokens.

/// PUT a raw-tail sidecar payload keyed by `chain_tip_hash_hex` (64-char
/// lower-hex blake3 digest of the LAST block in the saved chain).
///
/// Internally stores the value at
/// `wombatkv/v1/sidecar/raw_tail/b3=<chain_tip_hash_hex>` inside the given
/// namespace. The chain itself (`wombatkv/v1/block/...`) is untouched.
///
/// `err_buf` is optional. When non-NULL and `err_len > 0`, the most
/// recent error message is copied (NUL-terminated, truncated if needed)
/// into that buffer. The thread-local last-error slot is also set, so
/// `wmbt_kv_last_error()` remains a valid fallback.
///
/// Returns:
///    0   success.
///   -1   error; see `err_buf` / `wmbt_kv_last_error()`.
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_put_raw_tail(
    handle: *mut wmbt_kv_handle,
    namespace: *const c_char,
    chain_tip_hash_hex: *const c_char,
    bytes: *const u8,
    len: usize,
    err_buf: *mut c_char,
    err_len: usize,
) -> i32 {
    clear_last_error();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let h = match handle_ref(handle) {
            Ok(h) => h,
            Err(()) => return copy_last_error_to(err_buf, err_len),
        };
        let ns = match cstr_to_str(namespace) {
            Ok(s) => s,
            Err(()) => return copy_last_error_to(err_buf, err_len),
        };
        let hex = match cstr_to_str(chain_tip_hash_hex) {
            Ok(s) => s,
            Err(()) => return copy_last_error_to(err_buf, err_len),
        };
        let payload = match slice_from_raw_parts(bytes, len) {
            Ok(s) => s,
            Err(()) => return copy_last_error_to(err_buf, err_len),
        };
        match h.put_raw_tail(ns, hex, payload) {
            Ok(_) => 0,
            Err(msg) => record_block_error(&msg, err_buf, err_len),
        }
    }));
    match outcome {
        Ok(rc) => rc,
        Err(_) => record_block_error("panic in wmbt_kv_put_raw_tail", err_buf, err_len),
    }
}

/// GET a raw-tail sidecar payload with borrow semantics. Mirrors
/// `wmbt_kv_get_kv_borrowed` shape.
///
/// On hit: writes a borrowed pointer + length into `*out_bytes` /
/// `*out_len` and writes an opaque release handle into `*out_borrow`.
/// The pointer is valid until `wmbt_kv_release_borrow(*out_borrow)`.
///
/// `err_buf` is optional and has the same semantics as in
/// `wmbt_kv_put_raw_tail`.
///
/// Returns:
///    1   hit; outputs set.
///    0   miss (no sidecar for this chain-tip).
///   -1   error; see `err_buf` / `wmbt_kv_last_error()`.
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_get_raw_tail_borrowed(
    handle: *mut wmbt_kv_handle,
    namespace: *const c_char,
    chain_tip_hash_hex: *const c_char,
    out_bytes: *mut *const u8,
    out_len: *mut usize,
    out_borrow: *mut *mut wmbt_kv_borrow,
    err_buf: *mut c_char,
    err_len: usize,
) -> i32 {
    clear_last_error();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if out_bytes.is_null() || out_len.is_null() || out_borrow.is_null() {
            return record_block_error(
                "out_bytes / out_len / out_borrow must not be NULL",
                err_buf,
                err_len,
            );
        }
        let h = match handle_ref(handle) {
            Ok(h) => h,
            Err(()) => return copy_last_error_to(err_buf, err_len),
        };
        let ns = match cstr_to_str(namespace) {
            Ok(s) => s,
            Err(()) => return copy_last_error_to(err_buf, err_len),
        };
        let hex = match cstr_to_str(chain_tip_hash_hex) {
            Ok(s) => s,
            Err(()) => return copy_last_error_to(err_buf, err_len),
        };
        match h.get_raw_tail(ns, hex) {
            Ok(None) => 0,
            Ok(Some(bytes)) => publish_borrowed_bytes(bytes, out_bytes, out_len, out_borrow),
            Err(msg) => record_block_error(&msg, err_buf, err_len),
        }
    }));
    match outcome {
        Ok(rc) => rc,
        Err(_) => record_block_error("panic in wmbt_kv_get_raw_tail_borrowed", err_buf, err_len),
    }
}

/// Returns the most recent thread-local error message, or NULL.
///
/// The pointer is valid until the next `wmbt_kv_*` call on this thread.
#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wmbt_kv_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| slot.borrow().as_ref().map_or(std::ptr::null(), |cs| cs.as_ptr()))
}

/// Set the thread-local last-error AND copy the message into the
/// caller's `err_buf` when non-NULL. Returns -1 so call sites can
/// `return record_block_error(...)` directly.
fn record_block_error(msg: &str, err_buf: *mut c_char, err_len: usize) -> i32 {
    set_last_error(msg);
    write_to_err_buf(msg, err_buf, err_len);
    -1
}

/// Copy whatever is currently in the thread-local last-error slot into
/// `err_buf`. Used after low-level helpers (`handle_ref`, `cstr_to_str`)
/// that set the slot themselves.
fn copy_last_error_to(err_buf: *mut c_char, err_len: usize) -> i32 {
    if !err_buf.is_null() && err_len > 0 {
        LAST_ERROR.with(|slot| {
            if let Some(cs) = slot.borrow().as_ref() {
                let msg = cs.to_string_lossy();
                write_to_err_buf(&msg, err_buf, err_len);
            }
        });
    }
    -1
}

fn write_to_err_buf(msg: &str, err_buf: *mut c_char, err_len: usize) {
    if err_buf.is_null() || err_len == 0 {
        return;
    }
    let bytes = msg.as_bytes();
    // Reserve 1 byte for the NUL terminator.
    let max_copy = err_len.saturating_sub(1).min(bytes.len());
    // SAFETY: err_buf is non-NULL and the caller's contract pins err_len
    // bytes of writable storage. We write max_copy <= err_len - 1 bytes
    // of payload + a trailing NUL, never exceeding err_len.
    #[allow(unsafe_code)]
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), err_buf.cast::<u8>(), max_copy);
        *err_buf.add(max_copy) = 0;
    }
}

// ============================================================
// Pointer helpers, every unsafe block is justified inline.
// ============================================================

fn handle_ref<'a>(handle: *mut wmbt_kv_handle) -> Result<&'a Handle, ()> {
    if handle.is_null() {
        set_last_error("handle is NULL");
        return Err(());
    }
    // SAFETY: the C contract requires `handle` came from
    // `wmbt_kv_init_from_env` and has not yet been freed. The lifetime
    // 'a is bounded by the calling function's stack frame (the C
    // caller cannot drop it concurrently per the threading note).
    #[allow(unsafe_code)]
    let h = unsafe { &*(handle as *const Handle) };
    Ok(h)
}

fn cstr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, ()> {
    if ptr.is_null() {
        set_last_error("string pointer is NULL");
        return Err(());
    }
    // SAFETY: the caller guarantees `ptr` is a NUL-terminated C string
    // that lives for the duration of the call.
    #[allow(unsafe_code)]
    let cstr = unsafe { CStr::from_ptr(ptr) };
    cstr.to_str().map_err(|_| {
        set_last_error("string contained non-UTF-8 bytes");
    })
}

fn slice_from_raw_parts<'a, T>(ptr: *const T, len: usize) -> Result<&'a [T], ()> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        set_last_error("data pointer is NULL with non-zero len");
        return Err(());
    }
    // SAFETY: the caller guarantees `ptr` points to `len` consecutive
    // valid `T` values for the duration of the call.
    #[allow(unsafe_code)]
    let s = unsafe { std::slice::from_raw_parts(ptr, len) };
    Ok(s)
}

fn publish_borrowed_bytes(
    bytes: Bytes,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
    out_borrow: *mut *mut wmbt_kv_borrow,
) -> i32 {
    let len = bytes.len();
    let inner = Box::new(BorrowInner {
        data: bytes,
        _multi: Vec::new(),
        _ptrs: Vec::new(),
        _lens: Vec::new(),
    });
    let data_ptr = inner.data.as_ptr();
    let raw = Box::into_raw(inner);
    // SAFETY: caller-provided output pointers were checked by the extern
    // function before this helper is called.
    #[allow(unsafe_code)]
    unsafe {
        *out_ptr = data_ptr;
        *out_len = len;
        *out_borrow = raw.cast::<wmbt_kv_borrow>();
    }
    1
}

/// Publish a Vec<Bytes> as N borrowed payload pointers + N lengths.
///
/// Writes:
///   `*out_payload_ptrs = &borrow.ptrs[0]`
///   `*out_payload_lens = &borrow.lens[0]`
///   `*out_borrow = the BorrowInner pointer`
///
/// The arrays live inside the `BorrowInner` so they stay valid for the
/// borrow's lifetime. Each `Bytes` in `_multi` keeps its underlying Arc
/// payload alive: `_multi[i].as_ptr()` is valid until release.
fn publish_borrowed_blocks(
    items: Vec<Bytes>,
    out_payload_ptrs: *mut *const *const u8,
    out_payload_lens: *mut *const usize,
    out_borrow: *mut *mut wmbt_kv_borrow,
) -> i32 {
    let n = items.len();
    let mut ptrs: Vec<*const u8> = Vec::with_capacity(n);
    let mut lens: Vec<usize> = Vec::with_capacity(n);
    for b in &items {
        ptrs.push(b.as_ptr());
        lens.push(b.len());
    }
    let inner =
        Box::new(BorrowInner { data: Bytes::new(), _multi: items, _ptrs: ptrs, _lens: lens });
    // Capture array base addresses BEFORE moving the Box into a raw
    // pointer, otherwise we'd need `inner_ref` from `raw`.
    let ptrs_ptr = inner._ptrs.as_ptr();
    let lens_ptr = inner._lens.as_ptr();
    let raw = Box::into_raw(inner);
    // SAFETY: caller-provided output pointers were checked by the extern
    // function before this helper is called.
    #[allow(unsafe_code)]
    unsafe {
        *out_payload_ptrs = ptrs_ptr;
        *out_payload_lens = lens_ptr;
        *out_borrow = raw.cast::<wmbt_kv_borrow>();
    }
    1
}

// Suppress unused warning when c_void isn't pulled in by an upstream
// dep (keeps the import block self-contained for future surface
// additions).
#[allow(dead_code)]
fn _c_void_marker(_: *mut c_void) {}
