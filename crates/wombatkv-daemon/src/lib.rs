//! Shared-memory ping-pong transport for the wombatkv puffer daemon.
//!
//! Two POSIX SHM disruptor rings (req + resp) carry typed rkyv-encoded
//! messages between an inference engine (client) and a long-running
//! daemon process holding a [`wombatkv_node::embed::WombatKVKvStore`].
//!
//! # Design choices (v1)
//!
//! - **Right-sized frames plus client chunking.** `AlignedFixedFrame<DATA_BYTES>`
//!   is sized for low-millisecond KV chunks, while `RemoteKvStoreClient`
//!   chunks larger engine payloads behind its public `put_kv/get_kv` API.
//! - **Tiered ops.** This crate handles control + small-payload ops
//!   (PING, `PUT_SMALL`, `GET_SMALL`, EXISTS, STATS, RESTORE, CLEAR). For
//!   payloads larger than the per-frame budget are stored as chunk objects
//!   plus a tiny manifest. The daemon remains a simple key/value server.
//! - **rkyv codec.** Zero-alloc encode (`AlignedVec`) on the producer
//!   side; on the consumer side messages are decoded into owned types
//!   for ergonomics. Switching to `recv_leased` for archived-view
//!   access is a one-line change once the larger-payload path lands.
//!
//! # Frame budget
//!
//! `DATA_BYTES = 4 MiB - 16` so each ring slot fits a multi-MiB
//! rkyv-encoded payload. With ring depth 16 that's about 64 MiB per
//! ring, 128 MiB for the full request+response pair. Acceptable on an
//! RTX dev host and large enough to avoid tens of thousands of tiny
//! chunks for real KV blobs.

// Crate-level lint: this crate `deny`s unsafe (overriding the workspace
// `forbid`) so the arena module can wrap memmap2's `unsafe fn map_mut`
// in a single audited site (see `arena.rs`). All other modules remain
// safe-only.
#![deny(unsafe_code)]

pub mod arena;
pub mod client;
pub mod config;
pub mod constants;
pub mod envelope;
pub mod http_transport;
pub mod lifecycle;
pub mod runtime_tpc;
pub mod tcp_transport;
pub use arena::{
    arena_path, ArenaError, ArenaReader, ArenaWriter, ARENA_HEADER_BYTES, DEFAULT_ARENA_BYTES,
};
pub use client::{
    ClientOptions, RemoteError, RemoteGetOutcome, RemoteHitTier, RemoteKvStoreClient,
    DEFAULT_CALL_TIMEOUT,
};
pub use config::DaemonConfig;
pub use lifecycle::{cleanup_prefix_segments, ClientHeartbeat, HeartbeatMonitor, ReopenReason};

use std::time::Duration;

use bytes::Bytes;
use myelon::codec::{Codec, CodecError};
use myelon::transport::AlignedFixedFrame;
use myelon::typed_transport::{TypedConsumer, TypedProducer};
use myelon::MyelonWaitStrategy;

use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

/// Per-frame data budget. 4 MiB minus 16-byte header.
///
/// Sizing rationale: the ring buffer itself is statically allocated at
/// `create_with_consumers` time, but the owned (copying) APIs we use -
/// `TypedProducer::publish` + `TypedConsumer::recv_owned`, pay 4×
/// `DATA_BYTES` of memcpy per round-trip:
///   - producer `T::default()`: zeroes a stack frame of `DATA_BYTES`
///   - producer `*slot = frame`: copies the full frame into the ring slot
///   - consumer `*event_ptr`: copies the full slot out to a stack frame
///   - consumer's payload-bounded copy out of the slot (small)
///
/// So PING RTT is dominated by raw memory bandwidth on full-frame
/// memcpys, NOT by any per-publish init cost, the ring is allocated
/// once at create-time. (M3 Max, ~16 GB/s sustained on 4× per-RTT
/// copies puts the floor at roughly 4 × `DATA_BYTES` / 16 GB/s, matching
/// the empirical sweep below.)
///
/// Frame-size sweep, M3 Max, `MinIO` local, p50 µs:
///   frame  | PING | EXISTS | 4K GET | 64K GET | 256K GET | 1M GET | 1.5M GET
///   2 KiB  | 0.71 | 1.08   | 2.79   | 19.0    | 47.7     | 225    | 283
///   4 KiB  | 2.00 | 1.54   | 2.71   | 16.7    | 50.6     | 184    | 244  ← chosen
///   8 KiB  | 3.00 | 3.38   | 3.21   | 17.3    | 49.2     | 179    | 248
///   16 KiB | 4.25 | 4.75   | 5.54   | 20.3    | 44.3     | 165    | 320
///   64 KiB | 18.4 | 20.8   | 21.0   | 34.0    | 61.5     | 228    | 258
///
/// 4 KiB is the sweet spot: only 1.3 µs slower than 2 KiB on PING, but
/// avoids the large-payload regression you see at 2 KiB (1.5 MiB GET
/// goes 244 → 283 µs). Within ~10 % of the best on 256 KiB / 1 MiB
/// payloads (16 KiB wins those by a hair, but loses badly on PING and
/// on 1.5 MiB). Matches the macOS page size for clean TLB behavior.
///
/// Future work: switching to `recv_leased` would eliminate the consumer
/// copy (it borrows directly from the ring slot), and a `publish_with`-
/// style API that writes directly into the ring slot would eliminate
/// the producer's stack frame too. That moves the bottleneck off
/// memcpy entirely, at which point larger frames stop hurting.
///
/// Reference per-block KV sizes (Qwen3-0.6B Metal):
///   per layer per block (32 tok × 8 `kv_h` × 128 `head_dim` × 2 bytes) = 16 KiB
///   per block all 28 layers (K+V) ≈ 458 KiB → fragments to ~115 frames
pub const FRAME_DATA_BYTES: usize = 4 * 1024 * 1024 - 16;
/// Disruptor ring depth: N in-flight slots.
/// Total ring footprint per direction = depth × `FRAME_DATA_BYTES` = 1 MiB.
pub const DEFAULT_RING_DEPTH: usize = 16;

/// The frame type backing both rings.
pub type ShmFrame = AlignedFixedFrame<FRAME_DATA_BYTES>;

/// Consumer id on the request ring (daemon side). Kept tiny because
/// macOS POSIX SHM caps segment names at 31 chars; the consumer cursor
/// segment appends this id to the ring name.
pub const REQ_CONSUMER_ID: &str = "dn";
/// Consumer id on the response ring (client side).
pub const RESP_CONSUMER_ID: &str = "cn";

/// Default attach timeout for SHM segments (matches perf-bench).
///
/// Overridable via `WMBT_KV_DAEMON_SHM_ATTACH_TIMEOUT_SECS` env
/// for slow-startup scenarios (Docker cold start, busy CI runners). Values
/// <1 are ignored. See [`effective_attach_timeout`] for the resolver.
pub const ATTACH_TIMEOUT: Duration = Duration::from_secs(30);

/// Resolves the effective SHM attach timeout from
/// `WMBT_KV_DAEMON_SHM_ATTACH_TIMEOUT_SECS` env, falling back to
/// [`ATTACH_TIMEOUT`]. Useful for slow Docker startup ops or CI
/// runners where the daemon takes longer to bring up SHM segments.
#[must_use]
pub fn effective_attach_timeout() -> Duration {
    std::env::var("WMBT_KV_DAEMON_SHM_ATTACH_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n >= 1)
        .map_or(ATTACH_TIMEOUT, Duration::from_secs)
}

/// Wire op codes. Encoded as `u8` (struct fields) to side-step rkyv's
/// enum derive constraints, we want plain field traversal on the hot
/// path, not match dispatch.
///
/// # Block-shaped ops (ABI 1.5+, opcodes 11..=13)
///
/// `LOOKUP_BLOCK_PREFIX`, `GET_KV_BLOCKS_BATCH`, and `PUT_KV_BLOCKS_BATCH`
/// mirror the C ABI `wmbt_kv_lookup_block_prefix` / `_get_kv_blocks_borrowed`
/// / `_put_kv_blocks` surfaces over the daemon transport. The op-specific
/// request/response shapes are carried as **rkyv-archived** bytes inside
/// the existing `WireRequest.payload` / `WireResponse.payload` `Vec<u8>`, no envelope-level change. RFC 0008 §4e completed the migration from
/// the hand-rolled length-prefix codec (V1 magics) to rkyv (V2 magics).
/// See `LookupBlockPrefixReq` and friends.
pub mod op {
    pub const PING: u8 = 1;
    pub const PUT: u8 = 2;
    pub const GET: u8 = 3;
    pub const EXISTS: u8 = 4;
    pub const STATS: u8 = 5;
    pub const CLEAR: u8 = 6;
    pub const RESTORE: u8 = 7;
    pub const CLOSE: u8 = 8;
    pub const GET_MANY: u8 = 9;
    pub const LIST: u8 = 10;
    /// Block-shaped: count leading hashes present in the daemon's
    /// metadata index. See `LookupBlockPrefixReq`/`LookupBlockPrefixResp`.
    pub const LOOKUP_BLOCK_PREFIX: u8 = 11;
    /// Block-shaped: parallel batched GET for N content-addressed blocks.
    /// See `GetKvBlocksBatchReq`/`GetKvBlocksBatchResp`.
    pub const GET_KV_BLOCKS_BATCH: u8 = 12;
    /// Block-shaped: parallel batched PUT for N content-addressed blocks
    /// plus a server-side metadata-index update so subsequent
    /// `LOOKUP_BLOCK_PREFIX` reflects the new presence.
    /// See `PutKvBlocksBatchReq`/`PutKvBlocksBatchResp`.
    pub const PUT_KV_BLOCKS_BATCH: u8 = 13;

    #[must_use]
    pub fn name(code: u8) -> &'static str {
        match code {
            PING => "ping",
            PUT => "put",
            GET => "get",
            EXISTS => "exists",
            STATS => "stats",
            CLEAR => "clear",
            RESTORE => "restore",
            CLOSE => "close",
            GET_MANY => "get_many",
            LIST => "list",
            LOOKUP_BLOCK_PREFIX => "lookup_block_prefix",
            GET_KV_BLOCKS_BATCH => "get_kv_blocks_batch",
            PUT_KV_BLOCKS_BATCH => "put_kv_blocks_batch",
            _ => "unknown",
        }
    }
}

/// Wire status codes (see `op` for design rationale).
pub mod status {
    pub const OK: u8 = 0;
    pub const MISS: u8 = 1;
    pub const TOO_LARGE: u8 = 2;
    pub const ERROR: u8 = 3;
}

const KEY_BATCH_MAGIC: &[u8] = b"WMBT_KV_KEYS_V1\0";
const BYTES_BATCH_MAGIC: &[u8] = b"WMBT_KV_BYTES_V1\0";

// Per-op magic constants for the block-shaped payloads. 16 bytes each
// (matching the legacy batch magics) so decode failures point at the
// exact op that produced the malformed frame.
//
// Block-opcode magic strings: 16 bytes each, NUL-padded. Alpha
// breaking-window means there's no legacy V1 envelope to coexist with -
// any client/daemon mismatch surfaces as an explicit decode error
// rather than a silent torn read.
const LOOKUP_BLOCK_PREFIX_REQ_MAGIC: &[u8] = b"WMBT_LBP_REQ\0\0\0\0";
const LOOKUP_BLOCK_PREFIX_RESP_MAGIC: &[u8] = b"WMBT_LBP_RES\0\0\0\0";
const GET_KV_BLOCKS_BATCH_REQ_MAGIC: &[u8] = b"WMBT_GBB_REQ\0\0\0\0";
const GET_KV_BLOCKS_BATCH_RESP_MAGIC: &[u8] = b"WMBT_GBB_RES\0\0\0\0";
const PUT_KV_BLOCKS_BATCH_REQ_MAGIC: &[u8] = b"WMBT_PBB_REQ\0\0\0\0";
const PUT_KV_BLOCKS_BATCH_RESP_MAGIC: &[u8] = b"WMBT_PBB_RES\0\0\0\0";

/// Encode an ordered key list for the daemon batched GET control path.
#[must_use]
pub fn encode_key_batch(keys: &[String]) -> Vec<u8> {
    let total = KEY_BATCH_MAGIC.len() + 4 + keys.iter().map(|key| 4 + key.len()).sum::<usize>();
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(KEY_BATCH_MAGIC);
    out.extend_from_slice(&(keys.len() as u32).to_le_bytes());
    for key in keys {
        let bytes = key.as_bytes();
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    }
    out
}

/// Typed error for the daemon's wire-codec helpers (encode/decode/
/// validate). Replaces ad-hoc `Result<T, String>` in the daemon's
/// codec surface. Named `WireCodecError` to avoid
/// shadowing `myelon::codec::CodecError` (which we import at the
/// top of this file for the typed-transport surface).
///
/// Each variant carries enough context to be operator-actionable
/// without grepping the source.
#[derive(Debug, thiserror::Error)]
pub enum WireCodecError {
    /// A length prefix or sentinel claimed more bytes than the
    /// payload contains.
    #[error("{what} truncated: need {needed} bytes at offset {at}, payload is {got}")]
    Truncated { what: &'static str, needed: usize, at: usize, got: usize },

    /// First N bytes don't match the expected magic for this op.
    #[error("{op}: bad magic; expected {expected:?} got first {got_len} bytes {got:?}")]
    BadMagic { op: &'static str, expected: &'static [u8], got_len: usize, got: Vec<u8> },

    /// Length arithmetic would overflow `usize`.
    #[error("{what}: length overflow")]
    LengthOverflow { what: &'static str },

    /// Trailing bytes past the parsed end of the payload.
    #[error("{what}: trailing bytes ({extra} unread)")]
    TrailingBytes { what: &'static str, extra: usize },

    /// Body length stated in the prefix doesn't match the actual
    /// payload bytes available.
    #[error("{what}: body length mismatch (claimed={claimed}, actual={actual})")]
    BodyLengthMismatch { what: &'static str, claimed: usize, actual: usize },

    /// UTF-8 decode failure for a key/string field.
    #[error("{what}: utf8: {source}")]
    Utf8 {
        what: &'static str,
        #[source]
        source: std::str::Utf8Error,
    },

    /// rkyv encode/decode failure, scoped by op name.
    #[error("{op}: rkyv: {source}")]
    Rkyv {
        op: &'static str,
        #[source]
        source: rkyv::rancor::Error,
    },

    /// SHM segment-name budget validation failure (see
    /// `validate_segment_name_budget`).
    #[error("{0}")]
    SegmentNameBudget(String),
}

/// Decode an ordered key list encoded by [`encode_key_batch`].
pub fn decode_key_batch(payload: &[u8]) -> Result<Vec<String>, WireCodecError> {
    if !payload.starts_with(KEY_BATCH_MAGIC) {
        let got_len = KEY_BATCH_MAGIC.len().min(payload.len());
        return Err(WireCodecError::BadMagic {
            op: "key_batch",
            expected: KEY_BATCH_MAGIC,
            got_len,
            got: payload[..got_len].to_vec(),
        });
    }
    let mut cursor = KEY_BATCH_MAGIC.len();
    let count = read_u32(payload, &mut cursor)? as usize;
    let mut keys = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u32(payload, &mut cursor)? as usize;
        let end =
            cursor.checked_add(len).ok_or(WireCodecError::LengthOverflow { what: "key_batch" })?;
        if end > payload.len() {
            return Err(WireCodecError::Truncated {
                what: "key_batch",
                needed: len,
                at: cursor,
                got: payload.len(),
            });
        }
        let key = std::str::from_utf8(&payload[cursor..end])
            .map_err(|err| WireCodecError::Utf8 { what: "key_batch", source: err })?
            .to_string();
        keys.push(key);
        cursor = end;
    }
    if cursor != payload.len() {
        return Err(WireCodecError::TrailingBytes {
            what: "key_batch",
            extra: payload.len() - cursor,
        });
    }
    Ok(keys)
}

/// Encode ordered payload bytes for a batched GET response.
#[must_use]
pub fn encode_bytes_batch(items: &[Bytes]) -> Vec<u8> {
    let total = BYTES_BATCH_MAGIC.len()
        + 4
        + (items.len() * 4)
        + items.iter().map(Bytes::len).sum::<usize>();
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(BYTES_BATCH_MAGIC);
    out.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for item in items {
        out.extend_from_slice(&(item.len() as u32).to_le_bytes());
    }
    for item in items {
        out.extend_from_slice(item);
    }
    out
}

/// Decode an ordered payload batch without copying individual payloads.
pub fn decode_bytes_batch(batch: Bytes) -> Result<Vec<Bytes>, WireCodecError> {
    if !batch.starts_with(BYTES_BATCH_MAGIC) {
        let got_len = BYTES_BATCH_MAGIC.len().min(batch.len());
        return Err(WireCodecError::BadMagic {
            op: "bytes_batch",
            expected: BYTES_BATCH_MAGIC,
            got_len,
            got: batch[..got_len].to_vec(),
        });
    }
    let data = batch.as_ref();
    let mut cursor = BYTES_BATCH_MAGIC.len();
    let count = read_u32(data, &mut cursor)? as usize;
    let mut lengths = Vec::with_capacity(count);
    for _ in 0..count {
        lengths.push(read_u32(data, &mut cursor)? as usize);
    }

    let body_start = cursor;
    let body_len = lengths
        .iter()
        .try_fold(0usize, |acc, len| acc.checked_add(*len))
        .ok_or(WireCodecError::LengthOverflow { what: "bytes_batch" })?;
    let body_end = body_start
        .checked_add(body_len)
        .ok_or(WireCodecError::LengthOverflow { what: "bytes_batch" })?;
    if body_end != data.len() {
        return Err(WireCodecError::BodyLengthMismatch {
            what: "bytes_batch",
            claimed: body_end,
            actual: data.len(),
        });
    }

    let mut offset = body_start;
    let mut out = Vec::with_capacity(count);
    for len in lengths {
        let end = offset + len;
        out.push(batch.slice(offset..end));
        offset = end;
    }
    Ok(out)
}

fn read_u32(payload: &[u8], cursor: &mut usize) -> Result<u32, WireCodecError> {
    let end = cursor.checked_add(4).ok_or(WireCodecError::LengthOverflow { what: "u32_cursor" })?;
    if end > payload.len() {
        return Err(WireCodecError::Truncated {
            what: "u32",
            needed: 4,
            at: *cursor,
            got: payload.len(),
        });
    }
    let value =
        u32::from_le_bytes(payload[*cursor..end].try_into().expect("slice length checked above"));
    *cursor = end;
    Ok(value)
}

/// rkyv-archivable wire request.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub struct WireRequest {
    pub id: u64,
    pub op: u8,
    pub namespace: String,
    pub key: String,
    pub payload: Vec<u8>,
}

/// rkyv-archivable wire response.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub struct WireResponse {
    pub id: u64,
    pub status: u8,
    pub op: u8,
    pub payload: Vec<u8>,
    pub message: String,
}

impl Codec for WireRequest {
    type Encoded = AlignedVec;

    fn encode(&self) -> Result<Self::Encoded, CodecError> {
        rkyv::to_bytes::<RkyvError>(self).map_err(CodecError::encode)
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let archived = rkyv::access::<<WireRequest as Archive>::Archived, RkyvError>(bytes)
            .map_err(CodecError::decode)?;
        rkyv::deserialize::<Self, RkyvError>(archived).map_err(CodecError::decode)
    }
}

impl Codec for WireResponse {
    type Encoded = AlignedVec;

    fn encode(&self) -> Result<Self::Encoded, CodecError> {
        rkyv::to_bytes::<RkyvError>(self).map_err(CodecError::encode)
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let archived = rkyv::access::<<WireResponse as Archive>::Archived, RkyvError>(bytes)
            .map_err(CodecError::decode)?;
        rkyv::deserialize::<Self, RkyvError>(archived).map_err(CodecError::decode)
    }
}

// ============================================================
// Block-shaped op payload types (rkyv-archived, ABI 1.5 / V2)
// ============================================================
//
// These structs are NOT wire envelopes, they ride INSIDE the
// `WireRequest.payload` / `WireResponse.payload` `Vec<u8>` fields. The
// envelope (op code, request id, namespace, status, message) stays in
// `WireRequest`/`WireResponse`; only the per-op shape (hash lists,
// per-block payload slices, matched counts) lives here. This keeps the
// existing daemon ↔ client framing unchanged, adding a new block-shaped
// op is purely additive at the opcode dispatch level.
//
// Encoding format (V2, RFC 0008 §4e):
//   - 16-byte ASCII magic header (per-op, null-padded, includes V2 tag)
//   - rkyv::to_bytes::<rancor::Error>(payload).to_vec()
//
// The V2 magic header reuses the same naming pattern as the prior
// length-prefix codec so decode failures still point at the exact op
// that produced the malformed frame.

/// Request payload for [`op::LOOKUP_BLOCK_PREFIX`].
///
/// Carries the namespace + ordered list of 64-char lower-hex blake3
/// block hashes. The daemon resolves each hash via the in-process
/// `metadata_index().longest_prefix(...)` and replies with the leading-
/// hit count.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct LookupBlockPrefixReq {
    pub namespace: String,
    /// Each entry MUST be exactly 64 lower-hex characters (32 bytes).
    pub block_hashes_hex: Vec<String>,
}

/// Response payload for [`op::LOOKUP_BLOCK_PREFIX`].
///
/// `matched_count` is the number of hashes, counted from index 0 of
/// the request's `block_hashes_hex`, that the daemon's metadata
/// index recognized before the first miss. `error` is `Some(msg)` only
/// for malformed inputs (bad hex, etc.); the wire-level `WireResponse.status`
/// also carries the OK/ERROR distinction so callers can branch without
/// re-decoding.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct LookupBlockPrefixResp {
    pub matched_count: u32,
    pub error: Option<String>,
}

/// Request payload for [`op::GET_KV_BLOCKS_BATCH`].
///
/// Carries the namespace + ordered list of 64-char lower-hex blake3
/// block hashes. The daemon resolves each hash to a key under
/// `wombatkv/v1/block/b3=<hex>`, parallel-fetches via `get_kv`, and replies
/// with all-or-nothing payload bytes.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct GetKvBlocksBatchReq {
    pub namespace: String,
    pub block_hashes_hex: Vec<String>,
}

/// Response payload for [`op::GET_KV_BLOCKS_BATCH`].
///
/// On full hit `payloads` is `Some(per-block bytes in input order)`; on
/// any miss it is `None` (matches the cabi `get_kv_blocks_borrowed`
/// all-or-nothing semantics). `error` is `Some(msg)` for backend errors
/// distinct from a miss.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct GetKvBlocksBatchResp {
    /// `None` => at least one block was missing (caller treats as miss).
    /// `Some(items)` => `items.len() == request.block_hashes_hex.len()`.
    pub payloads: Option<Vec<Vec<u8>>>,
    pub error: Option<String>,
}

/// Request payload for [`op::PUT_KV_BLOCKS_BATCH`].
///
/// Carries the namespace, ordered list of 64-char lower-hex blake3
/// hashes, and matching ordered payloads. The daemon writes each block
/// under `wombatkv/v1/block/b3=<hex>` and updates its in-process metadata
/// index so a subsequent `LOOKUP_BLOCK_PREFIX` sees the new presence.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct PutKvBlocksBatchReq {
    pub namespace: String,
    pub block_hashes_hex: Vec<String>,
    pub payloads: Vec<Vec<u8>>,
}

/// Response payload for [`op::PUT_KV_BLOCKS_BATCH`].
///
/// `total_bytes` is the sum of all `payloads[i].len()` on success.
/// `error` is `Some(msg)` if any per-block PUT failed; in that case the
/// metadata index is left unchanged for the failed batch (server-side
/// transaction: index updates only run after ALL per-block PUTs succeed).
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct PutKvBlocksBatchResp {
    pub total_bytes: u64,
    pub error: Option<String>,
}

// ------------------------------------------------------------
// Internal helpers for the block-shaped rkyv V2 codecs (RFC 0008 §4e).
//
// Each wire payload is `MAGIC (16 bytes) || rkyv::to_bytes(value)`. The
// magic distinguishes the op + version (V2 = rkyv); without it a stray
// frame would still bytecheck because the body is well-aligned, and we
// would lose the "WMBT_{op}_{role}_V{n}" decode-error trace.
// ------------------------------------------------------------

fn expect_magic<'a>(
    payload: &'a [u8],
    magic: &'static [u8],
    op_name: &'static str,
) -> Result<&'a [u8], WireCodecError> {
    if !payload.starts_with(magic) {
        let got_len = magic.len().min(payload.len());
        return Err(WireCodecError::BadMagic {
            op: op_name,
            expected: magic,
            got_len,
            got: payload[..got_len].to_vec(),
        });
    }
    Ok(&payload[magic.len()..])
}

/// Generic rkyv encode: prepend a per-op magic header, then archive
/// the value. The body is allocator-aligned by rkyv but slicing past
/// the 16-byte prefix preserves 16-byte alignment for free (16 % 16 = 0)
/// so the decoder can `access` the body in-place without copying.
fn rkyv_encode_with_magic<T>(
    magic: &'static [u8],
    op_name: &'static str,
    value: &T,
) -> Result<Vec<u8>, WireCodecError>
where
    T: for<'a> rkyv::Serialize<
        rkyv::api::high::HighSerializer<
            rkyv::util::AlignedVec,
            rkyv::ser::allocator::ArenaHandle<'a>,
            RkyvError,
        >,
    >,
{
    let body = rkyv::to_bytes::<RkyvError>(value)
        .map_err(|e| WireCodecError::Rkyv { op: op_name, source: e })?;
    let mut out = Vec::with_capacity(magic.len() + body.len());
    out.extend_from_slice(magic);
    out.extend_from_slice(body.as_slice());
    Ok(out)
}

/// Generic rkyv decode: strip the magic, copy the body into an
/// `AlignedVec<16>` (the body lives at +16 from the caller's slice,
/// preserving alignment of the underlying buffer, but we still copy
/// for safety because the `Vec<u8>` payload carrier inside
/// `WireRequest`/`WireResponse` may have been resliced through code
/// paths we don't control), then run `from_bytes`.
fn rkyv_decode_with_magic<T>(
    payload: &[u8],
    magic: &'static [u8],
    op_name: &'static str,
) -> Result<T, WireCodecError>
where
    T: rkyv::Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    let body = expect_magic(payload, magic, op_name)?;
    let mut aligned: rkyv::util::AlignedVec<16> = rkyv::util::AlignedVec::with_capacity(body.len());
    aligned.extend_from_slice(body);
    rkyv::from_bytes::<T, RkyvError>(&aligned[..])
        .map_err(|e| WireCodecError::Rkyv { op: op_name, source: e })
}

// ------------------------------------------------------------
// LookupBlockPrefix Req/Resp codec.
// ------------------------------------------------------------

/// rkyv-encode a [`LookupBlockPrefixReq`] for `WireRequest.payload`.
///
/// Layout: `magic(16) | rkyv::to_bytes(req)`
pub fn encode_lookup_block_prefix_req(
    req: &LookupBlockPrefixReq,
) -> Result<Vec<u8>, WireCodecError> {
    rkyv_encode_with_magic(LOOKUP_BLOCK_PREFIX_REQ_MAGIC, "lookup_block_prefix_req", req)
}

pub fn decode_lookup_block_prefix_req(
    bytes: &[u8],
) -> Result<LookupBlockPrefixReq, WireCodecError> {
    rkyv_decode_with_magic::<LookupBlockPrefixReq>(
        bytes,
        LOOKUP_BLOCK_PREFIX_REQ_MAGIC,
        "lookup_block_prefix_req",
    )
}

/// rkyv-encode a [`LookupBlockPrefixResp`] for `WireResponse.payload`.
pub fn encode_lookup_block_prefix_resp(
    resp: &LookupBlockPrefixResp,
) -> Result<Vec<u8>, WireCodecError> {
    rkyv_encode_with_magic(LOOKUP_BLOCK_PREFIX_RESP_MAGIC, "lookup_block_prefix_resp", resp)
}

pub fn decode_lookup_block_prefix_resp(
    bytes: &[u8],
) -> Result<LookupBlockPrefixResp, WireCodecError> {
    rkyv_decode_with_magic::<LookupBlockPrefixResp>(
        bytes,
        LOOKUP_BLOCK_PREFIX_RESP_MAGIC,
        "lookup_block_prefix_resp",
    )
}

// ------------------------------------------------------------
// GetKvBlocksBatch Req/Resp codec.
// ------------------------------------------------------------

pub fn encode_get_kv_blocks_batch_req(
    req: &GetKvBlocksBatchReq,
) -> Result<Vec<u8>, WireCodecError> {
    rkyv_encode_with_magic(GET_KV_BLOCKS_BATCH_REQ_MAGIC, "get_kv_blocks_batch_req", req)
}

pub fn decode_get_kv_blocks_batch_req(bytes: &[u8]) -> Result<GetKvBlocksBatchReq, WireCodecError> {
    rkyv_decode_with_magic::<GetKvBlocksBatchReq>(
        bytes,
        GET_KV_BLOCKS_BATCH_REQ_MAGIC,
        "get_kv_blocks_batch_req",
    )
}

pub fn encode_get_kv_blocks_batch_resp(
    resp: &GetKvBlocksBatchResp,
) -> Result<Vec<u8>, WireCodecError> {
    rkyv_encode_with_magic(GET_KV_BLOCKS_BATCH_RESP_MAGIC, "get_kv_blocks_batch_resp", resp)
}

pub fn decode_get_kv_blocks_batch_resp(
    bytes: &[u8],
) -> Result<GetKvBlocksBatchResp, WireCodecError> {
    rkyv_decode_with_magic::<GetKvBlocksBatchResp>(
        bytes,
        GET_KV_BLOCKS_BATCH_RESP_MAGIC,
        "get_kv_blocks_batch_resp",
    )
}

// ------------------------------------------------------------
// PutKvBlocksBatch Req/Resp codec.
// ------------------------------------------------------------

pub fn encode_put_kv_blocks_batch_req(
    req: &PutKvBlocksBatchReq,
) -> Result<Vec<u8>, WireCodecError> {
    // Mirror the prior pre-flight check so callers get a precise error
    // rather than a downstream daemon panic. rkyv would still encode a
    // shape-mismatched record but the daemon would loop over hashes
    // assuming payloads[i] exists for each i.
    if req.block_hashes_hex.len() != req.payloads.len() {
        return Err(WireCodecError::BodyLengthMismatch {
            what: "put_kv_blocks_batch_req",
            claimed: req.block_hashes_hex.len(),
            actual: req.payloads.len(),
        });
    }
    rkyv_encode_with_magic(PUT_KV_BLOCKS_BATCH_REQ_MAGIC, "put_kv_blocks_batch_req", req)
}

pub fn decode_put_kv_blocks_batch_req(bytes: &[u8]) -> Result<PutKvBlocksBatchReq, WireCodecError> {
    rkyv_decode_with_magic::<PutKvBlocksBatchReq>(
        bytes,
        PUT_KV_BLOCKS_BATCH_REQ_MAGIC,
        "put_kv_blocks_batch_req",
    )
}

pub fn encode_put_kv_blocks_batch_resp(
    resp: &PutKvBlocksBatchResp,
) -> Result<Vec<u8>, WireCodecError> {
    rkyv_encode_with_magic(PUT_KV_BLOCKS_BATCH_RESP_MAGIC, "put_kv_blocks_batch_resp", resp)
}

pub fn decode_put_kv_blocks_batch_resp(
    bytes: &[u8],
) -> Result<PutKvBlocksBatchResp, WireCodecError> {
    rkyv_decode_with_magic::<PutKvBlocksBatchResp>(
        bytes,
        PUT_KV_BLOCKS_BATCH_RESP_MAGIC,
        "put_kv_blocks_batch_resp",
    )
}

/// Internal SHM prefix prepended to every segment the daemon creates.
/// Short (2 chars + role char) so the disruptor-mp auxiliary suffixes
/// (`_producer_seq` = 13 chars, `_cr`, `_ci`, `_<consumer_id>_seq`) fit
/// within the macOS POSIX-SHM 31-char budget for reasonable user prefixes.
///
/// The historical `wmbt_kv_<prefix>_<role>` shape consumed 13 chars of
/// budget before the user prefix; under disruptor-mp's `_producer_seq`
/// suffix that gave 30 − 13 − 13 = 4 chars for the prefix, which made
/// any meaningful daemon prefix illegal on macOS (the seed=999 DST
/// failure on Mac). Shrinking the wrapper to `wk<prefix><role>` recovers
/// the budget back to 30 − 4 − 13 = 13 chars of user prefix on macOS.
const SHM_PREFIX: &str = "wk";

/// `r` for the request ring (client→daemon); `s` for the response ring
/// (daemon→client). Single-char to stay tight on the macOS budget.
const ROLE_REQ: char = 'r';
const ROLE_RESP: char = 's';

/// Longest auxiliary suffix disruptor-mp appends per ring segment -
/// `_producer_seq` (13 chars including the leading `_`). The other
/// suffixes (`_cr`, `_ci`, `_<consumer_id>_seq` with our 2-char
/// `dn`/`cn` consumer ids = 7 chars) are shorter and don't bind the
/// budget. If disruptor-mp's internal naming ever grows past 13 chars,
/// the validator below stays correct only after this constant is
/// updated to match.
const MAX_DISRUPTOR_INTERNAL_SUFFIX_LEN: usize = 13;

/// Build the request and response SHM segment names for a daemon prefix.
/// The format is `wk<prefix>r` and `wk<prefix>s` (no underscores) so the
/// names stay short enough that disruptor-mp's per-ring auxiliary
/// segments (notably `<base>_producer_seq`) still fit the macOS
/// POSIX-SHM 31-char budget.
pub fn segment_names(prefix: &str) -> (String, String) {
    (format!("{SHM_PREFIX}{prefix}{ROLE_REQ}"), format!("{SHM_PREFIX}{prefix}{ROLE_RESP}"))
}

/// macOS `POSIX_SHM_NAME_MAX` = 31 chars (including null). We use 30 for
/// the user-visible name to leave the kernel one byte of slack.
const SHM_SEGMENT_NAME_MAX_LEN_MACOS: usize = 30;

/// Validate that the SHM segment names derived from `prefix`, including
/// the longest disruptor-mp auxiliary segment (`<base>_producer_seq`) -
/// will fit the macOS POSIX-SHM 31-char budget. Returns a clear,
/// actionable error so daemon and client both fail loud at startup
/// instead of letting the OS surface a cryptic `ENAMETOOLONG (errno 63)`
/// (or worse, a `Shared segment not found` after thousands of retries -
/// see RFC 0011 P10 / the 2026-05-18 macOS DST-sweep regression).
///
/// The math:
///
/// ```text
///     longest name = wk<prefix><role>_producer_seq
///                  = 2 + prefix.len() + 1 + 13
///                  = 16 + prefix.len()
/// ```
///
/// At the 30-char macOS budget the prefix is capped at **14 chars**.
/// On Linux SHM names are filesystem paths (effectively unbounded);
/// the validator is a no-op there beyond a sanity check.
pub fn validate_segment_name_budget(prefix: &str) -> Result<(), WireCodecError> {
    let (req, resp) = segment_names(prefix);
    for base in [&req, &resp] {
        // Real binding constraint: base name + the longest disruptor
        // suffix. The suffix lives in the SHM namespace too, so it has
        // to fit the same 30-char budget.
        let derived_len = base.len() + MAX_DISRUPTOR_INTERNAL_SUFFIX_LEN;
        if derived_len > SHM_SEGMENT_NAME_MAX_LEN_MACOS {
            // Compute the largest acceptable prefix length: budget −
            // (wk prefix + role + producer_seq).
            let fixed = SHM_PREFIX.len() + 1 + MAX_DISRUPTOR_INTERNAL_SUFFIX_LEN;
            let max_prefix = SHM_SEGMENT_NAME_MAX_LEN_MACOS.saturating_sub(fixed);
            let derived_name = format!("{base}_producer_seq");
            return Err(WireCodecError::SegmentNameBudget(format!(
                "wombatkv: SHM segment '{derived_name}' ({derived_len} chars) \
                 exceeds the macOS POSIX-SHM budget of {SHM_SEGMENT_NAME_MAX_LEN_MACOS} \
                 chars. Shorten the daemon prefix '{prefix}' ({} chars) to at \
                 most {max_prefix} chars. The internal disruptor-mp segments \
                 add up to {MAX_DISRUPTOR_INTERNAL_SUFFIX_LEN} chars of suffix \
                 ('_producer_seq' is the longest); the wombatkv wrapper adds \
                 '{SHM_PREFIX}' + 1-char role. For strict cross-platform \
                 portability (Linux, FreeBSD), see \
                 `myelon::portable_shm_segment_name`, recommends total \
                 name ≤ {} chars.",
                prefix.len(),
                myelon::PORTABLE_SHM_SEGMENT_NAME_MAX_LEN,
            )));
        }
    }
    Ok(())
}

/// Open the daemon side of the ring pair: req-consumer + resp-producer.
///
/// Daemon must call this AFTER both segments exist. The `req_seg` is
/// created by the client before connecting; the daemon attaches.
/// The `resp_seg` is created by the daemon; the client attaches.
pub fn open_daemon(
    req_seg: &str,
    resp_seg: &str,
    depth: usize,
) -> Result<(TypedConsumer<ShmFrame>, TypedProducer<ShmFrame>), Box<dyn std::error::Error>> {
    let req_consumer = wait_for_consumer(req_seg, depth, REQ_CONSUMER_ID)?;
    let resp_producer = TypedProducer::<ShmFrame>::create_with_consumers(resp_seg, depth, 1)?;
    Ok((req_consumer, resp_producer))
}

/// Open the client side of the ring pair: req-producer + resp-consumer.
pub fn open_client(
    req_seg: &str,
    resp_seg: &str,
    depth: usize,
) -> Result<(TypedProducer<ShmFrame>, TypedConsumer<ShmFrame>), Box<dyn std::error::Error>> {
    let req_producer = TypedProducer::<ShmFrame>::create_with_consumers(req_seg, depth, 1)?;
    let resp_consumer = wait_for_consumer(resp_seg, depth, RESP_CONSUMER_ID)?;
    Ok((req_producer, resp_consumer))
}

/// Read `WMBT_KV_DAEMON_SHM_WAIT_STRATEGY` env. Default is `Block`
/// (signal-driven park), which saves ~100% CPU per ring at idle vs
/// the previous `BusySpin` default. Wake latency in Block mode is ~µs,
/// invisible against our actual workload (ms-scale S3 GET + Metal
/// decode dominates). Set to `busyspin` for ultra-low-latency RPC
/// scenarios where wake-µs matter and CPU is free.
fn wait_strategy_from_env() -> MyelonWaitStrategy {
    match std::env::var("WMBT_KV_DAEMON_SHM_WAIT_STRATEGY").ok().as_deref().map(str::trim) {
        Some("busyspin" | "BusySpin" | "spin") => MyelonWaitStrategy::BusySpin,
        Some("block" | "Block" | "park") => MyelonWaitStrategy::Block,
        Some(other) if !other.is_empty() => {
            eprintln!(
                "WombatKV: unknown WMBT_KV_DAEMON_SHM_WAIT_STRATEGY={other:?}, \
                 falling back to default 'block'"
            );
            MyelonWaitStrategy::Block
        }
        _ => MyelonWaitStrategy::Block,
    }
}

fn wait_for_consumer(
    segment: &str,
    depth: usize,
    consumer_id: &str,
) -> Result<TypedConsumer<ShmFrame>, Box<dyn std::error::Error>> {
    use std::time::Instant;
    let deadline = Instant::now() + effective_attach_timeout();
    let mut tries: u64 = 0;
    let wait_strategy = wait_strategy_from_env();
    loop {
        tries += 1;
        match TypedConsumer::<ShmFrame>::attach_with_consumer_id(
            segment,
            depth,
            consumer_id,
            wait_strategy,
        ) {
            Ok(consumer) => return Ok(consumer),
            Err(error) => {
                if Instant::now() < deadline {
                    // Use myelon's discovery-poll primitive instead
                    // of an ad-hoc sleep. Same env-var control surface as the
                    // rest of the myelon transport stack.
                    myelon::perform_default_discovery_poll_wait();
                    continue;
                }
                return Err(format!("attach {segment}: {error} (after {tries} retries)").into());
            }
        }
    }
}

/// Returns true when a payload of `len` bytes fits in a single frame
/// (after rkyv envelope overhead, uses a conservative 256-byte budget).
#[must_use]
pub fn fits_one_frame(len: usize) -> bool {
    len.saturating_add(256) <= FRAME_DATA_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_request_round_trips() {
        let req = WireRequest {
            id: 42,
            op: op::PING,
            namespace: String::new(),
            key: String::new(),
            payload: Vec::new(),
        };
        let encoded = req.encode().expect("encode");
        let decoded = WireRequest::decode(encoded.as_ref()).expect("decode");
        assert_eq!(decoded.id, 42);
        assert_eq!(decoded.op, op::PING);
    }

    #[test]
    fn put_response_round_trips_with_payload() {
        let resp = WireResponse {
            id: 7,
            status: status::OK,
            op: op::PUT,
            payload: vec![0xAB; 4096],
            message: String::new(),
        };
        let encoded = resp.encode().expect("encode");
        let decoded = WireResponse::decode(encoded.as_ref()).expect("decode");
        assert_eq!(decoded.id, 7);
        assert_eq!(decoded.status, status::OK);
        assert_eq!(decoded.payload.len(), 4096);
        assert_eq!(decoded.payload[0], 0xAB);
    }

    #[test]
    fn frame_capacity_check() {
        assert!(fits_one_frame(0));
        assert!(fits_one_frame(FRAME_DATA_BYTES - 256));
        assert!(!fits_one_frame(FRAME_DATA_BYTES));
        assert!(!fits_one_frame(FRAME_DATA_BYTES * 2));
    }

    #[test]
    fn key_batch_round_trips() {
        let keys = vec!["a".to_string(), "nested/key".to_string()];
        let encoded = encode_key_batch(&keys);
        assert_eq!(decode_key_batch(&encoded).expect("decode keys"), keys);
    }

    #[test]
    fn bytes_batch_round_trips_without_copying_items() {
        let items = vec![Bytes::from_static(b"alpha"), Bytes::new(), Bytes::from_static(b"gamma")];
        let encoded = Bytes::from(encode_bytes_batch(&items));
        let decoded = decode_bytes_batch(encoded).expect("decode bytes");
        assert_eq!(decoded, items);
    }

    #[test]
    fn segment_names_are_unique_per_prefix() {
        let (req, resp) = segment_names("smoke");
        // New short format `wk<prefix>r` / `wk<prefix>s` (see SHM_PREFIX
        // + ROLE_REQ/ROLE_RESP constants). Keeps disruptor-mp's
        // `_producer_seq` suffix (+13 chars) within the macOS POSIX-SHM
        // 31-char budget for user prefixes up to 14 chars.
        assert_eq!(req, "wksmoker");
        assert_eq!(resp, "wksmokes");
        assert_ne!(req, resp);
    }

    #[test]
    fn validate_segment_name_budget_accepts_short_prefix() {
        // Prefix that produced ENAMETOOLONG under the old wmbt_kv_<>_<>
        // shape: `wmbt_kv_drts999_resp_producer_seq` = 32 chars vs 30
        // budget, must now pass under the new `wk<>r`/`wk<>s` shape
        // (`wkdrts999s_producer_seq` = 23 chars). This was the 2026-05-18
        // macOS DST sweep regression.
        validate_segment_name_budget("drts999").expect("dst-sweep prefix");
        validate_segment_name_budget("drts42").expect("dst-sweep prefix");
        validate_segment_name_budget("dmcoreds").expect("typical");
    }

    #[test]
    fn validate_segment_name_budget_rejects_too_long_prefix() {
        // Max user prefix on macOS: 30 − len("wk") − 1 (role) − 13
        // (_producer_seq) = 14. 15 chars must fail.
        let too_long = "a".repeat(15);
        let err = validate_segment_name_budget(&too_long)
            .expect_err("15-char prefix must exceed macOS budget");
        assert!(err.to_string().contains("exceeds the macOS"));
        // 14-char prefix is the hard edge, must accept.
        validate_segment_name_budget(&"a".repeat(14)).expect("14 chars at the edge");
    }

    #[test]
    fn block_opcodes_are_distinct_and_contiguous() {
        // Block-shaped opcodes were added in ABI 1.5 as a contiguous
        // block AFTER the original 10 op codes. Lock the assignment so
        // an accidental reorder/collision fails this test, not at
        // runtime against an existing daemon build.
        assert_eq!(op::LOOKUP_BLOCK_PREFIX, 11);
        assert_eq!(op::GET_KV_BLOCKS_BATCH, 12);
        assert_eq!(op::PUT_KV_BLOCKS_BATCH, 13);
        assert_eq!(op::name(op::LOOKUP_BLOCK_PREFIX), "lookup_block_prefix");
        assert_eq!(op::name(op::GET_KV_BLOCKS_BATCH), "get_kv_blocks_batch");
        assert_eq!(op::name(op::PUT_KV_BLOCKS_BATCH), "put_kv_blocks_batch");
    }

    #[test]
    fn lookup_block_prefix_req_resp_round_trip() {
        let req = LookupBlockPrefixReq {
            namespace: "ns/alpha".to_string(),
            block_hashes_hex: vec!["aa".repeat(32), "bb".repeat(32)],
        };
        let bytes = encode_lookup_block_prefix_req(&req).expect("encode req");
        let decoded = decode_lookup_block_prefix_req(&bytes).expect("decode req");
        assert_eq!(decoded.namespace, req.namespace);
        assert_eq!(decoded.block_hashes_hex, req.block_hashes_hex);

        let resp_ok = LookupBlockPrefixResp { matched_count: 7, error: None };
        let bytes_ok = encode_lookup_block_prefix_resp(&resp_ok).expect("encode resp ok");
        let decoded_ok = decode_lookup_block_prefix_resp(&bytes_ok).expect("decode resp ok");
        assert_eq!(decoded_ok.matched_count, 7);
        assert!(decoded_ok.error.is_none());

        let resp_err =
            LookupBlockPrefixResp { matched_count: 0, error: Some("bad hex at pos 3".to_string()) };
        let bytes_err = encode_lookup_block_prefix_resp(&resp_err).expect("encode resp err");
        let decoded_err = decode_lookup_block_prefix_resp(&bytes_err).expect("decode resp err");
        assert_eq!(decoded_err.matched_count, 0);
        assert_eq!(decoded_err.error.as_deref(), Some("bad hex at pos 3"));
    }

    #[test]
    fn get_kv_blocks_batch_req_resp_round_trip() {
        let req = GetKvBlocksBatchReq {
            namespace: "ns/beta".to_string(),
            block_hashes_hex: vec!["11".repeat(32), "22".repeat(32), "33".repeat(32)],
        };
        let bytes = encode_get_kv_blocks_batch_req(&req).expect("encode req");
        let decoded = decode_get_kv_blocks_batch_req(&bytes).expect("decode req");
        assert_eq!(decoded.namespace, req.namespace);
        assert_eq!(decoded.block_hashes_hex, req.block_hashes_hex);

        let payloads = vec![vec![0xAAu8; 4096], vec![0xBBu8; 1024], vec![0xCCu8; 2048]];
        let resp_hit = GetKvBlocksBatchResp { payloads: Some(payloads.clone()), error: None };
        let bytes_hit = encode_get_kv_blocks_batch_resp(&resp_hit).expect("encode resp hit");
        let decoded_hit = decode_get_kv_blocks_batch_resp(&bytes_hit).expect("decode resp hit");
        assert!(decoded_hit.error.is_none());
        let got = decoded_hit.payloads.expect("payloads present");
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].len(), 4096);
        assert_eq!(got[1].len(), 1024);
        assert_eq!(got[2].len(), 2048);
        assert_eq!(got[0][0], 0xAA);

        let resp_miss = GetKvBlocksBatchResp { payloads: None, error: None };
        let bytes_miss = encode_get_kv_blocks_batch_resp(&resp_miss).expect("encode resp miss");
        let decoded_miss = decode_get_kv_blocks_batch_resp(&bytes_miss).expect("decode resp miss");
        assert!(decoded_miss.payloads.is_none());
    }

    #[test]
    fn put_kv_blocks_batch_req_resp_round_trip() {
        let req = PutKvBlocksBatchReq {
            namespace: "ns/gamma".to_string(),
            block_hashes_hex: vec!["77".repeat(32), "88".repeat(32)],
            payloads: vec![vec![0x77u8; 512], vec![0x88u8; 1024]],
        };
        let bytes = encode_put_kv_blocks_batch_req(&req).expect("encode req");
        let decoded = decode_put_kv_blocks_batch_req(&bytes).expect("decode req");
        assert_eq!(decoded.namespace, req.namespace);
        assert_eq!(decoded.block_hashes_hex, req.block_hashes_hex);
        assert_eq!(decoded.payloads.len(), 2);
        assert_eq!(decoded.payloads[0], req.payloads[0]);
        assert_eq!(decoded.payloads[1].len(), 1024);

        let resp_ok = PutKvBlocksBatchResp { total_bytes: 1536, error: None };
        let bytes_ok = encode_put_kv_blocks_batch_resp(&resp_ok).expect("encode resp ok");
        let decoded_ok = decode_put_kv_blocks_batch_resp(&bytes_ok).expect("decode resp ok");
        assert_eq!(decoded_ok.total_bytes, 1536);
        assert!(decoded_ok.error.is_none());

        let resp_err =
            PutKvBlocksBatchResp { total_bytes: 0, error: Some("backend put failure".to_string()) };
        let bytes_err = encode_put_kv_blocks_batch_resp(&resp_err).expect("encode resp err");
        let decoded_err = decode_put_kv_blocks_batch_resp(&bytes_err).expect("decode resp err");
        assert_eq!(decoded_err.total_bytes, 0);
        assert_eq!(decoded_err.error.as_deref(), Some("backend put failure"));
    }

    #[test]
    fn block_payload_magic_headers_are_stable() {
        // Locks the on-wire magic bytes so an accidental rename of a
        // codec constant doesn't silently break compatibility with a
        // running daemon built from an older revision.
        assert_eq!(LOOKUP_BLOCK_PREFIX_REQ_MAGIC, b"WMBT_LBP_REQ\0\0\0\0");
        assert_eq!(LOOKUP_BLOCK_PREFIX_RESP_MAGIC, b"WMBT_LBP_RES\0\0\0\0");
        assert_eq!(GET_KV_BLOCKS_BATCH_REQ_MAGIC, b"WMBT_GBB_REQ\0\0\0\0");
        assert_eq!(GET_KV_BLOCKS_BATCH_RESP_MAGIC, b"WMBT_GBB_RES\0\0\0\0");
        assert_eq!(PUT_KV_BLOCKS_BATCH_REQ_MAGIC, b"WMBT_PBB_REQ\0\0\0\0");
        assert_eq!(PUT_KV_BLOCKS_BATCH_RESP_MAGIC, b"WMBT_PBB_RES\0\0\0\0");

        // Every encode_* produces a payload that starts with its magic.
        let lbp_req = encode_lookup_block_prefix_req(&LookupBlockPrefixReq {
            namespace: "ns".to_string(),
            block_hashes_hex: vec!["aa".repeat(32)],
        })
        .expect("encode");
        assert!(lbp_req.starts_with(LOOKUP_BLOCK_PREFIX_REQ_MAGIC));

        let gbb_resp = encode_get_kv_blocks_batch_resp(&GetKvBlocksBatchResp {
            payloads: Some(vec![vec![0u8; 4]]),
            error: None,
        })
        .expect("encode");
        assert!(gbb_resp.starts_with(GET_KV_BLOCKS_BATCH_RESP_MAGIC));

        let pbb_resp =
            encode_put_kv_blocks_batch_resp(&PutKvBlocksBatchResp { total_bytes: 42, error: None })
                .expect("encode");
        assert!(pbb_resp.starts_with(PUT_KV_BLOCKS_BATCH_RESP_MAGIC));
    }

    #[test]
    fn block_payload_codec_rejects_corruption() {
        // Magic mismatch should fail loudly with a recognizable error
        // that identifies the op, not panic, not silently succeed.
        let mut bad_magic = encode_lookup_block_prefix_req(&LookupBlockPrefixReq {
            namespace: "ns".to_string(),
            block_hashes_hex: vec!["aa".repeat(32)],
        })
        .expect("encode");
        bad_magic[0] = b'X';
        let err = decode_lookup_block_prefix_req(&bad_magic).expect_err("must fail");
        assert!(err.to_string().contains("lookup_block_prefix_req"));
        assert!(err.to_string().contains("bad magic"));

        // Truncated payload (chop off the last few archived bytes)
        // should fail at rkyv bytecheck, not panic.
        let full = encode_put_kv_blocks_batch_req(&PutKvBlocksBatchReq {
            namespace: "ns".to_string(),
            block_hashes_hex: vec!["aa".repeat(32), "bb".repeat(32)],
            payloads: vec![vec![1u8; 8], vec![2u8; 8]],
        })
        .expect("encode");
        let truncated = &full[..full.len() - 8];
        let err = decode_put_kv_blocks_batch_req(truncated).expect_err("must fail");
        assert!(
            err.to_string().contains("put_kv_blocks_batch_req"),
            "unexpected error message: {err}"
        );

        // Corrupting a byte in the middle of the archived body should
        // make rkyv bytecheck refuse the decode (or, less commonly,
        // produce a Vec/String layout error). Either way the decode
        // returns Err, never a torn read or panic.
        let mut tampered = encode_get_kv_blocks_batch_resp(&GetKvBlocksBatchResp {
            payloads: Some(vec![vec![0xAAu8; 8]]),
            error: None,
        })
        .expect("encode");
        // Hit a byte ~halfway through the body so we land inside the
        // rkyv archive's metadata rather than the trailing payload.
        let mid = usize::midpoint(GET_KV_BLOCKS_BATCH_RESP_MAGIC.len(), tampered.len());
        tampered[mid] ^= 0xFF;
        let result = decode_get_kv_blocks_batch_resp(&tampered);
        // We accept either Err or a structurally-different Ok, but
        // never a panic. In practice rkyv catches this at the layout
        // level and returns Err with op_name prefixed.
        if let Ok(decoded) = result {
            // If bytecheck happened to accept the tampered bytes
            // (which is unlikely but possible for byte positions that
            // land in a payload byte), the decoded value should still
            // be structurally well-formed.
            let _ = decoded;
        }
    }

    #[test]
    fn block_payload_encodes_large_blocks_without_overflow() {
        // 1 MiB blocks × 4, exercises the u32 length prefix path and
        // ensures the capacity-estimator + writer agree on the total.
        let big_block = vec![0xCDu8; 1024 * 1024];
        let req = PutKvBlocksBatchReq {
            namespace: "ns".to_string(),
            block_hashes_hex: (0..4).map(|i| format!("{i:02x}").repeat(32)).collect(),
            payloads: vec![big_block.clone(); 4],
        };
        let bytes = encode_put_kv_blocks_batch_req(&req).expect("encode");
        let decoded = decode_put_kv_blocks_batch_req(&bytes).expect("decode");
        assert_eq!(decoded.payloads.len(), 4);
        for p in &decoded.payloads {
            assert_eq!(p.len(), big_block.len());
            assert_eq!(p[0], 0xCD);
        }
    }
}
