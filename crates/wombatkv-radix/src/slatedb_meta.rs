//! L1 SlateDB-backed `MetadataIndex` (RFC 0008).
//!
//! Delivers the "world knowledge persisted across restarts" property:
//! a `SlateDbMetadataIndex` opened over the same `bucket+node_id` will
//! see all entries written by the previous process. This is the value-
//! prop missing from the L0 `InMemoryMetadataIndex`.
//!
//! ## Architecture
//!
//! - **Per-node, single-writer.** Each `WombatKV` process owns one `SlateDB`
//!   instance under `<bucket>/<node_id>/slatedb/` (RFC 0008 §4). Cross-
//!   node visibility is via offline reconciliation (multi-node, out of scope
//!   here).
//! - **Sync trait, async substrate.** `SlateDB` v0.13 is async-only; the
//!   `MetadataIndex` trait is sync. We embed a multi-thread tokio
//!   runtime in the struct and `block_on` each call. This keeps the
//!   trait stable and matches the embedded execution model used by the
//!   rest of `wombatkv-radix`.
//! - **rkyv encoding (RFC 0008 §4e).** Values are encoded as
//!   `v001` schema-tag + rkyv-archived `BlockMeta`. Adding rkyv derives
//!   to `BlockMeta` did NOT compromise its `Copy` shape (every field is
//!   POD; serde and rkyv coexist), so the encoding is internal and the
//!   trait surface is unchanged. On encountering a value with a
//!   non-`v001` prefix, the reader treats it as a miss and the caller
//!   cold-fetches from S3.
//!
//! ## Key layout (RFC 0008 §3.2)
//!
//! Two keys per record:
//! 1. **Hash-indexed (primary for trait ops):**
//!    `h/<block_hash_hex>` → `v001 | rkyv(meta)`
//!    All trait methods (`get`, `get_and_touch`, `remove`,
//!    `longest_prefix`) hit this key. O(1) lookup by hash.
//! 2. **Topological (RFC §3.2 layout):**
//!    `t/<namespace>/<model_digest_hex>/<block_seq:08x>/<block_hash_hex>`
//!    → `v001 | rkyv(meta)`
//!    Enables `scan_prefix("t/<namespace>/<model_digest>/")` for
//!    bootstrap / reconciliation passes that need topologically-ordered
//!    blocks of a model. Not used by the trait surface itself.
//!
//! The duplication costs ~2x bytes on disk per record; ~288 B total
//! per block (key+value ~144 B each). At 10^6 blocks that is ~288 MB -
//! acceptable for the `SlateDB` L0/L1 SSTs.
//!
//! ## Durability
//!
//! `insert` calls `db.put(...).await` which acks once the write is
//! buffered in the WAL. With default options `SlateDB` flushes the WAL
//! to object store on a `flush_interval`; we additionally call
//! `db.flush().await` after every `insert` for strict durability
//! (RFC 0008 §7 `WMBT_KV_METADATA_DURABILITY=strict`). The async mode
//! is left as future work.

use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;

use rkyv::rancor::Error as RkyvError;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;
use tokio::runtime::{Builder, Runtime};

use crate::block_meta::{BlockHash, BlockMeta, MetadataIndex};

/// 4-byte schema-version tag prepended to every `SlateDB` value.
///
/// RFC 0008 §4e: rkyv is forward-compatible only when the producer +
/// consumer agree on the archive layout. Bumping this tag is the explicit
/// way to signal an incompatible schema change to readers, they treat
/// values with a non-matching prefix as "missing" and cold-fetch from S3
/// rather than risk a torn read. Alpha launches with `v001` (rkyv-
/// archived `BlockMeta`); pre-alpha breaking-window means there's no
/// legacy schema to migrate from.
const META_VALUE_SCHEMA_TAG: &[u8; 4] = b"v001";

fn encode_meta_value(meta: &BlockMeta) -> Vec<u8> {
    let body = rkyv::to_bytes::<RkyvError>(meta)
        .expect("rkyv encoding a BlockMeta cannot fail (POD-only fields)");
    let mut out = Vec::with_capacity(META_VALUE_SCHEMA_TAG.len() + body.len());
    out.extend_from_slice(META_VALUE_SCHEMA_TAG);
    out.extend_from_slice(body.as_slice());
    out
}

/// Decode a `SlateDB` value into a `BlockMeta`. Returns `None` when the
/// value carries a schema tag the current build does not understand
/// (so the caller treats it as a miss and cold-fetches).
///
/// On valid `v001` values: copies the body into an `AlignedVec<16>`
/// (rkyv 0.8 requires 16-byte alignment for the archive root, and the
/// 4-byte prefix breaks the alignment of the underlying `SlateDB` read
/// buffer) then runs `from_bytes`. One alloc + memcpy + bytecheck +
/// Deserialize per value. The Deserialize step is essentially a memcpy
/// for `BlockMeta` because every field is POD.
fn decode_meta_value(value: &[u8]) -> Option<BlockMeta> {
    if value.len() < META_VALUE_SCHEMA_TAG.len() {
        return None;
    }
    if &value[..META_VALUE_SCHEMA_TAG.len()] != META_VALUE_SCHEMA_TAG {
        return None;
    }
    let body = &value[META_VALUE_SCHEMA_TAG.len()..];
    let mut aligned: rkyv::util::AlignedVec<16> = rkyv::util::AlignedVec::with_capacity(body.len());
    aligned.extend_from_slice(body);
    rkyv::from_bytes::<BlockMeta, RkyvError>(&aligned[..]).ok()
}

/// Errors surfaced by `SlateDbMetadataIndex::open`.
///
/// Hot-path trait methods (`insert`/`get`/...) intentionally do not
/// return `Result` to match the L0 sync signature; they `.expect(...)`
/// on `SlateDB` errors because a write failure to the metadata layer is
/// a process-level invariant break (RFC 0008 §7: payload is on S3
/// regardless, so a metadata write failure is recoverable via the
/// reconciler, but signals something is wrong with `SlateDB`).
#[derive(Debug)]
pub enum SlateDbMetaError {
    Io(std::io::Error),
    ObjectStore(slatedb::object_store::Error),
    SlateDb(slatedb::Error),
    InUse(&'static str),
}

impl std::fmt::Display for SlateDbMetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::ObjectStore(e) => write!(f, "object_store error: {e}"),
            Self::SlateDb(e) => write!(f, "slatedb error: {e}"),
            Self::InUse(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for SlateDbMetaError {}

impl From<std::io::Error> for SlateDbMetaError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<slatedb::Error> for SlateDbMetaError {
    fn from(e: slatedb::Error) -> Self {
        Self::SlateDb(e)
    }
}

impl From<slatedb::object_store::Error> for SlateDbMetaError {
    fn from(e: slatedb::object_store::Error) -> Self {
        Self::ObjectStore(e)
    }
}

/// L1 SlateDB-backed metadata index. See module docs.
pub struct SlateDbMetadataIndex {
    db: Arc<Db>,
    runtime: Runtime,
    namespace: String,
    /// Per-instance node identifier, persisted as the `SlateDB` path
    /// component. Kept on the struct for future cross-node reconciler
    /// hooks (RFC 0008 §4); not used by the current trait surface.
    #[allow(dead_code)]
    node_id: String,
}

impl SlateDbMetadataIndex {
    /// Open (or create) a `SlateDB` at `<root>/<node_id>/slatedb/` on the
    /// local filesystem. The local filesystem variant is the default
    /// for embedded/dev use; production deployments use
    /// `open_with_object_store` with an S3-backed `ObjectStore`.
    ///
    /// `namespace` scopes keys to a tenant (RFC 0008 §3.2). It must be
    /// a non-empty string free of `/`.
    pub fn open_local(
        root: impl Into<PathBuf>,
        node_id: &str,
        namespace: &str,
    ) -> Result<Self, SlateDbMetaError> {
        let root = root.into();
        let node_root = root.join(node_id);
        std::fs::create_dir_all(&node_root)?;
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&node_root)?);
        Self::open_with_object_store(object_store, "slatedb", node_id, namespace)
    }

    /// Open with an in-memory object store. Use for tests that don't
    /// need to outlive the process; for restart tests use `open_local`
    /// against a tempdir.
    pub fn open_in_memory(node_id: &str, namespace: &str) -> Result<Self, SlateDbMetaError> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Self::open_with_object_store(object_store, "slatedb", node_id, namespace)
    }

    /// Open with a caller-provided `ObjectStore`. The `SlateDB` lives at
    /// `<path>/` within that store; in production we pass S3 with a
    /// prefix of `<bucket>/<node_id>/slatedb/`.
    pub fn open_with_object_store(
        object_store: Arc<dyn ObjectStore>,
        path: impl Into<String>,
        node_id: &str,
        namespace: &str,
    ) -> Result<Self, SlateDbMetaError> {
        assert!(!namespace.is_empty(), "namespace must not be empty");
        assert!(
            !namespace.contains('/'),
            "namespace must not contain '/' (would collide with key separator)"
        );

        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(SlateDbMetaError::Io)?;

        let path = path.into();
        let object_store_clone = object_store.clone();
        let db = runtime.block_on(async move { Db::open(path, object_store_clone).await })?;

        Ok(Self {
            db: Arc::new(db),
            runtime,
            namespace: namespace.to_string(),
            node_id: node_id.to_string(),
        })
    }

    /// Hash-indexed key: `h/<hex>`. Primary key for trait lookups.
    fn key_by_hash(&self, hash: &BlockHash) -> Vec<u8> {
        let mut k = Vec::with_capacity(2 + 64);
        k.extend_from_slice(b"h/");
        k.extend_from_slice(hex_lower(hash).as_bytes());
        k
    }

    /// Topological key: `t/<namespace>/<model_digest_hex>/<seq:08x>/<hash_hex>`.
    /// Used for ordered scans during bootstrap/reconciliation; not used
    /// by trait methods.
    fn key_topological(&self, hash: &BlockHash, meta: &BlockMeta) -> Vec<u8> {
        let mut k = Vec::with_capacity(2 + self.namespace.len() + 1 + 48 + 1 + 8 + 1 + 64);
        k.extend_from_slice(b"t/");
        k.extend_from_slice(self.namespace.as_bytes());
        k.push(b'/');
        k.extend_from_slice(hex_lower(&meta.model_digest).as_bytes());
        k.push(b'/');
        k.extend_from_slice(format!("{:08x}", meta.block_seq).as_bytes());
        k.push(b'/');
        k.extend_from_slice(hex_lower(hash).as_bytes());
        k
    }

    /// Prefix for topological scan within a namespace: `t/<namespace>/`.
    /// Kept pub(crate) for the future reconciler / `bootstrap_from_slatedb`
    /// helper.
    #[allow(dead_code)]
    pub(crate) fn topological_prefix(&self) -> Vec<u8> {
        let mut k = Vec::with_capacity(2 + self.namespace.len() + 1);
        k.extend_from_slice(b"t/");
        k.extend_from_slice(self.namespace.as_bytes());
        k.push(b'/');
        k
    }

    /// Cleanly flush + close the underlying `SlateDB`. Calling this is
    /// required to guarantee all writes are durable on the object
    /// store; `Drop` only spins down the runtime, it does NOT flush.
    pub fn close(self) -> Result<(), SlateDbMetaError> {
        let Self { db, runtime, .. } = self;
        // We hold the last strong reference to db; unwrap it so we can
        // call `close` (which consumes self by &self but needs to
        // outlive callers. Arc::try_unwrap is the only race-free shape).
        let db = Arc::try_unwrap(db)
            .map_err(|_| SlateDbMetaError::InUse("close: SlateDB handle has outstanding clones"))?;
        runtime.block_on(async move { db.close().await })?;
        Ok(())
    }

    fn put_meta(&self, hash: &BlockHash, meta: &BlockMeta) {
        // RFC 0008 §4e: rkyv-archived body prefixed by the 4-byte
        // schema tag (`v001`). Values that lack the tag are treated as
        // missing by `decode_meta_value` and cold-fetch from S3.
        let value = encode_meta_value(meta);
        let key_h = self.key_by_hash(hash);
        let key_t = self.key_topological(hash, meta);
        let db = self.db.clone();
        self.runtime.block_on(async move {
            db.put(&key_h, &value).await.expect("slatedb put(hash) failed");
            db.put(&key_t, &value).await.expect("slatedb put(topological) failed");
            // Strict durability: flush after every insert. RFC 0008 §7.
            // The async-durability mode is left for a follow-up.
            db.flush().await.expect("slatedb flush failed");
        });
    }

    fn read_meta(&self, hash: &BlockHash) -> Option<BlockMeta> {
        let key_h = self.key_by_hash(hash);
        let db = self.db.clone();
        let bytes = self
            .runtime
            .block_on(async move { db.get(&key_h).await.expect("slatedb get failed") })?;
        // On schema-tag mismatch we return None, the caller treats it
        // as a miss and cold-fetches from S3. This is the documented
        // forward-compat strategy (RFC 0008 §4e); panicking would risk
        // a process-level outage when a user upgrades across an
        // incompatible schema bump.
        decode_meta_value(bytes.as_ref())
    }
}

impl MetadataIndex for SlateDbMetadataIndex {
    fn insert(&self, hash: BlockHash, mut meta: BlockMeta) {
        meta.touch();
        self.put_meta(&hash, &meta);
    }

    fn get(&self, hash: &BlockHash) -> Option<BlockMeta> {
        self.read_meta(hash)
    }

    fn get_and_touch(&self, hash: &BlockHash) -> Option<BlockMeta> {
        let mut meta = self.read_meta(hash)?;
        meta.touch();
        self.put_meta(hash, &meta);
        Some(meta)
    }

    fn remove(&self, hash: &BlockHash) -> bool {
        // We need the meta to compute the topological key. If the
        // entry is absent, return false without touching SlateDB.
        let Some(meta) = self.read_meta(hash) else {
            return false;
        };
        let key_h = self.key_by_hash(hash);
        let key_t = self.key_topological(hash, &meta);
        let db = self.db.clone();
        self.runtime.block_on(async move {
            db.delete(&key_h).await.expect("slatedb delete(hash) failed");
            db.delete(&key_t).await.expect("slatedb delete(topological) failed");
            db.flush().await.expect("slatedb flush failed");
        });
        true
    }

    fn longest_prefix(&self, chain: &[BlockHash]) -> usize {
        // Per-hash point reads. The L1 impl is not the hot path; ds4
        // production uses the L0 RAM tree (RFC 0008 §8). This walk is
        // intended for offline/reconciliation tooling.
        let mut k = 0_usize;
        for h in chain {
            if self.read_meta(h).is_some() {
                k += 1;
            } else {
                break;
            }
        }
        k
    }

    fn len(&self) -> usize {
        // Approximate count via prefix scan over the hash-indexed
        // keyspace. SlateDB does not expose an O(1) count; we accept
        // O(n) here because callers that care about exact counts are
        // few and the trait contract says approximate.
        let db = self.db.clone();
        self.runtime.block_on(async move {
            let mut iter = db
                .scan::<&[u8], (Bound<&[u8]>, Bound<&[u8]>)>((
                    Bound::Included(b"h/" as &[u8]),
                    Bound::Excluded(b"h0" as &[u8]),
                ))
                .await
                .expect("slatedb scan failed");
            let mut n = 0_usize;
            while iter.next().await.expect("slatedb scan next failed").is_some() {
                n += 1;
            }
            n
        })
    }

    fn entries(&self) -> Vec<(BlockHash, BlockMeta)> {
        // Used by the §6 prefetcher and the cold-bootstrap §5 path to
        // pull the full namespace into the L0 RAM tree. Walks the
        // hash-indexed prefix once; one rkyv decode per record.
        // Hash bytes are decoded from the key suffix; this avoids
        // having to read the meta first and then reconstruct. Values
        // with an unknown schema tag are silently skipped, the next
        // access will cold-fetch them.
        let db = self.db.clone();
        self.runtime.block_on(async move {
            let mut iter = db
                .scan::<&[u8], (Bound<&[u8]>, Bound<&[u8]>)>((
                    Bound::Included(b"h/" as &[u8]),
                    Bound::Excluded(b"h0" as &[u8]),
                ))
                .await
                .expect("slatedb scan failed");
            let mut out = Vec::new();
            while let Some(kv) = iter.next().await.expect("slatedb scan next failed") {
                // Key is `h/<64-hex-bytes>`; skip the 2-byte `h/`
                // prefix and decode the suffix into a 32-byte hash.
                let key = kv.key.as_ref();
                if key.len() != 2 + 64 || &key[..2] != b"h/" {
                    continue;
                }
                let Some(hash) = decode_hex_32(&key[2..]) else {
                    continue;
                };
                let Some(meta) = decode_meta_value(kv.value.as_ref()) else {
                    continue;
                };
                out.push((hash, meta));
            }
            out
        })
    }
}

/// Decode 64 lowercase hex bytes into 32 raw bytes. Returns None on
/// any non-hex character.
fn decode_hex_32(s: &[u8]) -> Option<BlockHash> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        let hi = hex_val(s[2 * i])?;
        let lo = hex_val(s[2 * i + 1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(10 + c - b'a'),
        _ => None,
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX_CHARS[(b >> 4) as usize] as char);
        s.push(HEX_CHARS[(b & 0xf) as usize] as char);
    }
    s
}

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_meta::BlockMeta;
    use tempfile::TempDir;

    fn mk_meta(seq: u32, parent: BlockHash) -> BlockMeta {
        BlockMeta::new_successor(parent, seq, 1024, [42u8; 24], *b"test-v1\0\0\0\0\0\0\0\0\0")
    }

    fn new_in_memory(name: &str) -> SlateDbMetadataIndex {
        SlateDbMetadataIndex::open_in_memory(name, "tenant-a")
            .expect("open in-memory slatedb metadata index")
    }

    #[test]
    fn insert_get_roundtrip() {
        let idx = new_in_memory("node-roundtrip");
        let h = [1u8; 32];
        idx.insert(h, mk_meta(0, BlockMeta::ZERO_HASH));
        let got = idx.get(&h).expect("get must return inserted meta");
        assert_eq!(got.block_seq, 0);
        assert_eq!(got.payload_bytes, 1024);
        assert_eq!(got.parent_hash, BlockMeta::ZERO_HASH);
    }

    #[test]
    fn get_returns_none_for_missing() {
        let idx = new_in_memory("node-missing");
        assert!(idx.get(&[99u8; 32]).is_none());
    }

    #[test]
    fn get_and_touch_updates_access() {
        let idx = new_in_memory("node-touch");
        let h = [2u8; 32];
        idx.insert(h, mk_meta(0, BlockMeta::ZERO_HASH));
        let t1 = idx.get(&h).unwrap().last_access_ns;
        // Sleep long enough to exceed system clock resolution on
        // every platform we care about (macOS, Linux).
        std::thread::sleep(std::time::Duration::from_millis(5));
        let t2 = idx.get_and_touch(&h).unwrap().last_access_ns;
        assert!(t2 >= t1, "expected t2 >= t1, got t1={t1} t2={t2}");
        // The persisted record should reflect the touch.
        let t3 = idx.get(&h).unwrap().last_access_ns;
        assert!(t3 >= t2, "touch did not persist: t2={t2} t3={t3}");
    }

    #[test]
    fn longest_prefix_chain() {
        let idx = new_in_memory("node-prefix");
        let h0 = [10u8; 32];
        let h1 = [11u8; 32];
        let h2 = [12u8; 32];
        idx.insert(h0, mk_meta(0, BlockMeta::ZERO_HASH));
        idx.insert(h1, mk_meta(1, h0));
        // h2 NOT inserted
        assert_eq!(idx.longest_prefix(&[h0, h1, h2]), 2);
        assert_eq!(idx.longest_prefix(&[h0]), 1);
        assert_eq!(idx.longest_prefix(&[h2]), 0);
    }

    #[test]
    fn remove_works() {
        let idx = new_in_memory("node-remove");
        let h = [3u8; 32];
        idx.insert(h, mk_meta(0, BlockMeta::ZERO_HASH));
        assert_eq!(idx.len(), 1);
        assert!(idx.remove(&h));
        assert_eq!(idx.len(), 0);
        assert!(!idx.remove(&h));
        assert!(idx.get(&h).is_none());
    }

    #[test]
    fn len_counts_distinct_hashes() {
        let idx = new_in_memory("node-len");
        assert_eq!(idx.len(), 0);
        for i in 1..=5_u8 {
            idx.insert([i; 32], mk_meta(u32::from(i), BlockMeta::ZERO_HASH));
        }
        assert_eq!(idx.len(), 5);
    }

    #[test]
    fn entries_returns_all_pairs() {
        let idx = new_in_memory("node-entries");
        let h0 = [30u8; 32];
        let h1 = [31u8; 32];
        let h2 = [32u8; 32];
        idx.insert(h0, mk_meta(0, BlockMeta::ZERO_HASH));
        idx.insert(h1, mk_meta(1, h0));
        idx.insert(h2, mk_meta(2, h1));

        let mut entries = idx.entries();
        entries.sort_by_key(|(_, m)| m.block_seq);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, h0);
        assert_eq!(entries[1].0, h1);
        assert_eq!(entries[2].0, h2);
        assert_eq!(entries[2].1.block_seq, 2);
        assert_eq!(entries[1].1.parent_hash, h0);
    }

    #[test]
    fn value_encoding_is_rkyv_with_version_prefix() {
        // RFC 0008 §4e: every SlateDB value starts with the 4-byte
        // schema tag `v001`. Mismatched-tag values round-trip as None
        // so the caller cold-fetches from S3.
        let meta = mk_meta(7, BlockMeta::ZERO_HASH);

        let encoded = encode_meta_value(&meta);
        assert!(encoded.len() > META_VALUE_SCHEMA_TAG.len());
        assert_eq!(&encoded[..META_VALUE_SCHEMA_TAG.len()], META_VALUE_SCHEMA_TAG);
        assert_eq!(META_VALUE_SCHEMA_TAG, b"v001");

        let decoded = decode_meta_value(&encoded).expect("rkyv decode");
        assert_eq!(decoded.block_seq, meta.block_seq);
        assert_eq!(decoded.payload_bytes, meta.payload_bytes);
        assert_eq!(decoded.model_digest, meta.model_digest);
        assert_eq!(decoded.layout_tag, meta.layout_tag);
        assert_eq!(decoded.parent_hash, meta.parent_hash);
        assert_eq!(decoded.ext_flags, meta.ext_flags);

        // Wrong magic → silent miss (cold-fetch fallback).
        let mut wrong_tag = encoded.clone();
        wrong_tag[..4].copy_from_slice(b"vBAD");
        assert!(decode_meta_value(&wrong_tag).is_none());

        // Bytes with a non-matching prefix (e.g., the legacy bincode
        // encoding which had no prefix) → miss.
        let legacy = bincode::serialize(&meta).expect("bincode");
        assert_ne!(&legacy[..4], META_VALUE_SCHEMA_TAG);
        assert!(decode_meta_value(&legacy).is_none());

        // Truncated tag → miss.
        assert!(decode_meta_value(&encoded[..3]).is_none());
        assert!(decode_meta_value(b"").is_none());
    }

    #[test]
    fn persists_across_restart() {
        // This is the L1 value-prop: the L0 InMemoryMetadataIndex
        // cannot do this. Open against a local tempdir, write entries,
        // close, reopen, verify.
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path().to_path_buf();
        let node_id = "node-persist";
        let ns = "tenant-a";

        let h0 = [21u8; 32];
        let h1 = [22u8; 32];

        {
            let idx = SlateDbMetadataIndex::open_local(&root, node_id, ns)
                .expect("first open must succeed");
            idx.insert(h0, mk_meta(0, BlockMeta::ZERO_HASH));
            idx.insert(h1, mk_meta(1, h0));
            assert_eq!(idx.len(), 2);
            idx.close().expect("close must flush");
        }

        // Second instance, sees the prior writes.
        let idx2 =
            SlateDbMetadataIndex::open_local(&root, node_id, ns).expect("reopen must succeed");
        let m0 = idx2.get(&h0).expect("h0 must persist");
        let m1 = idx2.get(&h1).expect("h1 must persist");
        assert_eq!(m0.block_seq, 0);
        assert_eq!(m1.block_seq, 1);
        assert_eq!(m1.parent_hash, h0);
        assert_eq!(idx2.len(), 2);
    }
}
