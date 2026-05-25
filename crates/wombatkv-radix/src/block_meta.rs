//! `BlockMeta` + `MetadataIndex` (RFC 0008).
//!
//! Stores per-block metadata in addition to the residency tracking in
//! `RadixIndex`: chain parent, sequence number, payload size, last
//! access. The L0 in-memory implementation here is sufficient on its
//! own; the SlateDB-backed impl wraps this with durability.
//!
//! The `BlockMeta` field types are chosen to be `Copy` and small, so
//! the index can hold millions of entries within tight memory budgets
//! (see `MemoryGuardrail`).
//!
//! ## Schema
//!
//! 152 bytes per block (with `model_digest` at 24 B + `layout_tag` at 16 B).
//! For 10^6 blocks: ~152 MB heap. With `MemoryGuardrail`, we cap entry
//! count vs available RAM.
//!
//! ## Block-sequence structure
//!
//! `parent_hash` makes each entry a node in the block-sequence graph
//! (RFC 0006 §3.1). `block_seq` gives the position in the sequence (0 =
//! root). Together they let a reconciler walk any block back to its
//! root to validate sequence integrity.

use std::collections::BTreeMap;

/// 32-byte blake3 content-address (RFC 0006 KVBlock/0.1 chunk-key hash).
pub type BlockHash = [u8; 32];

/// 24-character model fingerprint (truncated SHA256 of model card).
pub type ModelDigest = [u8; 24];

/// 16-byte tag for the per-block payload layout. Examples:
///   `b"ds4-v1\0\0\0\0\0\0\0\0\0\0"` : RFC 0007
///   `b"sglang-page-v1\0\0"`          : RFC 0003 page payload
pub type LayoutTag = [u8; 16];

/// Per-block metadata kept in the index. Distinct from
/// `wombatkv_radix::Entry` which tracks residency only.
///
/// `serde::{Serialize, Deserialize}` are derived for legacy callers (tests,
/// debug dumps, future JSON inspection tooling). `rkyv::{Archive, Serialize,
/// Deserialize}` are derived for the L1 SlateDB-backed impl's on-disk
/// codec: RFC 0008 §4e migrated `SlateDbMetadataIndex` from bincode to
/// rkyv for zero-copy reads on the bootstrap fast-path.
///
/// All field types are POD + `Copy`; both derive families are independent
/// at the type level, so adding rkyv does not compromise `Copy`-ness or
/// the existing serde shape. RFC 0008 Open Question 3, answered:
/// rkyv + serde coexist because every field is a fixed-size value type.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct BlockMeta {
    /// Parent hash in the block sequence. Zeroed when this is a root
    /// (e.g., the first block after the per-(model, quant) SEED).
    pub parent_hash: BlockHash,
    /// 0 for sequence root; +1 for each successor.
    pub block_seq: u32,
    /// Bytes in the per-block payload on S3.
    pub payload_bytes: u64,
    /// Last-access timestamp (nanos since UNIX epoch). Updated on every
    /// lookup hit; the prefetcher uses recency decay for scoring.
    pub last_access_ns: u64,
    /// Truncated model fingerprint (RFC 0006 §3.1 `model=<digest>`).
    pub model_digest: ModelDigest,
    /// Per-block layout family (RFC 0006 §3.2; ds4-v1, sglang-page-v1).
    pub layout_tag: LayoutTag,
    /// Layout-specific extension flags (e.g., compressor frontier
    /// included; rkyv schema version; reserved).
    pub ext_flags: u32,
}

impl BlockMeta {
    pub const ZERO_HASH: BlockHash = [0u8; 32];

    #[must_use]
    pub fn new_root(payload_bytes: u64, model_digest: ModelDigest, layout_tag: LayoutTag) -> Self {
        Self {
            parent_hash: Self::ZERO_HASH,
            block_seq: 0,
            payload_bytes,
            last_access_ns: now_ns(),
            model_digest,
            layout_tag,
            ext_flags: 0,
        }
    }

    #[must_use]
    pub fn new_successor(
        parent: BlockHash,
        seq: u32,
        payload_bytes: u64,
        model_digest: ModelDigest,
        layout_tag: LayoutTag,
    ) -> Self {
        Self {
            parent_hash: parent,
            block_seq: seq,
            payload_bytes,
            last_access_ns: now_ns(),
            model_digest,
            layout_tag,
            ext_flags: 0,
        }
    }

    pub fn touch(&mut self) {
        self.last_access_ns = now_ns();
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}

/// Abstract over the L0 (in-memory) and L1 (SlateDB-backed) variants.
/// Implementations MUST be `Send + Sync`; readers share via `Arc<dyn ..>`.
pub trait MetadataIndex: Send + Sync {
    /// Insert (or overwrite) a record. Updates the last-access stamp
    /// on the inserted record to "now".
    fn insert(&self, hash: BlockHash, meta: BlockMeta);

    /// Fetch a record without updating access time. Used by the
    /// reconciler / debug paths.
    fn get(&self, hash: &BlockHash) -> Option<BlockMeta>;

    /// Fetch + touch. The hot lookup path uses this; the touch keeps
    /// the LRU-shape access time current for the prefetcher's scoring.
    fn get_and_touch(&self, hash: &BlockHash) -> Option<BlockMeta>;

    /// Drop a record. Returns true if it existed.
    fn remove(&self, hash: &BlockHash) -> bool;

    /// Walk a candidate sequence of `block_hashes`; return the longest
    /// prefix length K where blocks `[0..K)` are all present in the
    /// index (mirrors `RadixIndex::lookup_blocks_into` but for meta-
    /// presence, not residency).
    fn longest_prefix(&self, blocks: &[BlockHash]) -> usize;

    /// Entry count (approximate for non-atomic implementations).
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot of all (hash, meta) pairs. Used by the background
    /// prefetch worker to score recency + first-block bonus per
    /// RFC 0008 §6. Default impl returns an empty Vec so existing
    /// implementations remain compilable; concrete types that can
    /// iterate cheaply (e.g., `InMemoryMetadataIndex`) override this.
    ///
    /// Implementations SHOULD avoid holding a write-lock across the
    /// returned Vec's lifetime, copy into the result and release.
    fn entries(&self) -> Vec<(BlockHash, BlockMeta)> {
        Vec::new()
    }
}

/// L0 in-memory implementation. Backed by an `RwLock<BTreeMap>` so reads
/// don't block each other; writes serialize. Suitable for a single
/// process. The L1 SlateDB-backed impl (production) wraps this with durable
/// segment writes per RFC 0008 §7.
pub struct InMemoryMetadataIndex {
    entries: std::sync::RwLock<BTreeMap<BlockHash, BlockMeta>>,
}

impl Default for InMemoryMetadataIndex {
    fn default() -> Self {
        Self { entries: std::sync::RwLock::new(BTreeMap::new()) }
    }
}

impl InMemoryMetadataIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Bulk-load existing entries (used on cold-bootstrap from S3 or
    /// `SlateDB`). Skips already-present hashes by default.
    pub fn bulk_load<I: IntoIterator<Item = (BlockHash, BlockMeta)>>(&self, items: I) {
        let mut w = self.entries.write().unwrap();
        for (h, m) in items {
            w.entry(h).or_insert(m);
        }
    }

    /// Compare-and-delete primitive used by the LRU eviction worker
    /// (RFC 0009 §4.3 race-safety story).
    ///
    /// Removes the entry for `hash` iff its current `last_access_ns`
    /// matches `expected_last_access_ns`. If a concurrent
    /// `get_and_touch` raced the eviction worker between snapshot and
    /// delete, the stamps will differ and this call returns `false` -
    /// the worker skips this block this cycle and revisits next pass.
    ///
    /// Returns:
    /// - `true` if the entry existed AND its stamp matched (entry was
    ///   removed),
    /// - `false` if the entry was absent OR its stamp had changed
    ///   (entry untouched).
    ///
    /// Atomic: holds the write lock for the duration of the
    /// compare-and-delete. Cheap because the comparison is a single
    /// `u64` read.
    pub fn remove_if_unchanged(&self, hash: &BlockHash, expected_last_access_ns: u64) -> bool {
        let mut w = self.entries.write().unwrap();
        match w.get(hash) {
            Some(meta) if meta.last_access_ns == expected_last_access_ns => {
                w.remove(hash);
                true
            }
            _ => false,
        }
    }
}

impl MetadataIndex for InMemoryMetadataIndex {
    fn insert(&self, hash: BlockHash, mut meta: BlockMeta) {
        meta.touch();
        self.entries.write().unwrap().insert(hash, meta);
    }

    fn get(&self, hash: &BlockHash) -> Option<BlockMeta> {
        self.entries.read().unwrap().get(hash).copied()
    }

    fn get_and_touch(&self, hash: &BlockHash) -> Option<BlockMeta> {
        let mut w = self.entries.write().unwrap();
        let entry = w.get_mut(hash)?;
        entry.touch();
        Some(*entry)
    }

    fn remove(&self, hash: &BlockHash) -> bool {
        self.entries.write().unwrap().remove(hash).is_some()
    }

    fn longest_prefix(&self, blocks: &[BlockHash]) -> usize {
        let r = self.entries.read().unwrap();
        let mut k = 0;
        for h in blocks {
            if r.contains_key(h) {
                k += 1;
            } else {
                break;
            }
        }
        k
    }

    fn len(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    fn entries(&self) -> Vec<(BlockHash, BlockMeta)> {
        let r = self.entries.read().unwrap();
        let mut out = Vec::with_capacity(r.len());
        for (h, m) in r.iter() {
            out.push((*h, *m));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_meta(seq: u32, parent: BlockHash) -> BlockMeta {
        BlockMeta::new_successor(parent, seq, 1024, [42u8; 24], *b"test-v1\0\0\0\0\0\0\0\0\0")
    }

    #[test]
    fn insert_get_roundtrip() {
        let idx = InMemoryMetadataIndex::new();
        let h = [1u8; 32];
        idx.insert(h, mk_meta(0, BlockMeta::ZERO_HASH));
        let got = idx.get(&h).unwrap();
        assert_eq!(got.block_seq, 0);
        assert_eq!(got.payload_bytes, 1024);
    }

    #[test]
    fn get_and_touch_updates_access() {
        let idx = InMemoryMetadataIndex::new();
        let h = [2u8; 32];
        idx.insert(h, mk_meta(0, BlockMeta::ZERO_HASH));
        let t1 = idx.get(&h).unwrap().last_access_ns;
        std::thread::sleep(std::time::Duration::from_millis(2));
        let t2 = idx.get_and_touch(&h).unwrap().last_access_ns;
        assert!(t2 >= t1);
    }

    #[test]
    fn longest_prefix_chain() {
        let idx = InMemoryMetadataIndex::new();
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
        let idx = InMemoryMetadataIndex::new();
        let h = [3u8; 32];
        idx.insert(h, mk_meta(0, BlockMeta::ZERO_HASH));
        assert_eq!(idx.len(), 1);
        assert!(idx.remove(&h));
        assert_eq!(idx.len(), 0);
        assert!(!idx.remove(&h));
    }

    #[test]
    fn bulk_load_preserves_existing() {
        let idx = InMemoryMetadataIndex::new();
        let h = [5u8; 32];
        idx.insert(h, mk_meta(99, BlockMeta::ZERO_HASH));
        idx.bulk_load([(h, mk_meta(0, BlockMeta::ZERO_HASH))]);
        // Existing entry wins (don't clobber)
        assert_eq!(idx.get(&h).unwrap().block_seq, 99);
    }

    #[test]
    fn block_meta_root_has_zero_parent() {
        let m = BlockMeta::new_root(2048, [7u8; 24], *b"test-v1\0\0\0\0\0\0\0\0\0");
        assert_eq!(m.parent_hash, BlockMeta::ZERO_HASH);
        assert_eq!(m.block_seq, 0);
        assert_eq!(m.payload_bytes, 2048);
    }

    #[test]
    fn remove_if_unchanged_compare_and_delete() {
        let idx = InMemoryMetadataIndex::new();
        let h = [9u8; 32];
        idx.insert(h, mk_meta(0, BlockMeta::ZERO_HASH));
        let snapshot = idx.get(&h).unwrap();
        // Stamp matches the snapshot: should delete.
        assert!(idx.remove_if_unchanged(&h, snapshot.last_access_ns));
        assert!(idx.get(&h).is_none());

        // Re-insert and simulate a racing `get_and_touch` that updated
        // the stamp between snapshot and CAS.
        idx.insert(h, mk_meta(0, BlockMeta::ZERO_HASH));
        let snapshot = idx.get(&h).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        idx.get_and_touch(&h);
        // Stamp drifted: CAS should refuse and the entry should remain.
        assert!(!idx.remove_if_unchanged(&h, snapshot.last_access_ns));
        assert!(idx.get(&h).is_some());

        // Absent hash returns false (no panic).
        assert!(!idx.remove_if_unchanged(&[0xAAu8; 32], 0));
    }

    #[test]
    fn entries_snapshot_returns_all_pairs() {
        let idx = InMemoryMetadataIndex::new();
        let h0 = [20u8; 32];
        let h1 = [21u8; 32];
        let h2 = [22u8; 32];
        idx.insert(h0, mk_meta(0, BlockMeta::ZERO_HASH));
        idx.insert(h1, mk_meta(1, h0));
        idx.insert(h2, mk_meta(2, h1));

        let mut entries = idx.entries();
        entries.sort_by_key(|(_, m)| m.block_seq);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, h0);
        assert_eq!(entries[1].0, h1);
        assert_eq!(entries[2].0, h2);
        assert_eq!(entries[0].1.block_seq, 0);
        assert_eq!(entries[2].1.block_seq, 2);
    }
}
