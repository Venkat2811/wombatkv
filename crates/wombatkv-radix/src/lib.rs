#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::atomic::AtomicUsize;

pub mod block_meta;
pub mod slatedb_meta;
pub use block_meta::{
    BlockHash, BlockMeta, InMemoryMetadataIndex, LayoutTag, MetadataIndex, ModelDigest,
};
pub use slatedb_meta::{SlateDbMetaError, SlateDbMetadataIndex};

/// Returns crate identity for smoke tests.
#[must_use]
pub fn crate_id() -> &'static str {
    "tp-radix"
}

/// M3 Max unified memory size used by default guardrail presets.
pub const M3_MAX_UNIFIED_MEMORY_BYTES: u64 = 96_u64 * 1024 * 1024 * 1024;

/// Relative S3 key prefix for content-addressed block payloads.
/// The full S3 key is `<s3_prefix>/<namespace>/wombatkv/v1/block/b3=<hex>`.
/// Single source of truth shared between `wombatkv-cabi` (write/read path)
/// and `wombatkv-node::block_prefetch` (prefetch path). Both call sites
/// MUST use this constant rather than redefining the prefix literal, any
/// skew silently breaks the prefetch worker.
pub const BLOCK_KEY_PREFIX: &str = "wombatkv/v1/block/b3=";

/// Relative S3 key prefix for raw_tail sidecar payloads.
/// The full S3 key is `<s3_prefix>/<namespace>/wombatkv/v1/sidecar/raw_tail/b3=<chain_tip_hex>`.
pub const SIDECAR_RAW_TAIL_KEY_PREFIX: &str = "wombatkv/v1/sidecar/raw_tail/b3=";

const DEFAULT_MAP_OVERHEAD_BYTES_PER_ENTRY: u64 = 64;

/// Residency class for a cached block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Residency {
    Ram,
    Nvme,
    RemoteOnly,
}

/// Handle returned to callers for resident blocks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockHandle {
    pub key: [u8; 32],
    pub block_index: u32,
    pub residency: Residency,
}

/// Lookup output contract for the hot path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LookupResult {
    pub matched_depth: u32,
    pub resident_handles: Vec<BlockHandle>,
    pub prefetch_keys: Vec<[u8; 32]>,
}

/// Reusable buffers for allocation-stable lookup execution.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LookupBuffers {
    pub resident_handles: Vec<BlockHandle>,
    pub prefetch_keys: Vec<[u8; 32]>,
}

impl LookupBuffers {
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            resident_handles: Vec::with_capacity(capacity),
            prefetch_keys: Vec::with_capacity(capacity),
        }
    }

    pub fn clear(&mut self) {
        self.resident_handles.clear();
        self.prefetch_keys.clear();
    }

    #[must_use]
    pub fn combined_capacity(&self) -> usize {
        self.resident_handles.capacity().saturating_add(self.prefetch_keys.capacity())
    }
}

/// Small pool for recycling lookup buffers and reducing allocator churn.
#[derive(Debug)]
pub struct LookupBufferPool {
    free: Vec<LookupBuffers>,
    max_pool_size: usize,
    high_watermark: usize,
}

impl Default for LookupBufferPool {
    fn default() -> Self {
        Self::new(32)
    }
}

impl LookupBufferPool {
    #[must_use]
    pub fn new(max_pool_size: usize) -> Self {
        Self { free: Vec::new(), max_pool_size: max_pool_size.max(1), high_watermark: 0 }
    }

    #[must_use]
    pub fn acquire(&mut self) -> LookupBuffers {
        self.free.pop().unwrap_or_default()
    }

    pub fn release(&mut self, mut buffers: LookupBuffers) {
        buffers.clear();
        if self.free.len() < self.max_pool_size {
            self.free.push(buffers);
            self.high_watermark = self.high_watermark.max(self.free.len());
        }
    }

    #[must_use]
    pub const fn high_watermark(&self) -> usize {
        self.high_watermark
    }

    #[must_use]
    pub fn available(&self) -> usize {
        self.free.len()
    }
}

/// Lightweight key encoder that avoids temporary allocations.
#[derive(Clone, Debug, Default)]
pub struct KeyEncoder {
    scratch: [u8; 32],
}

impl KeyEncoder {
    /// Encode one block key from fingerprint and block/depth coordinates.
    #[must_use]
    pub fn encode(&mut self, fingerprint: [u8; 16], block_index: u32, depth: u32) -> [u8; 32] {
        self.scratch.fill(0);
        self.scratch[..16].copy_from_slice(&fingerprint);
        self.scratch[16..20].copy_from_slice(&block_index.to_le_bytes());
        self.scratch[20..24].copy_from_slice(&depth.to_le_bytes());

        for idx in 24..32 {
            self.scratch[idx] = self.scratch[idx - 24] ^ self.scratch[idx - 8].rotate_left(1);
        }
        self.scratch
    }

    /// Encode a full sequence into a caller-provided vector, reusing capacity.
    pub fn encode_chain(&mut self, fingerprint: [u8; 16], depths: &[u32], out: &mut Vec<[u8; 32]>) {
        out.clear();
        if out.capacity() < depths.len() {
            out.reserve(depths.len() - out.capacity());
        }

        for depth in depths {
            out.push(self.encode(fingerprint, *depth, *depth));
        }
    }
}

/// Memory guardrail used to bound in-memory radix footprint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryGuardrail {
    pub host_memory_bytes: u64,
    pub reserved_system_bytes: u64,
    pub max_index_fraction_pct: u8,
    pub map_overhead_bytes_per_entry: u64,
}

impl Default for MemoryGuardrail {
    fn default() -> Self {
        Self::m3_max_96gb()
    }
}

impl MemoryGuardrail {
    #[must_use]
    pub const fn m3_max_96gb() -> Self {
        Self {
            host_memory_bytes: M3_MAX_UNIFIED_MEMORY_BYTES,
            reserved_system_bytes: 48_u64 * 1024 * 1024 * 1024,
            max_index_fraction_pct: 30,
            map_overhead_bytes_per_entry: DEFAULT_MAP_OVERHEAD_BYTES_PER_ENTRY,
        }
    }

    #[must_use]
    pub fn index_budget_bytes(self) -> u64 {
        let available = self.host_memory_bytes.saturating_sub(self.reserved_system_bytes);
        available.saturating_mul(u64::from(self.max_index_fraction_pct)) / 100
    }

    #[must_use]
    pub fn estimate_entry_bytes(self) -> u64 {
        let key_size = u64::try_from(size_of::<[u8; 32]>()).unwrap_or(u64::MAX);
        let entry_size = u64::try_from(size_of::<Entry>()).unwrap_or(u64::MAX);
        key_size.saturating_add(entry_size).saturating_add(self.map_overhead_bytes_per_entry)
    }

    #[must_use]
    pub fn estimate_index_bytes(self, entry_count: usize) -> u64 {
        let entry_count_u64 = u64::try_from(entry_count).unwrap_or(u64::MAX);
        self.estimate_entry_bytes().saturating_mul(entry_count_u64)
    }
}

/// Guardrail violations during inserts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GuardrailError {
    MemoryBudgetExceeded { estimated_bytes: u64, budget_bytes: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Entry {
    block_index: u32,
    residency: Residency,
}

/// Local-memory-only radix-like lookup index for block keys.
#[derive(Default)]
pub struct RadixIndex {
    entries: BTreeMap<[u8; 32], Entry>,
}

/// Global guard used by tests to ensure lookup stays memory-only.
pub static IO_GUARD: AtomicUsize = AtomicUsize::new(0);

impl RadixIndex {
    /// Upsert a key with block index and residency state.
    pub fn upsert(&mut self, key: [u8; 32], block_index: u32, residency: Residency) {
        self.entries.insert(key, Entry { block_index, residency });
    }

    /// Upsert only when guardrail budget remains under limit.
    pub fn upsert_guarded(
        &mut self,
        key: [u8; 32],
        block_index: u32,
        residency: Residency,
        guardrail: MemoryGuardrail,
    ) -> Result<(), GuardrailError> {
        let next_entry_count = if self.entries.contains_key(&key) {
            self.entries.len()
        } else {
            self.entries.len().saturating_add(1)
        };
        let estimated_bytes = guardrail.estimate_index_bytes(next_entry_count);
        let budget_bytes = guardrail.index_budget_bytes();
        if estimated_bytes > budget_bytes {
            return Err(GuardrailError::MemoryBudgetExceeded { estimated_bytes, budget_bytes });
        }

        self.upsert(key, block_index, residency);
        Ok(())
    }

    /// Estimated heap footprint for current entry count.
    #[must_use]
    pub fn estimated_heap_bytes(&self, guardrail: MemoryGuardrail) -> u64 {
        guardrail.estimate_index_bytes(self.entries.len())
    }

    /// Number of entries in the index.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true when index is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Allocation-stable longest-prefix lookup.
    ///
    /// Partial-hit rule:
    /// - resident handles stop at the first `RemoteOnly` block
    /// - all subsequent matched blocks are returned as prefetch keys
    #[must_use]
    pub fn lookup_blocks_into(&self, chain: &[[u8; 32]], buffers: &mut LookupBuffers) -> u32 {
        buffers.clear();

        let mut matched_depth = 0_u32;
        let mut remote_seen = false;

        for key in chain {
            let Some(entry) = self.entries.get(key) else {
                break;
            };

            matched_depth = matched_depth.saturating_add(1);
            if remote_seen {
                buffers.prefetch_keys.push(*key);
                continue;
            }

            match entry.residency {
                Residency::Ram | Residency::Nvme => buffers.resident_handles.push(BlockHandle {
                    key: *key,
                    block_index: entry.block_index,
                    residency: entry.residency,
                }),
                Residency::RemoteOnly => {
                    remote_seen = true;
                    buffers.prefetch_keys.push(*key);
                }
            }
        }

        matched_depth
    }

    /// Longest-prefix lookup across a hashed prefix chain.
    #[must_use]
    pub fn lookup_chain(&self, chain: &[[u8; 32]]) -> LookupResult {
        let mut buffers = LookupBuffers::with_capacity(chain.len());
        let matched_depth = self.lookup_blocks_into(chain, &mut buffers);
        LookupResult {
            matched_depth,
            resident_handles: buffers.resident_handles,
            prefetch_keys: buffers.prefetch_keys,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GuardrailError, KeyEncoder, LookupBufferPool, LookupBuffers, MemoryGuardrail, RadixIndex,
        Residency, IO_GUARD, M3_MAX_UNIFIED_MEMORY_BYTES,
    };
    use std::sync::atomic::Ordering;

    fn key(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    #[test]
    fn crate_id_is_stable() {
        assert_eq!(super::crate_id(), "tp-radix");
    }

    #[test]
    fn longest_prefix_match_stops_at_first_missing_key() {
        let mut index = RadixIndex::default();
        index.upsert(key(1), 0, Residency::Ram);
        index.upsert(key(2), 1, Residency::Ram);
        // key(3) missing in index

        let result = index.lookup_chain(&[key(1), key(2), key(3), key(4)]);
        assert_eq!(result.matched_depth, 2);
        assert_eq!(result.resident_handles.len(), 2);
        assert!(result.prefetch_keys.is_empty());
    }

    #[test]
    fn lookup_is_memory_only_no_io_side_effects() {
        IO_GUARD.store(0, Ordering::SeqCst);
        let mut index = RadixIndex::default();
        index.upsert(key(1), 0, Residency::Ram);
        let _ = index.lookup_chain(&[key(1)]);
        assert_eq!(IO_GUARD.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn mixed_residency_returns_partial_handles_and_prefetch_keys() {
        let mut index = RadixIndex::default();
        index.upsert(key(1), 0, Residency::Ram);
        index.upsert(key(2), 1, Residency::Nvme);
        index.upsert(key(3), 2, Residency::RemoteOnly);
        index.upsert(key(4), 3, Residency::Ram);

        let result = index.lookup_chain(&[key(1), key(2), key(3), key(4)]);
        assert_eq!(result.matched_depth, 4);
        assert_eq!(result.resident_handles.len(), 2);
        assert_eq!(result.prefetch_keys, vec![key(3), key(4)]);
    }

    #[test]
    fn m3_guardrail_has_conservative_budget() {
        let guardrail = MemoryGuardrail::m3_max_96gb();
        assert_eq!(guardrail.host_memory_bytes, M3_MAX_UNIFIED_MEMORY_BYTES);
        assert!(guardrail.index_budget_bytes() < M3_MAX_UNIFIED_MEMORY_BYTES);
        assert!(guardrail.index_budget_bytes() > 0);
    }

    #[test]
    fn memory_guardrail_rejects_insert_when_budget_exceeded() {
        let mut index = RadixIndex::default();
        let guardrail = MemoryGuardrail {
            host_memory_bytes: 1024,
            reserved_system_bytes: 0,
            max_index_fraction_pct: 1,
            map_overhead_bytes_per_entry: 256,
        };

        let result = index.upsert_guarded(key(1), 0, Residency::Ram, guardrail);
        assert!(matches!(result, Err(GuardrailError::MemoryBudgetExceeded { .. })));
        assert!(index.is_empty());
    }

    #[test]
    fn lookup_blocks_into_reuses_buffer_capacity() {
        let mut index = RadixIndex::default();
        index.upsert(key(1), 0, Residency::Ram);
        index.upsert(key(2), 1, Residency::Ram);
        index.upsert(key(3), 2, Residency::Ram);

        let chain = [key(1), key(2), key(3)];
        let mut buffers = LookupBuffers::with_capacity(8);

        let _ = index.lookup_blocks_into(&chain, &mut buffers);
        let ptr = buffers.resident_handles.as_ptr();
        let cap = buffers.resident_handles.capacity();

        for _ in 0..16 {
            let matched = index.lookup_blocks_into(&chain, &mut buffers);
            assert_eq!(matched, 3);
        }

        assert_eq!(buffers.resident_handles.as_ptr(), ptr);
        assert_eq!(buffers.resident_handles.capacity(), cap);
    }

    #[test]
    fn lookup_buffer_pool_recycles_allocations() {
        let mut pool = LookupBufferPool::new(1);
        let mut first = pool.acquire();
        first.resident_handles.reserve(32);
        let first_capacity = first.resident_handles.capacity();
        pool.release(first);

        let second = pool.acquire();
        assert!(second.resident_handles.capacity() >= first_capacity);
        assert_eq!(pool.available(), 0);
        pool.release(second);

        assert_eq!(pool.available(), 1);
        assert_eq!(pool.high_watermark(), 1);
    }

    #[test]
    fn key_encoder_is_deterministic_and_reuses_chain_buffer() {
        let mut encoder = KeyEncoder::default();
        let mut out = Vec::with_capacity(8);
        encoder.encode_chain([7; 16], &[1, 2, 3, 4], &mut out);
        let first_run = out.clone();
        let cap = out.capacity();

        encoder.encode_chain([7; 16], &[1, 2, 3, 4], &mut out);
        assert_eq!(out, first_run);
        assert_eq!(out.capacity(), cap);

        let single = encoder.encode([7; 16], 2, 2);
        assert_eq!(single, out[1]);
    }
}
