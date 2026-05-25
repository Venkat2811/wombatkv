#![forbid(unsafe_code)]

use std::collections::BTreeMap;

/// Deterministic fingerprint key for strict cache compatibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fingerprint(pub [u8; 32]);

/// Logical key for an individual cached block.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockKey {
    pub tenant_id: String,
    pub fingerprint: Fingerprint,
    pub prefix_hash: [u8; 32],
    pub block_index: u32,
}

/// Result of lookup on the in-memory store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LookupResult {
    pub matched_blocks: usize,
    pub payloads: Vec<Vec<u8>>,
    pub missing_keys: Vec<BlockKey>,
}

/// Minimal in-memory KV reuse store for the embedded-mode vertical slice.
#[derive(Default)]
pub struct InMemoryKvStore {
    blocks: BTreeMap<BlockKey, Vec<u8>>,
}

impl InMemoryKvStore {
    /// Store a block payload for a specific block key.
    pub fn put_block(&mut self, key: BlockKey, payload: Vec<u8>) {
        self.blocks.insert(key, payload);
    }

    /// Publish an ordered prefix chain in one call.
    pub fn publish_prefix(
        &mut self,
        tenant_id: &str,
        fingerprint: Fingerprint,
        token_blocks: &[Vec<u32>],
        payloads: &[Vec<u8>],
    ) -> Result<(), &'static str> {
        if token_blocks.len() != payloads.len() {
            return Err("token_blocks and payloads length mismatch");
        }

        let hashes = chained_prefix_hashes(tenant_id, fingerprint, token_blocks);
        for (index, (prefix_hash, payload)) in hashes.iter().zip(payloads.iter()).enumerate() {
            let block_index = u32::try_from(index).map_err(|_| "block index overflow")?;
            self.put_block(
                BlockKey {
                    tenant_id: tenant_id.to_string(),
                    fingerprint,
                    prefix_hash: *prefix_hash,
                    block_index,
                },
                payload.clone(),
            );
        }
        Ok(())
    }

    /// Lookup the deepest contiguous prefix and return cached payloads.
    #[must_use]
    pub fn lookup_prefix(
        &self,
        tenant_id: &str,
        fingerprint: Fingerprint,
        token_blocks: &[Vec<u32>],
    ) -> LookupResult {
        let hashes = chained_prefix_hashes(tenant_id, fingerprint, token_blocks);
        let mut payloads = Vec::new();
        let mut missing_keys = Vec::new();
        let mut matched_blocks = 0_usize;

        for (index, prefix_hash) in hashes.iter().enumerate() {
            let block_index = u32::try_from(index).expect("index bound by addressable memory");
            let key = BlockKey {
                tenant_id: tenant_id.to_string(),
                fingerprint,
                prefix_hash: *prefix_hash,
                block_index,
            };

            if missing_keys.is_empty() {
                if let Some(payload) = self.blocks.get(&key) {
                    payloads.push(payload.clone());
                    matched_blocks += 1;
                } else {
                    missing_keys.push(key);
                }
            } else {
                missing_keys.push(key);
            }
        }

        LookupResult { matched_blocks, payloads, missing_keys }
    }
}

/// Deterministic chained 32-byte hashes for token blocks.
#[must_use]
pub fn chained_prefix_hashes(
    tenant_id: &str,
    fingerprint: Fingerprint,
    token_blocks: &[Vec<u32>],
) -> Vec<[u8; 32]> {
    let mut current = seed_hash(tenant_id.as_bytes(), &fingerprint.0);
    let mut out = Vec::with_capacity(token_blocks.len());
    for block in token_blocks {
        let mut bytes = Vec::with_capacity(block.len() * 4);
        for token in block {
            bytes.extend_from_slice(&token.to_le_bytes());
        }
        current = mix_hash(&current, &bytes);
        out.push(current);
    }
    out
}

fn seed_hash(tenant: &[u8], fingerprint: &[u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"tp-seed-v1");
    hasher.update(tenant);
    hasher.update(fingerprint);
    *hasher.finalize().as_bytes()
}

fn mix_hash(prev: &[u8; 32], data: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"tp-chain-v1");
    hasher.update(prev);
    hasher.update(&(u64::try_from(data.len()).unwrap_or(u64::MAX)).to_le_bytes());
    hasher.update(data);
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::{chained_prefix_hashes, Fingerprint, InMemoryKvStore};

    fn fixture_fingerprint(tag: u8) -> Fingerprint {
        let mut bytes = [0_u8; 32];
        bytes.fill(tag);
        Fingerprint(bytes)
    }

    fn fixture_blocks() -> Vec<Vec<u32>> {
        vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8]]
    }

    #[test]
    fn strict_fingerprint_equality_is_required_for_hits() {
        let mut store = InMemoryKvStore::default();
        let blocks = fixture_blocks();
        let payloads = vec![b"a".to_vec(), b"b".to_vec()];
        store
            .publish_prefix("tenant-a", fixture_fingerprint(1), &blocks, &payloads)
            .expect("publish");

        let same = store.lookup_prefix("tenant-a", fixture_fingerprint(1), &blocks);
        let different = store.lookup_prefix("tenant-a", fixture_fingerprint(2), &blocks);
        assert_eq!(same.matched_blocks, 2);
        assert_eq!(different.matched_blocks, 0);
    }

    #[test]
    fn chained_hashes_are_deterministic_for_same_input() {
        let blocks = fixture_blocks();
        let first = chained_prefix_hashes("tenant-a", fixture_fingerprint(9), &blocks);
        let second = chained_prefix_hashes("tenant-a", fixture_fingerprint(9), &blocks);
        assert_eq!(first, second);
    }

    #[test]
    fn chained_hashes_match_blake3_reference_mixing() {
        let blocks = fixture_blocks();
        let fingerprint = fixture_fingerprint(7);
        let hashes = chained_prefix_hashes("tenant-a", fingerprint, &blocks);

        let mut seed = blake3::Hasher::new();
        seed.update(b"tp-seed-v1");
        seed.update(b"tenant-a");
        seed.update(&fingerprint.0);
        let mut prev = *seed.finalize().as_bytes();

        let mut expected = Vec::with_capacity(blocks.len());
        for block in &blocks {
            let mut bytes = Vec::with_capacity(block.len() * 4);
            for token in block {
                bytes.extend_from_slice(&token.to_le_bytes());
            }

            let mut mixer = blake3::Hasher::new();
            mixer.update(b"tp-chain-v1");
            mixer.update(&prev);
            mixer.update(&(u64::try_from(bytes.len()).unwrap_or(u64::MAX)).to_le_bytes());
            mixer.update(&bytes);
            prev = *mixer.finalize().as_bytes();
            expected.push(prev);
        }

        assert_eq!(hashes, expected);
    }

    #[test]
    fn second_request_reuses_published_prefix_blocks() {
        let mut store = InMemoryKvStore::default();
        let blocks = fixture_blocks();
        let payloads = vec![b"kv-1".to_vec(), b"kv-2".to_vec()];

        store
            .publish_prefix("tenant-a", fixture_fingerprint(3), &blocks, &payloads)
            .expect("publish");
        let lookup = store.lookup_prefix("tenant-a", fixture_fingerprint(3), &blocks);

        assert_eq!(lookup.matched_blocks, 2);
        assert_eq!(lookup.payloads, payloads);
        assert!(lookup.missing_keys.is_empty());
    }
}
