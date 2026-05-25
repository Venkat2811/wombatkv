#![forbid(unsafe_code)]

use std::cmp::Ordering;

pub mod metadata;
pub mod reuse;

/// Returns crate identity for smoke tests.
#[must_use]
pub fn crate_id() -> &'static str {
    "wombatkv-core"
}

/// Write id used as deterministic tie-breaker within a timestamp.
pub type WriteId = [u8; 16];

/// Minimal fact identity used by conflict-resolution contracts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FactRef {
    /// Millisecond unix epoch associated with the publish.
    pub epoch_ms: u64,
    /// `UUIDv7` bytes (or equivalent monotonic id) used to break epoch ties.
    pub write_id: WriteId,
}

impl FactRef {
    /// Stable total order key for manifest winner selection.
    #[must_use]
    pub const fn ordering_key(&self) -> (u64, WriteId) {
        (self.epoch_ms, self.write_id)
    }
}

/// Deterministic winner selection for same `(tenant, F, Hi)` facts.
#[must_use]
pub fn pick_winner(facts: &[FactRef]) -> Option<&FactRef> {
    facts.iter().min_by(|lhs, rhs| compare_facts(lhs, rhs))
}

/// Compare two facts using canonical order (lowest epoch, then lowest `write_id`).
#[must_use]
pub fn compare_facts(lhs: &FactRef, rhs: &FactRef) -> Ordering {
    lhs.ordering_key().cmp(&rhs.ordering_key())
}

/// Canonical WAL object key shape for globally unique chunk names.
#[must_use]
pub fn wal_chunk_key(
    tenant: &str,
    fingerprint: &str,
    epoch_ms: u64,
    node_id: &str,
    seq: u64,
) -> String {
    format!("wal/{tenant}/{fingerprint}/{epoch_ms}/chunk-{node_id}-{seq}.bin")
}

/// Counts the deepest contiguous resident prefix.
#[must_use]
pub fn contiguous_resident_prefix(residency: &[bool]) -> usize {
    residency.iter().take_while(|is_resident| **is_resident).count()
}

/// Hash function options for chained prefix hashing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HashFunction {
    Blake3,
    XxHash3,
}

/// Wire format options for adapter transport payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireFormat {
    MsgPack,
    FlatBuffers,
    Capnp,
}

/// Frozen embedded-mode defaults used until superseded by a versioned config RFC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct M0Defaults {
    pub block_size_tokens: u16,
    pub hash_function: HashFunction,
    pub wal_chunk_target_mb: u16,
    pub wire_format: WireFormat,
}

impl Default for M0Defaults {
    fn default() -> Self {
        Self {
            block_size_tokens: 128,
            hash_function: HashFunction::Blake3,
            wal_chunk_target_mb: 32,
            wire_format: WireFormat::MsgPack,
        }
    }
}

impl M0Defaults {
    /// Validate embedded-mode defaults and overrides against frozen bounds.
    pub fn validate(self) -> Result<(), &'static str> {
        const VALID_BLOCK_SIZES: [u16; 3] = [64, 128, 256];
        const VALID_WAL_CHUNK_MB: [u16; 3] = [8, 32, 64];

        if !VALID_BLOCK_SIZES.contains(&self.block_size_tokens) {
            return Err("block_size_tokens must be one of 64, 128, 256");
        }

        if !VALID_WAL_CHUNK_MB.contains(&self.wal_chunk_target_mb) {
            return Err("wal_chunk_target_mb must be one of 8, 32, 64");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{compare_facts, contiguous_resident_prefix, pick_winner, wal_chunk_key, FactRef};

    #[test]
    fn crate_id_is_stable() {
        assert_eq!(super::crate_id(), "wombatkv-core");
    }

    #[test]
    fn epoch_and_write_id_define_total_order() {
        let earlier = FactRef { epoch_ms: 1_700_000_000_001, write_id: [2; 16] };
        let later = FactRef { epoch_ms: 1_700_000_000_002, write_id: [1; 16] };
        let same_epoch_lower_write = FactRef { epoch_ms: 1_700_000_000_001, write_id: [1; 16] };

        assert!(compare_facts(&earlier, &later).is_lt());
        assert!(compare_facts(&same_epoch_lower_write, &earlier).is_lt());
    }

    #[test]
    fn winner_is_stable_independent_of_arrival_order() {
        let winner = FactRef { epoch_ms: 1_700_000_000_100, write_id: [1; 16] };
        let loser = FactRef { epoch_ms: 1_700_000_000_100, write_id: [9; 16] };

        let first_order = vec![loser.clone(), winner.clone()];
        let second_order = vec![winner.clone(), loser];

        assert_eq!(pick_winner(&first_order), Some(&winner));
        assert_eq!(pick_winner(&second_order), Some(&winner));
    }

    #[test]
    fn wal_keys_are_unique_across_nodes_for_same_epoch_and_seq() {
        let key_a = wal_chunk_key("t1", "f1", 1000, "node-a", 7);
        let key_b = wal_chunk_key("t1", "f1", 1000, "node-b", 7);
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn contiguous_prefix_stops_at_first_missing_block() {
        let residency = [true, true, false, true];
        assert_eq!(contiguous_resident_prefix(&residency), 2);
    }

    #[test]
    fn m0_defaults_are_stable_and_valid() {
        let defaults = super::M0Defaults::default();
        assert_eq!(defaults.block_size_tokens, 128);
        assert_eq!(defaults.hash_function, super::HashFunction::Blake3);
        assert_eq!(defaults.wal_chunk_target_mb, 32);
        assert_eq!(defaults.wire_format, super::WireFormat::MsgPack);
        assert_eq!(defaults.validate(), Ok(()));
    }

    #[test]
    fn invalid_block_size_is_rejected() {
        let invalid = super::M0Defaults { block_size_tokens: 96, ..super::M0Defaults::default() };
        assert_eq!(invalid.validate(), Err("block_size_tokens must be one of 64, 128, 256"));
    }

    #[test]
    fn invalid_wal_chunk_size_is_rejected() {
        let invalid = super::M0Defaults { wal_chunk_target_mb: 16, ..super::M0Defaults::default() };
        assert_eq!(invalid.validate(), Err("wal_chunk_target_mb must be one of 8, 32, 64"));
    }
}
