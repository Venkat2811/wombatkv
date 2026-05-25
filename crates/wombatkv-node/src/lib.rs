#![forbid(unsafe_code)]

pub mod block_prefetch;
pub mod compression;
pub mod embed;
pub mod embed_metrics;
pub mod foyer_cache;
pub mod kv_blob_cache;
pub mod latency_histogram;
pub mod lru;

/// Returns crate identity for smoke tests.
#[must_use]
pub fn crate_id() -> &'static str {
    "wombatkv-node"
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_id_is_stable() {
        assert_eq!(super::crate_id(), "wombatkv-node");
    }
}
