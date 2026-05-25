#![forbid(unsafe_code)]
//! End-to-end roundtrip of a vllm.rs-shaped PD KV payload through the
//! wombatkv foyer cache.
//!
//! Mirrors the wire format from
//! `vllm.rs@feat/myelon-typed-end-to-end:src/transfer/mod.rs`:
//!
//! ```ignore
//! pub enum KVTransferHandle {
//!     LocalIpc { ... },
//!     RemoteTcp {
//!         layer_data: Vec<(Vec<u8>, Vec<u8>)>,  // per-layer (K, V)
//!         num_blocks: usize,
//!     },
//! }
//! pub struct FinishedPrefillData {
//!     pub seq_id: usize,
//!     pub first_token: u32,
//!     pub transfer_handle: KVTransferHandle,
//!     pub sending_time: usize,
//!     pub num_cached_tokens: usize,
//! }
//! ```
//!
//! `transfer/comm.rs` serializes outgoing `TransferMessage` values with
//! `bincode::serialize`. We use the same crate + same shape so the bytes
//! we put through foyer match what vllm.rs would produce.

use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tempfile::tempdir;

use wombatkv_node::foyer_cache::{FoyerCacheConfig, FoyerHybridCache};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum KVTransferHandle {
    RemoteTcp { layer_data: Vec<(Vec<u8>, Vec<u8>)>, num_blocks: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FinishedPrefillData {
    seq_id: usize,
    first_token: u32,
    transfer_handle: KVTransferHandle,
    sending_time: usize,
    num_cached_tokens: usize,
}

fn synthesize_pd_payload(
    seq_id: usize,
    num_layers: usize,
    bytes_per_layer: usize,
    num_blocks: usize,
) -> FinishedPrefillData {
    let layer_data = (0..num_layers)
        .map(|layer_idx| {
            let k_byte = (layer_idx as u8).wrapping_mul(7).wrapping_add(0x21);
            let v_byte = (layer_idx as u8).wrapping_mul(11).wrapping_add(0x42);
            (vec![k_byte; bytes_per_layer], vec![v_byte; bytes_per_layer])
        })
        .collect::<Vec<_>>();

    FinishedPrefillData {
        seq_id,
        first_token: 99,
        transfer_handle: KVTransferHandle::RemoteTcp { layer_data, num_blocks },
        sending_time: 1_700_000_000,
        num_cached_tokens: 0,
    }
}

fn small_config(dir: std::path::PathBuf) -> FoyerCacheConfig {
    FoyerCacheConfig {
        ram_bytes: 16 * 1024 * 1024,
        ssd_dir: dir,
        ssd_bytes: 64 * 1024 * 1024,
        block_size: 4 * 1024 * 1024,
        buffer_pool_size: 8 * 1024 * 1024,
        iouring: false,
    }
}

fn cache_key(seq_id: usize) -> String {
    format!("vllm-rs/pd/seq-{seq_id:08x}")
}

#[test]
fn pd_kv_payload_roundtrips_through_foyer_with_byte_fidelity() {
    let dir = tempdir().expect("tempdir");
    let cache: Arc<FoyerHybridCache> =
        FoyerHybridCache::open(small_config(dir.path().to_path_buf())).expect("open foyer cache");

    // Mirrors a Qwen3-8B-class payload at small ctx: 36 layers, 4 KiB per
    // (K, V) pair per layer, 8 blocks. Total ~ 36 * 2 * 4096 = 288 KiB which
    // is well below the 16 MiB RAM tier so we exercise the warm-hit path.
    let original = synthesize_pd_payload(0xCAFEBABE, 36, 4096, 8);
    let encoded = bincode::serialize(&original).expect("bincode serialize");
    let key = cache_key(original.seq_id);

    cache.put(&key, Bytes::from(encoded.clone()));
    assert_eq!(cache.inserts(), 1);

    let fetched = cache.get(&key).expect("warm hit");
    assert_eq!(fetched.as_ref(), encoded.as_slice());
    assert_eq!(cache.hits(), 1);

    let decoded: FinishedPrefillData = bincode::deserialize(&fetched).expect("bincode deserialize");
    assert_eq!(decoded, original);

    match decoded.transfer_handle {
        KVTransferHandle::RemoteTcp { ref layer_data, num_blocks } => {
            assert_eq!(num_blocks, 8);
            assert_eq!(layer_data.len(), 36);
            assert_eq!(layer_data[0].0.len(), 4096);
            assert_eq!(layer_data[0].1.len(), 4096);
        }
    }
}

#[test]
fn many_concurrent_pd_payloads_keyed_by_seq_id_remain_isolated() {
    let dir = tempdir().expect("tempdir");
    let cache: Arc<FoyerHybridCache> =
        FoyerHybridCache::open(small_config(dir.path().to_path_buf())).expect("open foyer cache");

    let num_seqs = 16_usize;
    let originals: Vec<FinishedPrefillData> =
        (0..num_seqs).map(|seq| synthesize_pd_payload(seq, 8, 1024, 4)).collect();

    for payload in &originals {
        let bytes = bincode::serialize(payload).expect("serialize");
        cache.put(&cache_key(payload.seq_id), Bytes::from(bytes));
    }

    for payload in &originals {
        let fetched = cache.get(&cache_key(payload.seq_id)).expect("hit");
        let decoded: FinishedPrefillData = bincode::deserialize(&fetched).expect("deserialize");
        assert_eq!(decoded, *payload);
    }

    assert!(cache.hits() >= num_seqs as u64);
    assert!(cache.misses() == 0);

    // Negative path: a key we never wrote must miss without affecting the
    // payloads we already stored.
    let unknown = cache.get("vllm-rs/pd/seq-deadbeef");
    assert!(unknown.is_none());
    assert_eq!(cache.misses(), 1);
}

#[test]
fn pd_payload_survives_clear_then_reinsert() {
    let dir = tempdir().expect("tempdir");
    let cache: Arc<FoyerHybridCache> =
        FoyerHybridCache::open(small_config(dir.path().to_path_buf())).expect("open foyer cache");

    let payload = synthesize_pd_payload(0x42, 4, 256, 2);
    let key = cache_key(payload.seq_id);
    let bytes = bincode::serialize(&payload).expect("serialize");

    cache.put(&key, Bytes::from(bytes.clone()));
    assert!(cache.get(&key).is_some());

    cache.clear();
    assert!(cache.get(&key).is_none());

    cache.put(&key, Bytes::from(bytes.clone()));
    let fetched = cache.get(&key).expect("hit after re-insert");
    let decoded: FinishedPrefillData = bincode::deserialize(&fetched).expect("deserialize");
    assert_eq!(decoded, payload);
}
