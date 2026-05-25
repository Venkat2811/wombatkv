#![forbid(unsafe_code)]
//! Live `MinIO` integration test for the embeddable wombatkv KV store.
//!
//! Gated by `WMBT_KV_TEST_EMBED_LIVE=1` so unit-test runs stay deterministic.
//! When enabled, requires `MinIO` running at `WMBT_KV_S3_ENDPOINT`
//! (default `http://127.0.0.1:9000`). See `docker/minio-compose.yml`.
//!
//! What this proves end-to-end:
//!   1. `put_kv` writes through both foyer and `MinIO`.
//!   2. After a foyer clear, `get_kv` falls back to `MinIO` and re-promotes.
//!   3. After dropping the entire store and recreating it (process-restart
//!      simulation), `restore_from_s3` rebuilds foyer state from `MinIO` so
//!      subsequent reads are warm hits.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use tempfile::tempdir;

use wombatkv_node::embed::{EmbedConfig, GetOutcome, HitTier, WombatKVKvStore};
use wombatkv_node::foyer_cache::FoyerCacheConfig;
use wombatkv_store::wal_store::{S3ObjectStore, S3ObjectStoreConfig};

fn live_enabled() -> bool {
    matches!(std::env::var("WMBT_KV_TEST_EMBED_LIVE"), Ok(value) if value == "1")
}

fn build_minio_store() -> S3ObjectStore {
    let cfg = S3ObjectStoreConfig::from_env().expect("S3 config from env");
    let store = S3ObjectStore::new(cfg).expect("S3ObjectStore::new");
    store.ensure_bucket().expect("ensure bucket");
    store
}

fn small_foyer(dir: PathBuf) -> FoyerCacheConfig {
    FoyerCacheConfig {
        ram_bytes: 16 * 1024 * 1024,
        ssd_dir: dir,
        ssd_bytes: 64 * 1024 * 1024,
        block_size: 4 * 1024 * 1024,
        buffer_pool_size: 8 * 1024 * 1024,
        iouring: false,
    }
}

fn embed_config(s3_prefix: &str, foyer_dir: PathBuf) -> EmbedConfig {
    EmbedConfig {
        s3_prefix: s3_prefix.to_string(),
        foyer: small_foyer(foyer_dir),
        write_through_s3: true,
        compression: wombatkv_node::compression::BlockCompressionConfig::default(),
    }
}

fn run_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    format!("test-run-{nanos:x}")
}

#[test]
fn put_get_round_trip_against_minio_live() {
    if !live_enabled() {
        eprintln!("WMBT_KV_TEST_EMBED_LIVE!=1, skipping live MinIO test");
        return;
    }

    let dir = tempdir().expect("tempdir");
    let prefix = format!("kv/{}", run_id());
    let store =
        WombatKVKvStore::new(embed_config(&prefix, dir.path().to_path_buf()), build_minio_store())
            .expect("build store");

    let payload = Bytes::from_static(b"qwen3-pd-payload-live-minio");
    store.put_kv("ns-a", "seq-1", payload.clone()).expect("put");

    match store.get_kv("ns-a", "seq-1").expect("get") {
        GetOutcome::Hit { tier, payload: got } => {
            assert!(matches!(tier, HitTier::Foyer));
            assert_eq!(got, payload);
        }
        GetOutcome::Miss => panic!("expected hit"),
    }
}

#[test]
fn s3_fallback_after_foyer_clear_against_minio_live() {
    if !live_enabled() {
        eprintln!("WMBT_KV_TEST_EMBED_LIVE!=1, skipping live MinIO test");
        return;
    }

    let dir = tempdir().expect("tempdir");
    let prefix = format!("kv/{}", run_id());
    let store =
        WombatKVKvStore::new(embed_config(&prefix, dir.path().to_path_buf()), build_minio_store())
            .expect("build store");

    let payload = Bytes::from_static(b"survives-process-fall-through");
    store.put_kv("ns-a", "seq-7", payload.clone()).expect("put");
    store.clear_foyer();

    match store.get_kv("ns-a", "seq-7").expect("get") {
        GetOutcome::Hit { tier, payload: got } => {
            assert!(matches!(tier, HitTier::ObjectStore));
            assert_eq!(got, payload);
        }
        GetOutcome::Miss => panic!("expected MinIO fallback hit"),
    }
}

#[test]
fn restart_rebuilds_foyer_from_minio_live() {
    if !live_enabled() {
        eprintln!("WMBT_KV_TEST_EMBED_LIVE!=1, skipping live MinIO test");
        return;
    }

    let dir = tempdir().expect("tempdir");
    let prefix = format!("kv/{}", run_id());

    // Process A: write 4 sequences, then "crash"
    {
        let store =
            WombatKVKvStore::new(embed_config(&prefix, dir.path().join("a")), build_minio_store())
                .expect("build store a");
        for idx in 0..4_u32 {
            let key = format!("seq-{idx}");
            store.put_kv("ns", &key, Bytes::from(vec![idx as u8; 4096])).expect("put");
        }
    }

    // Process B: fresh foyer dir, fresh MinIO connection, restore from S3.
    let store_b: Arc<WombatKVKvStore<S3ObjectStore>> = Arc::new(
        WombatKVKvStore::new(embed_config(&prefix, dir.path().join("b")), build_minio_store())
            .expect("build store b"),
    );

    let restored = store_b.restore_from_s3("ns").expect("restore");
    assert_eq!(restored, 4, "expected to restore 4 keys, got {restored}");

    for idx in 0..4_u32 {
        let key = format!("seq-{idx}");
        match store_b.get_kv("ns", &key).expect("get") {
            GetOutcome::Hit { tier, payload } => {
                assert!(matches!(tier, HitTier::Foyer));
                assert_eq!(payload.as_ref(), vec![idx as u8; 4096].as_slice());
            }
            GetOutcome::Miss => panic!("expected foyer hit after restore for {key}"),
        }
    }
}
