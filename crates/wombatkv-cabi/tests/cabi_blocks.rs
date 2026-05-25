//! Tests for the block-shaped C ABI symbols (ABI 1.5).
//!
//! `wmbt_kv_lookup_block_prefix` is covered here without needing a live
//! S3, we construct an `S3ObjectStore` with fake-but-syntactically-valid
//! config (the constructor does not call S3) and stock the metadata
//! index directly. The PUT/GET round-trip is exercised by
//! `cabi_round_trip.rs` against `MinIO`, gated on `WMBT_KV_S3_*`.

#![allow(unsafe_code)]

use std::ffi::CString;
use std::path::PathBuf;
use std::sync::Arc;

use wombatkv::{wmbt_kv_handle, wmbt_kv_lookup_block_prefix, Handle, ABI_MAJOR, ABI_MINOR};
use wombatkv_node::embed::{EmbedConfig, WombatKVKvStore};
use wombatkv_node::foyer_cache::FoyerCacheConfig;
use wombatkv_radix::{BlockMeta, MetadataIndex};
use wombatkv_store::wal_store::{S3ObjectStore, S3ObjectStoreConfig};

/// Build a real `WombatKVKvStore<S3ObjectStore>` with credentials that
/// pass `S3ObjectStore::new`'s syntactic checks but never get used -
/// `lookup_block_prefix` only touches the in-memory metadata index,
/// not the object store.
fn build_store_offline() -> Arc<WombatKVKvStore<S3ObjectStore>> {
    let tmpdir =
        std::env::temp_dir().join(format!("wombatkv-cabi-blocks-test-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmpdir);

    let s3 = S3ObjectStore::new(S3ObjectStoreConfig {
        endpoint: "http://127.0.0.1:1".to_string(),
        bucket: "cabi-blocks-test".to_string(),
        access_key: "FAKE_KEY_FOR_OFFLINE_TEST".to_string(),
        secret_key: "FAKE_SECRET_FOR_OFFLINE_TEST".to_string(),
        region: "us-east-1".to_string(),
        max_retries: 1,
        retry_backoff_ms: 10,
    })
    .expect("S3ObjectStore::new (offline; no network call here)");

    let foyer = FoyerCacheConfig {
        ram_bytes: 1024 * 1024,
        ssd_dir: PathBuf::from(&tmpdir),
        ssd_bytes: 4 * 1024 * 1024,
        block_size: 64 * 1024,
        buffer_pool_size: 256 * 1024,
        iouring: false,
    };
    let cfg = EmbedConfig {
        s3_prefix: "cabi/blocks/test".to_string(),
        foyer,
        write_through_s3: false,
        compression: wombatkv_node::compression::BlockCompressionConfig::default(),
    };
    Arc::new(WombatKVKvStore::new(cfg, s3).expect("WombatKVKvStore::new (foyer init)"))
}

fn hex64(byte: u8) -> CString {
    let mut s = String::with_capacity(64);
    for _ in 0..32 {
        s.push_str(&format!("{byte:02x}"));
    }
    CString::new(s).expect("hex64 CString")
}

#[test]
fn lookup_block_prefix_counts_leading_hits() {
    let store = build_store_offline();
    let index = store.metadata_index();

    // Stock 3 hashes; leave a 4th out.
    let h0 = [0x10u8; 32];
    let h1 = [0x11u8; 32];
    let h2 = [0x12u8; 32];
    index.insert(h0, BlockMeta::new_root(1024, [0u8; 24], [0u8; 16]));
    index.insert(h1, BlockMeta::new_root(1024, [0u8; 24], [0u8; 16]));
    index.insert(h2, BlockMeta::new_root(1024, [0u8; 24], [0u8; 16]));

    let handle_box: Box<Handle> = Box::new(Handle::from_kvstore(store, "test-ns"));
    let handle_raw = Box::into_raw(handle_box).cast::<wmbt_kv_handle>();

    let ns = CString::new("test-ns").unwrap();

    // Hex strings for the 3 inserted + 1 missing.
    let h0_hex = hex64(0x10);
    let h1_hex = hex64(0x11);
    let h2_hex = hex64(0x12);
    let h3_hex = hex64(0x99); // not inserted
    let arr: [*const std::os::raw::c_char; 4] =
        [h0_hex.as_ptr(), h1_hex.as_ptr(), h2_hex.as_ptr(), h3_hex.as_ptr()];

    let mut matched: usize = 0;
    let mut err_buf = [0i8; 256];
    let rc = wmbt_kv_lookup_block_prefix(
        handle_raw,
        ns.as_ptr(),
        arr.as_ptr(),
        4,
        &raw mut matched,
        err_buf.as_mut_ptr(),
        err_buf.len(),
    );
    assert_eq!(rc, 0, "lookup rc={rc} matched={matched}");
    assert_eq!(matched, 3, "expected 3 leading hits, got {matched}");

    // All-in: full chain present.
    let arr_all: [*const std::os::raw::c_char; 3] =
        [h0_hex.as_ptr(), h1_hex.as_ptr(), h2_hex.as_ptr()];
    let mut matched_all: usize = 0;
    let rc_all = wmbt_kv_lookup_block_prefix(
        handle_raw,
        ns.as_ptr(),
        arr_all.as_ptr(),
        3,
        &raw mut matched_all,
        std::ptr::null_mut(),
        0,
    );
    assert_eq!(rc_all, 0);
    assert_eq!(matched_all, 3);

    // Leading miss: should report 0.
    let arr_miss: [*const std::os::raw::c_char; 2] = [h3_hex.as_ptr(), h0_hex.as_ptr()];
    let mut matched_miss: usize = 0;
    let rc_miss = wmbt_kv_lookup_block_prefix(
        handle_raw,
        ns.as_ptr(),
        arr_miss.as_ptr(),
        2,
        &raw mut matched_miss,
        std::ptr::null_mut(),
        0,
    );
    assert_eq!(rc_miss, 0);
    assert_eq!(matched_miss, 0);

    // Drop the Handle by reclaiming and freeing the Box.
    let _ = unsafe { Box::from_raw(handle_raw.cast::<Handle>()) };
}

#[test]
fn lookup_block_prefix_rejects_bad_hex() {
    let store = build_store_offline();
    let handle_box: Box<Handle> = Box::new(Handle::from_kvstore(store, "test-ns"));
    let handle_raw = Box::into_raw(handle_box).cast::<wmbt_kv_handle>();
    let ns = CString::new("test-ns").unwrap();

    // 63 chars: invalid length.
    let bad = CString::new("ab".repeat(31) + "a").unwrap();
    let arr: [*const std::os::raw::c_char; 1] = [bad.as_ptr()];

    let mut matched: usize = 0;
    let mut err_buf = [0i8; 256];
    let rc = wmbt_kv_lookup_block_prefix(
        handle_raw,
        ns.as_ptr(),
        arr.as_ptr(),
        1,
        &raw mut matched,
        err_buf.as_mut_ptr(),
        err_buf.len(),
    );
    assert_eq!(rc, -1, "expected error rc, got {rc}");
    // Error message should be populated.
    let msg = unsafe { std::ffi::CStr::from_ptr(err_buf.as_ptr()) }.to_string_lossy().into_owned();
    assert!(msg.contains("64 chars"), "msg=\"{msg}\"");

    let _ = unsafe { Box::from_raw(handle_raw.cast::<Handle>()) };
}

#[test]
fn abi_major_is_one_alpha_consolidated() {
    // Alpha consolidates the pre-alpha 1.1..1.6 history into one 1.0
    // surface (see `ABI_MAJOR` doc-comment). Block-chain surfaces are
    // part of that consolidated baseline, the symbols imported above
    // and the lookup_block_prefix tests below exercise them directly.
    assert_eq!(ABI_MAJOR, 1);
    // ABI_MINOR is allowed to bump in alpha for non-breaking additions
    // (e.g. new constructors like wmbt_kv_open_http), only assert it's
    // a valid u16, not a specific value.
    let _ = ABI_MINOR;
}
