//! Tests for the raw-tail sidecar C ABI (ABI 1.6, RFC 0007 §10.P5).
//!
//! Exercises `wmbt_kv_put_raw_tail` + `wmbt_kv_get_raw_tail_borrowed`
//! against a real foyer-backed `WombatKVKvStore` with `write_through_s3
//! = false` so no S3 traffic is required. The PUT/GET path is purely
//! local, these tests run unconditionally in CI without `MinIO`.

#![allow(unsafe_code)]

use std::ffi::CString;
use std::path::PathBuf;
use std::sync::Arc;

use wombatkv::{
    wmbt_kv_borrow, wmbt_kv_get_raw_tail_borrowed, wmbt_kv_handle, wmbt_kv_put_raw_tail,
    wmbt_kv_release_borrow, Handle, ABI_MAJOR, ABI_MINOR,
};
use wombatkv_node::embed::{EmbedConfig, WombatKVKvStore};
use wombatkv_node::foyer_cache::FoyerCacheConfig;
use wombatkv_store::wal_store::{S3ObjectStore, S3ObjectStoreConfig};

/// Build a real `WombatKVKvStore<S3ObjectStore>` with credentials that
/// pass syntactic checks but never get used: `write_through_s3=false`
/// means PUTs swallow any S3 errors and only commit to foyer locally.
fn build_store_offline(tag: &str) -> Arc<WombatKVKvStore<S3ObjectStore>> {
    let tmpdir = std::env::temp_dir().join(format!(
        "wombatkv-cabi-sidecar-test-{}-{}",
        tag,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmpdir);
    std::fs::create_dir_all(&tmpdir).expect("create tmpdir");

    let s3 = S3ObjectStore::new(S3ObjectStoreConfig {
        endpoint: "http://127.0.0.1:1".to_string(),
        bucket: "cabi-sidecar-test".to_string(),
        access_key: "FAKE_KEY_FOR_OFFLINE_TEST".to_string(),
        secret_key: "FAKE_SECRET_FOR_OFFLINE_TEST".to_string(),
        region: "us-east-1".to_string(),
        max_retries: 1,
        retry_backoff_ms: 10,
    })
    .expect("S3ObjectStore::new (offline)");

    let foyer = FoyerCacheConfig {
        ram_bytes: 64 * 1024 * 1024,
        ssd_dir: PathBuf::from(&tmpdir),
        ssd_bytes: 256 * 1024 * 1024,
        block_size: 64 * 1024,
        buffer_pool_size: 1024 * 1024,
        iouring: false,
    };
    let cfg = EmbedConfig {
        s3_prefix: format!("cabi/sidecar/test/{tag}"),
        foyer,
        write_through_s3: false,
        compression: Default::default(),
    };
    Arc::new(WombatKVKvStore::new(cfg, s3).expect("WombatKVKvStore::new"))
}

/// Return a 64-char lower-hex string by repeating the given byte.
fn hex64(byte: u8) -> CString {
    let mut s = String::with_capacity(64);
    for _ in 0..32 {
        s.push_str(&format!("{byte:02x}"));
    }
    CString::new(s).expect("hex64 CString")
}

#[test]
fn abi_major_is_one_alpha_consolidated() {
    // Alpha consolidates the pre-alpha 1.1..1.6 history into one 1.0
    // surface. Raw-tail sidecar symbols are part of that consolidated
    // baseline, exercised directly by the roundtrip test below.
    assert_eq!(ABI_MAJOR, 1);
    let _ = ABI_MINOR;
}

#[test]
fn raw_tail_put_then_get_roundtrips_bytes() {
    let store = build_store_offline("roundtrip");
    let handle_box: Box<Handle> = Box::new(Handle::from_kvstore(store, "test-ns"));
    let handle_raw = Box::into_raw(handle_box).cast::<wmbt_kv_handle>();

    let ns = CString::new("test-ns").unwrap();
    let chain_tip = hex64(0xAB);

    // Build a payload that mimics the DSV4 raw-tail shape (small for the
    // test, but uses a distinctive bit pattern so we can verify exact
    // bytes round-trip).
    let payload: Vec<u8> = (0..8192).map(|i| (i % 251) as u8).collect();

    // PUT.
    let mut err_buf = [0i8; 256];
    let rc_put = wmbt_kv_put_raw_tail(
        handle_raw,
        ns.as_ptr(),
        chain_tip.as_ptr(),
        payload.as_ptr(),
        payload.len(),
        err_buf.as_mut_ptr(),
        err_buf.len(),
    );
    assert_eq!(rc_put, 0, "put rc={rc_put}");

    // GET borrowed.
    let mut out_ptr: *const u8 = std::ptr::null();
    let mut out_len: usize = 0;
    let mut borrow: *mut wmbt_kv_borrow = std::ptr::null_mut();
    err_buf[0] = 0;
    let rc_get = wmbt_kv_get_raw_tail_borrowed(
        handle_raw,
        ns.as_ptr(),
        chain_tip.as_ptr(),
        &raw mut out_ptr,
        &raw mut out_len,
        &raw mut borrow,
        err_buf.as_mut_ptr(),
        err_buf.len(),
    );
    assert_eq!(rc_get, 1, "get rc={rc_get}");
    assert!(!out_ptr.is_null());
    assert_eq!(out_len, payload.len());

    // Verify byte-identical round-trip.
    let got = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
    assert_eq!(got, payload.as_slice());

    wmbt_kv_release_borrow(borrow);
    let _ = unsafe { Box::from_raw(handle_raw.cast::<Handle>()) };
}

#[test]
fn raw_tail_get_for_unknown_chain_tip_is_not_a_hit() {
    // Offline-only assertion: the FFI must NOT return rc=1 (hit) for a
    // chain-tip that was never PUT. In offline mode (no real S3 backing
    // the bucket endpoint) the underlying S3 GET fails with a network
    // error, so the FFI surfaces -1 rather than the clean 0 (miss) that
    // a real backing store would produce. We accept either; the
    // important invariant is "not a phantom hit". The on-MinIO
    // miss=0 path is exercised in `cabi_round_trip.rs::
    // cabi_round_trip_against_minio` (gated on WMBT_KV_S3_*).
    let store = build_store_offline("miss");
    let handle_box: Box<Handle> = Box::new(Handle::from_kvstore(store, "test-ns"));
    let handle_raw = Box::into_raw(handle_box).cast::<wmbt_kv_handle>();

    let ns = CString::new("test-ns").unwrap();
    let chain_tip_absent = hex64(0xCC);

    let mut out_ptr: *const u8 = std::ptr::null();
    let mut out_len: usize = 0;
    let mut borrow: *mut wmbt_kv_borrow = std::ptr::null_mut();
    let mut err_buf = [0i8; 256];
    let rc_get = wmbt_kv_get_raw_tail_borrowed(
        handle_raw,
        ns.as_ptr(),
        chain_tip_absent.as_ptr(),
        &raw mut out_ptr,
        &raw mut out_len,
        &raw mut borrow,
        err_buf.as_mut_ptr(),
        err_buf.len(),
    );
    assert!(rc_get == 0 || rc_get == -1, "expected miss=0 or net-err=-1, got rc={rc_get}");
    assert!(out_ptr.is_null());
    assert_eq!(out_len, 0);

    let _ = unsafe { Box::from_raw(handle_raw.cast::<Handle>()) };
}

#[test]
fn raw_tail_rejects_bad_hex_length() {
    let store = build_store_offline("badhex");
    let handle_box: Box<Handle> = Box::new(Handle::from_kvstore(store, "test-ns"));
    let handle_raw = Box::into_raw(handle_box).cast::<wmbt_kv_handle>();

    let ns = CString::new("test-ns").unwrap();
    let too_short = CString::new("ab".repeat(31) + "a").unwrap(); // 63 chars
    let payload = [0u8; 16];

    let mut err_buf = [0i8; 256];
    let rc = wmbt_kv_put_raw_tail(
        handle_raw,
        ns.as_ptr(),
        too_short.as_ptr(),
        payload.as_ptr(),
        payload.len(),
        err_buf.as_mut_ptr(),
        err_buf.len(),
    );
    assert_eq!(rc, -1);
    let msg = unsafe { std::ffi::CStr::from_ptr(err_buf.as_ptr()) }.to_string_lossy().into_owned();
    assert!(msg.contains("64 chars"), "msg=\"{msg}\"");

    let _ = unsafe { Box::from_raw(handle_raw.cast::<Handle>()) };
}

#[test]
fn raw_tail_large_payload_roundtrips() {
    // Approximate DSV4 raw-tail size (11.3 MB), verifies the FFI handles
    // the realistic payload size without overflow or truncation.
    let store = build_store_offline("large");
    let handle_box: Box<Handle> = Box::new(Handle::from_kvstore(store, "test-ns"));
    let handle_raw = Box::into_raw(handle_box).cast::<wmbt_kv_handle>();

    let ns = CString::new("test-ns").unwrap();
    let chain_tip = hex64(0xEF);
    let size: usize = 11_272_220; // exact DSV4 predicted raw-tail size
    let mut payload = vec![0u8; size];
    // Fill with a pattern we can verify cheaply at both ends.
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }

    let mut err_buf = [0i8; 256];
    let rc_put = wmbt_kv_put_raw_tail(
        handle_raw,
        ns.as_ptr(),
        chain_tip.as_ptr(),
        payload.as_ptr(),
        payload.len(),
        err_buf.as_mut_ptr(),
        err_buf.len(),
    );
    assert_eq!(rc_put, 0, "large put rc={rc_put}");

    let mut out_ptr: *const u8 = std::ptr::null();
    let mut out_len: usize = 0;
    let mut borrow: *mut wmbt_kv_borrow = std::ptr::null_mut();
    err_buf[0] = 0;
    let rc_get = wmbt_kv_get_raw_tail_borrowed(
        handle_raw,
        ns.as_ptr(),
        chain_tip.as_ptr(),
        &raw mut out_ptr,
        &raw mut out_len,
        &raw mut borrow,
        err_buf.as_mut_ptr(),
        err_buf.len(),
    );
    assert_eq!(rc_get, 1, "large get rc={rc_get}");
    assert_eq!(out_len, size);

    // Sample a few positions rather than memcmp the whole thing, keeps
    // the test fast while still catching alignment/length bugs.
    let got = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
    assert_eq!(got[0], payload[0]);
    assert_eq!(got[size / 2], payload[size / 2]);
    assert_eq!(got[size - 1], payload[size - 1]);

    wmbt_kv_release_borrow(borrow);
    let _ = unsafe { Box::from_raw(handle_raw.cast::<Handle>()) };
}
