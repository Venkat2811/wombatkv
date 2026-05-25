//! Adversarial byte-roundtrip test for the block-prefix C ABI.
//!
//! Tier A of the tensor-level fidelity test plan documented in
//! ds4/docs/MODE_VALIDATION.md. Goal: prove WombatKV itself does
//! NOT corrupt bytes on the put → get path, independent of any
//! engine integration.
//!
//! Tests the in-process round-trip via the C ABI surface
//! (`wmbt_kv_put_kv_blocks` + `wmbt_kv_get_kv_blocks_borrowed`)
//! across multiple block sizes, empty, single byte, small,
//! and realistic DSV4-block-sized payloads. Bytes in must equal
//! bytes out, exactly.
//!
//! Offline: no MinIO required. Foyer-only backing
//! (write_through_s3 = false). The S3ObjectStore is initialized
//! with placeholder creds and is never contacted.

#![allow(unsafe_code)]

use std::ffi::CString;
use std::sync::Arc;

use wombatkv::{
    wmbt_kv_borrow, wmbt_kv_get_kv_blocks_borrowed, wmbt_kv_handle, wmbt_kv_put_kv_blocks,
    wmbt_kv_release_borrow, Handle,
};
use wombatkv_node::embed::{EmbedConfig, WombatKVKvStore};
use wombatkv_node::foyer_cache::FoyerCacheConfig;
use wombatkv_store::wal_store::{S3ObjectStore, S3ObjectStoreConfig};

/// Build a foyer-only store (no S3 traffic) for offline byte-roundtrip
/// testing. SSD size is generous enough to hold a few realistic DSV4-
/// sized blocks (~2.8 MiB each).
fn build_offline_store(tag: &str) -> Arc<WombatKVKvStore<S3ObjectStore>> {
    let tmpdir =
        std::env::temp_dir().join(format!("wombatkv-cabi-adv-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&tmpdir);
    let _ = std::fs::create_dir_all(&tmpdir);

    let s3 = S3ObjectStore::new(S3ObjectStoreConfig {
        endpoint: "http://127.0.0.1:1".to_string(),
        bucket: "cabi-adversarial-test".to_string(),
        access_key: "FAKE_KEY_FOR_OFFLINE_TEST".to_string(),
        secret_key: "FAKE_SECRET_FOR_OFFLINE_TEST".to_string(),
        region: "us-east-1".to_string(),
        max_retries: 1,
        retry_backoff_ms: 10,
    })
    .expect("S3ObjectStore::new (offline)");

    let foyer = FoyerCacheConfig {
        ram_bytes: 64 * 1024 * 1024,
        ssd_dir: tmpdir,
        ssd_bytes: 256 * 1024 * 1024,
        block_size: 64 * 1024,
        buffer_pool_size: 4 * 1024 * 1024,
        iouring: false,
    };
    let cfg = EmbedConfig {
        s3_prefix: "cabi/adversarial".to_string(),
        foyer,
        write_through_s3: false,
        compression: wombatkv_node::compression::BlockCompressionConfig::default(),
    };
    Arc::new(WombatKVKvStore::new(cfg, s3).expect("WombatKVKvStore::new"))
}

/// Deterministic pseudo-random fill so the test is reproducible across
/// machines and reruns. Don't use rand crate, keep zero new deps.
fn fill_seeded(buf: &mut [u8], seed: u32) {
    let mut state: u32 = seed.wrapping_mul(0x9E37_79B9).wrapping_add(1);
    for b in buf.iter_mut() {
        // Linear congruential, good enough as a non-trivial byte pattern.
        state = state.wrapping_mul(1_103_515_245).wrapping_add(12345);
        *b = (state >> 16) as u8;
    }
}

/// Compute blake3-256 of payload bytes, format as 64-char lowercase hex.
fn block_hash_hex(payload: &[u8]) -> CString {
    let h = blake3::hash(payload);
    let mut s = String::with_capacity(64);
    for byte in h.as_bytes() {
        s.push_str(&format!("{byte:02x}"));
    }
    CString::new(s).expect("hex string CString")
}

#[test]
fn adversarial_blocks_byte_roundtrip_in_process() {
    let store = build_offline_store("inproc");
    let handle_box: Box<Handle> = Box::new(Handle::from_kvstore(store, "test-ns"));
    let handle_raw = Box::into_raw(handle_box).cast::<wmbt_kv_handle>();

    // Adversarial size set:
    //   0           empty
    //   1           single byte (edge)
    //   4 KiB       small
    //   64 KiB      medium (== foyer block size)
    //   2.8 MiB     realistic DSV4 layer block (43 layers × ...)
    //
    // Each filled with a DIFFERENT seed so we'd catch any payload index
    // swap inside the foyer cache.
    let sizes: &[usize] = &[0, 1, 4096, 64 * 1024, 2_823_168];
    let payloads: Vec<Vec<u8>> = sizes
        .iter()
        .enumerate()
        .map(|(i, &n)| {
            let mut v = vec![0u8; n];
            fill_seeded(&mut v, 0xC0DE_0000 + i as u32);
            v
        })
        .collect();

    // Hashes (content-addressed via blake3).
    let hash_cstrs: Vec<CString> = payloads.iter().map(|p| block_hash_hex(p)).collect();
    let hash_ptrs: Vec<*const std::os::raw::c_char> =
        hash_cstrs.iter().map(|c| c.as_ptr()).collect();
    let payload_ptrs: Vec<*const u8> = payloads.iter().map(std::vec::Vec::as_ptr).collect();
    let lens: Vec<usize> = payloads.iter().map(std::vec::Vec::len).collect();
    let n = payloads.len();

    let ns = CString::new("test-ns").unwrap();

    // PUT
    let bytes_written = wmbt_kv_put_kv_blocks(
        handle_raw,
        ns.as_ptr(),
        hash_ptrs.as_ptr(),
        payload_ptrs.as_ptr(),
        lens.as_ptr(),
        n,
    );
    assert!(bytes_written >= 0, "put_kv_blocks failed: rc={bytes_written}");
    let expected_total: usize = lens.iter().sum();
    assert_eq!(
        bytes_written as usize, expected_total,
        "put_kv_blocks bytes_written ({bytes_written}) != sum of input lens ({expected_total})"
    );

    // GET (borrowed)
    let mut out_payload_ptrs: *const *const u8 = std::ptr::null();
    let mut out_payload_lens: *const usize = std::ptr::null();
    let mut borrow: *mut wmbt_kv_borrow = std::ptr::null_mut();
    let rc_get = wmbt_kv_get_kv_blocks_borrowed(
        handle_raw,
        ns.as_ptr(),
        hash_ptrs.as_ptr(),
        n,
        &raw mut out_payload_ptrs,
        &raw mut out_payload_lens,
        &raw mut borrow,
    );
    assert_eq!(rc_get, 1, "get_kv_blocks_borrowed returned {rc_get} (expected 1 = all hits)");
    assert!(!out_payload_ptrs.is_null());
    assert!(!out_payload_lens.is_null());
    assert!(!borrow.is_null());

    // BYTE-EQUAL each block
    for i in 0..n {
        // SAFETY: API contract, out_payload_ptrs is a C array of n pointers,
        // each pointing to out_payload_lens[i] valid bytes for the borrow's
        // lifetime.
        let got_ptr = unsafe { *out_payload_ptrs.add(i) };
        let got_len = unsafe { *out_payload_lens.add(i) };
        assert_eq!(
            got_len,
            payloads[i].len(),
            "block {i}: returned len {got_len} != input len {}",
            payloads[i].len()
        );
        if got_len == 0 {
            // Empty payload: pointer may be null or non-null per API impl;
            // length-zero is the contract.
            continue;
        }
        assert!(!got_ptr.is_null(), "block {i}: NULL ptr with len > 0");
        let got = unsafe { std::slice::from_raw_parts(got_ptr, got_len) };
        assert_eq!(got, payloads[i].as_slice(), "block {i}: byte mismatch");
    }

    // Release the borrow + drop the handle
    wmbt_kv_release_borrow(borrow);
    let _ = unsafe { Box::from_raw(handle_raw.cast::<Handle>()) };
}

#[test]
fn adversarial_blocks_payload_index_independence() {
    // Sanity: if WombatKV ever confused block_i's payload with block_j's,
    // the byte-roundtrip in the test above would catch it. This test
    // additionally puts blocks in one order and gets them in a SHUFFLED
    // order to catch any internal assumption about (hash, payload) array
    // ordering.
    let store = build_offline_store("shuffle");
    let handle_box: Box<Handle> = Box::new(Handle::from_kvstore(store, "test-ns"));
    let handle_raw = Box::into_raw(handle_box).cast::<wmbt_kv_handle>();

    let mut payloads: Vec<Vec<u8>> = (0..4)
        .map(|i| {
            let mut v = vec![0u8; 1024];
            fill_seeded(&mut v, 0xBEEF_0000 + i as u32);
            v
        })
        .collect();
    let hash_cstrs: Vec<CString> = payloads.iter().map(|p| block_hash_hex(p)).collect();

    // PUT in order [0, 1, 2, 3]
    let hash_ptrs_put: Vec<_> = hash_cstrs.iter().map(|c| c.as_ptr()).collect();
    let payload_ptrs_put: Vec<_> = payloads.iter().map(std::vec::Vec::as_ptr).collect();
    let lens_put: Vec<_> = payloads.iter().map(std::vec::Vec::len).collect();
    let ns = CString::new("test-ns").unwrap();
    let rc_put = wmbt_kv_put_kv_blocks(
        handle_raw,
        ns.as_ptr(),
        hash_ptrs_put.as_ptr(),
        payload_ptrs_put.as_ptr(),
        lens_put.as_ptr(),
        payloads.len(),
    );
    assert!(rc_put >= 0);

    // GET in SHUFFLED order [3, 0, 2, 1]
    let order: [usize; 4] = [3, 0, 2, 1];
    let hash_ptrs_get: Vec<_> = order.iter().map(|&i| hash_cstrs[i].as_ptr()).collect();
    let mut out_payload_ptrs: *const *const u8 = std::ptr::null();
    let mut out_payload_lens: *const usize = std::ptr::null();
    let mut borrow: *mut wmbt_kv_borrow = std::ptr::null_mut();
    let rc_get = wmbt_kv_get_kv_blocks_borrowed(
        handle_raw,
        ns.as_ptr(),
        hash_ptrs_get.as_ptr(),
        order.len(),
        &raw mut out_payload_ptrs,
        &raw mut out_payload_lens,
        &raw mut borrow,
    );
    assert_eq!(rc_get, 1);

    for (k, &orig_i) in order.iter().enumerate() {
        let got_ptr = unsafe { *out_payload_ptrs.add(k) };
        let got_len = unsafe { *out_payload_lens.add(k) };
        assert_eq!(got_len, payloads[orig_i].len(), "shuffle k={k} (→orig {orig_i}) len");
        let got = unsafe { std::slice::from_raw_parts(got_ptr, got_len) };
        assert_eq!(
            got,
            payloads[orig_i].as_slice(),
            "shuffle k={k} (→orig {orig_i}) byte mismatch, payload index drift"
        );
    }

    wmbt_kv_release_borrow(borrow);
    // Use the payloads after the borrow drop to keep them in scope.
    payloads.clear();
    let _ = unsafe { Box::from_raw(handle_raw.cast::<Handle>()) };
}
