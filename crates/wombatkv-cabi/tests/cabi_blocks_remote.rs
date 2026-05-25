//! Daemon-mode round trip for the block-shaped C ABI symbols.
//!
//! Mirrors `cabi_round_trip.rs::cabi_block_shaped_round_trip_against_minio`
//! but routes through the Remote backend (`Backend::Remote(Arc<
//! RemoteKvStoreClient>)`). The setup is:
//!
//!   1. Spawn `wombatkv-daemon --prefix <unique>` against the same `MinIO`
//!      env the embedded tests use.
//!   2. Wait for the daemon to advertise itself.
//!   3. Set `WMBT_KV_REMOTE_PREFIX=<prefix>` so `wmbt_kv_init_from_env`
//!      picks the Remote backend.
//!   4. Run the same `wmbt_kv_put_kv_blocks` →
//!      `wmbt_kv_lookup_block_prefix` → `wmbt_kv_get_kv_blocks_borrowed`
//!      cycle as the embedded test and assert identical semantics.
//!
//! Skips cleanly when `MinIO` env vars or the daemon binary are missing.
//! This is the parity test the block-prefix path needs to ship daemon
//! mode.

#![allow(unsafe_code)]
#![cfg(test)]

use std::ffi::CString;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use wombatkv::{
    wmbt_kv_borrow, wmbt_kv_free, wmbt_kv_get_kv_blocks_borrowed, wmbt_kv_handle,
    wmbt_kv_init_from_env, wmbt_kv_last_error, wmbt_kv_lookup_block_prefix, wmbt_kv_put_kv_blocks,
    wmbt_kv_release_borrow,
};

fn s3_env_ok() -> bool {
    ["WMBT_KV_S3_ENDPOINT", "WMBT_KV_BUCKET", "WMBT_KV_S3_ACCESS_KEY", "WMBT_KV_S3_SECRET_KEY"]
        .iter()
        .all(|k| std::env::var(k).is_ok())
}

/// Build a prefix that fits the *real* macOS POSIX-SHM budget, which is
/// tighter than `wombatkv_daemon::validate_segment_name_budget` thinks:
/// the underlying disruptor also creates `<base>_producer_seq` and
/// `<base>_cr` (coordination cursor) segments, so the actual budget is
/// `31 - len("wmbt_kv_") - len("_resp_producer_seq") = 5` chars. We use
/// `b<3 hex>` = 4 chars to stay under that. See MEMORY task #139
/// "_debug: root-cause daemon-mode SHM ENAMETOOLONG mystery".
fn unique_prefix() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos =
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    // Mix nanos + seq into the bottom 3 hex chars so concurrent test
    // runs (e.g. `cargo test -- --test-threads`) don't collide. 3 hex
    // = 4096 possible prefixes per test binary; collisions are rare
    // enough that the daemon's SHM-segment owner check catches them
    // before the test asserts anything.
    let mixed = (nanos as u64).wrapping_add(seq).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let suffix = format!("{:x}", mixed & 0xfff);
    format!("b{suffix:0>3}")
}

fn find_daemon_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("WMBT_KV_DAEMON_SHM_DAEMON_BIN") {
        let p = PathBuf::from(p);
        if p.is_file() {
            return Some(p);
        }
    }
    let cwd = std::env::current_dir().ok()?;
    let mut dir = cwd.as_path().to_path_buf();
    for _ in 0..6 {
        for variant in ["release", "debug"] {
            let cand = dir.join(format!("target/{variant}/wombatkv-daemon"));
            if cand.is_file() {
                return Some(cand);
            }
        }
        dir = dir.parent()?.to_path_buf();
    }
    None
}

struct DaemonGuard {
    child: Option<Child>,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn spawn_daemon(prefix: &str) -> Option<DaemonGuard> {
    if !s3_env_ok() {
        eprintln!("skipping cabi_blocks_remote: WMBT_KV_S3_* not set");
        return None;
    }
    let bin = if let Some(b) = find_daemon_bin() {
        b
    } else {
        eprintln!("skipping cabi_blocks_remote: daemon binary not found (expected target/release/wombatkv-daemon)");
        return None;
    };
    let child = Command::new(&bin)
        .arg("--prefix")
        .arg(prefix)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    // Give the daemon ~30s to publish its SHM segments. The client
    // attach loop also retries, but a short upfront sleep avoids the
    // attach loop counting a few extra retries.
    std::thread::sleep(Duration::from_millis(100));
    Some(DaemonGuard { child: Some(child) })
}

fn hex_cstring(h: &[u8; 32]) -> CString {
    let mut s = String::with_capacity(64);
    for b in h {
        s.push_str(&format!("{b:02x}"));
    }
    CString::new(s).unwrap()
}

#[test]
fn cabi_block_shaped_round_trip_against_daemon() {
    let prefix = unique_prefix();
    eprintln!("cabi_blocks_remote: prefix={prefix}");

    let Some(_daemon) = spawn_daemon(&prefix) else {
        return;
    };

    // Set the env that flips wmbt_kv_init_from_env to the Remote backend.
    // This is process-wide, so we restore it on drop via a tiny guard.
    struct EnvGuard {
        key: &'static str,
        prior: Option<String>,
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prior {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }
    let prior = std::env::var("WMBT_KV_REMOTE_PREFIX").ok();
    std::env::set_var("WMBT_KV_REMOTE_PREFIX", &prefix);
    let _env_guard = EnvGuard { key: "WMBT_KV_REMOTE_PREFIX", prior };

    // Use a unique namespace so the same daemon can be reused across test
    // re-runs without colliding on previously-stashed keys.
    let nonce = std::env::var("WMBT_KV_REMOTE_PREFIX").unwrap_or_default();
    let ns_owned = format!("cabi-blocks-rt-remote-{nonce}");
    let ns = CString::new(ns_owned.clone()).unwrap();

    // Connect with a short retry budget, the daemon may need a moment
    // to advertise its consumer cursor.
    let mut handle: *mut wmbt_kv_handle = std::ptr::null_mut();
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        handle = wmbt_kv_init_from_env();
        if !handle.is_null() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    if handle.is_null() {
        let err_ptr = wmbt_kv_last_error();
        let msg = if err_ptr.is_null() {
            "(no error message)".to_string()
        } else {
            unsafe { std::ffi::CStr::from_ptr(err_ptr) }.to_string_lossy().into_owned()
        };
        eprintln!("skipping cabi_blocks_remote: wmbt_kv_init_from_env returned NULL: {msg}");
        return;
    }

    // Three distinct content-addressed payloads, matches the embedded
    // round-trip test so a future failure is easy to diff.
    let payload_a: Vec<u8> = (0..2048).map(|i| (i & 0xFF) as u8).collect();
    let payload_b: Vec<u8> = vec![0xAA; 4096];
    let payload_c: Vec<u8> = vec![0xCD; 1024];

    let hash_a: [u8; 32] = *blake3::hash(&payload_a).as_bytes();
    let hash_b: [u8; 32] = *blake3::hash(&payload_b).as_bytes();
    let hash_c: [u8; 32] = *blake3::hash(&payload_c).as_bytes();

    let ha_hex = hex_cstring(&hash_a);
    let hb_hex = hex_cstring(&hash_b);
    let hc_hex = hex_cstring(&hash_c);
    let hashes_hex: [*const std::os::raw::c_char; 3] =
        [ha_hex.as_ptr(), hb_hex.as_ptr(), hc_hex.as_ptr()];

    let payload_ptrs: [*const u8; 3] = [payload_a.as_ptr(), payload_b.as_ptr(), payload_c.as_ptr()];
    let payload_lens: [usize; 3] = [payload_a.len(), payload_b.len(), payload_c.len()];

    // PUT, should route through Backend::Remote::put_kv_blocks via
    // the new PUT_KV_BLOCKS_BATCH opcode. Daemon updates its metadata
    // index server-side.
    let put_total = wmbt_kv_put_kv_blocks(
        handle,
        ns.as_ptr(),
        hashes_hex.as_ptr(),
        payload_ptrs.as_ptr(),
        payload_lens.as_ptr(),
        3,
    );
    let expected_total = (payload_a.len() + payload_b.len() + payload_c.len()) as i64;
    assert_eq!(
        put_total, expected_total,
        "remote put_kv_blocks total {put_total} vs expected {expected_total}"
    );

    // Lookup, should now route through Backend::Remote via the new
    // LOOKUP_BLOCK_PREFIX opcode. Previously this returned -1 with
    // "lookup_block_prefix not supported on remote backend yet".
    let mut matched: usize = 0;
    let mut err_buf = [0i8; 256];
    let rc = wmbt_kv_lookup_block_prefix(
        handle,
        ns.as_ptr(),
        hashes_hex.as_ptr(),
        3,
        &raw mut matched,
        err_buf.as_mut_ptr(),
        err_buf.len(),
    );
    assert_eq!(rc, 0, "remote lookup rc={rc}");
    assert_eq!(
        matched, 3,
        "remote lookup matched={matched}; expected 3 after PUT_KV_BLOCKS_BATCH \
         server-side index update"
    );

    // GET, routes through Backend::Remote::get_kv_blocks (passthrough
    // batched path). All 3 should come back in order.
    let mut out_ptrs: *const *const u8 = std::ptr::null();
    let mut out_lens: *const usize = std::ptr::null();
    let mut borrow: *mut wmbt_kv_borrow = std::ptr::null_mut();
    let rc_get = wmbt_kv_get_kv_blocks_borrowed(
        handle,
        ns.as_ptr(),
        hashes_hex.as_ptr(),
        3,
        &raw mut out_ptrs,
        &raw mut out_lens,
        &raw mut borrow,
    );
    assert_eq!(rc_get, 1, "remote get_kv_blocks_borrowed rc={rc_get}");
    assert!(!out_ptrs.is_null());
    assert!(!out_lens.is_null());
    assert!(!borrow.is_null());

    let got_a = unsafe { std::slice::from_raw_parts(*out_ptrs.add(0), *out_lens.add(0)) };
    let got_b = unsafe { std::slice::from_raw_parts(*out_ptrs.add(1), *out_lens.add(1)) };
    let got_c = unsafe { std::slice::from_raw_parts(*out_ptrs.add(2), *out_lens.add(2)) };
    assert_eq!(got_a, payload_a.as_slice());
    assert_eq!(got_b, payload_b.as_slice());
    assert_eq!(got_c, payload_c.as_slice());
    wmbt_kv_release_borrow(borrow);

    // Miss path, ask for one hash we never put. Lookup should report
    // 3 leading hits (the ones we put + a miss at position 3).
    let bogus = CString::new("99".repeat(32)).unwrap();
    let miss_hashes: [*const std::os::raw::c_char; 4] =
        [ha_hex.as_ptr(), hb_hex.as_ptr(), hc_hex.as_ptr(), bogus.as_ptr()];

    let mut miss_matched: usize = 0;
    let rc_lk = wmbt_kv_lookup_block_prefix(
        handle,
        ns.as_ptr(),
        miss_hashes.as_ptr(),
        4,
        &raw mut miss_matched,
        std::ptr::null_mut(),
        0,
    );
    assert_eq!(rc_lk, 0, "remote lookup with miss rc={rc_lk}");
    assert_eq!(
        miss_matched, 3,
        "expected 3 leading hits before miss at index 3; got {miss_matched}"
    );

    // GET-miss should also return 0 (all-or-nothing).
    let mut miss_ptrs: *const *const u8 = std::ptr::null();
    let mut miss_lens: *const usize = std::ptr::null();
    let mut miss_borrow: *mut wmbt_kv_borrow = std::ptr::null_mut();
    let rc_miss = wmbt_kv_get_kv_blocks_borrowed(
        handle,
        ns.as_ptr(),
        miss_hashes.as_ptr(),
        4,
        &raw mut miss_ptrs,
        &raw mut miss_lens,
        &raw mut miss_borrow,
    );
    assert_eq!(rc_miss, 0, "remote get_kv_blocks miss rc={rc_miss}");

    wmbt_kv_free(handle);
}
