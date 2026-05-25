/* SPDX-License-Identifier: Apache-2.0
 *
 * wombatkv.h: C ABI for the WombatKV KV store.
 *
 * Producing dylib: libwombatkv.dylib (macOS) / libwombatkv.so (Linux)
 * Built from `crates/wombatkv-cabi` with:
 *     cargo build -p wombatkv-cabi --release
 *
 * Engines (e.g. llama.cpp) link against this library and invoke the
 * symbols below at the prefill/decode boundary to stash and restore
 * per-sequence KV state. Block-native engines such as vLLM can use the
 * generic namespace/key KV calls.
 *
 * ABI version: 1.0  (see wmbt_kv_abi_version()).
 *
 * THREADING: wmbt_kv_handle_t is internally synchronized; multiple
 * threads may share one handle. wmbt_kv_last_error() is thread-local.
 *
 * MEMORY: all input pointers are borrowed for the duration of the
 * call and may be freed by the caller immediately afterward. The
 * library makes its own copies. Output pointers (out_buf in
 * wmbt_kv_load) are written into by the call.
 */

#ifndef WOMBATKV_H
#define WOMBATKV_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle. Construct with wmbt_kv_init_from_env, free with wmbt_kv_free. */
typedef struct wmbt_kv_handle wmbt_kv_handle_t;

/* ABI version. Pack as (major << 16) | minor. */
uint32_t wmbt_kv_abi_version(void);

/* Compute the 64-hex-char (32-byte) blake3 digest of `in`.
 * `out` must point to a writable buffer of at least 65 bytes
 * (64 hex chars + NUL terminator). Returns 0 on success, -1 on
 * null arg. Threadsafe.
 *
 * Designed for content addressing (e.g., ds4's block-prefix hash);
 * NOT a keyed MAC. RFC 0011 §C5 / RFC 0013 §6. */
int32_t wmbt_kv_blake3_64hex(const uint8_t *in, size_t in_len, char *out);

/* Hardware-accelerated CRC32C (Castagnoli polynomial 0x1EDC6F41).
 *
 * Returns the finalized CRC32C value over `in_len` bytes at `in`.
 * Equivalent to wmbt_kv_crc32c_append(0, in, in_len). A null `in`
 * with `in_len == 0` is treated as the empty input.
 *
 * Implementation dispatches at runtime: x86_64 SSE4.2 (Intel
 * Nehalem+, AMD Bulldozer+), ARMv8.1 CRC32 (Apple Silicon, AWS
 * Graviton, Ampere), or a software-table fallback. Engines integrating
 * WombatKV (ds4, vLLM, SGLang, ...) route their envelope-CRC
 * verification through here so each integration inherits hardware
 * acceleration without re-implementing per-arch ifdef ladders.
 * Threadsafe. */
uint32_t wmbt_kv_crc32c(const uint8_t *in, size_t in_len);

/* Streaming CRC32C: append `in_len` bytes at `in` to a running
 * accumulator `crc` and return the new accumulator. Pass `crc = 0`
 * to start a fresh stream. Use to fold multiple non-contiguous
 * buffers into a single CRC without materialising them together.
 * Same hardware-dispatch story as wmbt_kv_crc32c. Threadsafe. */
uint32_t wmbt_kv_crc32c_append(uint32_t crc, const uint8_t *in, size_t in_len);

/* Initialize a handle from environment variables.
 *
 * Required: WMBT_KV_S3_ENDPOINT, WMBT_KV_BUCKET, WMBT_KV_S3_ACCESS_KEY,
 *           WMBT_KV_S3_SECRET_KEY.
 * Optional: WMBT_KV_S3_REGION, WMBT_KV_S3_FORCE_PATH_STYLE,
 *           WMBT_KV_PUFFER_RAM_BYTES, WMBT_KV_PUFFER_DISK_BYTES,
 *           WMBT_KV_PUFFER_DIR, WMBT_KV_PUFFER_BLOCK_SIZE_BYTES,
 *           WMBT_KV_S3_PREFIX, WMBT_KV_NAMESPACE,
 *           WMBT_KV_REMOTE_PREFIX,
 *           WMBT_KV_BLOCK_COMPRESS, WMBT_KV_BLOCK_COMPRESS_LEVEL.
 *
 * Block-storage compression (transparent; default off):
 *   WMBT_KV_BLOCK_COMPRESS=zstd        opt in (also: lz4 reserved, none)
 *   WMBT_KV_BLOCK_COMPRESS_LEVEL=<N>   zstd level 1..22, default 3
 * When enabled, put_kv writes a 10-byte `WBZ1` envelope at the
 * object-store boundary; get_kv detects the envelope and decompresses
 * transparently. Mixed buckets (old uncompressed + new compressed)
 * stay readable.
 *
 * If WMBT_KV_REMOTE_PREFIX is set, the handle connects to a
 * wombatkv-daemon over SHM (daemon mode) instead of opening an
 * in-process store (embedded mode).
 *
 * Returns NULL on error. Inspect wmbt_kv_last_error() for diagnostics. */
wmbt_kv_handle_t* wmbt_kv_init_from_env(void);

/* Construct a TCP-backed handle. `addr` is a NUL-terminated
 * "host:port" string; the daemon side must be running
 * `wombatkv-daemon --tcp <addr>`. Equivalent to setting
 * WMBT_KV_TCP_ADDR=<addr> in the env and calling
 * wmbt_kv_init_from_env, but lets the engine pick TCP without
 * having to mutate env vars (useful when one ds4 process opens
 * multiple handles or wants to A/B local-SHM vs remote-TCP).
 *
 * Cross-machine topology (RFC 0014):
 *   engine on host A → libwombatkv.dylib (this handle) → TCP →
 *       wombatkv-daemon on host B → foyer + S3.
 *
 * Returns NULL on connect failure. Inspect wmbt_kv_last_error(). */
wmbt_kv_handle_t* wmbt_kv_open_tcp(const char* addr);

/* Construct an HTTP-backed handle. `addr` is a NUL-terminated
 * "host:port" string; the daemon side must be running
 * `wombatkv-daemon --http <addr>`. Equivalent to setting
 * WMBT_KV_HTTP_ADDR=<addr> in the env and calling
 * wmbt_kv_init_from_env.
 *
 * Same WireRequest/WireResponse rkyv envelope as wmbt_kv_open_tcp,
 * but wrapped in HTTP/1.1 POSTs to /wmbt/v1/rpc. Use this when the
 * path between engine and daemon needs to traverse an HTTP-aware
 * load balancer, reverse proxy, or middlebox that won't pass raw
 * rkyv-over-TCP.
 *
 * Returns NULL on connect failure. Inspect wmbt_kv_last_error(). */
wmbt_kv_handle_t* wmbt_kv_open_http(const char* addr);

/* Free a handle. Safe to pass NULL. */
void wmbt_kv_free(wmbt_kv_handle_t* handle);

/* Most recent error message on the calling thread. The pointer is
 * valid until the next wmbt_kv_* call on this thread; copy if needed.
 * Returns NULL when there is no recorded error. */
const char* wmbt_kv_last_error(void);

/* Opaque borrow handle returned by the zero-copy `*_borrowed` getter
 * functions (`wmbt_kv_get_kv_blocks_borrowed`,
 * `wmbt_kv_get_raw_tail_borrowed`). The caller releases it via
 * `wmbt_kv_release_borrow` when done reading the payload. */
typedef struct wmbt_kv_borrow wmbt_kv_borrow_t;

/* Release a borrow handle. Safe to call with NULL. After release the
 * pointer returned via *out_ptr is no longer valid. */
void wmbt_kv_release_borrow(wmbt_kv_borrow_t* borrow);

/* Block-shaped surfaces (ABI 1.5).
 *
 * These wrap the in-process metadata index and the parallel batched-IO
 * paths for callers (vLLM connector, SGLang connector, ds4_server's
 * block-prefix path) that already own a content-addressed blocks.
 *
 * Hash format: 32-byte blake3 digest passed as a 64-character lower-hex
 * NUL-terminated C string ("0a1b...ef", no `0x` prefix, no separators).
 *
 * Key form: the C ABI internally maps each hash to the standalone key
 *           `wkv/v1/block/b3=<hex>`. Producers and consumers MUST agree
 *           on this scheme, it is part of the ABI contract.
 *
 * Backend support:
 *   - Embedded:       all three calls supported.
 *   - Daemon (SHM/TCP): put + get are supported; lookup_block_prefix
 *                     returns -1 with "lookup_block_prefix not supported
 *                     on remote backend yet", the daemon does not yet
 *                     expose its metadata index over the wire.
 */

/* Walk a chain of block hashes and report how many leading entries are
 * present in the metadata index.
 *
 * On success writes the longest matching prefix length into
 * *out_matched_count (counted from index 0). For a chain [h0, h1, h2]
 * where h0 and h1 are present but h2 is not, returns 0 with
 * *out_matched_count == 2.
 *
 * err_buf is optional. When non-NULL and err_len > 0, the most recent
 * error message is copied (NUL-terminated, truncated if needed) into
 * that buffer. The thread-local last-error slot is also set, so
 * wmbt_kv_last_error() remains a valid fallback.
 *
 * Returns:
 *    0   success; *out_matched_count set
 *   -1   error; see err_buf / wmbt_kv_last_error()
 */
int32_t wmbt_kv_lookup_block_prefix(
    wmbt_kv_handle_t*    handle,
    const char*          namespace_,
    const char* const*   block_hashes_hex,
    size_t               block_count,
    size_t*              out_matched_count,
    char*                err_buf,
    size_t               err_len
);

/* Parallel GET for a batch of content-addressed blocks.
 *
 * On full hit: writes a borrowed array of N payload pointers and N
 * lengths through *out_payload_ptrs / *out_payload_lens, plus a release
 * handle through *out_borrow. The arrays and the pointed-to bytes
 * remain valid until wmbt_kv_release_borrow(*out_borrow).
 *
 * All-or-nothing semantics: a single missing block returns 0 (miss) and
 * the out parameters are left untouched. Callers can fall back to a
 * cold-fetch path.
 *
 * Returns:
 *   1   all blocks hit; outputs set
 *   0   at least one miss
 *  -1   error; see wmbt_kv_last_error()
 */
int32_t wmbt_kv_get_kv_blocks_borrowed(
    wmbt_kv_handle_t*    handle,
    const char*          namespace_,
    const char* const*   block_hashes_hex,
    size_t               block_count,
    const uint8_t***     out_payload_ptrs,
    const size_t**       out_payload_lens,
    wmbt_kv_borrow_t**   out_borrow
);

/* Parallel PUT for a batch of content-addressed blocks.
 *
 * Writes each (block_hashes_hex[i], payloads[i], lens[i]) to the
 * configured backend and inserts a root metadata entry so that a
 * subsequent wmbt_kv_lookup_block_prefix reflects the new presence.
 *
 * Returns the total number of bytes written (sum of lens) on success.
 * On any per-block failure returns -1 and leaves the metadata index
 * untouched for the failing batch.
 *
 * Returns:
 *   >= 0   total bytes written
 *   -1     error; see wmbt_kv_last_error()
 */
int64_t wmbt_kv_put_kv_blocks(
    wmbt_kv_handle_t*    handle,
    const char*          namespace_,
    const char* const*   block_hashes_hex,
    const uint8_t* const* payloads,
    const size_t*        lens,
    size_t               block_count
);

/* Raw-tail sidecar surfaces (ABI 1.6, RFC 0007 §10.P5).
 *
 * The sidecar architecture stores ONE raw-KV blob per unique chain-tip
 * block hash under the key `wkv/v1/sidecar/raw_tail/b3=<hex>`. The
 * block chain itself (wkv/v1/block/...) is untouched, these surfaces
 * are layered ON TOP of the block API rather than modifying it.
 *
 * Storage cost: O(unique chain-tips) × raw_window_bytes (≈ 11.3 MB for
 * DSV4 with DS4_N_SWA=128). Not O(blocks × raw_window_bytes), which was
 * the cost of the every-block-trailer spike that this replaces.
 *
 * Composability: a partial-prefix match of length M ≤ N_saved will
 * find the sidecar at chain[M-1].hash (the chain-tip of the matched
 * prefix), because that hash is uniquely determined by the first M
 * tokens. The same prompt that produced chain[N-1] in a previous
 * session will, on partial reuse, see chain[M-1] of the new session
 * equal chain[M-1] of the old session for any M.
 *
 * Producer flow (per block-prefix save):
 *   wmbt_kv_put_kv_blocks(...)      // existing
 *   wmbt_kv_put_raw_tail(handle, ns, chain_hex[N-1], raw_bytes, len)
 *
 * Consumer flow (per block-prefix load with matched=M):
 *   wmbt_kv_get_kv_blocks_borrowed(...)        // existing
 *   ds4_session_load_blocks(...)               // existing
 *   wmbt_kv_get_raw_tail_borrowed(handle, ns, chain_hex[M-1], ...)
 *   // on hit: install raw bytes into SWA ring, skip re-prefill
 */

/* PUT a raw-tail sidecar payload keyed by the chain-tip block hash.
 *
 * `chain_tip_hash_hex` MUST be a 64-character lower-hex blake3 digest
 * (the hash of the LAST block in the chain being saved).
 *
 * `err_buf` is optional. When non-NULL and `err_len > 0`, the most
 * recent error message is copied (NUL-terminated, truncated if needed)
 * into that buffer. The thread-local last-error slot is also set, so
 * wmbt_kv_last_error() remains a valid fallback.
 *
 * Returns:
 *    0  success
 *   -1  error; see err_buf / wmbt_kv_last_error()
 */
int32_t wmbt_kv_put_raw_tail(
    wmbt_kv_handle_t*    handle,
    const char*          namespace_,
    const char*          chain_tip_hash_hex,
    const uint8_t*       bytes,
    size_t               len,
    char*                err_buf,
    size_t               err_len
);

/* GET a raw-tail sidecar payload by chain-tip block hash with borrow
 * semantics.
 *
 * On hit: writes a borrowed pointer + length into *out_bytes / *out_len
 * and writes an opaque release handle into *out_borrow. The pointer is
 * valid until wmbt_kv_release_borrow(*out_borrow).
 *
 * `err_buf` has the same semantics as in wmbt_kv_put_raw_tail.
 *
 * Returns:
 *    1  hit; outputs set
 *    0  miss (no sidecar for this chain-tip, caller falls back to the
 *           slow re-prefill path; absence is not an error)
 *   -1  error; see err_buf / wmbt_kv_last_error()
 */
int32_t wmbt_kv_get_raw_tail_borrowed(
    wmbt_kv_handle_t*    handle,
    const char*          namespace_,
    const char*          chain_tip_hash_hex,
    const uint8_t**      out_bytes,
    size_t*              out_len,
    wmbt_kv_borrow_t**   out_borrow,
    char*                err_buf,
    size_t               err_len
);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif  /* WOMBATKV_H */
