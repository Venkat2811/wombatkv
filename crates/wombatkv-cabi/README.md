# wombatkv-cabi

C ABI surface over `wombatkv-node::WombatKVKvStore`. Produces
`libwombatkv.dylib` (macOS) / `libwombatkv.so` (Linux) so C-callable
engines like **ds4**, **llama.cpp**, **Ollama** can talk to the
WombatKV substrate without a Rust path dependency.

If you're integrating WombatKV into a new engine: this is the entry
point.

## Build

```sh
cargo build --release -p wombatkv-cabi
# → target/release/libwombatkv.{dylib,so}
# → crates/wombatkv-cabi/include/wombatkv.h  (already in tree)
```

## Backends, picked at handle-open time

`wmbt_kv_init_from_env()` picks one of four backends from env vars:

| env var | backend | notes |
|---|---|---|
| (none of below set) | `Embedded` | in-process `WombatKVKvStore`, engine + cache + S3 all in one process |
| `WMBT_KV_REMOTE_PREFIX=<name>` | `Remote` (SHM daemon) | engine ↔ same-host daemon over disruptor SHM ring |
| `WMBT_KV_TCP_ADDR=<host:port>` | `RemoteTcp` | engine ↔ cross-host daemon over TCP (length-prefixed rkyv) |
| `WMBT_KV_HTTP_ADDR=<host:port>` | `RemoteHttp` | engine ↔ cross-host daemon over HTTP/1.1 (rkyv-in-POST, load-balancer friendly) |

Priority order: SHM > TCP > HTTP > Embedded.

Direct constructors `wmbt_kv_open_tcp(addr)` and
`wmbt_kv_open_http(addr)` bypass env dispatch when engines want to
pick a transport explicitly.

## Public C surface

See `include/wombatkv.h` for the full ABI. The surface is hand-
written (not bindgen-generated) so it's reviewable. Cluster summary:

**Lifecycle**
- `wmbt_kv_init_from_env()`, open handle from env-resolved backend
- `wmbt_kv_open_tcp(addr)` / `wmbt_kv_open_http(addr)`, explicit transport
- `wmbt_kv_free(handle)`, close
- `wmbt_kv_last_error()`, get last per-thread error string

**Block-prefix hot path** (chain-shaped KV layout, ds4, llama.cpp, Ollama)
- `wmbt_kv_lookup_block_prefix(namespace, fp, hashes, n_hashes, *matched)`
- `wmbt_kv_get_kv_blocks_borrowed(namespace, fp, hashes, n_hashes, **ptrs, *lens, *borrow)`
- `wmbt_kv_put_kv_blocks(namespace, fp, hashes, payloads, lens, n)`
- `wmbt_kv_release_borrow(borrow)`, release a borrowed block batch

**Sidecar (raw_tail), chain-tip partial bytes**
- `wmbt_kv_put_raw_tail(namespace, fp, chain_tip_hash, bytes, n)`
- `wmbt_kv_get_raw_tail_borrowed(namespace, fp, chain_tip_hash, *ptr, *len, *borrow)`

**Primitives (hardware-dispatched)**
- `wmbt_kv_crc32c(bytes, n)`, one-shot Castagnoli CRC32C
- `wmbt_kv_crc32c_append(crc, bytes, n)`, streaming CRC fold
- `wmbt_kv_blake3_64hex(bytes, n, out_hex)`: 32-byte BLAKE3 → 64-hex

On ARMv8.1+ the CRC32C path dispatches to `__crc32cb/h/w/d`
intrinsics; on x86_64 SSE4.2 to `_mm_crc32_u{8,16,32,64}`;
software-table fallback elsewhere. Same primitive across every
target an engine integration runs on.

## Return-code semantics (implicit contract, not in header)

| function | success | error | notes |
|---|---|---|---|
| `wmbt_kv_init_from_env` etc | non-NULL handle | NULL + `last_error()` | per-thread error string |
| `wmbt_kv_lookup_block_prefix` | `rc=0`, `*matched` set | `rc!=0` | `matched` = prefix length cached |
| `wmbt_kv_get_kv_blocks_borrowed` | `rc=1`, `**ptrs`, `*lens`, `*borrow` set | `rc!=1` | release with `_release_borrow` |
| `wmbt_kv_put_kv_blocks` | `rc>=0` (bytes written) | `rc<0` | |
| `wmbt_kv_get_raw_tail_borrowed` | `rc=1` (hit), `rc=0` (miss) | `rc=-1` | |
| `wmbt_kv_put_raw_tail` | `rc=0` | `rc!=0` | |

## Minimal C usage

```c
#include "wombatkv.h"
#include <stdio.h>

int main(void) {
    /* Resolve backend from WMBT_KV_* env vars at handle-open */
    void* h = wmbt_kv_init_from_env();
    if (!h) {
        fprintf(stderr, "wombatkv init failed: %s\n", wmbt_kv_last_error());
        return 1;
    }

    /* Block-prefix lookup: how many of these N hashes are already cached? */
    const char* fp = "abcdef0123456789abcdef01";  /* 24-hex model fingerprint */
    const char* hashes[3] = { /* 64-hex block hashes */ "00..", "11..", "22.." };
    size_t matched = 0;
    int rc = wmbt_kv_lookup_block_prefix("my-namespace", fp, hashes, 3, &matched);
    if (rc != 0) {
        fprintf(stderr, "lookup error: %s\n", wmbt_kv_last_error());
        wmbt_kv_free(h);
        return 1;
    }
    printf("matched first %zu of 3 blocks\n", matched);

    wmbt_kv_free(h);
    return 0;
}
```

Link with `-lwombatkv` and add the dylib's directory to your
runtime library path (or use rpath, as ds4's Makefile does):

```sh
cc -I$(WOMBATKV)/crates/wombatkv-cabi/include \
   -L$(WOMBATKV)/target/release \
   -lwombatkv \
   -Wl,-rpath,$(WOMBATKV)/target/release \
   demo.c -o demo
```

## ds4 reference integration

A full ds4 integration lives at
[`Venkat2811/ds4`](https://github.com/Venkat2811/ds4). The cabi
calls are all under `#ifdef DS4_WOMBATKV` so the default ds4 build
is byte-identical to upstream antirez/ds4. See `ds4_server.c` (the
`wmbt_kv_init_hooks` function) for the init-from-env pattern.

## Test

```sh
# Pure unit tests + adversarial byte-roundtrip:
cargo test -p wombatkv-cabi --release

# Full integration smoke (requires MinIO at $WMBT_KV_S3_ENDPOINT):
WMBT_KV_S3_ENDPOINT=http://127.0.0.1:9000 \
WMBT_KV_BUCKET=cabi-smoke \
WMBT_KV_S3_ACCESS_KEY=minioadmin WMBT_KV_S3_SECRET_KEY=minioadmin \
  cargo test --release -p wombatkv-cabi -- --ignored
```

## RFC pointers

- RFC 0006: KVBlock wire format (block-shaped surfaces)
- RFC 0014, cabi TCP extension (TCP + HTTP)
- RFC 0018, wire envelope discipline (universal envelope)
