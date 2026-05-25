# wombatkv-node

The embedded WombatKV KV store, foyer cache (RAM + SSD) over an
S3 backend, plus block-prefix lookup, prefetch worker, LRU eviction,
and block-storage compression.

This is the "embedded" mode of WombatKV (in-process). The "daemon"
mode (SHM / TCP / HTTP) lives in `wombatkv-daemon` but reuses the
`WombatKVKvStore` type from this crate as its backing store.

## Modules

| module | what |
|---|---|
| `embed` | `WombatKVKvStore`, top-level KV API (`put_kv`, `get_kv`, `lookup_block_prefix`, etc.) |
| `foyer_cache` | RAM + SSD tier wrapping the durable S3 layer |
| `kv_blob_cache` | flat-file cache for hot blobs |
| `block_prefetch` | background worker that pre-fetches top-K likely blocks |
| `lru` | LRU eviction worker (off-by-default; opt-in via `WMBT_KV_NAMESPACE_MAX_BYTES`) |
| `compression` | optional block-storage compression (zstd; off by default) |
| `embed_metrics` | timing + counter instrumentation |

Forbids `unsafe_code`.

## Stack position

```
wombatkv-node  ← you are here (embedded KV)
  ↓ used by
wombatkv-cabi  (C ABI → libwombatkv.dylib for ds4, llama.cpp, etc.)
wombatkv-daemon (SHM / TCP / HTTP server wrapping a WombatKVKvStore)
```

## Test

```sh
cargo test -p wombatkv-node --release
```
