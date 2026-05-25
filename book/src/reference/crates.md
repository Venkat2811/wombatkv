# Crate organization

WombatKV is a Cargo workspace of 9 crates with strict leaf-first
layering. Leaf crates (no internal deps) define the foundation; each
higher layer composes the leaves.

```
            ┌─────────────────────────────────────────────┐
            │           USERS (engines)                   │
            │      ds4 / vLLM / SGLang / future           │
            │           ↓ (C ABI)                         │
            └─────────────────────────────────────────────┘
                              │
                  ┌───────────▼───────────┐
                  │   wombatkv-cabi       │  cdylib (libwombatkv.so/.dylib)
                  │   extern "C"          │  + C header crates/wombatkv-cabi/include/wombatkv.h
                  └───────────┬───────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌──────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ wombatkv-    │  │ wombatkv-daemon  │  │ wombatkv-node    │
│ store        │  │ rlib + bin       │  │ embed-mode lib   │
│ (S3 backend) │  │ + SHM/TCP/HTTP   │  │ (foyer cache,    │
│              │  │ + arena + config │  │  prefetch, codec)│
└──────┬───────┘  └────────┬─────────┘  └────────┬─────────┘
       │                   │                     │
       ▼                   ▼                     ▼
┌──────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ wombatkv-    │  │ wombatkv-radix   │  │ wombatkv-core    │
│ format       │  │ (SlateDB L1      │  │ (FactRef,        │
│ (WAL/segment │  │  metadata index) │  │  wal_chunk_key,  │
│  codecs)     │  │                  │  │  contest logic)  │
└──────────────┘  └──────────────────┘  └──────────────────┘

side-cars (off the engine ↔ S3 critical path):
  wombatkv-dst  : DST harness, 20 failure classes
  wombatkv-bench, perf benchmark binaries
```

## At-a-glance

| Crate | Role | Output |
|---|---|---|
| `wombatkv-core` | Type system + WAL chunk naming + contest logic | rlib |
| `wombatkv-format` | Binary WAL v1 + segment v1 codecs | rlib |
| `wombatkv-store` | S3 backend (segment + WAL durability) | rlib |
| `wombatkv-radix` | SlateDB-backed metadata index (block-hash → location) | rlib |
| `wombatkv-node` | Embed mode: foyer hybrid cache + block prefetch + zstd codec | rlib |
| `wombatkv-daemon` | Daemon process: SHM ring + TCP/HTTP bridges + arena | rlib + bin |
| `wombatkv-cabi` | C ABI for engines: `wmbt_kv_*` extern "C" + CRC32C + BLAKE3 | **cdylib** + rlib |
| `wombatkv-dst` | Deterministic simulation testing: 20 failure classes + runner | rlib + 2 bins |
| `wombatkv-bench` | Perf benchmark binaries (no library) | bins only |

Each crate has its own README at `crates/wombatkv-*/README.md` with
the module map, public surface, and a minimal usage example.

## Dep edges

- `cabi → {node, store, daemon, radix}`
- `daemon → {node, store, radix}`
- `node → {core, format, radix, store}`
- `store → format`
- `radix`, `core`, `format`, `dst`: zero internal deps (leaves)

No cycles. The leaves can be embedded into constrained environments
(readers, diagnostic tools) without dragging in the full stack.
