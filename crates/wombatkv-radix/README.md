# wombatkv-radix

Block-metadata index, the in-memory and SlateDB-backed indexes that
let `wombatkv-node` answer `lookup_block_prefix` quickly without
hitting S3 for every hash.

## What lives here

- `block_meta`: `BlockMeta`, `BlockHash`, `InMemoryMetadataIndex`,
  `MetadataIndex` trait. The L0 hot path.
- `slatedb_meta`: `SlateDbMetadataIndex`, the L1 persistent index
  backed by SlateDB (RFC 0008).

Forbids `unsafe_code`.

## L0 / L1 split

- L0 (`InMemoryMetadataIndex`): BTreeMap of `BlockHash → BlockMeta`,
  rebuilt on every daemon start via `bootstrap_world_knowledge`
  (S3 scan) and `bootstrap_from_slatedb` (fast path).
- L1 (`SlateDbMetadataIndex`): SlateDB-backed, persists across
  restarts, scoped by `node_id` + `namespace`.

The daemon hydrates L0 from L1 first (fast), then from S3 (slow but
authoritative). Block PUTs write through L0 + L1 synchronously so
the next `lookup_block_prefix` sees them.

## Test

```sh
cargo test -p wombatkv-radix --release
```
