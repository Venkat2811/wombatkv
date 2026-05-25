# WombatKV

**Object-storage-native KV cache system for LLM inference.**

> *Wombat has your Blocks.*

WombatKV gives an LLM inference engine prefix-share blocks that survive
across processes, machines, and pod restarts. Anything an engine prefills
once (system prompts, RAG context, conversation history) lands in an
S3 bucket as content-addressed blocks. The next process, the next
machine, the next teammate gets sub-100ms warm-restore instead of
paying full prefill again.

The 0.1.0-alpha ships with the [ds4](https://github.com/Venkat2811/ds4)
adapter, validated on M3 Max with local MinIO and cross-host
Mac engine to Linux daemon.

Headline numbers, deployment-mode triggers, and the wins-vs-losses
table live in the top-level [`README.md`](../../README.md). This book
goes deeper.

## What this book covers

| Section | When to read it |
|---|---|
| [Getting Started](./getting-started/modes.md) | new operator: minimum env to bring up each mode |
| [Concepts](./concepts/architecture.md) | new contributor: crate layout, layered model, consistency story |
| [Operations](./operations/env.md) | day-to-day ops: env vars, tuning, benchmarks |
| [Reference](./reference/crates.md) | tour of the 9 crates |
| [Contributing](./contributing.md) | dev environment, test gates, PR norms |

## Status

WombatKV is **alpha**: pre-1.0, pre-OSS-launch. The system is
feature-stable for ds4-style integrations; vLLM, SGLang, llama.cpp,
Ollama, NVIDIA Dynamo, and llm-d integrations are on the roadmap.
Wire format, on-disk envelopes, and C ABI surface are under the
alpha breaking window: changes can land without back-compat shims
until the OSS tag.
