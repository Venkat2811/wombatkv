# Gaps and next steps

Forward-looking analysis from the 2026-05-24 methodical campaign.
Written to answer three questions:

1. What result-table cells are still missing for broader launch claims?
2. Why do remote TCP / HTTP daemon modes underperform today?
3. What should be added so benchmark claims become robust rather than anecdotal?

The current tables are strong for: exact restart, partial-prefix
restart, same-process loss cells, and local interleaved-session loss
cells. They are weak for: realistic public multi-turn breadth, trace
realism, remote daemon root-cause attribution, and high-confidence
percentiles.

## 1. Missing result-table cells

### A. Missing benchmark families

Highest-value families still to add:

1. **Public multi-turn replay**: `sharegpt_multiturn` true round-robin replay over public conversations (not just exact single-turn ShareGPT prompt reuse).
2. **Project Gutenberg multi-round QA / CxS**: the LMCache-style "concurrent users × session depth under TTFT SLO" benchmark, the cleanest way to summarize long-lived conversational capacity with one scalar.
3. **Trace / agent replay**: session-aware JSONL trace replay with realistic arrival timing, reuse distance, and working-set churn.
4. **Capacity / working-set cliff**: increase number of distinct conversations until native kv-disk and WombatKV each hit a cliff. Mooncake / LMCache results show storage-tier systems often look similar before the cliff and separate sharply after it.
5. **Transport microbench, expanded**: remote daemon needs its own isolated microbench family, not mixed into end-to-end restore numbers.

### B. Missing cells inside already-started families

**Exact family**, still missing or under-sampled:

- `sharegpt_exact_replay / restart_preserved`
- `sharegpt_exact_replay / same_process`
- Remote daemon rows on more than one prompt family
- Larger sample counts for rows currently at `n=3` or `n=5`

**Partial-prefix family**, current sweep is too narrow:

- Only `shared=10000` chars; need shared-prefix ratios `0 / 25 / 50 / 75 / 90 / 100%`
- Both byte-prefix and token-prefix controlled
- Output-length `1` and output-length `128`
- All 8 modes (native + native_cold + embedded_local + embedded_remote + 4 daemon variants)

**Scenario family**, completed: `pi_review`, `conversation_switch`. Still missing:

- `sharegpt_multiturn`
- `multi_user_multiturn`
- Stronger per-request hit accounting

### C. Missing native baselines

One discipline gap: there is still no clean published `DS4-Cold`
family with kv-disk disabled across every cell.

That baseline is not needed for fairness against WombatKV, but it is
needed to decompose:

- raw engine prefill cost
- native kv-disk benefit
- WombatKV incremental benefit beyond kv-disk

A complete suite keeps all three native reference lines: `DS4-Live`,
`DS4-Cold`, `DS4-KVDisk`.

## 2. Why remote TCP / HTTP underperform today

Current results isolate the main fact:

- `embedded_remote` stays strong (~82 ms exact-restart TTFT)
- Remote daemon does not (5300+ ms)

The main problem is not just "remote object storage exists." It is
specifically out-of-process save / load orchestration, transport
framing, request / response count, and the daemon-side restore
pipeline.

The hot restore path is:

1. Metadata bootstrap / block-prefix lookup
2. Batched block GET
3. Raw-tail repair
4. Install into ds4

Remote daemon work should target those stages directly.

### A. First: prove where the time is going

Before optimizing, instrument one restore into stage timings for:

- Client-side lookup call duration
- Number of daemon RPCs
- Bytes per RPC
- Daemon metadata lookup time
- Block-fetch time
- Raw-tail fetch time
- Object-store read bytes
- Object-store GET count
- ds4 block install time

Without that, remote optimization is guesswork.

### B. Likely performance problems

Based on the code and the observed embedded vs daemon split:

1. **Too many round trips**: block-prefix lookup and block fetch may still be fragmented from the network's point of view.
2. **Small-message tax**: HTTP especially pays when restore is many control messages plus medium-size payloads instead of one large batched transfer.
3. **Double copying / serialization**: daemon path may serialize blocks, frame them, then ds4 copies again on receipt.
4. **Raw-tail sidecar overhead**: if the block-prefix hit is good but raw-tail repair is still chatty, the network penalty can dominate.
5. **Metadata bootstrap not staying hot**: the daemon may pay extra lookup cost after restart relative to embedded local state.
6. **Daemon not exploiting locality**: if the daemon is remote and MinIO is also remote, transport and object-store paths may be serialized instead of overlapped.

### C. Concrete remote perf improvements to try

Ordered by likely leverage:

1. **One RPC, not a dialogue**: client sends chain tip + needed prefix fingerprint; daemon returns matched prefix plan, blocks, and raw-tail in one streamed response where possible.
2. **Aggressive batching**: fetch and return larger block groups per RPC; avoid per-block request / response loops.
3. **Explicit block-count / bytes-per-restore metrics**: tells whether poor remote numbers come from low hit quality or expensive transport.
4. **Apples-to-apples HTTP vs TCP at identical payload batching**: right now "HTTP is slower" is suggestive, not diagnostic. Force both down the same restore batch shape.
5. **Keep daemon next to the object store**: the good deployment shape is ds4 host ↔ daemon host over LAN, daemon host ↔ MinIO over loopback. Not ds4 host ↔ remote daemon ↔ remote MinIO with avoidable extra RTT.
6. **Reduce raw-tail work**: maximize prefix block reuse before sidecar repair; explicitly measure how many tokens are restored through blocks vs raw tail.
7. **Prewarm daemon metadata index**: architecture expects eager metadata bootstrap to matter; make that visible and deterministic in the benchmark protocol.
8. **Transport-native streaming**: TCP should use long-lived connections and avoid reconnect per restore. HTTP should use keep-alive and streaming bodies instead of buffering whole restores.
9. **Tune block size experimentally**: remote mode may want a different sweet spot than local embedded mode. Mooncake-style transfer work suggests page / block granularity can materially shift TTFT.
10. **Run isolated multi-load daemon benches**: the repo already has dedicated daemon / transport bench binaries; use them to separate transport ceiling from ds4 integration overhead.

## 3. Suite hardening (what to add for robust claims)

The suite is honest already. It is not robust enough yet.

### A. Fix methodology holes first

1. Add explicit `DS4-Cold` rows everywhere.
2. Raise low-sample cells to at least `n=20-30` measured requests.
3. Record TTFT `p50 / p95 / p99` consistently, not just medians.
4. Pair TTFT with end-to-end latency, output-token throughput, and ITL / TPOT.

The LMCache / LMBench lesson: TTFT alone is not enough.

### B. Add realistic public workloads

The biggest current gap is realism breadth:

1. ShareGPT round-robin replay with interleaved sessions and reuse-distance stress.
2. Project Gutenberg multi-round QA with long shared document, multiple users, session-depth sweep, and CxS-under-`TTFT_95` SLO.
3. LongBench / TriviaQA-style long-context QA (long input, short answer, prefill-dominant persistent reuse).
4. Agent / trace replay preserving timestamps, session ordering, prompt length, and overlap metadata.

The strongest claims come from long-lived or trace-driven workloads,
not just restart demos.

### C. Add exact shared-prefix methodology from SGLang / Dynamo

Current partial-prefix work is directionally good but not
benchmark-grade yet. Need:

- Exact ratio sweep
- Controlled tokenized shared prefix
- Concurrency sweep at each ratio
- Low-output run for prefill isolation
- Nontrivial-output run for E2E realism

This gives clean scaling curves instead of a few point estimates.

### D. Add capacity and eviction studies

Need to know not just whether WombatKV helps, but when it helps enough
to matter. Sweep:

- Number of distinct active conversations
- Average prompt length
- Session depth
- Preserved vs wiped local state
- Local-object-store vs remote-object-store

Report TTFT / throughput / hit-ratio vs working-set size. This is how
to expose the capacity cliff where native kv-disk starts thrashing
and WombatKV's tiering begins to pay for itself.

### E. Strengthen correctness validation

Add:

1. **Deterministic-output equivalence checks** at `temperature=0`: exact-prompt replay should match native output (or match within a defined tolerance if reasoning-only streaming complicates visible text).
2. **Structured output validators**: scenarios that already request JSON should validate schema and required fields.
3. **Response-shape logging**: assistant-content length, reasoning-content length, finish reason.
4. **Cache-hit attribution validation**: fix `cached_tokens_seen` and join client JSON with server-log markers for block-prefix hits.

### F. Improve run reproducibility

For every run point, capture exact repo SHAs, build flags, environment
variables, model path and checksum, daemon flags, MinIO config, host
CPU / memory / thermal state, network topology. Store one manifest
JSON per run directory.

### G. Cluster-topology discipline from llm-d

Even without Kubernetes, borrow the structure:

- Workload spec file
- Topology spec file
- Run manifest
- One summary artifact
- One per-request artifact

This makes reruns and future regressions much easier to compare.

## 4. Priority order for the next campaign

1. Add `DS4-Cold` everywhere.
2. Complete public `sharegpt_multiturn`.
3. Add Project Gutenberg CxS benchmark.
4. Add exact prefix-ratio sweep across all local modes.
5. Instrument remote daemon restore stages.
6. Isolate TCP vs HTTP with dedicated transport microbench at matched batch shape.
7. Add agent / trace replay.
8. Add working-set cliff study.

## 5. Claim discipline after the next round

After those additions, the suite will be strong enough to support
these claim classes separately:

1. Restart restore claims
2. Partial-prefix reuse claims
3. Session-switch / interleaving claims
4. Cross-host daemon claims
5. Capacity-cliff claims

Until then, the current tables are strong for the four claim classes
already noted (exact restart, partial-prefix restart, same-process
loss, local interleaved-session loss) and weak for the four called
out at the top of this doc.
