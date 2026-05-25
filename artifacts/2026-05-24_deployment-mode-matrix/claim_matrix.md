# Claim matrix

The shortest honest statement of what this campaign supports.
Mirrored from the campaign's final `claim_matrix.md` (2026-05-24).

## Supported claims

| Claim | Evidence |
|---|---|
| ds4 native wins same-process immediate reuse on one host. | canonical exact `same_process`: native **80.7 ms**, embedded_local 327.8 ms, daemon_shm 5932.1 ms. |
| ds4 native kv-disk wins exact same-prompt preserved local restart. | canonical exact `restart_preserved`: native **24.2 ms**, embedded_local 79.7 ms, daemon_shm 146.8 ms. |
| WombatKV embedded_local wins exact restart after local kv-disk state is wiped. | canonical exact `restart_wiped`: native 6986.7 ms, embedded_local **77.9 ms**. |
| WombatKV exact-restart gains are not limited to one synthetic prompt. | sharegpt_exact_replay / restart_wiped: native 5329.5 ms, embedded_local **78.1 ms**, embedded_remote **94.9 ms**. |
| WombatKV helps on partial-prefix reuse, not only exact replay. | partial-prefix shared=10000, suffix=2048: preserved native 8510.6 ms vs embedded_local **3475.2 ms**; wiped native 19840.9 ms vs **6097.5 ms**. |
| WombatKV embedded_local can beat both native baselines on realistic long-document multi-round workloads. | gutenberg_multiround: native 40841.9 ms, native_cold 60758.9 ms, embedded_local **29425.9 ms**. |
| Remote object storage by itself is not the reason remote daemon modes are slow. | exact restart: embedded_remote **82.1 ms** vs daemon_tcp_remote 5317.0 ms; remote restore `get_ms` is the large gap. |

## Qualified claims

| Claim | Qualification |
|---|---|
| WombatKV improves restart TTFT. | True in wiped-state and many partial-prefix cells; **false for exact same-prompt preserved local restart** where native kv-disk wins. |
| Daemon modes can improve restore latency over native ds4. | True in some controlled partial-prefix and exact wiped restart cells; **false on pi_review, conversation_switch, sharegpt_round_robin**. |
| TCP and HTTP remote daemon modes are functionally valid. | They produced coherent outputs in tested cells, but current performance is far behind embedded_remote. |
| Native ds4 is always better for realistic chat. | True in the measured interleaved ShareGPT and local-churn scenarios; **not true for the Gutenberg long-context workload**. |

## Unsupported claims

| Claim | Why |
|---|---|
| WombatKV is a universal win on public realistic workloads. | ShareGPT round-robin is a loss/parity cell for embedded_local; only Gutenberg is a strong realistic win. |
| Remote daemon performance is already competitive with embedded remote. | Exact restart and transport benches both show a large fetch/serve-path deficit. |
| Public multi-turn breadth is fully covered. | The monolithic `sharegpt_multiturn` and `multi_user_multiturn` scenario families were intentionally not finished. |
| The suite proves cross-engine conclusions beyond ds4. | This campaign is ds4-specific even though earlier research used LMCache, SGLang, Mooncake, and llm-d to shape the methodology. |
