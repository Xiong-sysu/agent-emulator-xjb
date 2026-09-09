# AgentEmulator

Trace-driven joint simulation of agents and a sharded blockchain (built on
BlockEmulator-X). An agent story written as JSONL traces is compiled into real
blockchain transactions inside the supervisor, executed by a sharded PBFT
chain, and summarized into statistics — multi-round stories run on one chain
with continuous nonces.

## Architecture

```
traces/rounds/round_1.jsonl      round_2.jsonl   ...   (the story)
        |                              ^
        v                              | FileAgentAPI (next round file)
  agentemu.Host  (inside the supervisor, tx_source = agent_source)
  join/leave -> DID register/revoke call, pay -> transfer, append_log -> audit
        |
        v  existing injection path (speed / measure / stop logic untouched)
  shards x nodes: TxPool -> PBFT -> Chain/EVM
        |
        v
  exp/results/relay_stats_*.csv         exp/agentemu-results/*
  (chain measurements)                  (plan, events, registry, rounds)
        |
        v
  cmd/agentemu-report  ->  Agent_Stats.csv (plan vs chain reconciliation)
```

The blockchain kernel is not modified: the agent source only feeds the
existing transaction injection pipeline (`supervisor/txsource`). Legacy
`random_source` / `csv_source` experiments work exactly as before.

## Quick start

```bash
bash example_run_agent.sh    # 1 shard x 4 nodes + supervisor, runs until done
go run ./cmd/agentemu-report \
  -plan ./exp/agentemu-results/agent_transactions.jsonl \
  -chain ./exp/results/relay_stats_detail_tx_info.csv
```

`example_run_agent.sh` stops by itself: once every planned transaction is
injected and empty blocks pile up, the supervisor broadcasts the stop message
and the whole cluster exits.

To preview the transaction plan of round 1 without running a chain:

```bash
go run ./cmd/agentemu -config agentEmuConfig.yaml
```

## Trace format

JSONL, one action per line. `ts` is ordered (ties broken by line order), and
trace actions never carry DIDs — identities are assigned deterministically
from the seed (`did:broker:0x<40 hex>` = sha256(seed:agent_id) tail) and stay
stable across rounds and runs.

```json
{"agent_id":"agent-alice","action":"join","params_hash":"doc-alice-v1","ts":1}
{"agent_id":"agent-alice","action":"pay","target":"agent-bob","amount":5,"request_id":"r1-pay-1","ts":4}
{"agent_id":"agent-alice","action":"append_log","params_hash":"request-r1-pay-1","request_id":"r1-pay-1","ts":6}
{"agent_id":"agent-bob","action":"leave","params_hash":"exit-bob","ts":3}
```

| action | meaning | compiled to |
|---|---|---|
| `join` | agent enters, gets/keeps its DID | `DIDRegistry.register` call + audit |
| `pay` | transfer to another agent (must have joined) | normal transfer + audit |
| `append_log` | behavior log entry | buffered (`merkle-audit`) or per-entry call (`onchain-audit`) |
| `leave` | agent exits | `DIDRegistry.revoke` call + audit |

### Multi-round stories

Round files live in one directory named `round_1.jsonl`, `round_2.jsonl`, ...
Round 1 is the `experiment.trace` in `agentEmuConfig.yaml`; the following
rounds are picked up automatically whenever the transaction queue runs dry.
The run stops at whichever comes first:

- the next round file is missing,
- `agent_rounds` (in `config_agent.yaml`) is reached,
- `tx_number` is exhausted.

One Host lives for the whole run, so nonces stay continuous across rounds and
the DID registry carries identities over.

After the last round is served the agent source reports exhaustion
(`txsource.Exhaustible`), which lets the supervisor's regular empty-block stop
logic fire even when `tx_number` is larger than the actual transaction count.

## Configuration

Two layers, both independent of the legacy `config.yaml`:

**`agentEmuConfig.yaml`** — the agent side:

```yaml
base:
  result_dir: ./exp/agentemu-results   # plan / events / registry / rounds meta
experiment:
  seed: 20260903                       # deterministic DID allocation
  trace: ./traces/rounds/round_1.jsonl # first round; dir implies the rest
protocols:
  pay:     {plugin: direct-pay}
  audit:   {plugin: merkle-audit, contract_address: "0x...20", batch_size: 2}
  identity:{plugin: did-simple,   contract_address: "0x...30"}
```

**`config_agent.yaml`** — the chain side (BlockEmulator config):

```yaml
supervisor:
  tx_source:
    tx_source_type: "agent_source"
    agentemu_config: "./agentEmuConfig.yaml"
    agent_rounds: 2
```

## 接入自定义 Agent 服务（HTTP 反馈循环）

把 `agent_api.mode` 切到 `http` 后，多轮循环的"导演"就不再是剧本文件，而是一个外部 Agent 服务：仿真器在每轮交易**全部上链确认后**，把结果 POST 给它，它返回下一轮 trace 或 stop。

```yaml
# agentEmuConfig-http.yaml
agent_api:
  mode: http
  endpoint: "http://127.0.0.1:9092/next_trace"  # 任意仿真器可达地址（本机/远程均可）
  request_timeout_seconds: 30    # 单次请求超时
  retries: 3                     # 失败重试（指数退避），耗尽后仿真优雅终止
  wait_confirm_timeout_seconds: 120 # 问询前等待链上确认的上限
  chain_results_csv: "./exp/results/relay_stats_detail_tx_info.csv"
```

**请求契约**（仿真器 → Agent 服务，`POST {endpoint}`）：

```json
{
  "round": 2, "record_count": 3, "tx_count": 4,
  "agents": [{"agent_id":"agent-alice","did":"did:broker:0x..","active":true}],
  "chain": {"total":12,"confirmed":12,"missing":0,"success_rate":1.0,
            "duration_s":11.0,"throughput_tps":1.09,"avg_confirm_latency_ms":8000.0,
            "transactions":[{"hash":"..","confirmed":true,"latency_ms":8000.0,"cross_shard":false}]}
}
```

**响应契约**：`{"stop": <bool>, "trace": [<record>, ...]}`，record 字段同 trace 格式（`agent_id/action/target/amount/ts/request_id/params_hash`）。`stop: true` 或空 trace 结束仿真。

**语义要点**：

- 等待确认：问询前轮询链上 CSV 直到本轮全部交易 hash 出现（超时仅告警、带部分数据继续），Agent 总是基于真实的链上结果决策；
- 失败策略：Agent 服务超时/返回坏数据 → 重试（默认 3 次、指数退避）→ 仍失败则记一次错误并优雅终止，已上链数据与统计完整保留；
- trace 质量兜底：引用不存在的 agent 或非法 action 会被 Host 的既有校验拒绝（错误带轮次号）。

**模型与 API key 的位置**：谁直连 LLM 谁持有 key——仿真器只配 endpoint，模型名/api_base/api_key 全在 Agent 服务侧（启动参数 + 环境变量，不落明文）。

**示例服务**（`cmd/agentemu-example-agent`，也是开发模板）：

```bash
# 确定性策略（不依赖 LLM，用于验收闭环）
bash example_run_agent_http.sh

# DeepSeek 驱动（OpenAI 兼容；key 从环境变量读）
export DEEPSEEK_API_KEY=sk-your-key-here
bash example_run_agent_llm.sh
```

LLM 模式下决策由模型生成（temperature 0 + `response_format: json_object`），天然非确定：这类轮次不进入可复现性承诺（对应 04 规格书 `deterministic: false` 约定）。

自己写 Agent 服务时：保留请求/响应类型（见 `cmd/agentemu-example-agent/main.go` 的 `feedbackRequest` 与 `httpapi.NextTraceResponse`），替换决策函数即可；监听非本机地址时建议自行加鉴权头。

## Outputs

| file | writer | content |
|---|---|---|
| `exp/agentemu-results/agent_transactions.jsonl` | agent source | every compiled transaction (the plan) |
| `exp/agentemu-results/Agent_Events.csv` | agent source | per-action metric events |
| `exp/agentemu-results/agent_registry.json` | agent source | agent_id -> DID mapping and state |
| `exp/agentemu-results/agent_rounds.json` | agent source | per-round record/tx counts |
| `exp/results/relay_stats_*.csv` | BlockEmulator measure | on-chain lifecycles (unchanged) |
| `exp/agentemu-results/Agent_Stats.csv` | `cmd/agentemu-report` | plan vs chain reconciliation |

`Agent_Stats.csv` columns: `total, confirmed, missing, success_rate, rounds,
duration_s, throughput_tps, avg_confirm_latency_ms`.

## Design notes

- **In-process compilation**: the agentemu Host runs inside the supervisor
  (`tx_source = agent_source`), so transactions are served at the existing
  injection speed and reuse the existing measure/stop machinery.
- **No kernel changes**: contract-call transactions to the (not yet deployed)
  DID/audit addresses are data-on-chain placeholders; the EVM executes them
  as no-ops. Real contract deployment is future work.
- **Determinism**: DIDs derive from `seed + agent_id`; the trace ordering is
  `(ts, line number)`. Block packing itself still depends on runtime timing,
  as in any BlockEmulator run.

## Scope of this MVP

Not included (by design, see documents/MVP-PLAN.md): real smart-contract
deployment, experiment.lock/verify, LLM-driven agents (the `AgentAPI`
interface in `agentemu/agentapi.go` is the reserved extension point — e.g. an
HTTP-backed AI agent would implement `NextTrace`), and large-scale load
tests.
