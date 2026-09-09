#!/bin/bash

# AgentEmulator LLM feedback run: like example_run_agent_http.sh, but the
# example agent service decides via DeepSeek (any OpenAI-compatible API
# works). The API key is read from DEEPSEEK_API_KEY; the placeholder below
# only lets the pipeline start — replace it with a real key for actual LLM
# decisions.

set -ex

AGENT_ADDR=127.0.0.1:9092
MAX_ROUNDS=3
API_BASE="${DEEPSEEK_API_BASE:-https://api.deepseek.com/v1}"
MODEL="${DEEPSEEK_MODEL:-deepseek-chat}"

export DEEPSEEK_API_KEY="${DEEPSEEK_API_KEY:-sk-placeholder}"

# Delete the old experiment directory.
rm -rf ./exp/
mkdir -p ./exp/

# Download modules and pre-compile.
go mod download
go build ./...

# Build and start the example agent service in LLM mode (a built binary can
# be killed directly, unlike a `go run` child).
go build -o ./exp/agentemu-example-agent ./cmd/agentemu-example-agent
./exp/agentemu-example-agent \
  -addr "${AGENT_ADDR}" -max-rounds "${MAX_ROUNDS}" \
  -llm -api-base "${API_BASE}" -model "${MODEL}" &
AGENT_PID=$!

cleanup() {
  kill "${AGENT_PID}" 2>/dev/null || true
}
trap cleanup EXIT

# Start the consensus nodes of shard 0.
CLUSTER_PIDS=()
for j in 0 1 2 3; do
  go run cmd/consensusnode/main.go -shard_id=0 -node_id="${j}" -config=config_agent_http.yaml &
  CLUSTER_PIDS+=($!)
done

# Start the supervisor.
go run cmd/supervisor/main.go -shard_id=0x7fffffff -node_id=0 -config=config_agent_http.yaml &
CLUSTER_PIDS+=($!)

# The agent service never exits on its own; wait only for the cluster.
wait "${CLUSTER_PIDS[@]}"
